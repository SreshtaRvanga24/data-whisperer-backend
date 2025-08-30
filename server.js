import express from "express";
import multer, { diskStorage } from "multer";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import cors from "cors";
import { spawn, exec } from "child_process";
import fs from "fs"; // Add this import
import dotenv from "dotenv";
dotenv.config();

import path from "path";

import sf from "snowflake-sdk";

// ------------------ Env ------------------
const {
  SNOWFLAKE_ACCOUNT,
  SNOWFLAKE_USER,
  SNOWFLAKE_PASSWORD,
  SNOWFLAKE_ROLE,
  SNOWFLAKE_WAREHOUSE,
  SNOWFLAKE_DATABASE,
  SNOWFLAKE_SCHEMA,
  PORT = 8080,
} = process.env;

// ------------------ Utils ------------------
// Conservative whitelist for identifiers (DB/SCHEMA/TABLE)
const IDENT_RE = /^[A-Za-z0-9_]+$/;
function ensureIdent(name, what = "identifier") {
  if (typeof name !== "string" || !IDENT_RE.test(name)) {
    const err = new Error(`Invalid ${what}: ${name}`);
    err.status = 400;
    throw err;
  }
  return `'${name.toUpperCase()}'`;
}

function parseLimit(v, def = 50) {
  if (v === undefined || v === null || v === "") return def;
  const n = Number(v);
  if (!Number.isInteger(n) || n < 1 || n > 500) {
    const err = new Error("Invalid limit. Must be integer 1–500.");
    err.status = 400;
    throw err;
  }
  return n;
}

// ------------------ Snowflake conn ------------------
const connection = sf.createConnection({
  account: SNOWFLAKE_ACCOUNT,
  username: SNOWFLAKE_USER,
  password: SNOWFLAKE_PASSWORD,
  role: SNOWFLAKE_ROLE,
  warehouse: SNOWFLAKE_WAREHOUSE,
  database: SNOWFLAKE_DATABASE,
  schema: SNOWFLAKE_SCHEMA,
  application: "DW_CLEANSED_EXPLORER",
});

connection.connect((err, conn) => {
  if (err) {
    console.error("Unable to connect to Snowflake:", err.message);
    process.exit(1);
  } else {
    console.log("Connected to Snowflake as", conn.getId());
  }
});

function query({ sqlText, binds = [] }) {
  return new Promise((resolve, reject) => {
    connection.execute({
      sqlText,
      binds,
      complete: (err, _stmt, rows) => (err ? reject(err) : resolve(rows)),
    });
  });
}


const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const app = express();

// Add JSON parsing middleware
app.use(express.json());

app.use(
  cors({
    origin: "http://localhost:8080", // Your React app URL
    credentials: true,
  })
);

// Configure storage
const storage = diskStorage({
  destination: (req, file, cb) => {
    cb(null, "uploads/"); // Save to uploads folder
  },
  filename: (req, file, cb) => {
    cb(null, Date.now() + "-" + file.originalname);
  },
});

const upload = multer({ storage: storage });

// Upload endpoint
app.post("/api/upload", upload.single("file"), (req, res) => {
  if (!req.file) {
    return res.status(400).json({ error: "No file uploaded" });
  }
  const relativePath = req.file.path;
  const pythonScript = join(__dirname, "snowflake_conn.py");
  const fileToProcess = join(__dirname, relativePath);

  const pythonProcess = spawn("python", [pythonScript, fileToProcess]);

  let output = "";
  let errorOutput = "";

  pythonProcess.stdout.on("data", (data) => {
    output += data.toString();
    console.log("Python output:", data.toString());
  });

  pythonProcess.stderr.on("data", (data) => {
    errorOutput += data.toString();
    console.error("Python error:", data.toString());
  });

  pythonProcess.on("close", (code) => {
    console.log(`Python process exited with code ${code}`);

    if (code === 0) {
      console.log("Data processing completed successfully");
    } else {
      console.log("Python script execution failed");
    }
  });

  res.json({
    message: "File saved successfully",
    filename: req.file.filename,
    path: req.file.path,
  });
});

// // New endpoint to get Snowflake tables and columns
// app.get("/api/snowflake/tables", async (req, res) => {
//   try {
//     const pythonScript = join(__dirname, "get_snowflake_metadata.py");
    
//     const pythonProcess = spawn("python", [pythonScript]);
    
//     let output = "";
//     let errorOutput = "";

//     pythonProcess.stdout.on("data", (data) => {
//       output += data.toString();
//     });

//     pythonProcess.stderr.on("data", (data) => {
//       errorOutput += data.toString();
//     });

//     pythonProcess.on("close", (code) => {
//       if (code === 0) {
//         try {
//           const tables = JSON.parse(output);
//           res.json({ success: true, data: tables });
//         } catch (parseError) {
//           console.error("Error parsing Python output:", parseError);
//           res.status(500).json({ 
//             success: false, 
//             error: "Failed to parse metadata response" 
//           });
//         }
//       } else {
//         console.error("Python script failed:", errorOutput);
//         res.status(500).json({ 
//           success: false, 
//           error: "Failed to fetch Snowflake metadata",
//           details: errorOutput 
//         });
//       }
//     });

//   } catch (error) {
//     console.error("Server error:", error);
//     res.status(500).json({ 
//       success: false, 
//       error: "Internal server error" 
//     });
//   }
// });

// // NEW: Dashboard save endpoint
// app.post("/api/dashboard/save", async (req, res) => {
//   try {
//     const dashboardData = req.body;
    
//     // Validate required fields
//     if (!dashboardData.name || !dashboardData.config) {
//       return res.status(400).json({
//         success: false,
//         error: "Dashboard name and config are required"
//       });
//     }

//     // Create dashboards directory if it doesn't exist
//     const dashboardsDir = join(__dirname, "dashboards");
//     if (!fs.existsSync(dashboardsDir)) {
//       fs.mkdirSync(dashboardsDir, { recursive: true });
//     }

//     // Create filename with timestamp
//     const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
//     const safeName = dashboardData.name.replace(/[^a-zA-Z0-9]/g, '_');
//     const filename = `${safeName}_${timestamp}.json`;
//     const filepath = join(dashboardsDir, filename);

//     // Add metadata to dashboard data
//     const enrichedData = {
//       ...dashboardData,
//       id: Date.now().toString(),
//       savedAt: new Date().toISOString(),
//       version: "1.0.0",
//       metadata: {
//         totalTables: Object.keys(dashboardData.config.dashboardConfig || {}).length,
//         totalColumns: Object.values(dashboardData.config.dashboardConfig || {})
//           .reduce((sum, table) => sum + (table.selectionCount || 0), 0),
//         createdBy: "dashboard-builder",
//         format: "snowflake-dashboard-config"
//       }
//     };

//     // Write to file
//     fs.writeFileSync(filepath, JSON.stringify(enrichedData, null, 2));

//     console.log(`Dashboard saved: ${filename}`);

//     res.json({
//       success: true,
//       data: {
//         id: enrichedData.id,
//         filename,
//         path: filepath,
//         relativePath: `dashboards/${filename}`,
//         dashboard: enrichedData,
//         message: "Dashboard saved successfully"
//       }
//     });

//   } catch (error) {
//     console.error("Error saving dashboard:", error);
//     res.status(500).json({
//       success: false,
//       error: "Failed to save dashboard",
//       details: error.message
//     });
//   }
// });

// // NEW: Get all saved dashboards
// app.get("/api/dashboards", async (req, res) => {
//   try {
//     const dashboardsDir = join(__dirname, "dashboards");
    
//     // Create directory if it doesn't exist
//     if (!fs.existsSync(dashboardsDir)) {
//       fs.mkdirSync(dashboardsDir, { recursive: true });
//       return res.json({ success: true, data: [] });
//     }

//     // Read all dashboard files
//     const files = fs.readdirSync(dashboardsDir)
//       .filter(file => file.endsWith('.json'))
//       .sort((a, b) => b.localeCompare(a)); // Sort by filename (newest first)

//     const dashboards = files.map(filename => {
//       try {
//         const filepath = join(dashboardsDir, filename);
//         const data = JSON.parse(fs.readFileSync(filepath, 'utf8'));
        
//         return {
//           id: data.id || filename.replace('.json', ''),
//           name: data.name,
//           filename,
//           createdAt: data.createdAt || data.savedAt,
//           metadata: data.metadata || {},
//           config: data.config,
//           summary: {
//             totalTables: Object.keys(data.config?.dashboardConfig || {}).length,
//             totalColumns: Object.values(data.config?.dashboardConfig || {})
//               .reduce((sum, table) => sum + (table.selectionCount || 0), 0)
//           }
//         };
//       } catch (error) {
//         console.error(`Error reading dashboard file ${filename}:`, error);
//         return null;
//       }
//     }).filter(Boolean); // Remove null entries

//     res.json({
//       success: true,
//       data: dashboards
//     });

//   } catch (error) {
//     console.error("Error fetching dashboards:", error);
//     res.status(500).json({
//       success: false,
//       error: "Failed to fetch dashboards",
//       details: error.message
//     });
//   }
// });

// // NEW: Get specific dashboard by ID
// app.get("/api/dashboards/:id", async (req, res) => {
//   try {
//     const { id } = req.params;
//     const dashboardsDir = join(__dirname, "dashboards");
    
//     if (!fs.existsSync(dashboardsDir)) {
//       return res.status(404).json({
//         success: false,
//         error: "Dashboard not found"
//       });
//     }

//     // Find file by ID (check both ID and filename)
//     const files = fs.readdirSync(dashboardsDir).filter(file => file.endsWith('.json'));
//     let targetFile = null;

//     for (const filename of files) {
//       try {
//         const filepath = join(dashboardsDir, filename);
//         const data = JSON.parse(fs.readFileSync(filepath, 'utf8'));
        
//         if (data.id === id || filename.replace('.json', '') === id) {
//           targetFile = { filename, data };
//           break;
//         }
//       } catch (error) {
//         continue;
//       }
//     }

//     if (!targetFile) {
//       return res.status(404).json({
//         success: false,
//         error: "Dashboard not found"
//       });
//     }

//     res.json({
//       success: true,
//       data: targetFile.data
//     });

//   } catch (error) {
//     console.error("Error fetching dashboard:", error);
//     res.status(500).json({
//       success: false,
//       error: "Failed to fetch dashboard",
//       details: error.message
//     });
//   }
// });

// // NEW: Delete dashboard
// app.delete("/api/dashboards/:id", async (req, res) => {
//   try {
//     const { id } = req.params;
//     const dashboardsDir = join(__dirname, "dashboards");
    
//     if (!fs.existsSync(dashboardsDir)) {
//       return res.status(404).json({
//         success: false,
//         error: "Dashboard not found"
//       });
//     }

//     // Find and delete file
//     const files = fs.readdirSync(dashboardsDir).filter(file => file.endsWith('.json'));
//     let deleted = false;

//     for (const filename of files) {
//       try {
//         const filepath = join(dashboardsDir, filename);
//         const data = JSON.parse(fs.readFileSync(filepath, 'utf8'));
        
//         if (data.id === id || filename.replace('.json', '') === id) {
//           fs.unlinkSync(filepath);
//           deleted = true;
//           console.log(`Dashboard deleted: ${filename}`);
//           break;
//         }
//       } catch (error) {
//         continue;
//       }
//     }

//     if (!deleted) {
//       return res.status(404).json({
//         success: false,
//         error: "Dashboard not found"
//       });
//     }

//     res.json({
//       success: true,
//       message: "Dashboard deleted successfully"
//     });

//   } catch (error) {
//     console.error("Error deleting dashboard:", error);
//     res.status(500).json({
//       success: false,
//       error: "Failed to delete dashboard",
//       details: error.message
//     });
//   }
// });

app.listen(3001, () => {
  console.log("Server running on port 3001");
  
  // Create necessary directories on startup
  const dirs = ['uploads', 'dashboards'];
  dirs.forEach(dir => {
    const dirPath = join(__dirname, dir);
    if (!fs.existsSync(dirPath)) {
      fs.mkdirSync(dirPath, { recursive: true });
      console.log(`Created directory: ${dir}`);
    }
  });
});

// POST /api/dashboard/auto-save
// POST /api/dashboard/auto-save
// app.post("/api/dashboard/auto-save", async (req, res) => {
//   try {
//     const { sessionId, name, config, createdAt } = req.body;

//     // Create directory path
//     const savedDashboardsDir = join(__dirname, "saved_dashboards");

//     // Ensure directory exists
//     if (!fs.existsSync(savedDashboardsDir)) {
//       fs.mkdirSync(savedDashboardsDir, { recursive: true });
//     }

//     const fileName = `dashboard_${sessionId}.json`;
//     const filePath = join(savedDashboardsDir, fileName);

//     // Save the dashboard data
//     fs.writeFileSync(
//       filePath,
//       JSON.stringify(
//         {
//           sessionId,
//           name,
//           config,
//           createdAt,
//           lastUpdated: new Date().toISOString(),
//         },
//         null,
//         2
//       )
//     );

//     res.json({
//       success: true,
//       message: "Dashboard auto-saved successfully",
//       sessionId,
//     });
//   } catch (error) {
//     console.error("Auto-save error:", error);
//     res.status(500).json({
//       success: false,
//       error: error.message,
//     });
//   }
// });

app.get("/api/cleansed/tables", async (req, res, next) => {
  try {
    const schema = (req.query.schema || SNOWFLAKE_SCHEMA || "").toString();
    const targetSchema = ensureIdent(schema, "schema");
// AND TABLE_TYPE = 'BASE TABLE'
    let s =  `
        SELECT TABLE_NAME, ROW_COUNT
        FROM ${SNOWFLAKE_DATABASE}.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = ${targetSchema}
        ORDER BY TABLE_NAME;
      `;
      console.log("query print",s);
    const rows = await query({
      sqlText: `
        SELECT TABLE_NAME, ROW_COUNT
        FROM ${SNOWFLAKE_DATABASE}.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = ${targetSchema}
        ORDER BY TABLE_NAME;
      `,
      binds: [schema.toUpperCase()],
    });

    const data = rows.map((r) => ({
      name: r.TABLE_NAME,
      rowCount: r.ROW_COUNT || 0,
    }));

    res.json(data);
  } catch (e) {
    next(e);
  }
});

/**
 * 2) Fetch sample data for a specific table
 * GET /api/cleansed/tables/:table/data?schema=CLEANSED&limit=50
 * Response: { columns: ["ID","EMAIL",...], rows: [ {ID:1,EMAIL:"..."}, ... ] }
 */
app.get("/api/cleansed/tables/:table/data", async (req, res, next) => {
  try {
    const schema = (req.query.schema || SNOWFLAKE_SCHEMA || "").toString();
    const table = (req.params.table || "").toString();
    const limit = parseLimit(req.query.limit);

    // Validate identifiers
    // const db = ensureIdent(SNOWFLAKE_DATABASE, "database");
    // const sch = ensureIdent(schema, "schema");
    // const tbl = ensureIdent(table, "table");

    const fq = `${SNOWFLAKE_DATABASE}.${schema}.${table}`;
    let s = `SELECT * FROM ${fq} LIMIT = ${limit}`;
    console.log("query print",s);
    // Get sample rows
    const rows = await query({
      sqlText: `SELECT * FROM ${fq} LIMIT ${limit};`,
      binds: [limit],
    });

    const columns = rows.length ? Object.keys(rows[0]) : [];

    res.json({ columns, rows });
  } catch (e) {
    next(e);
  }
});

// GET /api/cleansed/tables/:table/quality?schema=CLEANSED
app.get("/api/cleansed/tables/:table/quality", async (req, res, next) => {
  try {
    const schema = (req.query.schema || SNOWFLAKE_SCHEMA || "").toString();
    const table  = (req.params.table || "").toString();

    // 1) pull column names in order
    const cols = await query({
      sqlText: `
        SELECT COLUMN_NAME, DATA_TYPE
        FROM ${SNOWFLAKE_DATABASE}.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
        ORDER BY ORDINAL_POSITION
      `,
      binds: [schema.toUpperCase(), table.toUpperCase()],
    });
    if (!cols.length) return res.json({ total: 0, columns: [] });

    // 2) build aggregate SELECT in one pass
    const fq =
      `${SNOWFLAKE_DATABASE}.` +
      `${schema}.` +
      `${table}`;

    const pieces = [
      `COUNT(*) AS TOTAL`
    ];

    cols.forEach(c => {
      const col = `${c.COLUMN_NAME}`;
      pieces.push(
        `SUM(IFF(${col} IS NOT NULL, 1, 0)) AS ${col}_NON_NULL`,
        `COUNT(DISTINCT ${col}) AS ${col}_DISTINCT`
      );
      // blank count is only meaningful for strings
      if (String(c.DATA_TYPE).toUpperCase().includes("CHAR")) {
        pieces.push(`SUM(IFF(${col} IS NULL OR REGEXP_REPLACE(${col}, '\\\\s+', '') = '', 1, 0)) AS ${col}_BLANK`);
      } else {
        pieces.push(`SUM(IFF(${col} IS NULL, 1, 0)) AS ${col}_BLANK`); // equals nulls for non-strings
      }
    });

    const sql = `SELECT ${pieces.join(",\n")} FROM ${fq}`;
    console.log('aa', sql)
    const agg = await query({ sqlText: sql });
    const row = agg[0];
    const total = Number(row.TOTAL) || 0;

    const out = cols.map(c => {
      const key = `${c.COLUMN_NAME}`;
      const nonNull   = Number(row[`${key}_NON_NULL`]) || 0;
      const distinct  = Number(row[`${key}_DISTINCT`]) || 0;
      const blanks    = Number(row[`${key}_BLANK`]) || 0;
      const nulls     = total - nonNull;
      const pct       = total ? Math.round((nonNull / total) * 1000) / 10 : 0; // 1 decimal

      return {
        column: c.COLUMN_NAME,
        dataType: c.DATA_TYPE,
        total,
        nonNull,
        nulls,
        blanks,          // for VARCHAR = nulls+empty/whitespace
        distinctCount: distinct,
        completenessPct: pct
      };
    });

    // table-level average completeness (simple mean of column pct)
    const avgCompleteness = out.length
      ? Math.round(out.reduce((s, x) => s + x.completenessPct, 0) / out.length * 10) / 10
      : 0;

    res.json({ total, avgCompleteness, columns: out });
  } catch (e) {
    next(e);
  }
});
