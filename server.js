import express from "express";
import multer, { diskStorage } from "multer";
import { dirname,join } from "path";
import { fileURLToPath } from "url";
import cors from "cors";
import { spawn, exec } from "child_process";


const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const app = express();
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

  const pythonProcess = spawn("python", [pythonScript,fileToProcess]);

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

app.listen(3001, () => {
  console.log("Server running on port 3001");
});
