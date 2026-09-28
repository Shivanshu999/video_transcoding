//This script runs inside the container launched above. It processes one video and then shuts down.
import {
  S3Client,
  GetObjectCommand,
  PutObjectCommand,
} from "@aws-sdk/client-s3";
import fs from "node:fs";
import fsPromises from "node:fs/promises";
import path from "node:path";
import { pipeline } from "node:stream/promises"; // Required for safe downloads
import ffmpeg from "fluent-ffmpeg"; //use spawn instead of fluent-ffmpeg 

const RESOLUTIONS = [
  { name: "720p", width: 1280, height: 720 },
  { name: "480p", width: 854, height: 480 },
  { name: "360p", width: 640, height: 360 },
];

const s3Client = new S3Client({
    region: "us-east-1",  
    credentials: {
        accessKeyId: process.env.AWS_ACCESS_KEY_ID || "",
        secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY || "",
    },
});

// Env variables passed from the orchestrator
const BUCKET_NAME = process.env.BUCKET_NAME;
const KEY = process.env.KEY || "videos/IMG_1343.MOV"; // Fallback for testing

async function init() {
  console.log(`Starting download for: ${KEY}`);

   //GetObjectCommand: Ask S3 for the file.
  const command = new GetObjectCommand({ 
    Bucket: BUCKET_NAME,
    Key: KEY,
  });
  // Send the command to S3
  const result = await s3Client.send(command);

  // 1. Ensure local directory exists
  await fsPromises.mkdir("videos", { recursive: true });

  // 2. Determine local file path (keep original extension)
  const originalFilePath = path.join("videos", path.basename(KEY));
  
  //pipeline(...): This is a Stream. It downloads the file chunk-by-chunk directly to the hard drive.
  //Why? If you loaded a 2GB video into a variable (RAM), your container would crash. Streaming handles files of any size efficiently.

  await pipeline(result.Body, fs.createWriteStream(originalFilePath));
  
  // Confirm download
  const originalVideoPath = path.resolve(originalFilePath);
  console.log("Original video downloaded to:", originalVideoPath);

  // Start the transcoder

  //RESOLUTIONS.map: We create a transcoding job for each resolution.
  const promises = RESOLUTIONS.map((resolution) => {
    // Create a unique output name to prevent overwriting if multiple files run
    const output = `video-${resolution.name}-${path.basename(KEY, path.extname(KEY))}.mp4`;

    //new Promise: We wrap the FFmpeg job in a Promise so we can use await later.
    return new Promise((resolve, reject) => {
      ffmpeg(originalVideoPath)
        .output(output)
        // libx264: The standard video compression codec (H.264).
        .withVideoCodec("libx264")
        .withAudioCodec("aac")
        .withSize(`${resolution.width}x${resolution.height}`)
        .on('start', () => console.log(`Starting ${resolution.name} conversion`))
        //.on("end"): This triggers when FFmpeg finishes converting one resolution.
        //
        .on("end", async () => {
          console.log(`Finished ${resolution.name} Uploading...`);
          
          try { 
            // Read the processed file from disk for upload
            const fileStream = fs.createReadStream(output);

            //PutObjectCommand: Uploads the new file to the "production" bucket.
            const putCommand = new PutObjectCommand({
                Bucket: "production-bucket.xyz",
                Key: output,
                Body: fileStream, // Stream the upload body
            });
            
            await s3Client.send(putCommand);
            console.log(`Uploaded ${output}`);
            
            // Optional: Delete local temp file to save space
            //fsPromises.unlink(output): Deletes the local converted file immediately to free up disk space.
            await fsPromises.unlink(output);
            
            resolve();
          } catch (err) {
            console.error(`Upload failed for ${resolution.name}`, err);
            reject(err);
          }
        })
        .on("error", (err) => {
          console.error(`FFmpeg error for ${resolution.name}:`, err);
          reject(err);
        })
        .format("mp4")
        .run();
    });
  });

  //Promise.all: This pauses the script until all 3 resolutions (720p, 480p, 360p) are finished and uploaded. Once this line passes, the script ends, and the Docker container shuts down automatically.
  await Promise.all(promises);
  console.log("All resolutions processed and uploaded.");
}

init().catch((err) => {
  console.error("Script failed:", err);
  process.exit(1);
});