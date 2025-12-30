import os
import json
import math
import uuid
import shutil
import subprocess
import tempfile
import requests
import boto3
from urllib.parse import urlparse
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from speedrampFIFO import run_fifo
from speedrampFISO import run_fiso
from speedrampSIFO import run_sifo
from speedrampSISO import run_siso


MIN_SPEED = 1.0
MAX_SPEED = 5.0
# The script will now override this if it's unsafe
TARGET_SEGMENTS = 240 
CRF = 18
PRESET = "veryfast"
# -----------------------------
# Helper: download from S3 URL
# -----------------------------


app = FastAPI()

CAMPAIGN_BASE_URL = "https://campaign.dev.whilter.ai"

class VideoRequest(BaseModel):
    microbriefId: str
    input_s3_url: str
    category: str



class VideoResponse(BaseModel):
    assets: list[str]


def download_from_s3_url(s3_url: str, dst_path: str):
    with requests.get(s3_url, stream=True, timeout=60) as r:
        r.raise_for_status()
        with open(dst_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)

# -----------------------------
# Helper: upload to S3
# -----------------------------
def upload_to_s3(
    file_path: str,
    bucket: str,
    key: str,
    region: str = "ap-south-1",
) -> str:
    s3 = boto3.client("s3", region_name=region)

    s3.upload_file(
        file_path,
        bucket,
        key,
        ExtraArgs={"ContentType": "video/mp4"}
    )

    return f"https://{bucket}.s3.{region}.amazonaws.com/{key}"

# -----------------------------
# Your existing helpers (unchanged)
# -----------------------------
def get_video_info(path: str):
    cmd = [
        "ffprobe", "-v", "error",
        "-select_streams", "v:0",
        "-show_entries", "format=duration:stream=r_frame_rate,nb_frames",
        "-of", "json",
        path
    ]
    out = subprocess.check_output(cmd).decode()
    data = json.loads(out)

    duration = float(data["format"].get("duration", 0.0))
    stream = data["streams"][0]

    num, den = map(int, stream.get("r_frame_rate", "30/1").split("/"))
    fps = num / den if den else 30.0

    total_frames = int(stream.get("nb_frames", duration * fps))
    return duration, total_frames, fps


def speed_at(u: float) -> float:
    return MIN_SPEED + (MAX_SPEED - MIN_SPEED) * (math.cos(math.pi * u) ** 2)

# -------------------------------------------------
# 🚀 MAIN PIPELINE: S3 URL → S3 URL
# -------------------------------------------------
def process_video_from_s3_to_s3(
    input_s3_url: str,
    output_bucket: str,
    category:str,
    output_prefix: str = "processed-videos/",
    region: str = "ap-south-1",
) -> str:
    """
    Downloads a video from S3 URL, processes it, uploads result to S3.

    Returns:
        str: Output S3 URL
    """

    workdir = tempfile.mkdtemp()
    try:
        input_path = os.path.join(workdir, "input.mp4")
        output_path = os.path.join(workdir, "output.mp4")

        # 1️⃣ Download
        download_from_s3_url(input_s3_url, input_path)

        match category.upper():
            case "FIFO":
                run_fifo(input_path,output_path)
            case "FISO":
                run_fiso(input_path,output_path)
            case "SIFO":
                run_sifo(input_path,output_path)
            case "SISO":
                run_siso(input_path,output_path)
            

        # 4️⃣ Upload output
        output_key = f"{output_prefix}{uuid.uuid4().hex}.mp4"
        return upload_to_s3(output_path, output_bucket, output_key, region)

    finally:
        shutil.rmtree(workdir)


@app.post("/process-video", response_model=VideoResponse)
def process_video(req: VideoRequest):
    try:

        # 1️⃣ Process video (S3 → S3)
        output_url = process_video_from_s3_to_s3(
            input_s3_url=req.input_s3_url,
            category=req.category,
            output_bucket="company-garden"
        )

        # 2️⃣ PUT request to campaign service
        put_url = (
            f"{CAMPAIGN_BASE_URL}/meta-campaign/"
            f"update-meta-campaign-content/{req.microbriefId}"
        )

        payload = {
            "assets": [output_url]
        }

        headers = {
            "Content-Type": "application/json"
        }

        resp = requests.put(
            put_url,
            json=payload,
            headers=headers,
            timeout=20
        )

        resp.raise_for_status()

        # 3️⃣ Return required response
        return {"assets": [output_url]}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__== "__main__":
    print(upload_to_s3('fourth.mp4','company-garden','video1'))
#     print(process_video_from_s3_to_s3('https://company-garden.s3.ap-south-1.amazonaws.com/videos','company-garden'))

    