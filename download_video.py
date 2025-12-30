import boto3
import os
from botocore.exceptions import ClientError
import os
import requests
from typing import List
from urllib.parse import urlparse


def download_video_from_s3(
    s3_url: str,
    output_dir: str = "videos",
    filename: str | None = None,
    timeout: int = 60,
) -> str:
    """
    Download a video file from an S3 URL (public or presigned).

    Args:
        s3_url (str): S3 HTTP/HTTPS URL
        output_dir (str): Directory to save the video
        filename (str | None): Optional custom filename
        timeout (int): Request timeout in seconds

    Returns:
        str: Local path to the downloaded video
    """

    os.makedirs(output_dir, exist_ok=True)

    if filename is None:
        parsed = urlparse(s3_url)
        filename = os.path.basename(parsed.path)

        if not filename:
            raise ValueError("Could not infer filename from S3 URL")

    output_path = os.path.join(output_dir, filename)

    with requests.get(s3_url, stream=True, timeout=timeout) as response:
        response.raise_for_status()

        with open(output_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)

    return output_path



def upload_video_to_s3(
    file_path: str,
    bucket_name: str,
    s3_key: str,
    region: str = "ap-south-1",
    content_type: str = "video/mp4",
) -> str:
    """
    Upload a video file to S3.

    Args:
        file_path (str): Local path to video
        bucket_name (str): S3 bucket name
        s3_key (str): Path inside bucket (e.g. videos/demo.mp4)
        region (str): AWS region
        content_type (str): MIME type

    Returns:
        str: Public or S3 object URL
    """

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"{file_path} does not exist")

    s3 = boto3.client("s3", region_name=region)

    try:
        s3.upload_file(
            Filename=file_path,
            Bucket=bucket_name,
            Key=s3_key,
            ExtraArgs={
                "ContentType": content_type
            }
        )
    except ClientError as e:
        raise RuntimeError(f"S3 upload failed: {e}")

    return f"https://{bucket_name}.s3.{region}.amazonaws.com/{s3_key}"



def update_meta_campaign_assets(
    microbrief_id: str,
    asset_urls: List[str],
    base_url: str = "https://campaign.dev.whilter.ai",
    timeout: int = 20,
) -> dict:
    """
    Send PUT request to update meta-campaign content with uploaded asset URLs.

    Args:
        microbrief_id (str): Microbrief ID
        asset_urls (List[str]): List of uploaded S3 video URLs
        base_url (str): Campaign service base URL
        timeout (int): Request timeout

    Returns:
        dict: JSON response from server
    """

    url = f"{base_url}/meta-campaign/update-meta-campaign-content/{microbrief_id}"

    payload = {
        "assets": asset_urls
    }

    headers = {
        "Content-Type": "application/json"
    }

    response = requests.put(
        url,
        json=payload,
        headers=headers,
        timeout=timeout
    )

    response.raise_for_status()
    return response.json()

