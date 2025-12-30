import math
import subprocess
import json
import os

# --- CONFIGURATION ---
IN_PATH  = "/Users/anas/Desktop/Editor-test/2test.mp4"
OUT_PATH = "/Users/anas/Desktop/Editor-test/out_ramp_slowfast_end.mp4"

MIN_SPEED = 1.0
MAX_SPEED = 5.0
TARGET_SEGMENTS = 240 
CRF = 18
PRESET = "veryfast"

def get_video_info(path: str):
    if not os.path.exists(path):
        raise FileNotFoundError(f"Input file not found: {path}")

    cmd = [
        "ffprobe", "-v", "error",
        "-select_streams", "v:0",
        "-show_entries", "format=duration:stream=r_frame_rate,nb_frames",
        "-of", "json",
        path
    ]
    
    out = subprocess.check_output(cmd).decode().strip()
    data = json.loads(out)
    
    try:
        duration = float(data['format']['duration'])
    except KeyError:
        duration = 0.0

    stream = data['streams'][0]
    fps_str = stream.get('r_frame_rate', '30/1')
    num, den = map(int, fps_str.split('/'))
    fps = num / den if den != 0 else 30.0

    if 'nb_frames' in stream:
        total_frames = int(stream['nb_frames'])
    else:
        total_frames = int(duration * fps)

    return duration, total_frames, fps

def speed_at(u: float) -> float:
    """
    u in [0..1]
    Slow (1x) -> Fast (5x) using half sine:
    - u=0   (Start) => sin(0)=0    => MIN_SPEED
    - u=1   (End)   => sin(pi/2)=1 => MAX_SPEED
    """
    return MIN_SPEED + (MAX_SPEED - MIN_SPEED) * (math.sin(u * math.pi / 2) ** 2)

def run_sifo(
    input_path: str,
    output_path: str,
):
    try:
        duration, total_frames, fps = get_video_info(input_path)
    except Exception as e:
        print(f"❌ Error reading video info: {e}")
        return

    print(f"   Video info: {duration:.2f}s | {total_frames} frames | {fps:.2f} fps")

    # --- SAFETY LOGIC ---
    # Since the END is 5x, we still need the safety check
    min_frames_per_seg = math.ceil(MAX_SPEED)

    max_segments = int(total_frames / min_frames_per_seg)
    actual_segments = min(TARGET_SEGMENTS, max_segments)

    if actual_segments < 1:
        actual_segments = 1

    frames_per_seg = int(total_frames / actual_segments)

    print(f"   Safety Check: Max speed is {MAX_SPEED}x. Segments need >= {min_frames_per_seg} frames.")
    print(f"   Processing: {actual_segments} segments ({frames_per_seg} frames each).")

    chains = []
    labels = []
    current_frame = 0

    for i in range(actual_segments):
        start_f = current_frame
        end_f = total_frames if i == actual_segments - 1 else current_frame + frames_per_seg

        if start_f >= total_frames:
            break

        u = i / actual_segments
        s = speed_at(u)

        lab = f"v{i}"
        labels.append(f"[{lab}]")

        chains.append(
            f"[0:v]trim=start_frame={start_f}:end_frame={end_f},"
            f"setpts=PTS-STARTPTS,"
            f"setpts=PTS/{s:.4f}"
            f"[{lab}]"
        )

        current_frame = end_f

    chains.append(f"{''.join(labels)}concat=n={len(labels)}:v=1:a=0[v]")
    filter_complex = ";".join(chains)

    if len(filter_complex) > 30000:
        print("⚠️ Warning: Filter chain is very long.")

    cmd = [
        "ffmpeg", "-y",
        "-i", input_path,
        "-filter_complex", filter_complex,
        "-map", "[v]",
        "-an",
        "-c:v", "libx264",
        "-crf", str(CRF),
        "-preset", PRESET,
        "-movflags", "+faststart",
        output_path
    ]

    print("🚀 Running FFmpeg (SIFO)...")
    subprocess.run(cmd, check=True)
    print(f"✅ Done! Output saved to: {output_path}")

if __name__ == "__main__":
    runSIFO()