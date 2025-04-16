import subprocess

print("🎧 Listening to IPL live feed... (Ctrl+C to stop)\n")

process = subprocess.Popen(
    ["fluvio", "consume", "ipl-live-feed", "--tail", "1"],
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
    text=True
)


try:
    for line in process.stdout:
        print("✅ Received:", line.strip())
except KeyboardInterrupt:
    print("\n👋 Exiting consumer...")
    process.terminate()
