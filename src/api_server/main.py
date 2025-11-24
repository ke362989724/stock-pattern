# api_server/main.py
from fastapi import FastAPI
import subprocess
import uvicorn
import os
from pathlib import Path
from fastapi.responses import FileResponse


app = FastAPI(title="Stock Pattern Server")


BASE_DIR = Path(__file__).resolve().parent.parent
script_relative_path = os.path.join(BASE_DIR, "generate_csv_file.py")
CSV_SCRIPT_PATH = BASE_DIR / "existing_package" / "generate_csv_file.py"
INIT_SCRIPT_PATH = BASE_DIR / "existing_package" / "init.py"
ALL_PATTERN_PATH = BASE_DIR / "existing_package" / "all-daily.json"

@app.get("/generate")
async def generate_pattern():
    if not CSV_SCRIPT_PATH.exists() or not INIT_SCRIPT_PATH.exists():
        return "CSV script not found or init script not found"
    
    print("INIT_SCRIPT_PATH", INIT_SCRIPT_PATH)
    
    subprocess.run(["python", str(CSV_SCRIPT_PATH)], capture_output=True, text=True)
    subprocess.run(["python", str(INIT_SCRIPT_PATH), '--pattern', 'all'], capture_output=True, text=True)
    return FileResponse(
            path=str(ALL_PATTERN_PATH),
            media_type="application/json",
            filename="all-daily.json"   # this is the name the browser will save as
    )

if __name__ == "__main__":
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)