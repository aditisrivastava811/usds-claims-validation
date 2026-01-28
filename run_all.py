from pathlib import Path
from datetime import datetime

def main():
    now = datetime.now().isoformat()

    outputs = Path("outputs")
    outputs.mkdir(exist_ok=True)

    report_path = outputs / "setup_check.txt"
    report_path.write_text(
        f"Setup successful!\nTimestamp: {now}\n",
        encoding="utf-8"
    )

    print("✅ Setup check complete.")
    print(f"Created: {report_path}")

if __name__ == "__main__":
    main()

