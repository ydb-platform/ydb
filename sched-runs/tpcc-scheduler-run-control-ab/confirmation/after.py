import time,json,subprocess
from pathlib import Path
while not Path('../diagnostics-done').exists():
 state=json.loads(Path('../progress.json').read_text())['phase']
 if state=='failed': raise SystemExit(1)
 time.sleep(10)
subprocess.run(['python3','run.py'],check=True)
