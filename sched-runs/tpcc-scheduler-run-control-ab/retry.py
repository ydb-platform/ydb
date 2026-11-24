import subprocess,json
from pathlib import Path
subprocess.run(['python3','build.py'],check=True)
if json.loads(Path('build-status.json').read_text())['phase']!='ready': raise SystemExit(1)
with Path('run.log').open('w') as log:
 subprocess.run(['python3','run.py'],stdout=log,stderr=subprocess.STDOUT,check=True)
if json.loads(Path('progress.json').read_text())['phase']!='completed':raise SystemExit(1)
with Path('trace-after.log').open('w') as log:
 subprocess.run(['python3','trace-after.py'],stdout=log,stderr=subprocess.STDOUT,check=True)
