import subprocess,json,time
from pathlib import Path
while True:
 p=json.loads(Path('progress.json').read_text())['phase']
 if p=='completed': break
 if p=='failed': raise SystemExit(1)
 time.sleep(10)
for mode in ('all','base'):
 subprocess.run(['python3','diagnose.py',mode],check=True)
Path('diagnostics-done').write_text('all and base complete\n')
