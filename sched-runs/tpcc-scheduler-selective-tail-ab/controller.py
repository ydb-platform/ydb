import json,subprocess,time
from pathlib import Path
previous=Path('/home/mfilitov/tpcc-scheduler-tail-ab/progress.json')
repo=Path('/home/mfilitov/ydb-pr56258-local-consumer')
while True:
    p=json.loads(previous.read_text())['phase']
    if p=='completed': break
    if p=='failed': raise RuntimeError('previous suite failed')
    time.sleep(5)
subprocess.run(['git','apply','--check',str(Path('selective.patch').resolve())],cwd=repo,check=True)
subprocess.run(['git','apply',str(Path('selective.patch').resolve())],cwd=repo,check=True)
subprocess.run(['git','diff','--check'],cwd=repo,check=True)
subprocess.run(['python3','build.py'],check=True)
if json.loads(Path('build-status.json').read_text())['phase']!='ready': raise RuntimeError('build failed')
subprocess.run(['python3','run.py'],check=True)
