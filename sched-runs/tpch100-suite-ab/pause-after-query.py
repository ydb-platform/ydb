from pathlib import Path
import json,os,signal,time
root=Path("/home/mfilitov/tpch100-suite-ab")
pid=int((root/"suite.pid").read_text())
for _ in range(18300):
    data=json.loads((root/"runs.json").read_text())
    if len(data)>=20:break
    time.sleep(0.1)
else:raise RuntimeError("current query did not finish")
assert b"/home/mfilitov/tpch100-suite-ab/run_suite.py" in Path(f"/proc/{pid}/cmdline").read_bytes()
os.kill(pid,signal.SIGTERM)
for _ in range(120):
    path=Path(f"/proc/{pid}/stat")
    if not path.exists() or path.read_text().split()[2]=="Z":break
    time.sleep(0.25)
else:raise RuntimeError("benchmark did not stop")
progress=json.loads((root/"progress.json").read_text())
progress["phase"]="paused-q8-memory-budget"
(root/"progress.json").write_text(json.dumps(progress,indent=2)+"\n")
(root/"paused-for-q8").write_text("stopped\n")
print(len(data),flush=True)
