from pathlib import Path
import json, os, signal, subprocess, time
root=Path("/home/mfilitov/tpcc-scheduler-ab")
suite=Path("/home/mfilitov/tpch100-suite-ab")
root.mkdir(exist_ok=True)
def save(phase,**kw):
 (root/"setup-status.json").write_text(json.dumps(dict(phase=phase,**kw),indent=2)+"\n")
pid=int((suite/"suite.pid").read_text())
initial=len(json.loads((suite/"runs.json").read_text()))
save("waiting-for-tpch-query",completed=initial)
deadline=time.monotonic()+1800
while time.monotonic()<deadline:
 try: stat=Path(f"/proc/{pid}/stat").read_text().split()[2]
 except FileNotFoundError:break
 if stat=="Z":break
 if len(json.loads((suite/"runs.json").read_text()))>initial:
  assert b"/home/mfilitov/tpch100-suite-ab/run_suite.py" in Path(f"/proc/{pid}/cmdline").read_bytes()
  os.kill(pid,signal.SIGTERM)
  break
 time.sleep(0.1)
else:raise RuntimeError("TPC-H query did not finish")
for _ in range(300):
 try: stat=Path(f"/proc/{pid}/stat").read_text().split()[2]
 except FileNotFoundError:break
 if stat=="Z":break
 time.sleep(0.1)
else:raise RuntimeError("TPC-H runner did not stop")
(root/"resume-tpch-after-tpcc").write_text(str(pid)+"\n")
save("building-cli",command=["./ya","make","-r","ydb/apps/ydb"])
with (root/"cli-build.log").open("w") as log:
 result=subprocess.run(["./ya","make","-r","ydb/apps/ydb"],cwd="/home/mfilitov/ydb-pr56258-local-consumer",stdout=log,stderr=subprocess.STDOUT)
save("cli-ready" if result.returncode==0 else "cli-build-failed",exit_code=result.returncode)

