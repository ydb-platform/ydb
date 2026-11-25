from pathlib import Path
import datetime,json,subprocess,time
root=Path("/home/mfilitov/tpcc-scheduler-continuation-ab")
folder=root/"on-1"
deadline=time.monotonic()+900
while time.monotonic()<deadline:
 log=folder/"run.stderr"
 if log.exists():
  lines=log.read_text(errors="replace").splitlines()
  line=next((s for s in lines if "Starting warmup for " in s),None)
  if line:
   started=datetime.datetime.fromisoformat(line.split()[0].replace("Z","+00:00")).timestamp()
   break
 if json.loads((root/"progress.json").read_text()).get("phase")=="failed":raise RuntimeError("benchmark failed")
 time.sleep(.1)
else:raise RuntimeError("no ON warmup")
while time.time()<started+8:time.sleep(.05)
if time.time()>started+10:raise RuntimeError("too late to profile warmup")
pid=int((folder/"server.pid").read_text())
begin=time.time()
command=["perf","record","--no-buildid-cache","--no-buildid","-e","cpu-clock:u","-F","99","-g","--call-graph","fp","-p",str(pid),"-o",str(root/"warmup-on.perf.data"),"--","sleep","12"]
try:
 with (root/"warmup-perf.log").open("w") as out:
  result=subprocess.run(command,stdout=out,stderr=subprocess.STDOUT,timeout=18)
  code=result.returncode
except subprocess.TimeoutExpired:code="timeout"
end=time.time()
(root/"warmup-profile.json").write_text(json.dumps(dict(command=command,exit_code=code,warmup_start_ts=started,start_ts=begin,end_ts=end,ended_before_measurement=end<started+28),indent=2)+"\n")

