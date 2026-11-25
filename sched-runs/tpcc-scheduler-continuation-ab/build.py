from pathlib import Path
import json,subprocess,time,os
root=Path("/home/mfilitov/tpcc-scheduler-continuation-ab")
root.mkdir(exist_ok=True)
def status(phase,**kw):
 (root/"build-status.json").write_text(json.dumps(dict(phase=phase,**kw),indent=2)+"\n")
status("building-and-testing",started_utc=time.strftime("%Y-%m-%dT%H:%M:%SZ",time.gmtime()))
command="./ya make --build relwithdebinfo -tA ydb/library/yql/dq/task_scheduler/ut ydb/tests/tools/kqprun 2>&1 | tee /home/mfilitov/tpcc-scheduler-continuation-ab/build-full.log | tail -n 80"
with (root/"build-tail.log").open("w") as log:
 result=subprocess.run(["bash","-o","pipefail","-c",command],cwd="/home/mfilitov/ydb-pr56258-local-consumer",stdout=log,stderr=subprocess.STDOUT)
if result.returncode==0:
 source=Path("/home/mfilitov/ydb-pr56258-local-consumer/ydb/tests/tools/kqprun/kqprun").resolve(strict=True)
 os.link(source,root/"kqprun")
status("ready" if result.returncode==0 else "failed",exit_code=result.returncode)

