from pathlib import Path
import importlib.util,json,time,resource
root=Path(__file__).resolve().parent
for _ in range(720):
    p=root/"progress.json"
    if p.exists():
        phase=json.loads(p.read_text())["phase"]
        if phase in ("completed","failed"):break
    time.sleep(2)
assert phase=="completed",phase
resource.setrlimit(resource.RLIMIT_NOFILE,(65536,resource.getrlimit(resource.RLIMIT_NOFILE)[1]))
spec=importlib.util.spec_from_file_location("suite",root/"run_suite.py")
b=importlib.util.module_from_spec(spec);spec.loader.exec_module(b)
runs=json.loads((root/"runs.json").read_text())
assert len(runs)==44
manifest=json.loads((root/"manifest.json").read_text())
manifest["extra_repetitions"]={"q05":1}
(root/"manifest.json").write_text(json.dumps(manifest,indent=2)+"\n")
for enabled in (True,False):
    b.status("checking_q5",mode="on" if enabled else "off",runs_completed=len(runs),total_runs=46)
    runs.append(b.execute(5,2,enabled))
    b.write_reports(runs)
b.status("completed",runs_completed=len(runs),total_runs=46,validated=sum(r["validation"]["ok"] for r in runs))
