from pathlib import Path
import shutil,subprocess,json,sys
src=Path(__file__).resolve().parents[1]
base=Path(sys.argv[1] if len(sys.argv) > 1 else '/tmp/sync-guid-recovery-mutations');base.mkdir(parents=True,exist_ok=True)
variants=[
('early-completion','PSrc/Storage.p','goto EWaitLocal;','ReplyAndDie(SyncDone);','Proofs','tcProofFullSyncRetry'),
('restart','PSrc/Storage.p','if (result.Status == RESTART || result.Status == NODATA) {','if (result.Status == RESTART) { current = result.SyncState; NextRequest(); } else if (result.Status == NODATA) {','Recovery','tcStaleMetadata'),
('minority-final','PSrc/GuidRecovery.p','if (finalGuid != 0 && finalGuid != knownGuid) {','if (false) {','Proofs','tcProofGuidConflict'),
('history','PSrc/Quorum.p','return !HasGroupQuorum(topology, missing);','sources -= (owner); return HasGroupQuorum(topology, sources);','Recovery','tcDegradedFreshGroup'),
('catchup','PSrc/Quorum.p','peers += (owner);','// omit local metadata','Recovery','tcDegradedRestart'),
('cursor','PSrc/Storage.p','if (fullRecovery && status == Error) { current = old; }','// no rollback','Proofs','tcProofFullSyncRetry'),
('commit','PSrc/GuidRecovery.p','goto CommittingHistory;','Decide();','Proofs','tcProofClosedSurvey'),
('reject-fresh','PSrc/GuidRecovery.p','outcome = ReachAVerdict(topology, local, votes, history[owner]);','outcome = (Decision = Inconsistency, Guid = 0, Step = STATE__Terminated);','Recovery','tcDegradedFreshGroup'),
]
results=[]
for name,file,old,new,project,test in variants:
 dst=base/name
 if dst.exists(): raise SystemExit("Use a fresh output directory: " + str(dst))
 for folder in ['PSrc','PSpec','PTst','Proofs']:shutil.copytree(src/folder,dst/folder,dirs_exist_ok=True)
 for f in ['Recovery.pproj','Proofs.pproj']:shutil.copy(src/f,dst/f)
 p=dst/file;s=p.read_text();assert old in s;s=s.replace(old,new);p.write_text(s)
 with (dst/'compile.log').open('w') as log:r=subprocess.run(['p','compile','--pproj',project+'.pproj','--mode','pchecker','--outdir','generated'],cwd=dst,stdout=log,stderr=subprocess.STDOUT)
 if r.returncode: raise SystemExit(name + ': compilation failed; inspect compile.log')
 assembly=dst/'generated/PChecker/net8.0'/('SyncGuidRecovery'+('Proofs' if project=='Proofs' else '')+'.dll')
 with (dst/'check.log').open('w') as log:r=subprocess.run(['p','check',str(assembly),'--testcase',test,'--schedules','50','--seed','1','--max-steps','10000','--fail-on-maxsteps','--outdir',str(dst/'results')],cwd=dst,stdout=log,stderr=subprocess.STDOUT)
 results.append(dict(mutation=name,testcase=test,exit=r.returncode));print(results[-1],flush=True)
(base/'results.json').write_text(json.dumps(results,indent=2)+'\n')

if len(results) != len(variants) or any(result["exit"] == 0 for result in results):
 raise SystemExit("A mutation was not detected; inspect results.json")
