import run as bench
import urllib.request, urllib.parse, os, resource, shutil, subprocess, json, time
from pathlib import Path
resource.setrlimit(resource.RLIMIT_NOFILE,(65536,resource.getrlimit(resource.RLIMIT_NOFILE)[1]))
import sys
folder=bench.ROOT/(sys.argv[1]+'-diagnostic')
folder.mkdir()
base=bench.DATA_ROOT/'baseline-storage'
storage=bench.DATA_ROOT/'seed/storage'
assert not storage.exists()
blocks=sum(f.stat().st_blocks*512 for f in base.rglob('*') if f.is_file())
assert shutil.disk_usage(bench.ROOT).free > blocks+5*(1024**3)
subprocess.run(['cp','-a','--reflink=auto','--sparse=always',str(base),str(storage)],check=True)
cfg=bench.config
def configure(folder,enabled):
 p=cfg(folder,enabled)
 p.write_text(p.read_text().replace('EnableTaskExecutionTrace: false','EnableTaskExecutionTrace: true\n    TaskExecutionTraceMaxQueries: 96\n    TaskExecutionTraceMaxEvents: 2048\n    TaskExecutionTraceMaxBytes: 1048576\n    TaskExecutionTraceRetentionMs: 600000'))
 return p
bench.config=configure
def read(n,query=None):
 params={'task_traces':'1'}
 if query is not None: params['query']=query
 with urllib.request.urlopen('http://127.0.0.1:'+str(bench.MON+n)+'/actors/kqp_channels?'+urllib.parse.urlencode(params),timeout=20) as resp:
  return json.load(resp)
try:
 process=bench.server(folder,storage,True)
 bench.cli(folder,'load',['run','-w','10','--warmup','2s','--time','8s','--threads','2','--max-sessions','16','--no-delays','--no-tui','--format','Json','--extended-stats'])
 for node in range(4):
  index=read(node);bench.save(folder/f'index-{node}.json',index)
  entries=index.get('queries',[])
  for i,entry in enumerate(entries):
   key=entry.get('query')
   if key: bench.save(folder/f'trace-{node}-{i}.json',read(node,key))
 print('collected',len(list(folder.glob('trace-*.json'))),flush=True)
finally:
 for proc in reversed(bench.ACTIVE): bench.stop(proc)
 if storage.exists(): shutil.rmtree(storage)
