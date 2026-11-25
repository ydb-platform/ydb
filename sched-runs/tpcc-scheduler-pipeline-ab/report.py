from pathlib import Path
from statistics import mean
from hashlib import sha256
import sys,json
r=Path(sys.argv[1])
load=lambda p:json.loads((r/p).read_text())
mf=load('manifest.json')
runs=load('runs.json')
f=lambda n:f'{n:.2f}'.replace('.',',')
assert mf['pipeline_delta_sha256']==sha256((r/'pipeline.patch').read_bytes()).hexdigest()
assert mf['run_order']==[x['label'].split('-')[0] for x in runs]
def cpu(x):
 return 1000*x['server_average_cores']/x['tx_per_second']
out=['# TPC-C, ожидание входа и число воркеров','',
     'Один kqprun, 4 ноды, 2 пользовательских потока на ноду, 10 складов, 64 сессии. Перед каждым прогоном восстановлен исходный снимок базы. Прогрев 30 с, измерение 120 с, трассировка выключена. Успешные транзакции всех пяти типов входят в пропускную способность. CPU относится к процессу сервера.','',
     '| Прогон | Транзакций/с | CPU, мс/успех | Новых заказов/мин | P95 NewOrder, мс | Неуспешных транзакций | Steal, % |',
     '|---|---:|---:|---:|---:|---:|---:|']
names={'off':'OFF','all':'ON, ожидание первого входа у Arg','pipe':'ON, ожидание входа у цепочек, 8 воркеров','two':'ON, ожидание входа у цепочек, 2 воркера'}
def counter(x,k):
 return x['counters'].get('TaskSchedulerWorkers.'+k,0)
for x in runs:
 m=x['label'].split('-')[0]
 c=load(x['label']+'/server-command.json')
 conf=(r/x['label']/'config.conf').read_text()
 assert c['enabled']==(m!='off') and c['binary_inode']==mf['server_inode'] and x['consistency_check_ok']
 assert 'EnableTaskScheduling: '+str(m!='off').lower() in conf
 assert 'TaskSchedulerShards: '+str(1 if m=='two' else 4) in conf
 assert all((c['environment'][k]=='1')==(m!='off') for k in ['YDB_DQ_TASK_FINISH_ONCE','YDB_DQ_TASK_WAIT_FIRST_INPUT','YDB_DQ_TASK_REFRESH_INLINE'])
 assert (c['environment']['YDB_DQ_TASK_WAIT_FIRST_PIPELINE']=='1')==(m in ('two','pipe'))
 assert c['environment']['YDB_DQ_TASK_TAIL_WAKE']=='0'
 out.append(f"| {x['label']} | {f(x['tx_per_second'])} | {f(cpu(x))} | {f(x['new_orders_per_minute'])} | {x['latencies']['NewOrder']['95']} | {x['failed']} | {f(x['host']['steal_pct'])} |")
out+=['','| Режим | Транзакций/с | CPU, мс/успех | Новых заказов/мин | Run | Пустых Run | Пустых активаций воркеров |','|---|---:|---:|---:|---:|---:|---:|']
for m,n in names.items():
 g=[x for x in runs if x['label'].startswith(m+'-')]
 assert len(g)==2
 w=round(mean(counter(x,'WorkerRuns') for x in g))
 z=round(mean(x['counters'].get('TaskSchedulerPoC.NoObservedProgressRuns',0) for x in g))
 a=round(mean(counter(x,'EmptyActivations') for x in g))
 out.append(f"| {n} | {f(mean(x['tx_per_second'] for x in g))} | {f(mean(map(cpu,g)))} | {f(mean(x['new_orders_per_minute'] for x in g))} | {w or '—'} | {z or '—'} | {a or '—'} |")
out+=['','Счётчики Run и активаций включают прогрев. Для OFF эти счётчики недоступны. CPU и транзакции/с приведены за измерительный интервал. Каждый прогон прошёл workload tpcc check.']
(r/'report.md').write_text('\n'.join(out)+'\n')
