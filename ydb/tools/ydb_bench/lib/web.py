"""Local web UI and durable application service for benchmark runs.

The HTTP handlers in this module deliberately only translate requests.  A
``RunService`` owns workers, manifests and the replayable event log, so closing
a browser connection cannot stop a benchmark.
"""

import csv
import hashlib
import json
import math
import mimetypes
import socket
import statistics
import tempfile
import threading
import uuid
import webbrowser
from collections import deque
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, quote, unquote, urlparse

from ydb.tools.ydb_bench.benchmarks import BENCHMARKS
from ydb.tools.ydb_bench.lib.common import BenchmarkError, BenchmarkInterrupted, atomic_write_json, atomic_write_text
from ydb.tools.ydb_bench.lib.config import BACKGROUND_LOAD_MODES, build_run_plan, load_config
from ydb.tools.ydb_bench.lib.results import ResultStore, load_manifest
from ydb.tools.ydb_bench.lib.actors_core import run_benchmark
from ydb.tools.ydb_bench.lib.common import extract_executable
from ydb.tools.ydb_bench.lib.import_results import MAX_TOTAL_SIZE, export_archive, import_archive
from ydb.tools.ydb_bench.lib.local_ydb import run_local_ydb
from ydb.tools.ydb_bench.lib.topology import AFFINITY_MODES, discover_topology, plan_affinity, topology_record

_CSP = "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self'; font-src 'self'; connect-src 'self'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'"
_STREAM_CHUNK_SIZE = 1024 * 1024
_HTML = (
    "<!doctype html><html lang=en><meta charset=utf-8>"
    '<meta name=viewport content="width=device-width,initial-scale=1">'
    "<title>YDB benchmark</title><link rel=stylesheet href=/app.css>"
    "<body><div id=app>Loading YDB benchmark…</div><script src=/app.js></script></body></html>"
)
_CSS = (
    "\n"
    ':root{color-scheme:light dark;font:14px/1.45 system-ui,sans-serif;--line:#8992a2;--panel:#f4f7fb;--text:#172033;--muted:'
    '#667085;--accent:#1b62b9;--good:#087443;--bad:#b42318;--warn:#a15c00;--topology-accent:#6b5bd2}\n'
    '*{box-sizing:border-box}body{margin:0;color:var(--text);background:#fff}a{color:var(--accent);cursor:pointer;text-decora'
    'tion:none}a:hover{text-decoration:underline}button,input,select,textarea{font:inherit}button{cursor:pointer;border:1px s'
    'olid #667085;border-radius:5px;background:#fff;color:var(--text);padding:.38rem .65rem}button.primary{background:var(--a'
    'ccent);color:#fff;border-color:var(--accent)}button.danger{color:var(--bad);border-color:var(--bad)}button:disabled{opac'
    'ity:.5;cursor:not-allowed}.shell{display:grid;grid-template-columns:14rem minmax(0,1fr);min-height:100vh}.sidebar{paddin'
    'g:1.4rem 1rem;background:#172033;color:#fff}.brand{font-weight:700;font-size:1.05rem;margin:0 0 1.7rem}.sidebar a{displa'
    'y:block;color:#d6e2f7;padding:.55rem .65rem;border-radius:5px;margin:.15rem 0}.sidebar a.active,.sidebar a:hover{color:#'
    'fff;background:#315882;text-decoration:none}.content{min-width:0}.topbar{min-height:3.7rem;border-bottom:1px solid #d0d5'
    'dd;padding:.8rem 1.6rem;display:flex;justify-content:space-between;gap:1rem;align-items:center}.topbar .active-run{font-'
    'size:.9rem;color:var(--muted)}main{max-width:1160px;padding:1.5rem 1.6rem 3rem}.breadcrumbs{color:var(--muted);font-size'
    ':.9rem;margin:0 0 .6rem}.page-title{margin:0 0 1rem;font-size:1.5rem}.toolbar{display:flex;gap:.55rem;align-items:center'
    ';flex-wrap:wrap;margin:.8rem 0}.filters,.grid{display:grid;gap:.7rem}.filters{grid-template-columns:repeat(auto-fit,minm'
    'ax(10rem,1fr));background:var(--panel);padding:.8rem;border:1px solid #d0d5dd;border-radius:6px}.field{display:grid;gap:'
    '.25rem}.field label{font-size:.85rem;color:var(--muted)}input,select,textarea{border:1px solid #98a2b3;border-radius:4px'
    ';padding:.42rem;background:#fff;color:var(--text)}textarea.yaml{width:100%;min-height:33rem;tab-size:2;font-family:ui-mo'
    'nospace,SFMono-Regular,Menlo,monospace;line-height:1.35}.notice{padding:.7rem .85rem;border-radius:5px;background:#eef4f'
    'f;border:1px solid #b2ccff;margin:.8rem 0}.notice.error{background:#fff0f0;border-color:#fecdca;color:var(--bad)}.notice'
    '.good{background:#ecfdf3;border-color:#abefc6;color:var(--good)}table{border-collapse:collapse;width:100%;margin:.7rem 0'
    '}th,td{border-bottom:1px solid #d0d5dd;padding:.52rem;text-align:left;vertical-align:top}th{font-size:.8rem;color:var(--'
    'muted);font-weight:600}.status{font-weight:600}.status.completed,.status.passed{color:var(--good)}.status.failed,.status'
    '.cancelled{color:var(--bad)}.status.running,.status.pending{color:var(--warn)}.muted{color:var(--muted)}.split{display:g'
    'rid;grid-template-columns:minmax(13rem,22rem) minmax(0,1fr);gap:1rem}.card{border:1px solid #d0d5dd;border-radius:7px;pa'
    'dding:1rem;margin:.8rem 0}.profile-list button{display:block;width:100%;text-align:left;border:0;border-radius:0;margin:'
    '0;padding:.55rem;background:transparent}.profile-list button.selected{background:#dbeafe;color:#0b4a8b}.form-grid{displa'
    'y:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:.75rem}.checkboxes{display:flex;flex-wrap:wrap;gap:.4rem .8rem;'
    'padding:.5rem;border:1px solid #d0d5dd;border-radius:4px;max-height:13rem;overflow:auto}.checkboxes label{font-size:.9re'
    'm}.run-tree details{padding:.45rem 0;border-bottom:1px solid #e4e7ec}.run-tree summary{cursor:pointer}.log{white-space:p'
    're-wrap;overflow:auto;max-height:20rem;background:#101828;color:#e4e7ec;border-radius:5px;padding:.7rem;font-family:ui-m'
    'onospace,SFMono-Regular,Menlo,monospace}.metric{font-size:1.1rem;font-weight:650}.actions{display:flex;gap:.35rem;flex-w'
    'rap:wrap}.tabs{display:flex;gap:.2rem;border-bottom:1px solid #d0d5dd;margin-bottom:1rem}.tabs a{padding:.55rem .85rem}.'
    'tabs a.active{color:var(--text);border-bottom:3px solid var(--accent);font-weight:650}.empty{padding:2rem;text-align:cen'
    'ter;color:var(--muted);border:1px dashed #98a2b3;border-radius:7px}.topology-summary{display:grid;grid-template-columns:'
    'minmax(12rem,18rem) minmax(0,1fr);gap:1rem;align-items:center}.cpu-ranges{font-family:ui-monospace,SFMono-Regular,Menlo,'
    'monospace;overflow-wrap:anywhere}.topology-map{display:grid;grid-template-columns:repeat(auto-fit,minmax(18rem,1fr));gap'
    ':.8rem}.numa-block{border:1px solid #c9c1ff;border-left:4px solid var(--topology-accent);border-radius:6px;background:#f'
    '8f7ff;padding:.75rem}.numa-header{display:flex;align-items:baseline;justify-content:space-between;gap:.5rem;margin-botto'
    'm:.5rem}.topology-tree,.topology-tree ul,.affinity-tree,.affinity-tree ul{list-style:none;margin:.45rem 0 0;padding-l'
    'eft:1.1rem}.topology-tree>li,.affinity-tree>li{padding-left:0}.topology-tree li,.affinity-tree li{position:relative;ma'
    'rgin:.35rem 0}.topology-tree li:before,.affinity-tree li:before{content:"";position:absolute;left:-.75rem;top:.72rem;wi'
    'dth:.55rem;border-top:1px solid #b8b0ec}.topology-node{border:1px solid #d9d6f5;border-radius:5px;background:#fff;paddin'
    'g:.4rem .55rem}.topology-node-header{display:flex;align-items:baseline;justify-content:space-between;gap:.6rem}.core-list'
    '{display:grid;grid-template-columns:repeat(auto-fit,minmax(10rem,1fr));gap:.4rem}.core-item{border:1px solid #e4e7ec;bo'
    'rder-radius:4px;padding:.35rem .45rem;background:#fff}.core-item .cpu-ranges,.core-item small{display:block}.core-item sm'
    'all{color:var(--muted)}.affinity-tree{pa'
    'dding-left:.25rem}.affinity-node{display:flex;align-items:center;gap:.55rem;flex-wrap:wrap}.affinity-unavailable{color:var'
    '(--muted)}.availability-badge{font-size:.75rem;font-weight:650;color:var(--bad);b'
    'ackground:#fff0f0;border:1px solid #fecdca;border-radius:999px;padding:.1rem .4rem}.affinity-reason{font-size:.85rem;co'
    'lor:var(--bad)}.cha'
    'rt-controls{display:grid;grid-template-columns:repeat(auto-fit,minmax(12rem,1fr));gap:.75rem}.series-picker{max-height:1'
    '5rem;overflow:auto;border:1px solid #d0d5dd;border-radius:5px;padding:.55rem}.series-picker label{display:block;margin:.'
    '25rem 0}.series-cpus{display:block;margin-left:1.35rem;color:var(--muted);font:12px ui-monospace,SFMono-Regular,Menlo,mo'
    'nospace}.chart-panel{border-top:1px solid #e4e7ec;padding-top:1rem;margin-top:1rem}.chart-surface{position:relative}.cha'
    'rt-panel svg{display:block;width:100%;height:auto;background:#fff}.chart-tooltip{position:absolute;z-index:2;pointer-eve'
    'nts:none;min-width:15rem;max-width:28rem;padding:.55rem .65rem;border-radius:5px;background:#101828;color:#fff;box-shado'
    'w:0 4px 14px #10182855;font-size:.82rem}.chart-tooltip[hidden]{display:none}.chart-tooltip strong{display:block;margin-b'
    'ottom:.3rem}.tooltip-row{display:grid;grid-template-columns:.65rem minmax(8rem,1fr) auto;gap:.4rem;align-items:center;ma'
    'rgin:.16rem 0}.tooltip-dot{width:.55rem;height:.55rem;border-radius:50%}.tooltip-value{font-family:ui-monospace,SFMono-R'
    'egular,Menlo,monospace;font-variant-numeric:tabular-nums}.chart-cursor{stroke:#475467;stroke-width:1;stroke-dasharray:4 '
    '3;pointer-events:none}.chart-legend{display:flex;flex-wrap:wrap;gap:.4rem 1rem}.legend-swatch{display:inline-block;width'
    ':.9rem;height:.2rem;vertical-align:middle;margin-right:.3rem}.chart-grid{stroke:#e4e7ec;stroke-width:1}.chart-axis{strok'
    'e:#667085;stroke-width:1}.chart-label{fill:#475467;font:12px system-ui,sans-serif}.chart-line{fill:none;stroke-width:2.5'
    ';stroke-linejoin:round;stroke-linecap:round}.chart-point{stroke:#fff;stroke-width:1.5}.coverage{font-size:.85rem;color:v'
    'ar(--muted)}.chart-color-0{color:#1b62b9}.chart-color-1{color:#c2410c}.chart-color-2{color:#087443}.chart-color-3{color:'
    '#7c3aed}.chart-color-4{color:#be185d}.chart-color-5{color:#0e7490}.chart-color-6{color:#854d0e}.chart-color-7{color:#94a'
    '3b8}.chart-color-8{color:#ef4444}.chart-color-9{color:#818cf8}.chart-color-10{color:#22c55e}.chart-color-11{color:#d946e'
    'f}.chart-bg-0{background:#1b62b9}.chart-bg-1{background:#c2410c}.chart-bg-2{background:#087443}.chart-bg-3{background:#7'
    'c3aed}.chart-bg-4{background:#be185d}.chart-bg-5{background:#0e7490}.chart-bg-6{background:#854d0e}.chart-bg-7{backgroun'
    'd:#94a3b8}.chart-bg-8{background:#ef4444}.chart-bg-9{background:#818cf8}.chart-bg-10{background:#22c55e}.chart-bg-11{bac'
    'kground:#d946ef}@media(max-width:760px){.shell{display:block}.sidebar{padding:.7rem;display:flex;gap:.3rem;overflow:auto'
    '}.brand{display:none}.sidebar a{white-space:nowrap}.topbar,main{padding-left:1rem;padding-right:1rem}.split,.topology-su'
    'mmary{grid-template-columns:1fr}}\n'
    '.grid,.grid>*{min-width:0}.run-tree{overflow:hidden}.run-tree details{min-width:0}.affinity-details>td>details{overflow:'
    'hidden}.affinity-details table{display:block;max-width:100%;overflow-x:auto}.modal-backdrop{position:fixed;inset:0;z-ind'
    'ex:20;display:grid;place-items:center;padding:1.5rem;background:#10182899}.modal{display:flex;flex-direction:column;widt'
    'h:min(68rem,100%);max-height:calc(100vh - 3rem);overflow:hidden;border-radius:8px;background:#fff;box-shadow:0 20px 40px'
    ' #10182855}.modal-header,.modal-footer{display:flex;align-items:center;justify-content:space-between;gap:1rem;padding:1r'
    'em 1.2rem}.modal-header{border-bottom:1px solid #d0d5dd}.modal-header h2{margin:0}.modal-body{overflow:auto;padding:0 1.'
    '2rem 1rem}.modal-footer{justify-content:flex-end;border-top:1px solid #d0d5dd}.line-filter{min-width:0;border:1px solid '
    '#d0d5dd;border-radius:5px;padding:.5rem}.line-filter label{display:block;margin:.25rem 0}.chart-settings-summary{display'
    ':flex;align-items:center;gap:.7rem;flex-wrap:wrap;margin:.8rem 0}.chart-board>.card{position:relative}.query-row{display'
    ':flex;align-items:center;gap:.4rem;flex-wrap:wrap;padding:.55rem;margin:.45rem 0;border:1px solid #d0d5dd;border-radius:'
    '6px;background:var(--panel)}.query-row select{max-width:15rem}.query-token{display:flex;align-items:center;gap:.3rem;pad'
    'ding:.2rem .35rem;border-radius:4px;background:#fff;border:1px solid #d0d5dd}.query-token b{color:#6941c6;font-weight:60'
    '0}.query-actions{margin-left:auto}.run-tabs{display:flex;gap:.35rem;overflow-x:auto;margin:1rem 0;border-bottom:1px solid '
    '#d0d5dd}.run-tab{display:block;padding:.6rem .8rem;border-radius:6px 6px 0 0;color:var(--muted);text-decoration:none;whit'
    'e-space:nowrap}.run-tab:hover{background:var(--panel)}.run-tab.active{color:var(--text);font-weight:650;background:#fff;b'
    'order:1px solid #d0d5dd;border-bottom-color:#fff;margin-bottom:-1px}.profile-overview td:first-child{font-weight:650}.run'
    '-section-title{display:flex;align-items:baseline;justify-content:space-between;gap:1rem;flex-wrap:wrap}.downloads{display'
    ':inline-block}.downloads summary{cursor:pointer}.downloads .actions{margin-top:.5rem}\n'
    """
.local-live{display:grid;grid-template-columns:minmax(16rem,1.4fr) repeat(3,minmax(9rem,1fr));gap:.8rem;align-items:stretch}
.local-live>div,.local-kpis>div{padding:.8rem;border:1px solid #d0d5dd;border-radius:7px;background:var(--panel)}
.local-live strong,.local-kpis strong{display:block;font-size:1.18rem;margin-top:.2rem}
.local-phase{font-size:1.25rem;font-weight:700}.local-phase-progress{width:100%;margin-top:.65rem}
.local-kpis{display:grid;grid-template-columns:repeat(auto-fit,minmax(10rem,1fr));gap:.7rem;margin:.8rem 0}
.local-kpis .primary-result{border-color:#84adff;background:#eff6ff}
.local-stages{display:flex;gap:.55rem;overflow-x:auto;padding:.25rem 0 .7rem}
.local-stage{min-width:13rem;padding:.65rem;border:1px solid #d0d5dd;border-radius:7px;background:#fff}
.local-stage.current{border-color:#84adff;background:#eff6ff}.local-stage .stage-arrow{color:var(--muted);margin-top:.35rem}
.local-charts{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:.9rem}
.local-charts .chart-panel{margin:0;padding:.8rem;border:1px solid #d0d5dd;border-radius:7px}
.local-charts .chart-panel h3{margin-top:0}
.local-attempts-scroll{max-width:100%;overflow-x:auto}
.local-attempts{width:max-content;min-width:100%}.local-attempts td,.local-attempts th{white-space:nowrap}
.local-current-command{margin:.8rem 0;padding:.8rem;border:1px solid #d0d5dd;border-radius:7px;background:#101828;color:#fff}
.local-current-command .muted{color:#d0d5dd}
.local-command-code{margin:.45rem 0 0;white-space:pre-wrap;overflow-wrap:anywhere;font:12px ui-monospace,SFMono-Regular,Menlo,monospace}
.local-command-cell{width:22rem;min-width:22rem;max-width:22rem;white-space:normal!important}
.local-command-history{width:22rem}.local-command-history[open]{max-height:24rem;overflow:auto}
.local-command-history summary,.local-profile-config summary{cursor:pointer}
.local-command-entry{margin:.55rem 0;padding:.55rem;border:1px solid #e4e7ec;border-radius:5px;background:var(--panel)}
.local-profile-config{margin:.8rem 0;padding:.7rem .8rem;border:1px solid #d0d5dd;border-radius:7px;background:#fff}
.local-profile-config pre{max-height:24rem;overflow:auto;white-space:pre;margin:.7rem 0 0}
.attempt-pass{color:var(--good);font-weight:650}.attempt-fail{color:var(--bad);font-weight:650}
@media(max-width:900px){.local-live{grid-template-columns:1fr 1fr}.local-charts{grid-template-columns:1fr}}
"""
    '.status.queued{color:var(--warn)}\n'
)
_JS = (
    "\n"
    '/* Offline UI: every request goes to the loopback ydb_bench service. */\n'
    "const app=document.querySelector('#app');\n"
    'const esc=value=>String(value??\'\').replace(/[&<>"\']/g,char=>({\'&\':\'&amp;\',\'<\':\'&lt;\',\'>\':\'&gt;\',\'"\':\'&quot;\',"\'":\'&#39;\''
    '}[char]));\n'
    'const enc=value=>encodeURIComponent(value);\n'
    "let editor={yaml:sessionStorage.getItem('ydb-bench-draft')||'ping-bench:\\n  baseline:\\n    threads: [1]\\n    duration: 3"
    "\\n    repetitions: 1\\n    affinity: [none]\\n',perf:false,continueOnError:false,model:null,error:null,selected:null};\n"
    "let activeRun=sessionStorage.getItem('ydb-bench-active-run')||'';\n"
    'let refreshTimer=null;\n'
    "async function api(path,options={}){const response=await fetch(path,options);const type=response.headers.get('content-ty"
    "pe')||'';const body=type.includes('application/json')?await response.json():await response.text();if(!response.ok)throw "
    'Error(body.error||body||response.statusText);return body}\n'
    "function jsonOptions(value){return {method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(value)"
    '}}\n'
    "function route(){return decodeURIComponent(location.hash.slice(1)||'runs')}\n"
    'function setRoute(value){location.hash=value}\n'
    'function displayError(error){return \'<div class="notice error">\'+esc(error.message||error)+\'</div>\'}\n'
    "function secondsLabel(seconds){return Number.isFinite(Number(seconds))?Math.max(0,Number(seconds)).toFixed(1)+' s':'—'}\n"
    "function humanTime(value){if(!value)return 'Not started';const date=new Date(value);return Number.isNaN(date.getTime())"
    "?'—':new Intl.DateTimeFormat(undefined,{dateStyle:'medium',timeStyle:'short'}).format(date)}\n"
    "function elapsedLabel(seconds){seconds=Math.max(0,Math.round(Number(seconds)));if(!Number.isFinite(seconds))return '—'"
    ";const days=Math.floor(seconds/86400),hours=Math.floor(seconds%86400/3600),minutes=Math.floor(seconds%3600/60),remain"
    "ing=seconds%60;if(days)return days+'d '+hours+'h';if(hours)return hours+'h '+minutes+'m';if(minutes)return minutes+'m '+"
    "remaining+'s';return remaining+'s'}\n"
    "function duration(record){if(!record.started_at)return 'Not started';const end=record.finished_at?Date.parse(record.fin"
    "ished_at):record.status==='running'?Date.now():NaN;return elapsedLabel((end-Date.parse(record.started_at))/1000)}\n"
    "function cpuRanges(cpus){if(!Array.isArray(cpus)||!cpus.length)return '—';const values=[...new Set(cpus.map(Number).filt"
    'er(Number.isSafeInteger))].sort((left,right)=>left-right);const ranges=[];for(let index=0;index<values.length;){let end='
    'index;while(end+1<values.length&&values[end+1]===values[end]+1)end++;ranges.push(values[index]===values[end]?String(valu'
    "es[index]):values[index]+'-'+values[end]);index=end+1}return ranges.join(', ')}\n"
    'function stepDuration(step){if(step.duration_seconds!==null&&step.duration_seconds!==undefined&&Number.isFinite(Number(s'
    "tep.duration_seconds)))return secondsLabel(step.duration_seconds);if(step.state==='running'&&step.started_at)return seco"
    "ndsLabel((Date.now()-Date.parse(step.started_at))/1000);return '—'}\n"
    'function status(value){return \'<span class="status \'+esc(value||\'unknown\')+\'">\'+esc(value||\'unknown\')+\'</span>\'}\n'
    "function shell(current,body,breadcrumb=''){const navigation=[['runs','Runs'],['new','New run'],['topology','System topol"
    "ogy'],['comparisons','Comparisons']];return '<div class=shell><aside class=sidebar><div class=brand>YDB benchmark</div>'"
    '+navigation.map(([id,label])=>\'<a class="\'+(current===id?\'active\':\'\')+\'" href="#\'+id+\'">\'+label+\'</a>\').join(\'\')+\'</asid'
    "e><div class=content><header class=topbar><strong>'+esc(current==='new'?'New run':current==='topology'?'System topology'"
    ':current===\'comparisons\'?\'Comparisons\':\'Runs\')+\'</strong><span class=active-run>\'+ (activeRun?\'<a href="#run/\'+enc(activ'
    'eRun)+\'">Active run: \'+esc(activeRun)+\'</a>\':\'No active run\')+\'</span></header><main>\'+breadcrumb+body+\'</main></div></d'
    "iv>'}\n"
    "function breadcrumbs(items){return items.length?'<div class=breadcrumbs>'+items.map((item,index)=>index===items.length-1"
    '?esc(item.label):\'<a href="#\'+esc(item.route)+\'">\'+esc(item.label)+\'</a>\').join(\' / \')+\'</div>\':\'\'}\n'
    "function saveDraft(){sessionStorage.setItem('ydb-bench-draft',editor.yaml)}\n"
    "function compactIntegerRanges(values){const numbers=values.map(Number);if(!numbers.length)return '';const parts=[];for(l"
    'et index=0;index<numbers.length;){let end=index;while(end+1<numbers.length&&numbers[end+1]===numbers[end]+1)end++;parts.'
    "push(index===end?String(numbers[index]):numbers[index]+'-'+numbers[end]);index=end+1}return parts.join(', ')}\n"
    "function yamlArray(values){return '['+values.map(value=>String(value)).join(', ')+']'}\n"
    "const localYdbOperations={kv:['upsert','select','read-rows','mixed'],stock:['user-hist','rand-user-hist','add-rand-"
    "order','put-rand-order','put-same-order']};\n"
    "const localYdbGeometryKeys={static_nodes:'static-nodes',dynamic_nodes:'dynamic-nodes',max_dynamic_nodes:'max-dynamic-"
    "nodes',disk_size_gb:'disk-size-gb',storage_groups:'storage-groups'};\n"
    "const localYdbSearchKeys={resolution_percent:'resolution-percent'};\n"
    "const localYdbObjectiveKeys={target_role:'target-role',plateau_gain_percent:'plateau-gain-percent',plateau_points:'p"
    "lateau-points',cpu_saturation_percent:'cpu-saturation-percent'};\n"
    "const localYdbSloKeys={max_ms:'max-ms',max_errors:'max-errors',min_achieved_rate_ratio:'min-achieved-rate-ratio'};\n"
    "const localYdbAffinityKeys={ydb_cli:'ydb-cli',static_nodes:'static-nodes',dynamic_nodes:'dynamic-nodes'};\n"
    "function defaultLocalYdbWorkload(type){return type==='stock'?{type:'stock',operation:'put-rand-order',options:{'min-p"
    "artitions':40,products:100,quantity:1000,orders:100,'auto-partition':1,limit:10}}:{type:'kv',operation:'upsert',options"
    ":{'min-partitions':40,'max-partitions':1000,'partition-size-mb':2000,'init-upserts':0,'max-first-key':65536,'value-siz"
    "e':64,columns:2,'rows-per-query':1}}}\n"
    "function defaultLocalYdb(){return {workload:defaultLocalYdbWorkload('kv'),"
    "geometry:{preset:'single',static_nodes:1,dynamic_nodes:1,max_dynamic_nodes:1,disk_size_gb:64,storage_groups:1},client"
    ":{threads:64},load:{parameter:'rate',allow_errors:false,values:[1000]},measurement:{warmup:10,duration:30,rep"
    "etitions:3},affinity:{ydb_cli:{mode:'pack-numa-pack-chiplet-spread-core',cpus:'one-chiplet'},static_nodes:{mode:'none'"
    ",cpus:null},dynamic_nodes:{mode:'none',cpus:null}}}}\n"
    "function serializeLocalYdb(lines,profile){const config=profile.local_ydb,workload=config.workload;lines.push('    work"
    "load:','      type: '+workload.type,'      operation: '+workload.operation,'      options:');for(const [key,value] of "
    "Object.entries(workload.options))lines.push('        '+key+': '+value);lines.push('    geometry:','      preset: '+conf"
    "ig.geometry.preset);for(const [key,yamlKey] of Object.entries(localYdbGeometryKeys))lines.push('      '+yamlKey+': '+c"
    "onfig.geometry[key]);lines.push('    client:','      threads: '+config.client.threads,'    load:','      parameter: '"
    "+config.load.parameter,'      allow-errors: '+Boolean(config.load.allow_errors));if(config.load.values)lines.push('      values: '+yamlArray(config.load.values));else{lines."
    "push('      search:','        start: '+config.load.search.start,'        maximum: '+config.load.search.maximum);if("
    "config.load.objective.type==='latency-slo')lines.push('        multiplier: '+config.load.search.multiplier);for("
    "const [key,yamlKey] of Object.entries(localYdbSearchKeys))lines."
    "push('        '+yamlKey+': '+config.load.search[key]);lines.push('      objective:','        type: '+config.load.object"
    "ive.type);if(config.load.objective.type==='maximize-throughput')for(const [key,yamlKey] of Object.entries(localYdbOb"
    "jectiveKeys))lines.push('        '+yamlKey+': '+config.load.objective[key]);else{lines.push('        percentile: '+config.load.o"
    "bjective.percentile);for(const [key,yamlKey] of Object.entries(localYdbSloKeys))lines.push('        '+yamlKey+': '+con"
    "fig.load.objective[key])}}lines.push('    measurement:','      warmup: '+config.measurement.warmup,' "
    "     duration: '+config.measurement.duration,'      repetitions: '+config.measurement.repetitions,'    affinity:');f"
    "or(const [key,yamlKey] of Object.entries(localYdbAffinityKeys)){const role=config.affinity[key];lines.push('      '+yam"
    "lKey+':','        mode: '+role.mode);if(role.cpus!==null&&role.cpus!==undefined)lines.push('        cpus: '+role.cpus)}"
    "if(profile.timeout!==null&&profile.timeout!==undefined&&profile.timeout!=='')lines.push('    timeout: '+profile.timeo"
    "ut)}\n"
    'function serializeConfig(model){let lines=[];for(const benchmark of model.benchmarks||[]){const entries=(model.profiles|'
    "|[]).filter(profile=>profile.benchmark===benchmark.name);if(!entries.length)continue;lines.push(benchmark.name+':');for("
    "const profile of entries){lines.push('  '+profile.name+':');if(benchmark.profile_kind==='local-ydb'){serializeLocalYdb"
    "(lines,profile);continue}lines.push('    threads: '+yamlArray(profile.threads));for(c"
    "onst parameter of benchmark.parameters)lines.push('    '+parameter.name+': '+yamlArray(profile.parameters[parameter.name"
    "]||parameter.default));lines.push('    duration: '+profile.duration);lines.push('    repetitions: '+profile.repetitions)"
    ";lines.push('    affinity: '+yamlArray(profile.affinity));lines.push('    background-load: '+yamlArray(profile.background_"
    "load||['none']));if(profile.timeout!==null&&profile.timeout!==undefined&&profil"
    "e.timeout!=='')lines.push('    timeout: '+profile.timeout)}}return lines.join('\\n')+'\\n'}\n"
    "async function syncEditor(){try{const value=await api('/api/editor-config',jsonOptions({yaml:editor.yaml,perf:editor.per"
    'f}));editor.model=value;editor.error=null;if(!editor.selected&&value.profiles.length)editor.selected=value.profiles[0].k'
    'ey;return value}catch(error){editor.model=null;editor.error=error.message;return null}}\n'
    'function profileByKey(key){return (editor.model?.profiles||[]).find(profile=>profile.key===key)}\n'
    'function updateProfile(key,mutate){const profile=profileByKey(key);if(!profile)return;mutate(profile);editor.yaml=serial'
    'izeConfig(editor.model);saveDraft()}\n'
    'function planSummary(){const profiles=editor.model?.profiles||[];let count=0,seconds=0;for(const profile of profiles){co'
    'nst benchmark=editor.model.benchmarks.find(item=>item.name===profile.benchmark),cases=(benchmark?.parameters||[]).filter'
    '(item=>item.matrix).reduce((total,item)=>total*(profile.parameters[item.name]?.length||1),1),processes=profile.affinity.'
    "length*(profile.background_load||['none']).length*profile.threads.length*profile.repetitions*cases;count+=processes;seconds+=processes*profile.duration}return {cou"
    'nt,seconds}}\n'
    "function editorControls(){return '<div class=toolbar><button id=validate>Validate</button><button id=download-yaml>Downl"
    "oad YAML</button><button id=save-host>Save YAML on host</button><label><input id=perf type=checkbox '+(editor.perf?'chec"
    "ked':'')+'> perf</label><label><input id=continue type=checkbox '+(editor.continueOnError?'checked':'')+'> continue on e"
    "rror</label><button class=primary id=start-run>Start run</button></div><div id=editor-message></div>'}\n"
    'function parameterCases(benchmark,profile){let cases=[[]];for(const parameter of benchmark.parameters.filter(item=>item.'
    'matrix)){const values=profile.parameters[parameter.name]||parameter.default;cases=cases.flatMap(parts=>values.map(value='
    ">[...parts,parameter.name+'='+value]))}return cases}\n"
    'function bindEditorControls(){\n'
    "  const message=document.querySelector('#editor-message');\n"
    '  const showMessage=(text,kind=\'good\')=>{message.innerHTML=\'<div class="notice \'+kind+\'">\'+esc(text)+\'</div>\'};\n'
    "  if(editor.model&&document.querySelector('.profile-list')){\n"
    '    const queue=[];\n'
    '    for(const profile of editor.model.profiles){const benchmark=editor.model.benchmarks.find(item=>item.name===profile.b'
    "enchmark);for(const affinity of profile.affinity)for(const backgroundLoad of (profile.background_load||['none']))for(const threads of profile.threads)for(const parameters of parameterC"
    "ases(benchmark,profile))for(let repeat=1;repeat<=profile.repetitions;repeat++)queue.push(profile.benchmark+' / '+profile"
    ".name+' / '+affinity+' / '+backgroundLoad+' / '+threads+' threads'+(parameters.length?' / '+parameters.join(', '):'')+' / repeat '+repeat)}\n"
    "    message.insertAdjacentHTML('beforebegin','<details class=card><summary>Expected queue ('+queue.length+' processes)</"
    "summary><ol>'+queue.map(item=>'<li><code>'+esc(item)+'</code></li>').join('')+'</ol></details>');\n"
    '  }\n'
    "  document.querySelector('#perf').onchange=async event=>{editor.perf=event.target.checked;await syncEditor();renderNew()"
    '};\n'
    "  document.querySelector('#continue').onchange=event=>{editor.continueOnError=event.target.checked};\n"
    "  document.querySelector('#validate').onclick=async()=>{\n"
    "    try {const value=await api('/api/validate',jsonOptions({yaml:editor.yaml,perf:editor.perf}));showMessage(value.valid"
    "?'Valid configuration: '+value.steps+' planned processes.':value.error,value.valid?'good':'error')}\n"
    "    catch(error){showMessage(error.message,'error')}\n"
    '  };\n'
    "  document.querySelector('#download-yaml').onclick=()=>{\n"
    "    const blob=new Blob([editor.yaml],{type:'application/x-yaml'}),link=document.createElement('a');\n"
    "    link.href=URL.createObjectURL(blob);link.download='ydb-bench.yaml';link.click();URL.revokeObjectURL(link.href)\n"
    '  };\n'
    "  document.querySelector('#save-host').onclick=async()=>{\n"
    "    try {const value=await api('/api/drafts',jsonOptions({yaml:editor.yaml}));showMessage('Saved on host: '+value.path)}"
    '\n'
    "    catch(error){showMessage(error.message,'error')}\n"
    '  };\n'
    "  document.querySelector('#start-run').onclick=async()=>{\n"
    "    try {const value=await api('/api/runs',jsonOptions({yaml:editor.yaml,perf:editor.perf,continue_on_error:editor.conti"
    "nueOnError}));activeRun=value.id;sessionStorage.setItem('ydb-bench-active-run',activeRun);setRoute('run/'+enc(value.id))"
    '}\n'
    "    catch(error){showMessage(error.message,'error')}\n"
    '  }\n'
    '}\n'
    """
function localField(id,label,value,help='',attributes=''){
  return '<div class=field><label for="'+id+'">'+esc(label)+'</label><input id="'+id+'" value="'+esc(value)+'" '+attributes+'><small class=muted>'+esc(help)+'</small></div>'
}
function localSelect(id,label,value,choices,help=''){
  return '<div class=field><label for="'+id+'">'+esc(label)+'</label><select id="'+id+'">'+
    choices.map(choice=>'<option value="'+esc(choice)+'" '+(choice===value?'selected':'')+'>'+esc(choice)+'</option>').join('')+
    '</select><small class=muted>'+esc(help)+'</small></div>'
}
function localCheck(id,label,checked,help=''){
  return '<div class=field><label><input id="'+id+'" type=checkbox '+(checked?'checked':'')+'> '+esc(label)+'</label><small class=muted>'+esc(help)+'</small></div>'
}
function localYdbProfileEditor(profile){
  const config=profile.local_ydb,workload=config.workload,geometry=config.geometry,load=config.load,measurement=config.measurement;
  const loadMode=load.values?'points':load.objective.type;
  const options=Object.entries(workload.options).map(([key,value])=>localField('local-option-'+key,key,value)).join('');
  const geometryFields=Object.entries(localYdbGeometryKeys)
    .map(([key,label])=>localField('local-geometry-'+key,label,geometry[key],'','type=number min=1')).join('');
  const loadCommon=
    localSelect('local-load-mode','Objective',loadMode,['points','maximize-throughput','latency-slo'])+
    localSelect('local-load-parameter','Parameter',load.parameter,['rate','threads'])+
    localCheck(
      'local-load-allow-errors','Allow failed workload requests',Boolean(load.allow_errors),
      'Failed requests remain visible in results but do not limit load search.'
    );
  const searchFields=loadMode==='points'?'':
    localField('local-load-start','Start',load.search.start,'','type=number min=1')+
    localField('local-load-maximum','Maximum',load.search.maximum,'','type=number min=1')+
    (loadMode==='latency-slo'?
      localField(
        'local-load-multiplier','Growth multiplier',load.search.multiplier,
        'Used to find the first failing latency point.','type=number min=1 step=any'
      ):'')+
    localField(
      'local-load-search-resolution-percent',
      loadMode==='maximize-throughput'?'Ternary resolution (%)':'Boundary resolution (%)',
      load.search.resolution_percent,'','type=number min=0 max=100 step=any'
    );
  const loadFields=loadMode==='points'?
    localField('local-load-values','Values',(load.values||[]).join(', '),'Comma-separated values and ranges'):
    searchFields+(loadMode==='maximize-throughput'?
      localSelect('local-load-target-role','Target role',load.objective.target_role,['static','dynamic','total'])+
      localField(
        'local-load-plateau-gain-percent','Plateau gain (%)',load.objective.plateau_gain_percent,
        '','type=number min=0 step=any'
      )+
      localField('local-load-plateau-points','Plateau comparisons',load.objective.plateau_points,'','type=number min=1')+
      localField(
        'local-load-cpu-saturation-percent','CPU saturation (%)',load.objective.cpu_saturation_percent,
        '','type=number min=0 max=100 step=any'
      ):'');
  const slo=loadMode==='latency-slo'?'<h3>Latency SLO</h3><div class=form-grid>'+
    localSelect('local-slo-percentile','Percentile',load.objective.percentile,['p50','p95','p99','pmax'])+
    localField('local-slo-max-ms','Maximum latency (ms)',load.objective.max_ms,'','type=number min=0 step=any')+
    localField(
      'local-slo-max-errors','Maximum errors',load.objective.max_errors,
      load.allow_errors?'Ignored while failed requests are allowed.':'',
      'type=number min=0 '+(load.allow_errors?'disabled':'')
    )+
    localField(
      'local-slo-min-achieved-rate-ratio','Minimum achieved rate ratio',load.objective.min_achieved_rate_ratio,
      '','type=number min=0 max=1 step=any'
    )+'</div>':'';
  const affinity=Object.entries(localYdbAffinityKeys).map(([key,label])=>{
    const role=config.affinity[key],disabled=role.mode==='none'?'disabled':'';
    return '<div class=card><strong>'+esc(label)+'</strong><div class=form-grid>'+
      localSelect('local-affinity-'+key+'-mode','Mode',role.mode,editor.model.affinity_modes)+
      localField(
        'local-affinity-'+key+'-cpus','CPUs',role.cpus??'','integer, one-chiplet, or remaining',disabled
      )+'</div></div>'
  }).join('');
  return '<div id=local-editor><h2 class=page-title>'+esc(profile.benchmark)+' / '+esc(profile.name)+'</h2>'+
    '<div class=form-grid>'+localSelect(
      'benchmark','Benchmark',profile.benchmark,editor.model.benchmarks.map(item=>item.name)
    )+localField('profile-name','Profile name',profile.name,'letters, digits, . _ and -')+'</div>'+
    '<h3>Workload</h3><div class=form-grid>'+localSelect(
      'local-workload-type','Type',workload.type,['kv','stock']
    )+localSelect(
      'local-workload-operation','Operation',workload.operation,localYdbOperations[workload.type]
    )+options+'</div><h3>Cluster geometry</h3><div class=form-grid>'+
    localSelect('local-geometry-preset','Preset',geometry.preset,['single','storage','custom'])+geometryFields+
    '</div><h3>Client and load</h3><div class=form-grid>'+
    localField('local-client-threads','YDB CLI threads',config.client.threads,'','type=number min=1')+
    loadCommon+loadFields+'</div>'+slo+'<h3>Measurement</h3><div class=form-grid>'+
    localField('local-measurement-warmup','Warmup (seconds)',measurement.warmup,'','type=number min=0')+
    localField('local-measurement-duration','Duration (seconds)',measurement.duration,'','type=number min=1')+
    localField('local-measurement-repetitions','Repetitions',measurement.repetitions,'','type=number min=1')+
    localField(
      'local-timeout','Timeout (seconds)',profile.timeout??'','empty selects the computed timeout','type=number min=1'
    )+'</div><h3>Role affinity</h3>'+affinity+
    '<div class=toolbar><button class=danger id=delete-profile>Delete profile</button></div></div>'
}
function localNumber(id,minimum=1){
  const value=Number(document.querySelector('#'+id).value);
  if(!Number.isFinite(value)||value<minimum)throw Error(id+' must be a number not below '+minimum+'.');
  return value
}
function localInteger(id,minimum=1){const value=localNumber(id,minimum);if(!Number.isSafeInteger(value))throw Error(id+' must be an integer.');return value}
function localCpu(id,mode){
  if(mode==='none')return null;
  const raw=document.querySelector('#'+id).value.trim();
  if(raw==='one-chiplet'||raw==='remaining')return raw;
  const value=Number(raw);
  if(!Number.isSafeInteger(value)||value<1){
    throw Error(id+' must be a positive integer, one-chiplet, or remaining.')
  }
  return value
}
function bindLocalYdbEditor(profile){
  const message=()=>document.querySelector('#editor-message');
  const update=event=>{try{
    const benchmarkName=document.querySelector('#benchmark').value,name=document.querySelector('#profile-name').value.trim();
    if(!/^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$/.test(name))throw Error('Profile name is unsafe.');
    if(editor.model.profiles.some(item=>
      item.key!==profile.key&&item.benchmark===benchmarkName&&item.name===name
    ))throw Error('A profile with this benchmark and name already exists.');
    if(benchmarkName!==profile.benchmark){
      const benchmark=editor.model.benchmarks.find(item=>item.name===benchmarkName);
      profile.benchmark=benchmarkName;profile.name=name;profile.key=benchmarkName+'/'+name;
      delete profile.local_ydb;
      profile.parameters=Object.fromEntries(benchmark.parameters.map(item=>[item.name,item.default]));
      profile.threads=[1];profile.duration=3;profile.repetitions=1;profile.affinity=['none'];
      profile.background_load=['none'];editor.selected=profile.key;editor.yaml=serializeConfig(editor.model);
      saveDraft();renderNew();return
    }
    profile.name=name;profile.key=benchmarkName+'/'+name;const config=profile.local_ydb;
    if(event.target.id==='local-workload-type'){
      config.workload=defaultLocalYdbWorkload(event.target.value);editor.selected=profile.key;
      editor.yaml=serializeConfig(editor.model);saveDraft();renderNew();return
    }
    if(event.target.id==='local-geometry-preset'){
      const preset=event.target.value;config.geometry.preset=preset;
      if(preset==='single'){
        config.geometry.dynamic_nodes=1;config.geometry.max_dynamic_nodes=1
      }else if(preset==='storage'){
        config.geometry.max_dynamic_nodes=Math.max(
          8,config.geometry.dynamic_nodes,config.geometry.max_dynamic_nodes
        )
      }
      editor.yaml=serializeConfig(editor.model);saveDraft();renderNew();return
    }
    if(event.target.id==='local-load-mode'){
      const mode=event.target.value,allow_errors=Boolean(config.load.allow_errors);
      if(mode==='points'){
        config.load={parameter:config.load.parameter,allow_errors,values:config.load.values||[1000]}
      }else{
        const search=config.load.search||{start:1000,maximum:100000,multiplier:2,resolution_percent:2};
        const old=config.load.objective||{};
        const objective={
          type:mode,target_role:old.target_role||'dynamic',plateau_gain_percent:old.plateau_gain_percent??2,
          plateau_points:old.plateau_points||2,cpu_saturation_percent:old.cpu_saturation_percent||95
        };
        if(mode==='latency-slo')Object.assign(objective,{
          percentile:old.percentile||'p99',max_ms:old.max_ms??10,max_errors:old.max_errors??0,
          min_achieved_rate_ratio:old.min_achieved_rate_ratio??.98
        });
        config.load={parameter:config.load.parameter,allow_errors,search,objective}
      }
      editor.yaml=serializeConfig(editor.model);saveDraft();renderNew();return
    }
    if(event.target.id.startsWith('local-affinity-')&&event.target.id.endsWith('-mode')){
      const key=event.target.id.slice('local-affinity-'.length,-'-mode'.length);
      const mode=event.target.value,old=config.affinity[key].cpus;
      config.affinity[key]={mode,cpus:mode==='none'?null:(old??(key==='ydb_cli'?'one-chiplet':1))};
      editor.yaml=serializeConfig(editor.model);saveDraft();renderNew();return
    }
    config.workload.operation=document.querySelector('#local-workload-operation').value;
    for(const input of document.querySelectorAll('[id^=local-option-]')){
      const key=input.id.slice('local-option-'.length);
      const minimum=['init-upserts','orders','auto-partition'].includes(key)?0:1;
      config.workload.options[key]=localInteger(input.id,minimum)
    }
    config.geometry.preset=document.querySelector('#local-geometry-preset').value;
    for(const key of Object.keys(localYdbGeometryKeys)){
      config.geometry[key]=localInteger('local-geometry-'+key)
    }
    if(config.geometry.preset==='single'){
      config.geometry.dynamic_nodes=1;config.geometry.max_dynamic_nodes=1
    }
    config.client.threads=localInteger('local-client-threads');
    const loadMode=document.querySelector('#local-load-mode').value;
    const parameter=document.querySelector('#local-load-parameter').value;
    const allow_errors=document.querySelector('#local-load-allow-errors').checked;
    if(loadMode==='points'){
      config.load={parameter,allow_errors,values:arrayField(document.querySelector('#local-load-values').value)}
    }else{
      const objective={type:loadMode};
      const multiplier=loadMode==='latency-slo'?
        localNumber('local-load-multiplier',1):(config.load.search?.multiplier??2);
      config.load={
        parameter,allow_errors,
        search:{
          start:localInteger('local-load-start'),maximum:localInteger('local-load-maximum'),multiplier,
          resolution_percent:localNumber('local-load-search-resolution-percent',0)
        },
        objective
      };
      if(loadMode==='maximize-throughput')Object.assign(objective,{
        target_role:document.querySelector('#local-load-target-role').value,
        plateau_gain_percent:localNumber('local-load-plateau-gain-percent',0),
        plateau_points:localInteger('local-load-plateau-points'),
        cpu_saturation_percent:localNumber('local-load-cpu-saturation-percent',0)
      });
      else Object.assign(objective,{
        percentile:document.querySelector('#local-slo-percentile').value,
        max_ms:localNumber('local-slo-max-ms',0),max_errors:localInteger('local-slo-max-errors',0),
        min_achieved_rate_ratio:localNumber('local-slo-min-achieved-rate-ratio',0)
      })
    }
    config.measurement={
      warmup:localInteger('local-measurement-warmup',0),
      duration:localInteger('local-measurement-duration'),
      repetitions:localInteger('local-measurement-repetitions')
    };
    for(const key of Object.keys(localYdbAffinityKeys)){
      const mode=document.querySelector('#local-affinity-'+key+'-mode').value;
      config.affinity[key]={mode,cpus:localCpu('local-affinity-'+key+'-cpus',mode)}
    }
    const timeout=document.querySelector('#local-timeout').value.trim();
    profile.timeout=timeout===''?null:localInteger('local-timeout');
    profile.threads=[config.client.threads];profile.duration=config.measurement.duration;
    profile.repetitions=1;profile.affinity=['roles'];profile.background_load=['none'];
    editor.selected=profile.key;editor.yaml=serializeConfig(editor.model);saveDraft();
    if(['profile-name','local-geometry-preset','local-load-allow-errors'].includes(event.target.id))renderNew()
  }catch(error){message().innerHTML=displayError(error)}};
  for(const input of document.querySelectorAll('#local-editor input,#local-editor select'))input.onchange=update;
  document.querySelector('#delete-profile').onclick=()=>{
    editor.model.profiles=editor.model.profiles.filter(item=>item.key!==profile.key);
    editor.selected=editor.model.profiles[0]?.key||null;editor.yaml=serializeConfig(editor.model);
    saveDraft();renderNew()
  }
}
"""
    'function profileEditor(profile){\n'
    '  const benchmark=(editor.model.benchmarks||[]).find(item=>item.name===profile.benchmark);\n'
    "  if(benchmark.profile_kind==='local-ydb')return localYdbProfileEditor(profile);\n"
    "  if(!benchmark.builder_supported)return '<h2 class=page-title>'+esc(profile.benchmark)+' / '+esc(profile.name)+"
    "'</h2><div class=notice>Edit this benchmark in the YAML tab; its nested cluster, workload, load controller, and role "
    "affinity settings are preserved there.</div>';\n"
    '  const field=(id,label,value,help=\'\')=>\'<div class=field><label for="\'+id+\'">\'+esc(label)+\'</label><input id="\'+id+\'" v'
    'alue="\'+esc(value)+\'"><small class=muted>\'+esc(help)+\'</small></div>\';\n'
    "  const parameterFields=benchmark.parameters.map((parameter,index)=>parameter.choices.length?'<div class=field><label>'+"
    "esc(parameter.name)+'</label><div class=checkboxes>'+parameter.choices.map(choice=>'<label><input type=checkbox class=pa"
    'rameter-choice data-parameter-index="\'+index+\'" value="\'+esc(choice)+\'" \'+((profile.parameters[parameter.name]||[]).incl'
    "udes(choice)?'checked':'')+'> '+esc(choice)+'</label>').join('')+'</div><small class=muted>'+esc(parameter.description)+"
    "'</small></div>':field('parameter-'+index,parameter.name,parameter.type==='integer'?compactIntegerRanges(profile.paramet"
    "ers[parameter.name]||[]):(profile.parameters[parameter.name]||[]).join(', '),parameter.description)).join('');\n"
    "  const memoryMb=profile.benchmark==='memory-bandwidth-bench'?Math.max(...profile.threads)*Math.max(...(profile.paramete"
    "rs['buffer-size-mb']||[0])):0;\n"
    "  return '<h2 class=page-title>'+esc(profile.benchmark)+' / '+esc(profile.name)+'</h2>'+(memoryMb?'<div class=notice>Max"
    "imum private-buffer footprint per process: <strong>'+esc(memoryMb)+' MiB</strong>.</div>':'')+'<div class=form-grid><div"
    ' class=field><label>Benchmark</label><select id=benchmark>\'+editor.model.benchmarks.map(item=>\'<option value="\'+esc(item'
    '.name)+\'" \'+(item.name===profile.benchmark?\'selected\':\'\')+\'>\'+esc(item.name)+\'</option>\').join(\'\')+\'</select></div>\'+fie'
    "ld('profile-name','Profile name',profile.name,'letters, digits, . _ and -')+field('threads','Threads',compactIntegerRang"
    "es(profile.threads),'values and ranges, for example 1-16')+parameterFields+field('duration','Duration (seconds)',profile"
    ".duration)+field('repetitions','Repetitions',profile.repetitions)+'</div><div class=field><label>Affinity modes</label><"
    'div class=checkboxes>\'+editor.model.affinity_modes.map(mode=>\'<label><input class=affinity type=checkbox value="\'+esc(mo'
    'de)+\'" \'+(profile.affinity.includes(mode)?\'checked\':\'\')+\'> \'+esc(mode)+\'</label>\').join(\'\')+\'</div></div><div class=tool'
    "bar><div class=field><label>Background load</label><div class=checkboxes>'+editor.model.background_load_modes.map(mode=>"
    "'<label><input class=background-load type=checkbox value=\"'+esc(mode)+'\" '+((profile.background_load||['none']).inclu"
    "des(mode)?'checked':'')+'> '+esc(mode)+'</label>').join('')+'</div></div><button class=danger id=delete-profile>Delete pro"
    "file</button></div>'\n"
    '}\n'
    'function arrayField(value,minimum=1){\n'
    "  const parts=value.split(',').map(part=>part.trim()).filter(Boolean),values=[],seen=new Set;\n"
    "  if(!parts.length)throw Error('Enter one or more integers or ranges.');\n"
    '  for(const part of parts){\n'
    '    const match=/^(\\d+)(?:\\s*-\\s*(\\d+))?$/.exec(part);\n'
    "    if(!match)throw Error('Values must be integers or ranges such as 1-16.');\n"
    '    const first=Number(match[1]),last=Number(match[2]||match[1]);\n'
    "    if(!Number.isSafeInteger(first)||!Number.isSafeInteger(last)||first<minimum||last<first)throw Error('Ranges must use"
    " integers not below '+minimum+' in ascending order.');\n"
    "    if(last-first+1>10000||values.length+last-first+1>10000)throw Error('A field may expand to at most 10,000 values.');"
    '\n'
    '    for(let number=first;number<=last;number++){\n'
    "      if(seen.has(number))throw Error('Values and ranges must not overlap.');\n"
    '      seen.add(number);values.push(number);\n'
    '    }\n'
    '  }\n'
    '  return values\n'
    '}\n'
    """
function bindProfileEditor(profile){
  const update=event=>{try{
    const name=document.querySelector('#profile-name').value.trim();
    const benchmarkName=document.querySelector('#benchmark').value;
    const benchmark=editor.model.benchmarks.find(item=>item.name===benchmarkName);
    const benchmarkChanged=event?.target?.id==='benchmark';
    if(!/^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$/.test(name))throw Error('Profile name is unsafe.');
    if(editor.model.profiles.some(item=>
      item.key!==profile.key&&item.benchmark===benchmarkName&&item.name===name
    ))throw Error('A profile with this benchmark and name already exists.');
    updateProfile(profile.key,item=>{
      item.benchmark=benchmarkName;item.name=name;item.key=benchmarkName+'/'+name;
      if(benchmarkChanged&&benchmark.profile_kind==='local-ydb'){
        item.local_ydb=defaultLocalYdb();item.parameters={};item.threads=[64];item.duration=30;
        item.repetitions=1;item.affinity=['roles'];item.background_load=['none'];return
      }
      delete item.local_ydb;item.threads=arrayField(document.querySelector('#threads').value);item.parameters={};
      benchmark.parameters.forEach((parameter,index)=>{
        if(benchmarkChanged){item.parameters[parameter.name]=[...parameter.default];return}
        if(parameter.choices.length){
          const selector='.parameter-choice[data-parameter-index="'+index+'"]:checked';
          const selected=[...document.querySelectorAll(selector)].map(input=>input.value);
          if(!selected.length)throw Error('Select at least one value for '+parameter.name+'.');
          item.parameters[parameter.name]=selected;return
        }
        const raw=document.querySelector('#parameter-'+index)?.value||parameter.default.join(', ');
        item.parameters[parameter.name]=parameter.type==='integer'?
          arrayField(raw,parameter.minimum??1):raw.split(',').map(value=>value.trim()).filter(Boolean)
      });
      item.duration=Number(document.querySelector('#duration').value);
      item.repetitions=Number(document.querySelector('#repetitions').value);
      item.affinity=[...document.querySelectorAll('.affinity:checked')].map(input=>input.value);
      item.background_load=[...document.querySelectorAll('.background-load:checked')].map(input=>input.value);
      if(!item.background_load.length)throw Error('Select at least one background load mode.')
    });
    editor.selected=benchmarkName+'/'+name;
    if(benchmarkChanged||(
      !event?.target?.classList.contains('affinity')&&
      !event?.target?.classList.contains('background-load')&&
      !event?.target?.classList.contains('parameter-choice')
    ))renderNew()
  }catch(error){document.querySelector('#editor-message').innerHTML=displayError(error)}};
  const selector=
    '#benchmark,#profile-name,#threads,[id^=parameter-],.parameter-choice,'+
    '#duration,#repetitions,.affinity,.background-load';
  for(const input of document.querySelectorAll(selector))input.onchange=update;
  document.querySelector('#delete-profile').onclick=()=>{
    editor.model.profiles=editor.model.profiles.filter(item=>item.key!==profile.key);
    editor.selected=editor.model.profiles[0]?.key||null;editor.yaml=serializeConfig(editor.model);
    saveDraft();renderNew()
  }
}
"""
    """
function addProfile(){
  const selectedBenchmark=document.querySelector('#add-benchmark')?.value;
  const benchmark=editor.model.benchmarks.find(item=>item.name===selectedBenchmark)||editor.model.benchmarks[0];
  let suffix=1,name='profile';
  while((editor.model.profiles||[]).some(item=>item.benchmark===benchmark.name&&item.name===name))name='profile-'+suffix++;
  const profile={
    key:benchmark.name+'/'+name,benchmark:benchmark.name,name,threads:[1],
    parameters:Object.fromEntries(benchmark.parameters.map(item=>[item.name,item.default])),
    duration:3,repetitions:1,timeout:null,affinity:['none'],background_load:['none']
  };
  if(benchmark.profile_kind==='local-ydb'){profile.local_ydb=defaultLocalYdb();profile.threads=[64];profile.duration=30;profile.repetitions=1;profile.affinity=['roles']}
  editor.model.profiles.push(profile);editor.selected=profile.key;editor.yaml=serializeConfig(editor.model);saveDraft();renderNew()
}
"""
    "async function renderNew(tab){clearRefresh();if(tab)sessionStorage.setItem('ydb-bench-editor-tab',tab);tab=sessionStorag"
    "e.getItem('ydb-bench-editor-tab')||'builder';await syncEditor();const summary=planSummary();let content='<h1 class=page-"
    'title>New run</h1><div class=tabs><a class="\'+(tab===\'builder\'?\'active\':\'\')+\'" href="#new">Builder</a><a class="\'+(tab=='
    '=\'yaml\'?\'active\':\'\')+\'" href="#new/yaml">YAML</a></div>\'+editorControls();if(tab===\'yaml\'){content+=\'<textarea class=yam'
    "l id=yaml-editor spellcheck=false>'+esc(editor.yaml)+'</textarea><div class=muted>Invalid YAML remains editable and is n"
    "ot overwritten by Builder.</div>';app.innerHTML=shell('new',content);document.querySelector('#yaml-editor').oninput=even"
    't=>{editor.yaml=event.target.value;saveDraft();clearTimeout(window.ydbBenchYamlTimer);window.ydbBenchYamlTimer=setTimeou'
    't(async()=>{await syncEditor();document.querySelector(\'#editor-message\').innerHTML=editor.error?\'<div class="notice erro'
    'r">\'+esc(editor.error)+\'</div>\':\'<div class="notice good">Builder model is synchronized.</div>\'},350)};bindEditorControl'
    "s();return}if(editor.error){content+=displayError(editor.error)+'<p>Fix the YAML in the YAML tab before editing with Bui"
    "lder.</p>';app.innerHTML=shell('new',content);bindEditorControls();return}const selected=profileByKey(editor.selected)||"
    "editor.model.profiles[0];content+='<div class=notice>Plan: <strong>'+summary.count+'</strong> processes; requested measu"
    "rement time <strong>'+Math.ceil(summary.seconds)+' s</strong>; output root is <code>'+esc(editor.model.output)+'</code>."
    '</div><div class=split><section class="card profile-list"><div class=toolbar><strong>Profiles</strong><button id=add-pro'
    'file>Add</button></div>\'+editor.model.profiles.map(profile=>\'<button data-profile="\'+esc(profile.key)+\'" class="\'+(profi'
    'le.key===selected?.key?\'selected\':\'\')+\'">\'+esc(profile.benchmark)+\' / \'+esc(profile.name)+\'</button>\').join(\'\')+\'</secti'
    "on><section class=card>'+ (selected?profileEditor(selected):'<div class=empty>Add a benchmark profile to begin.</div>')+"
    "'</section></div>';app.innerHTML=shell('new',content);bindEditorControls();document.querySelector('#add-profile').onclic"
    "k=addProfile;for(const button of document.querySelectorAll('[data-profile]'))button.onclick=()=>{editor.selected=button."
    "dataset.profile;renderNew()};if(selected){const benchmark=editor.model.benchmarks.find(item=>item.name===selected.bench"
    "mark);if(benchmark?.profile_kind==='local-ydb')bindLocalYdbEditor(selected);else if(benchmark?.builder_supported)bindPro"
    "fileEditor(selected)}}\n"
    'function clearRefresh(){if(refreshTimer){clearInterval(refreshTimer);refreshTimer=null}}\n'
    "function runFilters(){return '<div class=filters><div class=field><label>Status</label><select id=f-status><option value"
    '="">Any</option><option>queued</option><option>running</option><option>completed</option><option>failed</option><option>'
    'cancelled</option><option>recovery_required</option></select></div><div class=field><label>Benchmark</label><input id=f-'
    'benchmark placeholder="ping-bench"></div><div class=field><label>Profile</label><input id=f-profile placeholder="baselin'
    'e"></div><div class=field><label>Source</label><select id=f-source><option value="">Any</option><option value=local>Loca'
    'l</option><option value=imported>Imported</option></select></div><div class=field><label>From</label><input id=f-since t'
    "ype=date></div><div class=field><label>To</label><input id=f-until type=date></div></div>'}\n"
    "function runHref(id,kind){return '/api/runs/'+enc(id)+'/'+kind}\n"
    "async function renderRuns(){clearRefresh();let content='<h1 class=page-title>Runs</h1><p class=muted>Local and imported "
    "benchmark results. Filters apply without leaving this page.</p>'+runFilters()+'<div class=toolbar><input id=import-file "
    'type=file accept=.zip><button id=import-run>Import results</button><button id=apply-filters>Apply filters</button></div>'
    "<div id=runs-table></div>';app.innerHTML=shell('runs',content);async function load(){const query=new URLSearchParams();f"
    "or(const [name,id] of Object.entries({status:'f-status',benchmark:'f-benchmark',profile:'f-profile',source:'f-source',si"
    "nce:'f-since',until:'f-until'})){const value=document.querySelector('#'+id).value.trim();if(value)query.set(name,value)}"
    "try{const runs=await api('/api/runs?'+query);document.querySelector('#runs-table').innerHTML=runs.length?'<table><thead>"
    '<tr><th>Run</th><th>Status</th><th>Source</th><th>Started / duration</th><th>Profiles / repeats</th><th>perf</th><th>Act'
    'ions</th></tr></thead><tbody>\'+runs.map(run=>\'<tr><td><a href="#run/\'+enc(run.id)+\'">\'+esc(run.id)+\'</a><br><small class'
    "=muted>'+esc(run.config_path||'config snapshot')+'</small></td><td>'+status(run.status)+'</td><td>'+esc(run.source)+'</t"
    "d><td><time title=\"'+esc(run.started_at||'')+'\">'+esc(humanTime(run.started_at))+'</time><br><small>'+duration(run)+"
    "'</small></td><td>'+run.profiles+' / '+run.repetitions+'</t"
    'd><td>\'+ (run.perf?\'yes\':\'no\')+\'</td><td><div class=actions><a href="#run/\'+enc(run.id)+\'">Open</a><a data-repeat="\'+esc'
    '(run.id)+\'">Repeat</a><a href="\'+runHref(run.id,\'config\')+\'">YAML</a><a href="\'+runHref(run.id,\'manifest\')+\'">run.json</'
    'a><a href="\'+runHref(run.id,\'archive\')+\'">Archive</a></div></td></tr>\').join(\'\')+\'</tbody></table>\':\'<div class=empty>No'
    " runs match these filters.</div>';for(const item of document.querySelectorAll('[data-repeat]'))item.onclick=event=>{even"
    "t.preventDefault();reuseRun(item.dataset.repeat)}}catch(error){document.querySelector('#runs-table').innerHTML=displayEr"
    "ror(error)}}document.querySelector('#apply-filters').onclick=load;document.querySelector('#import-run').onclick=async()="
    ">{try{const file=document.querySelector('#import-file').files[0];if(!file)throw Error('Choose a portable ZIP archive fir"
    "st.');await api('/api/import',{method:'POST',body:await file.arrayBuffer()});await load()}catch(error){document.querySel"
    "ector('#runs-table').innerHTML=displayError(error)}};await load()}\n"
    "async function reuseRun(id){try{const value=await api('/api/runs/'+enc(id)+'/config.json');editor.yaml=value.yaml;editor"
    ".perf=Boolean(value.perf);editor.continueOnError=Boolean(value.continue_on_error);saveDraft();setRoute('new')}catch(erro"
    'r){alert(error.message)}}\n'
    "const chartColors=['#1b62b9','#c2410c','#087443','#7c3aed','#be185d','#0e7490','#854d0e','#94a3b8','#ef4444','#818cf8','"
    "#22c55e','#d946ef'];\n"
    'function metricLabel(value){const number=Number(value);if(!Number.isFinite(number))return String(value);return Math.abs('
    "number)>=1e9?(number/1e9).toFixed(2)+'B':Math.abs(number)>=1e6?(number/1e6).toFixed(2)+'M':Math.abs(number)>=1e3?(number"
    "/1e3).toFixed(2)+'k':Number.isInteger(number)?String(number):number.toFixed(2)}\n"
    'function chartNumber(value){return value===null||value===undefined?NaN:Number(value)}\n'
    "function chartSeriesLabel(series,compact=false){return compact?series.affinity:series.run+' / '+series.profile+' / '+ser"
    'ies.affinity}\n'
    "function seriesCpuNote(series){if(series.cpu_masks&&Object.keys(series.cpu_masks).length)return 'CPUs by threads: '+Obje"
    "ct.entries(series.cpu_masks).sort((left,right)=>Number(left[0])-Number(right[0])).map(([threads,cpus])=>threads+' → '+(c"
    "pus===null?'unrestricted':cpuRanges(cpus))).join('; ');return Object.hasOwn(series,'cpus')?(series.cpus===null?'CPUs: un"
    "restricted':'CPUs: '+cpuRanges(series.cpus)):'CPUs: not recorded'}\n"
    'function svgChart(metric,xName,xValues,seriesRows,colors){\n'
    '  const width=900,height=330,left=78,right=24,top=24,bottom=52,plotWidth=width-left-right,plotHeight=height-top-bottom,v'
    'alueFor=(item,row)=>chartNumber(row?.[item.metric||metric]);\n'
    '  const values=[];for(const item of seriesRows)for(const x of xValues){const value=valueFor(item,item.rows.get(String(x)'
    '));if(Number.isFinite(value))values.push(value)}\n'
    "  if(!values.length)return '<div class=empty>No numeric values for '+esc(metric)+'.</div>';\n"
    '  let yMin=Math.min(...values),yMax=Math.max(...values);if(yMin===yMax){const pad=Math.abs(yMin)*.05||1;yMin-=pad;yMax+='
    'pad}else{const pad=(yMax-yMin)*.08;yMin-=pad;yMax+=pad}\n'
    '  const numericX=xValues.map(Number),xMin=Math.min(...numericX),xMax=Math.max(...numericX),xPos=value=>left+(xMax===xMin'
    '?plotWidth/2:(Number(value)-xMin)/(xMax-xMin)*plotWidth),yPos=value=>top+(yMax-Number(value))/(yMax-yMin)*plotHeight;\n'
    '  let svg=\'<svg viewBox="0 0 \'+width+\' \'+height+\'" role=img aria-label="\'+esc(metric)+\' by \'+esc(xName)+\'">\';\n'
    "  for(let tick=0;tick<=4;tick++){const y=top+plotHeight*tick/4,value=yMax-(yMax-yMin)*tick/4;svg+='<line class=chart-gri"
    'd x1="\'+left+\'" y1="\'+y+\'" x2="\'+(width-right)+\'" y2="\'+y+\'"/><text class=chart-label x="\'+(left-8)+\'" y="\'+(y+4)+\'" tex'
    "t-anchor=end>'+esc(metricLabel(value))+'</text>'}\n"
    '  const tickValues=xValues.length<=10?xValues:[...new Set([0,1,2,3,4,5].map(index=>xValues[Math.round(index*(xValues.len'
    'gth-1)/5)]))];\n'
    '  for(const value of tickValues){const x=xPos(value);svg+=\'<line class=chart-grid x1="\'+x+\'" y1="\'+top+\'" x2="\'+x+\'" y2='
    '"\'+(top+plotHeight)+\'"/><text class=chart-label x="\'+x+\'" y="\'+(height-25)+\'" text-anchor=middle>\'+esc(metricLabel(value'
    "))+'</text>'}\n"
    '  svg+=\'<line class=chart-axis x1="\'+left+\'" y1="\'+(top+plotHeight)+\'" x2="\'+(width-right)+\'" y2="\'+(top+plotHeight)+\'"/'
    '><line class=chart-axis x1="\'+left+\'" y1="\'+top+\'" x2="\'+left+\'" y2="\'+(top+plotHeight)+\'"/><text class=chart-label x="\''
    '+(left+plotWidth/2)+\'" y="\'+(height-5)+\'" text-anchor=middle>\'+esc(xName)+\'</text>\';\n'
    '  seriesRows.forEach((item,index)=>{const color=colors[(item.colorIndex??index)%colors.length],segments=[];let segment=[]'
    ';for(const x of xValues){const row=item.rows.get(String(x)),y=valueFor(item,row);if(Number.isFinite(y)){segment.push({x'
    ',y,row});continue}if(segment.length){segments.push(segment);segment=[]}}if(segment.length)segments.push(segment);for(con'
    'st points of segments)svg+=\'<polyline class=chart-line stroke="\'+color+\'" points="\'+points.map(point=>xPos(point.x)+'
    '\',\'+yPos(point.y)).join(\' \')+\'"/>\';for(const point of segments.flat())svg+=\'<circle class=chart-point fill="\'+color+\'"'
    ' cx="\'+xPos(point.x)+\'" cy="\'+yPos(point.y)+\'" r="4"><title>\'+esc(item.label+\'; \'+xName+\'=\'+point.x+\'; \'+(item.me'
    'tric||metric)+\'=\'+point.y)+\'</title></circle>\'});\n'
    '  svg+=\'<line class=chart-cursor x1="0" y1="\'+top+\'" x2="0" y2="\'+(top+plotHeight)+\'" visibility="hidden"/>\';\n'
    "  return '<div class=chart-surface>'+svg+'</svg><div class=chart-tooltip hidden></div></div>'\n"
    '}\n'
    """
function bindChartTooltips(container,xName,xValues,seriesRows,metrics,colors,synchronize=false){
  const width=900,left=78,right=24,plotWidth=width-left-right,numericX=xValues.map(Number);
  const xMin=Math.min(...numericX),xMax=Math.max(...numericX);
  const xPos=value=>left+(xMax===xMin?plotWidth/2:(Number(value)-xMin)/(xMax-xMin)*plotWidth);
  const seriesFor=metric=>Array.isArray(seriesRows)?seriesRows:(seriesRows[metric]||[]);
  const panels=[...container.querySelectorAll('.chart-panel')].map(panel=>({
    panel,
    metric:panel.dataset.metric,
    svg:panel.querySelector('svg'),
    surface:panel.querySelector('.chart-surface'),
    tooltip:panel.querySelector('.chart-tooltip'),
    cursor:panel.querySelector('.chart-cursor'),
  })).filter(item=>item.svg&&item.surface&&item.tooltip&&item.cursor&&metrics.includes(item.metric));
  const hideAll=()=>{for(const item of panels){
    item.tooltip.hidden=true;item.cursor.setAttribute('visibility','hidden');
    item.cursor.removeAttribute('data-selected-x')
  }};
  const syncBoundary=synchronize?panels[0]?.panel.closest('.local-charts'):null;
  if(syncBoundary)syncBoundary.onmouseleave=hideAll;
  for(const active of panels){
    if(!syncBoundary)active.svg.onmouseleave=hideAll;
    active.svg.onmousemove=event=>{
      const bounds=active.svg.getBoundingClientRect(),viewX=(event.clientX-bounds.left)*width/bounds.width;
      const selected=xValues.reduce(
        (best,value)=>Math.abs(xPos(value)-viewX)<Math.abs(xPos(best)-viewX)?value:best,xValues[0]
      );
      const cursorX=xPos(selected),targets=synchronize?panels:[active];
      for(const item of panels)item.tooltip.hidden=true;
      for(const item of targets){
        item.cursor.setAttribute('x1',cursorX);item.cursor.setAttribute('x2',cursorX);
        item.cursor.setAttribute('visibility','visible');item.cursor.setAttribute('data-selected-x',selected)
      }
      const values=seriesFor(active.metric).map((item,index)=>({
        label:item.label,colorClass:(item.colorIndex??index)%chartColors.length,
        value:chartNumber(item.rows.get(String(selected))?.[item.metric||active.metric])
      })).filter(item=>Number.isFinite(item.value)).sort((leftItem,rightItem)=>
        rightItem.value-leftItem.value||leftItem.label.localeCompare(rightItem.label)
      );
      if(!values.length)return;
      active.tooltip.innerHTML='<strong>'+esc(xName)+' = '+esc(metricLabel(selected))+'</strong>'+
        values.map(item=>'<div class=tooltip-row><i class="tooltip-dot chart-bg-'+item.colorClass+
          '"></i><span class="chart-color-'+item.colorClass+'">'+esc(item.label)+
          '</span><span class=tooltip-value>'+esc(metricLabel(item.value))+'</span></div>').join('');
      active.tooltip.hidden=false;
      const surfaceBounds=active.surface.getBoundingClientRect(),tooltipWidth=active.tooltip.offsetWidth;
      const rawLeft=event.clientX-surfaceBounds.left+12;
      active.tooltip.style.left=Math.max(4,Math.min(rawLeft,surfaceBounds.width-tooltipWidth-4))+'px';
      active.tooltip.style.top=Math.max(4,event.clientY-surfaceBounds.top-active.tooltip.offsetHeight-10)+'px'
    }
  }
}
"""
    "async function loadChartData(runIds){const query=new URLSearchParams;for(const run of runIds)query.append('run',run);ret"
    "urn api('/api/chart-data?'+query)}\n"
    "function globLabelMatch(value,pattern){value=String(value);pattern=String(pattern||'*');return pattern.split('|').map(it"
    "em=>item.trim()).filter(Boolean).some(mask=>{if(mask==='*')return true;const parts=mask.split('*');let offset=0;if(parts"
    '[0]&&!value.startsWith(parts[0]))return false;for(const part of parts){if(!part)continue;const found=value.indexOf(part,'
    "offset);if(found<0)return false;offset=found+part.length}return mask.endsWith('*')||offset===value.length})}\n"
    'function chartMultiplierDimensions(data,state,series){\n'
    "  const queried=new Set(state.queries.flatMap(query=>Object.keys(query)).filter(name=>name!=='metric'));return data.dime"
    'nsions.filter(name=>name!==state.x&&data.dimension_metadata?.[name]?.series!==false&&(queried.has(name)||new Set(series'
    '.flatMap(item=>item.rows.map(row=>row[name]).filter(value=>value!==undefined)).map(String)).size>1))\n'
    '}\n'
    'function labelExpandedSeries(result,queries,scope){\n'
    "  const matches=(item,query)=>Object.entries(query).every(([name,value])=>name==='metric'||globLabelMatch(item.facets[na"
    'me],value)),matched=result.filter(item=>queries.some(query=>matches(item,query))),facetNames=[...new Set(matched.flatMa'
    'p(item=>Object.keys(item.facets)))],varyingFacets=facetNames.filter(name=>new Set(matched.map(item=>item.facets[name]))'
    '.size>1);\n'
    "  for(const item of result){const labels=varyingFacets.map(name=>name+'='+item.facets[name]),prefix=scope.singleProfile?"
    "'':item.run+' / '+item.profile;item.label=scope.singleProfile?(labels.join('; ')||'value'):prefix+(labels.length?'['+l"
    "abels.join(';"
    " ')+']':'')}return result\n"
    '}\n'
    'function mountSingleChart(container,data,scope={}){\n'
    '  if(!container)return;\n'
    "  if(!data.series.length){container.innerHTML='<div class=empty>No completed summary.csv data is available for this sele"
    "ction.</div>';return}\n"
    "  const state={benchmark:scope.benchmark||'',profile:scope.profile||'',x:scope.x||(data.dimensions.includes('threads')?'"
    "threads':data.dimensions[0]),ys:new Set(data.metrics.includes('median_msgs_per_sec')?['median_msgs_per_sec']:data.metrics"
    '.slice(0,1)),lines:null,lineFilters:{},queries:(scope.queries||[{}]).map(query=>({...query})),settingsOpen:Boolean(scope'
    '.open)};\n'
    '  const available=(all=false)=>data.series.filter(series=>(!state.benchmark||series.benchmark===state.benchmark)&&(!stat'
    'e.profile||series.profile===state.profile));\n'
    '  const resetSeriesState=()=>{state.lines=null;state.lineFilters={}};\n'
    '  function expandedSeries(series){\n'
    '    const multiplierDimensions=chartMultiplierDimensions(data,state,series);\n'
    '    const result=[];\n'
    '    for(const item of series){const groups=new Map;for(const row of item.rows){const values=multiplierDimensions.map(nam'
    'e=>row[name]),key=JSON.stringify(values);if(!groups.has(key))groups.set(key,{values,rows:[]});groups.get(key).rows.push('
    'row)}for(const [key,group] of groups){const facets={affinity:String(item.affinity)};multiplierDimensions.forEach((name,in'
    "dex)=>{facets[name]=String(group.values[index])});result.push({...item,id:item.id+'::'+key,facets,rows:group.rows})"
    '}}\n'
    '    return labelExpandedSeries(result,state.queries,scope)\n'
    '  }\n'
    '  function render(){\n'
    '    const benchmarks=[...new Set(data.series.map(item=>item.benchmark))].sort();if(!state.benchmark)state.benchmark=benc'
    'hmarks[0];\n'
    '    const profiles=[...new Set(data.series.filter(item=>item.benchmark===state.benchmark).map(item=>item.profile))].sort'
    '();if(!state.profile||!profiles.includes(state.profile))state.profile=profiles[0];\n'
    '    const baseSeries=available();\n'
    '    const dimensions=data.dimensions.filter(name=>baseSeries.some(item=>item.rows.some(row=>row[name]!==undefined)));if('
    '!dimensions.includes(state.x))state.x=dimensions[0];\n'
    '    const allSeries=expandedSeries(baseSeries),filterOptions={};for(const item of allSeries)for(const [name,value] of Ob'
    'ject.entries(item.facets))if((filterOptions[name]??=new Set).add(value));\n'
    '    for(const [name,values] of Object.entries(filterOptions))if(!state.lineFilters[name])state.lineFilters[name]=new Set'
    '(values);\n'
    "    const matches=(item,query)=>Object.entries(query).every(([name,value])=>name==='metric'||globLabelMatch(item.facets["
    'name],value)),series=allSeries.filter(item=>state.queries.some(query=>matches(item,query)));if(state.lines===null)state.'
    'lines=new Set(series.map(item=>item.id));\n'
    '    const select=(id,label,values,current)=>\'<div class=field><label for="\'+id+\'">\'+esc(label)+\'</label><select id="\'+id'
    '+\'">\'+values.map(value=>\'<option \'+(value===current?\'selected\':\'\')+\'>\'+esc(value)+\'</option>\').join(\'\')+\'</select></div>'
    "';\n"
    '    const facetNames=Object.entries(filterOptions).filter(([,values])=>values.size>1).map(([name])=>name),queryRows=stat'
    'e.queries.map((query,index)=>\'<div class=query-row data-query="\'+index+\'"><span class=query-token><b>metric</b> = <selec'
    "t class=query-metric>'+data.metrics.map(metric=>'<option '+(metric===(query.metric||[...state.ys][0])?'selected':'')+'>'"
    "+esc(metric)+'</option>').join('')+'</select></span>'+facetNames.map(name=>{const listId='query-values-'+index+'-'+name;"
    'return \'<span class=query-token><b>\'+esc(name)+\'</b> = <input class=query-facet data-facet="\'+esc(name)+\'" value="\'+esc('
    'query[name]||\'*\')+\'" list="\'+esc(listId)+\'" placeholder="*"><datalist id="\'+esc(listId)+\'"><option value="*">\'+[...filte'
    'rOptions[name]].sort((left,right)=>left.localeCompare(right,undefined,{numeric:true})).map(value=>\'<option value="\'+esc('
    'value)+\'">\').join(\'\')+\'</datalist></span>\'}).join(\'\')+\'<span class=query-actions><button class=remove-query \'+(state.que'
    "ries.length===1?'disabled':'')+'>Remove</button></span></div>').join('');\n"
    "    const settings='<div class=chart-controls>'+(scope.benchmark?'':select('chart-benchmark','Benchmark',benchmarks,stat"
    "e.benchmark))+(scope.profile?'':select('chart-profile','Profile',profiles,state.profile))+select('chart-x','X axis',dime"
    "nsions,state.x)+'</div><h3>Lines</h3><p class=muted>Each row adds matching lines. Use <code>*</code> as a wildcard and <"
    "code>|</code> for alternatives, for example <code>pack-numa-*-pack-core</code> or <code>25|50|75</code>.</p>'+queryRows+"
    "'<button id=add-query>Add line row</button>';\n"
    "    let controls='<div class=chart-settings-summary><button id=open-chart-settings>Configure chart</button>'+(scope.onRe"
    "move?'<button class=danger id=remove-chart>Remove chart</button>':'')+'<span class=muted>X: '+esc(state.x)+'; metrics: '"
    "+esc([...new Set(state.queries.map(item=>item.metric||[...state.ys][0]))].join(', '))+'; lines: '+series.filter(item=>st"
    "ate.lines.has(item.id)).length+'</span></div>'+(state.settingsOpen?'<div class=modal-backdrop id=chart-settings-backdrop"
    '><section class=modal role=dialog aria-modal=true aria-labelledby=chart-settings-title><header class=modal-header><h2 id'
    '=chart-settings-title>Chart settings</h2><button id=close-chart-settings aria-label="Close chart settings">Close</button'
    "></header><div class=modal-body>'+settings+'</div><footer class=modal-footer><button class=primary id=apply-chart-settin"
    "gs>Done</button></footer></section></div>':'')+'<div id=chart-warning></div><div id=chart-output></div>';\n"
    '    container.innerHTML=controls;\n'
    "    container.querySelector('#open-chart-settings').onclick=()=>{state.settingsOpen=true;render()};\n"
    "    container.querySelector('#remove-chart')?.addEventListener('click',scope.onRemove);\n"
    "    const closeSettings=()=>{state.settingsOpen=false;render()};container.querySelector('#close-chart-settings')?.addEve"
    "ntListener('click',closeSettings);container.querySelector('#apply-chart-settings')?.addEventListener('click',closeSettin"
    "gs);container.querySelector('#chart-settings-backdrop')?.addEventListener('click',event=>{if(event.target.id==='chart-se"
    "ttings-backdrop')closeSettings()});\n"
    "    const benchmark=container.querySelector('#chart-benchmark');if(benchmark)benchmark.onchange=()=>{state.benchmark=ben"
    "chmark.value;state.profile='';resetSeriesState();render()};\n"
    "    const profile=container.querySelector('#chart-profile');if(profile)profile.onchange=()=>{state.profile=profile.value"
    ';resetSeriesState();render()};\n'
    "    const xAxis=container.querySelector('#chart-x');if(xAxis)xAxis.onchange=event=>{state.x=event.target.value;resetSeri"
    'esState();render()};\n'
    "    container.querySelector('#add-query')?.addEventListener('click',()=>{state.queries.push({metric:[...state.ys][0]});r"
    'ender()});\n'
    "    for(const row of container.querySelectorAll('.query-row')){const index=Number(row.dataset.query),query=state.queries"
    "[index];row.querySelector('.query-metric').onchange=event=>{query.metric=event.target.value;state.ys=new Set(state.queri"
    "es.map(item=>item.metric||[...state.ys][0]));render()};for(const input of row.querySelectorAll('.query-facet'))input.onc"
    "hange=event=>{query[input.dataset.facet]=event.target.value;render()};row.querySelector('.remove-query').onclick=()=>{if"
    '(state.queries.length>1){state.queries.splice(index,1);state.ys=new Set(state.queries.map(item=>item.metric||[...state.y'
    's][0]));render()}}}\n'
    '    draw()\n'
    '  }\n'
    '  function draw(){\n'
    "    const matches=(item,query)=>Object.entries(query).every(([name,value])=>name==='metric'||globLabelMatch(item.facets["
    'name],value)),chosen=expandedSeries(available()).filter(item=>state.lines.has(item.id)).map(item=>({...item,metrics:new '
    'Set(state.queries.filter(query=>matches(item,query)).map(query=>query.metric||[...state.ys][0]))})).filter(item=>item.me'
    "trics.size);const output=container.querySelector('#chart-output'),warning=container.querySelector('#chart-warning');\n"
    "    if(!chosen.length||!state.ys.size){warning.innerHTML='';output.innerHTML='<div class=empty>Select at least one line "
    "and one Y axis.</div>';return}\n"
    '    const selectedMetrics=[...new Set(chosen.flatMap(item=>[...item.metrics]))],indexed=[];for(const item of chosen){con'
    'st rows=new Map;for(const row of item.rows)if(row[state.x]!==undefined)rows.set(String(row[state.x]),row);for(const metr'
    "ic of item.metrics)indexed.push({item,rows,metric,label:item.label+(selectedMetrics.length>1?' · '+metric:''),colorIndex"
    ':indexed.length})}\n'
    '    const sets=indexed.map(item=>new Set([...item.rows].filter(([,row])=>Number.isFinite(Number(row[item.metric]))).map('
    '([x])=>x))),common=[...sets[0]].filter(value=>sets.every(set=>set.has(value))).sort((a,b)=>Number(a)-Number(b)),union=ne'
    'w Set(sets.flatMap(set=>[...set]));\n'
    "    const xValues=[...union].sort((a,b)=>Number(a)-Number(b));if(!xValues.length){warning.innerHTML='<div class=\"notice"
    " error\">No numeric values are available.</div>';output.innerHTML='';return}\n"
    "    if(common.length<union.size){const coverage=indexed.map(item=>esc(item.label)+': '+sets[indexed.indexOf(item)].size+'"
    " / '+union.size).join('; ');warning.innerHTML='<div class=notice><strong>Incomplete data:</strong> missing values are o"
    "mitted, and internal gaps break chart lines.<div class=coverage>'+coverage+'</div></div>'}els"
    "e warning.innerHTML='<div class=\"notice good\">All selected lines cover '+union.size+' '+esc(state.x)+' values.</div>'"
    ';\n'
    "    if(indexed.length===1)indexed[0].label=indexed[0].metric;const colors=indexed.map((_,index)=>chartColors[index%char"
    "tColors.length]);const legend=indexed.length===1?'':'<div class=chart-legend>'+i"
    'ndexed.map((item,index)=>\'<span><i class="legend-swatch chart-bg-\'+index%chartColors.length+\'"></i>\'+esc(item.label)+\'</'
    "span>').join('')+'</div>';\n"
    "    const metricTitle=selectedMetrics.join(', '),chartTitle=scope.title?metricTitle+' — '+scope.title:metricTitle;output."
    'innerHTML=legend+\'<section class=chart-panel data-metric="combined"><h3>\'+esc(chartTitle)+\'</h3>\'+svgChart(\'combined\',s'
    'tate.x,xValues,indexed,colors)+\'</section>\';bindChartTooltips(out'
    "put,state.x,xValues,indexed,['combined'],colors)\n"
    '  }\n'
    '  render()\n'
    '}\n'
    'function defaultActorCharts(data,scope){\n'
    "  const facets=scope.benchmark==='ping-bench'?['actorPairs','in_flight']:scope.benchmark==='star-ping-bench'?['actorPa"
    "irs','star_multiply']:null;if(!facets||!scope.profile||!data.metrics.includes('median_msgs_per_sec'))return [];\n"
    '  const combinations=new Map;for(const series of data.series){if(series.benchmark!==scope.benchmark||series.profile!==s'
    'cope.profile)continue;for(const row of series.rows){if(facets.some(name=>row[name]===undefined))continue;const values=fa'
    'cets.map(name=>String(row[name])),key=JSON.stringify(values);combinations.set(key,Object.fromEntries(facets.map((name'
    ',index)=>[name,values[index]])))}}\n'
    "  return [...combinations.entries()].sort(([left],[right])=>left.localeCompare(right,undefined,{numeric:true})).map(([,"
    "query])=>({open:false,x:'threads',title:facets.map(name=>name+'='+query[name]).join(', '),queries:[{metric:'median_msgs_"
    "per_sec',...query}]}))\n"
    '}\n'
    'function defaultMemoryCharts(data,scope){\n'
    "  if(scope.benchmark!=='memory-bandwidth-bench'||!scope.profile)return [];const facets=['random_percent','random_mode','"
    "buffer_size_mb','part_size_kb','scope'],metrics=[['memory_traffic_mb_per_sec','sum'],['ops_per_sec','sum']];for(const me"
    "tric of ['worker_max_min_spread_pct','worker_mean_min_gap_pct'])if(data.metrics.includes(metric))metrics.push([metric,'"
    "fairness']);if(metrics.some(([metric])=>!data.metrics.includes(metric)))return [];\n"
    '  const combinations=new Map;for(const series of data.series){if(series.benchmark!==scope.benchmark||series.profile!==s'
    "cope.profile)continue;for(const row of series.rows){if(row.repeat_aggregation!=='median'||row.worker_aggregation!=='sum"
    "'||!['sequential','random'].includes(row.scope)||facets.some(name=>row[name]===undefined))continue;const values=facets.m"
    'ap(name=>String(row[name])),key=JSON.stringify(values);combinations.set(key,Object.fromEntries(facets.map((name,index)'
    '=>[name,values[index]])))}}\n'
    "  return [...combinations.entries()].sort(([left],[right])=>left.localeCompare(right,undefined,{numeric:true})).flatMap"
    "(([,fixed])=>metrics.map(([metric,workerAggregation])=>({open:false,x:'threads',title:facets.map(name=>name+'='+fixed["
    "name]).join(', '),queries:[{metric,...fixed,worker_aggregation:workerAggregation,repeat_aggregation:'median',repeat:'*'"
    '}]})))\n'
    '}\n'
    'function defaultChartScope(data,scope){\n'
    '  const benchmarks=[...new Set(data.series.map(item=>item.benchmark))].sort(),benchmark=scope.benchmark||benchmarks[0]'
    ",profiles=[...new Set(data.series.filter(item=>item.benchmark===benchmark).map(item=>item.profile))].sort();return {ben"
    "chmark,profile:scope.profile||profiles[0]||''}\n"
    '}\n'
    'function mountChartBuilder(container,data,scope={}){\n'
    '  if(!container)return;let nextId=1,presetScope=defaultChartScope(data,scope),charts=[...defaultActorCharts(data,presetS'
    'cope),...defaultMemoryCharts(data,presetScope)].map(chart=>({...chart,...presetScope}));if(!c'
    'harts.length)charts=[{open:false}];chart'
    's=charts.map(chart=>({...chart,id:nextId++}));\n'
    "  function renderBoard(){container.innerHTML='<div class=toolbar><button class=primary id=add-chart>Add chart</button></"
    'div><div class=chart-board>\'+charts.map(chart=>\'<section class=card data-chart="\'+chart.id+\'"></section>\').join(\'\')+\'</d'
    "iv>';container.querySelector('#add-chart').onclick=()=>{charts.push({id:nextId++,open:true});renderBoard()};for(const ch"
    'art of charts){const target=container.querySelector(\'[data-chart="\'+chart.id+\'"]\');mountSingleChart(target,data,{...scop'
    'e,...chart,onRemove:charts.length>1?()=>{charts=charts.filter(item=>item.id!==chart.id);renderBoard()}:null});cha'
    'rt.open=false}}\n'
    '  renderBoard()\n'
    '}\n'
    """
const localPhaseLabels={
  'preparing-cluster':'Preparing cluster','starting-static-nodes':'Starting static nodes','waiting-for-static-nodes':'Waiting for static nodes',
  'bootstrapping-cluster':'Bootstrapping cluster','creating-database':'Creating database','starting-dynamic-nodes':'Starting dynamic nodes',
  'waiting-for-database':'Waiting for database','cluster-ready':'Cluster ready','initializing-workload':'Initializing workload',
  'warming-up':'Warming up','measuring':'Measuring','cleaning-workload':'Cleaning workload','evaluating-attempt':'Evaluating attempt',
  'scaling-dynamic-nodes':'Scaling dynamic nodes','stopping-cluster':'Stopping cluster','finishing':'Writing results',
  completed:'Completed',failed:'Failed',cancelled:'Cancelled'
};
function localPhaseLabel(phase){return localPhaseLabels[phase]||String(phase||'Preparing').replaceAll('-',' ')}
function localShellArg(value){value=String(value);return /^[A-Za-z0-9_@%+=:,./-]+$/.test(value)?value:"'"+value.replaceAll("'","'\\\"'\\\"'")+"'"}
function localCommandText(record){
  const argv=Array.isArray(record?.argv)?record.argv:[];
  const command=argv.map(localShellArg).join(' ');
  const cpus=Array.isArray(record?.cpu_affinity)?record.cpu_affinity:[];
  return cpus.length?'taskset --cpu-list '+localShellArg(cpus.join(','))+' '+command:command
}
function localCommandDetails(item,open){
  const commands=Array.isArray(item.commands)?item.commands:[];
  if(!commands.length)return '—';
  return '<details class=local-command-history data-command-attempt="'+esc(item.attempt)+'"'+
    (open?' open':'')+'><summary>'+commands.length+' commands</summary>'+commands.map(command=>
      '<div class=local-command-entry><strong>Repetition '+esc(command.repetition)+' · '+
      esc(localPhaseLabel(command.phase))+'</strong><pre class=local-command-code><code>'+esc(localCommandText(command))+
      '</code></pre></div>'
    ).join('')+'</details>'
}
function localProfileDetails(data,open){
  const configuration={
    parameters:data.parameters||{},timeout_seconds:data.timeout_seconds??null,role_affinity:data.role_affinity||{}
  };
  return '<details class=local-profile-config data-local-profile-config'+(open?' open':'')+
    '><summary><strong>Launch parameters</strong> <span class=muted>Normalized profile and effective CPU affinity; '+
    'exact commands are listed per attempt.</span></summary><pre><code>'+esc(JSON.stringify(configuration,null,2))+
    '</code></pre></details>'
}
function localElapsed(started,finished=null){
  const value=Date.parse(started),end=finished?Date.parse(finished):Date.now();
  return Number.isFinite(value)&&Number.isFinite(end)?Math.max(0,(end-value)/1000):0
}
function localKpi(label,value,help='',primary=false){
  return '<div class="'+(primary?'primary-result':'')+'"><span class=muted>'+esc(label)+'</span><strong>'+esc(value??'—')+
    '</strong>'+(help?'<small class=muted>'+esc(help)+'</small>':'')+'</div>'
}
function localOutcomeLabel(outcome){
  return ({
    'boundary-found':'SLO boundary found','plateau-found':'Throughput plateau found',
    'lower-bound':'Capacity lower bound','best-observed':'Best observed point',
    'no-feasible-point':'No feasible point','bounded-by-errors':'Bounded by workload errors',
    'search-limit-reached':'Search limit reached'
  })[outcome]||outcome||'Search in progress'
}
function localSearchAxisLabel(parameter,workload){
  if(parameter==='threads')return 'YDB CLI threads';
  if(parameter==='rate')return workload==='stock'?'Offered rate (transactions/s)':'Offered rate (requests/s)';
  return parameter||'Search value'
}
function localAttemptRows(attempts,xField='attempt'){return new Map(attempts.map(item=>[String(item[xField]),item]))}
function localChart(title,metric,xName,xValues,series){
  return '<section class=chart-panel data-metric="'+esc(metric)+'"><h3>'+esc(title)+'</h3>'+
    svgChart(metric,xName,xValues,series,chartColors)+'</section>'
}
function localBestRows(attempts,objective,xField='attempt'){
  let bestLoad=null,bestThroughput=-Infinity,currentStage=null;const rows=new Map;
  for(const item of attempts){
    if(currentStage!==item.search_stage){currentStage=item.search_stage;bestLoad=null;bestThroughput=-Infinity}
    if(item.passed){
      if(objective==='latency-slo'){if(bestLoad===null||item.load>bestLoad)bestLoad=item.load}
      else if(Number(item.throughput)>bestThroughput){bestThroughput=Number(item.throughput);bestLoad=item.load}
    }
    rows.set(String(item[xField]),{...item,current_best:bestLoad,passed_load:item.passed?item.load:null,failed_load:item.passed?null:item.load})
  }
  return rows
}
function renderLocalYdbProfile(container,data){
  if(data.state==='preparing'){
    container.innerHTML='<div class=notice>Preparing local YDB profile and extracting binaries…</div>';return
  }
  const openCommandAttempts=new Set(
    [...container.querySelectorAll('[data-command-attempt][open]')].map(details=>details.dataset.commandAttempt)
  );
  const profileConfigOpen=container.querySelector('[data-local-profile-config][open]')!==null;
  const progress=data.progress||{},attempts=data.attempts||[],searches=data.searches||[];
  const result=data.result||null,parameters=data.parameters||{},loadConfig=parameters.load||{};
  const objective=loadConfig.objective?.type||'points';
  const phaseElapsed=localElapsed(progress.phase_started_at);
  const phaseDuration=Number(progress.phase_duration_seconds);
  const profileElapsed=localElapsed(data.started_at,data.finished_at);
  const remaining=Number.isFinite(phaseDuration)?Math.max(0,phaseDuration-phaseElapsed):null;
  const phaseHelp=[
    progress.attempt?'attempt #'+progress.attempt:null,
    progress.repetition?'repetition '+progress.repetition+'/'+progress.repetitions:null,
    Number.isFinite(remaining)?elapsedLabel(remaining)+' remaining':null
  ].filter(Boolean).join(' · ');
  const phaseProgress=Number.isFinite(phaseDuration)?
    '<progress class=local-phase-progress max="'+phaseDuration+'" value="'+
      Math.min(phaseDuration,phaseElapsed)+'"></progress>':'';
  let html=loadConfig.allow_errors?
    '<div class=notice>Failed workload requests are allowed for this profile and remain visible in metrics.</div>':'';
  html+='<div class=local-live><div><span class=muted>Current phase</span><div class=local-phase>'+
    esc(localPhaseLabel(progress.phase||data.state))+'</div><div class=muted>'+
    esc(phaseHelp||'Waiting for the next milestone')+'</div>'+phaseProgress+'</div>';
  const dynamicNodes=
    progress.dynamic_nodes??result?.dynamic_nodes??parameters.geometry?.dynamic_nodes??'—';
  const candidate=progress.load===undefined?
    '—':(progress.parameter||loadConfig.parameter||'load')+' '+metricLabel(progress.load);
  html+=localKpi('Profile elapsed',elapsedLabel(profileElapsed))+
    localKpi('Geometry',(parameters.geometry?.static_nodes??'—')+' static · '+dynamicNodes+' dynamic')+
    localKpi('Candidate',candidate)+'</div>';
  html+=localProfileDetails(data,profileConfigOpen);
  if(progress.current_command?.argv?.length){
    html+='<section class=local-current-command><span class=muted>Running command</span>'+
      '<pre class=local-command-code><code>'+esc(localCommandText(progress.current_command))+
      '</code></pre></section>'
  }
  if(result){
    const selected=result.selected_metrics||{};
    const latencyMetric=(loadConfig.objective?.percentile||'p99')+'_ms';
    const selectedLabel=result.selected_load===null||result.selected_load===undefined?
      '—':metricLabel(result.selected_load);
    html+='<div class=local-kpis>'+localKpi(
      localOutcomeLabel(result.outcome),selectedLabel,result.parameter||loadConfig.parameter,true
    )+localKpi('Achieved throughput',metricLabel(selected.throughput??'—'),'transactions/s')+
      localKpi(
        loadConfig.objective?.percentile||'p99',metricLabel(selected[latencyMetric]??'—'),'ms'
      )+localKpi('Dynamic nodes',result.dynamic_nodes,result.stop_reason||'')+'</div>'
  }else{
    html+='<div class=local-kpis>'+localKpi(
      'Completed attempts',attempts.length,'search stage '+(progress.search_stage||1),true
    )+localKpi(
      'Latest throughput',attempts.length?metricLabel(attempts.at(-1).throughput):'—','transactions/s'
    )+localKpi(
      'Latest p99',attempts.length?metricLabel(attempts.at(-1).p99_ms):'—','ms'
    )+'</div>'
  }
  const currentStage=Number(progress.search_stage||0);
  const lastStored=searches.length?Math.max(...searches.map(item=>Number(item.stage)||0)):0;
  const xAxis=container.dataset.localYdbXAxis==='parameter'?'parameter':'attempt';
  const searchParameter=loadConfig.parameter||result?.parameter||'load';
  const searchAxisLabel=localSearchAxisLabel(searchParameter,parameters.workload?.type);
  let chartBinding=null;
  const stageCards=searches.map(item=>
    '<div class=local-stage><strong>Stage '+esc(item.stage)+' · '+esc(item.dynamic_nodes)+
    ' dynamic</strong><div>'+esc(localOutcomeLabel(item.outcome))+'</div><div class=muted>selected '+
    esc(metricLabel(item.selected_load))+' · '+esc(elapsedLabel(item.duration_seconds))+
    '</div><div class=stage-arrow>'+esc(
      item.next_action==='scale-dynamic-nodes'?
        'Scale → '+item.next_dynamic_nodes+' dynamic nodes':item.stop_reason||'Finish'
    )+'</div></div>'
  ).join('');
  const currentStageCard=currentStage>lastStored&&data.state==='running'?
    '<div class="local-stage current"><strong>Stage '+esc(currentStage)+' · '+
    esc(progress.dynamic_nodes??'—')+' dynamic</strong><div>In progress</div><div class=muted>'+
    esc(attempts.filter(item=>Number(item.search_stage)===currentStage).length)+
    ' completed attempts</div></div>':'';
  html+='<h3>Geometry stages</h3><div class=local-stages>'+stageCards+currentStageCard+'</div>';
  if(attempts.length){
    const xField=xAxis==='parameter'?'load':'attempt';
    const xName=xAxis==='parameter'?searchAxisLabel:'Attempt';
    const xValues=xAxis==='parameter'?
      [...new Set(attempts.map(item=>Number(item.load)))].sort((left,right)=>left-right):
      attempts.map(item=>item.attempt);
    const stages=[];
    for(const item of attempts){
      let stage=stages.find(value=>value.search_stage===item.search_stage);
      if(!stage){
        stage={search_stage:item.search_stage,dynamic_nodes:item.dynamic_nodes,attempts:[]};stages.push(stage)
      }
      stage.attempts.push(item)
    }
    const groups=(xAxis==='parameter'?stages:[{attempts}]).map(item=>({
      rows:localAttemptRows(item.attempts,xField),bestRows:localBestRows(item.attempts,objective,xField),
      suffix:xAxis==='parameter'&&stages.length>1?
        ' · stage '+item.search_stage+' · '+item.dynamic_nodes+' dynamic':''
    }));
    const candidateSeries=groups.flatMap(group=>[
      {rows:group.bestRows,metric:'load',label:'Candidate'+group.suffix,colorIndex:7},
      {rows:group.bestRows,metric:'current_best',label:'Current best'+group.suffix,colorIndex:0},
      {rows:group.bestRows,metric:'passed_load',label:'Passed'+group.suffix,colorIndex:10},
      {rows:group.bestRows,metric:'failed_load',label:'Failed'+group.suffix,colorIndex:8}
    ]);
    const throughputSeries=groups.flatMap(group=>{
      const values=[{
        rows:group.rows,metric:'throughput',label:'Achieved throughput'+group.suffix,colorIndex:0
      }];
      if(loadConfig.parameter==='rate')values.push({
        rows:group.rows,metric:'load',label:'Offered rate'+group.suffix,colorIndex:7
      });
      return values
    });
    const latencySeries=groups.flatMap(group=>
      ['p50_ms','p95_ms','p99_ms','pmax_ms'].map((metric,index)=>({
        rows:group.rows,metric,label:metric.replace('_ms','')+group.suffix,colorIndex:index
      }))
    );
    if(loadConfig.objective?.type==='latency-slo'){
      const sloRows=new Map(xValues.map(value=>[String(value),{slo_ms:loadConfig.objective.max_ms}]));
      latencySeries.push({rows:sloRows,metric:'slo_ms',label:'SLO',colorIndex:8})
    }
    const cpuMetrics=[
      ['static_cpu_mean','Static'],['dynamic_cpu_mean','Dynamic'],
      ['cli_cpu_mean','YDB CLI'],['host_cpu_mean','Host']
    ];
    const cpuSeries=groups.flatMap(group=>cpuMetrics.map(([metric,label],index)=>({
      rows:group.rows,metric,label:label+group.suffix,colorIndex:index
    })));
    const errorSeries=groups.flatMap(group=>[
      {rows:group.rows,metric:'errors',label:'Errors'+group.suffix,colorIndex:8},
      {rows:group.rows,metric:'retries',label:'Retries'+group.suffix,colorIndex:1}
    ]);
    chartBinding={
      xName,xValues,
      series:{
        load:candidateSeries,throughput:throughputSeries,latency_ms:latencySeries,
        cpu_percent:cpuSeries,errors:errorSeries
      }
    };
    const axisHelp=xAxis==='parameter'?
      'Points are ordered by the searched parameter; geometry stages remain separate.':
      'Execution order shows how the controller moved through candidate values.';
    html+='<div class=run-section-title><h3>Search process</h3><div class=actions><span class=muted>X axis</span>'+
      '<button type=button data-local-chart-x=attempt class="'+(xAxis==='attempt'?'primary':'')+
      '" aria-pressed="'+(xAxis==='attempt')+'">Attempts (search order)</button>'+
      '<button type=button data-local-chart-x=parameter class="'+(xAxis==='parameter'?'primary':'')+
      '" aria-pressed="'+(xAxis==='parameter')+'">'+esc(searchAxisLabel)+'</button></div></div>'+
      '<p class=muted>'+esc(axisHelp)+'</p><div class=local-charts>'+
      localChart('Candidate and current best','load',xName,xValues,candidateSeries)+
      localChart('Offered and achieved throughput','throughput',xName,xValues,throughputSeries)+
      localChart('Latency','latency_ms',xName,xValues,latencySeries)+
      localChart('CPU by role','cpu_percent',xName,xValues,cpuSeries)+
      localChart('Errors and retries','errors',xName,xValues,errorSeries)+'</div>';
    html+='<h3>Attempts</h3><div class=local-attempts-scroll tabindex=0 role=region aria-label="Search attempts">'+
      '<table class=local-attempts><thead><tr><th>#</th><th>Stage</th><th>Dynamic</th><th>Candidate</th>'+
      '<th>Throughput</th><th>p99</th><th>Errors</th><th>Static CPU</th><th>Dynamic CPU</th><th>CLI CPU</th>'+
      '<th>Verdict</th><th>Decision</th><th>Duration</th><th>Commands</th></tr></thead><tbody>'+
      attempts.map(item=>'<tr><td>'+esc(item.attempt)+'</td><td>'+esc(item.search_stage)+'</td><td>'+
        esc(item.dynamic_nodes)+'</td><td>'+esc(metricLabel(item.load))+'</td><td>'+esc(metricLabel(item.throughput))+
        '</td><td>'+esc(metricLabel(item.p99_ms))+' ms</td><td>'+esc(item.errors)+'</td><td>'+
        esc(metricLabel(item.static_cpu_mean))+'%</td><td>'+esc(metricLabel(item.dynamic_cpu_mean))+
        '%</td><td>'+esc(metricLabel(item.cli_cpu_mean))+'%</td><td class="'+
        (item.passed?'attempt-pass':'attempt-fail')+'">'+(item.passed?'PASS':'FAIL')+'</td><td>'+esc(item.decision)+
        '</td><td>'+esc(elapsedLabel(item.duration_seconds))+'</td><td class=local-command-cell>'+
        localCommandDetails(item,openCommandAttempts.has(String(item.attempt)))+'</td></tr>'
      ).join('')+'</tbody></table></div>';
  }else html+='<div class=empty>No completed search attempts yet. The timeline will appear after the first measurement.</div>';
  container.innerHTML=html;
  for(const axisButton of container.querySelectorAll('[data-local-chart-x]'))axisButton.onclick=()=>{
    container.dataset.localYdbXAxis=axisButton.dataset.localChartX;renderLocalYdbProfile(container,data)
  };
  if(chartBinding)bindChartTooltips(
    container,chartBinding.xName,chartBinding.xValues,chartBinding.series,
    Object.keys(chartBinding.series),chartColors,true
  )
}
async function mountLocalYdbProfile(container,runId,profile,runState){
  let loading=false,terminal=false;
  const scheduleRunRefresh=()=>{if(['running','queued'].includes(runState)&&!refreshTimer)refreshTimer=setTimeout(()=>renderRun(runId,'local-ydb/'+profile),700)};
  const refresh=async()=>{
    if(loading)return;loading=true;
    try{
      const data=await api('/api/runs/'+enc(runId)+'/local-ydb-profile?profile='+enc(profile));
      renderLocalYdbProfile(container,data);terminal=!['running','preparing'].includes(data.state);
      if(terminal&&refreshTimer){clearInterval(refreshTimer);refreshTimer=null}
      if(terminal)scheduleRunRefresh()
    }catch(error){container.innerHTML=displayError(error)}finally{loading=false}
  };
  await refresh();if(!terminal&&['running','queued','recovery_required'].includes(runState)&&!refreshTimer)refreshTimer=setInterval(refresh,1000)
}
    """
    "function profileGroups(steps){const groups={};for(const step of steps){const key=step.benchmark+'/'+step.profile;(groups"
    '[key]??=[]).push(step)}return groups}\n'
    'function affinityGroups(steps){const groups={};for(const step of steps)(groups[step.affinity]??=[]).push(step);return gr'
    'oups}\n'
    "function aggregateState(steps){if(steps.some(step=>step.state==='running'))return 'running';if(steps.some(step=>step.sta"
    "te==='failed'))return 'failed';if(steps.some(step=>step.state==='pending'))return 'pending';if(steps.some(step=>step.sta"
    "te==='cancelled'))return 'cancelled';if(steps.every(step=>step.state==='unsupported'))return 'unsupported';return 'passe"
    "d'}\n"
    'function caseLabel(run){const entries=Object.entries(run.parameters||{});return entries.length?entries.map(([name,value]'
    ")=>name+'='+value).join(', '):'—'}\n"
    'function affinityRows(id,steps){return Object.entries(affinityGroups(steps)).map(([affinity,runs])=>{const done=runs.fil'
    "ter(run=>!['pending','running'].includes(run.state)).length,details=runs.map(run=>'<tr><td>'+esc(run.threads??'—')+'</td"
    "><td>'+esc(caseLabel(run))+'</td><td>'+run.repeat+'</td><td>'+status(run.state)+'</td><td>'+esc(stepDuration(run))+'</td"
    '><td>\'+(run.artifacts||[]).map(path=>\'<a href="\'+runHref(id,\'artifact/\'+path.split(\'/\').map(enc).join(\'/\'))+\'">\'+esc(pat'
    "h.split('/').pop())+'</a>').join(' ')+'</td></tr>').join('');return '<tr><td>'+esc(affinity)+'</td><td>'+done+' / '+runs"
    ".length+'</td><td>'+status(aggregateState(runs))+'</td></tr><tr class=affinity-details><td colspan=3><details><summary>D"
    'etails</summary><table><tr><th>Threads</th><th>Parameters</th><th>Repeat</th><th>State</th><th>Duration</th><th>Artifact'
    "s</th></tr>'+details+'</table></details></td></tr>'}).join('')}\n"
    "async function renderRun(id,selectedProfile=''){\n"
    '  clearRefresh();\n'
    '  try{\n'
    "    const run=await api('/api/runs/'+enc(id));\n"
    "    activeRun=run.current_run_id||(['running','recovery_required'].includes(run.state)?id:'');\n"
    "    const queueNotice=run.state==='queued'?'<div class=notice>Queue position: '+esc(run.queue_position??'—')+'. '+(run.c"
    'urrent_run_id?\'<a href="#run/\'+enc(run.current_run_id)+\'">Currently running: \'+esc(run.current_run_id)+\'</a>\':\'Waiting f'
    "or the dispatcher.')+'</div>':'';\n"
    "    sessionStorage.setItem('ydb-bench-active-run',activeRun);\n"
    '    const groups=profileGroups(run.steps||[]),profileKeys=Object.keys(groups),activeProfile=groups[selectedProfile]?sele'
    "ctedProfile:profileKeys.length===1?profileKeys[0]:'',activeBenchmark=activeProfile?activeProfile.split('/')[0]:'';\n"
    "    const crumbs=[{route:'runs',label:'Runs'},{route:'run/'+enc(id),label:id}];if(activeProfile&&profileKeys.length>1)cr"
    "umbs.push({route:'run/'+enc(id)+'/profile/'+enc(activeProfile),label:activeProfile});\n"
    "    let content=breadcrumbs(crumbs)+queueNotice+'<h1 class=page-title>'+esc(id)+'</h1><div class=toolbar><button id=ref"
    "resh-run>Refresh</button>'+(['queued','running'].includes(run.state)?'<button class=danger id=cancel-run>Cancel</button>'"
    ":'')+'<button id=repeat-run>Repeat with this YAML</button><details class=downloads><summary>Downloads</summary><div cla"
    "ss=actions><a href=\"'+runHref(id,'config')+'\">YAML</a><a href=\"'+runHref(id,'manifest')+'\">run.json</a><a href=\"'+r"
    "unHref(id,'archive')+'\">Artifacts</a></div></details></div><div class=grid><section class=card><div class=form-grid><d"
    "iv><div class=muted>Status</div>'+status(run.status)+'</div><div><div class=muted>Output</div><code>'+esc(run.output_di"
    "rectory||id)+'</code></div><div><div class=muted>Time</div>'+esc(humanTime(run.started_at))+' / '+duration(run)+'</div>"
    "<div><div class=muted>Progress</div>'+run.finished_steps+' / '+run.steps.length+' steps</div></div></section>';\n"
    "    if(run.state==='recovery_required')content+='<div class=\"notice error\"><strong>Interrupted.</strong> The web servi"
    "ce restarted while this run was active. Verify that the previous benchmark process stopped before repeating it.</div>'"
    ";\n"
    "    if(profileKeys.length>1)content+='<nav class=run-tabs><a class=\"run-tab '+(!activeProfile?'active':'')+'\" href=\"#r"
    "un/'+enc(id)+'\">Overview</a>'+profileKeys.map(key=>'<a class=\"run-tab '+(key===activeProfile?'active':'')+'\" href=\"#"
    "run/'+enc(id)+'/profile/'+enc(key)+'\">'+esc(key)+'</a>').join('')+'</nav>';\n"
    "    if(!activeProfile)content+='<section class=\"card profile-overview\"><h2>Profiles</h2><table><tr><th>Profile</th><th"
    ">Progress</th><th>State</th><th>Affinity modes</th></tr>'+profileKeys.map(key=>{const steps=groups[key],done=steps.fil"
    "ter(step=>!['pending','running'].includes(step.state)).length,affinities=new Set(steps.map(step=>step.affinity)).size;re"
    "turn '<tr><td><a href=\"#run/'+enc(id)+'/profile/'+enc(key)+'\">'+esc(key)+'</a></td><td>'+done+' / '+steps.length+'</td"
    "><td>'+status(aggregateState(steps))+'</td><td>'+affinities+'</td></tr>'}).join('')+'</table></section>';\n"
    "    if(activeProfile)content+=activeBenchmark==='local-ydb'?'<section class=card><div class=run-section-title><h2>YDB "
    "load search</h2><strong>'+esc(activeProfile)+'</strong></div><div id=local-ydb-result>Loading live search data…</div>"
    "</section>':'<section class=card><div class=run-section-title><h2>Results</h2><strong>'+esc(activeProfile)+'</strong>"
    "</div><p class=muted>Affinity variants are lines. Choose a common X axis, one or more Y metrics, and fixed values for "
    "the remaining dimensions.</p><div id=run-chart>Loading summary data…</div></section>';\n"
    "    if(activeProfile){const steps=groups[activeProfile],open=run.state==='running'?' open':'';content+='<section class=\""
    "card run-tree\"><details'+open+'><summary><strong>Execution details</strong> — affinity, cases and artifacts</summary><"
    "table><tr><th>Affinity</th><th>Runs</th><th>State</th></tr>'+affinityRows(id,steps)+'</table></details></section>'}\n"
    "    const running=(run.steps||[]).find(step=>step.state==='running'),live=['running','queued','failed','recovery_requir"
    "ed'].includes(run.state);\n"
    "    if(live)content+='<section class=card><h2>Current step</h2>'+ (running?'<p><strong>'+esc(running.benchmark)+' / '+e"
    "sc(running.profile)+'</strong>, '+esc(running.affinity)+', '+esc(running.threads??'—')+' threads, repeat '+running.repe"
    "at+', elapsed '+esc(stepDuration(running))+'</p>':'<p class=muted>No step is currently running.</p>')+'<h3>Live stdout"
    "</h3><pre class=log>'+esc(run.tail?.stdout||'No stdout captured yet.')+'</pre><h3>Live stderr</h3><pre class=log>'+esc"
    "(run.tail?.stderr||'No stderr captured yet.')+'</pre></section>';content+='</div>';\n"
    "    app.innerHTML=shell('runs',content);\n"
    "    document.querySelector('#refresh-run').onclick=()=>renderRun(id,activeProfile);\n"
    "    document.querySelector('#repeat-run').onclick=()=>reuseRun(id);\n"
    "    const cancel=document.querySelector('#cancel-run');\n"
    "    if(cancel)cancel.onclick=async()=>{try{await api('/api/runs/'+enc(id)+'/cancel',{method:'POST'});renderRun(id,acti"
    'veProfile)}catch(error){alert(error.message)}};\n'
    "    if(activeProfile){const pieces=activeProfile.split('/'),benchmark=pieces.shift(),profile=pieces.join('/');if("
    "benchmark==='local-ydb')await mountLocalYdbProfile(document.querySelector('#local-ydb-result'),id,profile,run.state);"
    "else try{mountChartBuilder(document.querySelector('#run-chart'),await loadChartData([id]),{benchmark,profile,"
    "singleProfile:true})}catch(error){document.querySelector('#run-chart').innerHTML=displayError(error)}}\n"
    "  }catch(error){app.innerHTML=shell('runs',breadcrumbs([{route:'runs',label:'Runs'},{route:'run/'+enc(id),label:id}])+di"
    'splayError(error))}\n'
    '}\n'
    'function affinityPath(mode){if(mode===\'none\')return [\'No pinning\'];const parts=mode.split(\'-\'),result=[],labels={num'
    "a:'NUMA',chiplet:'Chiplet',core:'Core'};for(let index=0;index<parts.length;index+=2)result.push((labels[parts[index+1]]||parts[index+1])+'"
    ": '+parts[index]);return result}\n"
    'function affinityTree(items){const root={children:new Map};for(const item of items){let node=root;for(const label of affi'
    'nityPath(item.mode)){if(!node.children.has(label))node.children.set(label,{children:new Map,item:null});node=node.childr'
    "en.get(label)}node.item=item}const render=node=>'<ul class=affinity-tree>'+[...node.children.entries()].map(([label,chi"
    "ld])=>{const item=child.item,unavailable=item&&!item.supported;return '<li><div class=\"affinity-node '+(unavailable?'af"
    "finity-unavailable':'')+'\"><strong>'+esc(label)+'</strong>'+(item?'<code>'+esc(item.mode)+'</code>':'')+(unavailable?"
    "'<span class=availability-badge>Unavailable</span><span class=affinity-reason>'+esc(item.reason||'Not supported by thi"
    "s topology.')+'</span>':'')+'</div>'+(child.ch"
    "ildren.size?render(child):'')+'</li>'}).join('')+'</ul>';return render(root)}\n"
    'async function renderTopology(){\n'
    '  clearRefresh();\n'
    '  try{\n'
    "    const value=await api('/api/system-topology'),topology=value.topology;\n"
    '    const chipletsByNode=new Map,coreIndex=new Map,siblingsByCpu=new Map;\n'
    '    for(const chiplet of topology.chiplets)chipletsByNode.set(chiplet.numa_node,[...(chipletsByNode.get(chiplet.numa_nod'
    'e)||[]),chiplet]);\n'
    '    topology.physical_cores.forEach((cpus,index)=>cpus.forEach(cpu=>coreIndex.set(cpu,{index,cpus})));for(const siblings '
    'of topology.smt_siblings)for(const cpu of siblings)siblingsByCpu.set(cpu,siblings);\n'
    '    const coresFor=cpus=>{const allowed=new Set(cpus),seen=new Set,result=[];for(const cpu of cpus){const core=coreIndex.'
    'get(cpu);if(!core||seen.has(core.index))continue;seen.add(core.index);const visible=core.cpus.filter(item=>allowed.has(it'
    'em)),siblings=[...new Set(visible.flatMap(item=>siblingsByCpu.get(item)||[item]))].filter(item=>allowed.has(item));resu'
    'lt.push({...core,cpus:visible,siblings})}return result};\n'
    "    const coreList=cpus=>'<ul class=core-list>'+coresFor(cpus).map(core=>'<li class=core-item><strong>Core '+core.index"
    "+'</strong><span class=cpu-ranges>vCPU '+esc(cpuRanges(core.cpus))+'</span><small>'+(core.siblings.length>1?core.si"
    "blings.length+' SMT threads':'1 hardware thread')+'</small></li>').join('')+'</ul>';\n"
    '    const numaBlocks=topology.numa_nodes.map(node=>{\n'
    '      const chiplets=chipletsByNode.get(node.id)||[];\n'
    "      const children=chiplets.length?chiplets.map((chiplet,index)=>'<li><div class=topology-node><div class=topology-no"
    "de-header><strong>'+esc(chiplet.label||'L3 / chiplet '+(index+1))+'</strong><span class=cpu-ranges>CPU '+esc(cpuRanges(chiplet.cpus))+'</span></"
    "div>'+coreList(chiplet.cpus)+'</div></li>').join(''):'<li><div class=topology-node>'+coreList(node.cpus)+'</div></li>';"
    "return '<article class=numa-block><div class=numa-header><strong>NUMA '+esc(node.id)+'</strong><small class=muted>'+node"
    ".cpus.length+' CPUs</small></div><div class=cpu-ranges>CPU '+esc(cpuRanges(node.cpus))+'</div><ul class=topology-tree>'"
    "+children+'</ul></article>'\n"
    "    }).join('');\n"
    "    let content='<h1 class=page-title>System topology</h1><p class=muted>Only CPUs allowed by this process cpuset are sh"
    'own. Unsupported modes are never silently substituted.</p><section class="card topology-summary"><div><div class=metric>'
    "'+topology.allowed_cpus.length+' allowed CPUs</div><div class=muted>Compressed CPU ranges</div></div><div class=cpu-rang"
    "es>'+esc(cpuRanges(topology.allowed_cpus))+'</div></section><section class=card><h2>NUMA, cache and cores</h2><p cla"
    "ss=muted>Physical cores include their visible SMT thread count.</p><div class=topology-map>'+numaBlocks+'</div></section"
    "><section class=card><h2>Affinity availability</h2>'+affinityTree(value.affinity)+'</section>'+(topology.hierarchy"
    "_reasons.length?'<section class=card><h2>Topology notes</h2><ul>'+topology.hierarchy_reasons.map(item=>'<li><strong>'+es"
    "c(item.level)+':</strong> '+esc(item.reason)+'</li>').join('')+'</ul></section>':'');\n"
    "    app.innerHTML=shell('topology',content);\n"
    "  }catch(error){app.innerHTML=shell('topology',displayError(error))}\n"
    '}\n'
    'async function renderComparisons(){\n'
    '  clearRefresh();\n'
    '  try{\n'
    "    const value=await api('/api/comparisons');\n"
    "    let content='<h1 class=page-title>Comparisons</h1><p class=muted>Select runs, then choose a benchmark, profile, axes"
    ' and exact affinity lines. Charts use the common X intersection and report incomplete coverage.</p><section class=card><'
    "h2>Runs</h2>'+ (value.runs.length?'<div class=series-picker>'+value.runs.map(run=>'<label><input class=compare type=chec"
    'kbox value="\'+esc(run.id)+\'" \'+(value.selected.includes(run.id)?\'checked\':\'\')+\'> \'+esc(run.id)+\' <span class=muted>(\'+es'
    "c(run.source)+')</span></label>').join('')+'</div>':'<div class=empty>No runs are available.</div>')+'<div class=toolbar"
    '><button class=primary id=save-comparisons>Update comparison</button></div></section><section class=card><h2>Comparison '
    "chart</h2><div id=comparison-chart>'+(value.selected.length?'Loading summary data…':'Select one or more runs.')+'</div><"
    "/section>';\n"
    "    app.innerHTML=shell('comparisons',content);\n"
    "    document.querySelector('#save-comparisons').onclick=async()=>{await api('/api/comparisons/selection',jsonOptions([.."
    ".document.querySelectorAll('.compare:checked')].map(input=>input.value)));renderComparisons()};\n"
    "    if(value.selected.length)try{mountChartBuilder(document.querySelector('#comparison-chart'),await loadChartData(value"
    ".selected))}catch(error){document.querySelector('#comparison-chart').innerHTML=displayError(error)}\n"
    "  }catch(error){app.innerHTML=shell('comparisons',displayError(error))}\n"
    '}\n'
    "async function compose(){const current=route();if(current==='runs')return renderRuns();if(current==='new')return renderN"
    "ew('builder');if(current==='new/yaml')return renderNew('yaml');if(current==='topology')return renderTopology();if(curren"
    "t==='comparisons')return renderComparisons();if(current.startsWith('run/')){const pieces=current.split('/');if(pieces[2]"
    "==='profile')return renderRun(pieces[1],pieces.slice(3).join('/'));return renderRun(pieces.slice(1).join('/'))}setRoute("
    "'runs')}\n"
    "addEventListener('hashchange',compose);compose();\n"
)


class _RunServiceHTTPServer(ThreadingHTTPServer):
    """Tie HTTP server teardown to the benchmark worker lifecycle."""

    def shutdown(self):
        super().shutdown()
        service = getattr(self, "service", None)
        if service is not None:
            service.shutdown()

    def server_close(self):
        service = getattr(self, "service", None)
        if service is not None:
            service.shutdown()
        super().server_close()


class _IPv6ThreadingHTTPServer(_RunServiceHTTPServer):
    address_family = socket.AF_INET6


def _utc_now():
    return datetime.now(timezone.utc).isoformat()


def _is_loopback(host):
    return host in ("localhost", "127.0.0.1", "::1")


def _manifests(output):
    root = Path(output).resolve()
    if not root.is_dir():
        raise BenchmarkError("result directory does not exist: {}".format(root))
    records = []
    for candidate in root.rglob("run.json"):
        try:
            manifest = load_manifest(candidate)
        except BenchmarkError:
            continue
        if "topology" not in manifest and "steps" not in manifest:
            continue
        records.append((str(candidate.parent.relative_to(root)) or ".", manifest))
    return sorted(records, key=lambda value: value[0])


def _run_directory(output, run_id):
    """Resolve a read-model ID without allowing a URL to escape ``output``."""
    root = Path(output).resolve()
    candidate = (root / run_id).resolve()
    if candidate == root or root not in candidate.parents or not candidate.is_dir():
        raise BenchmarkError("run not found: {}".format(run_id))
    return candidate


def _content_disposition(filename):
    fallback = "".join(
        character if character.isascii() and (character.isalnum() or character in "._-") else "_"
        for character in filename
    )
    return "attachment; filename=\"{}\"; filename*=UTF-8''{}".format(fallback or "download", quote(filename, safe=""))


def _copy_stream(source, destination):
    while True:
        chunk = source.read(_STREAM_CHUNK_SIZE)
        if not chunk:
            return
        destination.write(chunk)


def _duration_seconds(manifest):
    started, finished = manifest.get("started_at"), manifest.get("finished_at")
    if not started or not finished:
        return None
    try:
        return max(0.0, (datetime.fromisoformat(finished) - datetime.fromisoformat(started)).total_seconds())
    except ValueError:
        return None


def read_model(output):
    root = Path(output).resolve()
    result = {}
    for run_id, manifest in _manifests(output):
        run_root = root / run_id
        steps = manifest.get("steps", [])
        runs = manifest.get("runs", [])
        profile_keys = {
            (str(item.get("benchmark")), str(item.get("profile")))
            for item in steps + runs
            if item.get("benchmark") is not None and item.get("profile") is not None
        }
        result[run_id] = {
            "id": run_id,
            "status": manifest.get("status", "unknown"),
            "state": manifest.get("state", "unknown"),
            "source": (
                "imported"
                if (
                    (run_root / ".imported").is_file()
                    or manifest.get("imported")
                    or manifest.get("source") == "imported"
                    or manifest.get("origin")
                )
                else "local"
            ),
            "queued_at": manifest.get("queued_at"),
            "started_at": manifest.get("started_at"),
            "finished_at": manifest.get("finished_at"),
            "duration_seconds": _duration_seconds(manifest),
            "profiles": len(profile_keys),
            "repetitions": len(steps),
            "benchmarks": sorted(
                {str(item.get("benchmark")) for item in steps + runs if item.get("benchmark") is not None}
            ),
            "profile_names": sorted(
                {str(item.get("profile")) for item in steps + runs if item.get("profile") is not None}
            ),
            "perf": bool(manifest.get("profiler")),
            "config_path": manifest.get("config", {}).get("path")
            or ("config.yaml" if (run_root / "config.yaml").is_file() else "config snapshot"),
            "output_directory": str(run_root),
            "runs": runs,
            "steps": steps,
            "topology": manifest.get("topology"),
            "events": manifest.get("events", 0),
            "finished_steps": sum(
                1 for item in steps if item.get("state") in ("passed", "failed", "unsupported", "cancelled")
            ),
        }
    return result


def benchmark_catalog():
    """Small UI-facing registry representation, generated from adapters."""
    return [
        {
            "name": item.name,
            "description": item.description,
            "builder_supported": item.builder_supported,
            "profile_kind": item.profile_kind,
            "parameter_name": item.parameter_name,
            "parameter_description": item.parameter_description,
            "parameters": [
                {
                    "name": parameter.name,
                    "description": parameter.description,
                    "type": parameter.value_type,
                    "default": list(parameter.default),
                    "matrix": parameter.matrix,
                    "choices": list(parameter.choices),
                    "minimum": parameter.minimum,
                    "maximum": parameter.maximum,
                }
                for parameter in item.parameters
            ],
            "dimensions": [{"name": dimension.name, "series": dimension.series} for dimension in item.dimensions],
            "metrics": [{"name": metric.name, "unit": metric.unit} for metric in item.metrics],
        }
        for item in BENCHMARKS.values()
    ]


def editor_model(loaded, output):
    """Return the validated YAML as the Builder's non-lossy editable model."""
    profiles = []
    for configuration in loaded.runs:
        benchmark = configuration.benchmark
        profile = {
            "key": "{}/{}".format(benchmark.name, configuration.profile),
            "benchmark": benchmark.name,
            "name": configuration.profile,
            "threads": list(configuration.threads),
            "parameters": {},
            "duration": configuration.duration_seconds,
            "repetitions": configuration.repetitions,
            "timeout": configuration.timeout_seconds if configuration.timeout_explicit else None,
            "affinity": list(configuration.affinity_modes),
            "background_load": list(configuration.background_load_modes),
        }
        if benchmark.profile_kind == "local-ydb":
            profile["local_ydb"] = configuration.parameters["local_ydb"]
        else:
            profile["parameters"] = {name: list(values) for name, values in configuration.parameters.items()}
        profiles.append(profile)
    return {
        "output": str(Path(output).resolve()),
        "benchmarks": benchmark_catalog(),
        "affinity_modes": list(AFFINITY_MODES),
        "background_load_modes": list(BACKGROUND_LOAD_MODES),
        "profiles": profiles,
    }


def comparison_keys(model, selected):
    """Return only keys actually available under the requested comparison scope."""
    selected = [run_id for run_id in selected if run_id in model]
    per_run = []
    for run_id in selected:
        steps = model[run_id].get("steps", [])
        keys = {
            (str(s.get("benchmark")), str(s.get("profile")), str(s.get("affinity")))
            for s in steps
            if s.get("benchmark") is not None and s.get("profile") is not None and s.get("affinity") is not None
        }
        # Older completed top-level records can lack steps; retain their local
        # benchmark/profile availability but never invent an affinity.
        pairs = {
            (str(r.get("benchmark")), str(r.get("profile")))
            for r in model[run_id].get("runs", [])
            if r.get("benchmark") is not None and r.get("profile") is not None
        }
        per_run.append((keys, pairs | {(a, b) for a, b, _ in keys}))
    common_affinity = set.intersection(*(item[0] for item in per_run)) if per_run else set()
    common_pairs = set.intersection(*(item[1] for item in per_run)) if per_run else set()
    one_affinity_pairs = {
        pair for pair in common_pairs if all(len({a for x, y, a in keys if (x, y) == pair}) == 1 for keys, _ in per_run)
    }
    within_run = {run_id: sorted("/".join(pair) for pair in pairs) for run_id, (_, pairs) in zip(selected, per_run)}
    return {
        "benchmark_profile_affinity": sorted("/".join(key) for key in common_affinity),
        "benchmark_profile_one_affinity": sorted("/".join(pair) for pair in one_affinity_pairs),
        "within_run_benchmark_profile": within_run,
    }


def _summary_value(value):
    """Decode a CSV cell without losing non-numeric future dimensions."""
    try:
        number = float(value)
    except (TypeError, ValueError):
        return value
    if not math.isfinite(number):
        return value
    return int(number) if number.is_integer() else number


_MEMORY_FAIRNESS_METRICS = (
    "worker_max_min_spread_pct",
    "worker_mean_min_gap_pct",
)


def _add_memory_fairness_rows(grouped, dimension_fields):
    """Derive per-repeat worker imbalance, then aggregate those percentages."""
    key_fields = [name for name in dimension_fields if name != "worker_aggregation"] + ["repeat"]
    aggregate_key_fields = [name for name in key_fields if name != "repeat"]
    derived_count = 0
    for rows in grouped.values():
        raw_groups = {}
        for row in rows:
            if row.get("repeat_aggregation") != "raw" or row.get("scope") not in ("sequential", "random"):
                continue
            key = tuple(row.get(name) for name in key_fields)
            raw_groups.setdefault(key, {})[row.get("worker_aggregation")] = row
        derived = []
        for values in raw_groups.values():
            if not all(name in values for name in ("min", "max", "mean")):
                continue
            minimum = values["min"].get("ops_per_sec")
            maximum = values["max"].get("ops_per_sec")
            mean = values["mean"].get("ops_per_sec")
            if not all(isinstance(value, (int, float)) and math.isfinite(value) for value in (minimum, maximum, mean)):
                continue
            if mean == 0:
                continue
            derived.append(
                {
                    **{name: values["mean"].get(name) for name in dimension_fields},
                    "worker_aggregation": "fairness",
                    "repeat_aggregation": "raw",
                    "repeat": values["mean"].get("repeat"),
                    _MEMORY_FAIRNESS_METRICS[0]: (maximum - minimum) / mean * 100,
                    _MEMORY_FAIRNESS_METRICS[1]: (mean - minimum) / mean * 100,
                }
            )
        rows.extend(derived)
        derived_count += len(derived)
        aggregate_groups = {}
        for row in derived:
            key = tuple(row.get(name) for name in aggregate_key_fields)
            aggregate_groups.setdefault(key, []).append(row)
        aggregators = {
            "median": statistics.median,
            "mean": statistics.mean,
            "min": min,
            "max": max,
        }
        for key, repetitions in aggregate_groups.items():
            base = dict(zip(aggregate_key_fields, key))
            for name, aggregate in aggregators.items():
                rows.append(
                    {
                        **base,
                        "worker_aggregation": "fairness",
                        "repeat_aggregation": name,
                        "repeat": "*",
                        **{
                            metric: aggregate([row[metric] for row in repetitions])
                            for metric in _MEMORY_FAIRNESS_METRICS
                        },
                    }
                )
    return derived_count


def chart_data(output, run_ids):
    """Read bounded profile summaries into UI-facing affinity series."""
    if not isinstance(run_ids, list) or not run_ids or len(run_ids) > 20:
        raise BenchmarkError("charts require between 1 and 20 run ids")
    result = []
    dimensions, metrics, metric_metadata, dimension_metadata = set(), set(), {}, {}
    for run_id in run_ids:
        root = _run_directory(output, run_id)
        for path in sorted(root.glob("*/*/summary.csv")):
            if path.stat().st_size > 16 * 1024 * 1024:
                raise BenchmarkError("summary CSV is too large: {}".format(path.relative_to(root)))
            affinity_cpus = {}
            affinity_cpu_masks = {}
            profile_manifest_path = path.parent / "run.json"
            if profile_manifest_path.is_file():
                try:
                    profile_manifest = json.loads(profile_manifest_path.read_text(encoding="utf-8"))
                    for item in profile_manifest.get("affinity", []):
                        if not isinstance(item, dict) or not isinstance(item.get("mode"), str):
                            continue
                        if isinstance(item.get("threads"), int):
                            affinity_cpu_masks.setdefault(item["mode"], {})[str(item["threads"])] = item.get("cpus")
                        else:
                            affinity_cpus[item["mode"]] = item.get("cpus")
                except (OSError, ValueError, TypeError):
                    affinity_cpus = {}
                    affinity_cpu_masks = {}
            with path.open(newline="", encoding="utf-8") as stream:
                reader = csv.DictReader(stream)
                fields = [name for name in (reader.fieldnames or []) if isinstance(name, str) and name]
                if "affinity_mode" not in fields:
                    continue
                benchmark_name = path.relative_to(root).parts[0]
                benchmark_definition = BENCHMARKS.get(benchmark_name) if benchmark_name in BENCHMARKS else None
                normalized_repetitions = benchmark_name == "memory-bandwidth-bench"
                has_memory_fairness = False
                prefixes = ("median_", "mean_", "min_", "max_")
                metric_fields = [name for name in fields if name.startswith(prefixes)]
                dimension_fields = [
                    name
                    for name in fields
                    if name not in metric_fields and name not in ("affinity_mode", "repetitions")
                ]
                grouped = {}
                for index, row in enumerate(reader):
                    if index >= 100000:
                        raise BenchmarkError("summary CSV has too many rows: {}".format(path.relative_to(root)))
                    affinity = row.get("affinity_mode")
                    if not affinity:
                        continue
                    if normalized_repetitions:
                        base = {name: _summary_value(row.get(name)) for name in dimension_fields}
                        for aggregation in ("median", "mean", "min", "max"):
                            values = {
                                metric.name: _summary_value(row.get(aggregation + "_" + metric.name))
                                for metric in benchmark_definition.metrics
                            }
                            grouped.setdefault(affinity, []).append(
                                {**base, "repeat_aggregation": aggregation, "repeat": "*", **values}
                            )
                    else:
                        grouped.setdefault(affinity, []).append(
                            {name: _summary_value(row.get(name)) for name in fields}
                        )
                if normalized_repetitions:
                    repetitions_path = path.with_name("repetitions.csv")
                    if repetitions_path.is_file():
                        with repetitions_path.open(newline="", encoding="utf-8") as repetitions_stream:
                            for row in csv.DictReader(repetitions_stream):
                                affinity = row.get("affinity_mode")
                                if affinity:
                                    grouped.setdefault(affinity, []).append(
                                        {
                                            name: _summary_value(row.get(name))
                                            for name in dimension_fields
                                            + ["repeat"]
                                            + [metric.name for metric in benchmark_definition.metrics]
                                        }
                                        | {"repeat_aggregation": "raw"}
                                    )
                    has_memory_fairness = bool(_add_memory_fairness_rows(grouped, dimension_fields))
                    dimension_fields += ["repeat_aggregation", "repeat"]
                    metric_fields = [metric.name for metric in benchmark_definition.metrics]
                    if has_memory_fairness:
                        metric_fields += list(_MEMORY_FAIRNESS_METRICS)
                dimensions.update(dimension_fields)
                metrics.update(metric_fields)
                if benchmark_definition is not None:
                    for dimension in benchmark_definition.dimensions:
                        dimension_metadata[dimension.name] = {"series": dimension.series}
                    for metric in benchmark_definition.metrics:
                        if normalized_repetitions:
                            metric_metadata[metric.name] = {"unit": metric.unit, "description": metric.description}
                        else:
                            for prefix in ("median_", "min_", "max_"):
                                metric_metadata[prefix + metric.name] = {
                                    "unit": metric.unit,
                                    "description": metric.description,
                                }
                    if has_memory_fairness:
                        metric_metadata.update(
                            {
                                _MEMORY_FAIRNESS_METRICS[0]: {
                                    "unit": "%",
                                    "description": "Worker max-minus-min spread as a percentage of the mean.",
                                },
                                _MEMORY_FAIRNESS_METRICS[1]: {
                                    "unit": "%",
                                    "description": "Slowest worker gap from the mean as a percentage of the mean.",
                                },
                            }
                        )
                for affinity, rows in grouped.items():
                    benchmark, profile = path.relative_to(root).parts[:2]
                    series = {
                        "id": "{}/{}/{}/{}".format(run_id, benchmark, profile, affinity),
                        "run": run_id,
                        "benchmark": benchmark,
                        "profile": profile,
                        "affinity": affinity,
                        "rows": rows,
                    }
                    if affinity in affinity_cpus:
                        series["cpus"] = affinity_cpus[affinity]
                    if affinity in affinity_cpu_masks:
                        series["cpu_masks"] = affinity_cpu_masks[affinity]
                    result.append(series)
    return {
        "series": result,
        "dimensions": sorted(dimensions),
        "metrics": sorted(metrics),
        "metric_metadata": metric_metadata,
        "dimension_metadata": dimension_metadata,
    }


def _load_yaml(yaml_text):
    """Use the CLI parser/validator without allocating a result directory."""
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", suffix=".yaml", delete=False) as stream:
        stream.write(yaml_text)
        path = Path(stream.name)
    try:
        return load_config(path)
    finally:
        path.unlink(missing_ok=True)


class RunService:
    """Own running jobs, bounded live tails, and durable event replay.

    ``executor`` is an adapter callable ``(run, emit, cancelled)``. It may emit
    ``step-started``, ``step-finished``, ``stdout``, ``stderr`` and arbitrary
    progress dictionaries. This small boundary makes web integration testable
    without a real benchmark binary.
    """

    def __init__(self, output, executor=None, event_limit=256, tail_limit=65536, perf_available=True):
        self.output = Path(output).resolve()
        self.output.mkdir(parents=True, exist_ok=True)
        self.executor = executor or self._unsupported_executor
        self.event_limit, self.tail_limit = event_limit, tail_limit
        self.perf_available = perf_available
        self._runs, self._lock = {}, threading.RLock()
        self._accepting_runs = True
        self._queue = deque()
        self._active_run_id = None
        self._dispatcher_thread = None
        self._selection_path = self.output / ".comparison-selection.json"
        self._recover()

    def _recover(self):
        for run_id, manifest in _manifests(self.output):
            # A process may still be live after a server restart. Never restart
            # it without an adapter-specific proof that it is gone.
            if manifest.get("state") == "running":
                manifest["status"] = "recovery_required"
                manifest["state"] = "recovery_required"
                atomic_write_json(self.output / run_id / "run.json", manifest)

    def _load(self, yaml_text, perf=False):
        if perf and not self.perf_available:
            raise BenchmarkError("--perf requires ydb_bench built with --build=profile")
        with tempfile.NamedTemporaryFile("w", encoding="utf-8", suffix=".yaml", delete=False) as stream:
            stream.write(yaml_text)
            path = Path(stream.name)
        try:
            return load_config(path, perf_enabled=perf)
        finally:
            path.unlink(missing_ok=True)

    def validate(self, yaml_text, perf=False):
        try:
            loaded = self._load(yaml_text, perf)
        except BenchmarkError as error:
            return {"valid": False, "error": str(error)}
        return {
            "valid": True,
            "sha256": hashlib.sha256(yaml_text.encode()).hexdigest(),
            "steps": len(build_run_plan(loaded).steps),
        }

    def plan(self, yaml_text, perf=False):
        validation = self.validate(yaml_text, perf)
        if not validation["valid"]:
            return validation
        plan = build_run_plan(self._load(yaml_text, perf))
        validation["plan"] = [
            {
                "id": s.id,
                "benchmark": s.benchmark,
                "profile": s.profile,
                "affinity": s.affinity,
                "background_load": s.background_load,
                "threads": s.threads,
                "case": s.case,
                "parameters": s.parameters,
                "repeat": s.repeat,
            }
            for s in plan.steps
        ]
        return validation

    def editor_config(self, yaml_text, perf=False):
        if not yaml_text.strip():
            return {
                "output": str(self.output),
                "benchmarks": benchmark_catalog(),
                "affinity_modes": list(AFFINITY_MODES),
                "background_load_modes": list(BACKGROUND_LOAD_MODES),
                "profiles": [],
            }
        loaded = self._load(yaml_text, perf)
        return editor_model(loaded, self.output)

    def start(self, yaml_text, perf=False, continue_on_error=False):
        with self._lock:
            if not self._accepting_runs:
                raise BenchmarkError("web run service is shutting down")
        plan_result = self.plan(yaml_text, perf)
        if not plan_result["valid"]:
            raise BenchmarkError(plan_result["error"])
        loaded = self._load(yaml_text, perf)
        topology = topology_record(discover_topology())
        with self._lock:
            # This lock is also the start-vs-shutdown publication boundary.  A
            # run is either rejected without creating files, or is registered
            # with its worker before shutdown takes its active-run snapshot.
            if not self._accepting_runs:
                raise BenchmarkError("web run service is shutting down")
            run_id = "{}-web".format(datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"))
            while (self.output / run_id).exists():
                run_id = "{}-{}".format(run_id, uuid.uuid4().hex[:6])
            root = self.output / run_id
            root.mkdir()
            atomic_write_text(root / "config.yaml", yaml_text)
            queued_at = _utc_now()
            manifest = {
                "schema_version": 4,
                "status": "queued",
                "state": "queued",
                "queued_at": queued_at,
                "config": {"snapshot": yaml_text, "sha256": plan_result["sha256"], "path": "config.yaml"},
                "topology": topology,
                "profiler": (
                    {"type": "perf-record", "event": "cycles:u", "frequency_hz": 99, "call_graph": "dwarf"}
                    if perf
                    else None
                ),
                "options": {"perf": perf, "continue_on_error": bool(continue_on_error)},
                "runs": [],
                "steps": [dict(item, state="pending", artifacts=[]) for item in plan_result["plan"]],
                "events": 0,
            }
            run = {
                "id": run_id,
                "root": root,
                "loaded": loaded,
                "store": ResultStore(root / "run.json", manifest),
                "events": deque(maxlen=self.event_limit),
                "tail": {"stdout": "", "stderr": ""},
                "cancel": threading.Event(),
                "cancel_requested": False,
                "finished": threading.Event(),
                "finalized": False,
                "lock": threading.RLock(),
                "continue_on_error": bool(continue_on_error),
                "failed": False,
            }
            run["store"].write()
            self._runs[run_id] = run
            self._queue.append(run)
            self._ensure_dispatcher_locked()
        return {"id": run_id, "state": "queued"}

    def _ensure_dispatcher_locked(self):
        if self._dispatcher_thread is not None:
            return
        self._dispatcher_thread = threading.Thread(
            target=self._dispatch,
            daemon=True,
            name="ydb-bench-web-queue",
        )
        self._dispatcher_thread.start()

    def _dispatch(self):
        while True:
            with self._lock:
                while self._queue:
                    run = self._queue.popleft()
                    with run["lock"]:
                        if run["store"].manifest["state"] != "queued":
                            continue
                        self._active_run_id = run["id"]
                        run["store"].manifest.update(
                            {
                                "state": "running",
                                "status": "running",
                                "started_at": _utc_now(),
                            }
                        )
                        self._emit_locked(run, {"type": "run-started"})
                    break
                else:
                    self._active_run_id = None
                    self._dispatcher_thread = None
                    return
            self._run(run)
            with self._lock:
                if self._active_run_id == run["id"]:
                    self._active_run_id = None

    def _emit(self, run, event):
        with run["lock"]:
            if run["finalized"]:
                return
            self._emit_locked(run, event)

    def _emit_locked(self, run, event):
        event = dict(event)
        event["sequence"] = run["store"].manifest.get("events", 0) + 1
        event["at"] = _utc_now()
        if event.get("type") in ("stdout", "stderr"):
            key = event["type"]
            run["tail"][key] = (run["tail"][key] + str(event.get("data", "")))[-self.tail_limit :]
        step_id = event.get("step_id")
        if event.get("type") == "step-started" and step_id:
            run["store"].transition_step(step_id, "running", **event.get("fields", {}))
        if event.get("type") == "step-progress" and step_id:
            run["store"].update_step(step_id, **event.get("fields", {}))
        if event.get("type") == "step-artifacts" and step_id:
            run["store"].add_artifacts(step_id, event.get("artifacts", []))
            for artifact in event.get("artifacts", []):
                if str(artifact).endswith(("stdout.txt", "stderr.txt")):
                    key = "stdout" if str(artifact).endswith("stdout.txt") else "stderr"
                    try:
                        run["tail"][key] = (run["tail"][key] + (run["root"] / artifact).read_text(encoding="utf-8"))[
                            -self.tail_limit :
                        ]
                    except OSError:
                        pass
        if event.get("type") == "step-finished" and step_id:
            run["store"].transition_step(step_id, event.get("state", "passed"), **event.get("fields", {}))
        run["events"].append(event)
        run["store"].manifest["events"] = event["sequence"]
        with (run["root"] / "events.jsonl").open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(event, sort_keys=True) + "\n")
        run["store"].write()

    def _unsupported_executor(self, run, emit, cancelled):
        raise BenchmarkError("web execution adapter is not configured")

    @staticmethod
    def _cancel_unfinished(run):
        for step in list(run["store"].manifest["steps"]):
            if step["state"] in ("pending", "running"):
                run["store"].transition_step(step["id"], "cancelled")

    def _run(self, run):
        error = None
        try:
            self.executor(run, lambda event: self._emit(run, event), run["cancel"])
        except Exception as caught:
            error = caught
        try:
            with run["lock"]:
                if not run["finalized"]:
                    self._finalize_locked(run, error)
        finally:
            run["finished"].set()

    def _finalize_locked(self, run, error=None):
        if run["cancel"].is_set():
            self._cancel_unfinished(run)
            state, status = "cancelled", "cancelled"
        elif error is not None:
            self._cancel_unfinished(run)
            state, status = "failed", "failed"
        elif run["failed"]:
            self._cancel_unfinished(run)
            state, status = "failed", "failed"
        elif run["store"].manifest["runs"] and all(
            profile.get("status") == "unsupported" for profile in run["store"].manifest["runs"]
        ):
            state, status = "unsupported", "unsupported"
        else:
            # An executor is not allowed to report a completed run with a
            # hidden pending step.  Keep the durable queue terminal even for a
            # faulty adapter, then make the invariant visible.
            pending = [step for step in run["store"].manifest["steps"] if step["state"] in ("pending", "running")]
            if pending:
                self._cancel_unfinished(run)
                state, status = "failed", "failed"
                run["store"].manifest["error"] = "executor returned with unfinished run steps"
            else:
                state, status = "passed", "completed"
        if error is not None:
            run["store"].manifest["error"] = str(error)
        run["store"].manifest.update({"state": state, "status": status, "finished_at": _utc_now()})
        self._emit_locked(run, {"type": "run-finished", "state": state})
        run["finalized"] = True
        run["finished"].set()

    def cancel(self, run_id):
        with self._lock:
            run = self._runs.get(run_id)
            if not run:
                return {"id": run_id, "cancelled": True, "state": "not-running"}
            with run["lock"]:
                state = run["store"].manifest["state"]
                if state == "queued":
                    run["cancel_requested"] = True
                    run["cancel"].set()
                    try:
                        self._queue.remove(run)
                    except ValueError:
                        pass
                    self._cancel_unfinished(run)
                    run["store"].manifest.update(
                        {
                            "state": "cancelled",
                            "status": "cancelled",
                            "finished_at": _utc_now(),
                        }
                    )
                    self._emit_locked(run, {"type": "cancel-requested"})
                    self._emit_locked(run, {"type": "run-finished", "state": "cancelled"})
                    run["finalized"] = True
                    run["finished"].set()
                elif state == "running" and not run["cancel_requested"]:
                    run["cancel_requested"] = True
                    run["cancel"].set()
                    self._emit_locked(run, {"type": "cancel-requested"})
                return {"id": run_id, "cancelled": True, "state": run["store"].manifest["state"]}

    def shutdown(self, timeout=None):
        """Stop accepting runs, cancel the queue, and wait for the dispatcher.

        Production teardown uses the default unbounded wait: its executors pass
        the cancellation event into ``run_command``, which interrupts and,
        after its own grace period, kills the benchmark process group.  A
        diagnostic caller may supply one shared timeout.  Such a timeout is
        only reported to the caller; the still-running manifest deliberately
        remains nonterminal and a later call can continue waiting.
        """
        if timeout is not None:
            timeout = max(0.0, float(timeout))
        with self._lock:
            self._accepting_runs = False
            runs = list(self._runs.values())
            dispatcher = self._dispatcher_thread
        for run in runs:
            self.cancel(run["id"])
        if dispatcher is not None:
            dispatcher.join(timeout)
        timed_out = []
        if dispatcher is not None and dispatcher.is_alive():
            with self._lock:
                if self._active_run_id is not None:
                    timed_out.append(self._active_run_id)
        return {"cancelled": [run["id"] for run in runs], "timed_out": timed_out}

    def model(self):
        model = read_model(self.output)
        with self._lock:
            positions = {
                run["id"]: index
                for index, run in enumerate(
                    (queued for queued in self._queue if queued["store"].manifest["state"] == "queued"),
                    1,
                )
            }
            for run_id, item in model.items():
                item.update(
                    {
                        "current_run_id": self._active_run_id,
                        "queue_position": positions.get(run_id),
                    }
                )
        return model

    def settings(self):
        return {"output": str(self.output), "perf_available": self.perf_available}

    def topology(self):
        topology = discover_topology()
        return {
            "topology": topology_record(topology),
            "affinity": [
                {
                    "mode": mode,
                    "supported": (placement := plan_affinity(mode, topology, 1)).supported,
                    "cpus": None if placement.cpus is None else list(placement.cpus),
                    "reason": placement.reason,
                }
                for mode in AFFINITY_MODES
            ],
        }

    def filtered_model(self, filters):
        def matches(record):
            if filters.get("status") and record["status"] != filters["status"]:
                return False
            if filters.get("source") and record["source"] != filters["source"]:
                return False
            if filters.get("benchmark") and filters["benchmark"] not in record["benchmarks"]:
                return False
            if filters.get("profile") and filters["profile"] not in record["profile_names"]:
                return False
            started = record.get("started_at") or ""
            if filters.get("since") and started[:10] < filters["since"]:
                return False
            if filters.get("until") and started[:10] > filters["until"]:
                return False
            return True

        records = [record for record in self.model().values() if matches(record)]
        return sorted(
            records,
            key=lambda record: (
                record.get("queued_at") or record.get("started_at") or record.get("finished_at") or "",
                record["id"],
            ),
            reverse=True,
        )

    def save_draft(self, yaml_text):
        # Store only generated IDs under the configured result root; the API
        # never accepts a host pathname supplied by the browser.
        draft_id = "{}-{}.yaml".format(datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"), uuid.uuid4().hex[:6])
        path = self.output / "drafts" / draft_id
        atomic_write_text(path, yaml_text)
        return {"id": draft_id, "path": str(path)}

    def run_config(self, run_id):
        root = _run_directory(self.output, run_id)
        manifest = load_manifest(root / "run.json")
        path = root / "config.yaml"
        if path.is_file():
            yaml_text = path.read_text(encoding="utf-8")
        else:
            yaml_text = manifest.get("config", {}).get("snapshot")
        if not isinstance(yaml_text, str):
            raise BenchmarkError("run does not contain a YAML configuration")
        options = manifest.get("options", {})
        return {
            "yaml": yaml_text,
            "perf": bool(options.get("perf", manifest.get("profiler"))),
            "continue_on_error": bool(options.get("continue_on_error", False)),
        }

    def artifact(self, run_id, relative_path):
        root = _run_directory(self.output, run_id)
        candidate = (root / relative_path).resolve()
        if candidate == root or root not in candidate.parents or not candidate.is_file() or candidate.is_symlink():
            raise BenchmarkError("artifact not found: {}".format(relative_path))
        return candidate

    def archive(self, run_id):
        return export_archive(_run_directory(self.output, run_id))

    def chart_data(self, run_ids):
        return chart_data(self.output, run_ids)

    def local_ydb_profile(self, run_id, profile):
        root = _run_directory(self.output, run_id)
        manifest = load_manifest(root / "run.json")
        record = next(
            (
                item
                for item in manifest.get("runs", [])
                if item.get("benchmark") == "local-ydb" and item.get("profile") == profile
            ),
            None,
        )
        if record is None:
            if any(
                item.get("benchmark") == "local-ydb" and item.get("profile") == profile
                for item in manifest.get("steps", [])
            ):
                return {
                    "benchmark": "local-ydb",
                    "profile": profile,
                    "status": "preparing",
                    "state": "preparing",
                }
            raise BenchmarkError("local-ydb profile not found: {}".format(profile))
        relative = record.get("manifest") or str(Path(record.get("directory", "")) / "run.json")
        unresolved = root / relative
        candidate = unresolved.resolve()
        if candidate == root or root not in candidate.parents or unresolved.is_symlink():
            raise BenchmarkError("local-ydb profile manifest escapes the run directory")
        if not candidate.is_file():
            return {
                "benchmark": "local-ydb",
                "profile": profile,
                "status": record.get("status", "preparing"),
                "state": "preparing",
            }
        if candidate.stat().st_size > 16 * 1024 * 1024:
            raise BenchmarkError("local-ydb profile manifest is too large")
        value = load_manifest(candidate)
        fields = (
            "schema_version",
            "benchmark",
            "profile",
            "status",
            "state",
            "started_at",
            "finished_at",
            "parameters",
            "timeout_seconds",
            "role_affinity",
            "progress",
            "attempts",
            "searches",
            "result",
            "error",
        )
        return {name: value[name] for name in fields if name in value}

    def comparisons(self, selected=None):
        model = self.model()
        if selected is None:
            try:
                selected = json.loads(self._selection_path.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                selected = []
        selected = [item for item in selected if isinstance(item, str) and item in model]
        return {
            "runs": [{"id": item["id"], "source": item["source"]} for item in model.values()],
            "selected": selected,
            "keys": comparison_keys(model, selected),
        }

    def select_comparisons(self, selected):
        if not isinstance(selected, list) or not all(isinstance(item, str) for item in selected):
            raise BenchmarkError("comparison selection must be a list of run ids")
        atomic_write_json(self._selection_path, selected)
        return self.comparisons(selected)

    def detail(self, run_id):
        item = self.model().get(run_id)
        with self._lock:
            run = self._runs.get(run_id)
            if item and run:
                with run["lock"]:
                    item.update({"tail": dict(run["tail"])})
        return item

    def events(self, run_id, after=0):
        with self._lock:
            run = self._runs.get(run_id)
        if run:
            with run["lock"]:
                return [dict(e) for e in run["events"] if e["sequence"] > after]
        path = _run_directory(self.output, run_id) / "events.jsonl"
        if not path.is_file():
            return []
        events = []
        for line in path.read_text(encoding="utf-8").splitlines():
            event = json.loads(line)
            if event["sequence"] > after:
                events.append(event)
        return events


def production_executor(resource_loader, tool_revision):
    """Adapt the existing actors-core executor to the durable web service."""

    def execute(run, emit, cancelled):
        if resource_loader is None:
            raise BenchmarkError("the benchmark executable resource loader is not configured")
        with tempfile.TemporaryDirectory(prefix="ydb-bench-web-") as work:
            binaries = {}
            background_binary = None
            if any("none" != mode for config in run["loaded"].runs for mode in config.background_load_modes):
                background_binary = extract_executable(resource_loader("background_load"), work, "background_load")
            for configuration in run["loaded"].runs:
                profile_binaries = {}
                for resource_name in configuration.benchmark.resources:
                    if resource_name not in binaries:
                        binaries[resource_name] = extract_executable(
                            resource_loader(resource_name), work, resource_name
                        )
                    profile_binaries[resource_name] = binaries[resource_name]
                binary = profile_binaries[configuration.benchmark.resource_name]
                if cancelled.is_set():
                    return
                relative = Path(configuration.benchmark.name) / configuration.profile
                directory = run["root"] / relative
                directory.mkdir(parents=True, exist_ok=True)
                with run["lock"]:
                    if run["finalized"]:
                        return
                    run["store"].manifest["runs"].append(
                        {
                            "benchmark": configuration.benchmark.name,
                            "profile": configuration.profile,
                            "status": "running",
                            "directory": str(relative),
                        }
                    )
                    run["store"].write()

                def event(event):
                    item = dict(event)
                    with run["lock"]:
                        if "affinity" in item:
                            item["step_id"] = next(
                                step["id"]
                                for step in run["store"].manifest["steps"]
                                if step["benchmark"] == configuration.benchmark.name
                                and step["profile"] == configuration.profile
                                and step["affinity"] == item["affinity"]
                                and step.get("background_load", "none") == item.get("background_load", "none")
                                and step["threads"] == item["threads"]
                                and step["case"] == item["case"]
                                and step["repeat"] == item["repeat"]
                            )
                        if item.get("type") == "step-artifacts":
                            item["artifacts"] = [str(relative / artifact) for artifact in item["artifacts"]]
                        emit(item)

                try:
                    if configuration.benchmark.executor == "local-ydb":
                        profile = run_local_ydb(
                            profile_binaries,
                            configuration,
                            directory,
                            tool_revision,
                            work_dir_hint=work,
                            event_sink=event,
                            cancel_event=cancelled,
                        )
                    else:
                        profile = run_benchmark(
                            binary,
                            configuration,
                            directory,
                            tool_revision,
                            work_dir_hint=work,
                            event_sink=event,
                            cancel_event=cancelled,
                            background_binary=background_binary,
                        )
                except BenchmarkInterrupted:
                    with run["lock"]:
                        if run["finalized"]:
                            return
                        run["store"].manifest["runs"][-1].update({"status": "cancelled"})
                        run["store"].write()
                    raise
                except BenchmarkError as error:
                    with run["lock"]:
                        if run["finalized"]:
                            return
                        run["store"].manifest["runs"][-1].update(
                            {"status": "failed", "error": str(error), "manifest": str(relative / "run.json")}
                        )
                        # The actor benchmark stops after its first failed process.
                        # The durable queue still records every remaining member of
                        # this profile as terminal before the next profile starts.
                        for step in list(run["store"].manifest["steps"]):
                            if (
                                step["benchmark"] == configuration.benchmark.name
                                and step["profile"] == configuration.profile
                                and step["state"] == "pending"
                            ):
                                emit(
                                    {
                                        "type": "step-finished",
                                        "step_id": step["id"],
                                        "state": "cancelled",
                                        "fields": {"reason": "profile stopped after failure"},
                                    }
                                )
                        run["store"].write()
                    if not run["continue_on_error"]:
                        raise
                    with run["lock"]:
                        run["failed"] = True
                    continue
                with run["lock"]:
                    if run["finalized"]:
                        return
                    run["store"].manifest["runs"][-1].update(
                        {
                            "status": profile.get("status", "completed"),
                            "manifest": str(relative / "run.json"),
                            "summary": str(relative / profile["summary"]),
                        }
                    )
                    run["store"].write()

    return execute


def _handler(service):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_args):
            pass

        def _send(self, status, content_type, body, headers=None):
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Security-Policy", _CSP)
            self.send_header("X-Content-Type-Options", "nosniff")
            self.send_header("Content-Length", str(len(body)))
            for name, value in (headers or {}).items():
                self.send_header(name, value)
            self.end_headers()
            self.wfile.write(body)

        def _json(self, status, value):
            self._send(status, "application/json", json.dumps(value).encode())

        def _attachment(self, content_type, filename, body):
            self._send(200, content_type, body, {"Content-Disposition": _content_disposition(filename)})

        def _file_attachment(self, content_type, filename, path):
            self.send_response(200)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Security-Policy", _CSP)
            self.send_header("X-Content-Type-Options", "nosniff")
            self.send_header("Content-Length", str(path.stat().st_size))
            self.send_header("Content-Disposition", _content_disposition(filename))
            self.end_headers()
            with path.open("rb") as stream:
                _copy_stream(stream, self.wfile)

        def _raw_body(self):
            try:
                size = int(self.headers.get("Content-Length", 0))
            except ValueError:
                raise BenchmarkError("invalid Content-Length")
            if size < 0 or size > MAX_TOTAL_SIZE:
                raise BenchmarkError("request exceeds import size limit")
            return self.rfile.read(size)

        def _body(self):
            try:
                return self._raw_body().decode("utf-8")
            except UnicodeDecodeError as error:
                raise BenchmarkError("request body must be UTF-8") from error

        def _options(self):
            body = self._body()
            if self.headers.get("Content-Type", "").split(";", 1)[0].lower() != "application/json":
                return {"yaml": body, "perf": False, "continue_on_error": False}
            try:
                value = json.loads(body)
            except ValueError as error:
                raise BenchmarkError("malformed JSON request") from error
            if not isinstance(value, dict) or not isinstance(value.get("yaml"), str):
                raise BenchmarkError("request must contain a YAML string")
            if not isinstance(value.get("perf", False), bool) or not isinstance(
                value.get("continue_on_error", False), bool
            ):
                raise BenchmarkError("perf and continue_on_error must be booleans")
            return {
                "yaml": value["yaml"],
                "perf": value.get("perf", False),
                "continue_on_error": value.get("continue_on_error", False),
            }

        def _json_body(self):
            try:
                value = json.loads(self._body())
            except ValueError as error:
                raise BenchmarkError("malformed JSON request") from error
            return value

        def do_GET(self):
            parsed = urlparse(self.path)
            path = parsed.path
            if path == "/":
                return self._send(200, "text/html; charset=utf-8", _HTML.encode())
            if path == "/app.css":
                return self._send(200, "text/css; charset=utf-8", _CSS.encode())
            if path == "/app.js":
                return self._send(200, "application/javascript; charset=utf-8", _JS.encode())
            if path == "/api/settings":
                return self._json(200, service.settings())
            if path == "/api/benchmarks":
                return self._json(200, benchmark_catalog())
            if path == "/api/system-topology":
                return self._json(200, service.topology())
            if path == "/api/runs":
                filters = {
                    name: values[-1]
                    for name, values in parse_qs(parsed.query).items()
                    if name in ("status", "benchmark", "profile", "source", "since", "until")
                }
                fields = (
                    "id",
                    "status",
                    "state",
                    "source",
                    "queued_at",
                    "started_at",
                    "finished_at",
                    "duration_seconds",
                    "profiles",
                    "repetitions",
                    "perf",
                    "config_path",
                    "output_directory",
                    "benchmarks",
                    "profile_names",
                    "current_run_id",
                    "queue_position",
                )
                return self._json(200, [{key: item[key] for key in fields} for item in service.filtered_model(filters)])
            if path == "/api/comparisons":
                return self._json(200, service.comparisons())
            if path == "/api/chart-data":
                return self._json(200, service.chart_data(parse_qs(parsed.query).get("run", [])))
            if path.startswith("/api/runs/") and path.endswith("/local-ydb-profile"):
                run_id = unquote(path[len("/api/runs/") : -len("/local-ydb-profile")])
                profile = parse_qs(parsed.query).get("profile", [""])[-1]
                if not profile:
                    return self._json(400, {"error": "local-ydb profile is required"})
                try:
                    value = service.local_ydb_profile(run_id, profile)
                except BenchmarkError as error:
                    return self._json(400, {"error": str(error)})
                return self._json(202 if value.get("state") == "preparing" else 200, value)
            if path.startswith("/api/runs/") and path.endswith("/config.json"):
                return self._json(200, service.run_config(unquote(path[len("/api/runs/") : -len("/config.json")])))
            if path.startswith("/api/runs/") and path.endswith("/config"):
                run_id = unquote(path[len("/api/runs/") : -len("/config")])
                value = service.run_config(run_id)
                return self._attachment(
                    "application/x-yaml; charset=utf-8",
                    "{}-config.yaml".format(run_id.replace("/", "-")),
                    value["yaml"].encode("utf-8"),
                )
            if path.startswith("/api/runs/") and path.endswith("/manifest"):
                run_id = unquote(path[len("/api/runs/") : -len("/manifest")])
                manifest = load_manifest(_run_directory(service.output, run_id) / "run.json")
                return self._attachment(
                    "application/json",
                    "{}-run.json".format(run_id.replace("/", "-")),
                    (json.dumps(manifest, indent=2, sort_keys=True) + "\n").encode(),
                )
            if path.startswith("/api/runs/") and path.endswith("/archive"):
                run_id = unquote(path[len("/api/runs/") : -len("/archive")])
                with service.archive(run_id) as archive:
                    return self._file_attachment(
                        "application/zip", "{}-results.zip".format(run_id.replace("/", "-")), archive
                    )
            if path.startswith("/api/runs/") and "/artifact/" in path:
                run_id, relative = path[len("/api/runs/") :].split("/artifact/", 1)
                artifact = service.artifact(unquote(run_id), unquote(relative))
                content_type = mimetypes.guess_type(artifact.name)[0] or "application/octet-stream"
                return self._attachment(content_type, artifact.name, artifact.read_bytes())
            if path.endswith("/events") and path.startswith("/api/runs/"):
                run_id = unquote(path[len("/api/runs/") : -len("/events")])
                try:
                    after = int(parse_qs(parsed.query).get("after", [0])[0])
                except ValueError:
                    return self._json(400, {"error": "events after must be an integer"})
                events = service.events(run_id, after)
                payload = (
                    b"".join(("id: %s\ndata: %s\n\n" % (e["sequence"], json.dumps(e))).encode() for e in events)
                    or b": connected\n\n"
                )
                return self._send(200, "text/event-stream", payload)
            if path.startswith("/api/runs/"):
                item = service.detail(unquote(path[len("/api/runs/") :]))
                return self._json(200 if item else 404, item or {"error": "run not found"})
            return self._json(404, {"error": "not found"})

        def do_POST(self):
            path = urlparse(self.path).path
            try:
                if path == "/api/import":
                    return self._json(201, import_archive(service.output, self._raw_body()))
                if path == "/api/validate":
                    options = self._options()
                    return self._json(200, service.validate(options["yaml"], options["perf"]))
                if path == "/api/plan":
                    options = self._options()
                    return self._json(200, service.plan(options["yaml"], options["perf"]))
                if path == "/api/editor-config":
                    options = self._options()
                    return self._json(200, service.editor_config(options["yaml"], options["perf"]))
                if path == "/api/drafts":
                    options = self._options()
                    return self._json(201, service.save_draft(options["yaml"]))
                if path == "/api/runs":
                    options = self._options()
                    return self._json(
                        201, service.start(options["yaml"], options["perf"], options["continue_on_error"])
                    )
                if path == "/api/comparisons/selection":
                    selected = self._json_body()
                    return self._json(200, service.select_comparisons(selected))
                if path.startswith("/api/runs/") and path.endswith("/repeat"):
                    return self._json(200, service.run_config(unquote(path[len("/api/runs/") : -len("/repeat")])))
                if path.startswith("/api/runs/") and path.endswith("/cancel"):
                    return self._json(200, service.cancel(unquote(path[len("/api/runs/") : -len("/cancel")])))
            except BenchmarkError as error:
                return self._json(400, {"error": str(error)})
            return self._json(404, {"error": "not found"})

    return Handler


def make_server(listen, port, output, allow_remote=False, executor=None, perf_available=True):
    if not _is_loopback(listen) and not allow_remote:
        raise BenchmarkError("non-loopback --listen requires --allow-remote")
    server_class = _IPv6ThreadingHTTPServer if ":" in listen else _RunServiceHTTPServer
    service = RunService(output, executor=executor, perf_available=perf_available)
    server = server_class((listen, port), _handler(service))
    server.service = service
    return server


def serve(listen, port, output, no_open=False, allow_remote=False, executor=None, perf_available=True):
    server = make_server(listen, port, output, allow_remote, executor, perf_available)
    url_host = "[{}]".format(listen) if ":" in listen else listen
    url = "http://{}:{}/".format(url_host, server.server_port)
    print(url)
    if not no_open:
        webbrowser.open(url)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
