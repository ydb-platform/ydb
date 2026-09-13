"""Local web UI and durable application service for benchmark runs.

The HTTP handlers in this module deliberately only translate requests.  A
``RunService`` owns workers, manifests and the replayable event log, so closing
a browser connection cannot stop a benchmark.
"""

import csv
import hashlib
import json
import os
import math
import mimetypes
import re
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

import yaml

from ydb.tools.ydb_bench.benchmarks import BENCHMARKS
from ydb.tools.ydb_bench.lib.linux_telemetry import LogicalCpuSampler
from ydb.tools.ydb_bench.lib.common import BenchmarkError, BenchmarkInterrupted, atomic_write_json, atomic_write_text
from ydb.tools.ydb_bench.lib.config import BACKGROUND_LOAD_MODES, build_run_plan, load_config
from ydb.tools.ydb_bench.lib.results import ResultStore, _non_finite_json_as_null, load_manifest
from ydb.tools.ydb_bench.lib.actors_core import run_benchmark
from ydb.tools.ydb_bench.lib.common import binary_catalog, extract_executable, load_profile_binaries
from ydb.tools.ydb_bench.lib.import_results import MAX_TOTAL_SIZE, export_archive, import_archive
from ydb.tools.ydb_bench.lib.local_ydb import run_local_ydb
from ydb.tools.ydb_bench.lib.local_ydb_workloads import web_workload_catalog
from ydb.tools.ydb_bench.lib.topology import AFFINITY_MODES, discover_topology, plan_affinity, topology_record
from ydb.tools.ydb_bench.lib.ydb_telemetry import read_metrics
from ydb.tools.ydb_bench.lib.hosts import HostDirectory, allowed_path, allowed_post_path, open_peer, request_peer
from ydb.tools.ydb_bench.lib.federation import Federation, split_reference
from ydb.tools.ydb_bench.lib.cluster_templates import ClusterTemplateStore
from ydb.tools.ydb_bench.lib import cluster_templates_ui

_CSP = "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self'; font-src 'self'; connect-src 'self'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'"
_STREAM_CHUNK_SIZE = 1024 * 1024
_CHART_DATA_ROW_LIMIT = 100000
_LOCAL_YDB_ACTIVITY_LIMIT = 200
_LOCAL_YDB_ACTIVITY_SCAN_BYTES = 4 * 1024 * 1024
_EVENT_LOG_RECORD_BYTES = 4 * 1024 * 1024
_LOCAL_YDB_ACTIVITY_RESPONSE_BYTES = 512 * 1024
_MAX_SAFE_JSON_INTEGER = (1 << 53) - 1
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
    'ity:.5;cursor:not-allowed}.shell{min-height:100vh}.brand{font-weight:700;font-size:1.05rem;white-space:nowrap;'
    'color:var(--text)}.content{min-width:0}.topbar{min-height:3.7rem;border-bottom:1px solid #d0d5dd;'
    'padding:0 1.6rem;display:flex;flex-wrap:wrap;gap:0 1.6rem;align-items:center}.primary-nav{display:flex;'
    'flex-wrap:wrap;gap:0 1.2rem}.primary-nav a{color:var(--muted);padding:1rem 0;border-bottom:2px solid transparent;'
    'white-space:nowrap}.primary-nav a:hover{color:var(--accent);text-decoration:none}.primary-nav a[aria-current=page]{'
    'color:var(--text);font-weight:650;border-bottom-color:var(--accent)}.topbar .active-run{font-size:.9rem;'
    'color:var(--muted);margin-left:auto;min-width:0;max-width:100%;overflow-wrap:anywhere;padding:.6rem 0}'
    'main{padding:1.5rem 1.6rem 3rem}.breadcrumbs{color:var(--muted);font-size'
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
    'kground:#d946ef}@media(max-width:1000px){.primary-nav{order:3;flex-basis:100%}.primary-nav a{padding:.6rem 0}'
    '.brand{padding:.6rem 0}}@media(max-width:760px){.topbar,main{padding-left:1rem;padding-right:1rem}.split,.topology-su'
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
.local-profile-tabs{display:flex;gap:.35rem;margin:.2rem 0 1rem;border-bottom:1px solid #d0d5dd}
.local-profile-tab{display:block;padding:.6rem .85rem;border-radius:6px 6px 0 0;color:var(--muted);text-decoration:none}
.local-profile-tab:hover{background:var(--panel)}
.local-profile-tab.active{margin-bottom:-1px;border:1px solid #d0d5dd;border-bottom-color:#fff;background:#fff;color:var(--text);font-weight:650}
.local-profile-view[hidden]{display:none}
.local-result-heading{display:flex;align-items:flex-start;justify-content:space-between;gap:1rem;flex-wrap:wrap;margin-bottom:.45rem}
.local-result-heading h3{margin:.45rem 0 .2rem}.local-result-heading p{margin:.2rem 0}
.local-result-badge{display:inline-flex;padding:.15rem .5rem;border-radius:999px;background:#ecfdf3;color:var(--good);font-size:.78rem;font-weight:700}
.local-result-badge.warn{background:#fffaeb;color:var(--warn)}.local-result-badge.bad{background:#fff0f0;color:var(--bad)}
.local-result-source{min-width:11rem;padding:.55rem .7rem;border:1px solid #d0d5dd;border-radius:7px;background:var(--panel)}
.local-result-source strong{display:block;margin-top:.15rem}.local-result-section{margin-top:1.15rem}
.local-result-facts{display:grid;grid-template-columns:repeat(auto-fit,minmax(15rem,1fr));gap:.65rem;margin:.65rem 0}
.local-result-fact{padding:.65rem .75rem;border-left:3px solid #d0d5dd;background:var(--panel)}
.local-result-fact strong{display:block;margin-top:.15rem;overflow-wrap:anywhere}
.local-stages{display:flex;gap:.55rem;overflow-x:auto;padding:.25rem 0 .7rem}
.local-stage{min-width:13rem;padding:.65rem;border:1px solid #d0d5dd;border-radius:7px;background:#fff}
.local-stage.current{border-color:#84adff;background:#eff6ff}.local-stage .stage-arrow{color:var(--muted);margin-top:.35rem}
.local-charts{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:.9rem}
.local-charts>.chart-legend{grid-column:1/-1}
.local-charts .chart-panel{margin:0;padding:.8rem;border:1px solid #d0d5dd;border-radius:7px}
.local-charts .chart-panel h3{margin-top:0}
.local-attempts-scroll{max-width:100%;overflow-x:auto}
.local-attempts{width:max-content;min-width:100%}.local-attempts td,.local-attempts th{white-space:nowrap}
.discovery-status{display:flex;flex-wrap:wrap;align-items:baseline;gap:.4rem 1rem;margin:.8rem 0 .3rem}
.discovery-geometry{color:var(--muted);margin-bottom:.8rem}
.discovery-attempts{width:100%;table-layout:auto}
.discovery-attempts td,.discovery-attempts th{white-space:normal}
.discovery-attempts tr[data-attempt-href]{cursor:pointer}
.discovery-attempts tr[data-attempt-href]:hover{background:#f5f7fb}
[data-local-ydb-panel=discovery] .chart-panel{border:0;border-radius:0;padding:.4rem 0}
[data-local-ydb-panel=discovery] .local-stage{border:0;border-radius:0;padding:.3rem 0;background:none}
[data-local-ydb-panel=discovery] .local-stages{gap:1.5rem}
[data-local-ydb-panel=discovery] .local-profile-config,[data-local-ydb-panel=discovery] .local-activity{border:0}
.local-current-command{margin:.8rem 0;padding:.8rem;border:1px solid #d0d5dd;border-radius:7px;background:#101828;color:#fff}
.local-current-command .muted{color:#d0d5dd}
.local-command-code{margin:.45rem 0 0;white-space:pre-wrap;overflow-wrap:anywhere;font:12px ui-monospace,SFMono-Regular,Menlo,monospace}
.local-command-cell{width:22rem;min-width:22rem;max-width:22rem;white-space:normal!important}
.local-command-history{width:22rem}.local-command-history[open]{max-height:24rem;overflow:auto}
.local-command-history summary,.local-profile-config summary{cursor:pointer}
.local-command-entry{margin:.55rem 0;padding:.55rem;border:1px solid #e4e7ec;border-radius:5px;background:var(--panel)}
.local-profile-config{margin:.8rem 0;padding:.7rem .8rem;border:1px solid #d0d5dd;border-radius:7px;background:#fff}
.local-profile-config pre{max-height:24rem;overflow:auto;white-space:pre;margin:.7rem 0 0}
.local-activity{margin:.8rem 0;padding:.7rem .8rem;border:1px solid #d0d5dd;border-radius:7px;background:#fff}
.local-activity>summary{cursor:pointer}.local-activity-log{max-height:20rem;overflow:auto;margin:.65rem 0 0;padding:0;list-style:none}
.local-activity-item{display:grid;grid-template-columns:6.5rem minmax(10rem,1fr);gap:.35rem .75rem;padding:.45rem 0;border-top:1px solid #e4e7ec}
.local-activity-item:first-child{border-top:0}.local-activity-time{color:var(--muted);font-variant-numeric:tabular-nums}
.local-activity-command{grid-column:2;margin:.15rem 0}.local-activity-command summary{cursor:pointer;color:var(--muted)}
.attempt-pass{color:var(--good);font-weight:650}.attempt-fail{color:var(--bad);font-weight:650}
.comparison-delta{font-weight:650}.comparison-delta.good{color:var(--good)}.comparison-delta.bad{color:var(--bad)}
.comparison-config-changed{background:var(--panel);font-weight:600;overflow-wrap:anywhere}
#local-ydb-comparison table{width:100%;min-width:620px;table-layout:fixed}
#local-ydb-comparison td,#local-ydb-comparison th{white-space:normal;overflow-wrap:anywhere}
#local-ydb-comparison .comparison-results th:first-child{width:34%}
#local-ydb-comparison td.good{color:var(--good);background:transparent}
#local-ydb-comparison td.bad{color:var(--bad)}
#local-ydb-comparison .view-tabs{gap:.35rem;border-bottom:1px solid #d0d5dd;margin:1rem 0}
#local-ydb-comparison .view-tabs button{padding:.6rem .8rem;border:1px solid transparent;border-radius:6px 6px 0 0;background:transparent;color:var(--muted);margin-bottom:-1px}
#local-ydb-comparison .view-tabs button[aria-pressed=true]{background:#fff;color:var(--text);font-weight:650;border-color:#d0d5dd;border-bottom-color:#fff}
.verification-badge{display:inline-flex;align-items:center;margin-left:.35rem;padding:.08rem .42rem;border:1px solid #d0d5dd}
.verification-badge{border-radius:999px;background:var(--panel);color:var(--text);font-size:.75rem;font-weight:650;vertical-align:middle}
.verification-badge.bad{border-color:#fecdca;background:#fff0f0;color:var(--bad)}
.verification-summary{margin:.7rem 0;padding:.65rem .8rem;border:1px solid #d0d5dd;border-radius:7px;background:var(--panel);color:var(--text)}
.verification-summary.bad{border-color:#fecdca;background:#fff0f0;color:var(--bad)}
.profile-error{border-left:4px solid var(--bad);padding:.8rem 1rem;margin:.8rem 0;background:#fff0f0}
.profile-error h3{color:var(--bad);margin:0 0 .4rem}.profile-error pre{white-space:pre-wrap;overflow-wrap:anywhere}
.actor-flags{display:flex;flex-wrap:wrap;gap:.4rem 1rem;margin-bottom:1rem}
.actor-flag{position:relative;display:inline-flex;gap:.35rem;align-items:center;font-size:.85rem;cursor:pointer}
.actor-flag input{margin:0;width:auto}.actor-flag .flag-help{display:none;position:absolute;z-index:20;top:100%;left:0;
width:15rem;max-width:70vw;padding:.6rem;background:#fff;border:1px solid #d0d5dd;border-radius:5px;color:var(--text)}
.actor-flag:hover .flag-help,.actor-flag:focus-within .flag-help{display:block}
.view-tabs{display:flex;gap:.4rem;flex-wrap:wrap;margin:.8rem 0}.view-tabs button[aria-pressed=true]{background:var(--accent);color:#fff}
.dense-run{display:grid;grid-template-columns:minmax(0,1fr) auto;gap:.6rem;padding:.65rem 0;border-bottom:1px solid #d0d5dd}
.dense-run-meta{display:flex;flex-wrap:wrap;gap:.2rem .8rem;font-size:.8rem;color:var(--muted);font-variant-numeric:tabular-nums}
.dense-run-profiles{font-size:.9rem;margin:.2rem 0;overflow-wrap:anywhere}.dense-run-id{font-size:.8rem;overflow-wrap:anywhere}
.dense-run-actions{position:relative}.dense-run-actions .actions{position:absolute;right:0;z-index:10;background:#fff;
padding:.6rem;border:1px solid #d0d5dd;min-width:8rem;flex-direction:column}
.dense-run summary{cursor:pointer}.runs-toolbar{display:flex;flex-wrap:wrap;gap:.6rem;align-items:center;margin:.8rem 0}
.runs-actions{display:flex;gap:.6rem;flex-wrap:wrap;margin-left:auto}
.runs-heading{display:flex;align-items:center;justify-content:space-between;gap:1rem;flex-wrap:wrap;margin-bottom:1rem}
.runs-heading .page-title{margin:0}.new-run-link{padding:.38rem .65rem;border-radius:5px;background:var(--accent);color:#fff;white-space:nowrap}
.new-run-link:hover{color:#fff;text-decoration:none}
.import-dialog{width:min(30rem,calc(100vw - 2rem));padding:1.2rem;border:1px solid var(--line);border-radius:8px;background:#fff;color:var(--text)}
.import-dialog::backdrop{background:rgb(0 0 0 / 35%)}
.import-dialog h2{margin-top:0}.import-dialog input{max-width:100%;margin:.8rem 0}
.import-dialog .toolbar{justify-content:flex-end;margin-bottom:0}
.dense-run{position:relative}.dense-run:hover{background:#f5f7fb}
.dense-run-id::after{content:"";position:absolute;inset:0}
.dense-run-id:focus-visible::after{outline:2px solid var(--accent);outline-offset:2px}
.dense-run-actions{position:relative;z-index:1}
.dense-run-actions[open]{z-index:2}
.report-columns{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:1rem 2rem}
.report-table{width:100%;font-variant-numeric:tabular-nums}.report-table th,.report-table td{text-align:right}
.report-table th:first-child,.report-table td:first-child{text-align:left}
.report-config{margin-top:1rem}.report-config summary{cursor:pointer;font-weight:650}
.report-config .report-columns{margin:1rem 0}.report-config td{overflow-wrap:anywhere}
.report-source{margin:.75rem 0;color:var(--muted)}
.card.local-result-container{border:0;border-radius:0;box-shadow:none;padding-top:0}
.card.profile-overview{border:0;border-radius:0;box-shadow:none}
.profile-metric-summary{margin:0 0 1.25rem}
.profile-metric-summary .local-kpis{grid-template-columns:repeat(auto-fit,minmax(12rem,1fr));gap:1.5rem;margin-top:0}
.profile-metric-summary .local-kpis>div{border:0;background:none;padding:.4rem 0}
.profile-metric-summary .local-kpis strong{font-size:1.8rem}
.profile-metric-summary .metric-unit{font-size:.85rem;font-weight:400;color:var(--muted)}
.attempt-page .chart-panel{border:0;border-radius:0;padding:.4rem 0}
.attempt-page .profile-metric-summary{margin-top:.8rem}
.attempt-meta{display:flex;flex-wrap:wrap;gap:.4rem 1.2rem;color:var(--muted);margin:.5rem 0 1rem}
.attempt-command{margin:1rem 0}.attempt-command h3{margin-bottom:.4rem}
.attempt-command pre{white-space:pre-wrap;overflow-wrap:anywhere;max-height:none;padding:.8rem;background:var(--panel)}
.run-tabs{margin-bottom:.25rem}
.run-header{display:flex;align-items:center;justify-content:space-between;gap:1rem;flex-wrap:wrap;margin-bottom:.5rem}
.run-header .page-title{margin:0;min-width:0;overflow-wrap:anywhere}
.run-header .toolbar{margin:0 0 0 auto;flex-wrap:wrap}
.run-header .downloads{position:relative}
.run-header .downloads .actions{position:absolute;right:0;z-index:20;background:#fff;border:1px solid #d0d5dd;padding:.7rem}
.run-tabs{overflow:visible;flex-wrap:wrap}
.local-profile-tabs{flex-wrap:wrap}
[data-local-ydb-panel=result] .local-kpis>div{border:0;background:none;padding:.4rem 0;border-radius:0}
[data-local-ydb-panel=result] .local-kpis{gap:1.5rem}
[data-local-ydb-panel=result] .local-kpis strong{font-size:1.8rem}
@media(max-width:650px){.report-columns{grid-template-columns:1fr}}
@media(max-width:900px){.local-live{grid-template-columns:1fr 1fr}.local-charts{grid-template-columns:1fr}}
"""
    '.status.queued{color:var(--warn)}\n'
)
_CSS += """
.run-configuration{white-space:pre-wrap;overflow-wrap:anywhere;max-height:none;padding:1rem;background:var(--panel)}
.configuration-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:1.2rem 2rem}
.configuration-grid h3{margin:.6rem 0}.configuration-values{margin:0}
.configuration-values>div{display:grid;grid-template-columns:minmax(8rem,1fr) minmax(0,1.4fr);gap:1rem;padding:.5rem 0;border-bottom:1px solid #d0d5dd}
.configuration-values dt{color:var(--muted)}.configuration-values dd{margin:0;overflow-wrap:anywhere;white-space:pre-wrap}
.configuration-subgroup{margin:.6rem 0}.configuration-subgroup h4{margin:.8rem 0 .3rem}
.configuration-wide{grid-column:1/-1}.configuration-role-grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:1.5rem}
@media(max-width:800px){.configuration-role-grid,.configuration-grid{grid-template-columns:1fr}}
.new-run-page .profile-list{display:flex;flex-wrap:wrap;gap:.2rem;border-bottom:1px solid #d0d5dd;margin:.8rem 0 1rem}
.new-run-page .profile-list button{width:auto;margin:0 0 -1px;padding:.6rem .8rem;border:1px solid transparent;border-radius:5px 5px 0 0}
.new-run-page .profile-list button.selected{background:#fff;color:var(--text);border-color:#d0d5dd;border-bottom-color:#fff;font-weight:650}
.new-run-page .editor-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:1.5rem 2rem}
.new-run-page .editor-grid h3{margin-top:0}.new-run-page .editor-wide{grid-column:1/-1}
.new-run-page .editor-options{margin:1rem 0}.new-run-page .editor-options summary{cursor:pointer;color:var(--muted)}
.new-run-page .editor-options[open]>.form-grid{margin-top:.8rem}
.new-run-page .editor-role{display:grid;grid-template-columns:8rem minmax(0,1fr) minmax(8rem,.4fr);gap:1rem;align-items:start;padding:.6rem 0;border-bottom:1px solid #d0d5dd}
.new-run-page .editor-role strong{padding-top:1.7rem}.new-run-page .editor-role .field{min-width:0}
.new-run-page .editor-plan{margin:.8rem 0;color:var(--muted)}
.new-run-page .page-heading{display:flex;justify-content:space-between;align-items:center;gap:1rem;flex-wrap:wrap}
.new-run-page .page-heading .toolbar{margin:0}.new-run-page .page-heading .page-title{margin:0}
@media(max-width:800px){.new-run-page .editor-grid{grid-template-columns:1fr}}
@media(max-width:550px){.new-run-page .editor-role{grid-template-columns:1fr}.new-run-page .editor-role strong{padding-top:0}}
"""

_CSS += (
    '#cpu-topology .view-tabs{gap:.35rem;border-bottom:1px solid #d0d5dd;padding:0;margin:1rem 0;flex-wrap:wrap}'
    '#cpu-topology .view-tabs button{padding:.6rem .8rem;border:1px solid transparent;border-radius:6px 6px 0 0;'
    'background:none;color:var(--muted);margin-bottom:-1px;box-shadow:none}'
    '#cpu-topology .view-tabs button:hover{background:var(--panel)}'
    '#cpu-topology .view-tabs button[aria-pressed=true]{background:#fff;color:var(--text);font-weight:650;'
    'border-color:#d0d5dd;border-bottom-color:#fff}'
    "\n#cpu-topology .cpu-node{display:grid;grid-template-columns:75px minmax(0,1fr);gap:16px;padding:10px 0;border-bottom:1px solid #ced6e2}\n#cpu"
    "-topology .cpu-node-name{padding-top:20px;font-size:13px}\n#cpu-topology small{display:inline-block;color:#60708b;font-size:12px}\n#cpu-topolo"
    "gy .cpu-node-name small{display:block}\n#cpu-topology .cpu-groups{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,260px),1"
    "fr));gap:16px}\n#cpu-topology .cpu-group-label{font-size:12px;color:#60708b;margin-bottom:5px}\n#cpu-topology .cpu-core-grid{display:grid;grid"
    "-template-columns:repeat(8,minmax(0,1fr));gap:4px}\n#cpu-topology .cpu-core{display:flex;flex-direction:column;gap:2px;padding:0;border:0;min"
    "-width:0;background:none;font-size:12px;font-variant-numeric:tabular-nums;border-radius:3px}\n#cpu-topology .cpu-core[aria-pressed=true]{outl"
    "ine:2px solid #2167b9;outline-offset:1px}\n#cpu-topology .cpu-cell{display:block;width:100%;padding:3px 0;background:#edf2f8;border-radius:2p"
    "x}\n#cpu-topology .cpu-map-toolbar{display:flex;align-items:center;justify-content:space-between;gap:12px;flex-wrap:wrap}\n#cpu-topology .cpu-"
    "selection{display:flex;align-items:center;gap:12px 20px;flex-wrap:wrap;min-height:72px;font-size:13px;padding:12px 0}\n#cpu-topology .cpu-sel"
    "ection small{display:block}\n#cpu-topology .cpu-heat-scale{display:inline-block;width:64px;height:8px;background:linear-gradient(90deg,#edf2f"
    "8,#2167b9)}\n#cpu-topology .cpu-help{position:relative}\n#cpu-topology #cpu-help-button{border:1px solid #60708b;border-radius:50%;width:26px;"
    "height:26px;padding:0;color:#60708b}\n#cpu-topology #cpu-map-help{position:absolute;top:34px;left:0;width:min(300px,75vw);padding:12px;backgr"
    "ound:#fff;border:1px solid #ced6e2;border-radius:4px;z-index:20;box-shadow:0 4px 12px #18223722;font-size:13px}\n@media(max-width:600px){#cpu"
    "-topology .cpu-node{grid-template-columns:1fr;gap:8px}#cpu-topology .cpu-node-name{padding:0;display:flex;justify-content:space-between}}\n@m"
    "edia(pointer:coarse){#cpu-topology .cpu-core{min-height:44px}#cpu-topology #cpu-help-button{width:44px;height:44px}}\n"
)

_CSS += (
    '.comparison-profile-choice{display:flex;align-items:center;gap:12px;flex-wrap:wrap;padding:8px 0;border-bottom:1px solid var(--line)}'
    '.comparison-profile-choice span:first-of-type{flex:1;min-width:180px}'
    '#comparison-runs tr[data-picker-run]{cursor:pointer}'
    '#comparison-runs tr:hover,#comparison-runs .comparison-run-selected{background:var(--panel)}'
    '#comparison-runs td{white-space:normal;overflow-wrap:anywhere}'
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
    "const viewedHost=new URLSearchParams(location.search).get('host')||'';\n"
    """
function splitRunRef(value){const match=/^([0-9a-f]{8}-[0-9a-f-]{27}):(.*)$/.exec(value);return match?{host:match[1],id:match[2]}:null}
let editorHost='',editorHostOptions='',editorRenderVersion=0;
function editorApi(path,options){return api(editorHost?'/api/hosts/'+enc(editorHost)+path:path,options)}
async function refreshEditorActivity(){
  const button=document.querySelector('#start-run'),host=editorHost;
  if(!button||!['#new','#new/yaml'].includes(location.hash))return;
  try{
    const value=await editorApi('/api/activity-status');
    if(host===editorHost&&button===document.querySelector('#start-run')){
      button.textContent=value.active_run_id||value.queued?'Add to queue':'Start run'
    }
  }catch{}
}
function runDisplay(value){return splitRunRef(value)?.id||value}
function hostApiPath(path){
  if(path.startsWith('/api/federation/')||path.startsWith('/api/hosts'))return path;
  const match=/^[/]api[/]runs[/]([^/?]+)(.*)$/.exec(path),ref=match&&splitRunRef(decodeURIComponent(match[1]));
  if(ref)return '/api/hosts/'+enc(ref.host)+'/api/runs/'+enc(ref.id)+match[2];
  const routeRef=splitRunRef(decodeURIComponent(location.hash.split('/')[1]||''));
  if(match&&routeRef&&/^#(?:run|attempt)[/]/.test(location.hash))return '/api/hosts/'+enc(routeRef.host)+path;
  return viewedHost&&(/#(?:run|attempt)[/]/.test(location.hash)||path==='/api/system-topology'||path==='/api/cpu-usage')?
    '/api/hosts/'+enc(viewedHost)+path:path
}
function federationErrors(errors){return (errors||[]).map(item=>'<div class=notice>'+esc(item.host_name)+': '+esc(item.error)+'</div>').join('')}
async function hostChoices(selected='',all=true){
  const value=await api('/api/hosts'),hosts=[value.local,...value.hosts];
  return (all?'<option value="">All hosts</option>':'')+hosts.map(host=>'<option value="'+esc(host.id===value.local.id&&!all?'':host.id)+'" '+
    ((selected||value.local.id)===host.id&&!all||selected===host.id?'selected':'')+'>'+esc(host.name)+(host.id===value.local.id?' (this host)':'')+'</option>').join('')
}
"""
    "async function api(path,options={}){const response=await fetch(hostApiPath(path),options);const type=response.headers.get('content-ty"
    "pe')||'';const body=type.includes('application/json')?await response.json():await response.text();if(!response.ok)throw "
    'Error(body.error||body||response.statusText);return body}\n'
    "function jsonOptions(value){return {method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(value)"
    '}}\n'
    "function routeParts(){return (location.hash.slice(1)||'runs').split('/').map(decodeURIComponent)}\n"
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
    """
async function renderHosts(){
  clearRefresh();
  try{
    const value=await api('/api/hosts');
    if(location.hash!=='#hosts')return;
    app.innerHTML=shell('hosts','<div class=runs-toolbar><span class=muted>Benchmark hosts</span><div class=runs-actions>'+
      '<button id=refresh-hosts>Refresh</button><button id=add-host class=primary>Add host</button></div></div>'+
      '<div class=table-scroll><table><thead><tr><th>Host</th><th>Port</th><th>Connection</th><th>Activity</th><th>Views</th><th></th></tr></thead><tbody>'+
      '<tr><td>'+esc(value.local.name)+'<div class=muted>This server</div></td><td>'+esc(value.local.port??'—')+'</td><td>Online</td><td>—</td><td>'+
      '<a href="/?#runs">Runs</a> · <a href="/?#topology">Topology</a></td><td><button id=copy-host-token>Copy token</button></td></tr>'+
      value.hosts.map(host=>'<tr data-host="'+esc(host.id)+'"><td>'+esc(host.name)+'<div class=muted>'+esc(host.endpoint)+
      '</div></td><td>'+esc(host.port??'—')+'</td><td data-connection>Checking…</td><td data-activity>—</td><td><a href="/?host='+enc(host.id)+
      '#runs">Runs</a> · <a href="/?host='+enc(host.id)+'#topology">Topology</a></td><td><button data-remove>Remove</button></td></tr>').join('')+
      '</tbody></table></div><div id=hosts-error role=alert></div>'+
      '<dialog id=host-dialog class=import-dialog><h2>Add host</h2><label class=field>Name<input id=host-name maxlength=120></label>'+
      '<label class=field>Server endpoint<input id=host-endpoint placeholder="http://127.0.0.1:42420"></label>'+
      '<label class=field>Peer token<input id=host-token type=password autocomplete=off></label>'+
      '<p class=muted>HTTP sends the token and data unencrypted; use it only on trusted networks. Open Hosts on the server you want to add and click Copy token.</p>'+
      '<div id=host-error role=alert></div><div class=toolbar><button id=cancel-host>Cancel</button><button id=save-host>Add host</button></div></dialog>');
    const dialog=app.querySelector('#host-dialog');
    app.querySelector('#refresh-hosts').onclick=renderHosts;
    app.querySelector('#copy-host-token').onclick=async event=>{
      const button=event.currentTarget;
      button.disabled=true;
      app.querySelector('#hosts-error').textContent='';
      try{
        if(!navigator.clipboard)throw new Error('Clipboard access requires HTTPS or localhost.');
        const token=api('/api/hosts/token',jsonOptions({})).then(value=>value.token);
        if(window.ClipboardItem&&navigator.clipboard.write){
          await navigator.clipboard.write([new ClipboardItem({'text/plain':token.then(value=>new Blob([value],{type:'text/plain'}))})]);
        }else await navigator.clipboard.writeText(await token);
        button.textContent='Copied';
      }catch(error){app.querySelector('#hosts-error').textContent='Could not copy token: '+error.message}
      finally{button.disabled=false}
    };
    app.querySelector('#add-host').onclick=()=>dialog.showModal();
    app.querySelector('#cancel-host').onclick=()=>dialog.close();
    app.querySelector('#save-host').onclick=async event=>{
      event.target.disabled=true;
      try{await api('/api/hosts/add',jsonOptions({name:app.querySelector('#host-name').value,
        endpoint:app.querySelector('#host-endpoint').value,token:app.querySelector('#host-token').value}));await renderHosts()}
      catch(error){app.querySelector('#host-error').textContent=error.message;event.target.disabled=false}
    };
    for(const row of app.querySelectorAll('[data-host]')){
      row.querySelector('[data-remove]').onclick=async()=>{
        if(!confirm('Remove this host from the directory? Its processes and results will not be changed.'))return;
        try{await api('/api/hosts/remove',jsonOptions({id:row.dataset.host}));await renderHosts()}
        catch(error){app.querySelector('#hosts-error').textContent=error.message}
      };
      api('/api/hosts/'+enc(row.dataset.host)+'/api/activity-status').then(activity=>{
        if(!row.isConnected)return;
        row.querySelector('[data-connection]').textContent='Online';
        row.querySelector('[data-activity]').textContent=activity.active_run_id||'Idle';
      }).catch(()=>{if(row.isConnected){row.querySelector('[data-connection]').textContent='Offline';
        row.querySelector('[data-activity]').textContent='Unknown'}});
    }
  }catch(error){app.innerHTML=shell('hosts',displayError(error))}
}
function shell(current,body,breadcrumb=''){
  queueMicrotask(refreshActiveBanner);
  const navigation=[['runs','Runs'],['topology','System topology'],['comparisons','Comparisons'],['hosts','Hosts'],
    ['cluster-templates','Cluster templates']];
  const section=current==='new'?'runs':current;
  return '<div class=shell><div class=content><header class=topbar><a class=brand href="#runs">YDB benchmark</a>'+
    '<nav class=primary-nav aria-label="Main navigation">'+navigation.map(([id,label])=>
      '<a href="'+(id==='hosts'?'/?#hosts':'#'+id)+'"'+(section===id?' aria-current="page"':'')+'>'+label+'</a>').join('')+
    '</nav><span class=active-run>'+(activeRun?'<a href="#run/'+enc(activeRun)+'">Active run: '+esc(activeRun)+'</a>':
      'No active run')+'</span></header><main>'+
      (viewedHost&&/^#(?:run|attempt)[/]/.test(location.hash)?
        '<p class=muted>Remote host · '+esc(viewedHost)+' · <a href="/?#hosts">Back to hosts</a></p>':'')+
      breadcrumb+body+'</main></div></div>'
}
"""
    "function breadcrumbs(items){return items.length?'<div class=breadcrumbs>'+items.map((item,index)=>index===items.length-1"
    '?esc(item.label):\'<a href="#\'+esc(item.route)+\'">\'+esc(item.label)+\'</a>\').join(\' / \')+\'</div>\':\'\'}\n'
    "function saveDraft(){sessionStorage.setItem('ydb-bench-draft',editor.yaml)}\n"
    "function compactIntegerRanges(values){const numbers=values.map(Number);if(!numbers.length)return '';const parts=[];for(l"
    'et index=0;index<numbers.length;){let end=index;while(end+1<numbers.length&&numbers[end+1]===numbers[end]+1)end++;parts.'
    "push(index===end?String(numbers[index]):numbers[index]+'-'+numbers[end]);index=end+1}return parts.join(', ')}\n"
    "function yamlArray(values){return '['+values.map(value=>String(value)).join(', ')+']'}\n"
    "function yamlScalar(value){return typeof value==='string'?JSON.stringify(value):String(value)}\n"
    "const localYdbGeometryKeys={static_nodes:'static-nodes',dynamic_nodes:'dynamic-nodes',max_dynamic_nodes:'max-dynamic-"
    "nodes',disk_size_gb:'disk-size-gb',storage_groups:'storage-groups'};\n"
    "const localYdbActorSystemKeys={use_shared_threads:'use-shared-threads',use_united_pool:'use-united-pool',"
    "use_ring_queue:'use-ring-queue'};\n"
    "const localYdbSearchKeys={resolution_percent:'resolution-percent'};\n"
    "const localYdbObjectiveKeys={target_role:'target-role',plateau_gain_percent:'plateau-gain-percent',plateau_points:'p"
    "lateau-points',cpu_saturation_percent:'cpu-saturation-percent'};\n"
    "const localYdbSloKeys={max_ms:'max-ms',max_errors:'max-errors',min_achieved_rate_ratio:'min-achieved-rate-ratio'};\n"
    "const localYdbAffinityKeys={ydb_cli:'ydb-cli',static_nodes:'static-nodes',dynamic_nodes:'dynamic-nodes'};\n"
    "function localYdbWorkloadDefinition(type){const definition=(editor.model?.local_ydb_workloads||[]).find(item=>item.type"
    "===type);if(!definition)throw Error('Unknown local YDB workload: '+type);return definition}\n"
    """
const localYdbLoadDefaults={
  rate:{values:[1000],start:1000,maximum:100000},
  threads:{values:[1,2,4,8,16,32,64],start:1,maximum:256},
};
function localYdbLoadLimit(definition,workload,parameter){
  const constraint=definition?.load_limits?.[parameter];
  if(!constraint)return null;
  const option=Number(workload?.options?.[constraint.option]),multiplier=Number(constraint.multiplier);
  const limit=option*multiplier;
  return Number.isSafeInteger(limit)&&limit>0?limit:null
}
function localYdbDefaultClientThreads(definition){
  const value=Number(definition?.default_client_threads);
  return Number.isSafeInteger(value)&&value>0?value:64
}
function localYdbDefaultWarmupSeconds(definition){
  if(definition?.default_warmup_seconds===null)return null;
  const value=Number(definition?.default_warmup_seconds);
  return Number.isSafeInteger(value)&&value>=0?value:10
}
function localYdbMeasurementMaximumDuration(definition,warmup){
  const total=Number(definition?.maximum_total_seconds),warmupSeconds=Number(warmup);
  if(!Number.isSafeInteger(total)||total<=0||warmup===null||
      !Number.isSafeInteger(warmupSeconds)||warmupSeconds<0)return null;
  return total-warmupSeconds
}
function localYdbMeasurementForWorkload(measurement,definition){
  const warmup=localYdbDefaultWarmupSeconds(definition),minimum=definition.minimum_duration_seconds||1;
  let duration=Number(measurement.duration);
  if(!Number.isSafeInteger(duration)||duration<minimum)duration=minimum;
  const maximum=localYdbMeasurementMaximumDuration(definition,warmup);
  if(maximum!==null)duration=Math.min(duration,maximum);
  return {...measurement,warmup,duration}
}
function localYdbValidateMeasurement(measurement,definition){
  const maximum=localYdbMeasurementMaximumDuration(definition,measurement.warmup);
  if(maximum!==null&&measurement.duration>maximum){
    throw Error('Warmup plus duration must not exceed '+definition.maximum_total_seconds+' seconds.')
  }
  return measurement
}
function localYdbWarmupInput(id,definition){
  const raw=document.querySelector('#'+id).value.trim();
  return raw===''?localYdbDefaultWarmupSeconds(definition):localInteger(id,0)
}
function localYdbNeedsRerender(id,loadLimitInputs=[]){
  return ['profile-name','local-geometry-preset','local-load-allow-errors',
    'local-measurement-warmup'].includes(id)||loadLimitInputs.includes(id)
}
function localYdbParameterDefaults(parameter,definition=null,workload=null){
  const source=localYdbLoadDefaults[parameter]||localYdbLoadDefaults.threads;
  const limit=localYdbLoadLimit(definition,workload,parameter);
  if(limit===null)return {...source,values:[...source.values]};
  const values=[...new Set(source.values.map(value=>Math.min(value,limit)))].sort((left,right)=>left-right);
  return {values,start:Math.min(source.start,limit),maximum:Math.min(source.maximum,limit)}
}
function localYdbClampLoad(load,definition=null,workload=null){
  const limit=localYdbLoadLimit(definition,workload,load.parameter);
  if(limit===null)return load;
  if(load.values){
    const values=[...new Set(load.values.map(value=>Math.min(Number(value),limit)))]
      .filter(value=>Number.isSafeInteger(value)&&value>0).sort((left,right)=>left-right);
    return {...load,values:values.length?values:[limit]}
  }
  const maximum=Math.min(Number(load.search.maximum),limit);
  return {...load,search:{...load.search,start:Math.min(Number(load.search.start),maximum),maximum}}
}
function localYdbResetLoadParameter(load,parameter,definition=null,workload=null){
  const defaults=localYdbParameterDefaults(parameter,definition,workload);
  const reset=load.values?{...load,parameter,values:[...defaults.values]}:{
    ...load,parameter,search:{...(load.search||{}),start:defaults.start,maximum:defaults.maximum}
  };
  return localYdbClampLoad(reset,definition,workload)
}
function localYdbLoadForWorkload(load,parameters,definition=null,workload=null){
  const compatible=parameters.includes(load.parameter)?load:
    localYdbResetLoadParameter(load,parameters[0],definition,workload);
  return localYdbClampLoad(compatible,definition,workload)
}
"""
    "function defaultLocalYdbWorkload(type){const definition=localYdbWorkloadDefinition(type),operation=definition.default_"
    "operation,options=Object.fromEntries(definition.options.map(option=>[option.name,Object.prototype.hasOwnProperty.call("
    "option.operation_defaults,operation)?option.operation_defaults[operation]:option.default]));return {type,operation,opt"
    "ions}}\n"
    "function defaultLocalYdb(){const definition=localYdbWorkloadDefinition('kv');return {workload:defaultLocalYdbWorkload('kv'),"
    "actor_system:{use_shared_threads:false,use_united_pool:false,use_ring_queue:true},"
    "geometry:{preset:'single',static_nodes:1,dynamic_nodes:1,max_dynamic_nodes:1,disk_size_gb:64,storage_groups:1},client"
    ":{threads:localYdbDefaultClientThreads(definition)},load:{parameter:'rate',allow_errors:false,values:[1000]},measurement:{warmup:localYdbDefaultWarmupSeconds(definition),duration:30,rep"
    "etitions:3,verification_repetitions:3},affinity:{ydb_cli:{mode:'pack-numa-pack-chiplet-spread-core',cpus:'one-chiplet'},static_nodes:{mode:'none'"
    ",cpus:null},dynamic_nodes:{mode:'none',cpus:null}}}}\n"
    "function serializeLocalYdb(lines,profile){const config=profile.local_ydb,workload=config.workload;"
    "if(config.ydbd_binary)lines.push('    ydbd-binary: '+yamlScalar(config.ydbd_binary));lines.push('    work"
    "load:','      type: '+workload.type,'      operation: '+workload.operation,'      options:');for(const [key,value] of "
    "Object.entries(workload.options))lines.push('        '+key+': '+yamlScalar(value));lines.push('    geometry:','      preset: '+conf"
    "ig.geometry.preset);for(const [key,yamlKey] of Object.entries(localYdbGeometryKeys))lines.push('      '+yamlKey+': '+c"
    "onfig.geometry[key]);lines.push('    actor-system:');"
    "for(const [key,yamlKey] of Object.entries(localYdbActorSystemKeys))"
    "lines.push('      '+yamlKey+': '+Boolean(config.actor_system?.[key]??(key==='use_ring_queue')));"
    "for(const role of ['static_nodes','dynamic_nodes']){const count=config.actor_system?.[role]?.cpu_count;"
    "if(count!==undefined)lines.push('      '+role.replaceAll('_','-')+':','        cpu-count: '+count);}"
    "lines.push('    client:','      threads: '+config.client.threads,'    load:','      parameter: '"
    "+config.load.parameter,'      allow-errors: '+Boolean(config.load.allow_errors));if(config.load.values)lines.push('      values: '+yamlArray(config.load.values));else{lines."
    "push('      search:','        start: '+config.load.search.start,'        maximum: '+config.load.search.maximum);if("
    "config.load.objective.type==='latency-slo')lines.push('        multiplier: '+config.load.search.multiplier);for("
    "const [key,yamlKey] of Object.entries(localYdbSearchKeys))lines."
    "push('        '+yamlKey+': '+config.load.search[key]);lines.push('      objective:','        type: '+config.load.object"
    "ive.type);if(config.load.objective.type==='maximize-throughput')for(const [key,yamlKey] of Object.entries(localYdbOb"
    "jectiveKeys))lines.push('        '+yamlKey+': '+config.load.objective[key]);else{lines.push('        percentile: '+config.load.o"
    "bjective.percentile);for(const [key,yamlKey] of Object.entries(localYdbSloKeys))lines.push('        '+yamlKey+': '+con"
    "fig.load.objective[key])}}lines.push('    measurement:');if("
    "config.measurement.warmup!==null&&config.measurement.warmup!==undefined)lines.push('      warmup: '+config.measurement.warmup);lines.push("
    "'      duration: '+config.measurement.duration,'      repetitions: '+config.measurement.repetitions,"
    "'      verification-repetitions: '+(config.measurement.verification_repetitions??0),'    affinity:');f"
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
    "async function syncEditor(){const host=editorHost,yaml=editor.yaml,perf=editor.perf;try{const value=await editorApi('/api/editor-config',jsonOptions({yaml,perf}));"
    "if(host!==editorHost||yaml!==editor.yaml||perf!==editor.perf)return null;"
    'editor.model=value;editor.error=null;if(!editor.selected&&value.profiles.length)editor.selected=value.profiles[0].key;return value}'
    "catch(error){if(host===editorHost&&yaml===editor.yaml&&perf===editor.perf){editor.model=null;editor.error=error.message}return null}}\n"
    'function profileByKey(key){return (editor.model?.profiles||[]).find(profile=>profile.key===key)}\n'
    'function updateProfile(key,mutate){const profile=profileByKey(key);if(!profile)return;mutate(profile);editor.yaml=serial'
    'izeConfig(editor.model);saveDraft()}\n'
    'function planSummary(){const profiles=editor.model?.profiles||[];let count=0,seconds=0;for(const profile of profiles){co'
    'nst benchmark=editor.model.benchmarks.find(item=>item.name===profile.benchmark);if(profile.local_ydb){count++;continue}const cases=(benchmark?.parameters||[]).filter'
    '(item=>item.matrix).reduce((total,item)=>total*(profile.parameters[item.name]?.length||1),1),processes=profile.affinity.'
    "length*(profile.background_load||['none']).length*profile.threads.length*profile.repetitions*cases;count+=processes;seconds+=processes*profile.duration}return {cou"
    'nt,seconds}}\n'
    "function editorControls(){return '<div class=toolbar><label>Host <select id=run-host>'+editorHostOptions+'</select></label><button id=validate>Validate</button><button id=download-yaml>Downl"
    "oad YAML</button><button id=save-host>Save YAML on host</button><button class=primary id=start-run>Start run</button></div>'}\n"
    "function editorRunOptions(){return '<div class=toolbar><label><input id=perf type=checkbox '+(editor.perf?'chec"
    "ked':'')+'> perf</label><label><input id=continue type=checkbox '+(editor.continueOnError?'checked':'')+'> continue on e"
    "rror</label></div><div id=editor-message></div>'}\n"
    'function parameterCases(benchmark,profile){let cases=[[]];for(const parameter of benchmark.parameters.filter(item=>item.'
    'matrix)){const values=profile.parameters[parameter.name]||parameter.default;cases=cases.flatMap(parts=>values.map(value='
    ">[...parts,parameter.name+'='+value]))}return cases}\n"
    'function bindEditorControls(){\n'
    '  refreshEditorActivity();\n'
    "  document.querySelector('#run-host').onchange=async event=>{editorHost=event.target.value;clearTimeout(window.ydbBenchYamlTimer);await renderNew()};\n"
    "  const message=document.querySelector('#editor-message');\n"
    '  const showMessage=(text,kind=\'good\')=>{message.innerHTML=\'<div class="notice \'+kind+\'">\'+esc(text)+\'</div>\'};\n'
    "  if(editor.model&&document.querySelector('.profile-list')){\n"
    '    const queue=[];\n'
    '    for(const profile of editor.model.profiles){const benchmark=editor.model.benchmarks.find(item=>item.name===profile.b'
    "enchmark);if(profile.local_ydb){const local=profile.local_ydb;queue.push(profile.benchmark+' / '+profile.name+' / '+"
    "local.workload.type+' '+local.workload.operation+' / '+(local.load.values?'fixed load points':'adaptive load search'));continue}"
    "for(const affinity of profile.affinity)for(const backgroundLoad of (profile.background_load||['none']))"
    "for(const threads of profile.threads)for(const parameters of parameterC"
    "ases(benchmark,profile))for(let repeat=1;repeat<=profile.repetitions;repeat++)queue.push(profile.benchmark+' / '+profile"
    ".name+' / '+affinity+' / '+backgroundLoad+' / '+threads+' threads'+(parameters.length?' / '+parameters.join(', '):'')+' / repeat '+repeat)}\n"
    "    message.insertAdjacentHTML('beforebegin','<details class=editor-options><summary>Execution plan ('+queue.length+' profile executions)</"
    "summary><ol>'+queue.map(item=>'<li><code>'+esc(item)+'</code></li>').join('')+'</ol></details>');\n"
    '  }\n'
    "  document.querySelector('#perf').onchange=async event=>{editor.perf=event.target.checked;await syncEditor();renderNew()"
    '};\n'
    "  document.querySelector('#continue').onchange=event=>{editor.continueOnError=event.target.checked};\n"
    "  document.querySelector('#validate').onclick=async()=>{\n"
    "    try {const value=await editorApi('/api/validate',jsonOptions({yaml:editor.yaml,perf:editor.perf}));showMessage(value.valid"
    "?'Valid configuration: '+value.steps+' planned processes.':value.error,value.valid?'good':'error')}\n"
    "    catch(error){showMessage(error.message,'error')}\n"
    '  };\n'
    "  document.querySelector('#download-yaml').onclick=()=>{\n"
    "    const blob=new Blob([editor.yaml],{type:'application/x-yaml'}),link=document.createElement('a');\n"
    "    link.href=URL.createObjectURL(blob);link.download='ydb-bench.yaml';link.click();URL.revokeObjectURL(link.href)\n"
    '  };\n'
    "  document.querySelector('#save-host').onclick=async()=>{\n"
    "    try {const value=await editorApi('/api/drafts',jsonOptions({yaml:editor.yaml}));showMessage('Saved on host: '+value.path)}"
    '\n'
    "    catch(error){showMessage(error.message,'error')}\n"
    '  };\n'
    "  document.querySelector('#start-run').onclick=async event=>{\n"
    "    const button=event.currentTarget,host=editorHost;button.disabled=true;document.querySelector('#run-host').disabled=true;\n"
    "    try {const value=await editorApi('/api/runs',jsonOptions({yaml:editor.yaml,perf:editor.perf,continue_on_error:editor.conti"
    "nueOnError}));activeRun=host?host+':'+value.id:value.id;sessionStorage.setItem('ydb-bench-active-run',activeRun);setRoute('run/'+enc(activeRun))"
    '}\n'
    "    catch(error){showMessage(error.message,'error')}\n"
    "    finally{button.disabled=false;const hostSelect=document.querySelector('#run-host');if(hostSelect)hostSelect.disabled=false}\n"
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
function actorSystemFlag(key,checked){
  const help={use_shared_threads:'Allow executor pools to share worker threads. Default: off.',
    use_united_pool:'Enable the united executor pool implementation. Default: off.',
    use_ring_queue:'Use ring queues in the actor system. Default: on.'};
  return '<label class=actor-flag><input id="local-actor-system-'+key+'" type=checkbox '+(checked?'checked':'')+
    ' aria-describedby="flag-help-'+key+'">'+esc(key)+'<span class=flag-help role=tooltip id="flag-help-'+key+'">'+
    esc(help[key]||key)+'</span></label>'
}
function localYdbOptionField(option,value){
  const id='local-option-'+option.name;
  if(option.choices.length)return localSelect(id,option.name,value,option.choices);
  if(option.kind==='boolean')return localCheck(id,option.name,Boolean(value));
  if(option.kind==='integer'){
    const maximum=option.maximum===null?'':' max='+option.maximum;
    return localField(id,option.name,value,'','type=number min='+option.minimum+maximum)
  }
  return localField(id,option.name,value,'','type=text')
}
function localYdbOptionValue(option){
  const id='local-option-'+option.name,input=document.querySelector('#'+id);
  let value;
  if(option.choices.length){
    value=option.kind==='integer'?Number(input.value):option.kind==='boolean'?input.value==='true':input.value
  }else if(option.kind==='boolean'){
    value=input.checked
  }else if(option.kind==='integer'){
    value=localInteger(id,option.allow_zero?0:1)
  }else{
    value=input.value
  }
  if((option.kind==='string'||option.kind==='duration')&&!option.allow_empty&&!value.length){
    throw Error(option.name+' must not be empty.')
  }
  return value
}
function localYdbSloPercentile(definition,requested=null){
  const supported=Object.keys(definition.slo_metrics||{});
  if(requested&&supported.includes(requested))return requested;
  return supported.includes('p99')?'p99':supported[0]||null
}
function localYdbBinaryFields(config){
  const catalog=editor.model.binary_catalog||{},path=config.ydbd_binary||'';
  const choices=[['','Bundled ydbd'],...(catalog.ydbd||[]).map(item=>[item.path,item.version])];
  if(path&&!choices.some(([value])=>value===path))choices.push([path,'Custom path']);
  const selector='<div class=field><label for=local-ydbd-version>Version</label><select id=local-ydbd-version>'+
    choices.map(([value,label])=>'<option value="'+esc(value)+'" '+(value===path?'selected':'')+'>'+
      esc(label)+'</option>').join('')+'</select></div>';
  const notice=catalog.error?'<p class="notice error">'+esc(catalog.error)+'</p>':
    catalog.truncated?'<p class=notice>Binary catalog is truncated.</p>':'';
  const custom=path&&!(catalog.ydbd||[]).some(item=>item.path===path);
  return selector+'<details class="editor-options editor-wide" data-editor-detail=binary '+(custom?'open':'')+'><summary>Custom executable path</summary>'+
    localField('local-ydbd-binary','Executable path',path,
      'Absolute path on the benchmark host. Empty uses bundled ydbd.')+'</details>'+notice
}
function localYdbProfileEditor(profile){
  const config=profile.local_ydb,workload=config.workload,geometry=config.geometry,load=config.load,measurement=config.measurement;
  const definition=localYdbWorkloadDefinition(workload.type);
  const warmupHelp=definition.default_warmup_seconds===null?
    'Empty uses adaptive YDB CLI warmup.':
    'Empty uses the workload default ('+localYdbDefaultWarmupSeconds(definition)+' seconds).';
  const clientThreadsHelp='';
  const durationMaximum=localYdbMeasurementMaximumDuration(definition,measurement.warmup);
  const durationHelp=definition.maximum_total_seconds?
    'Warmup plus duration must not exceed '+definition.maximum_total_seconds+' seconds.':'';
  const loadMode=load.values?'points':load.objective.type;
  const sloPercentiles=Object.keys(definition.slo_metrics||{});
  const objectiveChoices=['points','maximize-throughput',...(sloPercentiles.length?['latency-slo']:[])];
  const options=definition.options.map(option=>localYdbOptionField(option,workload.options[option.name])).join('');
  const geometryLabels={static_nodes:'Static nodes',dynamic_nodes:'Dynamic nodes',max_dynamic_nodes:'Maximum dynamic nodes',
    disk_size_gb:'Disk size (GiB)',storage_groups:'Storage groups'};
  const geometryFields=Object.keys(localYdbGeometryKeys)
    .map(key=>localField('local-geometry-'+key,geometryLabels[key],geometry[key],'','type=number min=1')).join('');
  const actorSystemFields=Object.keys(localYdbActorSystemKeys)
    .map(key=>actorSystemFlag(key,Boolean(config.actor_system?.[key]??(key==='use_ring_queue')))).join('');
  const actorCpuFields=['static_nodes','dynamic_nodes'].map(role=>localField(
    'local-actor-cpu-'+role,(role==='static_nodes'?'Static':'Dynamic')+' node vCPUs',
    config.actor_system?.[role]?.cpu_count??'',
    'Per-node actor-system capacity; independent of CPU placement. Empty: automatic.',
    'type=number min=1 max=32767'
  )).join('');
  const loadCommon=
    localSelect('local-load-mode','Objective',loadMode,objectiveChoices)+
    localSelect('local-load-parameter','Parameter',load.parameter,definition.load_parameters)+
    (definition.reports_errors?localCheck(
      'local-load-allow-errors','Allow failed workload requests',Boolean(load.allow_errors),
      'Failed requests remain visible in results but do not limit load search.'
    ):'');
  const searchFields=loadMode==='points'?'':
    localField('local-load-start','Start',load.search.start,'','type=number min=1')+
    localField('local-load-maximum','Maximum',load.search.maximum,'','type=number min=1')+
    (loadMode==='latency-slo'?
      localField(
        'local-load-multiplier','Growth multiplier',load.search.multiplier,
        'Used to find the first failing latency point.','type=number min=1 step=any'
      ):'')+
    (loadMode==='maximize-throughput'?localField(
      'local-load-search-resolution-percent',
      'Ternary resolution (%)',
      load.search.resolution_percent,'','type=number min=0 max=100 step=any'
    ):'');
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
    localSelect('local-slo-percentile','Percentile',load.objective.percentile,sloPercentiles)+
    localField('local-slo-max-ms','Maximum latency (ms)',load.objective.max_ms,'','type=number min=0 step=any')+
    (definition.reports_errors?localField(
      'local-slo-max-errors','Maximum errors',load.objective.max_errors,
      load.allow_errors?'Ignored while failed requests are allowed.':'',
      'type=number min=0 '+(load.allow_errors?'disabled':'')
    ):'')+
    localField(
      'local-slo-min-achieved-rate-ratio','Minimum achieved rate ratio',load.objective.min_achieved_rate_ratio,
      '','type=number min=0 max=1 step=any'
    )+'</div>':'';
  const affinity=Object.entries(localYdbAffinityKeys).map(([key,label])=>{
    const role=config.affinity[key],disabled=role.mode==='none'?'disabled':'';
    const roleLabel={ydb_cli:'YDB CLI',static_nodes:'Static nodes',dynamic_nodes:'Dynamic nodes'}[key]||label;
    return '<div class=editor-role><strong>'+esc(roleLabel)+'</strong>'+
      localSelect('local-affinity-'+key+'-mode','Mode',role.mode,editor.model.affinity_modes)+
      localField(
        'local-affinity-'+key+'-cpus','CPUs',role.cpus??'','integer, one-chiplet, or remaining',disabled
      )+'</div>'
  }).join('');
  return '<div id=local-editor><div class=editor-grid><section><h3>Workload</h3>'+
    '<div class=form-grid>'+localSelect(
      'benchmark','Benchmark',profile.benchmark,editor.model.benchmarks.map(item=>item.name)
    )+localField('profile-name','Profile name',profile.name,'letters, digits, . _ and -')+'</div>'+
    '<div class=form-grid>'+localSelect(
      'local-workload-type','Type',workload.type,editor.model.local_ydb_workloads.map(item=>item.type)
    )+localSelect(
      'local-workload-operation','Operation',workload.operation,definition.operations
    )+'</div><div class=form-grid>'+localYdbBinaryFields(config)+
    '</div><details class=editor-options data-editor-detail=dataset><summary>Dataset settings</summary><div class=form-grid>'+options+
    '</div></details></section><section><h3>Load &amp; objective</h3><div class=form-grid>'+
    loadCommon+localField('local-client-threads','YDB CLI threads',config.client.threads,clientThreadsHelp,'type=number min=1')+
    loadFields+'</div>'+slo+'</section><section><h3>Measurement</h3><div class=form-grid>'+
    localField('local-measurement-warmup','Warmup (seconds)',measurement.warmup,warmupHelp,'type=number min=0')+
    localField(
      'local-measurement-duration','Duration (seconds)',measurement.duration,durationHelp,
      'type=number min='+(definition.minimum_duration_seconds||1)+
        (durationMaximum===null?'':' max='+durationMaximum)
    )+
    localField('local-measurement-repetitions','Repetitions',measurement.repetitions,'','type=number min=1')+
    localField(
      'local-measurement-verification-repetitions','Verification repetitions',
      measurement.verification_repetitions??0,
      'Independent holdout measurements at the selected load; 0 disables verification.',
      'type=number min=0 max=20'
    )+
    localField(
      'local-timeout','Timeout (seconds)',profile.timeout??'','empty selects the computed timeout','type=number min=1'
    )+'</div></section><section><h3>Cluster</h3><div class=form-grid>'+
    localSelect('local-geometry-preset','Preset',geometry.preset,['single','storage','custom'])+geometryFields+
    '</div><h3>Actor system (static and dynamic nodes)</h3><div class=actor-flags>'+actorSystemFields+
    '</div><div class=form-grid>'+actorCpuFields+'</div></section><section class=editor-wide><h3>CPU placement</h3>'+affinity+'</section></div>'+
    '<div class=toolbar><button class=danger id=delete-profile>Delete profile</button></div></div>'
}
function localNumber(id,minimum=1){
  const value=Number(document.querySelector('#'+id).value);
  if(!Number.isFinite(value)||value<minimum)throw Error(id+' must be a number not below '+minimum+'.');
  return value
}
function localInteger(id,minimum=1,maximum=null){
  const value=localNumber(id,minimum);
  if(!Number.isSafeInteger(value))throw Error(id+' must be an integer.');
  if(maximum!==null&&value>maximum)throw Error(id+' must not exceed '+maximum+'.');
  return value
}
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
    if(event.target.id==='local-ydbd-version'){
      if(event.target.value)config.ydbd_binary=event.target.value;else delete config.ydbd_binary;
      editor.yaml=serializeConfig(editor.model);saveDraft();renderNew();return
    }
    if(event.target.id==='local-workload-type'){
      config.workload=defaultLocalYdbWorkload(event.target.value);editor.selected=profile.key;
      const nextDefinition=localYdbWorkloadDefinition(event.target.value);
      const parameters=nextDefinition.load_parameters;
      config.client.threads=localYdbDefaultClientThreads(nextDefinition);
      config.measurement=localYdbMeasurementForWorkload(config.measurement,nextDefinition);
      config.load=localYdbLoadForWorkload(config.load,parameters,nextDefinition,config.workload);
      if(!nextDefinition.reports_errors){
        config.load.allow_errors=false;
        if(config.load.objective?.type==='latency-slo')config.load.objective.max_errors=0
      }
      if(config.load.objective?.type==='latency-slo'){
        const percentile=localYdbSloPercentile(nextDefinition,config.load.objective.percentile);
        if(percentile)config.load.objective.percentile=percentile;
        else{
          const defaults=localYdbParameterDefaults(config.load.parameter,nextDefinition,config.workload);
          config.load={
            parameter:config.load.parameter,allow_errors:false,values:[...defaults.values]
          }
        }
      }
      editor.yaml=serializeConfig(editor.model);saveDraft();renderNew();return
    }
    if(event.target.id==='local-load-parameter'){
      const parameter=event.target.value;
      if(parameter!==config.load.parameter)config.load=localYdbResetLoadParameter(
        config.load,parameter,localYdbWorkloadDefinition(config.workload.type),config.workload
      );
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
      const defaults=localYdbParameterDefaults(
        config.load.parameter,localYdbWorkloadDefinition(config.workload.type),config.workload
      );
      if(mode==='points'){
        config.load={parameter:config.load.parameter,allow_errors,values:config.load.values||[...defaults.values]}
      }else{
        const search=config.load.search||{start:defaults.start,maximum:defaults.maximum,multiplier:2,resolution_percent:2};
        const old=config.load.objective||{};
        const objective={
          type:mode,target_role:old.target_role||'dynamic',plateau_gain_percent:old.plateau_gain_percent??2,
          plateau_points:old.plateau_points||2,cpu_saturation_percent:old.cpu_saturation_percent||95
        };
        if(mode==='latency-slo')Object.assign(objective,{
          percentile:localYdbSloPercentile(
            localYdbWorkloadDefinition(config.workload.type),old.percentile
          ),max_ms:old.max_ms??10,max_errors:old.max_errors??0,
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
    const workloadDefinition=localYdbWorkloadDefinition(config.workload.type);
    for(const option of workloadDefinition.options){
      const key=option.name,value=localYdbOptionValue(option);
      if(option.maximum!==null&&value>option.maximum)throw Error(key+' must be <= '+option.maximum+'.');
      if(option.choices.length&&!option.choices.includes(value))throw Error(key+' must be one of '+option.choices.join(', ')+'.');
      config.workload.options[key]=value
    }
    config.geometry.preset=document.querySelector('#local-geometry-preset').value;
    for(const key of Object.keys(localYdbGeometryKeys)){
      config.geometry[key]=localInteger('local-geometry-'+key)
    }
    if(config.geometry.preset==='single'){
      config.geometry.dynamic_nodes=1;config.geometry.max_dynamic_nodes=1
    }
    config.client.threads=localInteger('local-client-threads');
    const binaryPath=document.querySelector('#local-ydbd-binary').value;
    if(binaryPath&&!binaryPath.startsWith('/'))throw Error('YDBD executable path must be absolute.');
    if(binaryPath)config.ydbd_binary=binaryPath;else delete config.ydbd_binary;
    config.actor_system=Object.fromEntries(Object.keys(localYdbActorSystemKeys).map(key=>[
      key,Boolean(document.querySelector('#local-actor-system-'+key)?.checked)
    ]));
    for(const role of ['static_nodes','dynamic_nodes']){
      const id='local-actor-cpu-'+role;
      if(document.querySelector('#'+id)?.value){
        const count=localInteger(id);
        if(count>32767)throw Error('Actor-system vCPUs must not exceed 32767.');
        config.actor_system[role]={cpu_count:count};
      }
    }
    const loadMode=document.querySelector('#local-load-mode').value;
    const parameter=document.querySelector('#local-load-parameter').value;
    const allow_errors=Boolean(document.querySelector('#local-load-allow-errors')?.checked);
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
          resolution_percent:loadMode==='maximize-throughput'?localNumber('local-load-search-resolution-percent',0):
            (config.load.search?.resolution_percent??2)
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
        max_ms:localNumber('local-slo-max-ms',0),
        max_errors:workloadDefinition.reports_errors?localInteger('local-slo-max-errors',0):0,
        min_achieved_rate_ratio:localNumber('local-slo-min-achieved-rate-ratio',0)
      })
    }
    config.load=localYdbClampLoad(config.load,workloadDefinition,config.workload);
    config.measurement=localYdbValidateMeasurement({
      warmup:localYdbWarmupInput('local-measurement-warmup',workloadDefinition),
      duration:localInteger('local-measurement-duration',workloadDefinition.minimum_duration_seconds||1),
      repetitions:localInteger('local-measurement-repetitions'),
      verification_repetitions:localInteger('local-measurement-verification-repetitions',0,20)
    },workloadDefinition);
    for(const key of Object.keys(localYdbAffinityKeys)){
      const mode=document.querySelector('#local-affinity-'+key+'-mode').value;
      config.affinity[key]={mode,cpus:localCpu('local-affinity-'+key+'-cpus',mode)}
    }
    const timeout=document.querySelector('#local-timeout').value.trim();
    profile.timeout=timeout===''?null:localInteger('local-timeout');
    profile.threads=[config.client.threads];profile.duration=config.measurement.duration;
    profile.repetitions=1;profile.affinity=['roles'];profile.background_load=['none'];
    editor.selected=profile.key;editor.yaml=serializeConfig(editor.model);saveDraft();
    const loadLimitInputs=Object.values(workloadDefinition.load_limits||{}).map(
      constraint=>'local-option-'+constraint.option
    );
    if(event.target.id==='local-ydbd-binary'||localYdbNeedsRerender(event.target.id,loadLimitInputs))renderNew()
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
    "  return (memoryMb?'<div class=notice>Max"
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
    """
const editorDetailState=new Map();
function rememberEditorDetails(){
  const page=document.querySelector('.new-run-page');
  if(!page)return;
  for(const detail of page.querySelectorAll('[data-editor-detail]')){
    editorDetailState.set(page.dataset.editorProfile+'|'+detail.dataset.editorDetail,detail.open)
  }
}
function restoreEditorDetails(){
  const page=document.querySelector('.new-run-page');
  if(!page)return;
  for(const detail of page.querySelectorAll('[data-editor-detail]')){
    const key=page.dataset.editorProfile+'|'+detail.dataset.editorDetail;
    if(editorDetailState.has(key))detail.open=editorDetailState.get(key)
  }
}
async function renderNew(tab){
  rememberEditorDetails();clearRefresh();const version=++editorRenderVersion;
  if(tab)sessionStorage.setItem('ydb-bench-editor-tab',tab);
  tab=sessionStorage.getItem('ydb-bench-editor-tab')||'builder';
  const hostOptions=await hostChoices(editorHost,false);await syncEditor();
  if(version!==editorRenderVersion||!['#new','#new/yaml'].includes(location.hash))return;
  editorHostOptions=hostOptions;
  if(editor.model&&!profileByKey(editor.selected))editor.selected=editor.model.profiles[0]?.key||null;
  const summary=planSummary();
  let content='<div class="new-run-page" data-editor-profile="'+esc(editor.selected||'')+'">'+
    '<div class=page-heading><h1 class=page-title>New run</h1>'+editorControls()+'</div>'+
    '<div class=tabs><a class="'+(tab==='builder'?'active':'')+'" href="#new">Builder</a>'+
    '<a class="'+(tab==='yaml'?'active':'')+'" href="#new/yaml">YAML</a></div>';
"""
    "if(tab==='yaml'){content+=editorRunOptions()+'<textarea class=yam"
    "l id=yaml-editor spellcheck=false>'+esc(editor.yaml)+'</textarea><div class=muted>Invalid YAML remains editable and is n"
    "ot overwritten by Builder.</div>';app.innerHTML=shell('new',content+'</div>');document.querySelector('#yaml-editor').oninput=even"
    't=>{editor.yaml=event.target.value;saveDraft();clearTimeout(window.ydbBenchYamlTimer);window.ydbBenchYamlTimer=setTimeou'
    't(async()=>{await syncEditor();document.querySelector(\'#editor-message\').innerHTML=editor.error?\'<div class="notice erro'
    'r">\'+esc(editor.error)+\'</div>\':\'<div class="notice good">Builder model is synchronized.</div>\'},350)};bindEditorControl'
    "s();return}if(editor.error){content+=displayError(editor.error)+'<p>Fix the YAML in the YAML tab before editing with Bui"
    "lder.</p>'+editorRunOptions();app.innerHTML=shell('new',content+'</div>');bindEditorControls();return}const selected=profileByKey(editor.selected)||"
    "editor.model.profiles[0];content+='<div class=editor-plan>'+editor.model.profiles.length+' profiles · '+summary.count+' executions'+"
    "(editor.model.profiles.some(profile=>profile.local_ydb&&!profile.local_ydb.load.values)?' · Duration depends on load search and verification':"
    "editor.model.profiles.some(profile=>profile.local_ydb)?' · Per-point measurement; startup and verification are additional':"
    "' · '+Math.ceil(summary.seconds)+' s measurement')+'</div>'"
    '+\'<section class=profile-list>\'+editor.model.profiles.map(profile=>\'<button data-profile="\'+esc(profile.key)+\'" class="\'+(profi'
    'le.key===selected?.key?\'selected\':\'\')+\'">\'+esc(profile.benchmark)+\' / \'+esc(profile.name)+\'</button>\').join(\'\')+'
    "'<button id=add-profile>+ Add profile</button></section><section>'+ (selected?profileEditor(selected):'<div class=empty>Add a benchmark profile to begin.</div>')+"
    "'</section>'+editorRunOptions()+'</div>';app.innerHTML=shell('new',content);restoreEditorDetails();bindEditorControls();document.querySelector('#add-profile').onclic"
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
    "function runHref(id,kind){return hostApiPath('/api/runs/'+enc(id)+'/'+kind)}\n"
    """
function sectionTabs(name,items){
  return '<div class=view-tabs aria-label="'+esc(name)+' views">'+items.map(([key,label],index)=>
    '<button type=button data-section-tab="'+esc(name+':'+key)+'" aria-pressed="'+(index===0)+'">'+
    esc(label)+'</button>').join('')+'</div>'
}
function bindSectionTabs(container,name){
  const buttons=[...container.querySelectorAll('[data-section-tab]')].filter(item=>item.dataset.sectionTab.startsWith(name+':'));
  const panels=[...container.querySelectorAll('[data-section-panel]')].filter(item=>item.dataset.sectionPanel.startsWith(name+':'));
  const storageKey='ydb-bench-view-'+name;
  function select(value){
    for(const button of buttons)button.setAttribute('aria-pressed',String(button.dataset.sectionTab===value));
    for(const panel of panels)panel.hidden=panel.dataset.sectionPanel!==value;
    container.dataset[name+'View']=value;
    sessionStorage.setItem(storageKey,value)
  }
  const previous=container.dataset[name+'View']||sessionStorage.getItem(storageKey);
  select(buttons.some(button=>button.dataset.sectionTab===previous)?previous:buttons[0].dataset.sectionTab);
  for(const button of buttons)button.onclick=()=>select(button.dataset.sectionTab)
}
let activeBannerLoading=false;
async function refreshActiveBanner(){
  if(activeBannerLoading||document.hidden)return;
  refreshEditorActivity();
  activeBannerLoading=true;
  try{
    const value=await api('/api/activity-status');
    activeRun=value.active_run_id||'';
    sessionStorage.setItem('ydb-bench-active-run',activeRun);
    const banner=document.querySelector('.active-run');
    if(banner)banner.innerHTML=(activeRun?'<a href="#run/'+enc(activeRun)+'">Running: '+esc(activeRun)+'</a>':'No active run')+
      (value.queued?' · Queue: '+esc(value.queued):'');
  }catch(error){
    const banner=document.querySelector('.active-run');
    if(banner)banner.textContent='Run status unavailable';
  }finally{activeBannerLoading=false}
}
let runsSort='newest';
function sortRuns(items,order){
  const timestamp=run=>Date.parse(run.started_at||run.queued_at||'')||0;
  const elapsed=run=>Number.isFinite(run.duration_seconds)?run.duration_seconds:-1;
  return [...items].sort((a,b)=>{
    const difference=order==='longest'?elapsed(b)-elapsed(a):order==='oldest'?timestamp(a)-timestamp(b):timestamp(b)-timestamp(a);
    return difference||String(b.id).localeCompare(String(a.id))
  })
}
function compactRun(run){
  const profiles=Array.isArray(run.profile_names)?run.profile_names:[],benchmarks=Array.isArray(run.benchmarks)?run.benchmarks:[];
  return '<article class=dense-run><div><div class=dense-run-meta>'+status(run.status)+'<time title="'+esc(run.started_at||run.queued_at||'')+'">'+
    esc(humanTime(run.started_at||run.queued_at))+'</time><span>'+duration(run)+'</span><span>'+
    esc(run.profiles)+' profiles · '+esc(run.repetitions)+' steps</span><span>perf '+(run.perf?'on':'off')+
    '</span><span>'+esc(run.source)+'</span><span>'+esc(run.host_name||'')+'</span></div><div class=dense-run-profiles>'+
    profiles.map(name=>'<span>'+esc(name)+'</span>').join(' · ')+
    '</div><div class=dense-run-meta><span>'+esc(benchmarks.join(' · '))+'</span><a class=dense-run-id href="#run/'+
    enc(run.id)+'">'+esc(run.run_id||run.id)+'</a><span>'+esc(run.config_path||'config snapshot')+'</span></div></div>'+
    '<details class=dense-run-actions><summary>Actions</summary><div class=actions>'+
    '<a href="#run/'+enc(run.id)+'">Open</a><a href="#new" data-repeat="'+esc(run.id)+'">Repeat</a>'+
    '<a href="'+runHref(run.id,'config')+'">YAML</a><a href="'+runHref(run.id,'manifest')+'">run.json</a>'+
    '<a href="'+runHref(run.id,'archive')+'">Archive</a></div></details></article>'
}
function bindAutomaticFilters(fields,reset,apply,connected){
  let timer;
  const update=()=>{reset.hidden=!fields.some(field=>field.value.trim())};
  const run=()=>{clearTimeout(timer);update();if(connected())apply()};
  for(const field of fields){
    field.oninput=()=>{clearTimeout(timer);update();timer=setTimeout(run,250)};
    field.onchange=run;
  }
  reset.onclick=()=>{for(const field of fields)field.value='';run()};
  update();
}
async function renderRuns(){
  clearRefresh();
  const hostOptions=await hostChoices();
  if(location.hash!=='#runs')return;
  app.innerHTML=shell('runs',runFilters()+
    '<div class=runs-toolbar><label>Host <select id=runs-host>'+hostOptions+'</select></label><label>Sort <select id=runs-sort>'+
    '<option value=newest>Newest first</option><option value=oldest>Oldest first</option>'+
    '<option value=longest>Longest first</option></select></label><div class=runs-actions><button id=open-import>Import</button>'+
    '<button id=reset-run-filters hidden>Reset filters</button>'+
    '<a class=new-run-link href="#new"><span aria-hidden=true>+</span> New run</a></div></div><div id=runs-table></div>'+
    '<dialog id=import-dialog class=import-dialog aria-labelledby=import-title><h2 id=import-title>Import results</h2>'+
    '<label for=import-file>Portable ZIP archive</label><input id=import-file type=file accept=".zip,application/zip">'+
    '<div id=import-error role=alert></div><div id=import-status role=status></div><div class=toolbar>'+
    '<button id=cancel-import>Cancel</button><button id=import-run class=primary>Import</button></div></dialog>');
  const target=document.querySelector('#runs-table'),sort=document.querySelector('#runs-sort');
  let records=[],request=0,hostErrors=[];
  sort.value=runsSort;
  function draw(){
    target.innerHTML=federationErrors(hostErrors)+(records.length?sortRuns(records,runsSort).map(compactRun).join(''):
      '<div class=empty>No runs match these filters.</div>');
    for(const item of target.querySelectorAll('[data-repeat]'))item.onclick=event=>{
      event.preventDefault();reuseRun(item.dataset.repeat)
    };
  }
  async function load(){
    const current=++request,query=new URLSearchParams();
    for(const [name,id] of Object.entries({status:'f-status',benchmark:'f-benchmark',profile:'f-profile',source:'f-source',since:'f-since',until:'f-until'})){
      const value=document.querySelector('#'+id).value.trim();if(value)query.set(name,value)
    }
    if(app.querySelector('#runs-host').value)query.set('host',app.querySelector('#runs-host').value);
    try{const value=await api('/api/federation/runs?'+query);if(current!==request||!target.isConnected)return;records=value.entries;hostErrors=value.errors;draw()}
    catch(error){if(current===request&&target.isConnected)target.innerHTML=displayError(error)}
  }
  sort.onchange=()=>{runsSort=sort.value;draw()};
  bindAutomaticFilters([...app.querySelectorAll('.filters input,.filters select'),app.querySelector('#runs-host')],
    app.querySelector('#reset-run-filters'),load,()=>target.isConnected);
  const dialog=document.querySelector('#import-dialog'),fileInput=document.querySelector('#import-file'),
    importButton=document.querySelector('#import-run'),cancelButton=document.querySelector('#cancel-import'),
    importError=document.querySelector('#import-error'),importStatus=document.querySelector('#import-status');
  let importing=false;
  document.querySelector('#open-import').onclick=()=>{
    fileInput.value='';importError.textContent='';importStatus.textContent='';dialog.showModal()
  };
  cancelButton.onclick=()=>dialog.close();
  dialog.addEventListener('cancel',event=>{if(importing)event.preventDefault()});
  importButton.onclick=async()=>{
    if(importing)return;
    const file=fileInput.files[0];
    importError.textContent='';
    if(!file){importError.textContent='Choose a portable ZIP archive first.';fileInput.focus();return}
    importing=true;importButton.disabled=true;cancelButton.disabled=true;fileInput.disabled=true;
    importStatus.textContent='Importing…';
    try{
      await api('/api/import',{method:'POST',body:await file.arrayBuffer()});
      if(dialog.isConnected){dialog.close();await load()}
    }catch(error){if(dialog.isConnected)importError.textContent=error.message}
    finally{
      importing=false;importButton.disabled=false;cancelButton.disabled=false;fileInput.disabled=false;
      importStatus.textContent=''
    }
  };
  await load()
}
    """
    "async function reuseRun(id){try{const value=await api('/api/runs/'+enc(id)+'/config.json');editor.yaml=value.yaml;editor"
    ".perf=Boolean(value.perf);editor.continueOnError=Boolean(value.continue_on_error);saveDraft();setRoute('new')}catch(erro"
    'r){alert(error.message)}}\n'
    "const chartColors=['#1b62b9','#c2410c','#087443','#7c3aed','#be185d','#0e7490','#854d0e','#94a3b8','#ef4444','#818cf8','"
    "#22c55e','#d946ef'];\n"
    "const chartPointLimit=10000;\n"
    'function metricLabel(value){const number=Number(value);if(!Number.isFinite(number))return String(value);return Math.abs('
    "number)>=1e9?(number/1e9).toFixed(2)+'B':Math.abs(number)>=1e6?(number/1e6).toFixed(2)+'M':Math.abs(number)>=1e3?(number"
    "/1e3).toFixed(2)+'k':Number.isInteger(number)?String(number):number.toFixed(2)}\n"
    "function chartNumber(value){return value===null||value===undefined||typeof value==='string'&&!value.trim()?NaN:Number(value)}\n"
    'function chartExtent(values){let minimum=Infinity,maximum=-Infinity;for(const value of values){const number=Number(value);'
    'if(!Number.isFinite(number))continue;minimum=Math.min(minimum,number);maximum=Math.max(maximum,number)}return minimum===Infinity?null:[minimum,maximum]}\n'
    "function chartSeriesLabel(series,compact=false){return compact?series.affinity:series.run+' / '+series.profile+' / '+ser"
    'ies.affinity}\n'
    "function seriesCpuNote(series){if(series.cpu_masks&&Object.keys(series.cpu_masks).length)return 'CPUs by threads: '+Obje"
    "ct.entries(series.cpu_masks).sort((left,right)=>Number(left[0])-Number(right[0])).map(([threads,cpus])=>threads+' → '+(c"
    "pus===null?'unrestricted':cpuRanges(cpus))).join('; ');return Object.hasOwn(series,'cpus')?(series.cpus===null?'CPUs: un"
    "restricted':'CPUs: '+cpuRanges(series.cpus)):'CPUs: not recorded'}\n"
    'function svgChart(metric,xName,xValues,seriesRows,colors){\n'
    '  const width=900,height=330,left=78,right=24,top=24,bottom=52,plotWidth=width-left-right,plotHeight=height-top-bottom,v'
    'alueFor=(item,row)=>chartNumber(row?.[item.metric||metric]);\n'
    '  const xKeys=new Set(xValues.map(String)),values=[];for(const item of seriesRows)for(const [x,row] of item.rows){if(!xKeys.has(String(x)))continue;'
    'const value=valueFor(item,row);if(Number.isFinite(value))values.push(value);if(values.length>chartPointLimit)return '
    "'<div class=notice>Chart omitted because it has more than '+chartPointLimit+' numeric points. Select fewer runs or lines.</div>'}\n"
    "  if(!values.length)return '<div class=empty>No numeric values for '+esc(metric)+'.</div>';\n"
    '  let [yMin,yMax]=chartExtent(values);const nonnegative=yMin>=0;if(yMin===yMax){const pad=Math.abs(yMin)*.05||1;yMin-=pad;yMax+='
    'pad}else{const pad=(yMax-yMin)*.08;yMin-=pad;yMax+=pad}if(nonnegative)yMin=0;\n'
    '  const [xMin,xMax]=chartExtent(xValues),xPos=value=>left+(xMax===xMin'
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
    ';const plottedX=item.connectMeasuredPoints?xValues.filter(x=>item.rows.has(String(x))):xValues;for(const x of plottedX){'
    'const row=item.rows.get(String(x)),y=valueFor(item,row);if(Number.isFinite(y)){segment.push({x'
    ',y,row});continue}if(segment.length){segments.push(segment);segment=[]}}if(segment.length)segments.push(segment);for(con'
    'st points of segments)svg+=\'<polyline class=chart-line stroke="\'+color+\'" points="\'+points.map(point=>xPos(point.x)+'
    '\',\'+yPos(point.y)).join(\' \')+\'"/>\';for(const point of segments.flat())svg+=\'<circle class=chart-point fill="\'+color+\'"'
    ' cx="\'+xPos(point.x)+\'" cy="\'+yPos(point.y)+\'" r="4"><title>\'+esc(item.label+\'; \'+xName+\'=\'+point.x+\'; \'+(item.me'
    'tric||metric)+\'=\'+point.y)+\'</title></circle>\'});\n'
    '  svg+=\'<line class=chart-cursor x1="0" y1="\'+top+\'" x2="0" y2="\'+(top+plotHeight)+\'" visibility="hidden"/>\';\n'
    "  return '<div class=chart-surface>'+svg+'</svg><div class=chart-tooltip hidden></div></div>'\n"
    '}\n'
    """
function bindChartTooltips(container,xName,xValues,seriesRows,metrics,colors,synchronize=false,formatValue=metricLabel){
  const seriesFor=metric=>Array.isArray(seriesRows)?seriesRows:(seriesRows[metric]||[]);
  const panels=[...container.querySelectorAll('.chart-panel')].map(panel=>({
    panel,
    metric:panel.dataset.metric,
    svg:panel.querySelector('svg'),
    surface:panel.querySelector('.chart-surface'),
    tooltip:panel.querySelector('.chart-tooltip'),
    cursor:panel.querySelector('.chart-cursor'),
  })).filter(item=>item.svg&&item.surface&&item.tooltip&&item.cursor&&metrics.includes(item.metric));
  const xExtent=chartExtent(xValues);if(!panels.length||!xExtent)return;
  const width=900,left=78,right=24,plotWidth=width-left-right,[xMin,xMax]=xExtent;
  const xPos=value=>left+(xMax===xMin?plotWidth/2:(Number(value)-xMin)/(xMax-xMin)*plotWidth);
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
          '</span><span class=tooltip-value>'+esc(formatValue(item.value))+'</span></div>').join('');
      active.tooltip.hidden=false;
      const surfaceBounds=active.surface.getBoundingClientRect(),tooltipWidth=active.tooltip.offsetWidth;
      const rawLeft=event.clientX-surfaceBounds.left+12;
      active.tooltip.style.left=Math.max(4,Math.min(rawLeft,surfaceBounds.width-tooltipWidth-4))+'px';
      active.tooltip.style.top=Math.max(4,event.clientY-surfaceBounds.top-active.tooltip.offsetHeight-10)+'px'
    }
  }
}
"""
    "async function loadChartData(runIds,benchmark=null){const ref=runIds.length===1?splitRunRef(runIds[0]):null;"
    "const query=new URLSearchParams;for(const run of runIds)query.append('run',ref?ref.id:run);"
    "if(benchmark)query.set('benchmark',benchmark);return api((ref?'/api/hosts/'+enc(ref.host):'')+'/api/chart-data?'+query)}\n"
    "async function loadLocalYdbComparison(runIds){const query=new URLSearchParams;for(const run of runIds)query.append('run',run);"
    "const result=await api('/api/federation/profiles?'+query);if(result.errors?.length)throw Error(result.errors.map(e=>e.host_name+': '+e.error).join('; '));return result}\n"
    "async function loadLocalYdbActivity(runId,profile,after){const query=new URLSearchParams({profile,after:String(after)});return api('/api/runs/'+enc(runId)+'/local-ydb-activity?'+query)}\n"
    "function chartMetricTitle(data,metric){const metadata=data.metric_metadata?.[metric]||{};return metadata.unit?metric+' ('+metadata.unit+')':metric}\n"
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
    "t class=query-metric>'+data.metrics.map(metric=>'<option value=\"'+esc(metric)+'\" '+(metric===(query.metric||[...state.ys][0])?'selected':'')+'>'"
    "+esc(chartMetricTitle(data,metric))+'</option>').join('')+'</select></span>'+facetNames.map(name=>{const listId='query-values-'+index+'-'+name;"
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
    "ic of item.metrics)indexed.push({item,rows,metric,label:item.label+(selectedMetrics.length>1?' · '+chartMetricTitle(data,metric):''),colorIndex"
    ':indexed.length})}\n'
    '    const sets=indexed.map(item=>new Set([...item.rows].filter(([,row])=>Number.isFinite(chartNumber(row[item.metric]))).map('
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
    "    const metricTitle=selectedMetrics.map(metric=>chartMetricTitle(data,metric)).join(', '),chartTitle=scope.title?metricTitle+' — '+scope.title:metricTitle;output."
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
    "function localComparisonKey(item){return JSON.stringify([item.run,item.profile])}\n"
    "function localComparisonId(item){return (item.host_name?item.host_name+' / ':'')+(item.run_id||item.run)+' / '+item.profile}\n"
    'function localComparisonConfig(item){\n'
    '  const parameters=item.parameters||{},workload=parameters.workload||{},geometry=parameters.geometry||{},client=parameters.client||{};\n'
    '  const measurement=parameters.measurement||{},load=parameters.load||{},objective=load.objective||{};\n'
    "  const warmup=Object.prototype.hasOwnProperty.call(measurement,'warmup')?"
    "(measurement.warmup===null?'automatic':measurement.warmup):'—';\n"
    "  const values={\n"
    "    'Workload':workload.type??'—','Operation':workload.operation??'—','Load parameter':load.parameter??'—',\n"
    "    'Load values':JSON.stringify(localComparisonStable(load.values??null)),\n"
    "    'Objective':objective.type??'points','Warmup seconds':warmup,\n"
    "    'Duration seconds':measurement.duration??'—','Repetitions':measurement.repetitions??'—',\n"
    "    'Verification repetitions':measurement.verification_repetitions??0,\n"
    "    'use_shared_threads':parameters.actor_system?.use_shared_threads??false,\n"
    "    'Static node vCPUs':parameters.actor_system?.static_nodes?.cpu_count??'automatic',\n"
    "    'Dynamic node vCPUs':parameters.actor_system?.dynamic_nodes?.cpu_count??'automatic',\n"
    "    'use_united_pool':parameters.actor_system?.use_united_pool??false,\n"
    "    'use_ring_queue':parameters.actor_system?.use_ring_queue??true,\n"
    "    'Geometry preset':geometry.preset??'—','Static nodes':geometry.static_nodes??'—',\n"
    "    'Initial dynamic nodes':geometry.dynamic_nodes??'—','Maximum dynamic nodes':geometry.max_dynamic_nodes??'—',\n"
    "    'Storage groups':geometry.storage_groups??'—','Disk size GiB':geometry.disk_size_gb??'—','YDB CLI threads':client.threads??'—',\n"
    "    'Affinity config':JSON.stringify(localComparisonStable(parameters.affinity??null)),\n"
    "    'YDB CLI CPUs':JSON.stringify(localComparisonStable(item.role_affinity?.ydb_cli??null)),\n"
    "    'Static CPUs':JSON.stringify(localComparisonStable(item.role_affinity?.static_nodes??null)),\n"
    "    'Dynamic CPUs':JSON.stringify(localComparisonStable(item.role_affinity?.dynamic_nodes??null))\n"
    "  };for(const [name,value] of Object.entries(workload.options||{}))values['Option '+name]=value;\n"
    "  for(const [name,value] of Object.entries(load.search||{}))values['Search '+name]=value;\n"
    "  for(const [name,value] of Object.entries(objective))values['Objective '+name]=value;\n"
    "  values['Allow errors']=Boolean(load.allow_errors);return values\n"
    '}\n'
    'function localComparisonContext(item){return {\n'
    "  'Host':item.platform?.uname?.node??'—','CPU':item.platform?.cpu_model??'—',\n"
    "  'Kernel':item.platform?.uname?.release??'—','CPU topology':JSON.stringify(localComparisonStable(item.cpu_topology??null)),\n"
    "  'YDB CLI build':item.binaries?.ydb_cli?.sha256??'—',\n"
    "  'Verification cluster':localResultMetrics(item.result).verified?localVerificationClusterLabel(item.verification):'not applicable'\n"
    '}}\n'
    "function localComparisonBuild(item){return {'ydbd':item.binaries?.ydbd?.sha256??'—','Tool revision':item.tool_revision??'—'}}\n"
    'function localComparisonStable(value){\n'
    "  if(Array.isArray(value))return value.map(localComparisonStable);\n"
    "  if(value&&typeof value==='object')return Object.fromEntries(Object.entries(value)\n"
    '    .sort(([left],[right])=>left.localeCompare(right))\n'
    '    .map(([name,item])=>[name,localComparisonStable(item)]));\n'
    '  return value\n'
    '}\n'
    'function localComparisonSemantic(item){\n'
    '  const parameters=item.parameters||{},load=parameters.load||{},objective=load.objective||{type:\'points\'};\n'
    '  const schema=localResultSchema(item);return localComparisonStable({\n'
    '    workload:parameters.workload||{},parameter:load.parameter,objective:objective.type,\n'
    "    latency_percentile:objective.type==='latency-slo'?objective.percentile:null,\n"
    "    slo_metric:objective.type==='latency-slo'?localSloMetric(schema,objective.percentile):null,\n"
    '    result_schema_id:schema.schema_id,throughput_unit:schema.throughput_unit\n'
    '  })\n'
    '}\n'
    'function localResultMetrics(result,schema=localLegacyResultSchema(null)){\n'
    "  const verified=Boolean(result?.metrics_source==='verification'&&result?.verified_metrics&&\n"
    "    typeof result.verified_metrics==='object');\n"
    "  const raw=(verified?result.verified_metrics:result?.selected_metrics)||{};\n"
    '  const metrics=Number(raw.empty_repetitions)>0?{\n'
    '    ...raw,...Object.fromEntries(Object.values(schema.slo_metrics||{}).map(name=>[name,null]))\n'
    '  }:raw;\n'
    "  return {metrics,verified,source:verified?(result.verification_mode==='adaptive'?'Adaptive verification':'Holdout'):'Search'}\n"
    '}\n'
    'function localVerificationCount(result,parameters={},verification={}){\n'
    '  return result?.verification_repetitions??verification?.configured_repetitions??verification?.completed_repetitions??\n'
    '    parameters?.measurement?.verification_repetitions??0\n'
    '}\n'
    'function localVerificationClusterLabel(verification={}){\n'
    "  if(verification?.cluster==='fresh')return 'fresh cluster';\n"
    "  if(verification?.cluster==='search')return 'retained search cluster';\n"
    "  return 'unknown cluster'\n"
    '}\n'
    'function localVerificationBadge(result,parameters={},verification={}){\n'
    '  const view=localResultMetrics(result);if(!view.verified)return \'\';\n'
    '  const repetitions=localVerificationCount(result,parameters,verification);\n'
    '  const accepted=result?.holdout_accepted??verification?.accepted;\n'
    "  const adaptive=result?.verification_mode==='adaptive';\n"
    "  return '<span class=\"verification-badge '+(accepted===false?'bad':'')+'\" title=\"'+\n"
    "    (adaptive?'Adaptive verification participates in load selection':'Independent holdout measurements')+'\">'+(adaptive?'Verified':'Holdout')+\n"
    "    (repetitions?' · '+esc(repetitions):'')+'</span>'\n"
    '}\n'
    'function localComparisonDelta(value,baseline,direction=null,compatible=true){\n'
    "  if(!compatible)return '<span class=muted>incompatible</span>';\n"
    "  if(value===null||value===undefined||value===''||baseline===null||baseline===undefined||baseline==='')return '—';\n"
    '  const current=Number(value),reference=Number(baseline);\n'
    "  if(!Number.isFinite(current)||!Number.isFinite(reference))return '—';\n"
    "  if(reference===0){if(current===0)return '<span class=\"comparison-delta\">0</span>';\n"
    "    return direction==='lower'?'<span class=\"comparison-delta bad\">+'+metricLabel(current)+'</span>':'—'};\n"
    "  const delta=(current/reference-1)*100,good=direction==='lower'?delta<0:direction==='higher'&&delta>0;\n"
    "  const bad=direction==='lower'?delta>0:direction==='higher'&&delta<0;\n"
    "  return '<span class=\"comparison-delta '+(good?'good':bad?'bad':'')+'\">'+(delta>0?'+':'')+delta.toFixed(1)+'%</span>'\n"
    '}\n'
    """
function mountLocalYdbComparison(container,data,chartData=null){
  const all=data.entries||[];
  if(!all.length){container.innerHTML='<div class=empty>No local YDB profiles in the selected runs.</div>';return}
  const stateKey='ydb-bench-comparison-profiles:'+JSON.stringify([...new Set(all.map(item=>item.run))].sort());
  if(!container.dataset.restored&&!data.readonly){
    try{Object.assign(container.dataset,JSON.parse(sessionStorage.getItem(stateKey)||'{}'))}catch{}
    container.dataset.restored='true';
  }
  const remember=()=>{
    if(data.readonly)return;
    const state=Object.fromEntries(['profiles','baseline','cpu','allConfig'].filter(key=>container.dataset[key]!==undefined)
      .map(key=>[key,container.dataset[key]]));
    try{sessionStorage.setItem(stateKey,JSON.stringify(state))}catch{}
  };
  const saved=container.dataset.profiles?JSON.parse(container.dataset.profiles):all.map(localComparisonKey);
  const entries=all.filter(item=>saved.includes(localComparisonKey(item)));
  const baseline=entries.find(item=>localComparisonKey(item)===container.dataset.baseline)||entries[0];
  const options=all.map(item=>'<label><input type=checkbox data-comparison-profile value="'+
    esc(localComparisonKey(item))+'" '+(entries.includes(item)?'checked':'')+'> '+esc(localComparisonId(item))+'</label>').join('');
  const toolbar=data.readonly?'':'<div class=toolbar><details><summary>Profiles · '+entries.length+'</summary><div class=series-picker>'+
    options+'</div><button type=button data-apply-profiles>Apply</button></details>'+
    (baseline?'<label>Baseline <select data-baseline>'+entries.map(item=>'<option value="'+esc(localComparisonKey(item))+
      '" '+(item===baseline?'selected':'')+'>'+esc(localComparisonId(item))+'</option>').join('')+'</select></label>':'')+'</div>';
  if(!baseline){
    container.innerHTML=toolbar+'<div class=empty>Select profiles to compare.</div>';
  }else{
    container.dataset.baseline=localComparisonKey(baseline);
    const schema=localResultSchema(baseline),view=localResultMetrics(baseline.result,schema);
    const semantic=JSON.stringify(localComparisonSemantic(baseline));
    const showCpu=container.dataset.cpu==='true';
    const rows=entries.map(item=>{
      const currentSchema=localResultSchema(item),currentView=localResultMetrics(item.result,currentSchema);
      const metrics=currentView.metrics,objective=item.parameters?.load?.objective||{};
      const [percentile,latencyMetric]=localPreferredSlo(currentSchema,objective);
      const latency=metrics[latencyMetric];
      const compatible=JSON.stringify(localComparisonSemantic(item))===semantic&&currentView.source===view.source;
      const incompatibility=currentView.source!==view.source?'Incompatible metric source':'Incompatible workload or result schema';
      const slo=objective.type==='latency-slo';
      const known=latency!==null&&latency!==undefined&&Number.isFinite(Number(latency));
      const passing=known&&Number(latency)<=Number(objective.max_ms);
      const href='#run/'+enc(item.run)+'/profile/'+enc('local-ydb/'+item.profile);
      return '<tr><td><a href="'+esc(href)+'"><strong>'+esc(item.profile)+'</strong></a>'+
        (item===baseline?' <span class=muted>Baseline</span>':'')+
        '<div class=muted>'+esc(item.host_name||'')+'</div><div class=muted title="'+esc(item.run_id||item.run)+'">'+
        esc(item.started_at?humanTime(item.started_at):(item.run_id||item.run))+' · '+esc(currentView.source)+'</div>'+
        '<div class=muted>'+esc(localSearchAxisLabel(item.parameters?.load?.parameter||'load',
          item.parameters?.workload?.type))+': '+esc(metricLabel(item.result?.selected_load??'—'))+'</div>'+
        (['passed','completed'].includes(item.state)?'':'<div class=muted>Profile state: '+esc(item.state??'—')+'</div>')+'</td>'+
        '<td>'+esc(metricLabel(metrics.throughput??'—'))+' <span class=muted>'+esc(currentSchema.throughput_unit)+'</span>'+
        '<div>'+(item===baseline?'—':compatible?localComparisonDelta(metrics.throughput,view.metrics.throughput):
          '<span class=muted>'+esc(incompatibility)+'</span>')+'</div></td>'+
        '<td>'+esc(metricLabel(latency??'—'))+(known?' ms':'')+' <span class=muted>'+esc(percentile??'')+'</span></td>'+
        '<td>'+esc(metricLabel(metrics.errors??'—'))+(item.parameters?.load?.allow_errors?' <span class=muted>allowed</span>':'')+'</td>'+
        '<td'+(slo&&known?' class="'+(passing?'good':'bad')+'"':'')+'>'+
        (slo?(known?(passing?'Satisfied':'Exceeded'):'Unknown'):'—')+
        (slo?'<div class=muted>'+esc(objective.percentile)+' ≤ '+esc(objective.max_ms)+' ms</div>':'')+'</td>'+
        (showCpu?['static_cpu_mean','dynamic_cpu_mean','cli_cpu_mean'].map(key=>'<td>'+
          esc(metricLabel(metrics[key]??'—'))+(metrics[key]!==undefined?'%':'')+'</td>').join(''):'')+'</tr>'
    }).join('');
    const groups=[
      ['Profile configuration',localComparisonConfig],
      ['Environment',localComparisonContext],
      ['Build',localComparisonBuild]
    ];
    let configRows='',differenceCount=0;
    for(const [title,project] of groups){
      const values=entries.map(project);
      const names=[...new Set(values.flatMap(Object.keys))];
      let groupRows='';
      for(const name of names){
        const cells=values.map(value=>value[name]??'—');
        const different=new Set(cells.map(value=>JSON.stringify(localComparisonStable(value)))).size>1;
        if(different)differenceCount++;
        if(!different&&container.dataset.allConfig!=='true')continue;
        const reference=cells[entries.indexOf(baseline)];
        groupRows+='<tr><th>'+esc(name)+'</th>'+cells.map(value=>{
          const full=typeof value==='object'?JSON.stringify(localComparisonStable(value)):String(value);
          const revision=value&&typeof value==='object'?(value.commit_id||value.hash):null;
          const short=revision?String(revision).slice(0,12)+' · '+(value.build_type||'unknown build'):
            /^[a-f0-9]{40,64}$/.test(full)?full.slice(0,12):full;
          return '<td'+(JSON.stringify(value)!==JSON.stringify(reference)?' class=comparison-config-changed':'')+
            '><span title="'+esc(full)+'">'+esc(short)+'</span></td>'
        }).join('')+'</tr>';
      }
      if(groupRows)configRows+='<tr><th colspan="'+(entries.length+1)+'">'+esc(title)+'</th></tr>'+groupRows;
    }
    container.innerHTML=toolbar+sectionTabs('comparison',[['results','Results'],['configuration','Configuration']])+
      '<div data-section-panel="comparison:results"><label><input type=checkbox data-comparison-cpu '+
      (showCpu?'checked':'')+'> CPU metrics</label><div class=local-attempts-scroll><table class="local-attempts comparison-results">'+
      '<thead><tr><th>Profile</th><th>Throughput · Δ vs baseline</th><th>Latency</th><th>Errors</th><th>SLO</th>'+
      (showCpu?'<th>Static CPU</th><th>Dynamic CPU</th><th>YDB CLI CPU</th>':'')+'</tr></thead><tbody>'+
      rows+'</tbody></table></div></div><div data-section-panel="comparison:configuration" hidden>'+
      '<label><input type=checkbox data-only-differences '+(container.dataset.allConfig==='true'?'':'checked')+
      '> Only differences</label> <span class=muted>'+differenceCount+' differing parameters</span>'+
      '<div class=local-attempts-scroll><table class=local-attempts><thead><tr><th>Parameter</th>'+
      entries.map(item=>'<th>'+esc(item.profile)+'<div class=muted>'+esc(item.host_name||'')+' · '+esc(item.run_id||item.run)+
        (item===baseline?' · Baseline':'')+'</div></th>').join('')+'</tr></thead><tbody>'+
      (configRows||'<tr><td colspan="'+(entries.length+1)+'">No configuration differences.</td></tr>')+
      '</tbody></table></div></div>';
    bindSectionTabs(container,'comparison');
    if(!data.readonly)container.querySelector('[data-baseline]').onchange=event=>{
      container.dataset.baseline=event.target.value;remember();mountLocalYdbComparison(container,data)
    };
    container.querySelector('[data-comparison-cpu]').onchange=event=>{
      container.dataset.cpu=String(event.target.checked);remember();mountLocalYdbComparison(container,data)
    };
    container.querySelector('[data-only-differences]').onchange=event=>{
      container.dataset.allConfig=String(!event.target.checked);remember();mountLocalYdbComparison(container,data)
    };
  }
  if(!data.readonly)container.querySelector('[data-apply-profiles]').onclick=()=>{
    container.dataset.profiles=JSON.stringify([...container.querySelectorAll('[data-comparison-profile]:checked')].map(input=>input.value));
    remember();mountLocalYdbComparison(container,data)
  }
}
    """
    """
const localPhaseLabels={
  'preparing-cluster':'Preparing cluster','starting-static-nodes':'Starting static nodes','waiting-for-static-nodes':'Waiting for static nodes',
  'bootstrapping-cluster':'Bootstrapping cluster','creating-database':'Creating database','starting-dynamic-nodes':'Starting dynamic nodes',
  'waiting-for-database':'Waiting for database','waiting-for-client-endpoints':'Waiting for client endpoints',
  'cluster-ready':'Cluster ready','initializing-workload':'Initializing workload',
  'warming-up':'Warming up','measuring':'Measuring','cleaning-workload':'Cleaning workload','evaluating-attempt':'Evaluating attempt',
  'verification-initializing':'Preparing verification workload','verification-warmup':'Warming up verification',
  'verification-measuring':'Measuring verification','verification-cleanup':'Cleaning verification workload',
  'verification-evaluating':'Evaluating verification','verification-completed':'Verification completed',
  'resuming-search':'Resuming search after rejected verification',
  'scaling-dynamic-nodes':'Scaling dynamic nodes','restarting-verification-cluster':'Restarting verification cluster',
  'stopping-cluster':'Stopping cluster','finishing':'Writing results',
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
function localActivityTime(value){
  const date=new Date(value);
  return Number.isFinite(date.valueOf())?date.toLocaleTimeString([],{hour:'2-digit',minute:'2-digit',second:'2-digit'}):'—'
}
function localActivityLabel(item){
  if(item.type==='step-started')return 'Profile started';
  if(item.type==='step-finished')return 'Profile '+(item.state||'finished');
  return localPhaseLabel(item.phase)
}
function localActivityContext(item){
  const verification=item.verification&&typeof item.verification==='object'?item.verification:null;
  return [
    item.search_stage?'stage '+item.search_stage:null,
    item.attempt?'attempt #'+item.attempt:null,
    item.repetition?'repetition '+item.repetition+'/'+(item.repetitions||'?'):null,
    item.load!==undefined?(item.parameter||'load')+' '+metricLabel(item.load):null,
    item.dynamic_nodes!==undefined?item.dynamic_nodes+' dynamic':null,
    item.target_dynamic_nodes!==undefined?'target '+item.target_dynamic_nodes+' dynamic':null,
    item.passed===true?'passed':item.passed===false?'failed':null,
    verification?.completed_repetitions!==undefined?
      'verification '+verification.completed_repetitions+'/'+(verification.configured_repetitions||'?'):null,
    verification?.accepted===true?'holdout accepted':verification?.accepted===false?'holdout rejected':null,
    item.decision||verification?.decision||item.reason||item.error||null
  ].filter(Boolean).join(' · ')
}
function localActivityLog(events,truncated,open,error=''){
  const rows=events.map(item=>{
    const command=item.current_command?.argv?.length?'<details class=local-activity-command><summary>Command</summary>'+
      '<pre class=local-command-code><code>'+esc(localCommandText(item.current_command))+'</code></pre></details>':'';
    const context=localActivityContext(item);
    return '<li class=local-activity-item><time class=local-activity-time datetime="'+esc(item.at||'')+'">'+
      esc(localActivityTime(item.at))+'</time><div><strong>'+esc(localActivityLabel(item))+'</strong>'+
      (context?'<div class=muted>'+esc(context)+'</div>':'')+'</div>'+command+'</li>'
  }).join('');
  return '<details class=local-activity data-local-activity'+(open?' open':'')+'><summary><strong>Recent activity</strong> '+
    '<span class=muted>'+events.length+' events</span></summary>'+
    (truncated?'<p class=muted>Earlier activity was omitted; recent events are bounded for display.</p>':'')+
    (error?'<p class="notice error">'+esc(error)+'</p>':'')+
    (rows?'<ol class=local-activity-log data-local-activity-log>'+rows+'</ol>':
      '<p class=muted>No profile activity has been recorded yet.</p>')+'</details>'
}
function localRestoreActivityScroll(container,scrollTop,pinned){
  const log=container.querySelector('[data-local-activity-log]');
  if(!log)return;
  log.scrollTop=pinned?log.scrollHeight:Math.min(scrollTop,Math.max(0,log.scrollHeight-log.clientHeight))
}
function localProfileDetails(data,open){
  const configuration={
    parameters:data.parameters||{},binaries:data.binaries||{},
    timeout_seconds:data.timeout_seconds??null,role_affinity:data.role_affinity||{}
  };
  return '<details class=local-profile-config data-local-profile-config'+(open?' open':'')+
    '><summary><strong>Launch parameters</strong> <span class=muted>Normalized profile and effective CPU affinity; '+
    'exact commands are listed per attempt.</span></summary><pre><code>'+esc(JSON.stringify(configuration,null,2))+
    '</code></pre></details>'
}
function localVerificationSummary(data){
  const result=data.result||{},view=localResultMetrics(result);
  const adaptive=result.verification_mode==='adaptive';
  const verification=data.verification||{},repetitions=localVerificationCount(
    result,data.parameters,verification
  );
  if(!view.verified){
    if(verification.status==='running'||verification.status==='pending'){
      return '<div class=notice><strong>Verification in progress.</strong> '+
        esc(verification.completed_repetitions??0)+'/'+esc(repetitions)+
        (adaptive?' verification':' independent')+' repetitions completed; KPIs still show the search measurement.</div>'
    }
    if(verification.status==='skipped'){
      return '<div class=notice><strong>Search measurement only.</strong> Verification was skipped: '+
        esc(verification.reason||'no feasible load was selected')+'.</div>'
    }
    if(verification.status==='failed'||verification.status==='cancelled'){
      return '<div class="notice error"><strong>Verification '+esc(verification.status)+'.</strong> '+
        esc(verification.error||'The independent holdout did not complete')+
        '. KPIs remain based on the search measurement.</div>'
    }
    return '<div class=notice><strong>Search measurement only.</strong> Configure verification repetitions to '+
      'publish independent holdout metrics.</div>'
  }
  const detail=repetitions?
    repetitions+(adaptive?' verification':' independent holdout')+' repetition'+(Number(repetitions)===1?'':'s'):
    (adaptive?'Adaptive verification measurements':'Independent holdout measurements');
  const outcomes=[];
  if(verification.cluster)outcomes.push(localVerificationClusterLabel(verification));
  if(verification.decision){
    outcomes.push((verification.evaluation_kind==='objective'?'Objective':'Validity check')+': '+verification.decision)
  }
  if(verification.throughput_delta_percent!==null&&verification.throughput_delta_percent!==undefined&&
      Number.isFinite(Number(verification.throughput_delta_percent)))outcomes.push(
    (Number(verification.throughput_delta_percent)>0?'+':'')+
      Number(verification.throughput_delta_percent).toFixed(1)+'% throughput vs search'
  );
  if(verification.saturated_repetitions!==undefined)outcomes.push(
    verification.saturated_repetitions+'/'+repetitions+' CPU-saturated'
  );
  const failed=(result.holdout_accepted??verification.accepted)===false;
  return '<div class="verification-summary '+(failed?'bad':'')+'">'+localVerificationBadge(
    result,data.parameters,verification
  )+' <strong>'+(adaptive?'Reported metrics come from adaptive verification.':
    'Reported metrics come from the independent holdout.')+'</strong> '+esc(detail)+
    ' at the selected load.'+(outcomes.length?' '+esc(outcomes.join(' · '))+'.':'')+
    ' Search measurements remain available in the attempt history.</div>'
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
    'bounded-by-invalid-sample':'Bounded by invalid measurement',
    'search-limit-reached':'Search limit reached'
  })[outcome]||outcome||'Search in progress'
}
function localSearchAxisLabel(parameter,workload){
  if(parameter==='threads')return 'YDB CLI threads';
  if(parameter==='rate')return workload==='stock'?'Offered rate (query operations/s)':
    'Offered rate (requests/s)';
  return parameter||'Search value'
}
function localLegacyResultSchema(workload){
  const throughputUnit={kv:'requests/s',stock:'query operations/s'}[workload]||'operations/s';
  return {
    schema_id:'generic-total-v1',throughput_unit:throughputUnit,reports_errors:true,
    metrics:[
      ['transactions','operations','median'],['throughput',throughputUnit,'median'],
      ['retries','retries','median'],['errors','errors','sum'],
      ['p50_ms','ms','median'],['p95_ms','ms','median'],['p99_ms','ms','median'],['pmax_ms','ms','median']
    ].map(([name,unit,repetition_aggregation])=>({name,unit,repetition_aggregation,required:true})),
    slo_metrics:{p50:'p50_ms',p95:'p95_ms',p99:'p99_ms',pmax:'pmax_ms'}
  }
}
function localResultSchema(value){
  const schema=value?.workload_result_schema;
  if(schema&&typeof schema==='object'&&Array.isArray(schema.metrics)&&schema.schema_id){
    if(value?.parameters?.workload?.type==='stock'&&schema.schema_id==='generic-total-v1'&&
      schema.throughput_unit==='transactions/s'){
      const throughputUnit='query operations/s';
      return {...schema,throughput_unit:throughputUnit,metrics:schema.metrics.map(metric=>
        metric.name==='throughput'?{...metric,unit:throughputUnit}:metric
      )}
    }
    return schema
  }
  return localLegacyResultSchema(value?.parameters?.workload?.type)
}
function localYdbThroughputUnit(value){
  return typeof value==='string'?localLegacyResultSchema(value).throughput_unit:
    localResultSchema(value).throughput_unit
}
function localMetricDescriptor(schema,name){return (schema.metrics||[]).find(item=>item.name===name)||null}
function localSloMetric(schema,percentile){return schema.slo_metrics?.[percentile]||null}
function localPreferredSlo(schema,objective={}){
  const requested=objective.type==='latency-slo'?objective.percentile:null;
  if(requested&&localSloMetric(schema,requested))return [requested,localSloMetric(schema,requested)];
  if(localSloMetric(schema,'p99'))return ['p99',localSloMetric(schema,'p99')];
  return Object.entries(schema.slo_metrics||{})[0]||[null,null]
}
function localMetricLabel(schema,name){
  if(schema.schema_id==='generic-total-v1'){
    if(name==='throughput')return 'Throughput';
    if(name==='errors')return 'Errors';
    const percentile=Object.entries(schema.slo_metrics||{}).find(([,metric])=>metric===name)?.[0];
    if(percentile)return percentile
  }
  if(name==='throughput')return 'Achieved throughput';
  const percentile=Object.entries(schema.slo_metrics||{}).find(([,metric])=>metric===name)?.[0];
  return percentile?percentile+' · '+name:name.replaceAll('_',' ')
}
function localMetricDirection(schema,name){
  if(name==='throughput')return 'higher';
  if(name==='errors'||name==='retries'||Object.values(schema.slo_metrics||{}).includes(name))return 'lower';
  return null
}
function localDisplayedMetrics(schema,objective={}){
  if(schema.schema_id!=='generic-total-v1')return schema.metrics||[];
  const [,sloMetric]=localPreferredSlo(schema,objective);
  return ['throughput',sloMetric,'errors'].map(name=>localMetricDescriptor(schema,name)).filter(Boolean)
}
function localComparisonCurveMetrics(schema,objective={}){
  return schema.schema_id==='generic-total-v1'?localDisplayedMetrics(schema,objective):schema.metrics||[]
}
function localAttemptMetric(item,metric,schema){
  return Number(item.empty_repetitions)>0&&Object.values(schema.slo_metrics||{}).includes(metric)?'—':item[metric]
}
function localAttemptRows(attempts,schema,xField='attempt'){
  const invalidMetrics=new Set(Object.values(schema.slo_metrics||{}));
  return new Map(attempts.map(item=>[String(item[xField]),Number(item.empty_repetitions)>0?{
    ...item,...Object.fromEntries([...invalidMetrics].map(name=>[name,null]))
  }:item]))
}
function localChart(title,metric,xName,xValues,series){
  return '<section class=chart-panel data-metric="'+esc(metric)+'"><h3>'+esc(title)+'</h3>'+
    svgChart(metric,xName,xValues,series,chartColors)+'</section>'
}
function localBestRows(attempts,objective,xField='attempt'){
  let bestLoad=null,currentStage=null,stageAttempts=[];const rows=new Map;
  for(const item of attempts){
    if(currentStage!==item.search_stage){currentStage=item.search_stage;bestLoad=null;stageAttempts=[]}
    stageAttempts.push(item);
    if(item.passed){
      if(objective?.type==='latency-slo'){
        if(bestLoad===null||item.load>bestLoad)bestLoad=item.load
      }else if(objective?.type==='maximize-throughput'){
        const saturated=stageAttempts.filter(value=>value.passed&&value.target_cpu_saturated);
        if(saturated.length){
          const bestThroughput=Math.max(...saturated.map(value=>Number(value.throughput)));
          const minimumThroughput=bestThroughput*(1-Number(objective.plateau_gain_percent||0)/100);
          bestLoad=Math.min(...saturated.filter(value=>Number(value.throughput)>=minimumThroughput).map(value=>value.load))
        }
      }else{
        const best=stageAttempts.filter(value=>value.passed).sort(
          (left,right)=>Number(right.throughput)-Number(left.throughput)||left.load-right.load
        )[0];
        bestLoad=best?.load??null
      }
    }
    rows.set(String(item[xField]),{...item,current_best:bestLoad,failed_load:item.passed?null:item.load})
  }
  return rows
}
function localMetricPresent(metrics,name){
  return metrics&&metrics[name]!==null&&metrics[name]!==undefined&&metrics[name]!==''
}
function localResultCard(label,value,unit='',help='',primary=false){
  return {label,value:metricLabel(value),unit,help,primary}
}
function localResultMetric(metrics,name,label,unit='',help='',primary=false){
  return localMetricPresent(metrics,name)?localResultCard(label,metrics[name],unit,help,primary):null
}
function localResultOutcome(result,objectiveType,verified=false,verification={}){
  const rejected=verified&&result?.holdout_accepted===false;
  const outcome=result?.outcome||'best-observed';
  const good=outcome==='boundary-found'||outcome==='plateau-found'||
    (outcome==='best-observed'&&objectiveType==='points');
  const details=[
    rejected?(verification.evaluation_kind==='objective'?
      'The independent holdout did not satisfy the configured objective.':
      verification.evaluation_kind==='validity'?'The independent holdout did not pass validity checks.':
        'The independent holdout did not pass verification.'):null,
    result?.stop_reason||(!rejected&&outcome==='no-feasible-point'?
      'No measured load satisfied the configured objective.':null)
  ].filter(Boolean);
  return {
    label:localOutcomeLabel(outcome),tone:rejected||outcome==='no-feasible-point'?'bad':good?'good':'warn',
    detail:details.join(' ')
  }
}
function localSelectedLoadCard(data,result,objective){
  const parameter=result.parameter||data.parameters?.load?.parameter||'load';
  let label='Selected load',help=localSearchAxisLabel(parameter,data.parameters?.workload?.type);
  if(objective.type==='points')label='Best measured point';
  else if(objective.type==='latency-slo'){
    label='Maximum passing load';
    if(result.failing_load!==null&&result.failing_load!==undefined){
      help+=' · first failing '+metricLabel(result.failing_load)
    }
  }else if(objective.type==='maximize-throughput'){
    label=result.outcome==='plateau-found'?'Plateau load':
      result.outcome==='lower-bound'?'Tested lower bound':'Best observed load'
  }
  const value=result.selected_load===null||result.selected_load===undefined?'No passing load':result.selected_load;
  return localResultCard(label,value,'',help,true)
}
function localGenericResultCards(data,schema,metrics,primary,secondary){
  const workload=data.parameters?.workload||{};
  const [,latencyMetric]=localPreferredSlo(schema,data.parameters?.load?.objective||{});
  const latencyPercentile=Object.entries(schema.slo_metrics||{}).find(([,name])=>name===latencyMetric)?.[0];
  if(workload.type==='stock'){
    primary.push(localResultMetric(
      metrics,'throughput','Successful query operations','query operations/s','',true
    ));
  }else{
    primary.push(localResultMetric(metrics,'throughput','Achieved throughput',schema.throughput_unit,'',true));
  }
  if(latencyMetric)primary.push(localResultMetric(
    metrics,latencyMetric,latencyPercentile||'Latency',localMetricDescriptor(schema,latencyMetric)?.unit||'ms'
  ));
  if(schema.reports_errors)primary.push(localResultMetric(metrics,'errors','Errors','failed requests'));
  secondary.push(localResultMetric(metrics,'transactions','Successful operations','operations'));
  secondary.push(localResultMetric(metrics,'retries','Retries','retries'));
  for(const [percentile,name] of Object.entries(schema.slo_metrics||{}))if(name!==latencyMetric){
    secondary.push(localResultMetric(metrics,name,percentile,localMetricDescriptor(schema,name)?.unit||'ms'))
  }
}
function localCustomResultCards(data,schema,metrics,primary,secondary){
  const displayed=new Set(localDisplayedMetrics(schema,data.parameters?.load?.objective||{}).map(item=>item.name));
  for(const metric of schema.metrics||[]){
    const card=localResultMetric(
      metrics,metric.name,localMetricLabel(schema,metric.name),metric.unit,metric.description||'',displayed.has(metric.name)
    );
    (displayed.has(metric.name)?primary:secondary).push(card)
  }
}
function localResultCpuFacts(metrics,objective={}){
  const target={static:'static',dynamic:'dynamic',total:'host'}[objective.target_role];
  const threshold=objective.type==='maximize-throughput'?objective.cpu_saturation_percent:null;
  return [
    ['static','Static nodes','static_cpu_mean','static_cpu_max','assigned capacity'],
    ['dynamic','Dynamic nodes','dynamic_cpu_mean','dynamic_cpu_max','assigned capacity'],
    ['cli','YDB CLI','cli_cpu_mean','cli_cpu_max','assigned capacity'],
    ['host','Host','host_cpu_mean','host_cpu_max','host']
  ].flatMap(([role,label,meanName,maxName,capacity])=>{
    if(!localMetricPresent(metrics,meanName)&&!localMetricPresent(metrics,maxName))return [];
    const values=[];
    if(localMetricPresent(metrics,meanName))values.push(metricLabel(metrics[meanName])+'% mean');
    if(localMetricPresent(metrics,maxName))values.push(metricLabel(metrics[maxName])+'% max');
    const help=['% of '+capacity,role===target?'search target':null,
      role===target&&threshold!==null&&threshold!==undefined?'saturation threshold '+metricLabel(threshold)+'%':null
    ].filter(Boolean).join(' · ');
    return [{label,value:values.join(' · '),help}]
  })
}
function localResultConfigFacts(data,result){
  const parameters=data.parameters||{},workload=parameters.workload||{},geometry=parameters.geometry||{};
  const load=parameters.load||{},measurement=parameters.measurement||{},roleAffinity=data.role_affinity||{};
  const objective=load.objective||{type:'points'},options=Object.entries(workload.options||{}).map(
    ([name,value])=>name+'='+value
  ).join(', ');
  const parameter=result.parameter||load.parameter;
  const selectedThreads=result.selected_load!==null&&result.selected_load!==undefined?result.selected_load:null;
  const cliThreads=parameter==='threads'?selectedThreads:parameters.client?.threads;
  const warmup=measurement.warmup===null?'automatic':(measurement.warmup??'—')+'s';
  const affinity=Object.entries(roleAffinity).map(([role,cpus])=>
    role.replaceAll('_',' ')+': '+(Array.isArray(cpus)?cpuRanges(cpus):'OS managed')
  ).join(' · ');
  return [
    {label:'Workload',value:[workload.type,workload.operation].filter(Boolean).join(' / ')||'—'},
    {label:'Objective',value:objective.type||'points',help:'Search parameter: '+(parameter||'—')},
    {label:'Final geometry',value:(geometry.static_nodes??'—')+' static · '+(result.dynamic_nodes??geometry.dynamic_nodes??'—')+
      ' dynamic',help:(geometry.storage_groups??'—')+' storage groups'},
    {label:'YDB CLI',value:cliThreads===null||cliThreads===undefined?
      (parameter==='threads'?'No feasible selected load':'—'):cliThreads+' threads',
      help:parameter==='threads'?'Selected load':'Configured client'},
    {label:'Measurement',value:(measurement.duration??'—')+'s × '+(measurement.repetitions??'—'),
      help:'warmup '+warmup},
    ...(options?[{label:'Workload options',value:options}]:[]),
    ...(affinity?[{label:'CPU placement',value:affinity}]:[])
  ]
}
function localResultViewModel(data){
  const result=data.result||null;
  if(!result)return {hasResult:false,error:data.error||'',state:data.state};
  const parameters=data.parameters||{},load=parameters.load||{},objective=load.objective||{type:'points'};
  const schema=localResultSchema(data),metricView=localResultMetrics(result,schema),metrics=metricView.metrics;
  const primary=[localSelectedLoadCard(data,result,objective)],secondary=[];
  if(result.selected_load!==null&&result.selected_load!==undefined&&Object.keys(metrics).length){
    if(schema.schema_id==='generic-total-v1')localGenericResultCards(data,schema,metrics,primary,secondary);
    else localCustomResultCards(data,schema,metrics,primary,secondary)
  }
  if(result.dynamic_nodes!==null&&result.dynamic_nodes!==undefined){
    primary.push(localResultCard('Dynamic nodes',result.dynamic_nodes,'',result.search_stage?'geometry stage '+result.search_stage:''))
  }
  const outcome=localResultOutcome(result,objective.type,metricView.verified,data.verification||{});
  const terminalProblem=['failed','cancelled','recovery_required','unsupported'].includes(data.state)||
    Boolean(data.error&&data.state!=='passed');
  if(terminalProblem){
    const interrupted=data.state==='cancelled'||data.state==='recovery_required';
    outcome.label=(interrupted?'Profile interrupted':'Profile failed')+' · partial result';
    outcome.tone=interrupted?'warn':'bad';
    outcome.detail=[
      data.error||null,'Metrics were recorded before the profile finished; treat them as partial.',
      'Recorded search outcome: '+localOutcomeLabel(result.outcome)+'.'
    ].filter(Boolean).join(' ')
  }
  return {
    hasResult:true,...outcome,source:metricView.source,
    sourceHelp:metricView.verified?(result.verification_mode==='adaptive'?
      'Verification participates in load selection':'Independent verification measurement'):'Selected search measurement',
    primary:primary.filter(Boolean),secondary:secondary.filter(Boolean),
    config:localResultConfigFacts(data,result),cpu:localResultCpuFacts(metrics,objective)
  }
}
function localResultFacts(title,facts){
  if(!facts.length)return '';
  return '<section class=local-result-section><h3>'+esc(title)+'</h3><div class=local-result-facts>'+facts.map(item=>
    '<div class=local-result-fact><span class=muted>'+esc(item.label)+'</span><strong>'+esc(item.value??'—')+
    '</strong>'+(item.help?'<small class=muted>'+esc(item.help)+'</small>':'')+'</div>'
  ).join('')+'</div></section>'
}
function localReportTable(title,rows,headers=[]){
  if(!rows.length)return '';
  return '<section class=local-result-section><h3>'+esc(title)+'</h3><table class=report-table>'+
    (headers.length?'<thead><tr>'+headers.map(value=>'<th>'+esc(value)+'</th>').join('')+'</tr></thead>':'')+
    '<tbody>'+rows.map(row=>'<tr>'+row.map(value=>'<td>'+esc(value??'—')+'</td>').join('')+'</tr>').join('')+
    '</tbody></table></section>'
}
function localReportConfiguration(data){
  const p=data.parameters||{},m=p.measurement||{},g=p.geometry||{},result=data.result||{},objective=p.load?.objective||{};
  const value=item=>item===null||item===undefined?'—':typeof item==='object'?JSON.stringify(item):String(item);
  const rows=object=>Object.entries(object||{}).map(([key,item])=>[key.replaceAll('_',' '),value(item)]);
  const affinity=Object.entries(data.role_affinity||{}).map(([role,cpus])=>[
    role.replaceAll('_',' '),Array.isArray(cpus)?cpuRanges(cpus):'OS managed'
  ]);
  return '<details class=report-config data-report-config><summary>Configuration</summary><div class=report-columns>'+
    localReportTable('Measurement',[
      ['Duration',m.duration===undefined?'—':m.duration+' s'],
      ['Warmup',m.warmup===null?'Automatic':m.warmup===undefined?'—':m.warmup+' s'],
      ['Repetitions',m.repetitions],['Objective',objective.type||'points'],
      ...(objective.type==='latency-slo'?[[objective.percentile||'Latency','≤ '+objective.max_ms+' ms']]:[])
    ])+localReportTable('Cluster',[
      ['Static nodes',g.static_nodes],['Dynamic nodes',result.dynamic_nodes??g.dynamic_nodes],
      ['Storage groups',g.storage_groups]
    ])+'</div>'+localReportTable('CPU placement',affinity,['Role','Logical CPU IDs'])+
    localReportTable('Workload',[
      ['Type',p.workload?.type],['Operation',p.workload?.operation],...rows(p.workload?.options)
    ])+localReportTable('Actor system',[
      ['Static node vCPUs',p.actor_system?.static_nodes?.cpu_count??'Automatic'],
      ['Dynamic node vCPUs',p.actor_system?.dynamic_nodes?.cpu_count??'Automatic'],
      ...rows(Object.fromEntries(Object.entries(p.actor_system||{}).filter(([key])=>!['static_nodes','dynamic_nodes'].includes(key))))
    ])+
    localReportTable('Binaries',Object.entries(data.binaries||{}).map(([role,binary])=>[
      role.replaceAll('_',' '),String(binary.name||'—').split('/').pop(),binary.sha256||'—'
    ]),['Role','Name','SHA-256'])+'</details>'
}
function localReportMetrics(data){
  const schema=localResultSchema(data),metrics=localResultMetrics(data.result||{},schema).metrics;
  const formatted=(name,unit)=>localMetricPresent(metrics,name)?
    (unit==='ms'&&name===schema.slo_metrics?.pmax&&Number(metrics[name])>=1000?
      metricLabel(Number(metrics[name])/1000)+' s':metricLabel(metrics[name])+(unit?' '+unit:'')):'—';
  const latency=Object.entries(schema.slo_metrics||{}).filter(([,name])=>localMetricPresent(metrics,name)).map(
    ([label,name])=>[label==='pmax'?'Maximum':label,formatted(name,localMetricDescriptor(schema,name)?.unit||'ms')]
  );
  const counters=[['transactions','Successful operations'],['errors','Errors'],['retries','Retries']].filter(
    ([name])=>localMetricPresent(metrics,name)
  ).map(([name,label])=>[label,formatted(name,'')]);
  if(data.parameters?.load?.allow_errors)counters.push(['Error policy','Errors allowed']);
  const cpu=[['dynamic','Dynamic nodes'],['static','Static nodes'],['cli','YDB CLI'],['host','Host (% of all CPUs)']].filter(
    ([role])=>localMetricPresent(metrics,role+'_cpu_mean')||localMetricPresent(metrics,role+'_cpu_max')
  ).map(([role,label])=>[label,formatted(role+'_cpu_mean','%'),formatted(role+'_cpu_max','%')]);
  const known=new Set(['throughput','transactions','errors','retries',...Object.values(schema.slo_metrics||{})]);
  const extra=(schema.metrics||[]).filter(metric=>!known.has(metric.name)&&localMetricPresent(metrics,metric.name)).map(
    metric=>[localMetricLabel(schema,metric.name),formatted(metric.name,metric.unit)]
  );
  return '<div class=report-columns>'+localReportTable('Latency',latency)+localReportTable('Requests',counters)+
    '</div>'+localReportTable('CPU usage · % of assigned CPUs',cpu,['Role','Mean','Peak'])+
    localReportTable('Additional workload metrics',extra)
}
function localResultPanel(data){
  const view=localResultViewModel(data);
  if(!view.hasResult){
    const terminal=!['running','preparing'].includes(data.state);
    return '<div class="empty '+(terminal&&view.error?'error':'')+'"><strong>'+
      (terminal?'No result was produced.':'Result will appear when the profile finishes.')+'</strong>'+
      (view.error?'<p>'+esc(view.error)+'</p>':'')+'</div>'
  }
  const result=data.result||{},verification=data.verification||{},normal=view.tone!=='bad'&&view.tone!=='warn';
  const verified=localResultMetrics(result,localResultSchema(data)).verified;
  const source=verified?'Result of verification · '+(data.parameters?.measurement?.duration??'—')+' s · '+
    (verification.completed_repetitions??'—')+' repetition(s)':'Search measurement · no completed verification';
  return '<div class=local-result-heading><span class="local-result-badge '+esc(view.tone)+'">'+
    esc(normal&&result.outcome==='boundary-found'?'SLO satisfied':view.label)+'</span></div>'+
    (!normal&&view.detail?'<p class=error>'+esc(view.detail)+'</p>':'')+
    '<p class=report-source>'+esc(source)+'</p>'+localReportMetrics(data)+localReportConfiguration(data)
}
function localYdbDefaultView(data){
  return ['running','preparing'].includes(data.state)?'discovery':data.result?'result':'discovery'
}
function localYdbDiscoveryLabel(data){
  return (data.parameters?.load?.objective?.type||'points')==='points'?'Measurements':'Discovery'
}
function localYdbViewHref(container,view){
  const runId=container.dataset.localYdbRunId,profile=container.dataset.localYdbProfile;
  return runId&&profile?'#run/'+enc(runId)+'/profile/'+enc('local-ydb/'+profile+'/view/'+view):'#'
}
function localYdbViewTabs(container,data,selected){
  const view=localResultViewModel(data),result=data.result||{};
  const metrics=view.hasResult?view.primary.slice(1).filter(item=>!['Errors','Dynamic nodes'].includes(item.label)):[];
  const selectedLoad=view.hasResult?view.primary[0]:null;
  const [percentile]=localPreferredSlo(localResultSchema(data),data.parameters?.load?.objective||{});
  const summary=metrics.length?'<section class=profile-metric-summary aria-label="Profile result summary">'+
    '<div class=local-kpis>'+metrics.map(item=>'<div><div class=muted>'+
      esc(item.label===percentile?'Latency ('+percentile+')':item.label)+'</div><strong>'+esc(item.value)+
      (item.unit?' <span class=metric-unit>'+esc(item.unit)+'</span>':'')+'</strong></div>').join('')+'</div>'+
    (selectedLoad?'<div class=muted>'+esc(localSearchAxisLabel(
      result.parameter||data.parameters?.load?.parameter,data.parameters?.workload?.type
    ))+': '+esc(selectedLoad.value)+'</div>':'')+
    (view.tone==='bad'||view.tone==='warn'?'<div class="'+esc(view.tone)+'">'+esc(view.label)+'</div>':'')+'</section>':'';
  return summary+'<nav class=local-profile-tabs aria-label="Local YDB profile view">'+[
    ['result','Result'],['discovery',localYdbDiscoveryLabel(data)]
  ].map(([view,label])=>'<a class="local-profile-tab '+(view===selected?'active':'')+'" '+
    (view===selected?'aria-current=page ':'')+'data-local-ydb-view="'+view+'" href="'+
    localYdbViewHref(container,view)+'">'+esc(label)+'</a>').join('')+'</nav>'
}
function localApplyYdbView(container,view,explicit,updateHistory=false){
  if(!['result','discovery'].includes(view))return;
  if(updateHistory){
    const activityLog=container.querySelector('[data-local-activity-log]');
    const activityPanel=activityLog?.closest?.('[data-local-ydb-panel]');
    if(activityLog&&!activityPanel?.hidden){
      container.dataset.localActivityScrollTop=String(activityLog.scrollTop);
      container.dataset.localActivityPinned=String(
        activityLog.scrollHeight-activityLog.scrollTop-activityLog.clientHeight<8
      )
    }
  }
  container.dataset.localYdbView=view;
  container.dataset.localYdbViewExplicit=explicit?'true':'false';
  for(const tab of container.querySelectorAll('[data-local-ydb-view]')){
    const active=tab.dataset.localYdbView===view;
    tab.classList.toggle('active',active);
    if(active)tab.setAttribute('aria-current','page');else tab.removeAttribute('aria-current')
  }
  for(const panel of container.querySelectorAll('[data-local-ydb-panel]')){
    panel.hidden=panel.dataset.localYdbPanel!==view
  }
  if(view==='discovery')localRestoreActivityScroll(
    container,Number(container.dataset.localActivityScrollTop||0),container.dataset.localActivityPinned!=='false'
  );
  if(updateHistory&&typeof history!=='undefined')history.replaceState(null,'',localYdbViewHref(container,view))
}
function localBindYdbViews(container){
  for(const tab of container.querySelectorAll('[data-local-ydb-view]'))tab.onclick=event=>{
    event.preventDefault();localApplyYdbView(container,tab.dataset.localYdbView,true,true)
  }
}
function localRestoreYdbViewFocus(container,view){
  if(!view)return;
  const tab=[...container.querySelectorAll('[data-local-ydb-view]')].find(
    item=>item.dataset.localYdbView===view
  );
  if(tab&&typeof tab.focus==='function')tab.focus({preventScroll:true})
}
function bindLocalAttemptRows(container){
  for(const row of container.querySelectorAll('[data-attempt-href]'))row.onclick=event=>{
    if(event.defaultPrevented||event.button!==0||event.ctrlKey||event.metaKey||event.shiftKey||event.altKey)return;
    if(event.target.closest('a,button,input,select,textarea,label,summary,details'))return;
    if(window.getSelection()?.toString())return;
    location.hash=row.dataset.attemptHref
  }
}
function renderLocalYdbProfile(container,data){
  const failure=['failed','cancelled'].includes(data.state)?'<section class=profile-error role=alert><h3>'+
    (data.state==='failed'?'Profile failed':'Profile cancelled')+'</h3><div>'+
    esc(String(data.error||'No diagnostic was recorded.').split(String.fromCharCode(10))[0].slice(0,240))+'</div>'+
    (data.error?'<details><summary>Error details</summary><pre>'+esc(data.error)+'</pre></details>':'')+'</section>':'';
  const focusedView=typeof document!=='undefined'&&container.contains?.(document.activeElement)?
    document.activeElement.dataset.localYdbView||'':'';
  const previousActivity=container.querySelector('[data-local-activity]');
  const previousActivityLog=container.querySelector('[data-local-activity-log]');
  const activityOpen=previousActivity?previousActivity.open:['running','preparing'].includes(data.state);
  const previousActivityPanel=previousActivityLog?.closest?.('[data-local-ydb-panel]');
  const activityVisible=previousActivityLog&&!previousActivityPanel?.hidden;
  if(activityVisible){
    container.dataset.localActivityScrollTop=String(previousActivityLog.scrollTop);
    container.dataset.localActivityPinned=String(
      previousActivityLog.scrollHeight-previousActivityLog.scrollTop-previousActivityLog.clientHeight<8
    )
  }
  const activityScrollTop=activityVisible?previousActivityLog.scrollTop:Number(container.dataset.localActivityScrollTop||0);
  const activityPinned=activityVisible?
    previousActivityLog.scrollHeight-previousActivityLog.scrollTop-previousActivityLog.clientHeight<8:
    container.dataset.localActivityPinned!=='false';
  const selectedView=container.dataset.localYdbViewExplicit==='true'&&
    ['result','discovery'].includes(container.dataset.localYdbView)?
    container.dataset.localYdbView:localYdbDefaultView(data);
  if(data.state==='preparing'){
    const discovery='<div class=notice>Preparing local YDB profile and extracting binaries…</div>'+localActivityLog(
      data.activity||[],Boolean(data.activity_truncated),activityOpen,data.activity_error||''
    );
    container.innerHTML=localYdbViewTabs(container,data,selectedView)+
      '<section class=local-profile-view data-local-ydb-panel=result'+
      (selectedView==='result'?'':' hidden')+'>'+localResultPanel(data)+'</section>'+
      '<section class=local-profile-view data-local-ydb-panel=discovery'+
      (selectedView==='discovery'?'':' hidden')+'>'+discovery+'</section>';
    localApplyYdbView(container,selectedView,container.dataset.localYdbViewExplicit==='true');
    localBindYdbViews(container);
    if(selectedView==='discovery')localRestoreActivityScroll(container,activityScrollTop,activityPinned);
    localRestoreYdbViewFocus(container,focusedView);return
  }
  const profileConfigOpen=container.querySelector('[data-local-profile-config][open]')!==null;
  const progress=data.progress||{},attempts=data.attempts||[],searches=data.searches||[];
  const result=data.result||null,parameters=data.parameters||{},loadConfig=parameters.load||{};
  const objective=loadConfig.objective||{type:'points'};
  const resultSchema=localResultSchema(data),throughputUnit=resultSchema.throughput_unit;
  const [latencyPercentile,latencyMetric]=localPreferredSlo(resultSchema,objective);
  const phaseElapsed=localElapsed(progress.phase_started_at);
  const phaseDuration=Number(progress.phase_duration_seconds);
  const profileElapsed=localElapsed(data.started_at,data.finished_at);
  const remaining=Number.isFinite(phaseDuration)?Math.max(0,phaseDuration-phaseElapsed):null;
  const phaseHelp=[
    progress.attempt?'attempt #'+progress.attempt:null,
    progress.repetition?'repetition '+progress.repetition+'/'+progress.repetitions:null,
    Number.isFinite(remaining)?elapsedLabel(remaining)+' remaining':null
  ].filter(Boolean).join(' · ');
  const profileActive=['running','preparing'].includes(data.state);
  const phaseProgress=profileActive&&Number.isFinite(phaseDuration)?
    '<progress class=local-phase-progress max="'+phaseDuration+'" value="'+
      Math.min(phaseDuration,phaseElapsed)+'"></progress>':'';
  let html=loadConfig.allow_errors?
    '<div class=notice>Failed workload requests are allowed for this profile and remain visible in metrics.</div>':'';
  html+='<div class=discovery-status><strong>'+
    esc(localPhaseLabel(profileActive?progress.phase||data.state:data.state))+'</strong><span class=muted>'+
    esc(elapsedLabel(profileElapsed))+' · '+esc(attempts.length)+' completed attempts</span>'+
    (profileActive?'<span class=muted>'+esc(phaseHelp)+'</span>':'');
  const dynamicNodes=
    progress.dynamic_nodes??result?.dynamic_nodes??parameters.geometry?.dynamic_nodes??'—';
  const candidate=progress.load===undefined?
    '—':(progress.parameter||loadConfig.parameter||'load')+' '+metricLabel(progress.load);
  html+=(profileActive?'<span>Candidate: '+esc(candidate)+'</span>':'')+'</div>'+phaseProgress+
    '<div class=discovery-geometry>'+esc(parameters.geometry?.static_nodes??'—')+' static · '+
    esc(dynamicNodes)+' dynamic'+(objective.type==='latency-slo'?
      ' · SLO: latency ('+esc(latencyPercentile)+') ≤ '+esc(objective.max_ms)+' ms':'')+'</div>';
  html+=localProfileDetails(data,profileConfigOpen);
  if(profileActive&&progress.current_command?.argv?.length){
    html+='<section class=local-current-command><span class=muted>Running command</span>'+
      '<pre class=local-command-code><code>'+esc(localCommandText(progress.current_command))+
      '</code></pre></section>'
  }
  html+=localActivityLog(
    data.activity||[],Boolean(data.activity_truncated),activityOpen,data.activity_error||''
  );
  if(!result&&attempts.length)html+='<p class=muted>Latest measurement: '+
    esc(metricLabel(attempts.at(-1).throughput))+' '+esc(throughputUnit)+
    (latencyMetric?' · latency ('+esc(latencyPercentile)+'): '+esc(metricLabel(
      localAttemptMetric(attempts.at(-1),latencyMetric,resultSchema)))+' '+
      esc(localMetricDescriptor(resultSchema,latencyMetric)?.unit||''):'')+'</p>';
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
  if(searches.length>1||currentStage>1)html+='<h3>Geometry stages</h3><div class=local-stages>'+
    stageCards+currentStageCard+'</div>';
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
      rows:localAttemptRows(item.attempts,resultSchema,xField),bestRows:localBestRows(item.attempts,objective,xField),
      suffix:xAxis==='parameter'&&stages.length>1?
        ' · stage '+item.search_stage+' · '+item.dynamic_nodes+' dynamic':''
    }));
    const bestLabel=objective.type==='latency-slo'?'Highest passing':
      (objective.type==='maximize-throughput'?'Plateau candidate':'Best observed');
    const candidateSeries=groups.flatMap(group=>[
      {rows:group.bestRows,metric:'load',label:'Candidate'+group.suffix,colorIndex:7},
      {rows:group.bestRows,metric:'current_best',label:bestLabel+group.suffix,colorIndex:0},
      {rows:group.bestRows,metric:'search_low',label:'Search low'+group.suffix,colorIndex:10},
      {rows:group.bestRows,metric:'search_high',label:'Search high'+group.suffix,colorIndex:9},
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
    const latencyMetrics=[...new Map(Object.entries(resultSchema.slo_metrics||{}).map(
      ([percentile,metric])=>[metric,{percentile,metric}]
    )).values()];
    const latencySeries=groups.flatMap(group=>latencyMetrics.map((item,index)=>({
      rows:group.rows,metric:item.metric,label:item.percentile+' · '+item.metric+group.suffix,colorIndex:index
    })));
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
    const errorMetrics=(resultSchema.metrics||[]).filter(item=>['errors','retries'].includes(item.name));
    const errorSeries=groups.flatMap(group=>errorMetrics.map((item,index)=>({
      rows:group.rows,metric:item.name,label:localMetricLabel(resultSchema,item.name)+group.suffix,
      colorIndex:item.name==='errors'?8:index+1
    })));
    const reservedMetrics=new Set([
      'transactions','throughput','errors','retries',...latencyMetrics.map(item=>item.metric)
    ]);
    const extraMetricGroups=new Map;
    for(const metric of resultSchema.metrics||[]){
      if(reservedMetrics.has(metric.name))continue;
      if(!extraMetricGroups.has(metric.unit))extraMetricGroups.set(metric.unit,[]);
      extraMetricGroups.get(metric.unit).push(metric)
    }
    const extraCharts=[...extraMetricGroups.entries()].map(([unit,metrics],index)=>{
      const alias='workload_metrics_'+index;
      return {alias,unit,metrics,series:groups.flatMap(group=>metrics.map((metric,colorIndex)=>({
        rows:group.rows,metric:metric.name,label:localMetricLabel(resultSchema,metric.name)+group.suffix,colorIndex
      })))}
    });
    const showSearchProgress=xAxis==='attempt';
    const chartSeries={throughput:throughputSeries,cpu_percent:cpuSeries};
    if(latencySeries.length)chartSeries.latency_ms=latencySeries;
    if(errorSeries.length)chartSeries.errors=errorSeries;
    for(const chart of extraCharts)chartSeries[chart.alias]=chart.series;
    if(showSearchProgress)chartSeries.load=candidateSeries;
    chartBinding={
      xName,xValues,
      series:chartSeries
    };
    html+='<div class=run-section-title><h3>Search process</h3><div class=actions><span class=muted>X axis</span>'+
      '<button type=button data-local-chart-x=attempt class="'+(xAxis==='attempt'?'primary':'')+
      '" aria-pressed="'+(xAxis==='attempt')+'">Attempts (search order)</button>'+
      '<button type=button data-local-chart-x=parameter class="'+(xAxis==='parameter'?'primary':'')+
      '" aria-pressed="'+(xAxis==='parameter')+'">'+esc(searchAxisLabel)+'</button></div></div>'+
      '<div class=local-charts>'+
      localChart(
        (loadConfig.parameter==='rate'?'Offered and achieved':'Achieved')+' throughput ('+throughputUnit+')',
        'throughput',xName,xValues,throughputSeries
      )+
      (latencySeries.length?localChart('Latency (ms)','latency_ms',xName,xValues,latencySeries):'')+
      (showSearchProgress?localChart(
        objective.type==='maximize-throughput'?'Ternary search progress':'Load search progress',
        'load',xName,xValues,candidateSeries
      ):'')+
      extraCharts.map(chart=>localChart(
        'Workload metrics ('+chart.unit+')',chart.alias,xName,xValues,chart.series
      )).join('')+
      localChart('CPU by role','cpu_percent',xName,xValues,cpuSeries)+
      (errorSeries.length?localChart('Errors and retries','errors',xName,xValues,errorSeries):'')+'</div>';
    const displayedMetrics=localDisplayedMetrics(resultSchema,objective);
    const workloadHeaders=displayedMetrics.map(metric=>'<th title="'+
      esc(metric.description||'')+'">'+esc(localMetricLabel(resultSchema,metric.name))+
      (metric.unit?' ('+esc(metric.unit)+')':'')+'</th>').join('');
    html+='<h3>Attempts</h3><div class=local-attempts-scroll tabindex=0 role=region aria-label="Search attempts">'+
      '<table class="local-attempts discovery-attempts"><thead><tr><th>#</th><th>'+esc(searchAxisLabel)+'</th>'+
      workloadHeaders+'<th>Verdict</th><th>Duration</th></tr></thead><tbody>'+
      attempts.map(item=>{
        const href=esc(localAttemptHref(container.dataset.localYdbRunId,container.dataset.localYdbProfile,item.attempt));
        return '<tr data-attempt-href="'+href+'"><td><a href="'+href+'">'+esc(item.attempt)+
        '</a></td><td>'+esc(metricLabel(item.load))+'</td>'+
        displayedMetrics.map(metric=>'<td>'+esc(metricLabel(
          localAttemptMetric(item,metric.name,resultSchema)??'—'
        ))+'</td>').join('')+'<td class="'+(item.passed?'attempt-pass':'attempt-fail')+'">'+
        (item.passed?'PASS':'FAIL')+'</td><td>'+esc(elapsedLabel(item.duration_seconds))+'</td></tr>'
      }).join('')+'</tbody></table></div>';
  }else html+='<div class=empty>'+(profileActive?
    'No completed search attempts yet. The timeline will appear after the first measurement.':
    'No completed measurements were recorded.')+'</div>';
  if(profileActive&&data.progress?.attempt)html+='<p><a href="'+esc(localAttemptHref(
    container.dataset.localYdbRunId,container.dataset.localYdbProfile,data.progress.attempt
  ))+'">Current attempt metrics</a></p>';
  if(data.verification?.configured_repetitions)html+='<p><a href="'+esc(localAttemptHref(
    container.dataset.localYdbRunId,container.dataset.localYdbProfile,'verification'
  ))+'">Verification metrics</a></p>';
  container.innerHTML=failure+localYdbViewTabs(container,data,selectedView)+
    '<section class=local-profile-view data-local-ydb-panel=result'+
    (selectedView==='result'?'':' hidden')+'>'+localResultPanel(data)+'</section>'+
    '<section class=local-profile-view data-local-ydb-panel=discovery'+
    (selectedView==='discovery'?'':' hidden')+'>'+html+'</section>';
  localApplyYdbView(container,selectedView,container.dataset.localYdbViewExplicit==='true');
  localBindYdbViews(container);
  bindLocalAttemptRows(container);
  if(selectedView==='discovery')localRestoreActivityScroll(container,activityScrollTop,activityPinned);
  localRestoreYdbViewFocus(container,focusedView);
  for(const axisButton of container.querySelectorAll('[data-local-chart-x]'))axisButton.onclick=()=>{
    container.dataset.localYdbXAxis=axisButton.dataset.localChartX;renderLocalYdbProfile(container,data)
  };
  if(chartBinding)bindChartTooltips(
    container,chartBinding.xName,chartBinding.xValues,chartBinding.series,
    Object.keys(chartBinding.series),chartColors,true
  )
}
async function mountLocalYdbProfile(container,runId,profile,runState,requestedView=''){
  container.dataset.localYdbRunId=runId;container.dataset.localYdbProfile=profile;
  if(['result','discovery'].includes(requestedView)){
    container.dataset.localYdbView=requestedView;container.dataset.localYdbViewExplicit='true'
  }else container.dataset.localYdbViewExplicit='false';
  let loading=false,terminal=false,observedActive=false,activity=[],activityAfter=0,activityTruncated=false;
  const profileSelection=()=>container.dataset.localYdbViewExplicit==='true'?
    'local-ydb/'+profile+'/view/'+container.dataset.localYdbView:'local-ydb/'+profile;
  const scheduleRunRefresh=()=>{if(['running','queued'].includes(runState)&&!refreshTimer){
    refreshTimer=setTimeout(()=>renderRun(runId,profileSelection()),700)
  }};
  const refresh=async()=>{
    if(loading)return;loading=true;
    try{
      const [data,activityUpdate]=await Promise.all([
        api('/api/runs/'+enc(runId)+'/local-ydb-profile?profile='+enc(profile)),
        loadLocalYdbActivity(runId,profile,activityAfter).catch(error=>({error:error.message}))
      ]);
      if(activityUpdate.error)data.activity_error='Recent activity could not be loaded: '+activityUpdate.error;
      else{
        const merged=new Map(activity.map(item=>[item.sequence,item]));
        for(const item of activityUpdate.events||[])merged.set(item.sequence,item);
        activity=[...merged.values()].sort((left,right)=>left.sequence-right.sequence);
        if(activity.length>200){activity=activity.slice(-200);activityTruncated=true}
        activityTruncated=activityTruncated||Boolean(activityUpdate.truncated);
        if(Number.isSafeInteger(activityUpdate.after))activityAfter=Math.max(activityAfter,activityUpdate.after)
      }
      data.activity=activity;data.activity_truncated=activityTruncated;
      renderLocalYdbProfile(container,data);terminal=!['running','preparing'].includes(data.state);
      if(terminal&&refreshTimer){clearInterval(refreshTimer);refreshTimer=null}
      if(terminal&&observedActive)scheduleRunRefresh();
      observedActive=!terminal
    }catch(error){container.innerHTML=displayError(error)}finally{loading=false}
  };
  await refresh();if(!terminal&&['running','queued','recovery_required'].includes(runState)&&!refreshTimer)refreshTimer=setInterval(refresh,1000)
}
function localAttemptHref(runId,profile,attempt){
  return '#attempt/'+enc(runId)+'/'+enc(profile)+'/'+enc(attempt)
}
function localCounterCharts(samples,repetition,nodeKey,raw){
  const selected=samples.filter(item=>String(item.context?.repetition)===String(repetition));
  const start=selected.find(item=>Number.isFinite(item.timestamp_unix))?.timestamp_unix;
  const xValues=[];
  const names=['Current','Default','Max','PossibleMax','PotentialMax'];
  const pools=[...new Set(selected.flatMap(sample=>sample.nodes.filter(
    node=>node.role+' '+node.index===nodeKey
  ).flatMap(node=>Object.keys(node.pools||{}))))].sort();
  const poolRows=new Map(pools.map(name=>[name,new Map]));
  for(const sample of selected){
    if(!Number.isFinite(sample.timestamp_unix)||!Number.isFinite(start))continue;
    const x=Number((sample.timestamp_unix-start).toFixed(3));
    const node=sample.nodes.find(item=>item.role+' '+item.index===nodeKey);
    for(const poolName of pools){
      const threadRow={};
      for(const name of names){
        const value=node?.pools?.[poolName]?.[name+'ThreadCountPercent'];
        threadRow[name]=Number.isFinite(value)?value/100:null
      }
      const counters=(raw?node?.pools:node?.rates)?.[poolName]||{};
      for(const name of ['ElapsedMicrosec','CpuMicrosec'])threadRow[name]=counters[name]??null;
      poolRows.get(poolName).set(String(x),threadRow)
    }
    xValues.push(x)
  }
  const threadSeries=pools.map((name,index)=>({label:name,rows:poolRows.get(name),colorIndex:index}));
  return {xValues,series:Object.fromEntries(
    [...names,'ElapsedMicrosec','CpuMicrosec'].map(name=>[name,threadSeries])
  )}
}
function localAttemptReport(data,item){
  if(!item)return '<p class=muted>No completed measurement for this attempt yet.</p>';
  const measurement=data.parameters?.measurement||{};
  return localReportMetrics({...data,result:{selected_metrics:localAttemptMetrics(data,item),metrics_source:'search'}})+
    '<p class=muted>Measurement: '+esc(measurement.duration??'—')+' s · warmup: '+
    esc(measurement.warmup===null?'automatic':(measurement.warmup??'—')+' s')+' · '+
    esc(item.completed_repetitions??measurement.repetitions??'—')+' repetition(s)</p>'
}
function localAttemptMetrics(data,item){
  return {...item,...(item===data.verification?data.result?.verified_metrics:null),...(item?.metrics||{})}
}
function localAttemptHeader(data,item,context){
  const schema=localResultSchema(data),objective=data.parameters?.load?.objective||{};
  const metrics=localAttemptMetrics(data,item),[percentile,latency]=localPreferredSlo(schema,objective);
  const passed=item?.passed??item?.accepted;
  const label=passed===true?(objective.type==='latency-slo'?'SLO satisfied':'PASS'):
    passed===false?'FAIL':localPhaseLabel(context.status||context.state||data.state);
  const reason=item?.error||item?.reason||(passed===true&&objective.type==='latency-slo'?
    'latency ('+percentile+') ≤ '+objective.max_ms+' ms':item?.decision);
  const kpi=(name,value,unit)=>'<div><div class=muted>'+esc(name)+'</div><strong>'+esc(metricLabel(value??'—'))+
    ' <span class=metric-unit>'+esc(unit)+'</span></strong></div>';
  return '<div class="'+(passed===false?'attempt-fail':passed===true?'attempt-pass':'muted')+'">'+esc(label)+
    (reason?' · '+esc(reason):'')+'</div><section class=profile-metric-summary aria-label="Attempt result summary">'+
    '<div class=local-kpis>'+kpi(schema.throughput_unit==='query operations/s'?'Successful query operations':
      localMetricLabel(schema,'throughput'),metrics.throughput,schema.throughput_unit)+
    (latency?kpi('Latency ('+percentile+')',localAttemptMetric(metrics,latency,schema),
      localMetricDescriptor(schema,latency)?.unit||'ms'):'')+'</div></section><div class=attempt-meta><span>'+
    esc(localSearchAxisLabel(data.parameters?.load?.parameter,data.parameters?.workload?.type))+': '+
    esc(context.load??'—')+'</span><span>Duration: '+esc(elapsedLabel(context.duration_seconds))+'</span><span>'+
    (context.search_stage?'Stage '+esc(context.search_stage)+' · ':'')+
    esc(data.parameters?.geometry?.static_nodes??'—')+' static · '+esc(context.dynamic_nodes??'—')+' dynamic</span></div>'
}
function localAttemptCommands(context){
  const commands=context.commands?.length?context.commands:context.current_command?[context.current_command]:[];
  return commands.length?commands.map(command=>'<section class=attempt-command><h3>'+esc(localPhaseLabel(command.phase))+
    '</h3><div class=muted>'+esc([
      command.repetition?'Repetition '+command.repetition:null,
      Number.isFinite(command.duration_seconds)?elapsedLabel(command.duration_seconds):null,
      command.exit_code!==undefined?'exit '+command.exit_code:null
    ].filter(Boolean).join(' · '))+'</div><pre><code>'+esc(localCommandText(command))+'</code></pre></section>').join(''):
    '<div class=empty>No recorded commands for this attempt.</div>'
}
function localAttemptView(value){return ['summary','counters','commands'].includes(value)?value:'summary'}
async function renderLocalYdbAttempt(runId,profile,attempt,requestedView='summary'){
  clearRefresh();
  let selectedView=localAttemptView(requestedView);
  const attemptHref=localAttemptHref(runId,profile,attempt);
  const discovery='#run/'+enc(runId)+'/profile/'+enc('local-ydb/'+profile+'/view/discovery');
  app.innerHTML=shell('runs','<div class=attempt-page><div class=breadcrumbs><a href="'+esc(discovery)+'">'+
    esc(runId+' / '+profile)+' / Discovery</a></div><div class=run-header><h1 class=page-title>'+
    (attempt==='verification'?'Verification':'Attempt '+esc(attempt))+
    '</h1><div class=toolbar><button id=counter-refresh>Refresh</button><details class=downloads hidden '+
    'id=attempt-downloads><summary>Downloads</summary><div class=actions id=attempt-artifacts></div></details></div></div>'+
    '<div id=attempt-error></div><div id=attempt-header></div><nav class=local-profile-tabs aria-label="Attempt details">'+
    [['summary','Summary'],['counters','YDB counters'],['commands','Commands']].map(([view,label])=>
      '<a class=local-profile-tab data-attempt-view="'+view+'" href="'+esc(attemptHref+'/'+view)+'">'+label+'</a>'
    ).join('')+'</nav><section data-attempt-panel=summary id=attempt-summary></section>'+
    '<section data-attempt-panel=counters><div class=toolbar>'+
    '<label id=counter-repetition-label>Repetition <select id=counter-repetition></select></label>'+
    '<span id=counter-single-repetition class=muted></span>'+
    '<label>Node <select id=counter-node></select></label>'+
    '<label><input type=checkbox id=counter-raw> Raw microsecond counters</label>'+
    '</div><div id=counter-notice></div><div id=counter-charts class=local-charts></div></section>'+
    '<section data-attempt-panel=commands id=attempt-commands></section></div>');
  const target=document.querySelector('#counter-charts'),summary=document.querySelector('#attempt-summary');
  const repetition=document.querySelector('#counter-repetition'),node=document.querySelector('#counter-node');
  const raw=document.querySelector('#counter-raw');
  let samples=[],loading=false;
  const options=(select,values)=>{
    if(JSON.stringify([...select.options].map(option=>option.value))===JSON.stringify(values.map(String)))return;
    const previous=select.value;
    select.innerHTML=values.map(value=>'<option value="'+esc(value)+'">'+esc(value)+'</option>').join('');
    if(values.map(String).includes(previous))select.value=previous
  };
  const draw=()=>{
    options(repetition,[...new Set(samples.map(item=>item.context?.repetition).filter(Number.isFinite))].sort((a,b)=>a-b));
    document.querySelector('#counter-repetition-label').hidden=repetition.options.length<2;
    document.querySelector('#counter-single-repetition').textContent=repetition.options.length===1?
      'Repetition '+repetition.value:'';
    const selected=samples.filter(item=>String(item.context?.repetition)===repetition.value);
    options(node,[...new Set(selected.flatMap(item=>item.nodes.map(value=>value.role+' '+value.index)))].sort());
    if(!selected.length){target.innerHTML='<div class=empty>No YDB counter samples for this attempt.</div>';return}
    const {xValues,series}=localCounterCharts(samples,repetition.value,node.value,raw.checked);
    const unit=raw.checked?'µs':'µs/s';
    target.innerHTML=['Current','Default','Max','PossibleMax','PotentialMax'].map(name=>
      localChart(name+' threads',name,'Time (s)',xValues,series[name])
    ).join('')+
      localChart('ElapsedMicrosec ('+unit+')','ElapsedMicrosec','Time (s)',xValues,series.ElapsedMicrosec)+
      localChart('CpuMicrosec ('+unit+')','CpuMicrosec','Time (s)',xValues,series.CpuMicrosec);
    bindChartTooltips(target,'Time (s)',xValues,series,Object.keys(series),chartColors,true,value=>value.toFixed(2))
  };
  const applyView=()=>{
    for(const panel of app.querySelectorAll('[data-attempt-panel]'))panel.hidden=panel.dataset.attemptPanel!==selectedView;
    for(const tab of app.querySelectorAll('[data-attempt-view]')){
      const active=tab.dataset.attemptView===selectedView;
      tab.classList.toggle('active',active);
      if(active)tab.setAttribute('aria-current','page');else tab.removeAttribute('aria-current')
    }
    if(selectedView==='counters')draw()
  };
  for(const tab of app.querySelectorAll('[data-attempt-view]'))tab.onclick=event=>{
    if(event.button!==0||event.ctrlKey||event.metaKey||event.shiftKey||event.altKey)return;
    event.preventDefault();selectedView=localAttemptView(tab.dataset.attemptView);
    history.pushState(null,'',attemptHref+'/'+selectedView);applyView()
  };
  applyView();
  for(const select of [repetition,node,raw])select.onchange=draw;
  const refresh=async()=>{
    if(loading||!target.isConnected)return;loading=true;
    try{
      const [data,metrics]=await Promise.all([
        api('/api/runs/'+enc(runId)+'/local-ydb-profile?profile='+enc(profile)),
        api('/api/runs/'+enc(runId)+'/local-ydb-metrics?profile='+enc(profile)+'&attempt='+enc(attempt)).catch(
          error=>({samples:[],error:String(error)})
        )
      ]);
      if(!target.isConnected)return;
      const item=attempt==='verification'?data.verification:(data.attempts||[]).find(value=>String(value.attempt)===attempt);
      const context=item||data.progress||{};
      document.querySelector('#attempt-error').innerHTML='';
      document.querySelector('#attempt-header').innerHTML=localAttemptHeader(data,item,context);
      summary.innerHTML=localAttemptReport(data,item);
      document.querySelector('#attempt-commands').innerHTML=localAttemptCommands(context);
      document.querySelector('#attempt-downloads').hidden=!metrics.artifact;
      document.querySelector('#attempt-artifacts').innerHTML=metrics.artifact?'<a href="'+esc(hostApiPath(metrics.artifact))+
        '">Profile YDB counters (JSONL)</a>':'';
      samples=metrics.samples||[];
      const errors=[...new Set(samples.flatMap(sample=>[
        sample.error,...sample.nodes.map(value=>value.error?value.role+' '+value.index+': '+value.error:null)
      ]).filter(Boolean))];
      document.querySelector('#counter-notice').innerHTML=[
        metrics.error,
        metrics.truncated?'Showing a limited sample history.':null,
        metrics.invalid_records?'Some invalid metric records were skipped.':null,...errors.slice(0,8)
      ].filter(Boolean).map(message=>'<div class=notice>'+esc(message)+'</div>').join('');
      if(selectedView==='counters')draw();
      if(!['running','preparing'].includes(data.state))clearRefresh()
    }catch(error){if(target.isConnected)document.querySelector('#attempt-error').innerHTML=displayError(error)}finally{loading=false}
  };
  document.querySelector('#counter-refresh').onclick=refresh;
  refreshTimer=setInterval(refresh,2000);await refresh()
}
    """
    """
function parseLocalYdbProfileSelection(groups,selected){
  if(groups[selected])return {profile:selected,view:''};
  const match=new RegExp('^(local-ydb/.+)/view/(result|discovery)$').exec(selected);
  return match&&groups[match[1]]?{profile:match[1],view:match[2]}:{profile:'',view:''}
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
    """
function configurationLabel(key){
  const labels={'ydbd-binary':'YDBD binary','ydb-cli':'YDB CLI','cpus':'CPUs','max-ms':'Maximum latency (ms)',
    'disk-size-gb':'Disk size (GiB)','warmup':'Warmup (s)','duration':'Duration (s)','timeout':'Timeout (s)',
    'min-achieved-rate-ratio':'Minimum achieved rate ratio'};
  return labels[key]||key.replaceAll('-',' ').replaceAll('_',' ').replace(/^./,letter=>letter.toUpperCase())
}
function configurationFields(value){
  const scalar=item=>Array.isArray(item)?item.map(scalar).join(', '):item===null?'—':String(item);
  if(!value||typeof value!=='object'||Array.isArray(value))return '<p>'+esc(scalar(value))+'</p>';
  const fields=[],groups=[];
  for(const [key,item] of Object.entries(value)){
    if(item&&typeof item==='object'&&!Array.isArray(item))groups.push(
      '<div class=configuration-subgroup><h4>'+esc(configurationLabel(key))+'</h4>'+configurationFields(item)+'</div>'
    );
    else fields.push('<div><dt>'+esc(configurationLabel(key))+'</dt><dd>'+esc(scalar(item))+'</dd></div>')
  }
  return (fields.length?'<dl class=configuration-values>'+fields.join('')+'</dl>':'')+groups.join('')
}
function configurationProfile(value){
  const titles={workload:'Workload',load:'Load & objective',measurement:'Measurement',geometry:'Cluster',
    affinity:'CPU placement','actor-system':'Actor system',client:'YDB CLI'};
  const sections=[],general={};
  for(const [key,item] of Object.entries(value)){
    if(!Object.hasOwn(titles,key))Object.defineProperty(general,key,{value:item,enumerable:true})
  }
  const section=(title,body)=>'<section><h3>'+esc(title)+'</h3>'+body+'</section>';
  if(value.workload){
    sections.push(section('Workload',configurationFields(general)+configurationFields(value.workload)));
    if(value.load||value.client)sections.push(section('Load & objective',
      (value.load?configurationFields(value.load):'')+(value.client?'<h4>YDB CLI</h4>'+configurationFields(value.client):'')
    ));
    if(value.measurement)sections.push(section('Measurement',configurationFields(value.measurement)));
    if(value.geometry||value['actor-system'])sections.push(section('Cluster',
      (value.geometry?configurationFields(value.geometry):'')+
      (value['actor-system']?'<h4>Actor system</h4>'+configurationFields(value['actor-system']):'')
    ));
    if(value.affinity)sections.push('<section class=configuration-wide><h3>CPU placement</h3><div class=configuration-role-grid>'+
      configurationFields(value.affinity)+'</div></section>')
  }else{
    if(Object.keys(general).length)sections.push(section('General',configurationFields(general)));
    for(const [key,title] of Object.entries(titles))if(Object.hasOwn(value,key))sections.push(section(title,configurationFields(value[key])))
  }
  return '<div class=configuration-grid>'+sections.join('')+'</div>'
}
const configurationSelections=new Map();
function runConfigurationHtml(id,saved){
  const profiles=[];
  for(const [benchmark,items] of Object.entries(saved.structured||{})){
    if(items&&typeof items==='object'&&!Array.isArray(items))for(const [name,value] of Object.entries(items)){
      if(value&&typeof value==='object'&&!Array.isArray(value))profiles.push({key:benchmark+'/'+name,value})
    }
  }
  const previous=configurationSelections.get(id);
  const selected=previous==='YAML'||profiles.some(profile=>profile.key===previous)?previous:profiles[0]?.key||'YAML';
  const tabs='<nav class=profile-list>'+profiles.map((profile,index)=>
    '<button type=button data-config-profile="'+index+'" class="'+(profile.key===selected?'selected':'')+'">'+esc(profile.key)+'</button>'
  ).join('')+'<button type=button data-config-profile="yaml" class="'+(selected==='YAML'?'selected':'')+'">YAML</button></nav>';
  const panels=profiles.map((profile,index)=>'<div data-config-panel="'+index+'" '+(profile.key===selected?'':'hidden')+'>'+configurationProfile(profile.value)+'</div>').join('');
  return '<section id=run-configuration-view class=new-run-page>'+tabs+
    '<p class=muted>perf: '+(saved.perf?'on':'off')+' · Continue on error: '+(saved.continue_on_error?'on':'off')+'</p>'+
    panels+'<div data-config-panel="yaml" '+(selected==='YAML'?'':'hidden')+'><pre class=run-configuration><code>'+esc(saved.yaml)+'</code></pre></div></section>'
}
function bindRunConfiguration(container,id){
  if(!container)return;
  for(const button of container.querySelectorAll('[data-config-profile]'))button.onclick=()=>{
    for(const other of container.querySelectorAll('[data-config-profile]'))other.classList.toggle('selected',other===button);
    for(const panel of container.querySelectorAll('[data-config-panel]'))panel.hidden=panel.dataset.configPanel!==button.dataset.configProfile;
    configurationSelections.set(id,button.textContent)
  }
}
"""
    "async function renderRun(id,selectedProfile='',runView=''){\n"
    '  clearRefresh();\n'
    '  try{\n'
    "    const run=await api('/api/runs/'+enc(id));\n"
    "    const directory=await api('/api/hosts'),owner=splitRunRef(id)?.host||viewedHost||directory.local.id;\n"
    "    const hostName=[directory.local,...directory.hosts].find(host=>host.id===owner)?.name||owner;\n"
    "    activeRun=run.current_run_id||(['running','recovery_required'].includes(run.state)?id:'');\n"
    "    const queueNotice=run.state==='queued'?'<div class=notice>Queue position: '+esc(run.queue_position??'—')+'. '+(run.c"
    'urrent_run_id?\'<a href="#run/\'+enc(run.current_run_id)+\'">Currently running: \'+esc(run.current_run_id)+\'</a>\':\'Waiting f'
    "or the dispatcher.')+'</div>':'';\n"
    "    sessionStorage.setItem('ydb-bench-active-run',activeRun);\n"
    '    const groups=profileGroups(run.steps||[]),profileKeys=Object.keys(groups),selection=parseLocalYdbProfileSelection('
    "groups,selectedProfile),activeProfile=runView==='configuration'?'':selection.profile||(profileKeys.length===1?profileKeys[0]:''),requestedLocalView="
    "selection.profile?selection.view:'',activeBenchmark=activeProfile?activeProfile.split('/')[0]:'';\n"
    "    const crumbs=[{route:'runs',label:'Runs'},{route:'run/'+enc(id),label:runDisplay(id)}];if(activeProfile&&profileKeys.length>1)cr"
    "umbs.push({route:'run/'+enc(id)+'/profile/'+enc(activeProfile),label:activeProfile});\n"
    "    let content=breadcrumbs(crumbs)+queueNotice+'<div class=run-header><h1 class=page-title>'+esc(activeProfile||runDisplay(id))+'</h1><div class=toolbar><button id=ref"
    "resh-run>Refresh</button>'+(['queued','running'].includes(run.state)?'<button class=danger id=cancel-run>Cancel</button>'"
    ":'')+'<button id=repeat-run>Repeat with this YAML</button><details class=downloads><summary>Downloads</summary><div cla"
    "ss=actions><a href=\"'+runHref(id,'config')+'\">YAML</a><a href=\"'+runHref(id,'manifest')+'\">run.json</a><a href=\"'+r"
    "unHref(id,'archive')+'\">Archive.zip</a></div></details></div></div><p class=muted>'+esc(hostName)+' · '+status(run.status)+' · '+"
    "esc(humanTime(run.started_at))+' · Run duration '+duration(run)+' · '+run.finished_steps+' / '+run.steps.length+"
    "' steps</p><div class=grid>';\n"
    "    if(run.state==='recovery_required')content+='<div class=\"notice error\"><strong>Interrupted.</strong> The web servi"
    "ce restarted while this run was active. Verify that the previous benchmark process stopped before repeating it.</div>'"
    ";\n"
    "    content+='<nav class=run-tabs>'+(profileKeys.length!==1?'<a class=\"run-tab '+(!activeProfile&&!runView?'active':'')+'\" href=\"#r"
    "un/'+enc(id)+'\">Overview</a>':'')+profileKeys.map(key=>'<a class=\"run-tab '+(key===activeProfile?'active':'')+'\" href=\"#"
    "run/'+enc(id)+'/profile/'+enc(key)+'\">'+esc(key)+'</a>').join('')+'<a class=\"run-tab '+(runView==='configuration'?'active':'')+'\" "
    "href=\"#run/'+enc(id)+'/configuration\">Configuration</a></nav>';\n"
    "    if(runView==='configuration'){try{const saved=await api('/api/runs/'+enc(id)+'/config.json');"
    "content+=runConfigurationHtml(id,saved)}"
    "catch(error){content+=displayError(error)}}\n"
    "    if(!activeProfile&&!runView)content+='<section class=\"card profile-overview\"><h2>Profiles</h2><table><tr><th>Profile</th><th"
    ">Progress</th><th>State</th><th>Affinity modes</th></tr>'+profileKeys.map(key=>{const steps=groups[key],done=steps.fil"
    "ter(step=>!['pending','running'].includes(step.state)).length,affinities=new Set(steps.map(step=>step.affinity)).size;re"
    "turn '<tr><td><a href=\"#run/'+enc(id)+'/profile/'+enc(key)+'\">'+esc(key)+'</a></td><td>'+done+' / '+steps.length+'</td"
    "><td>'+status(aggregateState(steps))+'</td><td>'+affinities+'</td></tr>'}).join('')+'</table></section>';\n"
    "    if(activeProfile)content+=activeBenchmark==='local-ydb'?'<section class=\"card local-result-container\"><div id=local-ydb-result>Loading profile data…</div>"
    "</section>':'<section class=card><div class=run-section-title><h2>Results</h2><strong>'+esc(activeProfile)+'</strong>"
    "</div><p class=muted>Affinity variants are lines. Choose a common X axis, one or more Y metrics, and fixed values for "
    "the remaining dimensions.</p><div id=run-chart>Loading summary data…</div></section>';\n"
    "    if(activeProfile){const steps=groups[activeProfile],open=run.state==='running'?' open':'';content+='<section class=\""
    "card run-tree\"><details'+open+'><summary><strong>Execution details</strong> — affinity, cases and artifacts</summary><"
    "table><tr><th>Affinity</th><th>Runs</th><th>State</th></tr>'+affinityRows(id,steps)+'</table></details></section>'}\n"
    "    const running=(run.steps||[]).find(step=>step.state==='running'),live=['running','queued','failed','recovery_requir"
    "ed'].includes(run.state),showLiveOutput=activeBenchmark!=='local-ydb';\n"
    "    if(live&&!runView)content+='<section class=card><h2>Current step</h2>'+ (running?'<p><strong>'+esc(running.benchmark)+' / '+e"
    "sc(running.profile)+'</strong>, '+esc(running.affinity)+', '+esc(running.threads??'—')+' threads, repeat '+running.repe"
    "at+', elapsed '+esc(stepDuration(running))+'</p>':'<p class=muted>No step is currently running.</p>')+(showLiveOutput"
    "?'<h3>Live stdout</h3><pre class=log>'+esc(run.tail?.stdout||'No stdout captured yet.')+'</pre><h3>Live stderr</h3><p"
    "re class=log>'+esc(run.tail?.stderr||'No stderr captured yet.')+'</pre>':'')+'</section>';content+='</div>';\n"
    "    app.innerHTML=shell('runs',content);\n"
    "    if(runView==='configuration')bindRunConfiguration(document.querySelector('#run-configuration-view'),id);\n"
    "    const selectedRoute=()=>{const local=document.querySelector('#local-ydb-result');return activeBenchmark==='local-ydb'&&"
    "local?.dataset.localYdbViewExplicit==='true'?activeProfile+'/view/'+local.dataset.localYdbView:activeProfile};\n"
    "    document.querySelector('#refresh-run').onclick=()=>renderRun(id,selectedRoute(),runView);\n"
    "    document.querySelector('#repeat-run').onclick=()=>reuseRun(id);\n"
    "    const cancel=document.querySelector('#cancel-run');\n"
    "    if(cancel)cancel.onclick=async()=>{try{await api('/api/runs/'+enc(id)+'/cancel',jsonOptions({}));renderRun(id,sele"
    'ctedRoute(),runView)}catch(error){alert(error.message)}};\n'
    "    if(activeProfile){const pieces=activeProfile.split('/'),benchmark=pieces.shift(),profile=pieces.join('/');if("
    "benchmark==='local-ydb')await mountLocalYdbProfile(document.querySelector('#local-ydb-result'),id,profile,run.state,requestedLocalView);"
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
    "\nfunction topologyGroups(topology){\n  const allowed=new Set(topology.allowed_cpus),seen=new Set();\n  const cores=(topology.physical_cores||["
    "]).map((cpus,index)=>({index,cpus:cpus.filter(cpu=>allowed.has(cpu))})).filter(core=>core.cpus.length);\n  for(const core of cores)for(const "
    "cpu of core.cpus)seen.add(cpu);\n  for(const cpu of allowed)if(!seen.has(cpu))cores.push({index:cores.length,cpus:[cpu]});\n  const nodes=topo"
    "logy.numa_nodes.length?topology.numa_nodes:[{id:'—',cpus:[...allowed]}];\n  return nodes.map(node=>{\n    const assigned=new Set(),groups=[];\n"
    "    for(const chiplet of topology.chiplets.filter(item=>item.numa_node===node.id)){\n      const cpus=chiplet.cpus.filter(cpu=>allowed.has(cp"
    "u)&&node.cpus.includes(cpu)&&!assigned.has(cpu));\n      if(!cpus.length)continue;cpus.forEach(cpu=>assigned.add(cpu));\n      groups.push({la"
    "bel:chiplet.label||'L3 / chiplet '+groups.length,cpus});\n    }\n    const rest=node.cpus.filter(cpu=>allowed.has(cpu)&&!assigned.has(cpu));\n "
    "   if(rest.length)groups.push({label:groups.length?'Other CPUs':'Cache grouping unavailable',cpus:rest});\n    return {...node,groups:groups."
    "map(group=>({...group,cores:cores.map(core=>({...core,cpus:core.cpus.filter(cpu=>group.cpus.includes(cpu))})).filter(core=>core.cpus.length)"
    "}))};\n  });\n}\nasync function renderTopology(){\n  clearRefresh();\n  try{\n    const hostOptions=await hostChoices(viewedHost,false);\n"
    "    const value=await api('/api/system-topology'),t=value.topology;\n"
    "    if(location.hash!=='#topology')return;\n    const nodes=topologyGroups(t),all=nodes.flatMap(n=>n.groups.flatMap(g=>g.cores));\n    const l"
    "ayout=nodes.map(node=>'<section class=cpu-node><div class=cpu-node-name><strong>NUMA '+esc(node.id)+'</strong><small data-node-usage=\"'+esc("
    "node.id)+'\">—</small></div><div class=cpu-groups>'+node.groups.map(group=>\n      '<div class=cpu-group><div class=cpu-group-label>'+esc(grou"
    "p.label)+'</div><div class=cpu-core-grid>'+group.cores.map(core=>\n        '<button class=cpu-core data-core=\"'+core.index+'\" aria-pressed=fa"
    "lse aria-label=\"Core '+core.index+'; vCPU '+esc(core.cpus.join(', '))+'\">'+core.cpus.map(cpu=>'<span class=cpu-cell data-cpu=\"'+cpu+'\">'+cpu"
    "+'</span>').join('')+'</button>'\n      ).join('')+'</div></div>').join('')+'</div></section>').join('');\n    app.innerHTML=shell('topology',"
    "'<div class=runs-toolbar><label>Host <select id=topology-host>'+hostOptions+'</select></label></div>"
    "<div id=cpu-topology><p class=muted>'+t.physical_cores.length+' physical cores · '+t.allowed_cpus."
    "length+' allowed vCPUs · '+t.numa_nodes.length+' NUMA nodes</p>'+\n      sectionTabs('topology',[['layout','Topology & CPU usage'],['affinity"
    "','Affinity availability']])+\n      '<section data-section-panel=\"topology:layout\"><div class=cpu-map-toolbar><div class=cpu-help><button id"
    "=cpu-help-button aria-label=\"About the CPU map\" aria-expanded=false aria-controls=cpu-map-help>?</button><div id=cpu-map-help hidden role=no"
    "te><p>Columns group known physical cores and their visible SMT threads; unknown topology uses single-vCPU groups. "
    "Numbers are vCPU IDs. macOS does not expose iowait or steal counters; these appear as unavailable.</p>"
    "<p>Colour shows busy CPU usage, excluding "
    "idle and iowait. Hover for values; click to keep a core selected. User includes nice; system includes IRQ time. Steal is reported separately"
    ".</p><p>Only CPUs allowed by this process cpuset are shown. Missing counters are not zero usage.</p>'+t.hierarchy_reasons.map(item=>'<p>'+es"
    "c(item.level)+': '+esc(item.reason)+'</p>').join('')+'</div></div><small id=cpu-sample-status>Waiting for CPU samples…</small><small>0% <spa"
    "n class=cpu-heat-scale></span> 100%</small></div><div id=cpu-selection class=cpu-selection>Select a core to inspect its vCPUs.</div>'+layout"
    "+'</section>'+\n      '<section data-section-panel=\"topology:affinity\" hidden><h2>Affinity availability</h2>'+affinityTree(value.affinity)+'<"
    "/section></div>');\n    app.querySelector('#topology-host').onchange=event=>{location.href='/?host='+enc(event.target.value)+'#topology'};\n"
    "    const target=document.querySelector('#cpu-topology');bindSectionTabs(target,'topology');\n    const help=target.queryS"
    "elector('#cpu-map-help'),helpButton=target.querySelector('#cpu-help-button');\n    const closeHelp=()=>{help.hidden=true;helpButton.setAttrib"
    "ute('aria-expanded','false')};\n    helpButton.onclick=()=>{help.hidden=!help.hidden;helpButton.setAttribute('aria-expanded',String(!help.hid"
    "den))};\n    target.addEventListener('keydown',e=>{if(e.key==='Escape')closeHelp()});\n    target.addEventListener('click',e=>{if(!e.target.cl"
    "osest('.cpu-help'))closeHelp()});\n    let selected=null,hovered=null,samples={},loading=false;\n    const pct=v=>Number.isFinite(v)?v.toFixed"
    "(2)+'%':'—';\n    function details(){\n      const index=hovered??selected,core=all.find(c=>c.index===index),box=target.querySelector('#cpu-se"
    "lection');\n      box.innerHTML=core?'<strong>Core '+core.index+'</strong>'+core.cpus.map(cpu=>{\n        const s=samples[cpu];return '<div><s"
    "trong>vCPU '+cpu+' · '+pct(s?.busy)+'</strong><small>User '+pct(s?.user)+' · system '+pct(s?.system)+' · iowait '+pct(s?.iowait)+' · steal '"
    "+pct(s?.steal)+'</small></div>'\n      }).join(''):'Select a core to inspect its vCPUs.';\n    }\n    for(const button of target.querySelectorA"
    "ll('[data-core]')){\n      const id=Number(button.dataset.core);\n      button.onclick=()=>{selected=id;target.querySelectorAll('[data-core]')"
    ".forEach(b=>b.setAttribute('aria-pressed',String(Number(b.dataset.core)===id)));details()};\n      button.onmouseenter=button.onfocus=()=>{ho"
    "vered=id;details()};\n      button.onmouseleave=button.onblur=()=>{hovered=null;details()};\n    }\n    const refresh=async()=>{\n      if(loadi"
    "ng||!target.isConnected||location.hash!=='#topology')return;loading=true;\n      try{\n        const data=await api('/api/cpu-usage');if(!targ"
    "et.isConnected)return;samples=data.cpus||{};\n        target.querySelector('#cpu-sample-status').textContent=!data.available?'CPU usage unava"
    "ilable on this host':Object.values(samples).some(v=>v!==null)?'Updated '+new Date().toLocaleTimeString():'Waiting for second CPU sample…';\n "
    "       for(const cell of target.querySelectorAll('[data-cpu]')){\n          const busy=samples[cell.dataset.cpu]?.busy,valid=Number.isFinite("
    "busy);\n          cell.style.background=valid?'color-mix(in srgb, #2167b9 '+busy+'%, #edf2f8)':'';\n          cell.style.color=valid&&busy>55?"
    "'#fff':'';\n          cell.parentElement.setAttribute('aria-label','Core '+cell.parentElement.dataset.core+'; '+[...cell.parentElement.childr"
    "en].map(c=>'vCPU '+c.dataset.cpu+' '+pct(samples[c.dataset.cpu]?.busy)).join('; '));\n        }\n        for(const node of nodes){\n          c"
    "onst values=node.cpus.map(cpu=>samples[cpu]?.busy).filter(Number.isFinite);\n          const el=[...target.querySelectorAll('[data-node-usage"
    "]')].find(e=>e.dataset.nodeUsage===String(node.id));\n          if(el)el.textContent=values.length===node.cpus.length&&values.length?pct(valu"
    "es.reduce((a,b)=>a+b,0)/values.length)+' busy':'—';\n        }\n        details();\n      }catch(error){if(target.isConnected){samples={};targe"
    "t.querySelector('#cpu-sample-status').textContent='CPU sampling failed';target.querySelectorAll('.cpu-cell').forEach(c=>{c.style.background="
    "'';c.style.color=''});target.querySelectorAll('[data-node-usage]').forEach(e=>e.textContent='—');details()}}\n      finally{loading=false}\n  "
    "  };\n    refreshTimer=setInterval(refresh,2000);await refresh();\n  }catch(error){if(location.hash==='#topology')app.innerHTML=shell('topolog"
    "y',displayError(error))}\n}\n"
    """
function filterSavedComparisons(records,filters){
  const query=(filters.query||'').trim().toLowerCase();
  return records.filter(record=>{
    const date=(record.created_at||'').slice(0,10);
    return (!query||[record.name,...record.profiles.flat()].join(' ').toLowerCase().includes(query))&&
      (!filters.since||date>=filters.since)&&(!filters.until||date<=filters.until)
  }).sort((a,b)=>{
    const dates=(Date.parse(a.created_at)||0)-(Date.parse(b.created_at)||0);
    const order=filters.sort==='name'?a.name.localeCompare(b.name):filters.sort==='oldest'?dates:-dates;
    return order||a.id.localeCompare(b.id)
  })
}
function filterComparisonRuns(runs,filters,selected){
  const query=(filters.query||'').trim().toLowerCase();
  return sortRuns(runs.filter(run=>{
    const names=Array.isArray(run.profile_names)?run.profile_names:[];
    const benchmarks=Array.isArray(run.benchmarks)?run.benchmarks:[];
    const date=(run.started_at||run.queued_at||'').slice(0,10);
    return (!filters.only||selected.has(run.id))&&(!filters.status||run.status===filters.status)&&
      (!filters.benchmark||benchmarks.includes(filters.benchmark))&&(!filters.since||date>=filters.since)&&
      (!query||[run.id,...names,...benchmarks].join(' ').toLowerCase().includes(query))
  }),filters.sort||'newest')
}
async function renderSavedComparisons(){
  clearRefresh();
  const route=location.hash,parts=route.slice(1).split('?')[0].split('/').map(decodeURIComponent),id=parts[1];
  const active=()=>location.hash===route;
  try{
    const catalog=await api('/api/federation/comparisons'),records=catalog.entries;
    if(!active())return;
    if(!id){
      app.innerHTML=shell('comparisons',
        federationErrors(catalog.errors)+'<div class=filters><label class=field>Comparison, profile or run<input id=saved-comparison-query type=search placeholder="Search comparisons"></label>'+
        '<label class=field>Created from (UTC)<input id=saved-comparison-since type=date></label>'+
        '<label class=field>Created to (UTC)<input id=saved-comparison-until type=date></label></div>'+
        '<div class=runs-toolbar><label>Sort <select id=saved-comparison-sort><option value=newest>Newest first</option>'+
        '<option value=oldest>Oldest first</option><option value=name>Name A–Z</option></select></label>'+
        '<span id=saved-comparison-count class=muted aria-live=polite></span><div class=runs-actions>'+
        '<button id=reset-comparison-filters hidden>Reset filters</button>'+
        '<a class=new-run-link href="#comparisons/new"><span aria-hidden=true>+</span> New comparison</a></div></div><div id=saved-comparison-list></div>');
      const query=app.querySelector('#saved-comparison-query'),since=app.querySelector('#saved-comparison-since'),
        until=app.querySelector('#saved-comparison-until'),sort=app.querySelector('#saved-comparison-sort'),
        list=app.querySelector('#saved-comparison-list'),count=app.querySelector('#saved-comparison-count');
      const draw=()=>{
        const filtered=filterSavedComparisons(records,{query:query.value,since:since.value,until:until.value,sort:sort.value});
        count.textContent=filtered.length+' / '+records.length+' comparisons';
        list.innerHTML=!records.length?'<div class=empty>No saved comparisons.</div>':
          !filtered.length?'<div class=empty>No comparisons match these filters.</div>':
          '<div class=table-scroll><table><thead><tr><th>Comparison</th><th>Created</th><th>Profiles</th></tr></thead><tbody>'+
          filtered.map(record=>'<tr data-comparison-id="'+esc(record.id)+'"><td><a href="#comparisons/'+enc(record.id)+'">'+
            esc(record.name)+'</a><div class=muted>'+esc(record.host_name)+' · '+record.profiles.map(pair=>esc(pair[1])).join(' · ')+
            '</div></td><td>'+esc(humanTime(record.created_at))+'</td><td>'+record.profiles.length+'</td></tr>').join('')+
          '</tbody></table></div>';
        for(const row of list.querySelectorAll('[data-comparison-id]'))row.onclick=event=>{
          if(event.target.closest('a,button,input,select')||event.button!==0||event.ctrlKey||event.metaKey||event.shiftKey||event.altKey)return;
          setRoute('comparisons/'+row.dataset.comparisonId)
        }
      };
      bindAutomaticFilters([query,since,until],app.querySelector('#reset-comparison-filters'),draw,()=>list.isConnected);
      sort.onchange=draw;
      draw();
      return
    }
    const record=id==='new'?null:records.find(item=>item.id===id||(!item.remote&&!splitRunRef(id)&&runDisplay(item.id)===id));
    if(id!=='new'&&!record)throw Error('Comparison not found');
    const editing=id==='new'||parts[2]==='edit';
    const crumb='<div class=breadcrumbs><a href="#comparisons">Comparisons</a> / '+esc(record?.name||'New comparison')+'</div>';
    if(editing){
      if(record?.remote)throw Error('Edit this comparison on its owning host: '+record.host_name);
      const runCatalog=await api('/api/federation/runs'),runs=runCatalog.entries,hostOptions=await hostChoices();if(!active())return;
      const selected=new Map((record?.profiles||[]).map(pair=>[JSON.stringify(pair),pair]));
      const seeds=record?[...new Set(record.profiles.map(pair=>pair[0]))]:new URLSearchParams(route.split('?')[1]||'').getAll('run');
      const chosenRuns=new Set(seeds),cache=new Map(),pending=new Map(),errors=new Map(),autoSelect=new Set(record?[]:seeds);
      let baseline=record?JSON.stringify(record.baseline):'',saving=false;
      const options=values=>'<option value="">All</option>'+[...new Set(values)].sort().map(value=>'<option value="'+esc(value)+'">'+esc(value)+'</option>').join('');
      app.innerHTML=shell('comparisons',crumb+'<h1 class=page-title>'+(record?'Edit comparison':'New comparison')+'</h1>'+
        '<div class=toolbar><label>Name <input id=comparison-name maxlength=200 value="'+esc(record?.name||'')+'"></label>'+
        '<button id=save-saved-comparison>'+(record?'Save':'Create comparison')+'</button><a href="#comparisons'+
        (record?'/'+enc(record.id):'')+'">Cancel</a></div>'+federationErrors(runCatalog.errors)+'<div id=comparison-error role=alert></div>'+
        '<div class=filters><div class=field><label for=comparison-query>Run or profile</label><input id=comparison-query placeholder="Name, profile or run ID"></div>'+
        '<div class=field><label for=comparison-host>Host</label><select id=comparison-host>'+hostOptions+'</select></div>'+
        '<div class=field><label for=comparison-status>Status</label><select id=comparison-status>'+options(runs.map(run=>run.status))+'</select></div>'+
        '<div class=field><label for=comparison-benchmark>Benchmark</label><select id=comparison-benchmark>'+
        options(runs.flatMap(run=>run.benchmarks||[]))+'</select></div><div class=field><label for=comparison-since>Started since</label>'+
        '<input id=comparison-since type=date></div></div><div class=runs-toolbar><span id=comparison-selection-count aria-live=polite></span>'+
        '<label><input id=comparison-selected-only type=checkbox> Selected only</label><button id=comparison-reset>Reset filters</button>'+
        '<label>Sort <select id=comparison-sort><option value=newest>Newest first</option><option value=oldest>Oldest first</option>'+
        '<option value=longest>Longest first</option></select></label></div><div class=table-scroll><table><thead><tr>'+
        '<th></th><th>Run / profiles</th><th>Started</th><th>Duration</th><th>Status</th></tr></thead><tbody id=comparison-runs></tbody></table></div>'+
        '<h3 id=comparison-profiles-title>Profiles</h3><div id=comparison-load-status aria-live=polite></div>'+
        '<div id=comparison-profile-options></div><label>Baseline <select id=comparison-baseline></select></label>');
      const element=id=>document.querySelector('#'+id);
      const drawProfiles=()=>{
        const available=[...chosenRuns].flatMap(id=>cache.get(id)||[]);
        const choices=new Map(available.map(item=>[localComparisonKey(item),[item.run,item.profile]]));
        for(const [key,pair] of selected)if(!choices.has(key))choices.set(key,pair);
        element('comparison-profile-options').innerHTML=[...choices].map(([key,pair])=>
          '<label class=comparison-profile-choice><input type=checkbox data-saved-profile value="'+esc(key)+'" '+(selected.has(key)?'checked':'')+'>'+
          '<span>'+esc(pair[1])+'</span><span class=muted>'+esc(runs.find(run=>run.id===pair[0])?.host_name||'')+' · '+esc(runDisplay(pair[0]))+'</span></label>').join('')||
          '<div class=muted>Select runs above to load profiles.</div>';
        element('comparison-profiles-title').textContent='Profiles · '+selected.size;
        if(!selected.has(baseline))baseline=selected.keys().next().value||'';
        element('comparison-baseline').innerHTML=[...selected].map(([key,pair])=>'<option value="'+esc(key)+'" '+
          (key===baseline?'selected':'')+'>'+esc((runs.find(run=>run.id===pair[0])?.host_name||'')+' / '+runDisplay(pair[0])+' / '+pair[1])+'</option>').join('');
        const loading=[...chosenRuns].filter(id=>pending.has(id));
        element('save-saved-comparison').disabled=saving||!!loading.length||!selected.size||!element('comparison-name').value.trim();
        element('comparison-load-status').innerHTML=(loading.length?'<div class=muted>Loading profiles for '+loading.length+' runs…</div>':'')+
          [...chosenRuns].filter(id=>errors.has(id)).map(id=>'<div class=notice>'+esc(id+': '+errors.get(id))+
          ' <button data-retry-run="'+esc(id)+'">Retry</button></div>').join('');
        for(const input of app.querySelectorAll('[data-saved-profile]'))input.onchange=()=>{
          if(input.checked)selected.set(input.value,JSON.parse(input.value));else selected.delete(input.value);drawProfiles()
        };
        for(const button of app.querySelectorAll('[data-retry-run]'))button.onclick=()=>loadRun(button.dataset.retryRun);
      };
      const drawRuns=()=>{
        const visible=filterComparisonRuns(runs.filter(run=>!element('comparison-host').value||run.host_id===element('comparison-host').value),{
          query:element('comparison-query').value,status:element('comparison-status').value,
          benchmark:element('comparison-benchmark').value,since:element('comparison-since').value,
          only:element('comparison-selected-only').checked,sort:element('comparison-sort').value
        },chosenRuns);
        const outside=[...chosenRuns].filter(id=>!visible.some(run=>run.id===id)).length;
        element('comparison-selection-count').textContent=chosenRuns.size+' selected'+(outside?' · '+outside+' outside filters':'')+' · '+visible.length+' shown';
        element('comparison-runs').innerHTML=visible.map(run=>'<tr data-picker-run="'+esc(run.id)+'" class="'+
          (chosenRuns.has(run.id)?'comparison-run-selected':'')+'"><td><input type=checkbox aria-label="Select '+esc(run.id)+
          '" '+(chosenRuns.has(run.id)?'checked':'')+'></td><td><div>'+esc((run.profile_names||[]).join(' · ')||'No profiles')+
          '</div><div class=muted>'+esc(run.host_name)+' · '+esc((run.benchmarks||[]).join(' · '))+' · '+esc(run.run_id||runDisplay(run.id))+'</div></td><td>'+
          esc(humanTime(run.started_at||run.queued_at))+'</td><td>'+duration(run)+'</td><td>'+status(run.status)+'</td></tr>').join('')||
          '<tr><td colspan=5>No runs match these filters.</td></tr>';
        for(const row of app.querySelectorAll('[data-picker-run]')){
          row.onclick=event=>{
            if(event.target.closest('input,button,a,select')||event.button!==0||event.ctrlKey||event.metaKey||event.shiftKey||event.altKey)return;
            toggleRun(row.dataset.pickerRun)
          };
          row.querySelector('input').onchange=()=>toggleRun(row.dataset.pickerRun)
        }
      };
      const loadRun=async id=>{
        if(pending.has(id))return;
        errors.delete(id);
        const request=loadLocalYdbComparison([id]);pending.set(id,request);drawProfiles();
        try{
          const result=await request;if(!active())return;
          cache.set(id,result.entries||[]);
          if(chosenRuns.has(id)&&autoSelect.has(id)){
            for(const item of result.entries||[])selected.set(localComparisonKey(item),[item.run,item.profile]);
            autoSelect.delete(id)
          }
          if(!(result.entries||[]).length)errors.set(id,'No local YDB profiles in this run')
        }catch(error){if(active())errors.set(id,error.message)}
        finally{pending.delete(id);if(active())drawProfiles()}
      };
      const toggleRun=id=>{
        element('comparison-error').innerHTML='';
        if(chosenRuns.has(id)){
          chosenRuns.delete(id);autoSelect.delete(id);
          for(const [key,pair] of selected)if(pair[0]===id)selected.delete(key)
        }else{
          if(chosenRuns.size>=20){element('comparison-error').textContent='Select at most 20 runs.';return}
          chosenRuns.add(id);autoSelect.add(id);
          if(cache.has(id)){
            for(const item of cache.get(id))selected.set(localComparisonKey(item),[item.run,item.profile]);
            autoSelect.delete(id)
          }else loadRun(id)
        }
        drawRuns();drawProfiles()
      };
      element('comparison-baseline').onchange=event=>{baseline=event.target.value};
      element('comparison-name').oninput=drawProfiles;
      element('comparison-query').oninput=drawRuns;
      for(const id of ['comparison-host','comparison-status','comparison-benchmark','comparison-since','comparison-selected-only','comparison-sort'])element(id).onchange=drawRuns;
      element('comparison-reset').onclick=()=>{
        for(const id of ['comparison-query','comparison-host','comparison-status','comparison-benchmark','comparison-since'])element(id).value='';
        element('comparison-selected-only').checked=false;drawRuns()
      };
      element('save-saved-comparison').onclick=async()=>{
        if(saving)return;saving=true;drawProfiles();
        try{
          const saved=await api('/api/saved-comparisons',jsonOptions({...record,name:element('comparison-name').value,
            profiles:[...selected.values()],baseline:JSON.parse(baseline)}));
          if(active())setRoute('comparisons/'+saved.id)
        }catch(error){if(active())element('comparison-error').innerHTML=displayError(error)}
        finally{saving=false;if(active())drawProfiles()}
      };
      drawRuns();drawProfiles();for(const id of chosenRuns)loadRun(id);return
    }
    app.innerHTML=shell('comparisons',crumb+'<div class=toolbar><h1 class=page-title>'+esc(record.name)+'</h1>'+
      (record.remote?'<span class=muted>Stored on '+esc(record.host_name)+' · read-only</span>':
        '<a href="#comparisons/'+enc(record.id)+'/edit">Edit comparison</a><button id=delete-comparison>Delete</button>')+'</div>'+
      '<div class=muted>'+record.profiles.length+' profiles · Baseline: '+esc(runDisplay(record.baseline[0])+' / '+record.baseline[1])+'</div>'+
      '<div id=comparison-error></div><div id=comparison-missing></div><section id=local-ydb-comparison>Loading profiles…</section>');
    const deleteComparison=document.querySelector('#delete-comparison');
    if(deleteComparison)deleteComparison.onclick=async()=>{
      if(!confirm('Delete comparison "'+record.name+'"? Benchmark results will be kept.'))return;
      try{await api('/api/saved-comparisons/delete',jsonOptions({id:record.id,revision:record.revision}));if(active())setRoute('comparisons')}
      catch(error){if(active())document.querySelector('#comparison-error').innerHTML=displayError(error)}
    };
    const entries=[],errors=[];
    for(const run of [...new Set(record.profiles.map(pair=>pair[0]))]){
      try{entries.push(...(await loadLocalYdbComparison([run])).entries)}catch(error){errors.push(run+': '+error.message)}
      if(!active())return
    }
    const keys=record.profiles.map(pair=>JSON.stringify(pair)),found=entries.filter(item=>keys.includes(localComparisonKey(item)));
    const missing=record.profiles.filter(pair=>!found.some(item=>localComparisonKey(item)===JSON.stringify(pair)));
    document.querySelector('#comparison-missing').innerHTML=missing.map(pair=>'<div class=notice>Result unavailable: '+esc(pair.join(' / '))+'</div>').join('')+
      errors.map(error=>displayError(error)).join('');
    const target=document.querySelector('#local-ydb-comparison');
    if(!found.some(item=>localComparisonKey(item)===JSON.stringify(record.baseline))){
      target.innerHTML='<div class=empty>Baseline unavailable. Edit comparison to choose another baseline.</div>';return
    }
    target.dataset.profiles=JSON.stringify(keys);target.dataset.baseline=JSON.stringify(record.baseline);target.dataset.restored='true';
    mountLocalYdbComparison(target,{entries:found,readonly:true})
  }catch(error){if(active())app.innerHTML=shell('comparisons',displayError(error))}
}
async function renderComparisons(){
  clearRefresh();
  try{
    const value=await api('/api/comparisons');
    const content='<h1 class=page-title>Comparisons</h1><section><div id=local-ydb-comparison>'+
      (value.selected.length?'Loading profiles…':'<a href="#runs">Select runs in Runs</a> to compare their profiles.')+
      '</div></section><section id=other-comparisons hidden><h2>Other benchmarks</h2><div id=comparison-chart></div></section>';
    app.innerHTML=shell('comparisons',content);
    if(!value.selected.length)return;
    const [local,charts]=await Promise.allSettled([loadLocalYdbComparison(value.selected),loadChartData(value.selected)]);
    if(location.hash!=='#comparisons')return;
    const target=document.querySelector('#local-ydb-comparison');
    if(local.status==='fulfilled')mountLocalYdbComparison(target,local.value);
    else target.innerHTML=displayError(local.reason);
    if(charts.status==='fulfilled'){
      const other={...charts.value,series:(charts.value.series||[]).filter(item=>item.benchmark!=='local-ydb')};
      if(other.series.length){
        document.querySelector('#other-comparisons').hidden=false;
        mountChartBuilder(document.querySelector('#comparison-chart'),other)
      }
    }else{
      document.querySelector('#other-comparisons').hidden=false;
      document.querySelector('#comparison-chart').innerHTML=displayError(charts.reason)
    }
  }catch(error){app.innerHTML=shell('comparisons',displayError(error))}
}
    """
    "async function compose(){if(!location.hash.slice(1))history.replaceState(history.state,'',location.pathname+location.search+'#runs');"
    "const pieces=routeParts(),current=pieces.join('/');if(pieces[0]==='cluster-templates')return renderClusterTemplates(pieces[1]);"
    "if(current==='hosts')return renderHosts();if(current==='runs')return renderRuns();if(current==='new')return renderN"
    "ew('builder');if(current==='new/yaml')return renderNew('yaml');if(current==='topology')return renderTopology();if(curren"
    "t==='comparisons'||pieces[0]==='comparisons')return renderSavedComparisons();if(pieces[0]==='attempt'&&[4,5].includes(pieces.length))"
    "return renderLocalYdbAttempt(pieces[1],pieces[2],pieces[3],pieces[4]);if(pieces[0]==='run'){"
    "if(pieces.length===3&&pieces[2]==='configuration')return renderRun(pieces[1],'','configuration');if(pieces[2]"
    "==='profile')return renderRun(pieces[1],pieces.slice(3).join('/'));return renderRun(pieces.slice(1).join('/'))}setRoute("
    "'runs')}\n"
    "addEventListener('hashchange',compose);setInterval(refreshActiveBanner,3000);compose();\n"
)


_CSS += cluster_templates_ui.CSS
_JS += cluster_templates_ui.JS


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
        "local_ydb_workloads": web_workload_catalog(),
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


_LOCAL_YDB_SCHEMA_MAX_METRICS = 128
_LOCAL_YDB_EXECUTOR_METRICS = (
    "static_cpu_mean",
    "static_cpu_max",
    "dynamic_cpu_mean",
    "dynamic_cpu_max",
    "cli_cpu_mean",
    "cli_cpu_max",
    "host_cpu_mean",
    "host_cpu_max",
)
_LOCAL_YDB_DERIVED_METRICS = (
    "load",
    "dynamic_nodes",
    "target_cpu_saturated",
    "empty_repetitions",
)
_LOCAL_YDB_CONTROL_METRICS = (
    "repetition",
    "attempt",
    "search_stage",
    "started_at",
    "finished_at",
    "duration_seconds",
    "commands",
    "passed",
    "decision",
    "search_low",
    "search_high",
    "throughput_gain_percent",
)
_LOCAL_YDB_RESERVED_WORKLOAD_METRICS = frozenset(
    _LOCAL_YDB_EXECUTOR_METRICS + _LOCAL_YDB_DERIVED_METRICS + _LOCAL_YDB_CONTROL_METRICS
)


def _legacy_local_ydb_result_schema(workload_type):
    throughput_unit = {
        "kv": "requests/s",
        "stock": "query operations/s",
    }.get(workload_type, "operations/s")
    return {
        "schema_id": "generic-total-v1",
        "metrics": [
            {
                "name": name,
                "unit": throughput_unit if name == "throughput" else unit,
                "repetition_aggregation": aggregation,
                "required": True,
            }
            for name, unit, aggregation in (
                ("transactions", "operations", "median"),
                ("throughput", "operations/s", "median"),
                ("retries", "retries", "median"),
                ("errors", "errors", "sum"),
                ("p50_ms", "ms", "median"),
                ("p95_ms", "ms", "median"),
                ("p99_ms", "ms", "median"),
                ("pmax_ms", "ms", "median"),
            )
        ],
        "slo_metrics": {
            "p50": "p50_ms",
            "p95": "p95_ms",
            "p99": "p99_ms",
            "pmax": "pmax_ms",
        },
        "throughput_unit": throughput_unit,
        "reports_errors": True,
    }


def _project_local_ydb_result_schema(value):
    if not isinstance(value, dict):
        raise BenchmarkError("local YDB workload result schema must be an object")
    schema_id = value.get("schema_id")
    throughput_unit = value.get("throughput_unit")
    reports_errors = value.get("reports_errors")
    raw_metrics = value.get("metrics")
    raw_slo_metrics = value.get("slo_metrics")
    if (
        not isinstance(schema_id, str)
        or re.fullmatch(r"[a-z0-9][a-z0-9._-]*", schema_id) is None
        or len(schema_id) > 256
    ):
        raise BenchmarkError("local YDB workload result schema id is invalid")
    if not isinstance(throughput_unit, str) or not throughput_unit or len(throughput_unit) > 128:
        raise BenchmarkError("local YDB workload throughput unit is invalid")
    if not isinstance(reports_errors, bool):
        raise BenchmarkError("local YDB workload reports_errors flag is invalid")
    if not isinstance(raw_metrics, list) or not raw_metrics or len(raw_metrics) > _LOCAL_YDB_SCHEMA_MAX_METRICS:
        raise BenchmarkError("local YDB workload result metrics are invalid")
    metrics = []
    names = set()
    for item in raw_metrics:
        if not isinstance(item, dict):
            raise BenchmarkError("local YDB workload result metric must be an object")
        name = item.get("name")
        unit = item.get("unit")
        aggregation = item.get("repetition_aggregation")
        required = item.get("required")
        description = item.get("description", "")
        if (
            not isinstance(name, str)
            or re.fullmatch(r"[a-z][a-z0-9_]*", name) is None
            or len(name) > 128
            or name in names
            or name in _LOCAL_YDB_RESERVED_WORKLOAD_METRICS
        ):
            raise BenchmarkError("local YDB workload result metric name is invalid")
        if not isinstance(unit, str) or not unit or len(unit) > 128:
            raise BenchmarkError("local YDB workload result metric unit is invalid")
        if aggregation not in ("median", "sum") or not isinstance(required, bool):
            raise BenchmarkError("local YDB workload result metric contract is invalid")
        if not isinstance(description, str) or len(description) > 4096:
            raise BenchmarkError("local YDB workload result metric description is invalid")
        descriptor = {
            "name": name,
            "unit": unit,
            "repetition_aggregation": aggregation,
            "required": required,
        }
        if description:
            descriptor["description"] = description
        metrics.append(descriptor)
        names.add(name)
    if "throughput" not in names:
        raise BenchmarkError("local YDB workload result schema must declare throughput")
    descriptors = {item["name"]: item for item in metrics}
    throughput = descriptors["throughput"]
    if (
        not throughput["required"]
        or throughput["repetition_aggregation"] != "median"
        or throughput["unit"] != throughput_unit
    ):
        raise BenchmarkError("local YDB workload throughput metric contract is invalid")
    errors = descriptors.get("errors")
    if reports_errors != (errors is not None):
        raise BenchmarkError("local YDB workload reports_errors does not match its metrics")
    if errors is not None and (not errors["required"] or errors["repetition_aggregation"] != "sum"):
        raise BenchmarkError("local YDB workload errors metric contract is invalid")
    if not isinstance(raw_slo_metrics, dict) or len(raw_slo_metrics) > _LOCAL_YDB_SCHEMA_MAX_METRICS:
        raise BenchmarkError("local YDB workload SLO metric mapping is invalid")
    slo_metrics = {}
    for percentile, metric_name in raw_slo_metrics.items():
        if (
            not isinstance(percentile, str)
            or re.fullmatch(r"p(?:\d+(?:\.\d+)?|max)", percentile) is None
            or len(percentile) > 128
            or not isinstance(metric_name, str)
            or metric_name not in names
        ):
            raise BenchmarkError("local YDB workload SLO metric mapping is invalid")
        metric = descriptors[metric_name]
        if not metric["required"] or metric["repetition_aggregation"] != "median" or metric["unit"] != "ms":
            raise BenchmarkError("local YDB workload SLO metric contract is invalid")
        slo_metrics[percentile] = metric_name
    return {
        "schema_id": schema_id,
        "metrics": metrics,
        "slo_metrics": slo_metrics,
        "throughput_unit": throughput_unit,
        "reports_errors": reports_errors,
    }


def _resolved_local_ydb_result_schema(manifest):
    has_persisted_schema = isinstance(manifest, dict) and "workload_result_schema" in manifest
    persisted = manifest.get("workload_result_schema") if has_persisted_schema else None
    parameters = manifest.get("parameters") if isinstance(manifest, dict) else None
    workload = parameters.get("workload") if isinstance(parameters, dict) else None
    workload_type = workload.get("type") if isinstance(workload, dict) else None
    schema = _project_local_ydb_result_schema(
        persisted if has_persisted_schema else _legacy_local_ydb_result_schema(workload_type)
    )
    if (
        workload_type == "stock"
        and schema["schema_id"] == "generic-total-v1"
        and schema["throughput_unit"] == "transactions/s"
    ):
        throughput_unit = "query operations/s"
        schema = {
            **schema,
            "throughput_unit": throughput_unit,
            "metrics": [
                {**metric, "unit": throughput_unit} if metric["name"] == "throughput" else metric
                for metric in schema["metrics"]
            ],
        }
    return schema


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


def _merge_chart_metric_metadata(target, name, metadata):
    current = target.get(name)
    if current is None or current == metadata:
        target[name] = metadata
        return
    if current.get("unit") != metadata.get("unit"):
        target[name] = {
            "unit": "varies",
            "description": "Metric units differ between the selected result schemas.",
            "conflict": True,
        }
        return
    descriptions = {item for item in (current.get("description"), metadata.get("description")) if item}
    target[name] = {
        "unit": current.get("unit", ""),
        "description": next(iter(descriptions)) if len(descriptions) == 1 else "",
        **({"conflict": True} if len(descriptions) > 1 else {}),
    }


def chart_data(output, run_ids, benchmark_filter=None):
    """Read bounded profile summaries into UI-facing affinity series."""
    if not isinstance(run_ids, list) or not run_ids or len(run_ids) > 20:
        raise BenchmarkError("charts require between 1 and 20 run ids")
    if benchmark_filter is not None and benchmark_filter not in BENCHMARKS:
        raise BenchmarkError("unknown chart benchmark: {}".format(benchmark_filter))
    result = []
    result_row_count = 0
    dimensions, metrics, metric_metadata, dimension_metadata = set(), set(), {}, {}
    for run_id in run_ids:
        root = _run_directory(output, run_id)
        pattern = "{}/*/summary.csv".format(benchmark_filter) if benchmark_filter else "*/*/summary.csv"
        for path in sorted(root.glob(pattern)):
            if path.stat().st_size > 16 * 1024 * 1024:
                raise BenchmarkError("summary CSV is too large: {}".format(path.relative_to(root)))
            affinity_cpus = {}
            affinity_cpu_masks = {}
            benchmark_name = path.relative_to(root).parts[0]
            profile_manifest = None
            local_result_schema = None
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
                    profile_manifest = None
            if benchmark_name == "local-ydb" and profile_manifest is not None:
                local_result_schema = _resolved_local_ydb_result_schema(profile_manifest)
            with path.open(newline="", encoding="utf-8") as stream:
                reader = csv.DictReader(stream)
                fields = [name for name in (reader.fieldnames or []) if isinstance(name, str) and name]
                if "affinity_mode" not in fields:
                    continue
                benchmark_definition = BENCHMARKS.get(benchmark_name) if benchmark_name in BENCHMARKS else None
                normalized_repetitions = benchmark_name == "memory-bandwidth-bench"
                has_memory_fairness = False
                prefixes = ("median_", "mean_", "min_", "max_", "sum_")
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
                result_row_count += sum(len(rows) for rows in grouped.values())
                if result_row_count > _CHART_DATA_ROW_LIMIT:
                    raise BenchmarkError("selected chart data has too many rows")
                dimensions.update(dimension_fields)
                metrics.update(metric_fields)
                file_metric_metadata = {}
                if benchmark_definition is not None:
                    for dimension in benchmark_definition.dimensions:
                        dimension_metadata[dimension.name] = {"series": dimension.series}
                    for metric in benchmark_definition.metrics:
                        if normalized_repetitions:
                            file_metric_metadata[metric.name] = {
                                "unit": metric.unit,
                                "description": metric.description,
                            }
                        else:
                            for prefix in ("median_", "min_", "max_"):
                                file_metric_metadata[prefix + metric.name] = {
                                    "unit": metric.unit,
                                    "description": metric.description,
                                }
                    if has_memory_fairness:
                        file_metric_metadata.update(
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
                if local_result_schema is not None:
                    for descriptor in local_result_schema["metrics"]:
                        metadata = {
                            "unit": descriptor["unit"],
                            "description": descriptor.get("description", ""),
                        }
                        prefixes = ["median_", "min_", "max_"]
                        if descriptor["repetition_aggregation"] == "sum":
                            prefixes.append("sum_")
                        for prefix in prefixes:
                            file_metric_metadata[prefix + descriptor["name"]] = metadata
                for name, metadata in file_metric_metadata.items():
                    _merge_chart_metric_metadata(metric_metadata, name, metadata)
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
                    if local_result_schema is not None:
                        series["result_schema_id"] = local_result_schema["schema_id"]
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

    def __init__(
        self, output, executor=None, event_limit=256, tail_limit=65536, perf_available=True, binaries_dir="bin"
    ):
        self.output = Path(output).resolve()
        self.output.mkdir(parents=True, exist_ok=True)
        self.hosts = HostDirectory(self.output)
        self.cluster_templates = ClusterTemplateStore(self.output)
        self.executor = executor or self._unsupported_executor
        self.event_limit, self.tail_limit = event_limit, tail_limit
        self.perf_available = perf_available
        self.binaries_dir = Path(binaries_dir).resolve()
        self._runs, self._lock = {}, threading.RLock()
        self._cpu_sampler = LogicalCpuSampler()
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
            model = {
                "output": str(self.output),
                "benchmarks": benchmark_catalog(),
                "affinity_modes": list(AFFINITY_MODES),
                "background_load_modes": list(BACKGROUND_LOAD_MODES),
                "profiles": [],
            }
        else:
            loaded = self._load(yaml_text, perf)
            model = editor_model(loaded, self.output)
        model["binary_catalog"] = binary_catalog(self.binaries_dir)
        return model

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
        sequence = run["store"].manifest.get("events", 0) + 1
        if sequence > _MAX_SAFE_JSON_INTEGER:
            raise BenchmarkError("event sequence exceeds the JSON safe-integer range")
        event["sequence"] = sequence
        event["at"] = _utc_now()
        if event.get("type") in ("stdout", "stderr"):
            event["data"] = str(event.get("data", ""))[-self.tail_limit :]
        persisted_event = event
        try:
            serialized_event = json.dumps(persisted_event, sort_keys=True, allow_nan=False)
        except ValueError as error:
            raise BenchmarkError("event contains a non-finite JSON number") from error
        serialized_size = len(serialized_event.encode("utf-8")) + 1
        if serialized_size > _EVENT_LOG_RECORD_BYTES:
            persisted_event = {
                "sequence": event["sequence"],
                "at": event["at"],
                "payload_truncated": True,
                "original_size_bytes": serialized_size,
            }
            for name in ("type", "step_id", "state"):
                value = event.get(name)
                if isinstance(value, str):
                    persisted_event[name] = value[:2048]
            fields = event.get("fields")
            if isinstance(fields, dict):
                persisted_fields = {}
                for name in ("reason", "error"):
                    value = fields.get(name)
                    if isinstance(value, str):
                        persisted_fields[name] = value[:2048]
                if persisted_fields:
                    persisted_event["fields"] = persisted_fields
            serialized_event = json.dumps(persisted_event, sort_keys=True, allow_nan=False)
            if len(serialized_event.encode("utf-8")) + 1 > _EVENT_LOG_RECORD_BYTES:
                persisted_event.pop("fields", None)
                for name in ("type", "step_id", "state"):
                    if name in persisted_event:
                        persisted_event[name] = persisted_event[name][:128]
                serialized_event = json.dumps(persisted_event, sort_keys=True, allow_nan=False)
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
        run["events"].append(persisted_event)
        run["store"].manifest["events"] = event["sequence"]
        with (run["root"] / "events.jsonl").open("a", encoding="utf-8") as stream:
            stream.write(serialized_event + "\n")
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

    def activity_status(self):
        with self._lock:
            return {
                "active_run_id": self._active_run_id,
                "queued": sum(run["store"].manifest["state"] == "queued" for run in self._queue),
            }

    def topology(self, mode=None, count=None, excluded=()):
        topology = discover_topology()
        result = {
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
        if mode is not None:
            if mode not in AFFINITY_MODES or type(count) is not int or not 1 <= count <= 65536:
                raise BenchmarkError("Invalid affinity mode or CPU count")
            if len(excluded) > 65536 or any(type(cpu) is not int or not 0 <= cpu <= 1048575 for cpu in excluded):
                raise BenchmarkError("Invalid excluded CPU list")
            placement = (
                plan_affinity(mode, topology, count, excluded_cpus=excluded)
                if excluded
                else plan_affinity(mode, topology, count)
            )
            result["placement"] = {
                "supported": placement.supported,
                "cpus": None if placement.cpus is None else list(placement.cpus),
                "reason": placement.reason,
                "excluded_cpus": sorted(set(excluded)),
            }
        return result

    def cpu_usage(self):
        result = self._cpu_sampler.sample()
        try:
            allowed = os.sched_getaffinity(0)
        except (AttributeError, OSError):
            allowed = result["cpus"]
        return {**result, "cpus": {cpu: value for cpu, value in result["cpus"].items() if cpu in allowed}}

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
        try:
            structured = yaml.load(yaml_text, Loader=yaml.BaseLoader)
            # Reject recursive YAML aliases without making the original YAML unavailable.
            json.dumps(structured)
            if not isinstance(structured, dict):
                structured = None
        except (yaml.YAMLError, TypeError, ValueError, RecursionError):
            structured = None
        return {
            "yaml": yaml_text,
            "structured": structured,
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

    def chart_data(self, run_ids, benchmark_filter=None):
        return chart_data(self.output, run_ids, benchmark_filter)

    def local_ydb_metrics(self, run_id, profile, attempt):
        if attempt != "verification" and not re.fullmatch(r"[1-9][0-9]{0,8}", str(attempt)):
            raise BenchmarkError("attempt must be a positive integer or verification")
        self.local_ydb_profile(run_id, profile)
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
            return {"samples": [], "truncated": False}
        relative = record.get("manifest") or str(Path(record.get("directory", "")) / "run.json")
        unresolved = (root / relative).parent / "ydb-metrics.jsonl"
        candidate = unresolved.resolve()
        if root not in candidate.parents or unresolved.is_symlink():
            raise BenchmarkError("local-ydb metrics escape the run directory")
        try:
            value = read_metrics(candidate, attempt)
            if candidate.is_file():
                value["artifact"] = "/api/runs/{}/artifact/{}".format(
                    quote(run_id, safe=""), quote(str(candidate.relative_to(root)), safe="/")
                )
            return value
        except OSError as error:
            raise BenchmarkError("cannot read local-ydb metrics: {}".format(error)) from error

    def local_ydb_profile(self, run_id, profile):
        root = _run_directory(self.output, run_id)
        manifest = load_manifest(root / "run.json")
        steps = [
            item
            for item in manifest.get("steps", [])
            if item.get("benchmark") == "local-ydb" and item.get("profile") == profile
        ]
        record = next(
            (
                item
                for item in manifest.get("runs", [])
                if item.get("benchmark") == "local-ydb" and item.get("profile") == profile
            ),
            None,
        )

        def unavailable_profile(record_status=None, error=None):
            state_by_status = {
                "completed": "passed",
                "interrupted": "cancelled",
                "pending": "preparing",
                "queued": "preparing",
                "running": "preparing",
            }
            if record_status:
                state = state_by_status.get(record_status, record_status)
                status = record_status
            elif any(item.get("state") == "running" for item in steps):
                state = status = "preparing"
            elif any(item.get("state") == "failed" for item in steps):
                state = status = "failed"
            elif any(item.get("state") == "pending" for item in steps):
                state = status = "preparing"
            elif any(item.get("state") == "cancelled" for item in steps):
                state = status = "cancelled"
            elif steps and all(item.get("state") == "unsupported" for item in steps):
                state = status = "unsupported"
            else:
                state, status = "passed", "completed"

            top_state = manifest.get("state")
            if state == "preparing" and top_state not in ("pending", "queued", "running"):
                state = top_state or "failed"
                status = manifest.get("status") or state

            value = {
                "benchmark": "local-ydb",
                "profile": profile,
                "status": status,
                "state": state,
            }
            step_error = next(
                (item.get("error") or item.get("reason") for item in steps if item.get("error") or item.get("reason")),
                None,
            )
            error = error or step_error or (manifest.get("error") if state not in ("preparing", "passed") else None)
            if error:
                value["error"] = error
            return value

        if record is None:
            if steps:
                return unavailable_profile()
            raise BenchmarkError("local-ydb profile not found: {}".format(profile))
        relative = record.get("manifest") or str(Path(record.get("directory", "")) / "run.json")
        unresolved = root / relative
        candidate = unresolved.resolve()
        if candidate == root or root not in candidate.parents or unresolved.is_symlink():
            raise BenchmarkError("local-ydb profile manifest escapes the run directory")
        if not candidate.is_file():
            return unavailable_profile(record.get("status"), record.get("error"))
        if candidate.stat().st_size > 16 * 1024 * 1024:
            raise BenchmarkError("local-ydb profile manifest is too large")
        value = load_manifest(candidate)
        value["workload_result_schema"] = _resolved_local_ydb_result_schema(value)
        top_state = manifest.get("state")
        if value.get("state") in ("preparing", "running") and top_state not in ("pending", "queued", "running"):
            value["state"] = top_state or "failed"
            value["status"] = manifest.get("status") or value["state"]
            if manifest.get("error"):
                value["error"] = manifest["error"]
        fields = (
            "schema_version",
            "benchmark",
            "profile",
            "status",
            "state",
            "started_at",
            "finished_at",
            "parameters",
            "workload_result_schema",
            "timeout_seconds",
            "role_affinity",
            "tool_revision",
            "binaries",
            "platform",
            "cpu_topology",
            "progress",
            "attempts",
            "searches",
            "verification",
            "result",
            "error",
        )
        return {name: value[name] for name in fields if name in value}

    def local_ydb_activity(self, run_id, profile, after=0):
        if not isinstance(profile, str) or not profile:
            raise BenchmarkError("local-ydb profile is required")
        if isinstance(after, bool) or not isinstance(after, int) or after < 0 or after > _MAX_SAFE_JSON_INTEGER:
            raise BenchmarkError("activity cursor must be a non-negative integer in the JSON safe range")

        root = _run_directory(self.output, run_id)
        manifest = load_manifest(root / "run.json")
        matching_steps = [
            item
            for item in manifest.get("steps", [])
            if item.get("benchmark") == "local-ydb" and item.get("profile") == profile
        ]
        profile_exists = bool(matching_steps) or any(
            item.get("benchmark") == "local-ydb" and item.get("profile") == profile for item in manifest.get("runs", [])
        )
        if not profile_exists:
            raise BenchmarkError("local-ydb profile not found: {}".format(profile))
        step_ids = {item["id"] for item in matching_steps if isinstance(item.get("id"), str) and item["id"]}

        def bounded_scalar(value, limit=2048):
            if isinstance(value, str):
                return value[:limit]
            if value is None or isinstance(value, bool):
                return value
            if isinstance(value, int) and abs(value) <= _MAX_SAFE_JSON_INTEGER:
                return value
            if isinstance(value, float) and math.isfinite(value) and abs(value) <= _MAX_SAFE_JSON_INTEGER:
                return value
            return None

        def project_command(value):
            if not isinstance(value, dict) or not isinstance(value.get("argv"), (list, tuple)):
                return None
            command = {
                "argv": [
                    str(part)[:512]
                    for part in value["argv"][:64]
                    if part is None or isinstance(part, (str, bool, int, float))
                ]
            }
            cpus = value.get("cpu_affinity")
            if isinstance(cpus, (list, tuple)):
                command["cpu_affinity"] = [
                    cpu
                    for cpu in cpus[:4096]
                    if isinstance(cpu, int) and not isinstance(cpu, bool) and 0 <= cpu <= _MAX_SAFE_JSON_INTEGER
                ]
            for name in ("phase", "repetition"):
                projected = bounded_scalar(value.get(name))
                if projected is not None:
                    command[name] = projected
            return command

        def project_progress(value):
            if not isinstance(value, dict):
                return {}
            result = {}
            fields = (
                "phase",
                "phase_started_at",
                "phase_duration_seconds",
                "search_stage",
                "attempt",
                "static_nodes",
                "dynamic_nodes",
                "parameter",
                "load",
                "repetition",
                "repetitions",
                "passed",
                "decision",
                "target_dynamic_nodes",
                "reason",
            )
            for name in fields:
                projected = bounded_scalar(value.get(name))
                if projected is not None:
                    result[name] = projected
            command = project_command(value.get("current_command"))
            if command is not None:
                result["current_command"] = command
            verification = value.get("verification")
            if isinstance(verification, bool):
                result["verification"] = verification
            elif isinstance(verification, dict):
                result["verification"] = {
                    name: projected
                    for name in (
                        "status",
                        "configured_repetitions",
                        "completed_repetitions",
                        "accepted",
                        "evaluation_kind",
                        "decision",
                        "throughput_delta_percent",
                        "saturated_repetitions",
                    )
                    if (projected := bounded_scalar(verification.get(name))) is not None
                }
            return result

        def project_event(event):
            event_type = event.get("type")
            if event_type not in ("step-started", "step-progress", "step-finished"):
                return None
            item = {
                "sequence": event["sequence"],
                "type": event_type,
            }
            at = bounded_scalar(event.get("at"))
            if at is not None:
                item["at"] = at
            if event_type == "step-progress":
                fields = event.get("fields")
                progress = fields.get("progress") if isinstance(fields, dict) else None
                item.update(project_progress(progress))
            elif event_type == "step-finished":
                state = bounded_scalar(event.get("state"))
                if state is not None:
                    item["state"] = state
                fields = event.get("fields")
                if not isinstance(fields, dict):
                    fields = {}
                for name in ("reason", "error"):
                    projected = bounded_scalar(fields.get(name))
                    if projected is not None:
                        item[name] = projected
            return item

        events_path = root / "events.jsonl"

        def event_log_snapshot_size():
            if events_path.is_symlink():
                raise BenchmarkError("run event log must be a regular file")
            try:
                size = events_path.stat().st_size
            except FileNotFoundError:
                return 0
            if not events_path.is_file():
                raise BenchmarkError("run event log must be a regular file")
            return size

        cursor = after
        matched = 0
        activity = deque(maxlen=_LOCAL_YDB_ACTIVITY_LIMIT)
        replay_gap = False

        def consume(event):
            nonlocal cursor, matched
            sequence = event["sequence"]
            if sequence <= after:
                return
            cursor = sequence
            step_id = event.get("step_id")
            if not isinstance(step_id, str) or step_id not in step_ids:
                return
            item = project_event(event)
            if item is not None:
                activity.append(item)
                matched += 1

        with self._lock:
            run = self._runs.get(run_id)
        live_events = None
        if run:
            with run["lock"]:
                last_sequence = run["store"].manifest.get("events", 0)
                if after >= last_sequence:
                    live_events = ()
                elif run["events"] and after >= run["events"][0]["sequence"] - 1:
                    live_events = tuple(dict(event) for event in run["events"] if event["sequence"] > after)
                else:
                    snapshot_size = event_log_snapshot_size()
        else:
            snapshot_size = event_log_snapshot_size()

        if live_events is not None:
            for event in live_events:
                consume(event)
        elif snapshot_size:
            try:
                stream = events_path.open("rb")
            except OSError as error:
                raise BenchmarkError("cannot read run event log") from error
            with stream:
                replay_start = max(
                    0,
                    snapshot_size - _LOCAL_YDB_ACTIVITY_SCAN_BYTES - _EVENT_LOG_RECORD_BYTES,
                )
                if replay_start:
                    stream.seek(replay_start - 1)
                    starts_at_record_boundary = stream.read(1) == b"\n"
                    stream.seek(replay_start)
                    if not starts_at_record_boundary:
                        partial = stream.readline(min(snapshot_size - replay_start, _EVENT_LOG_RECORD_BYTES + 1))
                        if len(partial) > _EVENT_LOG_RECORD_BYTES or not partial.endswith(b"\n"):
                            raise BenchmarkError("run event log contains an oversized or incomplete event")
                remaining = snapshot_size - stream.tell()
                previous_sequence = None
                first_sequence = None
                while remaining:
                    line = stream.readline(min(remaining, _EVENT_LOG_RECORD_BYTES + 1))
                    if not line:
                        raise BenchmarkError("run event log ended before its snapshot boundary")
                    remaining -= len(line)
                    if len(line) > _EVENT_LOG_RECORD_BYTES or not line.endswith(b"\n"):
                        raise BenchmarkError("run event log contains an oversized or incomplete event")
                    try:
                        event = json.loads(line.decode("utf-8"), parse_constant=_non_finite_json_as_null)
                    except (UnicodeDecodeError, ValueError) as error:
                        raise BenchmarkError("run event log contains malformed JSON") from error
                    if not isinstance(event, dict):
                        raise BenchmarkError("run event log event must be an object")
                    sequence = event.get("sequence")
                    if (
                        not isinstance(sequence, int)
                        or isinstance(sequence, bool)
                        or sequence <= 0
                        or (previous_sequence is not None and sequence <= previous_sequence)
                        or sequence > _MAX_SAFE_JSON_INTEGER
                    ):
                        raise BenchmarkError("run event log sequences must be strictly increasing integers")
                    if first_sequence is None:
                        first_sequence = sequence
                    previous_sequence = sequence
                    if not isinstance(event.get("type"), str):
                        raise BenchmarkError("run event log event type must be a string")
                    consume(event)
                if replay_start and first_sequence is None:
                    raise BenchmarkError("run event log contains an oversized or incomplete event")
                replay_gap = bool(replay_start and first_sequence > after + 1)
        bounded = deque()
        response_bytes = 0
        for item in reversed(activity):
            encoded_size = len(json.dumps(item, allow_nan=False, separators=(",", ":")).encode("utf-8")) + 1
            if encoded_size > _LOCAL_YDB_ACTIVITY_RESPONSE_BYTES:
                continue
            if response_bytes + encoded_size > _LOCAL_YDB_ACTIVITY_RESPONSE_BYTES:
                break
            bounded.appendleft(item)
            response_bytes += encoded_size
        return {
            "events": list(bounded),
            "after": cursor,
            "truncated": replay_gap or matched > len(bounded),
        }

    def local_ydb_comparison(self, run_ids):
        if not isinstance(run_ids, list) or not run_ids or len(run_ids) > 20:
            raise BenchmarkError("local YDB comparisons require between 1 and 20 run ids")
        if any(not isinstance(run_id, str) or not run_id for run_id in run_ids):
            raise BenchmarkError("local YDB comparison run ids must be non-empty strings")
        if len(set(run_ids)) != len(run_ids):
            raise BenchmarkError("local YDB comparison run ids must be unique")

        def project(value, fields):
            if not isinstance(value, dict):
                return {}
            return {name: value[name] for name in fields if name in value}

        def project_parameters(value):
            if not isinstance(value, dict):
                return {}
            workload = project(value.get("workload"), ("type", "operation", "options"))
            geometry = project(
                value.get("geometry"),
                (
                    "preset",
                    "static_nodes",
                    "dynamic_nodes",
                    "max_dynamic_nodes",
                    "disk_size_gb",
                    "storage_groups",
                ),
            )
            client = project(value.get("client"), ("threads",))
            actor_system = project(
                value.get("actor_system"),
                ("use_shared_threads", "use_united_pool", "use_ring_queue", "static_nodes", "dynamic_nodes"),
            )
            load = project(value.get("load"), ("parameter", "allow_errors", "values", "search", "objective"))
            measurement = project(
                value.get("measurement"),
                ("warmup", "duration", "repetitions", "verification_repetitions"),
            )
            affinity = project(value.get("affinity"), ("ydb_cli", "static_nodes", "dynamic_nodes"))
            return {
                name: item
                for name, item in (
                    ("workload", workload),
                    ("geometry", geometry),
                    ("actor_system", actor_system),
                    ("client", client),
                    ("load", load),
                    ("measurement", measurement),
                    ("affinity", affinity),
                )
                if item
            }

        def project_binaries(value):
            if not isinstance(value, dict):
                return {}
            return {
                name: item
                for name in ("ydbd", "ydb_cli")
                if (item := project(value.get(name), ("name", "sha256", "size")))
            }

        def project_platform(value):
            result = project(value, ("architecture", "cpu_count", "cpu_model", "physical_memory_bytes"))
            uname = project(
                value.get("uname") if isinstance(value, dict) else None,
                ("machine", "node", "release", "system", "version"),
            )
            if uname:
                result["uname"] = uname
            return result

        def project_topology(value):
            return project(
                value,
                (
                    "version",
                    "allowed_cpus",
                    "numa_nodes",
                    "chiplets",
                    "physical_cores",
                    "smt_siblings",
                    "hierarchy_reasons",
                ),
            )

        model = self.model()
        entries = []
        response_size = 0
        for run_id in run_ids:
            record = model.get(run_id)
            if record is None:
                raise BenchmarkError("run not found: {}".format(run_id))
            profiles = sorted(
                {
                    str(item["profile"])
                    for item in record.get("runs", []) + record.get("steps", [])
                    if item.get("benchmark") == "local-ydb" and item.get("profile") is not None
                }
            )
            for profile in profiles:
                value = self.local_ydb_profile(run_id, profile)
                result_schema = value.get("workload_result_schema")
                compact_fields = (
                    "status",
                    "state",
                    "started_at",
                    "finished_at",
                    "tool_revision",
                    "error",
                )
                entry = {
                    "run": run_id,
                    "profile": profile,
                    **{name: value[name] for name in compact_fields if name in value},
                }
                if isinstance(result_schema, dict):
                    entry["workload_result_schema"] = result_schema
                projections = {
                    "parameters": project_parameters(value.get("parameters")),
                    "role_affinity": project(value.get("role_affinity"), ("ydb_cli", "static_nodes", "dynamic_nodes")),
                    "binaries": project_binaries(value.get("binaries")),
                    "platform": project_platform(value.get("platform")),
                    "cpu_topology": project_topology(value.get("cpu_topology")),
                    "verification": project(
                        value.get("verification"),
                        (
                            "status",
                            "state",
                            "started_at",
                            "finished_at",
                            "load",
                            "dynamic_nodes",
                            "cluster",
                            "adaptive",
                            "configured_repetitions",
                            "completed_repetitions",
                            "accepted",
                            "evaluation_kind",
                            "decision",
                            "outcome",
                            "reason",
                            "duration_seconds",
                            "throughput_delta_percent",
                            "saturated_repetitions",
                            "error",
                        ),
                    ),
                }
                entry.update({name: item for name, item in projections.items() if item})
                result = value.get("result")
                if isinstance(result, dict):
                    result_fields = (
                        "outcome",
                        "objective",
                        "parameter",
                        "allow_errors",
                        "search_stage",
                        "dynamic_nodes",
                        "selected_load",
                        "passing_load",
                        "failing_load",
                        "stop_reason",
                        "verification_mode",
                        "metrics_source",
                        "verification_repetitions",
                        "holdout_accepted",
                    )
                    compact_result = {name: result[name] for name in result_fields if name in result}
                    metric_names = set(_LOCAL_YDB_EXECUTOR_METRICS + _LOCAL_YDB_DERIVED_METRICS)
                    if isinstance(result_schema, dict):
                        metric_names.update(
                            item["name"] for item in result_schema.get("metrics", ()) if isinstance(item, dict)
                        )
                    else:
                        metric_names.update(metric.name for metric in BENCHMARKS["local-ydb"].metrics)
                    metrics = result.get("selected_metrics")
                    if isinstance(metrics, dict):
                        compact_result["selected_metrics"] = {
                            name: metrics[name] for name in metric_names if name in metrics
                        }
                    verified_metrics = result.get("verified_metrics")
                    if isinstance(verified_metrics, dict):
                        compact_result["verified_metrics"] = {
                            name: verified_metrics[name] for name in metric_names if name in verified_metrics
                        }
                    entry["result"] = compact_result
                try:
                    response_size += len(json.dumps(entry, allow_nan=False, separators=(",", ":")).encode("utf-8")) + 1
                except (TypeError, ValueError) as error:
                    raise BenchmarkError("local YDB comparison contains invalid JSON data") from error
                if response_size > 4 * 1024 * 1024:
                    raise BenchmarkError("local YDB comparison response is too large")
                entries.append(entry)
                if len(entries) > 100:
                    raise BenchmarkError("local YDB comparison contains more than 100 profiles")
        return {"entries": entries}

    def saved_comparisons(self):
        path = self.output / ".saved-comparisons.json"
        with self._lock:
            if not path.exists():
                return []
            try:
                records = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, ValueError) as error:
                raise BenchmarkError("Cannot read saved comparisons") from error
            if not isinstance(records, list):
                raise BenchmarkError("Invalid saved comparisons")
            return records

    def run_list(self, filters):
        fields = (
            'id',
            'status',
            'state',
            'source',
            'queued_at',
            'started_at',
            'finished_at',
            'duration_seconds',
            'profiles',
            'repetitions',
            'perf',
            'config_path',
            'output_directory',
            'benchmarks',
            'profile_names',
            'current_run_id',
            'queue_position',
        )
        return [{key: item[key] for key in fields} for item in self.filtered_model(filters)]

    def save_comparison(self, value):
        if not isinstance(value, dict):
            raise BenchmarkError("Comparison must be an object")
        name, profiles, baseline = value.get("name"), value.get("profiles"), value.get("baseline")
        if not isinstance(name, str) or not name.strip() or len(name) > 200:
            raise BenchmarkError("Comparison name must contain 1 to 200 characters")
        if not isinstance(profiles, list) or not 1 <= len(profiles) <= 100:
            raise BenchmarkError("Select between 1 and 100 profiles")
        if any(
            not isinstance(item, list)
            or len(item) != 2
            or any(not isinstance(part, str) or not part or len(part) > 500 for part in item)
            for item in profiles
        ):
            raise BenchmarkError("Profiles must be run/profile pairs")
        if len({tuple(item) for item in profiles}) != len(profiles) or len({item[0] for item in profiles}) > 20:
            raise BenchmarkError("Select unique profiles from at most 20 runs")
        if baseline not in profiles:
            raise BenchmarkError("Baseline must be one of the selected profiles")
        with self._lock:
            records = self.saved_comparisons()
            previous = next((item for item in records if item["id"] == value.get("id")), None)
            if value.get("id") and previous is None:
                raise BenchmarkError("Comparison no longer exists")
            if previous and value.get("revision") != previous["revision"]:
                raise BenchmarkError("Comparison changed elsewhere; reload before saving")
            record = {
                "id": previous["id"] if previous else uuid.uuid4().hex,
                "name": name.strip(),
                "profiles": profiles,
                "baseline": baseline,
                "created_at": previous["created_at"] if previous else datetime.now(timezone.utc).isoformat(),
                "revision": previous["revision"] + 1 if previous else 1,
            }
            records = [item for item in records if item["id"] != record["id"]]
            records.insert(0, record)
            atomic_write_json(self.output / ".saved-comparisons.json", records)
            return record

    def delete_comparison(self, value):
        if not isinstance(value, dict):
            raise BenchmarkError("Comparison must be an object")
        with self._lock:
            records = self.saved_comparisons()
            record = next((item for item in records if item["id"] == value.get("id")), None)
            if record is None or record["revision"] != value.get("revision"):
                raise BenchmarkError("Comparison changed or no longer exists; reload first")
            atomic_write_json(
                self.output / ".saved-comparisons.json", [item for item in records if item["id"] != record["id"]]
            )
        return {"deleted": record["id"]}

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
        path = _run_directory(self.output, run_id) / "events.jsonl"
        with self._lock:
            run = self._runs.get(run_id)
        if run:
            with run["lock"]:
                live_events = [dict(event) for event in run["events"]]
                last_sequence = run["store"].manifest.get("events", 0)
                if after >= last_sequence:
                    return []
                if live_events and after >= live_events[0]["sequence"] - 1:
                    return [event for event in live_events if event["sequence"] > after]
                if path.is_symlink():
                    raise BenchmarkError("run event log must be a regular file")
                try:
                    snapshot_size = path.stat().st_size
                except FileNotFoundError:
                    return [event for event in live_events if event["sequence"] > after]
            # Capture the byte boundary while emissions are locked, then read
            # outside the lock.  Later appends belong to the next poll and
            # cannot expose a partially written JSON line in this replay.
            with path.open("rb") as stream:
                payload = stream.read(snapshot_size)
            return [
                event
                for line in payload.decode("utf-8").splitlines()
                if (event := json.loads(line, parse_constant=_non_finite_json_as_null))["sequence"] > after
            ]
        if path.is_symlink():
            raise BenchmarkError("run event log must be a regular file")
        if not path.is_file():
            return []
        snapshot_size = path.stat().st_size
        with path.open("rb") as stream:
            payload = stream.read(snapshot_size)
        events = []
        for line in payload.decode("utf-8").splitlines():
            event = json.loads(line, parse_constant=_non_finite_json_as_null)
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
                profile_binaries = load_profile_binaries(configuration, resource_loader, work, binaries)
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
            try:
                body = json.dumps(value, allow_nan=False).encode()
            except ValueError:
                status = 500
                body = b'{"error": "response contains a non-finite JSON number"}'
            self._send(status, "application/json", body)

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
            if self.path.startswith("/peer/"):
                if not service.hosts.authorized(self.headers.get("Authorization")):
                    return self._json(401, {"error": "peer authentication required"})
                peer_path = self.path[len("/peer") :]
                if not allowed_path(peer_path):
                    return self._json(403, {"error": "peer route not allowed"})
                self.path = peer_path
            parsed = urlparse(self.path)
            path = parsed.path
            if path.startswith('/api/federation/'):
                try:
                    federation = Federation(service)
                    query = parse_qs(parsed.query)
                    if path == '/api/federation/runs':
                        filters = {
                            name: values[-1]
                            for name, values in query.items()
                            if name in ('status', 'benchmark', 'profile', 'source', 'since', 'until')
                        }
                        return self._json(200, federation.runs(filters, query.get('host', [None])[-1]))
                    if path == '/api/federation/comparisons':
                        return self._json(200, federation.comparisons())
                    if path == '/api/federation/profiles':
                        return self._json(200, federation.profiles(query.get('run', [])))
                    return self._json(404, {'error': 'not found'})
                except BenchmarkError as error:
                    return self._json(400, {'error': str(error)})
            if path == "/api/host-info":
                return self._json(200, service.hosts.identity(self.server.server_port))
            if path == "/api/hosts":
                return self._json(
                    200, {"local": service.hosts.identity(self.server.server_port), "hosts": service.hosts.list()}
                )
            if path.startswith("/api/hosts/"):
                try:
                    host_id, suffix = path[len("/api/hosts/") :].split("/", 1)
                    target = "/" + suffix + ("?" + parsed.query if parsed.query else "")
                    if host_id == service.hosts.id:
                        if not allowed_path(target):
                            raise BenchmarkError('route is not allowed')
                        self.path = target
                        return self.do_GET()
                    record = service.hosts.get(host_id)
                    with open_peer(record, target) as response:
                        self.send_response(response.status)
                        self.send_header(
                            "Content-Type", response.headers.get("Content-Type", "application/octet-stream")
                        )
                        self.send_header("Content-Security-Policy", _CSP)
                        self.send_header("X-Content-Type-Options", "nosniff")
                        self.send_header("Connection", "close")
                        self.end_headers()
                        self.close_connection = True
                        try:
                            _copy_stream(response, self.wfile)
                        except OSError:
                            pass
                        return
                except (BenchmarkError, ValueError) as error:
                    return self._json(502, {"error": str(error)})
            if path == "/":
                return self._send(200, "text/html; charset=utf-8", _HTML.encode())
            if path == "/app.css":
                return self._send(200, "text/css; charset=utf-8", _CSS.encode())
            if path == "/app.js":
                return self._send(200, "application/javascript; charset=utf-8", _JS.encode())
            if path == "/api/settings":
                return self._json(200, service.settings())
            if path == "/api/activity-status":
                return self._json(200, service.activity_status())
            if path == "/api/benchmarks":
                return self._json(200, benchmark_catalog())
            if path == "/api/cpu-usage":
                return self._json(200, service.cpu_usage())
            if path == "/api/system-topology":
                try:
                    query = parse_qs(parsed.query)
                    mode = query.get("mode", [None])[-1]
                    count = int(query.get("cpus", ["0"])[-1]) if mode is not None else None
                    excluded = tuple(int(cpu) for cpu in query.get("exclude", [""])[-1].split(",") if cpu)
                    return self._json(200, service.topology(mode, count, excluded))
                except (BenchmarkError, ValueError) as error:
                    return self._json(400, {"error": str(error)})
            if path == "/api/cluster-templates":
                try:
                    return self._json(200, service.cluster_templates.list())
                except BenchmarkError as error:
                    return self._json(400, {"error": str(error)})
            if path == "/api/runs":
                filters = {
                    name: values[-1]
                    for name, values in parse_qs(parsed.query).items()
                    if name in ("status", "benchmark", "profile", "source", "since", "until")
                }
                return self._json(200, service.run_list(filters))
            if path == "/api/saved-comparisons":
                return self._json(200, service.saved_comparisons())
            if path == "/api/comparisons":
                return self._json(200, service.comparisons())
            if path == "/api/chart-data":
                query = parse_qs(parsed.query)
                try:
                    value = service.chart_data(query.get("run", []), query.get("benchmark", [None])[-1])
                except BenchmarkError as error:
                    return self._json(400, {"error": str(error)})
                return self._json(200, value)
            if path == "/api/local-ydb-comparison":
                try:
                    value = service.local_ydb_comparison(parse_qs(parsed.query).get("run", []))
                except BenchmarkError as error:
                    return self._json(400, {"error": str(error)})
                return self._json(200, value)
            if path.startswith("/api/runs/") and path.endswith("/local-ydb-activity"):
                run_id = unquote(path[len("/api/runs/") : -len("/local-ydb-activity")])
                query = parse_qs(parsed.query)
                profile = query.get("profile", [""])[-1]
                try:
                    after = int(query.get("after", [0])[-1])
                except ValueError:
                    return self._json(400, {"error": "activity cursor must be a non-negative integer"})
                try:
                    value = service.local_ydb_activity(run_id, profile, after)
                except BenchmarkError as error:
                    return self._json(400, {"error": str(error)})
                return self._json(200, value)
            if path.startswith("/api/runs/") and path.endswith("/local-ydb-metrics"):
                run_id = unquote(path[len("/api/runs/") : -len("/local-ydb-metrics")])
                query = parse_qs(parsed.query)
                try:
                    value = service.local_ydb_metrics(
                        run_id, query.get("profile", [""])[-1], query.get("attempt", [""])[-1]
                    )
                except BenchmarkError as error:
                    return self._json(400, {"error": str(error)})
                return self._json(200, value)
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
                try:
                    body = (json.dumps(manifest, indent=2, sort_keys=True, allow_nan=False) + "\n").encode()
                except ValueError:
                    return self._json(500, {"error": "run manifest contains a non-finite JSON number"})
                return self._attachment(
                    "application/json",
                    "{}-run.json".format(run_id.replace("/", "-")),
                    body,
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
                try:
                    payload = (
                        b"".join(
                            ("id: %s\ndata: %s\n\n" % (event["sequence"], json.dumps(event, allow_nan=False))).encode()
                            for event in events
                        )
                        or b": connected\n\n"
                    )
                except ValueError:
                    return self._json(500, {"error": "event log contains a non-finite JSON number"})
                return self._send(200, "text/event-stream", payload)
            if path.startswith("/api/runs/"):
                item = service.detail(unquote(path[len("/api/runs/") :]))
                return self._json(200 if item else 404, item or {"error": "run not found"})
            return self._json(404, {"error": "not found"})

        def do_POST(self):
            path = urlparse(self.path).path
            try:
                if path.startswith('/peer/api/'):
                    if not service.hosts.authorized(self.headers.get('Authorization')):
                        return self._json(401, {'error': 'peer authentication required'})
                    if (
                        self.headers.get('Origin')
                        or self.headers.get('Content-Type', '').split(';')[0] != 'application/json'
                    ):
                        return self._json(403, {'error': 'server-to-server JSON request required'})
                    target = path[len('/peer') :]
                    if not allowed_post_path(target):
                        return self._json(403, {'error': 'peer operation not allowed'})
                    self.path = target
                    return self.do_POST()
                if path.startswith('/api/hosts/'):
                    parts = path[len('/api/hosts/') :].split('/', 1)
                    if len(parts) == 2 and allowed_post_path('/' + parts[1]):
                        origin = self.headers.get('Origin')
                        if self.headers.get('Content-Type', '').split(';')[0] != 'application/json' or (
                            origin and urlparse(origin).netloc != self.headers.get('Host')
                        ):
                            return self._json(403, {'error': 'same-origin JSON request required'})
                        if parts[0] == service.hosts.id:
                            self.path = '/' + parts[1]
                            return self.do_POST()
                        options = self._json_body() if parts[1].endswith('/cancel') else self._options()
                        if not isinstance(options, dict):
                            raise BenchmarkError('request must be an object')
                        status, content_type, body = request_peer(service.hosts.get(parts[0]), '/' + parts[1], options)
                        return self._send(status, content_type, body, {'Cache-Control': 'no-store'})
                local_prefix = '/api/hosts/' + service.hosts.id
                if path.startswith(local_prefix + '/api/runs/'):
                    self.path = self.path[len(local_prefix) :]
                    return self.do_POST()
                if path in ('/api/saved-comparisons', '/api/saved-comparisons/delete'):
                    options = self._json_body()
                    if isinstance(options, dict) and isinstance(options.get('id'), str):
                        owner, item_id = split_reference(options['id'], service.hosts.id)
                        if owner != service.hosts.id:
                            return self._json(403, {'error': 'edit this comparison on its owning host'})
                        options['id'] = item_id
                    if path.endswith('/delete'):
                        return self._json(200, service.delete_comparison(options))
                    return self._json(201, service.save_comparison(options))
                if path in ('/peer/cluster/snapshot', '/peer/cluster/validate', '/peer/cluster/merge'):
                    if not service.hosts.authorized(self.headers.get('Authorization')):
                        return self._json(401, {'error': 'peer authentication required'})
                    if (
                        self.headers.get('Origin')
                        or self.headers.get('Content-Type', '').split(';')[0] != 'application/json'
                    ):
                        return self._json(403, {'error': 'server-to-server JSON request required'})
                    options = self._json_body()
                    if not isinstance(options, dict):
                        raise BenchmarkError('expected cluster object')
                    if path.endswith('/snapshot'):
                        return self._send(
                            200,
                            'application/json',
                            json.dumps(service.hosts.snapshot()).encode(),
                            {'Cache-Control': 'no-store'},
                        )
                    if path.endswith('/validate'):
                        service.hosts.validate_merge(options.get('members'))
                        return self._json(200, {'valid': True})
                    return self._json(200, service.hosts.merge(options.get('members')))
                if path in ("/api/hosts/add", "/api/hosts/remove", "/api/hosts/token"):
                    origin = self.headers.get("Origin")
                    if self.headers.get("Content-Type", "").split(";")[0] != "application/json" or (
                        origin and urlparse(origin).netloc != self.headers.get("Host")
                    ):
                        return self._json(403, {"error": "same-origin JSON request required"})
                    options = self._json_body()
                    if path == "/api/hosts/token":
                        return self._send(
                            200,
                            "application/json",
                            json.dumps({"token": service.hosts.token}).encode(),
                            {"Cache-Control": "no-store"},
                        )
                    if path.endswith("/add"):
                        return self._json(201, service.hosts.join(options))
                    if not isinstance(options, dict) or not isinstance(options.get("id"), str):
                        raise BenchmarkError("host id is required")
                    return self._json(200, service.hosts.remove(options["id"]))
                if path.startswith(("/peer/", "/api/hosts/")):
                    return self._json(403, {"error": "remote operation is not allowed"})
                if path == "/api/import":
                    return self._json(201, import_archive(service.output, self._raw_body()))
                if path in ("/api/cluster-templates", "/api/cluster-templates/delete"):
                    origin = self.headers.get("Origin")
                    if self.headers.get("Content-Type", "").split(";")[0] != "application/json" or (
                        origin and urlparse(origin).netloc != self.headers.get("Host")
                    ):
                        return self._json(403, {"error": "same-origin JSON request required"})
                    value = self._json_body()
                    if path.endswith("/delete"):
                        return self._json(200, service.cluster_templates.delete(value))
                    host_ids = {service.hosts.id} | {host["id"] for host in service.hosts.list()}
                    return self._json(201, service.cluster_templates.save(value, host_ids))
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
                if path == "/api/saved-comparisons":
                    return self._json(200, service.save_comparison(self._json_body()))
                if path == "/api/saved-comparisons/delete":
                    return self._json(200, service.delete_comparison(self._json_body()))
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


def make_server(listen, port, output, allow_remote=False, executor=None, perf_available=True, binaries_dir="bin"):
    if not _is_loopback(listen) and not allow_remote:
        raise BenchmarkError("non-loopback --listen requires --allow-remote")
    server_class = _IPv6ThreadingHTTPServer if ":" in listen else _RunServiceHTTPServer
    service = RunService(output, executor=executor, perf_available=perf_available, binaries_dir=binaries_dir)
    server = server_class((listen, port), _handler(service))
    peer_host = socket.getfqdn() if listen in ('0.0.0.0', '::') else listen
    service.hosts.port = server.server_port
    service.hosts.endpoint = 'http://{}:{}'.format(
        '[' + peer_host + ']' if ':' in peer_host else peer_host, server.server_port
    )
    server.service = service
    return server


def serve(
    listen, port, output, no_open=False, allow_remote=False, executor=None, perf_available=True, binaries_dir="bin"
):
    server = make_server(listen, port, output, allow_remote, executor, perf_available, binaries_dir)
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
