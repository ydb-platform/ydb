"""HTML dashboard rendering for tests resource dashboard.

Contains build_html_dashboard which takes a pre-computed payload and writes
the self-contained HTML file with Plotly.js charts, CPU suggestions table
and ya.make update script generator.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Optional

try:
    from .dashboard_html_payload import build_dashboard_payload
except ImportError:
    from dashboard_html_payload import build_dashboard_payload  # type: ignore[no-redef]


def build_html_dashboard(
    suite_filter: Optional[str],
    runs: list[dict[str, Any]],
    stats: dict[str, Any],
    out_html: Path,
    top_n: int,
    max_points: int = 1000,
    by_chunk: bool = False,
    cpu_suggestions: Optional[list[dict[str, Any]]] = None,
    run_config: Optional[dict[str, Any]] = None,
    suite_event_times: Optional[dict[str, dict[str, list[float]]]] = None,
    resources_overlay: Optional[dict[str, Any]] = None,
) -> None:
    payload = build_dashboard_payload(
        suite_filter=suite_filter,
        runs=runs,
        stats=stats,
        top_n=top_n,
        max_points=max_points,
        by_chunk=by_chunk,
        cpu_suggestions=cpu_suggestions,
        run_config=run_config,
        suite_event_times=suite_event_times,
        resources_overlay=resources_overlay,
    )

    html = f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>Chunk Resource Dashboard</title>
  <script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
  <style>
    body {{ font-family: Arial, sans-serif; margin: 16px; }}
    .toolbar {{ position: sticky; top: 0; z-index: 20; display: flex; gap: 8px; align-items: center; margin: 8px 0 10px 0; padding: 8px 0; background: rgba(255,255,255,0.96); backdrop-filter: blur(2px); }}
    .toolbar input[type="text"] {{ min-width: 340px; padding: 6px 8px; border: 1px solid #d0d7de; border-radius: 6px; }}
    .toolbar button {{ padding: 6px 10px; border: 1px solid #d0d7de; border-radius: 6px; background: #f6f8fa; cursor: pointer; }}
    .cpu-help {{ display: none; margin-top: 8px; padding: 8px 10px; border-radius: 8px; border: 1px solid #d0d7de; background: #f6f8fa; font-size: 12px; line-height: 1.45; }}
    .cpu-help ol, .cpu-help ul {{ margin: 6px 0 6px 18px; padding: 0; }}
    .metrics-help {{ margin: 8px 0 12px 0; }}
    .metrics-help .box {{ background: #f6f8fa; border: 1px solid #d0d7de; border-radius: 8px; padding: 10px 12px; font-size: 12px; line-height: 1.45; }}
    .metrics-help ul {{ margin: 6px 0 0 18px; padding: 0; }}
    .tabs {{ display: flex; gap: 8px; margin: 8px 0 12px 0; }}
    .tabbtn {{ padding: 6px 10px; border: 1px solid #d0d7de; border-radius: 6px; background: #f6f8fa; cursor: pointer; }}
    .tabbtn.active {{ background: #e7f3ff; border-color: #8cc8ff; }}
    .tab {{ display: none; }}
    .tab.active {{ display: block; }}
    .row {{ display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }}
    .row1 {{ display: grid; grid-template-columns: 1fr; gap: 16px; }}
    .chart {{ width: 100%; height: 420px; min-width: 0; }}
    .wide {{ width: 100%; height: 520px; }}
    .hoverbox {{ background: #f6f8fa; border: 1px solid #d0d7de; border-radius: 8px; padding: 8px 10px; margin: 6px 0 16px 0; max-height: 220px; overflow: auto; font-size: 12px; }}
    .clickbox {{ background: #fff; border: 1px solid #d0d7de; border-radius: 8px; padding: 8px 10px; margin: 6px 0 20px 0; max-height: 320px; overflow: auto; font-size: 12px; }}
    .clickbox table {{ width: 100%; border-collapse: collapse; }}
    .clickbox th, .clickbox td {{ border-bottom: 1px solid #eee; padding: 4px 6px; text-align: left; vertical-align: top; }}
    .clickbox th {{ position: sticky; top: 0; background: #fafafa; }}
    .clickbox thead tr:first-child th {{ background: #f2f4f7; }}
    .clickbox thead tr:nth-child(2) th {{ background: #f8fafc; }}
    /* Visual separators between logical column groups in CPU suggestions. */
    #cpuSuggestionsInner td.group-end, #cpuSuggestionsInner th.group-end {{
      position: relative;
      border-right: 1px solid #c9d1db;
    }}
    #cpuSuggestionsInner td.group-end::after, #cpuSuggestionsInner th.group-end::after {{
      content: "";
      position: absolute;
      top: -1px;
      right: -10px;
      width: 10px;
      height: calc(100% + 2px);
      pointer-events: none;
      background: linear-gradient(to right, rgba(144, 158, 176, 0.28), rgba(144, 158, 176, 0.0));
    }}
    pre {{ background: #f6f8fa; padding: 12px; border-radius: 8px; }}
    .time-link {{ cursor: pointer; color: #2563eb; text-decoration: underline dotted; white-space: nowrap; }}
    .time-link:hover {{ color: #e53e3e; }}
    .monitor-box {{
      display: none;
      margin: -4px 0 12px 0;
      padding: 8px 10px;
      border: 1px solid #d0d7de;
      border-radius: 8px;
      background: #f8fafc;
      font-size: 13px;
      line-height: 1.35;
    }}
    .monitor-box a {{
      color: #2563eb;
      text-decoration: none;
      font-weight: 600;
    }}
    .monitor-box a:hover {{ text-decoration: underline; }}
    .monitor-url {{
      margin-left: 8px;
      color: #6b7280;
      font-size: 12px;
      font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", monospace;
    }}
    .btn-primary {{
      padding: 7px 12px;
      border: 1px solid #1d4ed8;
      border-radius: 7px;
      background: #2563eb;
      color: #fff;
      font-weight: 600;
      cursor: pointer;
    }}
    .btn-primary:hover {{ background: #1d4ed8; }}
    .btn-primary:active {{ background: #1e40af; }}
  </style>
</head>
<body>
  <div id="dashboardHeader" style="margin-bottom:16px;padding:12px 16px;background:linear-gradient(135deg,#f8fafc 0%,#f1f5f9 100%);border-radius:8px;border:1px solid #e2e8f0;">
    <div id="headerRow1" style="display:flex;flex-wrap:wrap;align-items:center;gap:12px 20px;font-size:14px;line-height:1.5;">
      <span id="headerPr" style="display:none;"></span>
      <span id="headerBranch" style="display:none;"></span>
      <span id="headerCommit" style="display:none;font-family:ui-monospace,monospace;font-size:13px;"></span>
      <span style="flex:1;min-width:80px;"></span>
      <span id="headerLinks" style="display:flex;align-items:center;gap:8px;flex-wrap:wrap;"></span>
    </div>
    <div style="margin-top:8px;font-size:13px;color:#64748b;">
      Suite filter: {suite_filter or 'ALL SUITES'}
    </div>
    <div id="overlayStatus" style="margin-top:4px;font-size:12px;"></div>
  </div>
  <details style="margin: 8px 0 12px 0;">
    <summary><b>Run stats</b></summary>
    <pre id="stats"></pre>
  </details>
  <details style="margin: 8px 0 12px 0;">
    <summary><b>Config</b></summary>
    <pre id="runConfig"></pre>
  </details>
  <div id="monitoringLinkWrap" class="monitor-box">
    <b>Runner monitoring:</b>
    <a id="monitoringLink" href="#" target="_blank" rel="noopener noreferrer">Open dashboard</a>
    <span id="monitoringLinkMeta" class="monitor-url"></span>
  </div>
  <div id="cpuSuggestionsSection" style="display: none; margin: 16px 0;">
    <h3 style="display:flex;align-items:center;gap:8px;margin-bottom:6px;">
      <span>CPU suggestions (recommended_cpu for runner)</span>
      <button id="cpuHelpToggle" type="button" title="Show recommendation logic" style="padding:0 8px;min-width:auto;">?</button>
    </h3>
    <div id="cpuHelpText" class="cpu-help">
      <ol>
        <li><b>cores_est</b> for a chunk is calculated as <code>cpu_sec_report / duration_sec</code>.</li>
        <li>Per suite, recommendations use the distribution of chunk <b>cores_est</b> values.</li>
        <li><b>recommended_cpu</b> is p95 rounded to runner tiers: <code>1/2/4/8/16</code>.</li>
        <li>If there is at least one <b>test timeout</b>, <b>recommended_cpu</b> is increased by one tier (then MEDIUM cap is applied).</li>
        <li>When CLI flag <code>--maximize-reqs-for-timeout-tests</code> is enabled, suites with test timeouts use size max: <b>SMALL=1, MEDIUM=4, LARGE=all</b>.</li>
        <li><b>cpu_action</b> compares recommended cpu to <code>ya_cpu</code> from <code>ya.make</code>.</li>
      </ol>
      <ul>
        <li><b>status chunks</b>: counters from report rows where <code>chunk=true</code>.</li>
        <li><b>status tests</b>: counters from report rows where <code>chunk=false</code> (regular tests).</li>
        <li><b>timeouts</b>: <code>error_type == TIMEOUT</code> (fallback: status contains <code>timeout</code>).</li>
        <li><b>muted</b>: <code>muted/is_muted</code> or status <code>MUTE</code>.</li>
        <li><b>fails_total</b>: <code>errors + timeouts</code>.</li>
      </ul>
    </div>
    <p id="syntheticNote" style="display: none; font-size: 12px; color: #b8860b; margin: 4px 0;"></p>
    <div style="display:flex;align-items:center;gap:8px;margin:6px 0 8px 0;">
      <button id="generateCpuScriptBtn" class="btn-primary" type="button">Generate CPU update script</button>
      <span id="generateCpuScriptHint" style="font-size:12px;color:#586069;"></span>
    </div>
    <div id="cpuSuggestionsTable" class="clickbox"></div>
  </div>
  <div class="toolbar">
    <label for="suiteSearch"><b>Search suite:</b></label>
    <input id="suiteSearch" type="text" placeholder="e.g. ydb/core/tx/schemeshard/ut_cdc_stream_reboots" />
    <button onclick="clearSuiteSearch()">Clear</button>
    <label for="tzSelect"><b>Timezone:</b></label>
    <select id="tzSelect" onchange="applyTimezoneToAllCharts()" style="padding:6px 8px;border:1px solid #d0d7de;border-radius:6px;">
      <option value="Europe/Moscow">MSK</option>
      <option value="UTC">UTC</option>
      <option value="local" selected>Local</option>
    </select>
    <button onclick="clearAllMarkers()" title="Remove all suite markers from charts">Clear markers</button>
    <span style="display:flex;align-items:center;gap:6px;font-size:12px;border:1px solid #d0d7de;border-radius:6px;padding:3px 6px;background:#fff;">
      <b>Marker types:</b>
      <label><input type="checkbox" id="mkStart" checked onchange="applyMarkersToCharts()">start</label>
      <label><input type="checkbox" id="mkEnd" checked onchange="applyMarkersToCharts()">end</label>
      <label><input type="checkbox" id="mkPar" onchange="applyMarkersToCharts()">par</label>
      <label><input type="checkbox" id="mkOthers" onchange="applyMarkersToCharts()">others</label>
      <label><input type="checkbox" id="mkCpu" checked onchange="applyMarkersToCharts()">cpu</label>
      <label><input type="checkbox" id="mkRam" checked onchange="applyMarkersToCharts()">ram</label>
      <label><input type="checkbox" id="mkError" onchange="applyMarkersToCharts()">errors</label>
      <label><input type="checkbox" id="mkTimeout" onchange="applyMarkersToCharts()">timeouts</label>
    </span>
    <span id="markersLegend" style="display:none;font-size:11px;line-height:1.5;border:1px solid #d0d7de;border-radius:6px;padding:4px 8px;background:#fafafa;">
      <b>Markers:</b>
      <span style="margin-left:6px;">&#9135;&#9135; start</span>
      <span style="margin-left:6px;">&#8209;&#8209;&#8209; end</span>
      <span style="margin-left:6px;">&#183;&#183;&#183; par/others peak</span>
      <span style="margin-left:6px;">&#8209;&#183;&#8209; CPU/RAM peak</span>
      <span style="margin-left:6px;color:#b00020;">&#9473;&#9473; errors</span>
      <span style="margin-left:6px;color:#e65100;">&#9473;&#9473; timeouts</span>
      &nbsp;|&nbsp;
      <span id="markersLegendSuites"></span>
      <span id="markersLegendStats" style="margin-left:6px;color:#586069;"></span>
    </span>
    <span id="syntheticCheckboxWrap" style="margin-left: 16px;" title="">
      <label style="display:flex;align-items:center;gap:6px;">
        <input type="checkbox" id="includeSyntheticCb" checked />
        <span>Include estimated (from ya.make) CPU/RAM</span>
      </label>
    </span>
    <span id="layerTogglesWrap" style="display:none;margin-left:16px;font-size:12px;border:1px solid #d0d7de;border-radius:8px;padding:6px 10px;background:#fff;">
      <span style="display:inline-flex;align-items:center;gap:10px;flex-wrap:wrap;">
        <b style="margin-right:2px;">Layers:</b>
        <label style="display:inline-flex;align-items:center;gap:5px;white-space:nowrap;"><input type="checkbox" id="layerBySuite" checked onchange="applyLayerToggles()">Metrics by suite</label>
        <label style="display:inline-flex;align-items:center;gap:5px;white-space:nowrap;"><input type="checkbox" id="layerSystemCpu" checked onchange="applyLayerToggles()">System CPU</label>
        <label style="display:inline-flex;align-items:center;gap:5px;white-space:nowrap;"><input type="checkbox" id="layerSystemRam" checked onchange="applyLayerToggles()">System RAM</label>
      </span>
    </span>
  </div>
  <details class="metrics-help">
    <summary><b>How CPU/RAM chart metrics are calculated</b></summary>
    <div class="box">
      <ul>
        <li><b>By suites (stacked):</b> Sum of <i>peak</i> RAM/CPU per active chunk at each time. Can exceed actual system RAM because peaks occurred at different moments.</li>
        <li><b>Total (monitor, red line):</b> Absolute system CPU (cores) and RAM (GB) from <code>/proc</code>.</li>
        <li><b>Chunk duration (seconds):</b> <code>evlog_dur_sec = (end_us - start_us) / 1e6</code> from evlog B/E events.</li>
        <li><b>CPU time per chunk (report):</b> <code>cpu_sec_report = ru_utime + ru_stime</code> from report metrics. If value looks like microseconds (&gt;1000), it is divided by <code>1e6</code>.</li>
        <li><b>CPU shown on charts (cores_est):</b> <code>cores_est = cpu_sec_report / evlog_dur_sec</code>. Chart value at time <code>t</code> is the sum of active chunks at <code>t</code>.</li>
        <li><b>RAM per chunk (report):</b> <code>ram_kb_report = ru_maxrss</code>, fallback <code>ru_rss / 1024</code>.</li>
        <li><b>RAM shown on charts:</b> <code>ram_gb = ram_kb_report / (1024 * 1024)</code>. Chart value at time <code>t</code> is the sum of active chunks at <code>t</code>.</li>
        <li><b>Estimated mode (from ya.make):</b> if report metrics are missing and checkbox is enabled, script uses <code>REQUIREMENTS(cpu:X ram:Y)</code>: <code>cpu_sec_report = X * evlog_dur_sec</code>, <code>ram_kb_report = Y * 1024 * 1024</code>.</li>
      </ul>
    </div>
  </details>
  <div id="activeChunks" class="wide"></div>
  <div class="tabs" id="tabsBar" style="display: none;">
    <button id="tabBtnChunk" class="tabbtn active" onclick="showTab('chunkTab')">By Suite+Chunk</button>
    <button id="tabBtnSuite" class="tabbtn" onclick="showTab('suiteTab')">By Suite</button>
  </div>
  <div id="chunkTab" class="tab active" style="display: none;">
    <div id="activeLayerChunk" class="wide"></div>
    <div id="activeHoverChunk" class="hoverbox">Hover active-layer chart to see sorted contributors</div>
    <div id="activeClickChunk" class="clickbox">Click active-layer chart to pin a time and show sorted table</div>
    <div id="cpuLayer" class="wide"></div>
    <div id="cpuHover" class="hoverbox">Hover CPU chart to see sorted contributors</div>
    <div id="cpuClick" class="clickbox">Click CPU chart to pin a time and show sorted table</div>
    <div id="ramLayer" class="wide"></div>
    <div id="ramHover" class="hoverbox">Hover RAM chart to see sorted contributors</div>
    <div id="ramClick" class="clickbox">Click RAM chart to pin a time and show sorted table</div>
    <div id="diskLayer" class="wide" style="display:none;"></div>
    <div class="row">
      <div id="cpuPie" class="chart"></div>
      <div id="ramPie" class="chart"></div>
    </div>
    <div class="row1">
      <div id="activePie" class="chart"></div>
    </div>
  </div>
  <div id="suiteTab" class="tab">
    <div id="activeLayerSuite" class="wide"></div>
    <div id="activeHoverSuite" class="hoverbox">Hover active-layer chart to see sorted contributors</div>
    <div id="activeClickSuite" class="clickbox">Click active-layer chart to pin a time and show sorted table</div>
    <div id="cpuLayerSuite" class="wide"></div>
    <div id="cpuHoverSuite" class="hoverbox">Hover CPU chart to see sorted contributors</div>
    <div id="cpuClickSuite" class="clickbox">Click CPU chart to pin a time and show sorted table</div>
    <div id="ramLayerSuite" class="wide"></div>
    <div id="ramHoverSuite" class="hoverbox">Hover RAM chart to see sorted contributors</div>
    <div id="ramClickSuite" class="clickbox">Click RAM chart to pin a time and show sorted table</div>
    <div id="diskLayerSuite" class="wide" style="display:none;"></div>
    <div class="row">
      <div id="cpuPieSuite" class="chart"></div>
      <div id="ramPieSuite" class="chart"></div>
    </div>
    <div class="row1">
      <div id="activePieSuite" class="chart"></div>
    </div>
  </div>
  <script>
    function showTab(id) {{
      const tabs = ['chunkTab', 'suiteTab'];
      tabs.forEach(t => {{
        document.getElementById(t).classList.toggle('active', t === id);
      }});
      document.getElementById('tabBtnChunk').classList.toggle('active', id === 'chunkTab');
      document.getElementById('tabBtnSuite').classList.toggle('active', id === 'suiteTab');
      // Hidden-tab Plotly charts need explicit resize after becoming visible.
      setTimeout(() => {{
        const ids = id === 'suiteTab'
          ? ['activeLayerSuite', 'cpuLayerSuite', 'ramLayerSuite', 'diskLayerSuite', 'cpuPieSuite', 'ramPieSuite', 'activePieSuite', 'activeChunks']
          : ['activeLayerChunk', 'cpuLayer', 'ramLayer', 'diskLayer', 'cpuPie', 'ramPie', 'activePie', 'activeChunks'];
        ids.forEach(cid => {{
          const el = document.getElementById(cid);
          if (el && window.Plotly) {{
            Plotly.Plots.resize(el);
          }}
        }});
        // Re-apply markers after resize (charts lose shapes on hidden→visible transition).
        applyMarkersToCharts();
      }}, 0);
    }}

    const data = {json.dumps(payload, ensure_ascii=False)};
    const UTC_OFFSET_SEC = Number(data.utc_offset_sec || 0);
    const _fmtCache = {{}};
    function _selectedTimeZone() {{
      const sel = document.getElementById('tzSelect');
      const v = sel ? String(sel.value || 'Europe/Moscow') : 'Europe/Moscow';
      return v === 'local' ? null : v;
    }}
    function _selectedTimeZoneLabel() {{
      const sel = document.getElementById('tzSelect');
      const v = sel ? String(sel.value || 'Europe/Moscow') : 'Europe/Moscow';
      if (v === 'Europe/Moscow') return 'MSK';
      if (v === 'UTC') return 'UTC';
      return 'Local';
    }}
    function _fmtKey(tz, withDate) {{
      return String(tz || 'local') + '|' + (withDate ? 'd' : 't');
    }}
    function _getFormatter(withDate) {{
      const tz = _selectedTimeZone();
      const key = _fmtKey(tz, withDate);
      if (_fmtCache[key]) return _fmtCache[key];
      const base = {{
        year: 'numeric', month: '2-digit', day: '2-digit',
        hour: '2-digit', minute: '2-digit', second: '2-digit',
        hour12: false,
      }};
      if (!withDate) {{
        delete base.year; delete base.month; delete base.day;
      }}
      const opts = tz ? Object.assign(base, {{ timeZone: tz }}) : base;
      _fmtCache[key] = new Intl.DateTimeFormat('sv-SE', opts);
      return _fmtCache[key];
    }}
    function _formatTick(ms) {{
      return _getFormatter(false).format(new Date(Number(ms)));
    }}
    function _buildTickVals(minMs, maxMs, n=7) {{
      if (!Number.isFinite(minMs) || !Number.isFinite(maxMs)) return [];
      if (maxMs <= minMs) return [minMs];
      const out = [];
      const cnt = Math.max(2, n);
      for (let i = 0; i < cnt; i += 1) {{
        out.push(minMs + ((maxMs - minMs) * i) / (cnt - 1));
      }}
      return out;
    }}
    function xToDisplay(xSec) {{
      const x = Number(xSec);
      if (!Number.isFinite(x)) return xSec;
      return new Date((x + UTC_OFFSET_SEC) * 1000.0).toISOString();
    }}
    function xFromDisplay(xDisp) {{
      let ms = Number(xDisp);
      if (!Number.isFinite(ms)) ms = Date.parse(String(xDisp));
      if (!Number.isFinite(ms)) return xDisp;
      return (ms / 1000.0) - UTC_OFFSET_SEC;
    }}
    function axisTitle() {{
      return 'time (' + _selectedTimeZoneLabel() + ')';
    }}
    function formatTimeLabel(xDisp) {{
      let ms = Number(xDisp);
      if (!Number.isFinite(ms)) ms = Date.parse(String(xDisp));
      if (!Number.isFinite(ms)) return String(xDisp);
      return _getFormatter(true).format(new Date(ms));
    }}
    function _toMs(v) {{
      let n = Number(v);
      if (!Number.isFinite(n)) n = Date.parse(String(v));
      return Number.isFinite(n) ? n : null;
    }}
    function axisLayout(xsDisp) {{
      return {{
        title: axisTitle(),
        type: 'date',
        hoverformat: '%Y-%m-%d %H:%M:%S',
        // Hide Plotly default unified X title like "1.772e12".
        unifiedhovertitle: {{ text: '' }},
      }};
    }}
    function applyTimezoneToAllCharts() {{
      const ids = ['activeChunks', 'activeLayerChunk', 'activeLayerSuite', 'cpuLayer', 'ramLayer', 'cpuLayerSuite', 'ramLayerSuite', 'diskLayer', 'diskLayerSuite'];
      for (const id of ids) {{
        const el = document.getElementById(id);
        if (!el || !el.data || !el.layout || !window.Plotly) continue;
        const xs = [];
        for (const tr of el.data) {{
          if (tr && Array.isArray(tr.x)) {{
            for (const v of tr.x) {{
              const n = _toMs(v);
              if (n != null) xs.push(n);
            }}
          }}
        }}
        if (xs.length === 0) continue;
        const ax = axisLayout(xs);
        Plotly.relayout(el, {{
          'xaxis.title': ax.title,
          'xaxis.type': 'date',
          'xaxis.tickmode': 'auto',
          'xaxis.hoverformat': '%Y-%m-%d %H:%M:%S',
          'xaxis.unifiedhovertitle.text': '',
          'hoverdistance': -1,
          'spikedistance': -1,
        }});
      }}
      for (const id of ids) {{
        const el = document.getElementById(id);
        if (el && el.data && window.Plotly) Plotly.Plots.resize(el);
      }}
      applyMarkersToCharts();
      if (data.resources_overlay) {{
        setTimeout(() => {{
          for (const id of ['cpuLayer', 'ramLayer', 'cpuLayerSuite', 'ramLayerSuite']) {{
            const el = document.getElementById(id);
            if (!el || !el.data || !window.Plotly) continue;
            const indices = [];
            const customdatas = [];
            el.data.forEach((tr, i) => {{
              if (tr && tr.customdata && (tr.name === 'CPU total (monitor)' || tr.name === 'RAM total (monitor)') && Array.isArray(tr.x)) {{
                indices.push(i);
                customdatas.push((tr.x || []).map(v => formatTimeLabel(Number(v))));
              }}
            }});
            if (indices.length) Plotly.restyle(el, {{ customdata: customdatas }}, indices);
          }}
        }}, 0);
      }}
    }}
    document.getElementById('stats').textContent = JSON.stringify(data.stats, null, 2);
    document.getElementById('runConfig').textContent = JSON.stringify(data.run_config || {{}}, null, 2);
    function escapeHtml(s) {{
      if (s == null || s === '') return '';
      const div = document.createElement('div');
      div.textContent = String(s);
      return div.innerHTML;
    }}
    const cfg = data.run_config || {{}};
    const pr = cfg.pr != null && String(cfg.pr).trim() !== '' ? String(cfg.pr) : null;
    const branch = cfg.branch != null && String(cfg.branch).trim() !== '' ? String(cfg.branch).trim() : null;
    const commit = cfg.commit != null && String(cfg.commit).trim() !== '' ? String(cfg.commit).trim() : null;
    const artifactsUrl = (cfg.artifacts_url || '').trim().replace(/\/$/, '');
    const tryLinks = Array.isArray(cfg.try_links) ? cfg.try_links : [];
    const hasCiData = pr || branch || commit || artifactsUrl;
    const repo = escapeHtml((cfg.repo || 'ydb-platform/ydb').trim());
    if (pr) {{
      const el = document.getElementById('headerPr');
      const prSafe = escapeHtml(pr);
      el.textContent = 'PR #' + pr;
      el.style.display = '';
      el.innerHTML = '<a href="https://github.com/' + repo + '/pull/' + prSafe + '" target="_blank" rel="noopener" style="color:#2563eb;font-weight:600;text-decoration:none;">PR #' + prSafe + '</a>';
    }}
    if (branch) {{
      const el = document.getElementById('headerBranch');
      const branchSafe = escapeHtml(branch);
      el.textContent = '→ ' + branch;
      el.style.display = '';
      el.innerHTML = '<span style="color:#475569;">→</span> <b>' + branchSafe + '</b>';
    }}
    if (commit) {{
      const el = document.getElementById('headerCommit');
      const commitSafe = escapeHtml(commit);
      const short = String(commit).slice(0, 7);
      const shortSafe = escapeHtml(short);
      el.innerHTML = '<a href="https://github.com/' + repo + '/commit/' + commitSafe + '" target="_blank" rel="noopener" style="color:#0f172a;text-decoration:none;">' + shortSafe + '</a>';
      el.style.display = '';
    }}
    const linksEl = document.getElementById('headerLinks');
    if (artifactsUrl) {{
      const a = document.createElement('a');
      a.href = artifactsUrl + '/index.html';
      a.target = '_blank';
      a.rel = 'noopener';
      a.textContent = 'Artifacts';
      a.style.cssText = 'color:#2563eb;font-size:13px;text-decoration:none;';
      linksEl.appendChild(a);
      if (tryLinks.length) linksEl.appendChild(document.createTextNode(' · '));
    }}
    if (artifactsUrl && tryLinks.length) {{
      tryLinks.forEach((path, i) => {{
        if (i > 0) linksEl.appendChild(document.createTextNode('\u00a0'));
        const a = document.createElement('a');
        a.href = artifactsUrl + '/' + path;
        a.target = '_blank';
        a.rel = 'noopener';
        a.textContent = 'try ' + (i + 1);
        a.style.cssText = 'color:#2563eb;font-size:13px;text-decoration:none;';
        linksEl.appendChild(a);
      }});
    }}
    document.getElementById('headerRow1').style.display = hasCiData ? 'flex' : 'none';
    const overlayStatusEl = document.getElementById('overlayStatus');
    if (overlayStatusEl) {{
      const ro = data.resources_overlay;
      const cfg = data.run_config || {{}};
      const san = String(cfg.sanitizer || '').trim().toLowerCase();
      const buildType = san === 'address'
        ? 'release-asan'
        : san === 'thread'
          ? 'release-tsan'
          : san === 'memory'
            ? 'release-msan'
            : 'relwithdebinfo';
      overlayStatusEl.textContent = ro
        ? 'Monitor: CPU/RAM total (red), Disk I/O — shown · Build type: ' + buildType
        : 'Monitor: not available · Build type: ' + buildType;
      overlayStatusEl.style.color = ro ? '#16a34a' : '#94a3b8';
    }}
    function setupMonitoringLink() {{
      const cfg = data.run_config || {{}};
      const runner = String(cfg.runner || '').trim();
      const wrap = document.getElementById('monitoringLinkWrap');
      const link = document.getElementById('monitoringLink');
      const meta = document.getElementById('monitoringLinkMeta');
      if (!wrap || !link || !runner) return;
      const xSources = [
        data.xs_active,
        data.xs_active_suite,
        data.xs_cpu_suite,
        data.xs_ram_suite,
        data.xs_active_chunk,
        data.xs_cpu,
        data.xs_ram,
      ];
      let minSec = Number.POSITIVE_INFINITY;
      let maxSec = Number.NEGATIVE_INFINITY;
      xSources.forEach(arr => {{
        if (!Array.isArray(arr)) return;
        arr.forEach(v => {{
          const n = Number(v);
          if (!Number.isFinite(n)) return;
          if (n < minSec) minSec = n;
          if (n > maxSec) maxSec = n;
        }});
      }});
      if (!Number.isFinite(minSec) || !Number.isFinite(maxSec)) return;
      const fromMs = Math.floor((minSec + UTC_OFFSET_SEC) * 1000.0);
      const toMs = Math.ceil((maxSec + UTC_OFFSET_SEC) * 1000.0);
      const qs = new URLSearchParams();
      qs.set('from', String(fromMs));
      qs.set('to', String(toMs));
      qs.set('refresh', 'off');
      qs.set('p[host]', runner);
      const url = 'https://monitoring.yandex.cloud/folders/b1grf3mpoatgflnlavjd/dashboards/runner-summary?' + qs.toString();
      link.href = url;
      link.textContent = 'Open dashboard';
      if (meta) {{
        meta.textContent = runner + '  |  ' + new Date(fromMs).toISOString() + ' .. ' + new Date(toMs).toISOString();
      }}
      wrap.style.display = '';
    }}
    setupMonitoringLink();

    // Marker state lives at top scope so all functions can access it.
    const _markerChartIds = ['activeChunks', 'activeLayerChunk', 'activeLayerSuite', 'cpuLayer', 'ramLayer', 'cpuLayerSuite', 'ramLayerSuite', 'diskLayer', 'diskLayerSuite'];
    // Each entry: {{ shapes: [...], annotations: [...] }} by suite_path.
    const _suiteMarkerData = {{}};

    if (data.by_chunk) {{
      document.getElementById('tabsBar').style.display = 'flex';
      document.getElementById('chunkTab').style.display = 'block';
      document.getElementById('suiteTab').style.display = 'none';
    }} else {{
      document.getElementById('suiteTab').classList.add('active');
      document.getElementById('suiteTab').style.display = 'block';
    }}

    if (data.cpu_suggestions && data.cpu_suggestions.length > 0) {{
      document.getElementById('cpuSuggestionsSection').style.display = 'block';
      const nSyn = (data.stats && data.stats.runs_with_synthetic_metrics) ? Number(data.stats.runs_with_synthetic_metrics) : 0;
      const synEl = document.getElementById('syntheticNote');
      if (nSyn > 0 && synEl) {{
        synEl.style.display = 'block';
        synEl.textContent = 'Part of CPU/RAM is estimated from ya.make REQUIREMENTS for ' + nSyn + ' runs without report metrics.';
      }}

      // Default: show suites with the highest non-chunk failures first.
      let suggestionsSortCol = 17;  // test_fails_total
      let suggestionsSortAsc = false;

      function getSuggestionsForScript() {{
        const q = (document.getElementById('suiteSearch')?.value || '').trim().toLowerCase();
        const visible = data.cpu_suggestions.filter(s => !q || String(s.suite_path || '').toLowerCase().includes(q));
        const hasCpuReqValue = (v) => {{
          if (typeof v === 'number') return v > 0;
          const t = String(v ?? '').trim().toLowerCase();
          return t !== '' && t !== '0';
        }};
        return visible
          .filter(s => ['set', 'raise', 'lower'].includes(String(s.cpu_action || 'ok')) && hasCpuReqValue(s.recommended_cpu))
          .map(s => ({{
            suite_path: String(s.suite_path || ''),
            recommended_cpu: s.recommended_cpu,
            current_ya_cpu: s.ya_cpu_cores == null ? null : Number(s.ya_cpu_cores),
            action: String(s.cpu_action || 'ok'),
          }}));
      }}

      function buildCpuUpdateScript(items) {{
        const payloadJson = JSON.stringify(items, null, 2);
        const nowIso = new Date().toISOString();
        return [
          '#!/usr/bin/env python3',
          '# Apply cpu REQUIREMENTS updates in ya.make files.',
          '# Generated by tests_resource_dashboard HTML.',
          '# Usage: python3 apply_cpu_requirements.py --repo-root /path/to/ydb [--dry-run] [--mode all|only-raise|only-lower|only-set]',
          '',
          'from __future__ import annotations',
          '',
          'import argparse',
          'import ast',
          'import json',
          'import re',
          'from pathlib import Path',
          '',
          'GENERATED_AT = ' + JSON.stringify(nowIso),
          'UPDATES = json.loads(' + JSON.stringify(payloadJson) + ')',
          '',
          "PART_SUFFIX_RE = re.compile(r'/part\\\\d+$')",
          "RE_REQ_LINE = re.compile(r'^(\\\\s*REQUIREMENTS\\\\s*\\\\()(.*?)(\\\\)\\\\s*)$')",
          "RE_CPU = re.compile(r'\\\\bcpu\\\\s*:\\\\s*([^\\\\s)]+(?:\\\\([^)]*\\\\))?)', re.IGNORECASE)",
          "RE_IF = re.compile(r'^\\\\s*IF\\\\s*\\\\((.*)\\\\)\\\\s*$')",
          "RE_ELSE = re.compile(r'^\\\\s*ELSE\\\\s*\\\\(\\\\s*\\\\)\\\\s*$')",
          "RE_ENDIF = re.compile(r'^\\\\s*ENDIF\\\\s*\\\\(\\\\s*\\\\)\\\\s*$')",
          "ACTION_BY_MODE = dict()",
          "ACTION_BY_MODE['all'] = set(['raise', 'lower', 'set'])",
          "ACTION_BY_MODE['only-raise'] = set(['raise'])",
          "ACTION_BY_MODE['only-lower'] = set(['lower'])",
          "ACTION_BY_MODE['only-set'] = set(['set'])",
          '',
          'def normalize_suite_path(path: str) -> str:',
          "    return PART_SUFFIX_RE.sub('', path or '')",
          '',
          'def normalize_cpu_req(value: object) -> str:',
          "    s = str(value).strip()",
          "    if s.lower() == 'all':",
          "        return 'all'",
          '    try:',
          '        return str(int(float(s)))',
          '    except Exception:',
          "        return s or '1'",
          '',
          'def update_requirements_line(line: str, cpu: str) -> str:',
          '    m = RE_REQ_LINE.match(line)',
          '    if not m:',
          '        return line',
          '    prefix, body, suffix = m.group(1), m.group(2), m.group(3)',
          '    if RE_CPU.search(body):',
          "        body = RE_CPU.sub('cpu:' + str(cpu), body, count=1)",
          '    else:',
          "        body = (body.strip() + ' ' if body.strip() else '') + 'cpu:' + str(cpu)",
          '    return prefix + body + suffix',
          '',
          'def _safe_eval_bool(expr: str) -> bool:',
          '    tree = ast.parse(expr, mode="eval")',
          '    return _eval_ast_bool(tree.body)',
          '',
          'def _eval_ast_bool(node) -> bool:',
          '    if isinstance(node, ast.Constant):',
          '        return bool(node.value)',
          '    if isinstance(node, ast.BoolOp):',
          '        if isinstance(node.op, ast.And):',
          '            return all(_eval_ast_bool(v) for v in node.values)',
          '        if isinstance(node.op, ast.Or):',
          '            return any(_eval_ast_bool(v) for v in node.values)',
          '    if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.Not):',
          '        return not _eval_ast_bool(node.operand)',
          '    raise ValueError("Unsupported AST node: " + str(type(node)))',
          '',
          'def _eval_condition_default(cond: str) -> bool:',
          "    expr = (cond or '').strip()",
          '    has_san = False',
          "    expr = re.sub(r'\\\\bOS_WINDOWS\\\\b', 'False', expr)",
          "    expr = re.sub(r'\\\\bOS_LINUX\\\\b', 'True', expr)",
          "    expr = re.sub(r'\\\\bOS_DARWIN\\\\b', 'False', expr)",
          "    expr = re.sub(r'\\\\bSANITIZER_TYPE\\\\b', 'True' if has_san else 'False', expr)",
          "    expr = re.sub(r'\\\\bWITH_VALGRIND\\\\b', 'False', expr)",
          "    expr = re.sub(r'\\\\bOR\\\\b', 'or', expr)",
          "    expr = re.sub(r'\\\\bAND\\\\b', 'and', expr)",
          "    expr = re.sub(r'\\\\bNOT\\\\b', 'not', expr)",
          '    try:',
          '        return _safe_eval_bool(expr)',
          '    except Exception:',
          "        return False",
          '',
          'def _find_default_requirements_line(lines: list[str]) -> int | None:',
          '    stack: list[tuple[bool, bool, bool, bool]] = []',
          '    current_active = True',
          '    for i, raw in enumerate(lines):',
          "        line = raw.strip()",
          '        m_if = RE_IF.match(line)',
          '        if m_if:',
          '            parent_active = current_active',
          '            if_res = _eval_condition_default(m_if.group(1))',
          '            current_active = parent_active and if_res',
          '            stack.append((parent_active, if_res, current_active, False))',
          '            continue',
          '        if RE_ELSE.match(line):',
          '            if not stack:',
          '                continue',
          '            parent_active, if_res, prev_cur, seen_else = stack.pop()',
          '            if seen_else:',
          '                stack.append((parent_active, if_res, prev_cur, seen_else))',
          '                continue',
          '            current_active = parent_active and (not if_res)',
          '            stack.append((parent_active, if_res, current_active, True))',
          '            continue',
          '        if RE_ENDIF.match(line):',
          '            if stack:',
          '                stack.pop()',
          '            current_active = stack[-1][2] if stack else True',
          '            continue',
          '        if current_active and RE_REQ_LINE.match(raw):',
          '            return i',
          '    return None',
          '',
          'def _find_default_insert_index(lines: list[str]) -> int:',
          '    stack: list[tuple[bool, bool, bool, bool]] = []',
          '    current_active = True',
          '    last_active_size_idx: int | None = None',
          '    end_idx: int | None = None',
          '    for i, raw in enumerate(lines):',
          "        line = raw.strip()",
          "        if line == 'END()' and end_idx is None:",
          '            end_idx = i',
          '        m_if = RE_IF.match(line)',
          '        if m_if:',
          '            parent_active = current_active',
          '            if_res = _eval_condition_default(m_if.group(1))',
          '            current_active = parent_active and if_res',
          '            stack.append((parent_active, if_res, current_active, False))',
          '            continue',
          '        if RE_ELSE.match(line):',
          '            if not stack:',
          '                continue',
          '            parent_active, if_res, prev_cur, seen_else = stack.pop()',
          '            if seen_else:',
          '                stack.append((parent_active, if_res, prev_cur, seen_else))',
          '                continue',
          '            current_active = parent_active and (not if_res)',
          '            stack.append((parent_active, if_res, current_active, True))',
          '            continue',
          '        if RE_ENDIF.match(line):',
          '            if stack:',
          '                stack.pop()',
          '            current_active = stack[-1][2] if stack else True',
          '            continue',
          "        if current_active and line.startswith('SIZE('):",
          '            last_active_size_idx = i',
          '    if last_active_size_idx is not None:',
          '        return last_active_size_idx + 1',
          '    if end_idx is not None:',
          '        return end_idx',
          '    return len(lines)',
          '',
          'def _line_indent(raw: str) -> str:',
          '    return raw[:len(raw) - len(raw.lstrip())]',
          '',
          'def _choose_insert_indent(lines: list[str], insert_idx: int) -> str:',
          '    candidates: list[str] = []',
          '    if 0 <= insert_idx - 1 < len(lines):',
          '        candidates.append(lines[insert_idx - 1])',
          '    if 0 <= insert_idx < len(lines):',
          '        candidates.append(lines[insert_idx])',
          '    for raw in candidates:',
          '        stripped = raw.strip()',
          '        if not stripped:',
          '            continue',
          "        if stripped in {'ELSE()', 'ENDIF()', 'END()'}:",
          '            continue',
          '        if RE_IF.match(stripped) or RE_ELSE.match(stripped) or RE_ENDIF.match(stripped):',
          '            continue',
          '        return _line_indent(raw)',
          '    for raw in candidates:',
          '        if raw.strip():',
          '            return _line_indent(raw)',
          "    return ''",
          '',
          'def _has_module_block(lines: list[str]) -> bool:',
          "    return any(raw.strip() == 'END()' for raw in lines)",
          '',
          'def apply_one(repo_root: Path, suite_path: str, cpu: str, dry_run: bool) -> tuple[str, str]:',
          '    suite = normalize_suite_path(suite_path)',
          "    ya_make = repo_root / suite / 'ya.make'",
          '    if not ya_make.exists():',
          "        return suite, 'missing ya.make'",
          '',
          "    text = ya_make.read_text(encoding='utf-8', errors='replace')",
          '    lines = text.splitlines()',
          '',
          '    changed = False',
          '    req_idx = _find_default_requirements_line(lines)',
          '',
          '    if req_idx is not None:',
          '        new_line = update_requirements_line(lines[req_idx], cpu)',
          '        if new_line != lines[req_idx]:',
          '            lines[req_idx] = new_line',
          '            changed = True',
          '    else:',
          '        if not _has_module_block(lines):',
          "            return suite, 'skip (no module block)'",
          '        insert_idx = _find_default_insert_index(lines)',
          '        if insert_idx < 0 or insert_idx > len(lines):',
          '            insert_idx = len(lines)',
          '        indent = _choose_insert_indent(lines, insert_idx)',
          "        insert_line = indent + 'REQUIREMENTS(cpu:' + cpu + ')'",
          '        lines.insert(insert_idx, insert_line)',
          '        changed = True',
          '',
          '    if not changed:',
          "        return suite, 'no change'",
          '',
          '    if not dry_run:',
          "        ya_make.write_text('\\\\n'.join(lines) + '\\\\n', encoding='utf-8')",
          "    return suite, 'updated' if not dry_run else 'would update'",
          '',
          'def _action_allowed(action: str, mode: str) -> bool:',
          "    allowed = ACTION_BY_MODE.get(mode, ACTION_BY_MODE['all'])",
          "    return (action or '').lower() in allowed",
          '',
          'def main() -> None:',
          "    p = argparse.ArgumentParser(description='Apply generated cpu REQUIREMENTS updates to ya.make files')",
          "    p.add_argument('--repo-root', type=Path, required=True, help='Repository root path')",
          "    p.add_argument('--dry-run', action='store_true', help='Print planned changes without writing files')",
          "    p.add_argument('--mode', choices=['all', 'only-raise', 'only-lower', 'only-set'], default='all', help='Filter by cpu_action: all / only-raise / only-lower / only-set')",
          '    args = p.parse_args()',
          '',
          '    repo_root = args.repo_root.resolve()',
          '    if not repo_root.exists() or not repo_root.is_dir():',
          "        raise SystemExit('Invalid --repo-root: ' + str(repo_root))",
          '',
          '    seen = set()',
          '    updates = []',
          '    for row in UPDATES:',
          "        suite = normalize_suite_path(str(row.get('suite_path', '')))",
          "        cpu = normalize_cpu_req(row.get('recommended_cpu', 1))",
          "        action = str(row.get('action', '') or '').lower()",
          '        if not _action_allowed(action, args.mode):',
          '            continue',
          '        key = (suite, cpu)',
          '        if not suite or key in seen:',
          '            continue',
          '        seen.add(key)',
          '        updates.append((suite, cpu, action))',
          '',
          "    print('Generated at: ' + str(GENERATED_AT))",
          "    print('Mode: ' + str(args.mode))",
          "    print('Total suites to update: ' + str(len(updates)))",
          '    changed = 0',
          '    for suite, cpu, action in updates:',
          '        suite_out, status = apply_one(repo_root, suite, cpu, args.dry_run)',
          "        print('- ' + str(suite_out) + ': action=' + str(action) + ' cpu:' + str(cpu) + ' -> ' + str(status))",
          "        if status == 'updated' or status == 'would update':",
          '            changed += 1',
          "    print('Completed. Changed entries: ' + str(changed))",
          '',
          "if __name__ == '__main__':",
          '    main()',
          '',
        ].join('\\n');
      }}

      function downloadCpuUpdateScript() {{
        const items = getSuggestionsForScript();
        const hintEl = document.getElementById('generateCpuScriptHint');
        if (!items.length) {{
          if (hintEl) hintEl.textContent = 'No actionable rows in current filter.';
          return;
        }}
        const content = buildCpuUpdateScript(items);
        const blob = new Blob([content], {{ type: 'text/x-python' }});
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        const ts = new Date().toISOString().replace(/[:.]/g, '-');
        a.href = url;
        a.download = 'apply_cpu_requirements_' + ts + '.py';
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        URL.revokeObjectURL(url);
        if (hintEl) hintEl.textContent = 'Script downloaded for ' + items.length + ' suite(s).';
      }}

      function refreshCpuScriptHint() {{
        const items = getSuggestionsForScript();
        const hintEl = document.getElementById('generateCpuScriptHint');
        if (!hintEl) return;
        hintEl.textContent = items.length
          ? ('Will include ' + items.length + ' actionable suite(s) from current filter. Script supports --mode all|only-raise|only-lower|only-set.')
          : 'No actionable rows in current filter.';
      }}

      function sortableValue(raw) {{
        const s = String(raw ?? '').replace(/<[^>]*>/g, '').replace(/[\\s,]+/g, ' ').trim();
        const n = Number(s.replace(/[^\\d.+-]/g, ''));
        return Number.isFinite(n) && s.match(/[-+]?\\d/) ? n : s.toLowerCase();
      }}

      const suggestionsColumns = [
        'idx', 'suite_path', 'ya_ram_gb', 'ya_cpu_cores', 'ya_size',
        'chunks_count', 'median_cores', 'p95_cores', 'total_cpu_sec',
        'total_ram_gb', 'total_dur_sec', 'recommended_cpu', 'cpu_action',
        'test_errors', 'test_timeouts', 'test_muted', 'test_muted_timeouts', 'test_fails_total',
        'errors', 'timeouts', 'muted', 'muted_timeouts', 'fails_total',
        'max_parallel_self', 'max_parallel_self_at_sec',
        'peak_others_during_suite', 'peak_others_during_suite_at_sec',
        'peak_self_cpu_cores_during_suite', 'peak_self_cpu_at_sec',
        'peak_self_ram_gb_during_suite', 'peak_self_ram_at_sec'
      ];

      function actionCell(action) {{
        if (action === 'raise') return '<span style="color:#856404;background:#fff3cd;padding:2px 6px;border-radius:10px;">raise</span>';
        if (action === 'lower') return '<span style="color:#0c5460;background:#d1ecf1;padding:2px 6px;border-radius:10px;">lower</span>';
        if (action === 'set') return '<span style="color:#721c24;background:#f8d7da;padding:2px 6px;border-radius:10px;">set</span>';
        return '<span style="color:#155724;background:#d4edda;padding:2px 6px;border-radius:10px;">ok</span>';
      }}

      function renderSuggestionsTable() {{
        const q = (document.getElementById('suiteSearch')?.value || '').trim().toLowerCase();
        const filtered = data.cpu_suggestions.filter(s => !q || String(s.suite_path || '').toLowerCase().includes(q));
        const rowsData = filtered.map((s, i) => {{
          const cpuExplain = String(s.recommended_cpu_explain || '').replace(/"/g, '&quot;');
          const cpuReqText = String(s.recommended_cpu ?? 1);
          const cpuBadge = cpuReqText === 'all'
            ? '<span title="' + cpuExplain + '" style="display:inline-block;padding:2px 8px;border-radius:10px;background:#ffe8cc;color:#7c2d12;border:1px solid #fdba74;font-weight:700;">cpu:all</span>'
            : '<span title="' + cpuExplain + '"><b>cpu:' + cpuReqText + '</b></span>';
          const cpuCell = cpuBadge + (s.has_synthetic ? ' <span style="color:#b8860b;">(estimated from ya.make)</span>' : '');
          const sizeCapped = s.recommended_cpu_small_cap_applied || s.recommended_cpu_medium_cap_applied;
          const sizeCapHint = sizeCapped
            ? (s.recommended_cpu_small_cap_applied
                ? 'p95 exceeds SMALL limit (max cpu:1) — consider increasing SIZE to MEDIUM'
                : 'p95 exceeds MEDIUM limit (max cpu:4) — consider increasing SIZE to LARGE')
            : '';
          const sizeCell = sizeCapped
            ? '<span title="' + sizeCapHint + '" style="display:inline-flex;align-items:center;gap:3px;color:#991b1b;font-weight:600;">'
              + (s.ya_size || '') + ' <span style="font-size:11px;background:#fee2e2;border:1px solid #fca5a5;border-radius:4px;padding:0 4px;">cap!</span></span>'
            : String(s.ya_size ?? '');
          return [
            String(i + 1),
            '<label style="display:inline-flex;align-items:center;gap:5px;max-width:420px;">' +
            '<input type="checkbox" class="suite-marker-cb" style="flex-shrink:0;cursor:pointer;" data-suite="' + (s.suite_path || '').replace(/"/g, '&quot;') + '" onchange="toggleSuiteMarkers(this.dataset.suite, this.checked)">' +
            '<span style="overflow:hidden;text-overflow:ellipsis;white-space:nowrap;" title="' + (s.suite_path || '').replace(/"/g, '&quot;') + '">' + (s.suite_path || '') + '</span>' +
            '</label>',
            String(s.ya_ram_gb ?? ''),
            String(s.ya_cpu_cores ?? ''),
            sizeCell,
            String(s.chunks_count || 0),
            (s.median_cores != null ? Number(s.median_cores).toFixed(3) : ''),
            (s.p95_cores != null ? Number(s.p95_cores).toFixed(3) : ''),
            (s.total_cpu_sec != null ? Number(s.total_cpu_sec).toFixed(1) : ''),
            (s.total_ram_gb != null ? Number(s.total_ram_gb).toFixed(3) : ''),
            (s.total_dur_sec != null ? Number(s.total_dur_sec).toFixed(1) : '') + ' s',
            cpuCell,
            actionCell(s.cpu_action || 'ok'),
            String(s.test_errors || 0),
            String(s.test_timeouts || 0),
            String(s.test_muted || 0),
            String(s.test_muted_timeouts || 0),
            String(s.test_fails_total || 0),
            String(s.chunk_errors || 0),
            String(s.chunk_timeouts || 0),
            String(s.chunk_muted || 0),
            String(s.chunk_muted_timeouts || 0),
            String(s.chunk_fails_total || 0),
            String(s.max_parallel_self || 0),
            s.max_parallel_self_at_sec != null ? Number(s.max_parallel_self_at_sec).toFixed(1) : '',
            String(s.peak_others_during_suite || 0),
            s.peak_others_during_suite_at_sec != null ? Number(s.peak_others_during_suite_at_sec).toFixed(1) : '',
            s.peak_self_cpu_cores_during_suite != null ? Number(s.peak_self_cpu_cores_during_suite).toFixed(1) : '',
            s.peak_self_cpu_at_sec != null ? Number(s.peak_self_cpu_at_sec).toFixed(1) : '',
            s.peak_self_ram_gb_during_suite != null ? Number(s.peak_self_ram_gb_during_suite).toFixed(2) : '',
            s.peak_self_ram_at_sec != null ? Number(s.peak_self_ram_at_sec).toFixed(1) : '',
          ];
        }});

        rowsData.sort((a, b) => {{
          const av = sortableValue(a[suggestionsSortCol]);
          const bv = sortableValue(b[suggestionsSortCol]);
          if (av < bv) return suggestionsSortAsc ? -1 : 1;
          if (av > bv) return suggestionsSortAsc ? 1 : -1;
          return 0;
        }});

        const marker = (idx) => (idx === suggestionsSortCol ? (suggestionsSortAsc ? ' ▲' : ' ▼') : '');
        const topHeader =
          '<tr>' +
          '<th data-col="0" rowspan="2" style="cursor:pointer;user-select:none;">#' + marker(0) + '</th>' +
          '<th data-col="1" rowspan="2" style="cursor:pointer;user-select:none;">suite_path' + marker(1) + '</th>' +
          '<th colspan="3">ya.make</th>' +
          '<th colspan="1">runtime</th>' +
          '<th colspan="3">cpu usage</th>' +
          '<th colspan="1">ram usage</th>' +
          '<th colspan="1">runtime</th>' +
          '<th colspan="2">decision</th>' +
          '<th colspan="5">status tests</th>' +
          '<th colspan="5">status chunks</th>' +
          '<th colspan="2">suite parallelism</th>' +
          '<th colspan="2">others during suite</th>' +
          '<th colspan="2">cpu_self_peak during suite</th>' +
          '<th colspan="2">cpu_ram_peak during suite</th>' +
          '</tr>';
        const subHeader =
          '<tr>' +
          '<th data-col="2" style="cursor:pointer;user-select:none;">ya_ram_gb' + marker(2) + '</th>' +
          '<th data-col="3" style="cursor:pointer;user-select:none;">ya_cpu' + marker(3) + '</th>' +
          '<th data-col="4" class="group-end" style="cursor:pointer;user-select:none;">ya_size' + marker(4) + '</th>' +
          '<th data-col="5" class="group-end" style="cursor:pointer;user-select:none;">chunks' + marker(5) + '</th>' +
          '<th data-col="6" style="cursor:pointer;user-select:none;">median_cores' + marker(6) + '</th>' +
          '<th data-col="7" style="cursor:pointer;user-select:none;">p95_cores' + marker(7) + '</th>' +
          '<th data-col="8" class="group-end" style="cursor:pointer;user-select:none;">total_cpu_sec' + marker(8) + '</th>' +
          '<th data-col="9" class="group-end" style="cursor:pointer;user-select:none;">total_ram_gb' + marker(9) + '</th>' +
          '<th data-col="10" class="group-end" style="cursor:pointer;user-select:none;">total_dur_sec' + marker(10) + '</th>' +
          '<th data-col="11" style="cursor:pointer;user-select:none;">recommended_cpu' + marker(11) + '</th>' +
          '<th data-col="12" class="group-end" style="cursor:pointer;user-select:none;">cpu_action' + marker(12) + '</th>' +
          '<th data-col="13" style="cursor:pointer;user-select:none;">errors' + marker(13) + '</th>' +
          '<th data-col="14" style="cursor:pointer;user-select:none;">timeouts' + marker(14) + '</th>' +
          '<th data-col="15" style="cursor:pointer;user-select:none;">muted' + marker(15) + '</th>' +
          '<th data-col="16" style="cursor:pointer;user-select:none;">muted_timeouts' + marker(16) + '</th>' +
          '<th data-col="17" class="group-end" style="cursor:pointer;user-select:none;">fails_total' + marker(17) + '</th>' +
          '<th data-col="18" style="cursor:pointer;user-select:none;">errors' + marker(18) + '</th>' +
          '<th data-col="19" style="cursor:pointer;user-select:none;">timeouts' + marker(19) + '</th>' +
          '<th data-col="20" style="cursor:pointer;user-select:none;">muted' + marker(20) + '</th>' +
          '<th data-col="21" style="cursor:pointer;user-select:none;">muted_timeouts' + marker(21) + '</th>' +
          '<th data-col="22" class="group-end" style="cursor:pointer;user-select:none;">fails_total' + marker(22) + '</th>' +
          '<th data-col="23" style="cursor:pointer;user-select:none;" title="Peak simultaneous chunks of this suite">par' + marker(23) + '</th>' +
          '<th data-col="24" class="group-end" style="cursor:pointer;user-select:none;" title="Time (sec) when suite parallelism peaked">at (s)' + marker(24) + '</th>' +
          '<th data-col="25" style="cursor:pointer;user-select:none;" title="Peak chunks of OTHER suites while this suite was running">others' + marker(25) + '</th>' +
          '<th data-col="26" class="group-end" style="cursor:pointer;user-select:none;" title="Time (sec) of peak others">at (s)' + marker(26) + '</th>' +
          '<th data-col="27" style="cursor:pointer;user-select:none;" title="Peak CPU of THIS suite (cores) while this suite was running">cores' + marker(27) + '</th>' +
          '<th data-col="28" class="group-end" style="cursor:pointer;user-select:none;" title="Time (sec) of peak suite CPU">at (s)' + marker(28) + '</th>' +
          '<th data-col="29" style="cursor:pointer;user-select:none;" title="Peak RAM of THIS suite (GB) while this suite was running">GB' + marker(29) + '</th>' +
          '<th data-col="30" class="group-end" style="cursor:pointer;user-select:none;" title="Time (sec) of peak suite RAM">at (s)' + marker(30) + '</th>' +
          '</tr>';

        const groupEnds = new Set([4, 5, 8, 9, 10, 12, 17, 22, 24, 26, 28, 30]);
        const bodyHtml = rowsData.map(cols => (
          '<tr>' + cols.map((c, i) => '<td' + (groupEnds.has(i) ? ' class="group-end"' : '') + '>' + c + '</td>').join('') + '</tr>'
        )).join('');
        document.getElementById('cpuSuggestionsTable').innerHTML =
          '<table id=\"cpuSuggestionsInner\" style=\"width:100%;border-collapse:collapse;\"><thead>' + topHeader + subHeader + '</thead><tbody>' + bodyHtml + '</tbody></table>';

        // Fix sticky top for two-row thead: row 2 must sit below row 1.
        const theadRows = document.querySelectorAll('#cpuSuggestionsInner thead tr');
        if (theadRows.length >= 2) {{
          theadRows[0].querySelectorAll('th').forEach(th => {{
            th.style.position = 'sticky';
            th.style.top = '0';
            th.style.zIndex = '3';
          }});
          const row1H = theadRows[0].getBoundingClientRect().height;
          theadRows[1].querySelectorAll('th').forEach(th => {{
            th.style.position = 'sticky';
            th.style.top = row1H + 'px';
            th.style.zIndex = '2';
          }});
        }}

        const ths = document.querySelectorAll('#cpuSuggestionsInner thead th');
        ths.forEach(th => th.addEventListener('click', () => {{
          const col = Number(th.getAttribute('data-col'));
          if (suggestionsSortCol === col) suggestionsSortAsc = !suggestionsSortAsc;
          else {{
            suggestionsSortCol = col;
            suggestionsSortAsc = true;
          }}
          renderSuggestionsTable();
        }}));

        // Restore checkbox state for suites that still have markers.
        document.querySelectorAll('.suite-marker-cb').forEach(cb => {{
          if (_suiteMarkerData[cb.dataset.suite]) cb.checked = true;
        }});
        refreshCpuScriptHint();
      }}

      const helpBtn = document.getElementById('cpuHelpToggle');
      const helpBox = document.getElementById('cpuHelpText');
      if (helpBtn && helpBox) {{
        helpBtn.addEventListener('click', () => {{
          helpBox.style.display = helpBox.style.display === 'block' ? 'none' : 'block';
        }});
      }}
      const genBtn = document.getElementById('generateCpuScriptBtn');
      if (genBtn) {{
        genBtn.addEventListener('click', downloadCpuUpdateScript);
      }}
      window.renderSuggestionsTable = renderSuggestionsTable;
      renderSuggestionsTable();
    }}

    function colorForTrack(trackName) {{
      const s = data.track_suite[trackName];
      if (!s || s === 'other') return '#bdbdbd';
      return data.suite_color[s] || '#999';
    }}

    function suiteFromLabel(label) {{
      if (!label || label === 'other') return '';
      if (label.includes('::')) return label.split('::', 1)[0];
      return label;
    }}

    function hexToRgba(hex, alpha) {{
      if (!hex || !hex.startsWith('#') || (hex.length !== 7 && hex.length !== 4)) {{
        return `rgba(180,180,180,${{alpha}})`;
      }}
      let r, g, b;
      if (hex.length === 4) {{
        r = parseInt(hex[1] + hex[1], 16);
        g = parseInt(hex[2] + hex[2], 16);
        b = parseInt(hex[3] + hex[3], 16);
      }} else {{
        r = parseInt(hex.slice(1, 3), 16);
        g = parseInt(hex.slice(3, 5), 16);
        b = parseInt(hex.slice(5, 7), 16);
      }}
      return `rgba(${{r}},${{g}},${{b}},${{alpha}})`;
    }}

    function isMatch(label, q) {{
      if (!q) return true;
      const s = suiteFromLabel(label).toLowerCase();
      const l = String(label || '').toLowerCase();
      return s.includes(q) || l.includes(q);
    }}

    function updateStackedPlotColors(plotId, q) {{
      const el = document.getElementById(plotId);
      if (!el || !el.data || !window.Plotly) return;
      const lineColors = [];
      const lineWidths = [];
      const fillColors = [];
      const opacities = [];
      const idx = [];
      el.data.forEach((tr, i) => {{
        const name = tr.name || '';
        const base = colorForTrack(name);
        const matched = name === 'other' ? true : isMatch(name, q);
        lineColors.push(matched ? base : hexToRgba(base, 0.65));
        lineWidths.push(matched ? 1 : 0.8);
        // Keep original per-suite color, make non-matching traces only moderately dim.
        fillColors.push(matched ? base : hexToRgba(base, 0.55));
        opacities.push(matched ? (name === 'other' ? 0.35 : 0.78) : 0.52);
        idx.push(i);
      }});
      Plotly.restyle(el, {{
        'line.color': lineColors,
        'line.width': lineWidths,
        'fillcolor': fillColors,
        'opacity': opacities,
      }}, idx);
    }}

    function updatePieColors(plotId, q) {{
      const el = document.getElementById(plotId);
      if (!el || !el.data || !el.data[0] || !window.Plotly) return;
      const labels = el.data[0].labels || [];
      const colors = labels.map(lb => {{
        const base = lb === 'other' ? '#bdbdbd' : (data.suite_color[suiteFromLabel(lb)] || '#999');
        const matched = isMatch(lb, q);
        return hexToRgba(base, matched ? 0.92 : 0.18);
      }});
      Plotly.restyle(el, {{'marker.colors': [colors]}}, [0]);
    }}

    function reorderStackedPlotByQuery(plotId, q) {{
      const el = document.getElementById(plotId);
      if (!el || !el.data || !window.Plotly) return;
      const names = el.data.map(t => String(t?.name || ''));
      const matched = [];
      const rest = [];
      names.forEach((name, i) => {{
        if (name !== 'other' && isMatch(name, q)) matched.push(i);
        else rest.push(i);
      }});
      // In stackgroup the first traces are the lowest layers.
      const order = matched.concat(rest);
      const identity = order.every((v, i) => v === i);
      if (!identity) {{
        Plotly.moveTraces(el, order, [...Array(order.length).keys()]);
      }}
    }}

    function applySuiteSearch() {{
      const q = (document.getElementById('suiteSearch')?.value || '').trim().toLowerCase();
      const inc = document.getElementById('includeSyntheticCb')?.checked ?? true;
      const cpuSuiteBase = inc ? data.cpu_tracks_suite : data.cpu_tracks_suite_no_synthetic;
      const ramSuiteBase = inc ? data.ram_tracks_suite : data.ram_tracks_suite_no_synthetic;
      const cpuChunkBase = inc ? data.cpu_tracks : data.cpu_tracks_no_synthetic;
      const ramChunkBase = inc ? data.ram_tracks : data.ram_tracks_no_synthetic;

      if (data.by_chunk && data.xs_cpu && data.xs_cpu.length > 0) {{
        updateStackedPlotTracks('activeLayerChunk', data.active_tracks_chunk);
        updateStackedPlotTracks('cpuLayer', cpuChunkBase);
        updateStackedPlotTracks('ramLayer', ramChunkBase);
      }}
      updateStackedPlotTracks('activeLayerSuite', data.active_tracks_suite);
      updateStackedPlotTracks('cpuLayerSuite', cpuSuiteBase);
      updateStackedPlotTracks('ramLayerSuite', ramSuiteBase);

      ['activeLayerChunk', 'cpuLayer', 'ramLayer', 'activeLayerSuite', 'cpuLayerSuite', 'ramLayerSuite']
        .forEach(id => updateStackedPlotColors(id, q));
      ['activeLayerChunk', 'cpuLayer', 'ramLayer', 'activeLayerSuite', 'cpuLayerSuite', 'ramLayerSuite']
        .forEach(id => reorderStackedPlotByQuery(id, q));
      ['cpuPie', 'ramPie', 'activePie', 'cpuPieSuite', 'ramPieSuite', 'activePieSuite']
        .forEach(id => updatePieColors(id, q));
      if (window.renderSuggestionsTable) {{
        window.renderSuggestionsTable();
      }}
      // Plot restyle/recolor can drop layout overlays; re-apply selected markers.
      applyMarkersToCharts();
      if (window.applyLayerToggles) applyLayerToggles();
    }}

    function clearSuiteSearch() {{
      const el = document.getElementById('suiteSearch');
      if (el) el.value = '';
      applySuiteSearch();
    }}


    function _hexToRgba(hex, alpha) {{
      if (!hex || hex.length < 7) return 'rgba(100,100,200,' + alpha + ')';
      const r = parseInt(hex.slice(1,3), 16), g = parseInt(hex.slice(3,5), 16), b = parseInt(hex.slice(5,7), 16);
      return 'rgba(' + r + ',' + g + ',' + b + ',' + alpha + ')';
    }}

    function _updateMarkersLegend() {{
      const suites = Object.keys(_suiteMarkerData);
      const legend = document.getElementById('markersLegend');
      if (!legend) return;
      if (suites.length === 0) {{ legend.style.display = 'none'; return; }}
      legend.style.display = '';
      const badges = suites.map(sp => {{
        const color = (data.suite_color && data.suite_color[sp]) || '#6366f1';
        const short = sp.split('/').slice(-2).join('/');
        const markers = ((_suiteMarkerData[sp] || {{}}).markers || []);
        const errN = markers.filter(m => m.kind === 'error').length;
        const toN = markers.filter(m => m.kind === 'timeout').length;
        return '<span style="display:inline-block;margin-right:4px;padding:1px 5px;border-radius:3px;background:' + color + ';color:#fff;font-size:10px;" title="' + sp + ' | errors:' + errN + ' | timeouts:' + toN + '">' + short + '</span>';
      }});
      document.getElementById('markersLegendSuites').innerHTML = badges.join('');
      const st = _countEventMarkers();
      const statsEl = document.getElementById('markersLegendStats');
      if (statsEl) {{
        statsEl.textContent =
          'errors visible/all: ' + st.errorVisible + '/' + st.errorAll +
          ', timeouts visible/all: ' + st.timeoutVisible + '/' + st.timeoutAll;
      }}
    }}

    function _enabledMarkerKinds() {{
      const byId = {{
        start: 'mkStart',
        end: 'mkEnd',
        par: 'mkPar',
        others: 'mkOthers',
        cpu: 'mkCpu',
        ram: 'mkRam',
        error: 'mkError',
        timeout: 'mkTimeout',
      }};
      const enabled = new Set();
      Object.entries(byId).forEach(([kind, id]) => {{
        const el = document.getElementById(id);
        if (!el || el.checked) enabled.add(kind);
      }});
      return enabled;
    }}

    function _countEventMarkers() {{
      const enabledKinds = _enabledMarkerKinds();
      let errorAll = 0, timeoutAll = 0, errorVisible = 0, timeoutVisible = 0;
      Object.values(_suiteMarkerData).forEach(d => {{
        (d.markers || []).forEach(m => {{
          if (m.kind === 'error') {{
            errorAll += 1;
            if (enabledKinds.has('error')) errorVisible += 1;
          }} else if (m.kind === 'timeout') {{
            timeoutAll += 1;
            if (enabledKinds.has('timeout')) timeoutVisible += 1;
          }}
        }});
      }});
      return {{ errorAll, timeoutAll, errorVisible, timeoutVisible }};
    }}

    function _displayedPeakTime(chartId, suitePath) {{
      const el = document.getElementById(chartId);
      if (!el || !el.data || !Array.isArray(el.data)) return null;
      const tr = el.data.find(t => t && t.name === suitePath && Array.isArray(t.x) && Array.isArray(t.y));
      if (!tr) return null;
      let bestY = -Infinity;
      let bestX = null;
      for (let i = 0; i < tr.y.length; i++) {{
        const y = Number(tr.y[i]);
        if (!Number.isFinite(y)) continue;
        if (y > bestY) {{
          bestY = y;
          bestX = Number(tr.x[i]);
        }}
      }}
      if (!Number.isFinite(bestY) || bestY <= 0 || !Number.isFinite(Number(bestX))) return null;
      return xFromDisplay(Number(bestX));
    }}

    function applyMarkersToCharts() {{
      const enabledKinds = _enabledMarkerKinds();
      const allShapes = [];
      const allAnnotations = [];
      Object.values(_suiteMarkerData).forEach(d => {{
        (d.markers || []).forEach(m => {{
          if (!enabledKinds.has(m.kind)) return;
          allShapes.push({{
            type: 'line', x0: m.x, x1: m.x, y0: 0, y1: 1, yref: 'paper',
            line: {{ color: m.color, width: m.width, dash: m.dash }},
          }});
          allAnnotations.push({{
            x: m.x, y: m.yAnchor, yref: 'paper',
            text: '<b>' + m.label + '</b><br><span style="font-size:10px">' + m.shortName + '</span>',
            showarrow: true, arrowhead: 3, arrowsize: 0.8, arrowcolor: m.color,
            ax: 0, ay: -28,
            bgcolor: m.color, bordercolor: m.color, borderwidth: 1, borderpad: 3,
            font: {{ color: '#fff', size: 10 }},
            hovertext: m.title + ' (' + m.shortName + ')',
            captureevents: true,
            opacity: 0.85,
          }});
        }});
      }});
      _markerChartIds.forEach(id => {{
        const el = document.getElementById(id);
        if (el && el.layout) Plotly.relayout(el, {{ shapes: allShapes, annotations: allAnnotations }});
      }});
      _updateMarkersLegend();
    }}

    function toggleSuiteMarkers(suitePath, enabled) {{
      if (!enabled) {{
        delete _suiteMarkerData[suitePath];
        applyMarkersToCharts();
        return;
      }}
      const s = data.cpu_suggestions.find(x => x.suite_path === suitePath);
      if (!s) return;
      const color = (data.suite_color && data.suite_color[suitePath]) || '#6366f1';
      const shortName = suitePath.split('/').slice(-2).join('/');
      const markers = [];
      // yAnchor offsets so labels for different suites don't overlap.
      const suiteIdx = Object.keys(_suiteMarkerData).length;
      const yAnchor = Math.max(0.05, 0.95 - suiteIdx * 0.12);
      const addLine = (t, dash, width, label, title, kind, lineColor) => {{
        if (t == null || !Number.isFinite(Number(t))) return;
        const x = xToDisplay(Number(t));
        markers.push({{
          x, dash, width, label, title, kind,
          color: lineColor || color,
          shortName,
          yAnchor,
        }});
      }};
      addLine(s.suite_start_sec,                 'solid',    2,   'start',   'Suite start', 'start');
      addLine(s.suite_end_sec,                   'dash',     2,   'end',     'Suite end', 'end');
      addLine(s.max_parallel_self_at_sec,        'dot',      1.5, 'par\u2191', 'Peak parallel self chunks', 'par');
      addLine(s.peak_others_during_suite_at_sec, 'dot',      1.5, 'others\u2191', 'Peak parallel others', 'others');
      // Prefer visible chart peak times so markers match what user sees after downsampling/filtering.
      const visCpuAt = _displayedPeakTime('cpuLayerSuite', suitePath);
      const visRamAt = _displayedPeakTime('ramLayerSuite', suitePath);
      addLine(visCpuAt != null ? visCpuAt : s.peak_self_cpu_at_sec, 'dashdot', 1.5, 'cpu\u2191', 'Peak suite CPU', 'cpu');
      addLine(visRamAt != null ? visRamAt : s.peak_self_ram_at_sec, 'dashdot', 1.5, 'ram\u2191', 'Peak suite RAM', 'ram');
      const events = (data.suite_event_times && data.suite_event_times[suitePath]) || {{}};
      const errColor = '#b00020';
      const timeoutColor = '#e65100';
      (events.error_sec || []).forEach((t, i) => {{
        addLine(t, 'longdash', 2, 'err' + (i + 1), 'Test error end', 'error', errColor);
      }});
      (events.timeout_sec || []).forEach((t, i) => {{
        addLine(t, 'longdashdot', 2, 'to' + (i + 1), 'Test timeout end', 'timeout', timeoutColor);
      }});
      _suiteMarkerData[suitePath] = {{ markers }};
      applyMarkersToCharts();
    }}

    function clearAllMarkers() {{
      Object.keys(_suiteMarkerData).forEach(k => delete _suiteMarkerData[k]);
      document.querySelectorAll('.suite-marker-cb').forEach(cb => {{ cb.checked = false; }});
      applyMarkersToCharts();
    }}

    function updateStackedPlotTracks(divId, tracks) {{
      const el = document.getElementById(divId);
      if (!el || !el.data || !window.Plotly) return;
      const indicesToUpdate = [];
      const yArrays = [];
      el.data.forEach((tr, i) => {{
        const n = tr && tr.name;
        if (n != null && tracks[n] != null) {{
          indicesToUpdate.push(i);
          yArrays.push(tracks[n]);
        }}
      }});
      if (indicesToUpdate.length) Plotly.restyle(el, {{y: yArrays}}, indicesToUpdate);
    }}

    function applySyntheticToCharts() {{
      const inc = document.getElementById('includeSyntheticCb')?.checked ?? true;
      updateStackedPlotTracks('cpuLayerSuite', inc ? data.cpu_tracks_suite : data.cpu_tracks_suite_no_synthetic);
      updateStackedPlotTracks('ramLayerSuite', inc ? data.ram_tracks_suite : data.ram_tracks_suite_no_synthetic);
      if (data.by_chunk && data.xs_cpu && data.xs_cpu.length > 0) {{
        updateStackedPlotTracks('cpuLayer', inc ? data.cpu_tracks : data.cpu_tracks_no_synthetic);
        updateStackedPlotTracks('ramLayer', inc ? data.ram_tracks : data.ram_tracks_no_synthetic);
      }}
      applySuiteSearch();
    }}

    const cb = document.getElementById('includeSyntheticCb');
    const cbWrap = document.getElementById('syntheticCheckboxWrap');
    if (cb) {{
      if (!data.has_synthetic_metrics) {{
        cb.checked = false;
        cb.disabled = true;
        if (cbWrap) {{
          cbWrap.title = 'No runs with estimated metrics from ya.make in this report.';
        }}
      }}
      cb.addEventListener('change', applySyntheticToCharts);
    }}

    function humanFromKb(kb) {{
      const mb = kb / 1024.0;
      if (mb < 1024) return mb.toFixed(2) + ' MB';
      const gb = mb / 1024.0;
      return gb.toFixed(2) + ' GB';
    }}

    function humanFromSec(sec) {{
      if (sec < 60) return sec.toFixed(2) + ' s';
      if (sec < 3600) return (sec / 60).toFixed(2) + ' min';
      return (sec / 3600).toFixed(2) + ' h';
    }}

    function stackedArea(divId, xs, tracks, title, yTitle, stepMode) {{
      const names = Object.keys(tracks);
      const xDisp = (xs || []).map(xToDisplay);
      const traces = names.map((n) => {{
        const c = colorForTrack(n);
        return {{
          x: xDisp,
          y: tracks[n],
          mode: 'lines',
          line: {{width: 1, color: c, shape: stepMode ? 'hv' : 'linear'}},
          fillcolor: c,
          opacity: n === 'other' ? 0.45 : 0.75,
          name: n,
          stackgroup: 'one',
          hoverinfo: 'none',
        }};
      }});
      Plotly.newPlot(divId, traces, {{
        title,
        xaxis: axisLayout(xDisp),
        yaxis: {{title: yTitle}},
        hovermode: 'x unified',
        showlegend: false,
        margin: {{l: 60, r: 20, t: 50, b: 50}},
      }}, {{responsive: true}});
    }}

    function formatValue(y, unit) {{
      if (unit === 'active') return String(Math.round(y));
      if (unit === 'GB') return y.toFixed(3);
      return y.toFixed(3);
    }}

    function minVisibleValue(unit) {{
      if (unit === 'active') return 0.5;
      if (unit === 'GB') return 0.001;   // ~1 MB
      if (unit === 'cores') return 0.01; // suppress tiny CPU noise shown as 0.000
      return 0.0;
    }}

    function attachSortedHover(plotId, panelId, unit) {{
      const plot = document.getElementById(plotId);
      const panel = document.getElementById(panelId);
      if (!plot || !panel) return;
      plot.on('plotly_hover', (ev) => {{
        if (!ev || !ev.points || !ev.points.length) return;
        const t = ev.points[0].x;
        const eps = minVisibleValue(unit);
        const isOutline = (n) => n && (n.includes('outline') || n.endsWith(' outline'));
        const isMonitor = (n) => n && n.includes('(monitor)');
        const rows = ev.points
          .map(p => ({{name: p.data.name, y: Number(p.y || 0)}}))
          .filter(p => p.y > eps && !isOutline(p.name) && !isMonitor(p.name))
          .sort((a, b) => b.y - a.y);
        const top = rows.slice(0, 40);
        const lines = top.map(r => `${{r.name}}: ${{formatValue(r.y, unit)}} ${{unit}}`);
        panel.textContent = `t=${{formatTimeLabel(t)}}\\n` + lines.join('\\n');
      }});
      plot.on('plotly_unhover', () => {{
        panel.textContent = 'Move cursor over chart to see sorted contributors';
      }});
    }}

    function monitorValueAt(plotId, traceName, tDisp) {{
      const plot = document.getElementById(plotId);
      if (!plot || !plot.data || !Array.isArray(plot.data)) return null;
      const tr = plot.data.find(x => x && x.name === traceName && Array.isArray(x.x) && Array.isArray(x.y));
      if (!tr || tr.x.length === 0 || tr.y.length === 0) return null;
      const target = _toMs(tDisp);
      if (target == null) return null;
      let bestIdx = -1;
      let bestDist = Number.POSITIVE_INFINITY;
      for (let i = 0; i < tr.x.length; i += 1) {{
        const cur = _toMs(tr.x[i]);
        if (cur == null) continue;
        const d = Math.abs(cur - target);
        if (d < bestDist) {{
          bestDist = d;
          bestIdx = i;
        }}
      }}
      if (bestIdx < 0) return null;
      const v = Number(tr.y[bestIdx]);
      return Number.isFinite(v) ? v : null;
    }}

    function renderClickTable(plotId, panelId, rows, t, unit, monitorAtClick=null) {{
      const panel = document.getElementById(panelId);
      if (!panel) return;
      const top = rows.slice(0, 100);
      const includeSynthetic = document.getElementById('includeSyntheticCb')?.checked ?? true;
      const htmlRows = top.map((r, i) => {{
        const isSynthetic = includeSynthetic && data.has_synthetic_metrics && data.track_has_synthetic && data.track_has_synthetic[r.name];
        const syntheticBadge = isSynthetic ? ' <span style="color:#b8860b;">(estimated from ya.make)</span>' : '';
        return (
        '<tr>' +
          '<td>' + (i + 1) + '</td>' +
          '<td><span style="display:inline-block;width:10px;height:10px;border-radius:2px;margin-right:6px;vertical-align:middle;background:' + colorForTrack(r.name) + ';"></span>' + r.name + syntheticBadge + '</td>' +
          '<td>' + formatValue(r.y, unit) + ' ' + unit + '</td>' +
        '</tr>'
        );
      }}).join('');
      let totalsHtml = '';
      if (unit === 'GB') {{
        const totalBySuite = rows.reduce((acc, r) => acc + Number(r.y || 0), 0);
        const totalMonitoring = Number.isFinite(Number(monitorAtClick))
          ? Number(monitorAtClick)
          : monitorValueAt(plotId, 'RAM total (monitor)', t);
        totalsHtml =
          '<div style="margin:4px 0 8px 0;font-size:12px;color:#374151;">' +
          '<b>Total RAM by suite:</b> ' + totalBySuite.toFixed(3) + ' GB' +
          '&nbsp;&nbsp;|&nbsp;&nbsp;<b>Total RAM monitoring:</b> ' +
          (totalMonitoring == null ? 'n/a' : (Number(totalMonitoring).toFixed(3) + ' GB')) +
          '</div>';
      }} else if (unit === 'cores') {{
        const totalBySuite = rows.reduce((acc, r) => acc + Number(r.y || 0), 0);
        const totalMonitoring = Number.isFinite(Number(monitorAtClick))
          ? Number(monitorAtClick)
          : monitorValueAt(plotId, 'CPU total (monitor)', t);
        totalsHtml =
          '<div style="margin:4px 0 8px 0;font-size:12px;color:#374151;">' +
          '<b>Total CPU by suite:</b> ' + totalBySuite.toFixed(3) + ' cores' +
          '&nbsp;&nbsp;|&nbsp;&nbsp;<b>Total CPU monitoring:</b> ' +
          (totalMonitoring == null ? 'n/a' : (Number(totalMonitoring).toFixed(3) + ' cores')) +
          '</div>';
      }}
      panel.innerHTML =
        '<div><b>t=' + formatTimeLabel(t) + '</b> | rows: ' + top.length + '</div>' +
        totalsHtml +
        '<table><thead><tr><th>#</th><th>suite+chunk</th><th>value</th></tr></thead><tbody>' + htmlRows + '</tbody></table>';
    }}

    function attachSortedClick(plotId, panelId, unit) {{
      const plot = document.getElementById(plotId);
      if (!plot) return;
      plot.on('plotly_click', (ev) => {{
        if (!ev || !ev.points || !ev.points.length) return;
        const t = ev.points[0].x;
        const eps = minVisibleValue(unit);
        const isOutline = (n) => n && (n.includes('outline') || n.endsWith(' outline'));
        const isMonitor = (n) => n && n.includes('(monitor)');
        const monitorPoint = ev.points.find(p => p && p.data && typeof p.data.name === 'string' && p.data.name === (unit === 'GB' ? 'RAM total (monitor)' : 'CPU total (monitor)'));
        const monitorAtClick = monitorPoint ? Number(monitorPoint.y) : null;
        const rows = ev.points
          .map(p => ({{name: p.data.name, y: Number(p.y || 0)}}))
          .filter(p => p.y > eps && !isOutline(p.name) && !isMonitor(p.name))
          .sort((a, b) => b.y - a.y);
        renderClickTable(plotId, panelId, rows, t, unit, monitorAtClick);
      }});
    }}

    Plotly.newPlot('activeChunks', [{{
      x: (data.xs_active || []).map(xToDisplay),
      y: data.ys_active,
      mode: 'lines',
      name: 'active_chunks',
      line: {{color: '#444', width: 2, shape: 'hv'}},
      hoverinfo: 'none',
    }}], {{
      title: 'Concurrent running chunks',
      xaxis: axisLayout((data.xs_active || []).map(xToDisplay)),
      yaxis: {{title: 'count'}},
      hovermode: 'x unified',
    }}, {{responsive: true}});

    if (data.by_chunk && data.xs_active_chunk && data.xs_active_chunk.length > 0) {{
      stackedArea('activeLayerChunk', data.xs_active_chunk, data.active_tracks_chunk, 'Layered active chunks by suite+chunk', 'active chunks', true);
      attachSortedHover('activeLayerChunk', 'activeHoverChunk', 'active');
      attachSortedClick('activeLayerChunk', 'activeClickChunk', 'active');
      stackedArea('cpuLayer', data.xs_cpu, data.cpu_tracks, 'Layered CPU (estimated cores) by suite+chunk', 'cores');
      stackedArea('ramLayer', data.xs_ram, data.ram_tracks, 'Layered RAM by suite+chunk', 'GB');
      if (data.resources_overlay) {{
        const ro = data.resources_overlay;
        const xDisp = (ro.xs_evlog_sec || []).map(x => xToDisplay(x));
        const xLabels = xDisp.map(v => formatTimeLabel(v));
        Plotly.addTraces('cpuLayer', [
          {{ x: xDisp, y: ro.cpu_total_cores || [], mode: 'lines', name: 'CPU total (monitor) outline', line: {{ color: '#000000', width: 8 }}, legendrank: 1000, showlegend: false, hoverinfo: 'skip' }},
          {{ x: xDisp, y: ro.cpu_total_cores || [], mode: 'lines', name: 'CPU total (monitor)', line: {{ color: '#ff1744', width: 6 }}, legendrank: 1000, hovertemplate: '%{{x|%Y-%m-%d %H:%M:%S}}<br>%{{fullData.name}}: %{{y:.3f}} cores<extra></extra>' }},
        ]);
        Plotly.addTraces('ramLayer', [
          {{ x: xDisp, y: ro.ram_gb || [], mode: 'lines', name: 'RAM total (monitor) outline', line: {{ color: '#000000', width: 8 }}, legendrank: 1000, showlegend: false, hoverinfo: 'skip' }},
          {{ x: xDisp, y: ro.ram_gb || [], mode: 'lines', name: 'RAM total (monitor)', line: {{ color: '#ff1744', width: 6 }}, legendrank: 1000, hovertemplate: '%{{x|%Y-%m-%d %H:%M:%S}}<br>%{{fullData.name}}: %{{y:.3f}} GB<extra></extra>' }},
        ]);
        Plotly.relayout('cpuLayer', {{ hovermode: 'x unified' }});
        Plotly.relayout('ramLayer', {{ hovermode: 'x unified' }});
        const diskEl = document.getElementById('diskLayer');
        if (diskEl) {{
          diskEl.style.display = '';
          const evlogRange = ro.evlog_range_sec || [];
          const xaxisOpt = Object.assign({{}}, axisLayout(xDisp));
          if (evlogRange.length >= 2) {{
            xaxisOpt.range = [xToDisplay(Number(evlogRange[0])), xToDisplay(Number(evlogRange[1]))];
          }}
          Plotly.newPlot('diskLayer', [
            {{ x: xDisp, y: ro.disk_read_mb || [], mode: 'lines', name: 'Disk read (MB/s)', line: {{ color: '#ea580c' }} }},
            {{ x: xDisp, y: ro.disk_write_mb || [], mode: 'lines', name: 'Disk write (MB/s)', line: {{ color: '#7c3aed' }} }}
          ], {{
            title: 'Disk I/O (MB per 1s interval)',
            xaxis: xaxisOpt,
            yaxis: {{ title: 'MB' }},
            hovermode: 'x unified',
          }}, {{ responsive: true }});
        }}
      }}
      attachSortedHover('cpuLayer', 'cpuHover', 'cores');
      attachSortedHover('ramLayer', 'ramHover', 'GB');
      attachSortedClick('cpuLayer', 'cpuClick', 'cores');
      attachSortedClick('ramLayer', 'ramClick', 'GB');
    }}

    stackedArea('activeLayerSuite', data.xs_active_suite, data.active_tracks_suite, 'Layered active chunks by suite', 'active chunks', true);
    attachSortedHover('activeLayerSuite', 'activeHoverSuite', 'active');
    attachSortedClick('activeLayerSuite', 'activeClickSuite', 'active');

    stackedArea('cpuLayerSuite', data.xs_cpu_suite, data.cpu_tracks_suite, 'Layered CPU (estimated cores) by suite', 'cores');
    stackedArea('ramLayerSuite', data.xs_ram_suite, data.ram_tracks_suite, 'Layered RAM by suite', 'GB');
    if (data.resources_overlay) {{
      const ro = data.resources_overlay;
      const xDisp = (ro.xs_evlog_sec || []).map(x => xToDisplay(x));
      const xLabels = xDisp.map(v => formatTimeLabel(v));
      Plotly.addTraces('cpuLayerSuite', [
        {{ x: xDisp, y: ro.cpu_total_cores || [], mode: 'lines', name: 'CPU total (monitor) outline', line: {{ color: '#000000', width: 8 }}, legendrank: 1000, showlegend: false, hoverinfo: 'skip' }},
        {{ x: xDisp, y: ro.cpu_total_cores || [], mode: 'lines', name: 'CPU total (monitor)', line: {{ color: '#ff1744', width: 6 }}, legendrank: 1000, hovertemplate: '%{{x|%Y-%m-%d %H:%M:%S}}<br>%{{fullData.name}}: %{{y:.3f}} cores<extra></extra>' }},
      ]);
      Plotly.addTraces('ramLayerSuite', [
        {{ x: xDisp, y: ro.ram_gb || [], mode: 'lines', name: 'RAM total (monitor) outline', line: {{ color: '#000000', width: 8 }}, legendrank: 1000, showlegend: false, hoverinfo: 'skip' }},
        {{ x: xDisp, y: ro.ram_gb || [], mode: 'lines', name: 'RAM total (monitor)', line: {{ color: '#ff1744', width: 6 }}, legendrank: 1000, hovertemplate: '%{{x|%Y-%m-%d %H:%M:%S}}<br>%{{fullData.name}}: %{{y:.3f}} GB<extra></extra>' }},
      ]);
      Plotly.relayout('cpuLayerSuite', {{ hovermode: 'x unified' }});
      Plotly.relayout('ramLayerSuite', {{ hovermode: 'x unified' }});
      const diskEl = document.getElementById('diskLayerSuite');
      if (diskEl) {{
        diskEl.style.display = '';
        const evlogRange = ro.evlog_range_sec || [];
        const xaxisOpt = Object.assign({{}}, axisLayout(xDisp));
        if (evlogRange.length >= 2) {{
          xaxisOpt.range = [xToDisplay(Number(evlogRange[0])), xToDisplay(Number(evlogRange[1]))];
        }}
        Plotly.newPlot('diskLayerSuite', [
          {{ x: xDisp, y: ro.disk_read_mb || [], mode: 'lines', name: 'Disk read (MB/s)', line: {{ color: '#ea580c' }} }},
          {{ x: xDisp, y: ro.disk_write_mb || [], mode: 'lines', name: 'Disk write (MB/s)', line: {{ color: '#7c3aed' }} }}
        ], {{
          title: 'Disk I/O (MB per 1s interval)',
          xaxis: xaxisOpt,
          yaxis: {{ title: 'MB' }},
          hovermode: 'x unified',
        }}, {{ responsive: true }});
      }}
    }}
    attachSortedHover('cpuLayerSuite', 'cpuHoverSuite', 'cores');
    attachSortedHover('ramLayerSuite', 'ramHoverSuite', 'GB');
    attachSortedClick('cpuLayerSuite', 'cpuClickSuite', 'cores');
    attachSortedClick('ramLayerSuite', 'ramClickSuite', 'GB');

    if (data.by_chunk && data.pie_cpu_labels && data.pie_cpu_labels.length > 0) {{
      Plotly.newPlot('cpuPie', [{{
        type: 'pie',
        labels: data.pie_cpu_labels,
        values: data.pie_cpu_vals,
        customdata: data.pie_cpu_vals.map(v => humanFromSec(v)),
        marker: {{colors: data.pie_cpu_labels.map(lb => lb === 'other' ? '#bdbdbd' : (data.suite_color[suiteFromLabel(lb)] || '#999'))}},
        textinfo: 'percent',
        textposition: 'inside',
        automargin: true,
        sort: false,
        hovertemplate: '%{{label}}<br>cpu_sec=%{{value:.2f}}<br>cpu_human=%{{customdata}}<extra></extra>',
      }}], {{title: 'CPU consumers (top suite+chunk + other)', showlegend: false, margin: {{l: 20, r: 20, t: 50, b: 20}}}}, {{responsive: true}});
      Plotly.newPlot('ramPie', [{{
        type: 'pie',
        labels: data.pie_ram_labels,
        values: data.pie_ram_vals,
        customdata: data.pie_ram_vals.map(v => humanFromKb(v)),
        marker: {{colors: data.pie_ram_labels.map(lb => lb === 'other' ? '#bdbdbd' : (data.suite_color[suiteFromLabel(lb)] || '#999'))}},
        textinfo: 'percent',
        textposition: 'inside',
        automargin: true,
        sort: false,
        hovertemplate: '%{{label}}<br>ram_kb_sum=%{{value:.0f}} KB<br>ram_human=%{{customdata}}<extra></extra>',
      }}], {{title: 'RAM consumers (top suite+chunk + other)', showlegend: false, margin: {{l: 20, r: 20, t: 50, b: 20}}}}, {{responsive: true}});
      Plotly.newPlot('activePie', [{{
        type: 'pie',
        labels: data.pie_active_labels,
        values: data.pie_active_vals,
        customdata: data.pie_active_vals.map(v => humanFromSec(v)),
        marker: {{colors: data.pie_active_labels.map(lb => lb === 'other' ? '#bdbdbd' : (data.suite_color[suiteFromLabel(lb)] || '#999'))}},
        textinfo: 'percent',
        textposition: 'inside',
        automargin: true,
        sort: false,
        hovertemplate: '%{{label}}<br>active_time_sec=%{{value:.2f}}<br>active_human=%{{customdata}}<extra></extra>',
      }}], {{title: 'Active time share (suite+chunk, top + other)', showlegend: false, margin: {{l: 20, r: 20, t: 50, b: 20}}}}, {{responsive: true}});
    }}

    Plotly.newPlot('cpuPieSuite', [{{
      type: 'pie',
      labels: data.pie_cpu_suite_labels,
      values: data.pie_cpu_suite_vals,
      customdata: data.pie_cpu_suite_vals.map(v => humanFromSec(v)),
      marker: {{colors: data.pie_cpu_suite_labels.map(lb => lb === 'other' ? '#bdbdbd' : (data.suite_color[suiteFromLabel(lb)] || '#999'))}},
      textinfo: 'percent',
      textposition: 'inside',
      automargin: true,
      sort: false,
      hovertemplate: '%{{label}}<br>cpu_sec=%{{value:.2f}}<br>cpu_human=%{{customdata}}<extra></extra>',
    }}], {{title: 'CPU consumers by suite (top + other)', showlegend: false, margin: {{l: 20, r: 20, t: 50, b: 20}}}}, {{responsive: true}});

    Plotly.newPlot('ramPieSuite', [{{
      type: 'pie',
      labels: data.pie_ram_suite_labels,
      values: data.pie_ram_suite_vals,
      customdata: data.pie_ram_suite_vals.map(v => humanFromKb(v)),
      marker: {{colors: data.pie_ram_suite_labels.map(lb => lb === 'other' ? '#bdbdbd' : (data.suite_color[suiteFromLabel(lb)] || '#999'))}},
      textinfo: 'percent',
      textposition: 'inside',
      automargin: true,
      sort: false,
      hovertemplate: '%{{label}}<br>ram_kb_sum=%{{value:.0f}} KB<br>ram_human=%{{customdata}}<extra></extra>',
    }}], {{title: 'RAM consumers by suite (top + other)', showlegend: false, margin: {{l: 20, r: 20, t: 50, b: 20}}}}, {{responsive: true}});

    Plotly.newPlot('activePieSuite', [{{
      type: 'pie',
      labels: data.pie_active_suite_labels,
      values: data.pie_active_suite_vals,
      customdata: data.pie_active_suite_vals.map(v => humanFromSec(v)),
      marker: {{colors: data.pie_active_suite_labels.map(lb => lb === 'other' ? '#bdbdbd' : (data.suite_color[suiteFromLabel(lb)] || '#999'))}},
      textinfo: 'percent',
      textposition: 'inside',
      automargin: true,
      sort: false,
      hovertemplate: '%{{label}}<br>active_time_sec=%{{value:.2f}}<br>active_human=%{{customdata}}<extra></extra>',
    }}], {{title: 'Active time share by suite (top + other)', showlegend: false, margin: {{l: 20, r: 20, t: 50, b: 20}}}}, {{responsive: true}});

    if (data.resources_overlay) {{
      document.getElementById('layerTogglesWrap').style.display = '';
    }}
    function applyLayerToggles() {{
      if (!data.resources_overlay) return;
      const bySuite = document.getElementById('layerBySuite')?.checked ?? true;
      const systemCpu = document.getElementById('layerSystemCpu')?.checked ?? true;
      const systemRam = document.getElementById('layerSystemRam')?.checked ?? true;
      const CPU_OVERLAY_NAMES = ['CPU total (monitor)', 'CPU total (monitor) outline'];
      const RAM_OVERLAY_NAMES = ['RAM total (monitor)', 'RAM total (monitor) outline'];
      function setCpuVisibility(el) {{
        if (!el || !el.data || !window.Plotly) return;
        const vis = el.data.map((tr) => {{
          if (tr && CPU_OVERLAY_NAMES.includes(tr.name)) return systemCpu;
          return bySuite;
        }});
        Plotly.restyle(el, {{ visible: vis }}, [...Array(el.data.length).keys()]);
      }}
      function setRamVisibility(el) {{
        if (!el || !el.data || !window.Plotly) return;
        const vis = el.data.map((tr) => {{
          if (tr && RAM_OVERLAY_NAMES.includes(tr.name)) return systemRam;
          return bySuite;
        }});
        Plotly.restyle(el, {{ visible: vis }}, [...Array(el.data.length).keys()]);
      }}
      setCpuVisibility(document.getElementById('cpuLayerSuite'));
      setRamVisibility(document.getElementById('ramLayerSuite'));
      if (data.by_chunk) {{
        setCpuVisibility(document.getElementById('cpuLayer'));
        setRamVisibility(document.getElementById('ramLayer'));
      }}
    }}
    window.applyLayerToggles = applyLayerToggles;

    const suiteSearchEl = document.getElementById('suiteSearch');
    if (suiteSearchEl) {{
      suiteSearchEl.addEventListener('input', applySuiteSearch);
    }}
    applySuiteSearch();
  </script>
</body>
</html>
"""
    out_html.write_text(html, encoding="utf-8")
