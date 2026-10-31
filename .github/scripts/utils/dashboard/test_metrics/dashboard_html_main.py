"""HTML dashboard rendering for tests resource dashboard.

Contains build_html_dashboard which takes a pre-computed payload and writes
the self-contained HTML file with Plotly.js charts, CPU suggestions table
and ya.make update script generator.
"""

from __future__ import annotations

import html as html_lib
from pathlib import Path
from typing import Any, Optional

try:
    from .dashboard_html_payload import build_dashboard_payload
    from .html_embed import js_script_json
except ImportError:
    from dashboard_html_payload import build_dashboard_payload  # type: ignore[no-redef]
    from html_embed import js_script_json  # type: ignore[no-redef]


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
    tests_per_suite: Optional[dict[str, int]] = None,
    tests_per_chunk_by_label: Optional[dict[str, int]] = None,
    issues_summary: Optional[dict[str, int]] = None,
    suite_chunk_issues_summary: Optional[dict[str, int]] = None,
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
        tests_per_suite=tests_per_suite,
        tests_per_chunk_by_label=tests_per_chunk_by_label,
        issues_summary=issues_summary,
        suite_chunk_issues_summary=suite_chunk_issues_summary,
    )

    rf = (run_config or {}).get("runner_footprint") or {}
    rf_vcpu = rf.get("vcpu", "?")
    rf_ram = rf.get("ram_gb", "?")
    rf_budget = rf.get("mem_budget_gb", "?")
    rf_ya_mem = rf.get("ya_make_mem_limit_gb", "?")
    payload_js = js_script_json(payload)
    suite_filter_html = html_lib.escape(suite_filter or "ALL SUITES", quote=True)

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
    .measured-body {{ display: flex; gap: 16px; align-items: flex-start; }}
    .measured-nav {{ flex: 0 0 460px; min-width: 0; }}
    .measured-nav input[type="search"] {{ width: 100%; box-sizing: border-box; margin-bottom: 8px; padding: 6px 8px; border: 1px solid #d0d7de; border-radius: 6px; }}
    .measured-test-hits {{ max-height: 180px; overflow: auto; margin: 0 0 8px 0; padding: 0; list-style: none; font-size: 12px; border: 1px solid #d0d7de; border-radius: 6px; background: #fff; }}
    .measured-test-hits li {{ padding: 6px 8px; cursor: pointer; border-bottom: 1px solid #f0f2f4; }}
    .measured-test-hits li:hover, .measured-test-hits li.on {{ background: #e7f3ff; }}
    .measured-test-hits .hit-name {{ word-break: break-word; font-family: ui-monospace, SFMono-Regular, Menlo, monospace; }}
    .measured-test-hits .hit-chunk {{ color: #6b7280; }}
    .measured-tree {{ max-height: 720px; overflow: auto; border: 1px solid #d0d7de; border-radius: 8px; background: #fafbfc; font-size: 12px; }}
    .measured-tree button {{ border: 0; background: transparent; cursor: pointer; font: inherit; text-align: left; }}
    .measured-tree .mt-row {{ display: flex; align-items: flex-start; gap: 2px; }}
    .measured-tree .mt-twist, .measured-tree .mt-spacer {{ flex: 0 0 16px; width: 16px; padding: 4px 0; color: #57606a; }}
    .measured-tree .mt-pick {{ flex: 1; min-width: 0; padding: 3px 6px; border-radius: 4px; }}
    .measured-tree .mt-pick:hover {{ background: #eef2f6; }}
    .measured-tree .mt-pick.on {{ background: #e7f3ff; }}
    .measured-tree .mt-name {{ font-weight: 600; }}
    .measured-tree .mt-chunk .mt-name {{ font-weight: 400; }}
    .measured-tree .stats {{ display: block; color: #6b7280; font-weight: 400; font-variant-numeric: tabular-nums; }}
    .measured-tree .outcomes {{ margin-top: 1px; display: flex; flex-wrap: wrap; gap: 4px; align-items: center; }}
    .measured-tree .stat-max {{ color: #9a3412; font-weight: 700; background: #ffedd5; padding: 0 3px; border-radius: 3px; }}
    .measured-tree .st, #measuredMeta .st {{ display: inline-block; padding: 0 5px; border-radius: 10px; font-size: 10px; font-weight: 700; letter-spacing: 0.02em; }}
    .measured-tree .st-pass, #measuredMeta .st-pass {{ color: #166534; background: #dcfce7; }}
    .measured-tree .st-fail, #measuredMeta .st-fail {{ color: #fff; background: #dc2626; }}
    .measured-tree .st-mute, #measuredMeta .st-mute {{ color: #fff; background: #7c3aed; }}
    .measured-tree .st-skip, #measuredMeta .st-skip {{ color: #fff; background: #6b7280; }}
    .measured-tree .st-nl, #measuredMeta .st-nl {{ color: #fff; background: #6c757d; }}
    .measured-tree .st-other, #measuredMeta .st-other {{ color: #374151; background: #e5e7eb; }}
    .measured-tree .odur, #measuredMeta .odur {{ color: #6b7280; font-variant-numeric: tabular-nums; }}
    .measured-charts {{ flex: 1; min-width: 0; }}
    .measured-test-list {{ margin: 0 0 10px 0; padding: 0; list-style: none; font-size: 12px; line-height: 1.45; }}
    .measured-test-list li {{ display: flex; gap: 8px; align-items: flex-start; padding: 3px 6px; border-radius: 4px; }}
    .measured-test-list li.on {{ background: #e7f3ff; }}
    .measured-test-list .swatch {{ flex: 0 0 10px; width: 10px; height: 10px; margin-top: 5px; border-radius: 2px; }}
    .measured-test-list .tbody {{ flex: 1; min-width: 0; }}
    .measured-test-list .tline {{ display: flex; flex-wrap: wrap; gap: 6px; align-items: baseline; }}
    .measured-test-list .tname {{ min-width: 0; word-break: break-word; font-family: ui-monospace, SFMono-Regular, Menlo, monospace; }}
    .measured-test-list .tmeta {{ flex: 0 0 auto; color: #6b7280; font-variant-numeric: tabular-nums; white-space: nowrap; }}
    .measured-test-list .tlogs {{ display: flex; flex-wrap: wrap; gap: 8px; margin-top: 2px; }}
    .measured-test-list .tlogs a {{ color: #0969da; text-decoration: none; }}
    .measured-test-list .tlogs a:hover {{ text-decoration: underline; }}
    .measured-test-list .st, .measured-test-list .ebadge {{ display: inline-block; padding: 0 6px; border-radius: 10px; font-size: 10px; font-weight: 700; letter-spacing: 0.02em; }}
    .measured-test-list .st-pass {{ color: #166534; background: #dcfce7; }}
    .measured-test-list .st-fail {{ color: #fff; background: #dc2626; }}
    .measured-test-list .st-mute {{ color: #fff; background: #7c3aed; }}
    .measured-test-list .st-skip {{ color: #fff; background: #6b7280; }}
    .measured-test-list .st-other {{ color: #374151; background: #e5e7eb; }}
    .measured-test-list .eb-TIMEOUT {{ color: #fff; background: #ff8c00; }}
    .measured-test-list .eb-VERIFY {{ color: #fff; background: #b22222; }}
    .measured-test-list .eb-SANITIZER {{ color: #fff; background: #ff4500; }}
    .measured-test-list .eb-XFAILED {{ color: #fff; background: #6f42c1; }}
    .measured-test-list .eb-POSSIBLE_OOM {{ color: #fff; background: #d63384; }}
    .measured-test-list .eb-NOT_LAUNCHED {{ color: #fff; background: #6c757d; }}
    .measured-test-list .eb-other {{ color: #fff; background: #475569; }}
    .row1 {{ display: grid; grid-template-columns: 1fr; gap: 16px; }}
    .chart {{ width: 100%; height: 420px; min-width: 0; }}
    .wide {{ width: 100%; height: 520px; }}
    .tz-hover .hoverlayer g.legend {{ display: none !important; }}
    #chartTip {{ position: fixed; z-index: 40; display: none; pointer-events: none; background: #fff; border: 1px solid #444; padding: 6px 8px; font: 12px Arial, sans-serif; line-height: 1.35; white-space: pre; box-shadow: 0 1px 4px rgba(0,0,0,0.12); }}
    .hoverbox {{ background: #f6f8fa; border: 1px solid #d0d7de; border-radius: 8px; padding: 8px 10px; margin: 6px 0 16px 0; max-height: 220px; overflow: auto; font-size: 12px; white-space: pre-wrap; font-variant-numeric: tabular-nums; }}
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
    .headline-stats {{
      margin-bottom: 12px;
      padding: 10px 12px;
      border: 1px solid #d0d7de;
      border-radius: 8px;
      background: #f8fafc;
    }}
    .headline-stats-title {{
      font-size: 13px;
      font-weight: 600;
      margin-bottom: 8px;
    }}
    .headline-run {{
      display: flex;
      flex-wrap: nowrap;
      align-items: center;
      gap: 6px 16px;
      font-size: 13px;
      line-height: 1.4;
      margin-bottom: 12px;
    }}
    .headline-run b {{ font-variant-numeric: tabular-nums; }}
    .headline-run .muted {{ color: #64748b; font-weight: 400; }}
    .headline-run .group {{ white-space: nowrap; }}
    .headline-run .group + .group {{
      padding-left: 16px;
      border-left: 1px solid #cbd5e1;
    }}
    .headline-tables {{
      display: flex;
      flex-wrap: wrap;
      gap: 16px 28px;
      align-items: flex-start;
    }}
    .headline-block {{
      margin-top: 0;
    }}
    .headline-block h4 {{
      margin: 0 0 4px;
      font-size: 12px;
      font-weight: 600;
      color: #475569;
    }}
    .headline-table {{
      width: auto;
      border-collapse: collapse;
      background: #fff;
      font-size: 12px;
      font-variant-numeric: tabular-nums;
    }}
    .headline-table th, .headline-table td {{
      border: 1px solid #e2e8f0;
      padding: 4px 10px;
      text-align: right;
      white-space: nowrap;
    }}
    .headline-table th:first-child, .headline-table td:first-child {{
      text-align: left;
      font-weight: 600;
    }}
    .headline-table th {{
      background: #fff;
      color: #64748b;
      font-weight: 600;
    }}
    .headline-hint {{
      position: relative;
      cursor: help;
      text-decoration: underline dotted;
      text-underline-offset: 2px;
      outline: none;
    }}
    .headline-hint:hover::after, .headline-hint:focus::after {{
      content: attr(data-tooltip);
      position: absolute;
      z-index: 10000;
      left: 0;
      top: calc(100% + 6px);
      width: 360px;
      padding: 8px 10px;
      border: 1px solid #94a3b8;
      border-radius: 6px;
      background: #0f172a;
      color: #fff;
      box-shadow: 0 6px 18px rgba(15, 23, 42, 0.25);
      white-space: normal;
      font-weight: 400;
      line-height: 1.35;
      text-align: left;
      pointer-events: none;
    }}
    .headline-card {{
      border: 1px solid #e2e8f0;
      border-radius: 8px;
      background: #fff;
      padding: 8px 10px;
      font-size: 12px;
      line-height: 1.45;
    }}
    .headline-card-name {{
      color: #334155;
      font-weight: 600;
      margin-bottom: 4px;
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
    .layer-toggle-box {{
      display: inline-flex;
      align-items: flex-start;
      gap: 10px;
    }}
    .layer-toggle-list {{
      display: flex;
      flex-direction: column;
      gap: 6px;
    }}
    .layer-toggle-item {{
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 10px;
      min-width: 160px;
      white-space: nowrap;
    }}
    .layer-toggle-item input {{
      margin: 0;
      flex: 0 0 auto;
    }}
  </style>
</head>
<body>
  <div id="chartTip"></div>
  <div id="dashboardHeader" style="margin-bottom:16px;padding:12px 16px;background:linear-gradient(135deg,#f8fafc 0%,#f1f5f9 100%);border-radius:8px;border:1px solid #e2e8f0;">
    <div id="headerRow1" style="display:flex;flex-wrap:wrap;align-items:center;gap:12px 20px;font-size:14px;line-height:1.5;">
      <span id="headerPr" style="display:none;"></span>
      <span id="headerBranch" style="display:none;"></span>
      <span id="headerCommit" style="display:none;font-family:ui-monospace,monospace;font-size:13px;"></span>
      <span style="flex:1;min-width:80px;"></span>
      <span id="headerLinks" style="display:flex;align-items:center;gap:8px;flex-wrap:wrap;"></span>
    </div>
    <div style="margin-top:8px;font-size:13px;color:#64748b;">
      Suite filter: {suite_filter_html}
    </div>
    <div id="overlayStatus" style="margin-top:4px;font-size:12px;"></div>
  </div>
  <div id="headlineStats" class="headline-stats" style="display:none;">
    <div class="headline-stats-title">Resource summary</div>
    <div id="headlineStatsGrid" class="headline-stats-grid"></div>
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
        <li><b>recommended_cpu</b> is p95 rounded to runner tiers: <code>1/2/4/8/16</code>. It reflects <b>only observed per-chunk core usage</b>. <code>REQUIREMENTS(cpu)</code> is a per-chunk scheduler slot (reserved cores that cap how many chunks run in parallel), not a request for more raw CPU.</li>
        <li>Long test duration and timeouts drive <b>recommended_split</b>, <b>not</b> CPU. A single long test at/above the size threshold (<b>SMALL=60s, MEDIUM=600s, LARGE=3600s</b>) is flagged as a split hint only; it no longer bumps the CPU tier.</li>
        <li>SIZE still caps recommended CPU (SMALL=1, MEDIUM=4). When CLI flag <code>--maximize-reqs-for-timeout-tests</code> is enabled, suites crossing the duration threshold additionally use size max: <b>SMALL=1, MEDIUM=4, LARGE=4</b>.</li>
        <li><b>recommended_split</b> uses per-<b>chunk</b> load: sum test durations inside each chunk. Overloaded chunks (<code>sum &gt; size_timeout*0.98</code>) are detected <b>even without a timeout</b> (<code>split_severity=at_risk</code>); with a timeout it is <code>timeout</code>. Required chunk count is <code>ceil(overloaded_total / (size_timeout*0.5))</code>, and a single very heavy chunk is split into <code>ceil(max_chunk_load / (size_timeout*0.5))</code> pieces. If a <b>single test</b> alone is at/above the budget (<code>single_test_blocks_split</code>), SPLIT_FACTOR cannot help. In that case, if the test is only marginally over budget <b>and the runner was CPU-saturated</b> during the suite window (<code>single_test_contention_suspected</code>, from <code>resources_monitor.jsonl</code>), it is likely contention-inflated — TRY raising <code>REQUIREMENTS(cpu)</code> (fewer parallel chunks → decontention) before a test-level fix; otherwise (genuinely long) the fix is <code>TIMEOUT()</code> / reduce work / mute.</li>
        <li><b>cpu_action</b> compares recommended cpu to <code>ya_cpu</code>. A <b>lower</b> suggestion is suppressed (kept <b>ok</b>) while the suite is under split/time pressure, because a smaller cpu slot increases parallel chunks and can worsen timeouts/OOM.</li>
        <li><b>memory → cpu</b>: on a single runner the local <code>ya make -t</code> scheduler caps parallelism <b>only</b> by cpu (sum of <code>REQUIREMENTS(cpu)</code> ≤ cores). <code>REQUIREMENTS(ram)</code> is <b>not honored locally</b>, so it is never recommended. Instead <b>max_chunk_ram</b> (physical peak of the heaviest chunk) is turned into <b>cpu_for_memory</b>: allow at most <code>floor(mem_budget / max_chunk_ram)</code> parallel chunks, i.e. <code>cpu ≈ ceil(cores / that)</code> (provisioned runner ≈ <b>{rf_vcpu} cores / {rf_ram} GB</b>, per-suite budget ≈ <b>{rf_budget} GB</b>). Purple dashed lines on system charts mark provisioned limits from <code>.github/config/runners_footprints.yml</code>; the red monitor line is actual <code>/proc</code> usage (may be lower). When <code>cpu_for_memory &gt; p95 cpu</code>, <b>recommended_cpu</b> is raised to it (shown red) to prevent OOM.</li>
      </ol>
      <ul>
        <li><b>status chunks</b>: counters from report rows where <code>chunk=true</code>. <b>total</b> = all chunk rows, <b>passed</b> = status OK/PASS.</li>
        <li><b>status tests</b>: counters from report rows where <code>chunk=false</code> (regular tests, suite aggregate row excluded). <b>total</b> = all such rows, <b>passed</b> = status OK/PASS.</li>
        <li><b>timeouts</b>: <code>error_type == TIMEOUT</code> (fallback: status contains <code>timeout</code>).</li>
        <li><b>muted</b>: <code>muted/is_muted</code> or status <code>MUTE</code>.</li>
        <li><b>skipped</b>: status <code>SKIP</code> or <code>SKIPPED</code>.</li>
        <li><b>fails_total</b>: <code>errors + timeouts</code>.</li>
      </ul>
    </div>
    <p id="syntheticNote" style="display: none; font-size: 12px; color: #b8860b; margin: 4px 0;"></p>
    <div id="cpuSuggestionsTable" class="clickbox"></div>
    <details id="heavyTestsDetails" style="margin: 8px 0 12px 0;">
      <summary><b>Long/heavy tests near timeout</b></summary>
      <div id="cpuHeavyTestsTable" class="clickbox"></div>
    </details>
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
    <span id="syntheticCheckboxWrap" style="margin-left: 16px;" title="When enabled, suites without report CPU/RAM use ya.make REQUIREMENTS as estimated chart values.">
      <label style="display:flex;align-items:center;gap:6px;">
        <input type="checkbox" id="includeSyntheticCb" checked />
        <span>Include estimated (from ya.make) CPU/RAM</span>
      </label>
    </span>
    <span id="layerTogglesWrap" style="display:none;margin-left:16px;font-size:12px;border:1px solid #d0d7de;border-radius:8px;padding:6px 10px;background:#fff;">
      <span class="layer-toggle-box">
        <b>Layers:</b>
        <span class="layer-toggle-list">
          <label class="layer-toggle-item"><span>Metrics by suite</span><input type="checkbox" id="layerBySuite" checked onchange="applyLayerToggles()"></label>
          <label class="layer-toggle-item"><span>System CPU</span><input type="checkbox" id="layerSystemCpu" checked onchange="applyLayerToggles()"></label>
          <label class="layer-toggle-item"><span>System RAM</span><input type="checkbox" id="layerSystemRam" checked onchange="applyLayerToggles()"></label>
        </span>
      </span>
    </span>
  </div>
  <details class="metrics-help">
    <summary><b>How CPU/RAM chart metrics are calculated</b></summary>
    <div class="box">
      <ul>
        <li><b>By suites (stacked):</b> At time <code>t</code> we sum each active chunk contribution. For CPU it is <code>cpu_sec_report / duration_used_sec</code> (average cores for that run); for RAM it is the chunk memory metric from report converted to GB. This is a model by chunks, not an instantaneous per-process sample.</li>
        <li><b>Total (monitor, red line):</b> Absolute system CPU (cores) and RAM (GB) from <code>/proc</code> during the run (actual usage).</li>
        <li><b>Measured per test:</b> stacked layers from <code>ya-test-*</code> cgroups (CPU/RAM/disk every 3s), plus cgroup CPU throttling, CPU/memory/I/O PSI, and memory events. Select a chunk to align these signals with its run. The host chart shows iowait, PSI, disk busy time, and in-flight I/O. These fields are interval deltas; older logs omit unavailable signals.</li>
        <li><b>Limits (purple dashed):</b> Provisioned runner maximum from <code>runners_footprints.yml</code> for this build preset. Optional dotted line: ya.make cgroup RAM cap (~{rf_ya_mem} GB).</li>
        <li><b>Chunk duration:</b> <code>duration_report_sec</code> from report; <code>duration_evlog_sec</code> from evlog B/E. For CPU/cores we use <code>duration_used_sec = max(report, evlog)</code>.</li>
        <li><b>CPU time per chunk (report):</b> <code>cpu_sec_report = ru_utime + ru_stime</code> from report metrics (seconds).</li>
        <li><b>CPU shown on charts (cores_est):</b> <code>cores_est = cpu_sec_report / duration_used_sec</code> (duration_used_sec = max(report, evlog)). Chart value at time <code>t</code> is the sum of active chunks at <code>t</code>.</li>
        <li><b>RAM per chunk (report):</b> <code>ram_kb_report = max(0, suite_max_proc_tree_memory_consumption_kb − suite_initial_maxrss_(kb))</code> when both suite metrics exist (memory attributed to the test run); else <code>suite_max_proc_tree_memory_consumption_kb</code>; else <code>ru_maxrss</code>; fallback <code>ru_rss / 1024</code>.</li>
        <li><b>RAM shown on charts:</b> <code>ram_gb = ram_kb_report / (1024 * 1024)</code>. Chart value at time <code>t</code> is the sum of active chunks at <code>t</code>.</li>
        <li><b>Estimated mode (from ya.make):</b> if report metrics are missing and checkbox is enabled, script uses <code>REQUIREMENTS(cpu:X ram:Y)</code>: <code>cpu_sec_report = X * evlog_dur_sec</code>, <code>ram_kb_report = Y * 1024 * 1024</code>.</li>
        <li><b>Tests at cursor time:</b> in click tables, <code>tests</code> means tests in chunks that are active at that cursor moment (not total tests in suite).</li>
        <li><b>SPLIT_FACTOR:</b> <code>ya_split_factor</code> is the <b>effective</b> split from ya.make (with <code>FORK_TEST_FILES</code>: <code>TEST_SRCS count × SPLIT_FACTOR(N)</code>). Column <code>chunks</code> is real runtime chunk count. Hover cells for details. Highlight on <code>chunks</code> means mismatch with ya.make.</li>
        <li><b>Recommended split formula:</b>
          let <code>T = size_timeout_sec</code> (from SIZE), <code>B = 0.98 * T</code> (chunk timeout budget),
          and <code>L = 0.5 * T</code> (target chunk load). For each chunk we compute
          <code>sum_duration_sec(chunk)</code> from test durations in that chunk.
          Overloaded chunks are those with <code>sum_duration_sec &gt; B</code>.
          Then:
          <code>overloaded_total = Σ sum_duration_sec(overloaded chunks)</code>,
          <code>needed = ceil(overloaded_total / L)</code>,
          <code>extra = max(0, needed - overloaded_chunks_count)</code>.
          A single heavy chunk also forces <code>single_chunk_extra = ceil(max_chunk_load / L) - 1</code>,
          and <code>recommended_split = chunks_real + max(extra, single_chunk_extra)</code>.
          Overloaded chunks are detected regardless of timeouts: <code>split_severity</code> is
          <code>at_risk</code> when a chunk is over budget without a timeout, <code>timeout</code> once a
          timeout occurs. If no chunk exceeds the budget, split is not raised.
        </li>
      </ul>
      <details style="margin-top:8px;">
        <summary><b>Detailed CPU calculation (why chart value ≠ total_cpu_sec / total_dur_sec)</b></summary>
        <div style="margin:6px 0;">
          <p><b>Per run</b> (we use <code>dur_sec = max(duration_report_sec, duration_evlog_sec)</code> so cores are not inflated when evlog span is short):</p>
          <p><code>cpu_cores_est<sub>run</sub> = cpu_sec_report / dur_sec</code></p>
          <p><b>Chart value at time t</b> (we use all runs, not one chunk):</p>
          <p><code>value(t) = Σ cpu_cores_est<sub>run</sub></code> over all runs of this suite where <code>start_us ≤ t &lt; end_us</code>.</p>
          <p>So at each moment we sum estimated cores of every run that is active. If a suite has many parallel runs (e.g. part0..part19), the chart shows the sum of all of them at that time.</p>
          <p><b>Table columns total_cpu_sec / total_dur_sec</b> are from the <i>deduplicated</i> set (one run per suite+chunk for recommendations). So e.g. total_cpu_sec=89.4 and total_dur_sec=135.3 refer to one representative run; 89.4/135.3 ≈ 0.66 cores is the average for that run. The chart value (e.g. 373 cores) is <i>not</i> 89.4/135.3 — it is the sum of <code>cpu_sec_report/dur_sec</code> over all runs of the suite active at the cursor time (e.g. 40 runs × ~9.3 cores ≈ 373).</p>
        </div>
      </details>
    </div>
  </details>
  <div id="measuredWrap" style="display:none;margin:12px 0 16px 0;padding:10px 12px;border:1px solid #d0d7de;border-radius:8px;background:#fff;">
    <div style="display:flex;gap:12px;align-items:center;flex-wrap:wrap;margin-bottom:8px;">
      <b>Measured per test (cgroup layers)</b>
      <span id="measuredMeta" class="muted"></span>
    </div>
    <div class="measured-body">
      <div class="measured-nav">
        <input id="measuredTestSearch" type="search" placeholder="Find test by name…" />
        <ol id="measuredTestHits" class="measured-test-hits" style="display:none;"></ol>
        <div id="measuredTree" class="measured-tree"></div>
      </div>
      <div class="measured-charts">
        <div id="measuredTestsLane" class="wide" style="height:90px;display:none;"></div>
        <ol id="measuredTestsList" class="measured-test-list" style="display:none;"></ol>
        <div id="measuredCpu" class="wide" style="height:280px;"></div>
        <div id="measuredCpuHover" class="hoverbox">Hover CPU layers to see sorted contributors</div>
        <div id="measuredRam" class="wide" style="height:280px;"></div>
        <div id="measuredRamHover" class="hoverbox">Hover RAM layers to see sorted contributors</div>
        <div id="measuredWrite" class="wide" style="height:280px;"></div>
        <div id="measuredWriteHover" class="hoverbox">Hover write layers to see sorted contributors</div>
        <div id="measuredRead" class="wide" style="height:280px;"></div>
        <div id="measuredReadHover" class="hoverbox">Hover read layers to see sorted contributors</div>
        <div id="measuredPressure" class="wide" style="height:320px;display:none;"></div>
        <div id="measuredPressureHover" class="hoverbox">Hover to inspect per-chunk throttling and PSI</div>
      </div>
    </div>
  </div>
  <div id="activeChunks" class="wide"></div>
  <div id="activeChunksClick" class="clickbox">Click chart to see how many tests (chunks) were running at that time</div>
  <div id="hostContentionChart" class="wide" style="display:none;height:320px;"></div>
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
    <div id="testsLayerSuite" class="wide"></div>
    <div id="testsHoverSuite" class="hoverbox">Hover tests chart to see sorted contributors</div>
    <div id="testsClickSuite" class="clickbox">Click tests chart to pin a time and show sorted table</div>
    <div id="cpuLayerSuite" class="wide"></div>
    <div id="cpuHoverSuite" class="hoverbox">Hover CPU chart to see sorted contributors</div>
    <div id="cpuClickSuite" class="clickbox">Click CPU chart to pin a time and show sorted table</div>
    <details class="metrics-help" style="font-size:11px;color:#6b7280;margin:4px 0 0 0;">
      <summary><b>How the CPU value at cursor is calculated</b></summary>
      <div style="margin:4px 0;">
        <p><b>Per run:</b> <code>cpu_cores_est = cpu_sec_report / dur_sec</code> (we have real report for every run).</p>
        <p><b>At time t:</b> value = Σ <code>cpu_cores_est</code> over all runs of this suite with <code>start_us ≤ t &lt; end_us</code>. We do not use one chunk — we sum over all active runs.</p>
        <p><b>Why value (e.g. 373) ≠ total_cpu_sec/total_dur_sec (e.g. 89.4/135.3):</b> The table totals are from the deduplicated run (one row per suite); the chart value is the instantaneous sum over all runs active at that moment (e.g. many parts in parallel).</p>
      </div>
    </details>
    <div id="ramLayerSuite" class="wide"></div>
    <div id="ramHoverSuite" class="hoverbox">Hover RAM chart to see sorted contributors</div>
    <div id="ramClickSuite" class="clickbox">Click RAM chart to pin a time and show sorted table</div>
    <div id="diskLayerSuite" class="wide" style="display:none;"></div>
    <div class="row">
      <div id="cpuPieSuite" class="chart"></div>
      <div id="ramPieSuite" class="chart"></div>
    </div>
    <div class="row">
      <div id="activePieSuite" class="chart"></div>
      <div id="testsPieSuite" class="chart"></div>
    </div>
  </div>
  <script>
    function showTab(id) {{
      const tabs = ['chunkTab', 'suiteTab'];
      tabs.forEach(t => {{
        const el = document.getElementById(t);
        const on = t === id;
        el.classList.toggle('active', on);
        el.style.display = on ? 'block' : 'none';
      }});
      document.getElementById('tabBtnChunk').classList.toggle('active', id === 'chunkTab');
      document.getElementById('tabBtnSuite').classList.toggle('active', id === 'suiteTab');
      // Hidden-tab Plotly charts need explicit resize after becoming visible.
      setTimeout(() => {{
        const ids = id === 'suiteTab'
          ? ['activeLayerSuite', 'testsLayerSuite', 'cpuLayerSuite', 'ramLayerSuite', 'diskLayerSuite', 'cpuPieSuite', 'ramPieSuite', 'activePieSuite', 'testsPieSuite', 'activeChunks']
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

    const data = {payload_js};
    const UTC_OFFSET_SEC = Number(data.utc_offset_sec || 0);
    const _fmtCache = {{}};
    function formatHeadlineNumber(metric, value) {{
      const n = Number(value);
      if (!Number.isFinite(n)) return '—';
      if (metric === 'cpu' || metric === 'ram' || metric === 'disk' || metric === 'pct' || metric === 'queue') return n.toFixed(1);
      return String(Math.round(n));
    }}
    function formatHeadlineValue(metric, value) {{
      const n = Number(value);
      if (!Number.isFinite(n)) return '—';
      if (metric === 'cpu') return n.toFixed(3) + ' cores';
      if (metric === 'cpu_host') return n.toFixed(1) + ' cores';
      if (metric === 'ram') return n.toFixed(3) + ' GB';
      if (metric === 'ram_host') return n.toFixed(1) + ' GB';
      if (metric === 'disk') return n.toFixed(1) + ' MB/s';
      if (metric === 'total_duration_sec') {{
        const sec = Math.max(0, Math.round(n));
        const h = Math.floor(sec / 3600);
        const m = Math.floor((sec % 3600) / 60);
        const s = sec % 60;
        const hh = String(h).padStart(2, '0');
        const mm = String(m).padStart(2, '0');
        const ss = String(s).padStart(2, '0');
        return hh + ':' + mm + ':' + ss + ' (' + sec + 's)';
      }}
      return String(Math.round(n));
    }}
    function renderHeadlineStats() {{
      const wrap = document.getElementById('headlineStats');
      const grid = document.getElementById('headlineStatsGrid');
      const hs = data.headline_stats || {{}};
      if (!wrap || !grid || !hs || typeof hs !== 'object') return;
      const total = hs.total || {{}};
      const issues = hs.issues || {{}};
      const chunkIssues = hs.suite_chunk_issues || {{}};
      const timeoutTotal = Math.round(Number(issues.timeout || 0));
      const timeoutMuted = Math.round(Number(issues.timeout_muted || 0));
      const regularBase = Math.round(Number(issues.regular || 0));
      const regularMutedBase = Math.round(Number(issues.regular_muted || 0));
      const mutedTotal = Math.round(Number(issues.muted || 0));
      const mutedOther = Math.max(0, mutedTotal - timeoutMuted - regularMutedBase);
      // Include muted-only tests into "regular" bucket so totals remain consistent.
      const regularTotal = regularBase + mutedOther;
      const regularMuted = regularMutedBase + mutedOther;
      const skippedTotal = Math.round(Number(issues.skipped || 0));
      const failedTotal = timeoutTotal + regularTotal;
      const failedMuted = timeoutMuted + regularMuted;
      const cTimeoutTotal = Math.round(Number(chunkIssues.timeout || 0));
      const cTimeoutMuted = Math.round(Number(chunkIssues.timeout_muted || 0));
      const cRegularBase = Math.round(Number(chunkIssues.regular || 0));
      const cRegularMutedBase = Math.round(Number(chunkIssues.regular_muted || 0));
      const cMutedTotal = Math.round(Number(chunkIssues.muted || 0));
      const cMutedOther = Math.max(0, cMutedTotal - cTimeoutMuted - cRegularMutedBase);
      const cRegularTotal = cRegularBase + cMutedOther;
      const cRegularMuted = cRegularMutedBase + cMutedOther;
      const cFailedTotal = cTimeoutTotal + cRegularTotal;
      const cFailedMuted = cTimeoutMuted + cRegularMuted;
      const wallExact = formatHeadlineValue('total_duration_sec', hs.total_duration_sec).split(' (')[0];
      const runLine =
        '<div class="headline-run">' +
          '<span class="group"><b>' + String(Math.round(Number(total.suites || 0))) + '</b> suites</span>' +
          '<span class="group"><b>' + String(Math.round(Number(total.chunks || 0))) + '</b> chunks</span>' +
          '<span class="group"><b>' + String(Math.round(Number(total.tests || 0))) + '</b> tests</span>' +
          '<span class="group" title="From the first test start to the last test end">' +
            '<b>' + wallExact + '</b> total duration</span>' +
          '<span class="group">tests <b>' + String(failedTotal) + '</b> failed <span class="muted">(' + String(failedMuted) + ' muted)</span>' +
            ' · <b>' + String(timeoutTotal) + '</b> timeout · <b>' + String(skippedTotal) + '</b> skipped</span>' +
          '<span class="group">chunks <b>' + String(cFailedTotal) + '</b> failed <span class="muted">(' + String(cFailedMuted) + ' muted)</span>' +
            ' · <b>' + String(cTimeoutTotal) + '</b> timeout</span>' +
        '</div>';
      function statCell(metric, stats, field) {{
        const s = (stats && typeof stats === 'object') ? stats : {{}};
        return '<td>' + formatHeadlineNumber(metric, s[field]) + '</td>';
      }}
      function statTable(rows, withLimit) {{
        const body = rows.map((row) => {{
          const label = row[0];
          const metric = row[1];
          const stats = row[2];
          const limit = row[3];
          const hint = row[4];
          const labelHtml = hint
            ? '<span class="headline-hint" tabindex="0" data-tooltip="' + escapeHtml(hint) + '">' + label + '</span>'
            : label;
          return '<tr><td>' + labelHtml + '</td>' +
            (withLimit ? ('<td>' + (limit == null || limit === '' ? '—' : limit) + '</td>') : '') +
            statCell(metric, stats, 'max') +
            statCell(metric, stats, 'p95') +
            statCell(metric, stats, 'median') +
            statCell(metric, stats, 'avg') +
          '</tr>';
        }}).join('');
        return (
          '<table class="headline-table"><thead><tr>' +
            '<th></th>' +
            (withLimit ? '<th title="CPU cores and MemTotal from /proc on this host. Disk rows are throughput, so they have no capacity limit.">limit</th>' : '') +
            '<th>max</th><th>p95</th><th>median</th><th>avg</th>' +
          '</tr></thead><tbody>' + body + '</tbody></table>'
        );
      }}
      function hostCapacity(kind) {{
        const ro = data.resources_overlay || {{}};
        const lim = ro.runner_limits || {{}};
        const measured = ro.measured || {{}};
        const live = kind === 'cpu'
          ? (measured.cpu_cores || ro.cpu_cores)
          : (measured.ram_gb || ro.ram_total_gb);
        const fallback = kind === 'cpu' ? lim.cpu_cores_max : lim.ram_gb_max;
        const n = Number(live != null && live !== '' ? live : fallback);
        if (!Number.isFinite(n) || n <= 0) return null;
        if (kind === 'cpu') return String(Math.round(n));
        return formatHeadlineNumber('ram', n);
      }}
      const testsTable = statTable([
        ['CPU, cores', 'cpu', hs.cpu],
        ['RAM, GB', 'ram', hs.ram],
        ['Active chunks', 'count', hs.active_chunks],
        ['Tests in parallel', 'count', hs.tests],
      ]);
      const cpuHost = hs.cpu_host;
      const ramHost = hs.ram_host;
      const diskRead = hs.disk_read_host;
      const diskWrite = hs.disk_write_host;
      let hostBlock = '';
      if (cpuHost || ramHost || diskRead || diskWrite) {{
        const samples = Math.round(Number((cpuHost || ramHost || diskRead || {{}}).samples || 0));
        const sampleNote = samples ? ' · ' + String(samples) + ' samples' : '';
        const hostRows = [
          ['CPU, cores', 'cpu', cpuHost, hostCapacity('cpu')],
          ['RAM, GB', 'ram', ramHost, hostCapacity('ram')],
          ['Disk read, MB/s', 'disk', diskRead, null],
          ['Disk write, MB/s', 'disk', diskWrite, null],
        ];
        hostBlock =
          '<div class="headline-block"><h4>Host · /proc' + sampleNote + '</h4>' +
          statTable(hostRows, true) +
          '</div>';
      }}
      const telemetryRows = [];
      [
          ['CPU iowait, %', 'pct', hs.cpu_iowait_pct, null,
            'Доля времени CPU, проведённая в ожидании завершения I/O. Устойчиво высокое значение означает, что вычисления простаивают из-за диска или другого блочного устройства.'],
          ['CPU PSI some, %', 'pct', hs.pressure_cpu_some_pct, null,
            'Доля времени, когда хотя бы одна задача была готова исполняться, но ждала CPU. Высокое значение означает конкуренцию за процессор.'],
          ['CPU PSI full, %', 'pct', hs.pressure_cpu_full_pct, null,
            'Доля времени полного CPU-стопа: все исполняемые задачи одновременно ждали CPU. Для CPU на Linux обычно отсутствует или равна нулю.'],
          ['Memory PSI some, %', 'pct', hs.pressure_memory_some_pct, null,
            'Доля времени, когда хотя бы одна задача тормозила из-за нехватки памяти, reclaim или swap. Рост указывает на давление по RAM.'],
          ['Memory PSI full, %', 'pct', hs.pressure_memory_full_pct, null,
            'Доля времени, когда все активные задачи одновременно были заблокированы из-за памяти. Ненулевое устойчивое значение означает сильное давление по RAM.'],
          ['I/O PSI some, %', 'pct', hs.pressure_io_some_pct, null,
            'Доля времени, когда хотя бы одна задача была заблокирована на I/O. Показывает влияние медленного диска даже при неполной загрузке всего раннера.'],
          ['I/O PSI full, %', 'pct', hs.pressure_io_full_pct, null,
            'Доля времени, когда все активные задачи одновременно ждали I/O. Высокое значение означает общий I/O-затор раннера.'],
          ['Disk busy, %', 'pct', hs.disk_busy_pct, null,
            'Доля интервала, когда диск обслуживал хотя бы один запрос. Значения около 100% длительное время означают насыщение устройства.'],
          ['Disk queue, requests', 'queue', hs.disk_io_in_progress, null,
            'Число I/O-запросов в работе на момент замера. Устойчивая растущая очередь вместе с высоким Disk busy и I/O PSI указывает на перегрузку диска.'],
      ].forEach(row => {{ if (row[2]) telemetryRows.push(row); }});
      const telemetryBlock = telemetryRows.length
        ? '<div class="headline-block"><h4 title="Метрики давления на ресурсы всего раннера. Наведите на название строки для объяснения." style="cursor:help;">Host · telemetry</h4>' +
          statTable(telemetryRows, false) +
          '</div>'
        : '';
      const cgroupRows = [
        ['CPU, cores', 'cpu', hs.cpu_cgroup],
        ['RAM, GB', 'ram', hs.ram_cgroup],
        ['Disk read, MB/s', 'disk', hs.disk_read_cgroup],
        ['Disk write, MB/s', 'disk', hs.disk_write_cgroup],
        ['Active cgroups', 'count', hs.active_cgroups],
      ].filter(row => row[2]);
      const cgroupSamples = cgroupRows.length
        ? Math.round(Number((cgroupRows[0][2] || {{}}).samples || 0))
        : 0;
      const cgroupBlock = cgroupRows.length
        ? '<div class="headline-block"><h4>Tests · cgroups' +
          (cgroupSamples ? ' · ' + String(cgroupSamples) + ' samples' : '') +
          '</h4>' +
          statTable(cgroupRows, false) +
          '</div>'
        : '';
      grid.innerHTML =
        runLine +
        '<div class="headline-tables">' +
          telemetryBlock +
          cgroupBlock +
        '</div>' +
        '<div class="headline-tables" style="margin-top:16px;">' +
          '<div class="headline-block"><h4>Tests · report</h4>' + testsTable + '</div>' +
          hostBlock +
        '</div>';
      wrap.style.display = 'block';
    }}
    renderHeadlineStats();
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
    function hoverInstantMs(point) {{
      if (!point) return null;
      const idx = Number(point.pointNumber);
      const xs = point.data && Array.isArray(point.data.x) ? point.data.x : null;
      if (xs && Number.isFinite(idx) && idx >= 0 && idx < xs.length) {{
        const fromTrace = _toMs(xs[Math.floor(idx)]);
        if (fromTrace != null) return fromTrace;
      }}
      const raw = String(point.x || '').trim();
      if (!raw) return null;
      if (raw.endsWith('Z') || raw.includes('T')) return _toMs(raw);
      return _toMs(raw.replace(' ', 'T') + 'Z');
    }}
    function formatPointTime(point) {{
      const ms = hoverInstantMs(point);
      if (ms == null) return formatTimeLabel(point && point.x);
      return _getFormatter(true).format(new Date(ms));
    }}
    function formatAbsSecLabel(sec) {{
      const n = Number(sec);
      if (!Number.isFinite(n)) return '';
      const iso = xToDisplay(n);
      let ms = Number(iso);
      if (!Number.isFinite(ms)) ms = Date.parse(String(iso));
      if (!Number.isFinite(ms)) return String(iso);
      return _getFormatter(false).format(new Date(ms));
    }}
    function _toMs(v) {{
      let n = Number(v);
      if (!Number.isFinite(n)) n = Date.parse(String(v));
      return Number.isFinite(n) ? n : null;
    }}
    function activeCountAtTime(tDisplay) {{
      const xs = data.xs_active;
      const ys = data.ys_active;
      if (!Array.isArray(xs) || !Array.isArray(ys) || xs.length === 0 || ys.length === 0) return null;
      const targetSec = xFromDisplay(tDisplay);
      if (!Number.isFinite(targetSec)) return null;
      // Step function: count at t is ys[i] for the last i where xs[i] <= t (not "nearest" point).
      let idx = -1;
      for (let i = 0; i < xs.length; i++) {{
        if (Number(xs[i]) <= targetSec) idx = i;
        else break;
      }}
      if (idx < 0) return 0;
      const v = Number(ys[idx]);
      return Number.isFinite(v) ? Math.round(v) : null;
    }}
    function sourceSecAtClick(plotId, point) {{
      if (!point) return null;
      const idx = Number(point.pointNumber);
      if (!Number.isFinite(idx) || idx < 0) return null;
      const i = Math.floor(idx);
      let xs = null;
      if (plotId === 'cpuLayerSuite') xs = data.xs_cpu_suite;
      else if (plotId === 'ramLayerSuite') xs = data.xs_ram_suite;
      else if (plotId === 'activeLayerSuite') xs = data.xs_active_suite;
      else if (plotId === 'testsLayerSuite') xs = data.xs_tests_suite;
      else if (plotId === 'cpuLayer') xs = data.xs_cpu;
      else if (plotId === 'ramLayer') xs = data.xs_ram;
      else if (plotId === 'activeLayerChunk') xs = data.xs_active_chunk;
      else if (plotId === 'activeChunks') xs = data.xs_active;
      if (!Array.isArray(xs) || i >= xs.length) return null;
      const tSec = Number(xs[i]);
      return Number.isFinite(tSec) ? tSec : null;
    }}
    function testsInActiveChunksAtTime(trackName, tDisplay, tSecOverride=null) {{
      if (typeof trackName !== 'string' || !trackName) return '';
      const tSec = Number.isFinite(Number(tSecOverride)) ? Number(tSecOverride) : xFromDisplay(tDisplay);
      if (!Number.isFinite(tSec)) return '';
      const chunkRuns = Array.isArray(data.chunk_runs_for_tests) ? data.chunk_runs_for_tests : [];
      if (chunkRuns.length === 0) return '';
      let total = 0;
      if (trackName.includes('::')) {{
        for (const run of chunkRuns) {{
          if (!run || run.chunk_label !== trackName) continue;
          const b = Number(run.start_sec);
          const e = Number(run.end_sec);
          if (!Number.isFinite(b) || !Number.isFinite(e) || !(b <= tSec && tSec < e)) continue;
          total += Number(run.tests || 0);
        }}
      }} else {{
        for (const run of chunkRuns) {{
          if (!run || run.suite !== trackName) continue;
          const b = Number(run.start_sec);
          const e = Number(run.end_sec);
          if (!Number.isFinite(b) || !Number.isFinite(e) || !(b <= tSec && tSec < e)) continue;
          total += Number(run.tests || 0);
        }}
      }}
      return total > 0 ? String(Math.round(total)) : '';
    }}
    function axisLayout(xsDisp) {{
      const msVals = Array.isArray(xsDisp) ? xsDisp.map(_toMs).filter(v => v != null) : [];
      let tickVals = null;
      let tickText = null;
      if (msVals.length) {{
        let minMs = Number.POSITIVE_INFINITY;
        let maxMs = Number.NEGATIVE_INFINITY;
        for (const v of msVals) {{
          if (v < minMs) minMs = v;
          if (v > maxMs) maxMs = v;
        }}
        const ticks = _buildTickVals(minMs, maxMs, 8);
        tickVals = ticks.map(ms => new Date(ms).toISOString());
        tickText = ticks.map(ms => _formatTick(ms));
      }}
      return {{
        title: axisTitle(),
        type: 'date',
        tickmode: tickVals ? 'array' : 'auto',
        tickvals: tickVals || undefined,
        ticktext: tickText || undefined,
        hoverformat: '',
      }};
    }}
    function applyTimezoneToAllCharts() {{
      const ids = ['activeChunks', 'activeLayerChunk', 'activeLayerSuite', 'testsLayerSuite', 'cpuLayer', 'ramLayer', 'cpuLayerSuite', 'ramLayerSuite', 'diskLayer', 'diskLayerSuite'];
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
          'xaxis.tickmode': ax.tickmode,
          'xaxis.tickvals': ax.tickvals,
          'xaxis.ticktext': ax.ticktext,
          'xaxis.hoverformat': '',
          'hoverdistance': -1,
          'spikedistance': -1,
        }});
      }}
      for (const id of ids) {{
        const el = document.getElementById(id);
        if (el && el.data && window.Plotly) Plotly.Plots.resize(el);
      }}
      applyMarkersToCharts();
      if (window.renderSuggestionsTable) {{
        window.renderSuggestionsTable();
      }}
    }}
    document.getElementById('stats').textContent = JSON.stringify(data.stats, null, 2);
    document.getElementById('runConfig').textContent = JSON.stringify(data.run_config || {{}}, null, 2);
    function escapeHtml(s) {{
      if (s == null || s === '') return '';
      const div = document.createElement('div');
      div.textContent = String(s);
      return div.innerHTML.replaceAll('"', '&quot;').replaceAll("'", '&#39;');
    }}
    function safeColor(c) {{
      return /^#[0-9a-fA-F]{{3,8}}$/.test(String(c || '')) ? String(c) : '#999';
    }}
    function safeHttpBase(raw) {{
      try {{
        const u = new URL(String(raw || ''));
        if (u.protocol !== 'http:' && u.protocol !== 'https:') return '';
        return u.href.endsWith('/') ? u.href.slice(0, -1) : u.href;
      }} catch (e) {{
        return '';
      }}
    }}
    const cfg = data.run_config || {{}};
    const pr = cfg.pr != null && String(cfg.pr).trim() !== '' ? String(cfg.pr) : null;
    const branch = cfg.branch != null && String(cfg.branch).trim() !== '' ? String(cfg.branch).trim() : null;
    const commit = cfg.commit != null && String(cfg.commit).trim() !== '' ? String(cfg.commit).trim() : null;
      const artifactsUrl = (cfg.artifacts_url || '').trim();
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
    const artifactsBase = safeHttpBase(artifactsUrl);
    if (artifactsBase) {{
      const a = document.createElement('a');
      a.href = artifactsBase + '/index.html';
      a.target = '_blank';
      a.rel = 'noopener';
      a.textContent = 'Artifacts';
      a.style.cssText = 'color:#2563eb;font-size:13px;text-decoration:none;';
      linksEl.appendChild(a);
      if (tryLinks.length) linksEl.appendChild(document.createTextNode(' · '));
    }}
    if (artifactsBase && tryLinks.length) {{
      tryLinks.forEach((path, i) => {{
        if (i > 0) linksEl.appendChild(document.createTextNode('\u00a0'));
        const a = document.createElement('a');
        a.href = artifactsBase + '/' + String(path || '').split('/').filter(Boolean).join('/');
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
          let n = Number(v);
          if (!Number.isFinite(n)) {{
            n = Number(xFromDisplay(v));
          }}
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
      wrap.style.display = 'block';
    }}
    setupMonitoringLink();

    // Marker state lives at top scope so all functions can access it.
    const _markerChartIds = ['activeChunks', 'activeLayerChunk', 'activeLayerSuite', 'testsLayerSuite', 'cpuLayer', 'ramLayer', 'cpuLayerSuite', 'ramLayerSuite', 'diskLayer', 'diskLayerSuite'];
    // Each entry: {{ shapes: [...], annotations: [...] }} by suite_path.
    const _suiteMarkerData = {{}};

    if (data.by_chunk) {{
      document.getElementById('tabsBar').style.display = 'flex';
      document.getElementById('chunkTab').style.display = 'block';
      document.getElementById('suiteTab').style.display = 'none';
    }} else {{
      document.getElementById('chunkTab').classList.remove('active');
      document.getElementById('chunkTab').style.display = 'none';
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
      let suggestionsSortCol = 25;  // test_fails_total
      let suggestionsSortAsc = false;
      // Heavy tests table default: suite -> test.
      let heavySortCol = 1;  // 1=suite_path, 2=ya_size, 3=threshold, 4=test, 5=duration, 6=chunk
      let heavySortAsc = true;

      function sortableValue(raw) {{
        const s = String(raw ?? '').replace(/<[^>]*>/g, '').replace(/[\\s,]+/g, ' ').trim();
        const n = Number(s.replace(/[^\\d.+-]/g, ''));
        return Number.isFinite(n) && s.match(/[-+]?\\d/) ? n : s.toLowerCase();
      }}

      const suggestionsColumns = [
        'idx', 'suite_path', 'ya_ram_gb', 'ya_cpu_cores', 'ya_size', 'ya_split_factor',
        'chunks_count', 'recommended_split', 'split_action', 'median_cores', 'p95_cores', 'total_cpu_sec',
        'total_ram_gb', 'total_dur_sec', 'total_dur_report_sec', 'total_dur_evlog_sec', 'recommended_cpu', 'cpu_action',
        'test_total', 'test_passed', 'test_errors', 'test_timeouts', 'test_muted', 'test_muted_timeouts', 'test_skipped', 'test_fails_total',
        'chunk_total', 'chunk_passed', 'errors', 'timeouts', 'muted', 'muted_timeouts', 'fails_total',
        'max_parallel_self', 'max_parallel_self_at_sec',
        'peak_others_during_suite', 'peak_others_during_suite_at_sec',
        'peak_self_cpu_cores_during_suite', 'peak_self_cpu_at_sec',
        'peak_self_ram_gb_during_suite', 'peak_self_ram_at_sec',
        'max_chunk_ram_gb', 'cpu_for_memory'
      ];

      function titleAttr(text) {{
        return escapeHtml(String(text || ''));
      }}

      function tipCell(text, tooltip, extraStyle) {{
        const t = escapeHtml(text == null ? '' : String(text));
        const tip = titleAttr(tooltip || '');
        if (!tip) return t;
        const style = 'cursor:help;text-decoration:underline dotted;' + (extraStyle || '');
        return '<span title="' + tip + '" style="' + style + '">' + t + '</span>';
      }}

      function actionCell(action, tooltip) {{
        const tip = tooltip ? ' title="' + titleAttr(tooltip) + '"' : '';
        if (action === 'raise') return '<span' + tip + ' style="color:#856404;background:#fff3cd;padding:2px 6px;border-radius:10px;cursor:help;">raise</span>';
        if (action === 'lower') return '<span' + tip + ' style="color:#0c5460;background:#d1ecf1;padding:2px 6px;border-radius:10px;cursor:help;">lower</span>';
        if (action === 'set') return '<span' + tip + ' style="color:#721c24;background:#f8d7da;padding:2px 6px;border-radius:10px;cursor:help;">set</span>';
        return '<span' + tip + ' style="color:#155724;background:#d4edda;padding:2px 6px;border-radius:10px;cursor:help;">ok</span>';
      }}

      function renderSuggestionsTable() {{
        const q = (document.getElementById('suiteSearch')?.value || '').trim().toLowerCase();
        const filtered = data.cpu_suggestions.filter(s => !q || String(s.suite_path || '').toLowerCase().includes(q));
        const rowsData = filtered.map((s, i) => {{
          const cpuReqText = String(s.recommended_cpu ?? 1);
          const cpuTooltip = s.recommended_cpu_tooltip || s.recommended_cpu_explain || '';
          const cpuBadge = cpuReqText === 'all'
            ? tipCell('cpu:all', cpuTooltip, 'display:inline-block;padding:2px 8px;border-radius:10px;background:#ffe8cc;color:#7c2d12;border:1px solid #fdba74;font-weight:700;text-decoration:none;')
            : tipCell('cpu:' + cpuReqText, cpuTooltip, 'font-weight:700;');
          const cpuCell = cpuBadge + (s.has_synthetic ? ' ' + tipCell('(estimated from ya.make)', 'CPU/RAM estimated from ya.make REQUIREMENTS when report metrics missing.', 'color:#b8860b;text-decoration:none;') : '');
          const durationBoost = Boolean(s.recommended_cpu_duration_boost);
          const sizeCapped = durationBoost && (s.recommended_cpu_small_cap_applied || s.recommended_cpu_medium_cap_applied);
          const maxTestDur = Number(s.max_test_duration_sec || 0);
          const durThreshold = Number(s.long_test_threshold_sec || 0);
          const sizeCapHint = sizeCapped
            ? ((s.recommended_cpu_small_cap_applied
                ? ('Longest test ' + maxTestDur.toFixed(0) + 's exceeds SMALL threshold ' + durThreshold + 's. CPU capped at 1 — consider SIZE(MEDIUM).')
                : ('Longest test ' + maxTestDur.toFixed(0) + 's exceeds MEDIUM threshold ' + durThreshold + 's. CPU capped at 4 — consider SIZE(LARGE).')))
            : (s.ya_size_tooltip || ('SIZE(' + (s.ya_size || '') + ') from ya.make: sets per-test timeout and CPU caps.'));
          const sizeCell = sizeCapped
            ? tipCell(String(s.ya_size || '') + ' ↑', sizeCapHint, 'display:inline-flex;align-items:center;gap:2px;color:#991b1b;font-weight:600;text-decoration:none;')
            : tipCell(String(s.ya_size ?? ''), sizeCapHint, '');
          const yaChunksCell = s.ya_split_factor != null
            ? tipCell(String(s.ya_split_factor), s.ya_split_factor_tooltip, '')
            : '';
          const realChunks = s.chunks_count ?? 0;
          const chunksMismatch = s.chunks_count_mismatch;
          const chunksTooltip = s.chunks_count_tooltip || (
            'Runtime chunks: ' + realChunks +
            (s.ya_split_factor != null ? '; ya.make effective: ' + s.ya_split_factor : '')
          );
          const chunksCell = tipCell(
            String(realChunks),
            chunksTooltip,
            chunksMismatch
              ? 'background:#fef3c7;color:#92400e;padding:2px 6px;border-radius:4px;border:1px solid #f59e0b;text-decoration:none;'
              : ''
          );
          const recSplit = Number(s.recommended_split ?? realChunks);
          const recSplitTooltip = s.recommended_split_tooltip || s.recommended_split_explain || '';
          const recSplitCell = tipCell(
            String(recSplit) + (recSplit > Number(realChunks) ? ' ↑' : ''),
            recSplitTooltip,
            recSplit > Number(realChunks) ? 'color:#991b1b;font-weight:700;text-decoration:none;' : ''
          );
          const testTip = s.test_status_tooltip || 'Test-level status counts from report.json.';
          const chunkTip = s.chunk_status_tooltip || 'Chunk-level status counts from report.json.';
          return [
            String(i + 1),
            '<label style="display:inline-flex;align-items:center;gap:5px;max-width:420px;">' +
            '<input type="checkbox" class="suite-marker-cb" style="flex-shrink:0;cursor:pointer;" data-suite="' + titleAttr(s.suite_path || '') + '" onchange="toggleSuiteMarkers(this.dataset.suite, this.checked)">' +
            tipCell(s.suite_path || '', s.suite_path_tooltip || s.suite_path, 'overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:380px;display:inline-block;') +
            '</label>',
            tipCell(String(s.ya_ram_gb ?? ''), s.ya_ram_gb_tooltip, ''),
            tipCell(String(s.ya_cpu_cores ?? ''), s.ya_cpu_cores_tooltip, ''),
            sizeCell,
            yaChunksCell,
            chunksCell,
            recSplitCell,
            actionCell(s.recommended_split_action || 'ok', s.split_action_tooltip),
            tipCell(s.median_cores != null ? Number(s.median_cores).toFixed(3) : '', s.median_cores_tooltip, ''),
            tipCell(s.p95_cores != null ? Number(s.p95_cores).toFixed(3) : '', s.p95_cores_tooltip, ''),
            tipCell(s.total_cpu_sec != null ? Number(s.total_cpu_sec).toFixed(1) : '', s.total_cpu_sec_tooltip, ''),
            tipCell(s.total_ram_gb != null ? Number(s.total_ram_gb).toFixed(3) : '', s.total_ram_gb_tooltip, ''),
            tipCell((s.total_dur_sec != null ? Number(s.total_dur_sec).toFixed(1) : '') + ' s', s.total_dur_sec_tooltip, ''),
            tipCell((s.total_dur_report_sec != null ? Number(s.total_dur_report_sec).toFixed(1) : '') + ' s', s.total_dur_report_sec_tooltip, ''),
            tipCell((s.total_dur_evlog_sec != null ? Number(s.total_dur_evlog_sec).toFixed(1) : '') + ' s', s.total_dur_evlog_sec_tooltip, ''),
            cpuCell,
            actionCell(s.cpu_action || 'ok', s.cpu_action_tooltip),
            tipCell(String(s.test_total ?? ''), testTip, ''),
            tipCell(String(s.test_passed ?? ''), testTip, ''),
            tipCell(String(s.test_errors || 0), testTip + ' errors = failed tests excluding timeouts.', ''),
            tipCell(String(s.test_timeouts || 0), testTip + ' timeouts often mean chunk overloaded or SIZE too small.', s.test_timeouts > 0 ? 'color:#991b1b;font-weight:700;text-decoration:none;' : ''),
            tipCell(String(s.test_muted || 0), testTip, ''),
            tipCell(String(s.test_muted_timeouts || 0), testTip, ''),
            tipCell(String(s.test_skipped || 0), testTip, ''),
            tipCell(String(s.test_fails_total || 0), testTip, ''),
            tipCell(String(s.chunk_total ?? ''), chunkTip, ''),
            tipCell(String(s.chunk_passed ?? ''), chunkTip, ''),
            tipCell(String(s.chunk_errors || 0), chunkTip, ''),
            tipCell(String(s.chunk_timeouts || 0), chunkTip + ' chunk timeout = whole chunk exceeded SIZE limit.', s.chunk_timeouts > 0 ? 'color:#991b1b;font-weight:700;text-decoration:none;' : ''),
            tipCell(String(s.chunk_muted || 0), chunkTip, ''),
            tipCell(String(s.chunk_muted_timeouts || 0), chunkTip, ''),
            tipCell(String(s.chunk_fails_total || 0), chunkTip, ''),
            tipCell(String(s.max_parallel_self || 0), s.max_parallel_self_tooltip, ''),
            tipCell(formatAbsSecLabel(s.max_parallel_self_at_sec), 'Wall-clock time when this suite reached peak parallelism.', ''),
            tipCell(String(s.peak_others_during_suite || 0), s.peak_others_tooltip, ''),
            tipCell(formatAbsSecLabel(s.peak_others_during_suite_at_sec), 'Wall-clock time when other suites peaked during this suite run.', ''),
            tipCell(s.peak_self_cpu_cores_during_suite != null ? Number(s.peak_self_cpu_cores_during_suite).toFixed(1) : '', s.peak_self_cpu_tooltip, ''),
            tipCell(formatAbsSecLabel(s.peak_self_cpu_at_sec), 'Wall-clock time of peak CPU for this suite.', ''),
            tipCell(s.peak_self_ram_gb_during_suite != null ? Number(s.peak_self_ram_gb_during_suite).toFixed(2) : '', s.peak_self_ram_tooltip, ''),
            tipCell(formatAbsSecLabel(s.peak_self_ram_at_sec), 'Wall-clock time of peak RAM for this suite.', ''),
            tipCell(s.max_chunk_ram_gb != null ? Number(s.max_chunk_ram_gb).toFixed(1) : '', s.mem_tooltip || s.mem_explain || '', s.mem_driven_cpu ? 'color:#991b1b;font-weight:700;' : ''),
            (s.cpu_for_memory != null
              ? tipCell('cpu:' + String(s.cpu_for_memory), s.mem_tooltip || s.mem_explain || '', s.mem_driven_cpu ? 'font-weight:700;color:#856404;background:#fff3cd;padding:2px 6px;border-radius:10px;text-decoration:none;' : 'font-weight:700;')
              : tipCell('—', s.mem_tooltip || 'REQUIREMENTS(ram) is ignored by the local scheduler; no memory-driven cpu bump.', '')),
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
          '<th colspan="4">ya.make</th>' +
          '<th colspan="3">runtime</th>' +
          '<th colspan="3">cpu usage</th>' +
          '<th colspan="1">ram usage</th>' +
          '<th colspan="3">runtime (used / report / evlog)</th>' +
          '<th colspan="2">decision</th>' +
          '<th colspan="8">status tests</th>' +
          '<th colspan="7">status chunks</th>' +
          '<th colspan="2">suite parallelism</th>' +
          '<th colspan="2">others during suite</th>' +
          '<th colspan="2">cpu_self_peak during suite</th>' +
          '<th colspan="2">cpu_ram_peak during suite</th>' +
          '<th colspan="2">memory → cpu</th>' +
          '</tr>';
        const subHeader =
          '<tr>' +
          '<th data-col="2" style="cursor:pointer;user-select:none;" title="REQUIREMENTS(ram:N) from ya.make">ya_ram_gb' + marker(2) + '</th>' +
          '<th data-col="3" style="cursor:pointer;user-select:none;" title="REQUIREMENTS(cpu:N) from ya.make">ya_cpu' + marker(3) + '</th>' +
          '<th data-col="4" style="cursor:pointer;user-select:none;" title="SIZE(SMALL/MEDIUM/LARGE): per-test timeout and CPU caps">ya_size' + marker(4) + '</th>' +
          '<th data-col="5" class="group-end" style="cursor:pointer;user-select:none;" title="Effective split from ya.make (FORK_TEST_FILES × SPLIT_FACTOR). Hover cell for formula.">ya_split_factor' + marker(5) + '</th>' +
          '<th data-col="6" style="cursor:pointer;user-select:none;" title="Chunk processes actually started at runtime. Hover for comparison with ya.make.">chunks' + marker(6) + '</th>' +
          '<th data-col="7" style="cursor:pointer;user-select:none;" title="Suggested SPLIT_FACTOR based on per-chunk serial test time vs SIZE timeout. Hover for details and ya.make value to set.">recommended_split' + marker(7) + '</th>' +
          '<th data-col="8" class="group-end" style="cursor:pointer;user-select:none;" title="raise/lower/ok: compare recommended_split to ya_split_factor. Hover badge for hint.">split_action' + marker(8) + '</th>' +
          '<th data-col="9" style="cursor:pointer;user-select:none;" title="Median estimated CPU cores per chunk run">median_cores' + marker(9) + '</th>' +
          '<th data-col="10" style="cursor:pointer;user-select:none;" title="95th percentile cores per chunk; basis for recommended_cpu">p95_cores' + marker(10) + '</th>' +
          '<th data-col="11" class="group-end" style="cursor:pointer;user-select:none;" title="Total CPU-seconds summed over chunk runs">total_cpu_sec' + marker(11) + '</th>' +
          '<th data-col="12" class="group-end" style="cursor:pointer;user-select:none;" title="Total peak RAM (GB) summed over chunk runs">total_ram_gb' + marker(12) + '</th>' +
          '<th data-col="13" style="cursor:pointer;user-select:none;" title="Sum of max(report, evlog) wall time per chunk">total_dur_sec' + marker(13) + '</th>' +
          '<th data-col="14" style="cursor:pointer;user-select:none;" title="Sum of report.json wall time per chunk">total_dur_report_sec' + marker(14) + '</th>' +
          '<th data-col="15" class="group-end" style="cursor:pointer;user-select:none;" title="Sum of evlog wall time per chunk">total_dur_evlog_sec' + marker(15) + '</th>' +
          '<th data-col="16" style="cursor:pointer;user-select:none;" title="Suggested REQUIREMENTS(cpu:N) for ya.make">recommended_cpu' + marker(16) + '</th>' +
          '<th data-col="17" class="group-end" style="cursor:pointer;user-select:none;" title="raise/lower/ok vs current ya.make cpu">cpu_action' + marker(17) + '</th>' +
          '<th data-col="18" style="cursor:pointer;user-select:none;" title="Total test rows in report">total' + marker(18) + '</th>' +
          '<th data-col="19" style="cursor:pointer;user-select:none;" title="Tests with OK/PASS status">passed' + marker(19) + '</th>' +
          '<th data-col="20" style="cursor:pointer;user-select:none;" title="Failed tests (non-timeout)">errors' + marker(20) + '</th>' +
          '<th data-col="21" style="cursor:pointer;user-select:none;" title="Tests that exceeded SIZE timeout">timeouts' + marker(21) + '</th>' +
          '<th data-col="22" style="cursor:pointer;user-select:none;" title="Tests in mute list">muted' + marker(22) + '</th>' +
          '<th data-col="23" style="cursor:pointer;user-select:none;" title="Muted tests that timed out">muted_timeouts' + marker(23) + '</th>' +
          '<th data-col="24" style="cursor:pointer;user-select:none;" title="Skipped tests">skipped' + marker(24) + '</th>' +
          '<th data-col="25" class="group-end" style="cursor:pointer;user-select:none;" title="errors + timeouts">fails_total' + marker(25) + '</th>' +
          '<th data-col="26" style="cursor:pointer;user-select:none;" title="Total chunk rows in report">total' + marker(26) + '</th>' +
          '<th data-col="27" style="cursor:pointer;user-select:none;" title="Chunks with OK/PASS">passed' + marker(27) + '</th>' +
          '<th data-col="28" style="cursor:pointer;user-select:none;" title="Failed chunks (non-timeout)">errors' + marker(28) + '</th>' +
          '<th data-col="29" style="cursor:pointer;user-select:none;" title="Chunks that exceeded SIZE timeout">timeouts' + marker(29) + '</th>' +
          '<th data-col="30" style="cursor:pointer;user-select:none;" title="Chunks with muted failures">muted' + marker(30) + '</th>' +
          '<th data-col="31" style="cursor:pointer;user-select:none;" title="Muted chunk timeouts">muted_timeouts' + marker(31) + '</th>' +
          '<th data-col="32" class="group-end" style="cursor:pointer;user-select:none;" title="chunk errors + timeouts">fails_total' + marker(32) + '</th>' +
          '<th data-col="33" style="cursor:pointer;user-select:none;" title="Max simultaneous chunk processes of this suite">par' + marker(33) + '</th>' +
          '<th data-col="34" class="group-end" style="cursor:pointer;user-select:none;" title="When peak parallelism happened">at' + marker(34) + '</th>' +
          '<th data-col="35" style="cursor:pointer;user-select:none;" title="Max parallel chunks of other suites during this suite">others' + marker(35) + '</th>' +
          '<th data-col="36" class="group-end" style="cursor:pointer;user-select:none;" title="When other suites peaked">at' + marker(36) + '</th>' +
          '<th data-col="37" style="cursor:pointer;user-select:none;" title="Peak estimated CPU cores (sum) for this suite">cores' + marker(37) + '</th>' +
          '<th data-col="38" class="group-end" style="cursor:pointer;user-select:none;" title="When CPU peaked">at' + marker(38) + '</th>' +
          '<th data-col="39" style="cursor:pointer;user-select:none;" title="Peak RAM (GB, sum) for this suite">GB' + marker(39) + '</th>' +
          '<th data-col="40" class="group-end" style="cursor:pointer;user-select:none;" title="When RAM peaked">at' + marker(40) + '</th>' +
          '<th data-col="41" style="cursor:pointer;user-select:none;" title="Peak RAM (GB) of the single heaviest chunk (physical proc-tree peak). Basis for memory-driven cpu.">max_chunk_ram' + marker(41) + '</th>' +
          '<th data-col="42" class="group-end" style="cursor:pointer;user-select:none;" title="cpu slot needed so heaviest_chunk_ram × parallel stays within the runner budget. REQUIREMENTS(ram) is ignored locally, so memory is throttled via cpu.">cpu_for_memory' + marker(42) + '</th>' +
          '</tr>';

        const groupEnds = new Set([5, 8, 11, 12, 15, 17, 25, 32, 34, 36, 38, 40, 42]);
        const bodyHtml = rowsData.map(cols => (
          '<tr>' + cols.map((c, i) => '<td' + (groupEnds.has(i) ? ' class="group-end"' : '') + '>' + c + '</td>').join('') + '</tr>'
        )).join('');
        document.getElementById('cpuSuggestionsTable').innerHTML =
          '<table id=\"cpuSuggestionsInner\" style=\"width:100%;border-collapse:collapse;\"><thead>' + topHeader + subHeader + '</thead><tbody>' + bodyHtml + '</tbody></table>';

        const heavyTestsFlat = [];
        for (const s of filtered) {{
          const tests = Array.isArray(s.heavy_tests_all) ? s.heavy_tests_all : [];
          for (const t of tests) {{
            if (!t) continue;
            const chunkIdx = t.chunk_idx;
            const chunkGroup = t.chunk_group;
            const chunkLabel = (chunkIdx == null)
              ? '—'
              : (chunkGroup ? (String(chunkGroup) + '/chunk' + String(chunkIdx)) : ('chunk' + String(chunkIdx)));
            heavyTestsFlat.push({{
              suite_path: String(s.suite_path || ''),
              ya_size: String(s.ya_size || ''),
              threshold_sec: Number(s.heavy_test_threshold_sec || 0),
              test_name: String(t.name || ''),
              duration_sec: Number(t.duration_sec || 0),
              chunk_label: chunkLabel,
            }});
          }}
        }}
        const heavySortValue = (row, col) => {{
          if (col === 1) return String(row.suite_path || '').toLowerCase();
          if (col === 2) return String(row.ya_size || '').toLowerCase();
          if (col === 3) return Number(row.threshold_sec || 0);
          if (col === 4) return String(row.test_name || '').toLowerCase();
          if (col === 5) return Number(row.duration_sec || 0);
          if (col === 6) return String(row.chunk_label || '').toLowerCase();
          return '';
        }};
        heavyTestsFlat.sort((a, b) => {{
          const av = heavySortValue(a, heavySortCol);
          const bv = heavySortValue(b, heavySortCol);
          if (av < bv) return heavySortAsc ? -1 : 1;
          if (av > bv) return heavySortAsc ? 1 : -1;
          // stable tie-breaker by suite->test so default stays readable.
          const as = String(a.suite_path || '').toLowerCase();
          const bs = String(b.suite_path || '').toLowerCase();
          if (as < bs) return -1;
          if (as > bs) return 1;
          const at = String(a.test_name || '').toLowerCase();
          const bt = String(b.test_name || '').toLowerCase();
          if (at < bt) return -1;
          if (at > bt) return 1;
          return 0;
        }});
        const heavyBody = heavyTestsFlat.map((x, i) => {{
          const durTip = 'Test duration ' + x.duration_sec.toFixed(1) + 's (' + (x.duration_sec / 60).toFixed(1) + ' min). Near SIZE timeout → consider more SPLIT_FACTOR or larger SIZE.';
          const thrTip = 'Heavy test threshold: 97% of the chunk timeout budget (98% of SIZE timeout) ≈ ' + x.threshold_sec.toFixed(0) + 's.';
          return (
            '<tr>' +
              '<td>' + (i + 1) + '</td>' +
              '<td>' + tipCell(x.suite_path, 'Suite path', 'max-width:320px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;display:inline-block;') + '</td>' +
              '<td>' + tipCell(x.ya_size, 'SIZE from ya.make sets per-test timeout', '') + '</td>' +
              '<td>' + tipCell((Number.isFinite(x.threshold_sec) ? x.threshold_sec.toFixed(1) + ' s' : ''), thrTip, '') + '</td>' +
              '<td>' + tipCell(x.test_name, 'Parametrized test name from report', 'max-width:400px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;display:inline-block;') + '</td>' +
              '<td>' + tipCell((Number.isFinite(x.duration_sec) ? x.duration_sec.toFixed(1) + ' s' : ''), durTip, x.duration_sec >= x.threshold_sec ? 'color:#991b1b;font-weight:700;text-decoration:none;' : '') + '</td>' +
              '<td>' + tipCell(x.chunk_label, 'Chunk where this test ran', '') + '</td>' +
            '</tr>'
          );
        }}).join('');
        const heavyWrap = document.getElementById('cpuHeavyTestsTable');
        const heavyDetails = document.getElementById('heavyTestsDetails');
        const heavySummaryLabel = document.querySelector('#heavyTestsDetails summary b');
        const heavyMarker = (col) => (col === heavySortCol ? (heavySortAsc ? ' ▲' : ' ▼') : '');
        if (heavySummaryLabel) {{
          heavySummaryLabel.textContent = 'Long/heavy tests near timeout (' + heavyTestsFlat.length + ')';
        }}
        if (heavyDetails && !heavyDetails.dataset.initCollapsed) {{
          heavyDetails.open = false;
          heavyDetails.dataset.initCollapsed = '1';
        }}
        if (heavyWrap) {{
          if (!heavyTestsFlat.length) {{
            heavyWrap.innerHTML = '<b>Long/heavy tests near timeout</b><div style=\"margin-top:6px;color:#64748b;\">No tests at >=97% of the chunk timeout budget in current filter.</div>';
            if (heavyDetails) heavyDetails.open = false;
          }} else {{
            heavyWrap.innerHTML =
              '<b>Long/heavy tests near timeout (>=97% of the chunk timeout budget)</b>' +
              '<table style=\"width:100%;border-collapse:collapse;margin-top:8px;\">' +
              '<thead><tr><th>#</th><th data-hcol=\"1\" style=\"cursor:pointer;user-select:none;\">suite_path' + heavyMarker(1) + '</th><th data-hcol=\"2\" style=\"cursor:pointer;user-select:none;\">ya_size' + heavyMarker(2) + '</th><th data-hcol=\"3\" style=\"cursor:pointer;user-select:none;\">threshold' + heavyMarker(3) + '</th><th data-hcol=\"4\" style=\"cursor:pointer;user-select:none;\">test' + heavyMarker(4) + '</th><th data-hcol=\"5\" style=\"cursor:pointer;user-select:none;\">duration' + heavyMarker(5) + '</th><th data-hcol=\"6\" style=\"cursor:pointer;user-select:none;\">chunk' + heavyMarker(6) + '</th></tr></thead>' +
              '<tbody>' + heavyBody + '</tbody></table>';
            const hths = heavyWrap.querySelectorAll('thead th[data-hcol]');
            hths.forEach(th => th.addEventListener('click', () => {{
              const col = Number(th.getAttribute('data-hcol'));
              if (heavySortCol === col) heavySortAsc = !heavySortAsc;
              else {{
                heavySortCol = col;
                heavySortAsc = true;
              }}
              renderSuggestionsTable();
            }}));
          }}
        }}

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
      }}

      const helpBtn = document.getElementById('cpuHelpToggle');
      const helpBox = document.getElementById('cpuHelpText');
      if (helpBtn && helpBox) {{
        helpBtn.addEventListener('click', () => {{
          helpBox.style.display = helpBox.style.display === 'block' ? 'none' : 'block';
        }});
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
      const useNoSynthetic = Boolean(data.has_synthetic_metrics) && !inc;
      const cpuSuiteBase = useNoSynthetic ? data.cpu_tracks_suite_no_synthetic : data.cpu_tracks_suite;
      const ramSuiteBase = useNoSynthetic ? data.ram_tracks_suite_no_synthetic : data.ram_tracks_suite;
      const cpuChunkBase = useNoSynthetic ? data.cpu_tracks_no_synthetic : data.cpu_tracks;
      const ramChunkBase = useNoSynthetic ? data.ram_tracks_no_synthetic : data.ram_tracks;

      if (data.by_chunk && data.xs_cpu && data.xs_cpu.length > 0) {{
        updateStackedPlotTracks('activeLayerChunk', data.active_tracks_chunk);
        updateStackedPlotTracks('cpuLayer', cpuChunkBase);
        updateStackedPlotTracks('ramLayer', ramChunkBase);
      }}
      updateStackedPlotTracks('activeLayerSuite', data.active_tracks_suite);
      updateStackedPlotTracks('testsLayerSuite', data.tests_tracks_suite);
      updateStackedPlotTracks('cpuLayerSuite', cpuSuiteBase);
      updateStackedPlotTracks('ramLayerSuite', ramSuiteBase);

      ['activeLayerChunk', 'cpuLayer', 'ramLayer', 'activeLayerSuite', 'testsLayerSuite', 'cpuLayerSuite', 'ramLayerSuite']
        .forEach(id => updateStackedPlotColors(id, q));
      ['activeLayerChunk', 'cpuLayer', 'ramLayer', 'activeLayerSuite', 'testsLayerSuite', 'cpuLayerSuite', 'ramLayerSuite']
        .forEach(id => reorderStackedPlotByQuery(id, q));
      ['cpuPie', 'ramPie', 'activePie', 'cpuPieSuite', 'ramPieSuite', 'activePieSuite', 'testsPieSuite']
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
        return '<span style="display:inline-block;margin-right:4px;padding:1px 5px;border-radius:3px;background:' + safeColor(color) + ';color:#fff;font-size:10px;cursor:help;" title="Suite: ' + titleAttr(sp) + '. Chart markers: ' + errN + ' error(s), ' + toN + ' timeout(s).">' + escapeHtml(short) + '</span>';
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
          bestX = tr.x[i];
        }}
      }}
      if (!Number.isFinite(bestY) || bestY <= 0 || bestX == null) return null;
      const peakMs = _toMs(bestX);
      if (peakMs == null) return null;
      return xFromDisplay(peakMs);
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
            text: '<b>' + escapeHtml(m.label) + '</b><br><span style="font-size:10px">' + escapeHtml(m.shortName) + '</span>',
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
      const useNoSynthetic = Boolean(data.has_synthetic_metrics) && !inc;
      updateStackedPlotTracks('cpuLayerSuite', useNoSynthetic ? data.cpu_tracks_suite_no_synthetic : data.cpu_tracks_suite);
      updateStackedPlotTracks('ramLayerSuite', useNoSynthetic ? data.ram_tracks_suite_no_synthetic : data.ram_tracks_suite);
      if (data.by_chunk && data.xs_cpu && data.xs_cpu.length > 0) {{
        updateStackedPlotTracks('cpuLayer', useNoSynthetic ? data.cpu_tracks_no_synthetic : data.cpu_tracks);
        updateStackedPlotTracks('ramLayer', useNoSynthetic ? data.ram_tracks_no_synthetic : data.ram_tracks);
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

    function addCpuLimitTrace(plotId, ro) {{
      if (!ro || !ro.runner_limits || !ro.runner_limits.cpu_cores_max) return;
      const xDisp = (ro.xs_evlog_sec || []).map(x => xToDisplay(x));
      if (!xDisp.length) return;
      const y = Number(ro.runner_limits.cpu_cores_max);
      Plotly.addTraces(plotId, [{{
        x: xDisp,
        y: xDisp.map(() => y),
        mode: 'lines',
        name: 'CPU limit (provisioned)',
        line: {{ color: '#9333ea', width: 2, dash: 'dash' }},
        legendrank: 1001,
        hovertemplate: 'CPU provisioned max: %{{y:.0f}} cores<extra></extra>',
      }}]);
    }}

    function addRamLimitTraces(plotId, ro) {{
      if (!ro || !ro.runner_limits) return;
      const lim = ro.runner_limits;
      const xDisp = (ro.xs_evlog_sec || []).map(x => xToDisplay(x));
      if (!xDisp.length) return;
      const traces = [];
      if (lim.ram_gb_max) {{
        traces.push({{
          x: xDisp,
          y: xDisp.map(() => Number(lim.ram_gb_max)),
          mode: 'lines',
          name: 'RAM limit (provisioned)',
          line: {{ color: '#9333ea', width: 2, dash: 'dash' }},
          legendrank: 1001,
          hovertemplate: 'RAM provisioned max: %{{y:.1f}} GB<extra></extra>',
        }});
      }}
      if (lim.ya_make_mem_limit_gb) {{
        traces.push({{
          x: xDisp,
          y: xDisp.map(() => Number(lim.ya_make_mem_limit_gb)),
          mode: 'lines',
          name: 'RAM ya.make cgroup',
          line: {{ color: '#c026d3', width: 2, dash: 'dot' }},
          legendrank: 1002,
          hovertemplate: 'ya.make MemoryMax (~95%): %{{y:.1f}} GB<extra></extra>',
        }});
      }}
      if (traces.length) Plotly.addTraces(plotId, traces);
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
      if (unit === 'tests') return String(Math.round(y));
      if (unit === 'GB') return y.toFixed(3);
      if (unit === 'MB/s') return y.toFixed(2);
      return y.toFixed(3);
    }}

    function minVisibleValue(unit) {{
      if (unit === 'active') return 0.5;
      if (unit === 'tests') return 0.5;
      if (unit === 'GB') return 0.001;   // ~1 MB
      if (unit === 'cores') return 0.01; // suppress tiny CPU noise shown as 0.000
      if (unit === 'MB/s') return 0.01;
      return 0.0;
    }}

    let chartTipHideTimer = null;
    function hoverPointLine(p) {{
      const tpl = String((p && p.data && p.data.hovertemplate) || '');
      if (!tpl || (p.data.hoverinfo === 'none')) return '';
      const y = Number(p.y);
      let s = tpl.replace(/<extra>[\\s\\S]*<\\/extra>/g, '');
      s = s.replace(/%\\{{y:\\.(\\d+)f\\}}/g, (_, d) => (Number.isFinite(y) ? y.toFixed(Number(d)) : ''));
      s = s.replace(/%\\{{fullData\\.name\\}}/g, p.data.name || '');
      s = s.replace(/%\\{{y\\}}/g, () => (Number.isFinite(y) ? String(y) : ''));
      return s.trim();
    }}
    function positionChartTip(ev) {{
      const tip = document.getElementById('chartTip');
      const e = ev && ev.event;
      if (!tip || !e || !Number.isFinite(e.clientX)) return;
      const pad = 14;
      tip.style.left = '0px';
      tip.style.top = '0px';
      const w = tip.offsetWidth;
      const h = tip.offsetHeight;
      let x = e.clientX + pad;
      let y = e.clientY + pad;
      if (x + w > window.innerWidth - 8) x = e.clientX - w - pad;
      if (y + h > window.innerHeight - 8) y = e.clientY - h - pad;
      tip.style.left = Math.max(8, x) + 'px';
      tip.style.top = Math.max(8, y) + 'px';
    }}
    function hoverContributionRows(ev, unit) {{
      const rows = (ev.points || []).map(p => ({{
        name: p && p.data ? p.data.name : '',
        y: Number(p && p.y || 0),
      }})).filter(p => isSuiteContribution(p.name));
      const total = rows.reduce((sum, row) => sum + row.y, 0);
      const eps = minVisibleValue(unit);
      const top = rows.filter(row => row.y > eps).sort((a, b) => b.y - a.y).slice(0, 40);
      return {{ total: total, top: top }};
    }}
    function hoverTotalLine(total, unit) {{
      return 'Total: ' + formatValue(Number(total) || 0, unit) + ' ' + unit;
    }}
    function showChartTip(ev, unit) {{
      const tip = document.getElementById('chartTip');
      if (!tip || !ev || !ev.points || !ev.points.length) return;
      clearTimeout(chartTipHideTimer);
      const label = formatPointTime(ev.points[0]);
      const seen = new Set();
      const lines = [];
      const contrib = unit ? hoverContributionRows(ev, unit) : null;
      if (contrib) lines.push(hoverTotalLine(contrib.total, unit));
      for (const p of ev.points) {{
        const line = hoverPointLine(p);
        if (!line || seen.has(line)) continue;
        seen.add(line);
        lines.push(line);
      }}
      tip.textContent = [label].concat(lines).join('\\n');
      tip.style.display = 'block';
      positionChartTip(ev);
    }}
    function hideChartTipSoon() {{
      clearTimeout(chartTipHideTimer);
      chartTipHideTimer = setTimeout(() => {{
        const tip = document.getElementById('chartTip');
        if (tip) tip.style.display = 'none';
      }}, 40);
    }}
    function isSuiteContribution(name) {{
      if (!name) return false;
      if (name.includes('outline') || name.endsWith(' outline')) return false;
      if (name.includes('(monitor)')) return false;
      if (name.includes('limit (')) return false;
      if (name.includes('ya.make cgroup')) return false;
      return true;
    }}
    function sourceSecFromPoint(point) {{
      if (!point) return null;
      const idx = Number(point.pointNumber);
      const xs = point.data && Array.isArray(point.data.x) ? point.data.x : null;
      if (!xs || !Number.isFinite(idx) || idx < 0 || idx >= xs.length) return null;
      const sec = xFromDisplay(xs[Math.floor(idx)]);
      return Number.isFinite(Number(sec)) ? Number(sec) : null;
    }}
    function activeCountAtSec(tSec) {{
      const xs = data.xs_active;
      const ys = data.ys_active;
      if (!Array.isArray(xs) || !Array.isArray(ys) || !Number.isFinite(Number(tSec))) return null;
      let idx = -1;
      for (let i = 0; i < xs.length; i++) {{
        if (Number(xs[i]) <= Number(tSec)) idx = i;
        else break;
      }}
      if (idx < 0) return 0;
      const v = Number(ys[idx]);
      return Number.isFinite(v) ? Math.round(v) : null;
    }}
    function attachSortedHover(plotId, panelId, unit) {{
      const plot = document.getElementById(plotId);
      const panel = document.getElementById(panelId);
      if (!plot || !panel) return;
      plot.classList.add('tz-hover');
      plot.on('plotly_hover', (ev) => {{
        if (!ev || !ev.points || !ev.points.length) return;
        const point = ev.points[0];
        const label = formatPointTime(point);
        const contrib = hoverContributionRows(ev, unit);
        const lines = [hoverTotalLine(contrib.total, unit)].concat(
          contrib.top.map(r => `${{r.name}}: ${{formatValue(r.y, unit)}} ${{unit}}`)
        );
        panel.textContent = `t=${{label}}\\n` + lines.join('\\n');
        showChartTip(ev, unit);
      }});
      plot.on('plotly_unhover', () => {{
        panel.textContent = 'Move cursor over chart to see sorted contributors';
        hideChartTipSoon();
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

    function renderClickTable(plotId, panelId, rows, t, unit, monitorAtClick=null, totalAtClick=null, tSecAtClick=null, timeLabel=null) {{
      const panel = document.getElementById(panelId);
      if (!panel) return;
      const top = rows.slice(0, 100);
      const showTestsColumn = unit !== 'tests';
      const includeSynthetic = document.getElementById('includeSyntheticCb')?.checked ?? true;
      const testsFor = (name) => testsInActiveChunksAtTime(name, t, tSecAtClick);
      const htmlRows = top.map((r, i) => {{
        const isSynthetic = includeSynthetic && data.has_synthetic_metrics && data.track_has_synthetic && data.track_has_synthetic[r.name];
        const syntheticBadge = isSynthetic ? ' <span style="color:#b8860b;">(estimated from ya.make)</span>' : '';
        const testsCell = testsFor(r.name);
        const testsColHtml = showTestsColumn
          ? ('<td>' + (testsCell !== '' ? testsCell : '—') + '</td>')
          : '';
        return (
        '<tr>' +
          '<td>' + (i + 1) + '</td>' +
          '<td><span style="display:inline-block;width:10px;height:10px;border-radius:2px;margin-right:6px;vertical-align:middle;background:' + safeColor(colorForTrack(r.name)) + ';"></span>' + escapeHtml(r.name) + syntheticBadge + '</td>' +
          testsColHtml +
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
      }} else if (unit === 'active') {{
        const totalActive = rows.reduce((acc, r) => acc + Number(r.y || 0), 0);
        totalsHtml =
          '<div style="margin:4px 0 8px 0;font-size:12px;color:#374151;">' +
          '<b>Tests (chunks) running at this time:</b> ' + Math.round(totalActive) +
          '</div>';
      }} else if (unit === 'tests') {{
        const totalTests = rows.reduce((acc, r) => acc + Number(r.y || 0), 0);
        totalsHtml =
          '<div style="margin:4px 0 8px 0;font-size:12px;color:#374151;">' +
          '<b>Tests running at this time:</b> ' + Math.round(totalTests) +
          '</div>';
      }}
      const runningCount = Number.isFinite(Number(tSecAtClick))
        ? activeCountAtSec(Number(tSecAtClick))
        : activeCountAtTime(t);
      const runningLine = (runningCount != null)
        ? '<div style="margin:4px 0 8px 0;font-size:12px;color:#374151;"><b>Tests (chunks) running at this time:</b> ' + runningCount + '</div>'
        : '';
      const summaryBlock = (unit === 'active' || unit === 'tests') ? totalsHtml : (runningLine + totalsHtml);
      const testsHeaderHtml = showTestsColumn ? '<th>tests in active chunks</th>' : '';
      panel.innerHTML =
        '<div><b>t=' + escapeHtml(timeLabel || formatTimeLabel(t)) + '</b> | rows: ' + top.length + '</div>' +
        summaryBlock +
        '<table><thead><tr><th>#</th><th>suite+chunk</th>' + testsHeaderHtml + '<th>value</th></tr></thead><tbody>' + htmlRows + '</tbody></table>';
    }}

    function attachSortedClick(plotId, panelId, unit) {{
      const plot = document.getElementById(plotId);
      if (!plot) return;
      plot.on('plotly_click', (ev) => {{
        if (!ev || !ev.points || !ev.points.length) return;
        const t = ev.points[0].x;
        const eps = minVisibleValue(unit);
        const monitorPoint = ev.points.find(p => p && p.data && typeof p.data.name === 'string' && p.data.name === (unit === 'GB' ? 'RAM total (monitor)' : 'CPU total (monitor)'));
        const monitorAtClick = monitorPoint ? Number(monitorPoint.y) : null;
        const rows = ev.points
          .map(p => ({{name: p.data.name, y: Number(p.y || 0)}}))
          .filter(p => p.y > eps && isSuiteContribution(p.name))
          .sort((a, b) => b.y - a.y);
        const tSecAtClick = sourceSecFromPoint(ev.points[0]);
        const totalAtClick = unit === 'active'
          ? rows.reduce((acc, r) => acc + Number(r.y || 0), 0)
          : Number(ev.points[0]?.y);
        renderClickTable(plotId, panelId, rows, t, unit, monitorAtClick, totalAtClick, tSecAtClick, formatPointTime(ev.points[0]));
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
    (function() {{
      const plot = document.getElementById('activeChunks');
      const panel = document.getElementById('activeChunksClick');
      if (plot && panel) {{
        plot.on('plotly_click', (ev) => {{
          if (!ev || !ev.points || !ev.points.length) return;
          const t = ev.points[0].x;
          const y = Number(ev.points[0].y);
          const n = Number.isFinite(y) ? Math.round(y) : activeCountAtTime(t);
          panel.innerHTML = '<b>t=' + formatPointTime(ev.points[0]) + '</b>: ' + (n != null ? n + ' tests (chunks) running' : '—');
        }});
      }}
    }})();

    if (data.by_chunk && data.xs_active_chunk && data.xs_active_chunk.length > 0) {{
      stackedArea('activeLayerChunk', data.xs_active_chunk, data.active_tracks_chunk, 'Layered active chunks by suite+chunk', 'active chunks', true);
      attachSortedHover('activeLayerChunk', 'activeHoverChunk', 'active');
      attachSortedClick('activeLayerChunk', 'activeClickChunk', 'active');
      stackedArea('cpuLayer', data.xs_cpu, data.cpu_tracks, 'Layered CPU (estimated cores) by suite+chunk', 'cores');
      stackedArea('ramLayer', data.xs_ram, data.ram_tracks, 'Layered RAM by suite+chunk', 'GB');
      if (data.resources_overlay) {{
        const ro = data.resources_overlay;
        const xDisp = (ro.xs_evlog_sec || []).map(x => xToDisplay(x));
        Plotly.addTraces('cpuLayer', [
          {{ x: xDisp, y: ro.cpu_total_cores || [], mode: 'lines', name: 'CPU total (monitor) outline', line: {{ color: '#000000', width: 8 }}, legendrank: 1000, showlegend: false, hoverinfo: 'skip' }},
          {{ x: xDisp, y: ro.cpu_total_cores || [], mode: 'lines', name: 'CPU total (monitor)', line: {{ color: '#ff1744', width: 6 }}, legendrank: 1000, hovertemplate: '%{{fullData.name}}: %{{y:.3f}} cores<extra></extra>' }},
        ]);
        Plotly.addTraces('ramLayer', [
          {{ x: xDisp, y: ro.ram_gb || [], mode: 'lines', name: 'RAM total (monitor) outline', line: {{ color: '#000000', width: 8 }}, legendrank: 1000, showlegend: false, hoverinfo: 'skip' }},
          {{ x: xDisp, y: ro.ram_gb || [], mode: 'lines', name: 'RAM total (monitor)', line: {{ color: '#ff1744', width: 6 }}, legendrank: 1000, hovertemplate: '%{{fullData.name}}: %{{y:.3f}} GB<extra></extra>' }},
        ]);
        addCpuLimitTrace('cpuLayer', ro);
        addRamLimitTraces('ramLayer', ro);
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
            {{ x: xDisp, y: (ro.disk_write_mb || []).map(v => Number(v) || 0), mode: 'lines', name: 'Disk write (+MB/s)', line: {{ color: '#65a30d' }}, hovertemplate: 'Disk write: %{{y:.2f}} MB/s<extra></extra>' }},
            {{ x: xDisp, y: (ro.disk_read_mb || []).map(v => -(Number(v) || 0)), mode: 'lines', name: 'Disk read (-MB/s)', line: {{ color: '#0284c7' }}, hovertemplate: 'Disk read: %{{y:.2f}} MB/s<extra></extra>' }}
          ], {{
            title: 'Disk bandwidth (+Write, -Read)',
            xaxis: xaxisOpt,
            yaxis: {{ title: 'MB/s' }},
            hovermode: 'x unified',
            hoverlabel: {{ namelength: -1 }},
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
    stackedArea('testsLayerSuite', data.xs_tests_suite, data.tests_tracks_suite, 'Layered tests running by suite', 'tests', true);
    attachSortedHover('testsLayerSuite', 'testsHoverSuite', 'tests');
    attachSortedClick('testsLayerSuite', 'testsClickSuite', 'tests');

    stackedArea('cpuLayerSuite', data.xs_cpu_suite, data.cpu_tracks_suite, 'Layered CPU (estimated cores) by suite', 'cores');
    stackedArea('ramLayerSuite', data.xs_ram_suite, data.ram_tracks_suite, 'Layered RAM by suite', 'GB');
    if (data.resources_overlay) {{
      const ro = data.resources_overlay;
      const xDisp = (ro.xs_evlog_sec || []).map(x => xToDisplay(x));
      Plotly.addTraces('cpuLayerSuite', [
        {{ x: xDisp, y: ro.cpu_total_cores || [], mode: 'lines', name: 'CPU total (monitor) outline', line: {{ color: '#000000', width: 8 }}, legendrank: 1000, showlegend: false, hoverinfo: 'skip' }},
        {{ x: xDisp, y: ro.cpu_total_cores || [], mode: 'lines', name: 'CPU total (monitor)', line: {{ color: '#ff1744', width: 6 }}, legendrank: 1000, hovertemplate: '%{{fullData.name}}: %{{y:.3f}} cores<extra></extra>' }},
      ]);
      Plotly.addTraces('ramLayerSuite', [
        {{ x: xDisp, y: ro.ram_gb || [], mode: 'lines', name: 'RAM total (monitor) outline', line: {{ color: '#000000', width: 8 }}, legendrank: 1000, showlegend: false, hoverinfo: 'skip' }},
        {{ x: xDisp, y: ro.ram_gb || [], mode: 'lines', name: 'RAM total (monitor)', line: {{ color: '#ff1744', width: 6 }}, legendrank: 1000, hovertemplate: '%{{fullData.name}}: %{{y:.3f}} GB<extra></extra>' }},
      ]);
      addCpuLimitTrace('cpuLayerSuite', ro);
      addRamLimitTraces('ramLayerSuite', ro);
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
          {{ x: xDisp, y: (ro.disk_write_mb || []).map(v => Number(v) || 0), mode: 'lines', name: 'Disk write (+MB/s)', line: {{ color: '#65a30d' }}, hovertemplate: 'Disk write: %{{y:.2f}} MB/s<extra></extra>' }},
          {{ x: xDisp, y: (ro.disk_read_mb || []).map(v => -(Number(v) || 0)), mode: 'lines', name: 'Disk read (-MB/s)', line: {{ color: '#0284c7' }}, hovertemplate: 'Disk read: %{{y:.2f}} MB/s<extra></extra>' }}
        ], {{
          title: 'Disk bandwidth (+Write, -Read)',
          xaxis: xaxisOpt,
          yaxis: {{ title: 'MB/s' }},
          hovermode: 'x unified',
          hoverlabel: {{ namelength: -1 }},
        }}, {{ responsive: true }});
      }}
      const hostSignals = ro.host_resource_signals || {{}};
      const hostChart = document.getElementById('hostContentionChart');
      if (hostChart && Object.keys(hostSignals).length) {{
        const signalNames = {{
          cpu_iowait_pct: 'CPU iowait',
          pressure_cpu_some_pct: 'CPU PSI some',
          pressure_cpu_full_pct: 'CPU PSI full',
          pressure_memory_some_pct: 'Memory PSI some',
          pressure_memory_full_pct: 'Memory PSI full',
          pressure_io_some_pct: 'I/O PSI some',
          pressure_io_full_pct: 'I/O PSI full',
          disk_busy_pct: 'Disk busy',
          disk_io_in_progress: 'Disk I/O in progress',
        }};
        const traces = Object.keys(signalNames).filter(name => hostSignals[name]).map((name, index) => {{
          const isQueue = name === 'disk_io_in_progress';
          return {{
            x: xDisp,
            y: hostSignals[name],
            type: 'scatter',
            mode: 'lines',
            name: signalNames[name],
            yaxis: isQueue ? 'y2' : 'y',
            connectgaps: false,
            line: {{ width: 2, color: ['#d97706','#7c3aed','#0284c7','#65a30d','#dc2626','#0f766e','#c026d3','#1d4ed8','#b45309'][index % 9] }},
            hovertemplate: '%{{fullData.name}}: %{{y:.2f}}' + (isQueue ? ' requests<extra></extra>' : '%<extra></extra>'),
          }};
        }});
        if (traces.length) {{
          hostChart.style.display = '';
          Plotly.newPlot(hostChart, traces, {{
            title: 'Host contention signals (iowait, PSI, disk queue)',
            xaxis: axisLayout(xDisp),
            yaxis: {{ title: 'Stall / busy time (%)', rangemode: 'tozero' }},
            yaxis2: {{ title: 'Requests in progress', overlaying: 'y', side: 'right', rangemode: 'tozero' }},
            hovermode: 'x unified',
            hoverlabel: {{ namelength: -1 }},
          }}, {{ responsive: true }});
        }}
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

    Plotly.newPlot('testsPieSuite', [{{
      type: 'pie',
      labels: data.pie_tests_suite_labels,
      values: data.pie_tests_suite_vals,
      marker: {{colors: data.pie_tests_suite_labels.map(lb => lb === 'other' ? '#bdbdbd' : (data.suite_color[suiteFromLabel(lb)] || '#999'))}},
      textinfo: 'percent',
      textposition: 'inside',
      automargin: true,
      sort: false,
      hovertemplate: '%{{label}}<br>tests=%{{value:.0f}}<extra></extra>',
    }}], {{title: 'Tests count share by suite (top + other)', showlegend: false, margin: {{l: 20, r: 20, t: 50, b: 20}}}}, {{responsive: true}});

    if (data.resources_overlay) {{
      document.getElementById('layerTogglesWrap').style.display = '';
    }}
    function applyLayerToggles() {{
      if (!data.resources_overlay) return;
      const bySuite = document.getElementById('layerBySuite')?.checked ?? true;
      const systemCpu = document.getElementById('layerSystemCpu')?.checked ?? true;
      const systemRam = document.getElementById('layerSystemRam')?.checked ?? true;
      const CPU_OVERLAY_NAMES = ['CPU total (monitor)', 'CPU total (monitor) outline', 'CPU limit (provisioned)'];
      const RAM_OVERLAY_NAMES = ['RAM total (monitor)', 'RAM total (monitor) outline', 'RAM limit (provisioned)', 'RAM ya.make cgroup'];
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

    const measuredList = (data.resources_overlay && data.resources_overlay.measured_tests) || [];
    const measuredLayers = (data.resources_overlay && data.resources_overlay.measured_layers) || {{}};
    const measuredChunkTests = (data.resources_overlay && data.resources_overlay.measured_chunk_tests) || {{}};
    const measuredWrap = document.getElementById('measuredWrap');
    const MEASURED_PALETTE = ['#d97706','#7c3aed','#0284c7','#65a30d','#dc2626','#0f766e','#c026d3','#1d4ed8','#b45309','#15803d','#be123c','#4338ca'];
    const _measuredColors = {{}};
    const _origColorForTrack = colorForTrack;
    colorForTrack = function(trackName) {{
      if (_measuredColors[trackName]) return _measuredColors[trackName];
      return _origColorForTrack(trackName);
    }};
    function measuredPeak(track) {{
      return (track || []).reduce((m, v) => Math.max(m, Number(v) || 0), 0);
    }}
    function filterMeasuredTracks(allTracks, labels) {{
      const out = {{}};
      const selected = new Set(labels || []);
      Object.keys(allTracks || {{}}).forEach(name => {{
        const separator = name.lastIndexOf(' · ');
        const label = separator >= 0 ? name.slice(0, separator) : name;
        if (selected.has(label)) out[name] = allTracks[name];
      }});
      return out;
    }}
    function topMeasuredTracks(tracks, limit) {{
      const names = Object.keys(tracks || {{}}).sort((a, b) => measuredPeak(tracks[b]) - measuredPeak(tracks[a]));
      const ordered = {{}};
      if (names.length <= limit) {{
        names.slice().reverse().forEach(n => {{ ordered[n] = tracks[n]; }});
        return ordered;
      }}
      const top = names.slice(0, limit);
      const rest = names.slice(limit);
      const other = tracks[rest[0]].map((_, i) => rest.reduce((s, n) => s + (Number(tracks[n][i]) || 0), 0));
      ordered.other = other;
      top.slice().reverse().forEach(n => {{ ordered[n] = tracks[n]; }});
      return ordered;
    }}
    function plotMeasuredLayers(labels, titleSuffix) {{
      if (!window.Plotly || !measuredLayers.xs_evlog_sec) return;
      const charts = [
        ['measuredCpu', measuredLayers.cpu_tracks, 'Layered measured CPU' + titleSuffix, 'cores'],
        ['measuredRam', measuredLayers.ram_tracks, 'Layered measured RAM' + titleSuffix, 'GB'],
        ['measuredWrite', measuredLayers.write_tracks, 'Layered measured disk write' + titleSuffix, 'MB/s'],
        ['measuredRead', measuredLayers.read_tracks, 'Layered measured disk read' + titleSuffix, 'MB/s'],
      ];
      charts.forEach(([divId, allTracks, title, unit]) => {{
        const tracks = topMeasuredTracks(filterMeasuredTracks(allTracks, labels), 50);
        Object.keys(tracks).forEach((name, i) => {{
          _measuredColors[name] = name === 'other' ? '#bdbdbd' : MEASURED_PALETTE[i % MEASURED_PALETTE.length];
        }});
        stackedArea(divId, measuredLayers.xs_evlog_sec, tracks, title, unit);
      }});
      attachMeasuredHover('measuredCpu', 'measuredCpuHover', 'cores');
      attachMeasuredHover('measuredRam', 'measuredRamHover', 'GB');
      attachMeasuredHover('measuredWrite', 'measuredWriteHover', 'MB/s');
      attachMeasuredHover('measuredRead', 'measuredReadHover', 'MB/s');
    }}
    function plotMeasuredPressure(labels, titleSuffix) {{
      const el = document.getElementById('measuredPressure');
      const panel = document.getElementById('measuredPressureHover');
      if (!el || !window.Plotly) return;
      const allTracks = measuredLayers.pressure_tracks || {{}};
      const tracks = topMeasuredTracks(filterMeasuredTracks(allTracks, labels), 50);
      const names = Object.keys(tracks);
      if (!names.length) {{
        el.style.display = 'none';
        if (panel) panel.style.display = 'none';
        return;
      }}
      el.style.display = '';
      if (panel) panel.style.display = '';
      const xs = (measuredLayers.xs_evlog_sec || []).map(xToDisplay);
      const traces = names.map((name, i) => ({{
        x: xs,
        y: tracks[name],
        type: 'scatter',
        mode: 'lines',
        name: name,
        connectgaps: false,
        line: {{ width: 2, color: name === 'other' ? '#bdbdbd' : MEASURED_PALETTE[i % MEASURED_PALETTE.length] }},
        hovertemplate: '%{{fullData.name}}: %{{y:.2f}}%<extra></extra>',
      }}));
      Plotly.newPlot(el, traces, {{
        title: 'Cgroup throttling and pressure' + titleSuffix,
        xaxis: axisLayout(xs),
        yaxis: {{ title: 'Interval time (%)', rangemode: 'tozero' }},
        hovermode: 'x unified',
        hoverlabel: {{ namelength: -1 }},
        margin: {{ t: 48, r: 20, b: 30, l: 52 }},
      }}, {{ responsive: true }});
      el.on('plotly_hover', (ev) => {{
        if (!panel || !ev || !ev.points || !ev.points.length) return;
        panel.textContent = ev.points.map(p => `${{p.data.name}}: ${{Number(p.y).toFixed(2)}}%`).join(' · ');
      }});
      el.on('plotly_unhover', () => {{ if (panel) panel.textContent = 'Hover to inspect per-chunk throttling and PSI'; }});
    }}
    let _measuredActiveTests = [];
    function testAtSec(tSec) {{
      if (!Number.isFinite(Number(tSec))) return null;
      const t = Number(tSec);
      return (_measuredActiveTests || []).find(row => Number(row.start_sec) <= t && t < Number(row.end_sec)) || null;
    }}
    function highlightMeasuredTest(name) {{
      document.querySelectorAll('#measuredTestsList li').forEach(li => {{
        li.classList.toggle('on', Boolean(name) && li.dataset.name === name);
      }});
    }}
    function measuredStatusLabel(status) {{
      const u = String(status || '').toUpperCase().replace(/\\s+/g, '_');
      if (u === 'OK' || u === 'PASS' || u === 'PASSED' || u === 'GOOD') return 'PASS';
      if (u === 'FAILED' || u === 'FAIL' || u === 'ERROR') return 'FAIL';
      if (u === 'MUTE' || u === 'MUTED') return 'MUTE';
      if (u === 'SKIP' || u === 'SKIPPED') return 'SKIP';
      if (u === 'NOT_LAUNCHED' || u === 'NOTLAUNCHED') return 'NOT LAUNCHED';
      return u;
    }}
    function measuredStatusClass(label) {{
      if (label === 'PASS') return 'st-pass';
      if (label === 'FAIL') return 'st-fail';
      if (label === 'MUTE') return 'st-mute';
      if (label === 'SKIP') return 'st-skip';
      if (label === 'NOT LAUNCHED') return 'st-nl';
      return 'st-other';
    }}
    function measuredErrorTags(errorType) {{
      return String(errorType || '').split(',').map(s => s.trim()).filter(Boolean);
    }}
    function measuredErrorBadgeClass(tag) {{
      const u = String(tag || '').toUpperCase().replace(/\\s+/g, '_');
      if (u === 'TIMEOUT' || u === 'VERIFY' || u === 'SANITIZER' || u === 'XFAILED' || u === 'POSSIBLE_OOM' || u === 'NOT_LAUNCHED') return 'eb-' + u;
      return 'eb-other';
    }}
    function measuredErrorBadgeLabel(tag) {{
      const u = String(tag || '').toUpperCase().replace(/\\s+/g, '_');
      if (u === 'POSSIBLE_OOM') return 'possible OOM';
      if (u === 'NOT_LAUNCHED') return 'NOT LAUNCHED';
      return String(tag || '').toUpperCase();
    }}
    function resolveArtifactUrl(url) {{
      const raw = String(url || '').trim();
      if (!raw) return '';
      if (/^https?:\\/\\//i.test(raw)) return raw;
      const base = String((data.config && data.config.artifacts_url) || '').trim();
      if (!base) return raw;
      return base.replace(/\\/?$/, '/') + raw.replace(/^\\//, '');
    }}
    function measuredLogEntries(links) {{
      const src = links && typeof links === 'object' ? links : {{}};
      const preferred = ['stderr', 'stdout', 'log', 'Log', 'logsdir'];
      const skip = new Set(['core', 'binary']);
      const names = [];
      preferred.forEach(name => {{ if (src[name] && !names.includes(name)) names.push(name); }});
      Object.keys(src).sort().forEach(name => {{
        const kind = String(name).toLowerCase().replace(/\\d+$/, '').trim();
        if (skip.has(kind) || kind.endsWith(' core') || kind.endsWith(' binary')) return;
        if (src[name] && !names.includes(name)) names.push(name);
      }});
      return names.map(name => ({{ name: name, url: resolveArtifactUrl(src[name]) }})).filter(x => x.url);
    }}
    function measuredTestHoverLine(point) {{
      const test = testAtSec(sourceSecFromPoint(point));
      highlightMeasuredTest(test && test.name);
      if (!test) return '';
      const status = measuredStatusLabel(test.status);
      const tags = measuredErrorTags(test.error_type).map(measuredErrorBadgeLabel);
      const extra = [status, ...tags].filter(Boolean).join(' · ');
      return test.name + ' (' + Number(test.duration_sec || 0).toFixed(1) + 's' + (extra ? ', ' + extra : '') + ')';
    }}
    function attachMeasuredHover(plotId, panelId, unit) {{
      const plot = document.getElementById(plotId);
      const panel = document.getElementById(panelId);
      if (!plot || !panel) return;
      plot.classList.add('tz-hover');
      plot.on('plotly_hover', (ev) => {{
        if (!ev || !ev.points || !ev.points.length) return;
        const point = ev.points[0];
        const label = formatPointTime(point);
        const testLine = measuredTestHoverLine(point);
        const contrib = hoverContributionRows(ev, unit);
        const lines = [hoverTotalLine(contrib.total, unit)].concat(
          contrib.top.map(r => `${{r.name}}: ${{formatValue(r.y, unit)}} ${{unit}}`)
        );
        panel.textContent = (testLine ? testLine + '\\n' : '') + `t=${{label}}\\n` + lines.join('\\n');
        showChartTip(ev, unit);
        const tip = document.getElementById('chartTip');
        if (tip && testLine && tip.style.display !== 'none' && !(tip.textContent || '').includes(testLine)) {{
          tip.textContent = testLine + '\\n' + (tip.textContent || '');
        }}
      }});
      plot.on('plotly_unhover', () => {{
        panel.textContent = 'Move cursor over chart to see sorted contributors';
        highlightMeasuredTest('');
        hideChartTipSoon();
      }});
    }}
    function testsForMeasuredSelection(sel) {{
      if (!sel || sel.kind !== 'chunk') return [];
      const row = measuredList.find(r => r.key === sel.key) || {{}};
      return measuredChunkTests[row.label] || measuredChunkTests[sel.key] || [];
    }}
    function testsForMeasuredRow(row) {{
      if (!row) return [];
      return measuredChunkTests[row.label] || measuredChunkTests[row.key] || [];
    }}
    function emptyMeasuredOutcomes() {{
      return {{ pass: 0, fail: 0, mute: 0, skip: 0, nl: 0, other: 0, duration: 0, n: 0 }};
    }}
    function measuredOutcomeKey(test) {{
      const tags = measuredErrorTags(test && test.error_type).map(tag => String(tag).toUpperCase().replace(/\\s+/g, '_'));
      const label = measuredStatusLabel(test && test.status);
      if (label === 'NOT LAUNCHED' || tags.includes('NOT_LAUNCHED')) return 'nl';
      if (label === 'MUTE') return 'mute';
      if (label === 'SKIP') return 'skip';
      if (label === 'FAIL') return 'fail';
      if (label === 'PASS') return 'pass';
      return 'other';
    }}
    function addMeasuredOutcomes(dst, src) {{
      dst.pass += src.pass;
      dst.fail += src.fail;
      dst.mute += src.mute;
      dst.skip += src.skip;
      dst.nl += src.nl;
      dst.other += src.other;
      dst.duration += src.duration;
      dst.n += src.n;
      return dst;
    }}
    function outcomesForTests(tests) {{
      const out = emptyMeasuredOutcomes();
      (tests || []).forEach(test => {{
        out[measuredOutcomeKey(test)] += 1;
        out.duration += Number(test.duration_sec) || 0;
        out.n += 1;
      }});
      return out;
    }}
    function outcomesForRows(rows) {{
      const out = emptyMeasuredOutcomes();
      (rows || []).forEach(row => {{
        if (!row._outcomes) row._outcomes = outcomesForTests(testsForMeasuredRow(row));
        addMeasuredOutcomes(out, row._outcomes);
      }});
      return out;
    }}
    function measuredDurText(sec) {{
      const s = Number(sec) || 0;
      if (s >= 3600) return (s / 3600).toFixed(1) + 'h';
      if (s >= 90) return (s / 60).toFixed(1) + 'm';
      return s.toFixed(1) + 's';
    }}
    function measuredOutcomesHtml(out) {{
      const parts = [];
      const add = (n, cls, label) => {{
        if (!n) return;
        parts.push('<span class="st ' + cls + '">' + n + ' ' + label + '</span>');
      }};
      add(out.pass, 'st-pass', 'pass');
      add(out.fail, 'st-fail', 'fail');
      add(out.mute, 'st-mute', 'mute');
      add(out.skip, 'st-skip', 'skip');
      add(out.nl, 'st-nl', 'nl');
      add(out.other, 'st-other', '?');
      parts.push('<span class="odur">' + measuredDurText(out.duration) + '</span>');
      return parts.join('');
    }}
    function plotMeasuredTestLane(tests) {{
      const el = document.getElementById('measuredTestsLane');
      const list = document.getElementById('measuredTestsList');
      if (!el) return;
      if (!tests || !tests.length) {{
        _measuredActiveTests = [];
        el.style.display = 'none';
        if (list) {{ list.style.display = 'none'; list.innerHTML = ''; }}
        return;
      }}
      el.style.display = '';
      el.style.height = '90px';
      _measuredActiveTests = tests;
      const xs = measuredLayers.xs_evlog_sec || [];
      const traces = tests.map((t, i) => {{
        const color = MEASURED_PALETTE[i % MEASURED_PALETTE.length];
        return {{
          type: 'scatter',
          mode: 'lines',
          x: [xToDisplay(t.start_sec), xToDisplay(t.end_sec)],
          y: [1, 1],
          line: {{ width: 18, color: color }},
          name: t.name,
          hovertemplate: t.name + '<br>' + Number(t.duration_sec || 0).toFixed(1) + 's' +
            (measuredStatusLabel(t.status) ? '<br>' + measuredStatusLabel(t.status) : '') +
            (measuredErrorTags(t.error_type).length ? '<br>' + measuredErrorTags(t.error_type).map(measuredErrorBadgeLabel).join(', ') : '') +
            '<extra></extra>',
          showlegend: false,
        }};
      }});
      if (window.Plotly) {{
        Plotly.newPlot(el, traces, {{
          title: 'Tests in this chunk',
          xaxis: axisLayout(xs.map(xToDisplay)),
          yaxis: {{ visible: false, range: [0.4, 1.6] }},
          hovermode: 'closest',
          margin: {{ t: 36, r: 20, b: 24, l: 40 }},
          height: 90,
        }}, {{ responsive: true }});
        el.on('plotly_hover', (ev) => {{
          if (!ev || !ev.points || !ev.points.length) return;
          measuredTestHoverLine(ev.points[0]);
          showChartTip(ev);
        }});
        el.on('plotly_unhover', () => {{
          highlightMeasuredTest('');
          hideChartTipSoon();
        }});
      }}
      if (list) {{
        list.style.display = '';
        list.innerHTML = tests.map((t, i) => {{
          const color = MEASURED_PALETTE[i % MEASURED_PALETTE.length];
          const from = formatTimeLabel(xToDisplay(t.start_sec));
          const to = formatTimeLabel(xToDisplay(t.end_sec));
          const status = measuredStatusLabel(t.status);
          const badges = (status ? '<span class="st ' + measuredStatusClass(status) + '">' + escapeHtml(status) + '</span>' : '') +
            measuredErrorTags(t.error_type).map(tag =>
              '<span class="ebadge ' + measuredErrorBadgeClass(tag) + '">' + escapeHtml(measuredErrorBadgeLabel(tag)) + '</span>'
            ).join('');
          const logs = measuredLogEntries(t.links).map(link =>
            '<a href="' + escapeHtml(link.url) + '" target="_blank" rel="noopener">' + escapeHtml(link.name) + '</a>'
          ).join('');
          return '<li data-name="' + escapeHtml(t.name) + '"><span class="swatch" style="background:' + color + '"></span>' +
            '<span class="tbody"><span class="tline"><span class="tname">' + escapeHtml(t.name) + '</span>' + badges + '</span>' +
            (logs ? '<span class="tlogs">' + logs + '</span>' : '') + '</span>' +
            '<span class="tmeta">' + escapeHtml(from) + ' – ' + escapeHtml(to) +
            ' · ' + Number(t.duration_sec || 0).toFixed(1) + 's</span></li>';
        }}).join('');
      }}
    }}
    function applyMeasuredTestBands(tests) {{
      const shapes = (tests || []).map((t, i) => {{
        const color = MEASURED_PALETTE[i % MEASURED_PALETTE.length];
        return {{
          type: 'rect',
          xref: 'x',
          yref: 'paper',
          x0: xToDisplay(t.start_sec),
          x1: xToDisplay(t.end_sec),
          y0: 0,
          y1: 1,
          fillcolor: hexToRgba(color, 0.10),
          line: {{ width: 1, color: hexToRgba(color, 0.35) }},
          layer: 'below',
        }};
      }});
      ['measuredCpu', 'measuredRam', 'measuredWrite', 'measuredRead', 'measuredPressure'].forEach(id => {{
        const plot = document.getElementById(id);
        if (plot && window.Plotly) Plotly.relayout(plot, {{ shapes: shapes, annotations: [] }});
      }});
    }}
    function measuredAgg(rows) {{
      let cpu = 0, ram = 0, write = 0, read = 0;
      (rows || []).forEach(row => {{
        cpu += Number(row.max_cpu) || 0;
        ram += Number(row.max_ram_gb) || 0;
        write += Number(row.max_write_mbps) || 0;
        read += Number(row.max_read_mbps) || 0;
      }});
      return {{ n: (rows || []).length, cpu: cpu, ram_gb: ram, write_mbps: write, read_mbps: read }};
    }}
    function measuredSignalsText(rows) {{
      const src = rows || [];
      const max = key => {{
        const vals = src.map(r => Number(r[key])).filter(Number.isFinite);
        return vals.length ? Math.max(...vals) : null;
      }};
      const sum = key => {{
        const vals = src.map(r => Number(r[key])).filter(Number.isFinite);
        return vals.length ? vals.reduce((a, b) => a + b, 0) : null;
      }};
      const fmt = value => value == null ? '—' : Number(value).toFixed(1) + '%';
      const fields = ['max_cpu_throttled_pct','max_cpu_psi_pct','max_memory_psi_pct','max_io_psi_pct'];
      const present = src.some(r => fields.some(k => r[k] != null));
      if (!present) return 'cgroup pressure: n/a';
      const events = ['memory_high_events_delta','memory_max_events_delta','memory_oom_events_delta','memory_oom_kill_events_delta'].map(sum);
      const throttledPeriods = sum('cpu_nr_throttled_delta');
      const throttledUsec = sum('cpu_throttled_usec_delta');
      const throttleCounts = throttledPeriods != null || throttledUsec != null
        ? ' (' + (throttledPeriods == null ? '—' : Math.round(throttledPeriods)) + ' periods, ' + (throttledUsec == null ? '—' : (throttledUsec / 1000000).toFixed(2) + 's') + ')'
        : '';
      const text = 'throttle ' + fmt(max('max_cpu_throttled_pct')) + throttleCounts + ' · PSI CPU/mem/I/O ' +
        fmt(max('max_cpu_psi_pct')) + '/' + fmt(max('max_memory_psi_pct')) + '/' + fmt(max('max_io_psi_pct'));
      const haveEvents = events.some(v => v != null);
      return text + (haveEvents ? ' · mem high/max/oom/kill ' + events.map(v => v == null ? '—' : String(Math.round(v))).join('/') : '');
    }}
    function measuredStatsText(agg, hideCount) {{
      const body = agg.cpu.toFixed(2) + 'c · ' + agg.ram_gb.toFixed(2) + 'G · w' +
        agg.write_mbps.toFixed(1) + ' r' + agg.read_mbps.toFixed(1);
      if (hideCount) return body;
      return agg.n + (agg.n > 1 ? ' · Σ ' : ' · ') + body;
    }}
    function measuredAggExtremes(aggs) {{
      const ext = {{ cpu: 0, ram_gb: 0, write_mbps: 0, read_mbps: 0 }};
      (aggs || []).forEach(agg => {{
        ext.cpu = Math.max(ext.cpu, Number(agg.cpu) || 0);
        ext.ram_gb = Math.max(ext.ram_gb, Number(agg.ram_gb) || 0);
        ext.write_mbps = Math.max(ext.write_mbps, Number(agg.write_mbps) || 0);
        ext.read_mbps = Math.max(ext.read_mbps, Number(agg.read_mbps) || 0);
      }});
      return ext;
    }}
    function suiteChunkExtremes(rows) {{
      return measuredAggExtremes((rows || []).map(row => ({{
        cpu: Number(row.max_cpu) || 0,
        ram_gb: Number(row.max_ram_gb) || 0,
        write_mbps: Number(row.max_write_mbps) || 0,
        read_mbps: Number(row.max_read_mbps) || 0,
      }})));
    }}
    function measuredStatMark(text, isMax) {{
      return isMax ? ('<span class="stat-max">' + text + '</span>') : text;
    }}
    function measuredStatPartsHtml(cpu, ram, write, read, extremes, siblingCount) {{
      const cpuTxt = Number(cpu || 0).toFixed(2);
      const ramTxt = Number(ram || 0).toFixed(2);
      const writeTxt = Number(write || 0).toFixed(1);
      const readTxt = Number(read || 0).toFixed(1);
      const canMark = siblingCount > 1 && extremes;
      return measuredStatMark(cpuTxt + 'c', canMark && extremes.cpu > 0 && cpuTxt === extremes.cpu.toFixed(2)) +
        ' · ' + measuredStatMark(ramTxt + 'G', canMark && extremes.ram_gb > 0 && ramTxt === extremes.ram_gb.toFixed(2)) +
        ' · ' + measuredStatMark('w' + writeTxt, canMark && extremes.write_mbps > 0 && writeTxt === extremes.write_mbps.toFixed(1)) +
        ' ' + measuredStatMark('r' + readTxt, canMark && extremes.read_mbps > 0 && readTxt === extremes.read_mbps.toFixed(1));
    }}
    function measuredStatsHtml(agg, extremes, siblingCount, hideCount) {{
      const body = measuredStatPartsHtml(agg.cpu, agg.ram_gb, agg.write_mbps, agg.read_mbps, extremes, siblingCount);
      if (hideCount) return body;
      return agg.n + (agg.n > 1 ? ' · Σ ' : ' · ') + body;
    }}
    function measuredChunkStatsHtml(row, extremes, chunkCount) {{
      return measuredStatPartsHtml(row.max_cpu, row.max_ram_gb, row.max_write_mbps, row.max_read_mbps, extremes, chunkCount);
    }}
    function buildMeasuredForest() {{
      const root = {{ name: 'all', path: '', children: {{}}, rows: [] }};
      measuredList.forEach(row => {{
        const parts = String(row.suite || 'unknown').split('/').filter(Boolean);
        let node = root;
        node.rows.push(row);
        let path = '';
        parts.forEach(part => {{
          path = path ? (path + '/' + part) : part;
          if (!node.children[part]) node.children[part] = {{ name: part, path: path, children: {{}}, rows: [] }};
          node = node.children[part];
          node.rows.push(row);
        }});
      }});
      return root;
    }}
    const measuredForest = buildMeasuredForest();
    const measuredOpen = new Set();
    function measuredAncestorPaths(path) {{
      const parts = String(path || '').split('/').filter(Boolean);
      const out = [];
      let cur = '';
      parts.forEach(part => {{
        cur = cur ? (cur + '/' + part) : part;
        out.push(cur);
      }});
      return out;
    }}
    function openMeasuredPath(path, includeSelf) {{
      measuredAncestorPaths(path).forEach((p, i, arr) => {{
        if (includeSelf || i < arr.length - 1) measuredOpen.add(p);
      }});
    }}
    function measuredRowsForSelection(sel) {{
      if (!sel || sel.kind === 'all') return measuredList;
      if (sel.kind === 'chunk') return measuredList.filter(row => row.key === sel.key);
      const path = sel.path || sel.suite || '';
      return measuredList.filter(row => row.suite === path || String(row.suite || '').startsWith(path + '/'));
    }}
    function measuredLabelsForSelection(sel) {{
      return measuredRowsForSelection(sel).map(row => row.label);
    }}
    function selectMeasured(sel) {{
      const rows = measuredRowsForSelection(sel);
      const labels = rows.map(row => row.label);
      const agg = measuredAgg(rows);
      const meta = document.getElementById('measuredMeta');
      const title = (!sel || sel.kind === 'all')
        ? 'all suites'
        : (sel.kind === 'chunk' ? ((measuredList.find(r => r.key === sel.key) || {{}}).label || sel.key) : (sel.path || sel.suite));
      const outcomes = outcomesForRows(rows);
      if (meta) {{
        meta.innerHTML = escapeHtml(title + ': ' + measuredStatsText(agg)) +
          ' · ' + measuredOutcomesHtml(outcomes) +
          '<div class="muted" style="margin-top:3px">' + escapeHtml(measuredSignalsText(rows)) + '</div>';
      }}
      const suffix = (!sel || sel.kind === 'all') ? ' (all)' : (sel.kind === 'chunk' ? ' (one chunk)' : ' / ' + (sel.path || sel.suite));
      plotMeasuredLayers(labels, suffix);
      plotMeasuredPressure(labels, suffix);
      const chunkTests = testsForMeasuredSelection(sel);
      plotMeasuredTestLane(chunkTests);
      applyMeasuredTestBands(chunkTests);
      if (sel && (sel.path || sel.suite)) openMeasuredPath(sel.path || sel.suite, false);
      if (sel && sel.kind === 'chunk' && (sel.path || sel.suite)) openMeasuredPath(sel.path || sel.suite, true);
      renderMeasuredTree(sel);
    }}
    function renderMeasuredNode(node, depth, sel, siblingExtremes, siblingCount) {{
      const childNames = Object.keys(node.children).sort();
      const chunks = node.rows.filter(row => row.suite === node.path).slice().sort((a, b) => {{
        const ca = a.chunk == null ? 1e9 : Number(a.chunk);
        const cb = b.chunk == null ? 1e9 : Number(b.chunk);
        return ca - cb;
      }});
      const hasKids = childNames.length > 0 || chunks.length > 0;
      const open = measuredOpen.has(node.path);
      const selected = sel && sel.kind === 'prefix' && sel.path === node.path;
      let html = '<div class="mt-row" style="padding-left:' + (depth * 12) + 'px">';
      html += hasKids
        ? ('<button type="button" class="mt-twist" data-toggle="' + escapeHtml(node.path) + '">' + (open ? '▼' : '▶') + '</button>')
        : '<span class="mt-spacer"></span>';
      html += '<button type="button" class="mt-pick' + (selected ? ' on' : '') + '" data-kind="prefix" data-path="' + escapeHtml(node.path) + '">' +
        '<span class="mt-name">' + escapeHtml(node.name) + '</span>' +
        '<span class="stats">' + measuredStatsHtml(measuredAgg(node.rows), siblingExtremes, siblingCount) + '</span>' +
        '<span class="stats outcomes">' + measuredOutcomesHtml(outcomesForRows(node.rows)) + '</span>' +
        '<span class="stats">' + escapeHtml(measuredSignalsText(node.rows)) + '</span></button></div>';
      if (open && hasKids) {{
        const kids = childNames.map(name => node.children[name]);
        const kidExt = measuredAggExtremes(kids.map(k => measuredAgg(k.rows)));
        kids.forEach(child => {{ html += renderMeasuredNode(child, depth + 1, sel, kidExt, kids.length); }});
        const extremes = suiteChunkExtremes(chunks);
        chunks.forEach(row => {{
          const on = sel && sel.kind === 'chunk' && sel.key === row.key;
          html += '<div class="mt-row mt-chunk" style="padding-left:' + ((depth + 1) * 12) + 'px">' +
            '<span class="mt-spacer"></span>' +
            '<button type="button" class="mt-pick' + (on ? ' on' : '') + '" data-kind="chunk" data-key="' + escapeHtml(row.key) + '" data-path="' + escapeHtml(node.path) + '">' +
            '<span class="mt-name">' + escapeHtml(row.chunk == null ? row.label : '[' + row.chunk + ']') + '</span>' +
            '<span class="stats">' + measuredChunkStatsHtml(row, extremes, chunks.length) + '</span>' +
            '<span class="stats outcomes">' + measuredOutcomesHtml(outcomesForRows([row])) + '</span>' +
            '<span class="stats">' + escapeHtml(measuredSignalsText([row])) + '</span></button></div>';
        }});
      }}
      return html;
    }}
    function renderMeasuredTree(sel) {{
      const tree = document.getElementById('measuredTree');
      if (!tree) return;
      const childNames = Object.keys(measuredForest.children).sort();
      const allOn = !sel || sel.kind === 'all';
      let html = '<div class="mt-row"><span class="mt-spacer"></span>' +
        '<button type="button" class="mt-pick' + (allOn ? ' on' : '') + '" data-kind="all">' +
        '<span class="mt-name">all</span><span class="stats">' + measuredStatsText(measuredAgg(measuredList)) + '</span>' +
        '<span class="stats outcomes">' + measuredOutcomesHtml(outcomesForRows(measuredList)) + '</span>' +
        '<span class="stats">' + escapeHtml(measuredSignalsText(measuredList)) + '</span></button></div>';
      const topKids = childNames.map(name => measuredForest.children[name]);
      const topExt = measuredAggExtremes(topKids.map(k => measuredAgg(k.rows)));
      topKids.forEach(child => {{ html += renderMeasuredNode(child, 1, sel, topExt, topKids.length); }});
      tree.innerHTML = html;
    }}
    const measuredTreeEl = document.getElementById('measuredTree');
    if (measuredTreeEl) {{
      measuredTreeEl.addEventListener('click', (ev) => {{
        const twist = ev.target.closest('[data-toggle]');
        if (twist) {{
          const path = twist.dataset.toggle;
          if (measuredOpen.has(path)) measuredOpen.delete(path);
          else measuredOpen.add(path);
          renderMeasuredTree(window._measuredSel || {{kind: 'all'}});
          return;
        }}
        const pick = ev.target.closest('.mt-pick');
        if (!pick) return;
        if (pick.dataset.kind === 'all') selectMeasured({{kind: 'all'}});
        else if (pick.dataset.kind === 'chunk') selectMeasured({{kind: 'chunk', key: pick.dataset.key, path: pick.dataset.path, suite: pick.dataset.path}});
        else selectMeasured({{kind: 'prefix', path: pick.dataset.path, suite: pick.dataset.path}});
      }});
    }}
    const _selectMeasured = selectMeasured;
    selectMeasured = function(sel) {{
      window._measuredSel = sel || {{kind: 'all'}};
      _selectMeasured(sel || {{kind: 'all'}});
    }};
    function buildMeasuredTestIndex() {{
      const out = [];
      const seen = new Set();
      Object.keys(measuredChunkTests || {{}}).forEach(label => {{
        if (String(label).startsWith('ya-test-')) return;
        const row = measuredList.find(r => r.label === label);
        (measuredChunkTests[label] || []).forEach(test => {{
          const id = label + '|' + test.name;
          if (seen.has(id)) return;
          seen.add(id);
          out.push({{
            name: test.name,
            label: label,
            key: row && row.key,
            suite: row && row.suite,
            chunk: row && row.chunk,
          }});
        }});
      }});
      return out;
    }}
    const measuredTestIndex = buildMeasuredTestIndex();
    function jumpToMeasuredTest(hit) {{
      if (!hit || !hit.key) return;
      if (hit.suite) openMeasuredPath(hit.suite, true);
      selectMeasured({{kind: 'chunk', key: hit.key, path: hit.suite, suite: hit.suite}});
      highlightMeasuredTest(hit.name);
    }}
    function renderMeasuredTestHits(query) {{
      const box = document.getElementById('measuredTestHits');
      if (!box) return;
      const q = String(query || '').trim().toLowerCase();
      if (!q) {{
        box.style.display = 'none';
        box.innerHTML = '';
        return;
      }}
      const hits = measuredTestIndex.filter(hit =>
        String(hit.name || '').toLowerCase().includes(q) ||
        String(hit.label || '').toLowerCase().includes(q)
      ).slice(0, 40);
      box.style.display = hits.length ? '' : 'none';
      box.innerHTML = hits.map((hit, i) =>
        '<li data-idx="' + i + '"><div class="hit-name">' + escapeHtml(hit.name) + '</div>' +
        '<div class="hit-chunk">' + escapeHtml(hit.label || '') + '</div></li>'
      ).join('') || '<li>No matches</li>';
      box._hits = hits;
    }}
    const measuredSearchEl = document.getElementById('measuredTestSearch');
    const measuredHitsEl = document.getElementById('measuredTestHits');
    if (measuredSearchEl) {{
      measuredSearchEl.addEventListener('input', () => renderMeasuredTestHits(measuredSearchEl.value));
    }}
    if (measuredHitsEl) {{
      measuredHitsEl.addEventListener('click', (ev) => {{
        const li = ev.target.closest('li');
        if (!li || li.dataset.idx == null) return;
        const hit = (measuredHitsEl._hits || [])[Number(li.dataset.idx)];
        jumpToMeasuredTest(hit);
      }});
    }}
    if (measuredWrap && measuredList.length && measuredLayers.xs_evlog_sec) {{
      measuredWrap.style.display = '';
      const suites = [...new Set(measuredList.map(row => row.suite).filter(Boolean))];
      if (suites.length === 1) {{
        openMeasuredPath(suites[0], false);
        selectMeasured({{kind: 'prefix', path: suites[0], suite: suites[0]}});
      }} else {{
        selectMeasured({{kind: 'all'}});
      }}
    }}
    const _toggleSuiteMarkersOrig = toggleSuiteMarkers;
    toggleSuiteMarkers = function(suitePath, enabled) {{
      _toggleSuiteMarkersOrig(suitePath, enabled);
      if (!enabled || !measuredList.length) return;
      if (measuredList.some(row => row.suite === suitePath || String(row.suite || '').startsWith(suitePath + '/'))) {{
        openMeasuredPath(suitePath, false);
        selectMeasured({{kind: 'prefix', path: suitePath, suite: suitePath}});
      }}
    }};

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
