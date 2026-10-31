"""HTML dashboard rendering for tests resource dashboard.

Contains build_html_dashboard which takes a pre-computed payload and writes
the HTML shell plus a sibling *_data.json (fetched at runtime) with Plotly.js
charts, CPU suggestions table and ya.make update script generator.
"""

from __future__ import annotations

import html as html_lib
from pathlib import Path
from typing import Any, Optional
from urllib.parse import quote

try:
    from .dashboard_html_payload import build_dashboard_payload
    from .html_embed import js_script_json, write_dashboard_data_json
except ImportError:
    from dashboard_html_payload import build_dashboard_payload  # type: ignore[no-redef]
    from html_embed import js_script_json, write_dashboard_data_json  # type: ignore[no-redef]


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
    ya_activity_deltas: Optional[dict[str, list[tuple[float, int]]]] = None,
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
        ya_activity_deltas=ya_activity_deltas,
    )

    rf = (run_config or {}).get("runner_footprint") or {}
    rf_vcpu = rf.get("vcpu", "?")
    rf_ram = rf.get("ram_gb", "?")
    rf_budget = rf.get("mem_budget_gb", "?")
    rf_ya_mem = rf.get("ya_make_mem_limit_gb", "?")
    out_data = out_html.with_name(out_html.stem + "_data.json")
    write_dashboard_data_json(out_data, payload)
    data_url_js = js_script_json(out_data.name)
    suggestions_name = out_html.stem + "_suggestions.json"
    suggestions_href = html_lib.escape(suggestions_name, quote=True)
    suggestions_label = html_lib.escape(suggestions_name)
    suite_filter_html = html_lib.escape(suite_filter or "ALL SUITES", quote=True)

    # Inline SVG favicon (data URI) so the browser tab is recognizable without
    # shipping a separate .ico next to the HTML artifact.
    favicon_svg = (
        "<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 32 32'>"
        "<rect width='32' height='32' rx='6' fill='#0f172a'/>"
        "<rect x='5' y='18' width='5' height='9' rx='1' fill='#38bdf8'/>"
        "<rect x='13' y='11' width='5' height='16' rx='1' fill='#34d399'/>"
        "<rect x='21' y='7' width='5' height='20' rx='1' fill='#fbbf24'/>"
        "<path d='M5 14l6-5 5 3 8-7' fill='none' stroke='#e2e8f0' stroke-width='2' "
        "stroke-linecap='round' stroke-linejoin='round'/>"
        "</svg>"
    )
    favicon_href = "data:image/svg+xml," + quote(favicon_svg, safe="")

    html = f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>Chunk Resource Dashboard</title>
  <link rel="icon" type="image/svg+xml" href="{favicon_href}" />
  <script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
  <style>
    body {{ font-family: Arial, sans-serif; margin: 16px; }}
    .toolbar {{ position: sticky; top: 0; z-index: 20; display: flex; gap: 8px; align-items: center; margin: 8px 0 10px 0; padding: 8px 0; background: rgba(255,255,255,0.96); backdrop-filter: blur(2px); }}
    .toolbar input[type="text"] {{ min-width: 340px; padding: 6px 8px; border: 1px solid #d0d7de; border-radius: 6px; }}
    .toolbar button {{ padding: 6px 10px; border: 1px solid #d0d7de; border-radius: 6px; background: #f6f8fa; cursor: pointer; }}
    .cpu-help {{ margin-top: 6px; padding: 8px 10px; border-radius: 8px; border: 1px solid #eaeef2; background: #fff; font-size: 12px; line-height: 1.45; color: #57606a; }}
    .cpu-help ol, .cpu-help ul {{ margin: 6px 0 6px 18px; padding: 0; }}
    /* Secondary collapses: quiet chrome, not H2. */
    details.aux-fold {{
      margin: 4px 0 8px 0;
    }}
    details.aux-fold > summary {{
      cursor: pointer;
      font-size: 12px;
      font-weight: 500;
      color: #656d76;
      list-style: none;
    }}
    details.aux-fold > summary::-webkit-details-marker {{ display: none; }}
    details.aux-fold > summary::before {{
      content: "▸ ";
      color: #8c959f;
      font-weight: 400;
    }}
    details.aux-fold[open] > summary::before {{ content: "▾ "; }}
    .section-label {{
      display: flex;
      align-items: center;
      gap: 8px;
      margin: 0 0 8px 0;
      font-size: 13px;
      font-weight: 600;
      color: #57606a;
      line-height: 1.3;
    }}
    .section-label .help-btn {{
      padding: 0 7px;
      min-width: auto;
      font-size: 12px;
      font-weight: 600;
      color: #57606a;
    }}
    /* Closed: one muted line; hidden entirely when empty (via .is-visible). */
    details.quiet-link-fold {{
      display: none;
      margin: 2px 0 0 0;
      border: 0;
      padding: 0;
      background: transparent;
    }}
    details.quiet-link-fold.is-visible {{ display: block; }}
    details.quiet-link-fold > summary {{
      cursor: pointer;
      list-style: none;
      font-size: 11px;
      font-weight: 400;
      color: #8c959f;
      line-height: 1.4;
    }}
    details.quiet-link-fold > summary::-webkit-details-marker {{ display: none; }}
    details.quiet-link-fold > summary::before {{
      content: "▸ ";
      color: #afb8c1;
    }}
    details.quiet-link-fold[open] > summary::before {{ content: "▾ "; }}
    details.quiet-link-fold[open] > summary {{ color: #656d76; }}
    details.quiet-link-fold .clickbox {{
      margin: 6px 0 0 0;
      max-height: 280px;
    }}
    .dash-footer {{
      margin: 28px 0 8px 0;
      padding-top: 12px;
      border-top: 1px solid #eaeef2;
    }}
    .dash-footer > summary {{
      cursor: pointer;
      list-style: none;
      font-size: 12px;
      font-weight: 500;
      color: #8c959f;
    }}
    .dash-footer > summary::-webkit-details-marker {{ display: none; }}
    .dash-footer > summary::before {{
      content: "▸ ";
      color: #afb8c1;
    }}
    .dash-footer[open] > summary::before {{ content: "▾ "; }}
    .dash-footer-body {{
      margin-top: 8px;
      padding: 8px 10px;
      border: 1px solid #eaeef2;
      border-radius: 8px;
      background: #fafbfc;
    }}
    .dash-footer-body .cpu-help {{
      display: block;
      margin-top: 6px;
    }}
    .dash-footer-body .box {{
      background: #fff;
      border: 1px solid #eaeef2;
      border-radius: 6px;
      padding: 8px 10px;
      font-size: 12px;
      line-height: 1.45;
      color: #57606a;
    }}
    .dash-footer-body ul {{ margin: 6px 0 0 18px; padding: 0; }}
    .tabs {{ display: flex; gap: 8px; margin: 8px 0 12px 0; }}
    .tabbtn {{ padding: 6px 10px; border: 1px solid #d0d7de; border-radius: 6px; background: #f6f8fa; cursor: pointer; }}
    .tabbtn.active {{ background: #e7f3ff; border-color: #8cc8ff; }}
    .tab {{ display: none; }}
    .tab.active {{ display: block; }}
    .row {{ display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }}
    .row1 {{ display: grid; grid-template-columns: 1fr; gap: 16px; }}
    .chart {{ width: 100%; height: 420px; min-width: 0; }}
    .wide {{ width: 100%; height: 520px; }}
    .chart-grid {{
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 20px 28px;
      margin: 4px 0 18px 0;
      align-items: start;
    }}
    @media (max-width: 1100px) {{
      .chart-grid {{ grid-template-columns: 1fr; gap: 18px; }}
    }}
    .chart-tile {{
      min-width: 0;
      background: transparent;
      padding: 0;
      display: flex;
      flex-direction: column;
      border: 0;
      box-shadow: none;
    }}
    .chart-tile-head {{
      display: flex;
      align-items: baseline;
      justify-content: space-between;
      gap: 12px;
      margin: 0 0 2px 2px;
      min-height: 22px;
    }}
    .chart-tile-title {{
      font-size: 15px;
      font-weight: 600;
      letter-spacing: 0.01em;
      color: #0f172a;
      text-transform: none;
    }}
    .chart-tile-sub {{
      display: block;
      margin-top: 2px;
      font-size: 12px;
      font-weight: 400;
      color: #656d76;
    }}
    .chart-tile-expand {{
      padding: 0;
      border: 0;
      background: transparent;
      cursor: pointer;
      font-size: 11px;
      line-height: 1;
      color: #0969da;
      opacity: 0.72;
    }}
    .chart-tile-expand:hover {{ opacity: 1; text-decoration: underline; }}
    .chart-pane {{
      width: 100%;
      height: 280px;
      min-width: 0;
      border-radius: 6px;
      overflow: hidden;
    }}
    /*
     * Reference width/height = ya make concurrency (.span-2).
     * Expand promotes a tile to that same full-row slot; expand all ⇒ old
     * vertical stack of wide charts.
     */
    .chart-tile.span-2,
    .chart-tile.expanded {{
      grid-column: 1 / -1;
      width: 100%;
    }}
    .chart-tile.span-2 .chart-pane,
    .chart-tile.expanded .chart-pane {{
      height: 520px;
    }}
    /* Hover/click detail panels stay visible in tile mode too — expand only
       changes chart width/height, not whether drill-down UI exists. */
    .chart-tile-meta {{
      display: block;
      margin-top: 8px;
    }}
    .chart-tile-meta .hoverbox,
    .chart-tile-meta .clickbox {{
      margin: 6px 0 12px 0;
      max-height: 180px;
      border-color: #eaeef2;
      background: #fafbfc;
    }}
    .chart-tile.expanded .chart-tile-meta .hoverbox {{
      max-height: 220px;
    }}
    .chart-tile.expanded .chart-tile-meta .clickbox {{
      max-height: 320px;
    }}
    .chart-secondary {{
      margin: 0 0 16px 0;
      border: 0;
      border-top: 1px solid #eaeef2;
      border-radius: 0;
      padding: 10px 0 0 0;
      background: transparent;
    }}
    .chart-secondary > summary {{
      cursor: pointer;
      font-weight: 600;
      font-size: 12px;
      color: #57606a;
      list-style: none;
    }}
    .chart-secondary > summary::-webkit-details-marker {{ display: none; }}
    .chart-secondary > summary::before {{
      content: "▸ ";
      color: #8c959f;
      font-weight: 400;
    }}
    .chart-secondary[open] > summary::before {{ content: "▾ "; }}
    .chart-secondary .chart-grid {{
      margin: 12px 0 0 0;
    }}
    .tz-hover .hoverlayer g.legend {{ display: none !important; }}
    #chartTip {{ position: fixed; z-index: 40; display: none; pointer-events: none; background: #fff; border: 1px solid #444; padding: 6px 8px; font: 12px Arial, sans-serif; line-height: 1.35; white-space: pre; box-shadow: 0 1px 4px rgba(0,0,0,0.12); }}
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
      margin-top: 6px;
      font-size: 12px;
      line-height: 1.4;
      color: #475569;
    }}
    .monitor-box a {{
      color: #2563eb;
      text-decoration: none;
      font-weight: 500;
    }}
    .monitor-box a:hover {{ text-decoration: underline; }}
    .monitor-url {{
      margin-left: 8px;
      color: #6b7280;
      font-size: 11px;
      font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", monospace;
      word-break: break-all;
    }}
    .headline-stats {{
      margin-bottom: 12px;
      padding: 10px 12px;
      border: 1px solid #d0d7de;
      border-radius: 8px;
      background: #f8fafc;
    }}
    .headline-stats-title {{
      font-size: 11px;
      font-weight: 500;
      color: #656d76;
      letter-spacing: 0.03em;
      text-transform: uppercase;
      margin-bottom: 6px;
    }}
    .headline-run {{
      display: flex;
      flex-wrap: nowrap;
      align-items: center;
      gap: 6px 16px;
      font-size: 13px;
      line-height: 1.45;
      margin-bottom: 12px;
      color: #57606a;
    }}
    .headline-run b {{
      font-variant-numeric: tabular-nums;
      font-size: 16px;
      font-weight: 700;
      color: #0f172a;
    }}
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
  <div id="dashboardLoadError" style="display:none;margin:16px;padding:12px 16px;border:1px solid #fca5a5;background:#fef2f2;border-radius:8px;color:#991b1b;font-size:14px;"></div>
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
    <div id="monitoringLinkWrap" class="monitor-box">
      <span>Runner monitoring:</span>
      <a id="monitoringLink" href="#" target="_blank" rel="noopener noreferrer">Open dashboard</a>
      <span id="monitoringLinkMeta" class="monitor-url"></span>
    </div>
  </div>
  <div id="headlineStats" class="headline-stats" style="display:none;">
    <div class="headline-stats-title">Resource summary</div>
    <div id="headlineStatsGrid" class="headline-stats-grid"></div>
  </div>
  <div id="cpuSuggestionsSection" style="display: none; margin: 16px 0;">
    <div class="section-label">
      <span>CPU suggestions</span>
      <button id="cpuHelpToggle" class="help-btn" type="button" title="Open recommendation docs in footer">?</button>
    </div>
    <p id="syntheticNote" style="display: none; font-size: 12px; color: #b8860b; margin: 4px 0;"></p>
    <p style="font-size:12px;color:#64748b;margin:4px 0 8px 0;">Full cell hover explanations: <a href="{suggestions_href}">{suggestions_label}</a></p>
    <div id="cpuSuggestionsTable" class="clickbox"></div>
    <details id="heavyTestsDetails" class="quiet-link-fold">
      <summary>Long/heavy near timeout (0)</summary>
      <div id="cpuHeavyTestsTable" class="clickbox"></div>
    </details>
  </div>
  <div class="toolbar">
    <label for="suiteSearch">Search suite:</label>
    <input id="suiteSearch" type="text" placeholder="e.g. ydb/core/tx/schemeshard/ut_cdc_stream_reboots" />
    <button onclick="clearSuiteSearch()">Clear</button>
    <label for="tzSelect">Timezone:</label>
    <select id="tzSelect" onchange="applyTimezoneToAllCharts()" style="padding:6px 8px;border:1px solid #d0d7de;border-radius:6px;">
      <option value="Europe/Moscow">MSK</option>
      <option value="UTC">UTC</option>
      <option value="local" selected>Local</option>
    </select>
    <button onclick="clearAllMarkers()" title="Remove all suite markers from charts">Clear markers</button>
    <span style="display:flex;align-items:center;gap:6px;font-size:12px;border:1px solid #d0d7de;border-radius:6px;padding:3px 6px;background:#fff;color:#57606a;">
      <span>Marker types:</span>
      <label><input type="checkbox" id="mkStart" checked onchange="applyMarkersToCharts()">start</label>
      <label><input type="checkbox" id="mkEnd" checked onchange="applyMarkersToCharts()">end</label>
      <label><input type="checkbox" id="mkPar" onchange="applyMarkersToCharts()">par</label>
      <label><input type="checkbox" id="mkOthers" onchange="applyMarkersToCharts()">others</label>
      <label><input type="checkbox" id="mkCpu" checked onchange="applyMarkersToCharts()">cpu</label>
      <label><input type="checkbox" id="mkRam" checked onchange="applyMarkersToCharts()">ram</label>
      <label><input type="checkbox" id="mkError" onchange="applyMarkersToCharts()">errors</label>
      <label><input type="checkbox" id="mkTimeout" onchange="applyMarkersToCharts()">timeouts</label>
    </span>
    <span id="markersLegend" style="display:none;font-size:11px;line-height:1.5;border:1px solid #d0d7de;border-radius:6px;padding:4px 8px;background:#fafafa;color:#57606a;">
      <span>Markers:</span>
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
        <span>Layers:</span>
        <span class="layer-toggle-list">
          <label class="layer-toggle-item"><span>Metrics by suite</span><input type="checkbox" id="layerBySuite" checked onchange="applyLayerToggles()"></label>
          <label class="layer-toggle-item"><span>System CPU</span><input type="checkbox" id="layerSystemCpu" checked onchange="applyLayerToggles()"></label>
          <label class="layer-toggle-item"><span>System RAM</span><input type="checkbox" id="layerSystemRam" checked onchange="applyLayerToggles()"></label>
        </span>
      </span>
    </span>
  </div>
  <div class="chart-grid" id="yaActivityGrid">
    <div class="chart-tile span-2" data-pane="yaActivity">
      <div class="chart-tile-head">
        <span class="chart-tile-title">ya make concurrency<span class="chart-tile-sub">build, archives, cache, test workers, links</span></span>
      </div>
      <div id="yaActivity" class="chart-pane"></div>
    </div>
  </div>
  <div class="tabs" id="tabsBar" style="display: none;">
    <button id="tabBtnChunk" class="tabbtn active" onclick="showTab('chunkTab')">By Suite+Chunk</button>
    <button id="tabBtnSuite" class="tabbtn" onclick="showTab('suiteTab')">By Suite</button>
  </div>
  <div id="chunkTab" class="tab active" style="display: none;">
    <div class="chart-grid" id="chunkChartGrid">
      <div class="chart-tile" data-pane="activeLayerChunk">
        <div class="chart-tile-head">
          <span class="chart-tile-title">Active chunks<span class="chart-tile-sub">how many suite+chunk runs are alive at each moment</span></span>
          <button type="button" class="chart-tile-expand" onclick="toggleChartTile(this)" title="Expand to full width (same as ya make concurrency)" aria-expanded="false">expand</button>
        </div>
        <div id="activeLayerChunk" class="chart-pane"></div>
        <div class="chart-tile-meta">
          <div id="activeHoverChunk" class="hoverbox">Hover active-layer chart to see sorted contributors</div>
          <div id="activeClickChunk" class="clickbox">Click active-layer chart to pin a time and show sorted table</div>
        </div>
      </div>
      <div class="chart-tile" data-pane="cpuLayer">
        <div class="chart-tile-head">
          <span class="chart-tile-title">CPU · estimated cores<span class="chart-tile-sub">stacked by suite+chunk; red line is host monitor</span></span>
          <button type="button" class="chart-tile-expand" onclick="toggleChartTile(this)" title="Expand to full width (same as ya make concurrency)" aria-expanded="false">expand</button>
        </div>
        <div id="cpuLayer" class="chart-pane"></div>
        <div class="chart-tile-meta">
          <div id="cpuHover" class="hoverbox">Hover CPU chart to see sorted contributors</div>
          <div id="cpuClick" class="clickbox">Click CPU chart to pin a time and show sorted table</div>
        </div>
      </div>
      <div class="chart-tile" data-pane="ramLayer">
        <div class="chart-tile-head">
          <span class="chart-tile-title">RAM · GB<span class="chart-tile-sub">stacked by suite+chunk; red line is host monitor</span></span>
          <button type="button" class="chart-tile-expand" onclick="toggleChartTile(this)" title="Expand to full width (same as ya make concurrency)" aria-expanded="false">expand</button>
        </div>
        <div id="ramLayer" class="chart-pane"></div>
        <div class="chart-tile-meta">
          <div id="ramHover" class="hoverbox">Hover RAM chart to see sorted contributors</div>
          <div id="ramClick" class="clickbox">Click RAM chart to pin a time and show sorted table</div>
        </div>
      </div>
      <div class="chart-tile" data-pane="diskLayer" id="diskTileChunk" style="display:none;">
        <div class="chart-tile-head">
          <span class="chart-tile-title">Disk bandwidth<span class="chart-tile-sub">write above zero, read below (MB/s)</span></span>
          <button type="button" class="chart-tile-expand" onclick="toggleChartTile(this)" title="Expand to full width (same as ya make concurrency)" aria-expanded="false">expand</button>
        </div>
        <div id="diskLayer" class="chart-pane"></div>
      </div>
    </div>
    <div class="row">
      <div id="cpuPie" class="chart"></div>
      <div id="ramPie" class="chart"></div>
    </div>
    <div class="row1">
      <div id="activePie" class="chart"></div>
    </div>
  </div>
  <div id="suiteTab" class="tab">
    <div class="chart-grid" id="suiteChartGrid">
      <div class="chart-tile" data-pane="cpuLayerSuite">
        <div class="chart-tile-head">
          <span class="chart-tile-title">CPU by suite · estimated cores<span class="chart-tile-sub">stacked per suite; red line is host monitor</span></span>
          <button type="button" class="chart-tile-expand" onclick="toggleChartTile(this)" title="Expand to full width (same as ya make concurrency)" aria-expanded="false">expand</button>
        </div>
        <div id="cpuLayerSuite" class="chart-pane"></div>
        <div class="chart-tile-meta">
          <div id="cpuHoverSuite" class="hoverbox">Hover CPU chart to see sorted contributors</div>
          <div id="cpuClickSuite" class="clickbox">Click CPU chart to pin a time and show sorted table</div>
        </div>
      </div>
      <div class="chart-tile" data-pane="ramLayerSuite">
        <div class="chart-tile-head">
          <span class="chart-tile-title">RAM by suite · GB<span class="chart-tile-sub">stacked per suite; red line is host monitor</span></span>
          <button type="button" class="chart-tile-expand" onclick="toggleChartTile(this)" title="Expand to full width (same as ya make concurrency)" aria-expanded="false">expand</button>
        </div>
        <div id="ramLayerSuite" class="chart-pane"></div>
        <div class="chart-tile-meta">
          <div id="ramHoverSuite" class="hoverbox">Hover RAM chart to see sorted contributors</div>
          <div id="ramClickSuite" class="clickbox">Click RAM chart to pin a time and show sorted table</div>
        </div>
      </div>
      <div class="chart-tile" data-pane="diskLayerSuite" id="diskTileSuite" style="display:none;">
        <div class="chart-tile-head">
          <span class="chart-tile-title">Disk bandwidth<span class="chart-tile-sub">write above zero, read below (MB/s)</span></span>
          <button type="button" class="chart-tile-expand" onclick="toggleChartTile(this)" title="Expand to full width (same as ya make concurrency)" aria-expanded="false">expand</button>
        </div>
        <div id="diskLayerSuite" class="chart-pane"></div>
      </div>
    </div>
    <details class="chart-secondary" id="testsBySuiteDetails">
      <summary>Tests in parallel by suite</summary>
      <div class="chart-grid">
        <div class="chart-tile span-2 expanded" data-pane="testsLayerSuite">
          <div class="chart-tile-head">
            <span class="chart-tile-title">Tests running by suite<span class="chart-tile-sub">how many individual tests are in flight at each moment</span></span>
          </div>
          <div id="testsLayerSuite" class="chart-pane"></div>
          <div class="chart-tile-meta">
            <div id="testsHoverSuite" class="hoverbox">Hover tests chart to see sorted contributors</div>
            <div id="testsClickSuite" class="clickbox">Click tests chart to pin a time and show sorted table</div>
          </div>
        </div>
      </div>
    </details>
    <details class="chart-secondary" id="activeBySuiteDetails">
      <summary>Active chunks by suite</summary>
      <div class="chart-grid">
        <div class="chart-tile span-2 expanded" data-pane="activeLayerSuite">
          <div class="chart-tile-head">
            <span class="chart-tile-title">Active chunks by suite<span class="chart-tile-sub">how many suite runs are alive at each moment</span></span>
          </div>
          <div id="activeLayerSuite" class="chart-pane"></div>
          <div class="chart-tile-meta">
            <div id="activeHoverSuite" class="hoverbox">Hover active-layer chart to see sorted contributors</div>
            <div id="activeClickSuite" class="clickbox">Click active-layer chart to pin a time and show sorted table</div>
          </div>
        </div>
      </div>
    </details>
    <div class="row">
      <div id="cpuPieSuite" class="chart"></div>
      <div id="ramPieSuite" class="chart"></div>
    </div>
    <div class="row">
      <div id="activePieSuite" class="chart"></div>
      <div id="testsPieSuite" class="chart"></div>
    </div>
  </div>
  <details class="dash-footer" id="docsFooter">
    <summary>Docs &amp; debug</summary>
    <div class="dash-footer-body">
      <details class="aux-fold" id="cpuHelpDetails">
        <summary>CPU recommendation logic</summary>
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
      </details>
      <details class="aux-fold">
        <summary>How charts are calculated</summary>
        <div class="box">
          <ul>
            <li><b>By suites (stacked):</b> At time <code>t</code> we sum each active chunk contribution. For CPU it is <code>cpu_sec_report / duration_used_sec</code> (average cores for that run); for RAM it is the chunk memory metric from report converted to GB. This is a model by chunks, not an instantaneous per-process sample.</li>
            <li><b>Total (monitor, red line):</b> Absolute system CPU (cores) and RAM (GB) from <code>/proc</code> during the run (actual usage).</li>
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
            <li><b>CPU at cursor vs table totals:</b> chart value at time <code>t</code> is Σ <code>cpu_sec_report / dur_sec</code> over all runs of the suite active at <code>t</code>. Table <code>total_cpu_sec / total_dur_sec</code> is from one deduplicated run — so e.g. 89.4/135.3 ≈ 0.66 cores is not the same as a stacked chart reading of hundreds of cores from many parallel parts.</li>
          </ul>
        </div>
      </details>
      <details class="aux-fold">
        <summary>Run stats</summary>
        <pre id="stats"></pre>
      </details>
      <details class="aux-fold">
        <summary>Config</summary>
        <pre id="runConfig"></pre>
      </details>
    </div>
  </details>
  <script>
    // Stubs so inline onclick/onchange stay defined even if data fetch fails.
    window.showTab = function() {{}};
    window.clearSuiteSearch = function() {{}};
    window.clearAllMarkers = function() {{}};
    window.applyTimezoneToAllCharts = function() {{}};
    window.applyMarkersToCharts = function() {{}};
    window.toggleSuiteMarkers = function() {{}};
    window.applyLayerToggles = function() {{}};
    window.toggleChartTile = function() {{}};

    (async function() {{
    const dataUrl = {data_url_js};
    let data;
    try {{
      const resp = await fetch(dataUrl);
      if (!resp.ok) throw new Error('HTTP ' + resp.status);
      data = await resp.json();
    }} catch (err) {{
      const el = document.getElementById('dashboardLoadError');
      if (el) {{
        el.style.display = 'block';
        el.textContent = 'Failed to load ' + dataUrl + ': ' + err +
          '. Open via HTTP with the sibling *_data.json next to this HTML (file:// fetch is blocked).';
      }}
      return;
    }}

    function resizeChartPanes(ids) {{
      (ids || []).forEach(cid => {{
        const el = document.getElementById(cid);
        if (el && window.Plotly) {{
          try {{ Plotly.Plots.resize(el); }} catch (e) {{ /* chart may not be plotted yet */ }}
        }}
      }});
    }}

    function toggleChartTile(btn) {{
      const tile = btn && btn.closest ? btn.closest('.chart-tile') : null;
      if (!tile) return;
      const open = tile.classList.toggle('expanded');
      btn.textContent = open ? 'collapse' : 'expand';
      btn.title = open
        ? 'Collapse to half-width tile'
        : 'Expand to full width (same as ya make concurrency)';
      btn.setAttribute('aria-expanded', open ? 'true' : 'false');
      const paneId = tile.getAttribute('data-pane');
      // Resize this pane and neighbours — grid reflow changes sibling widths too.
      const grid = tile.parentElement;
      const panes = grid
        ? Array.from(grid.querySelectorAll('.chart-pane')).map(el => el.id).filter(Boolean)
        : (paneId ? [paneId] : []);
      setTimeout(() => {{
        resizeChartPanes(panes);
        if (typeof applyMarkersToCharts === 'function') applyMarkersToCharts();
      }}, 0);
    }}
    window.toggleChartTile = toggleChartTile;

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
          ? ['activeLayerSuite', 'testsLayerSuite', 'cpuLayerSuite', 'ramLayerSuite', 'diskLayerSuite', 'cpuPieSuite', 'ramPieSuite', 'activePieSuite', 'testsPieSuite']
          : ['activeLayerChunk', 'cpuLayer', 'ramLayer', 'diskLayer', 'cpuPie', 'ramPie', 'activePie'];
        resizeChartPanes(ids);
        // Re-apply markers after resize (charts lose shapes on hidden→visible transition).
        applyMarkersToCharts();
      }}, 0);
    }}

    // Charts inside closed <details> plot with width≈0 if drawn on load.
    // Defer first Plotly.newPlot until the section is opened; later opens
    // only resize + re-apply markers.
    const _plottedDetailPanes = new Set();
    const _detailPlotFns = {{}};
    function ensureDetailsPlot(detailsId, paneId, plotFn) {{
      _detailPlotFns[paneId] = plotFn;
      const details = document.getElementById(detailsId);
      const run = () => {{
        if (_plottedDetailPanes.has(paneId)) {{
          setTimeout(() => {{
            resizeChartPanes([paneId]);
            if (typeof syncChartGroupXRange === 'function') syncChartGroupXRange(ALL_TIME_CHART_IDS);
            if (typeof applyMarkersToCharts === 'function') applyMarkersToCharts();
          }}, 0);
          return;
        }}
        try {{ plotFn(); }} catch (e) {{ console.warn('deferred plot failed', paneId, e); return; }}
        _plottedDetailPanes.add(paneId);
        setTimeout(() => {{
          resizeChartPanes([paneId]);
          if (typeof syncChartGroupXRange === 'function') syncChartGroupXRange(ALL_TIME_CHART_IDS);
          if (typeof applyMarkersToCharts === 'function') applyMarkersToCharts();
        }}, 0);
      }};
      if (details && !details.open) {{
        details.addEventListener('toggle', () => {{
          if (details.open) run();
        }});
        return;
      }}
      run();
    }}

    const UTC_OFFSET_SEC = Number(data.utc_offset_sec || 0);
    const _fmtCache = {{}};
    function formatHeadlineNumber(metric, value) {{
      const n = Number(value);
      if (!Number.isFinite(n)) return '—';
      if (metric === 'cpu' || metric === 'ram' || metric === 'disk') return n.toFixed(1);
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
      if (metric === 'total_duration_sec' || metric === 'build_duration_sec' || metric === 'test_duration_sec') {{
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
    function activitySpanSec(kinds) {{
      const activity = data.ya_activity || {{}};
      let lo = Number.POSITIVE_INFINITY;
      let hi = Number.NEGATIVE_INFINITY;
      (kinds || []).forEach((kind) => {{
        const series = activity[kind] || {{}};
        const xs = Array.isArray(series.xs) ? series.xs : [];
        const ys = Array.isArray(series.ys) ? series.ys : [];
        for (let i = 0; i < xs.length; i++) {{
          const x = Number(xs[i]);
          const y = Number(ys[i]);
          if (!Number.isFinite(x) || !Number.isFinite(y) || y <= 0) continue;
          if (x < lo) lo = x;
          if (x > hi) hi = x;
        }}
      }});
      if (!Number.isFinite(lo) || !Number.isFinite(hi) || hi < lo) return null;
      return {{ start: lo, end: hi }};
    }}
    function resolvePhaseDurations(hs) {{
      const hasBuild = Number.isFinite(Number(hs.build_duration_sec));
      const hasTest = Number.isFinite(Number(hs.test_duration_sec));
      const hasTotal = Number.isFinite(Number(hs.total_duration_sec));
      // New payloads already ship all three; keep them as-is.
      if (hasBuild && hasTest && hasTotal) {{
        return {{
          totalSec: Math.max(0, Number(hs.total_duration_sec)),
          buildSec: Math.max(0, Number(hs.build_duration_sec)),
          testSec: Math.max(0, Number(hs.test_duration_sec)),
        }};
      }}
      // Older JSON only had total_duration_sec (= test wall). Derive the rest
      // from ya_activity so the summary still shows build/test/total.
      let testSec = hasTest ? Math.max(0, Number(hs.test_duration_sec))
        : (hasTotal ? Math.max(0, Number(hs.total_duration_sec)) : 0);
      let buildSec = hasBuild ? Math.max(0, Number(hs.build_duration_sec)) : 0;
      let totalSec = hasTotal ? Math.max(0, Number(hs.total_duration_sec)) : 0;
      const buildSpan = activitySpanSec(['build', 'archive', 'cache', 'link']);
      const testSpan = activitySpanSec(['test_tm', 'test_report']);
      if (!hasBuild && buildSpan) {{
        const testStart = testSpan ? testSpan.start : null;
        if (testStart != null && testStart > buildSpan.start) {{
          buildSec = Math.max(0, testStart - buildSpan.start);
        }} else {{
          buildSec = Math.max(0, buildSpan.end - buildSpan.start);
        }}
      }}
      if (!hasTest && testSpan) {{
        testSec = Math.max(0, testSpan.end - testSpan.start);
      }}
      const starts = [];
      const ends = [];
      if (buildSpan) {{ starts.push(buildSpan.start); ends.push(buildSpan.end); }}
      if (testSpan) {{ starts.push(testSpan.start); ends.push(testSpan.end); }}
      if (starts.length && ends.length && buildSec > 0) {{
        totalSec = Math.max(0, Math.max.apply(null, ends) - Math.min.apply(null, starts));
      }} else if (!totalSec) {{
        totalSec = Math.max(buildSec, 0) + Math.max(testSec, 0);
      }}
      return {{ totalSec, buildSec, testSec }};
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
      const phases = resolvePhaseDurations(hs);
      const wallExact = formatHeadlineValue('total_duration_sec', phases.totalSec).split(' (')[0];
      const buildExact = formatHeadlineValue('build_duration_sec', phases.buildSec).split(' (')[0];
      const testExact = formatHeadlineValue('test_duration_sec', phases.testSec).split(' (')[0];
      const activityUnaligned = !!(data.stats && data.stats.evlog_activity_offset_unaligned);
      const unalignedNote = activityUnaligned
        ? ('<span class="group" style="color:#b45309;" title="No report↔evlog chunk matches; '
          + 'build/archive/cache/link/TM activity uses raw evlog timestamps (offset 0). '
          + 'If clocks differ, the yaActivity chart may look empty or misaligned.">'
          + '⚠ ya activity clock unaligned</span>')
        : '';
      // build_* comes from whole-job ya_activity; test_* from (possibly filtered) runs.
      const suiteFilterRaw = (data.suite_filter == null) ? '' : String(data.suite_filter).trim();
      const suiteFiltered = !!(suiteFilterRaw && suiteFilterRaw.toUpperCase() !== 'ALL SUITES');
      const filterScopeNote = suiteFiltered
        ? ('<span class="group muted" title="Suite filter is active: test duration / suite counts '
          + 'cover matching suites only; build duration is still the whole-job compile/link phase '
          + 'from ya activity.">build = whole job</span>')
        : '';
      const buildTip = suiteFiltered
        ? 'Whole-job build/archive/cache/link activity until the first filtered-suite test start'
        : 'From the first build/archive/cache/link activity to the first test start';
      const testTip = suiteFiltered
        ? 'Filtered suites only: first matching test start → last matching test end'
        : 'From the first test start to the last test end';
      const totalTip = suiteFiltered
        ? 'Earliest of whole-job build start / filtered test start → latest end'
        : 'From the first build/archive/cache/link activity to the last test end';
      const runLine =
        '<div class="headline-run">' +
          '<span class="group"><b>' + String(Math.round(Number(total.suites || 0))) + '</b> suites</span>' +
          '<span class="group"><b>' + String(Math.round(Number(total.chunks || 0))) + '</b> chunks</span>' +
          '<span class="group"><b>' + String(Math.round(Number(total.tests || 0))) + '</b> tests</span>' +
          '<span class="group" title="' + totalTip + '">' +
            '<b>' + wallExact + '</b> total duration</span>' +
          '<span class="group" title="' + buildTip + '">' +
            '<b>' + buildExact + '</b> build duration</span>' +
          '<span class="group" title="' + testTip + '">' +
            '<b>' + testExact + '</b> test duration</span>' +
          filterScopeNote +
          '<span class="group">tests <b>' + String(failedTotal) + '</b> failed <span class="muted">(' + String(failedMuted) + ' muted)</span>' +
            ' · <b>' + String(timeoutTotal) + '</b> timeout · <b>' + String(skippedTotal) + '</b> skipped</span>' +
          '<span class="group">chunks <b>' + String(cFailedTotal) + '</b> failed <span class="muted">(' + String(cFailedMuted) + ' muted)</span>' +
            ' · <b>' + String(cTimeoutTotal) + '</b> timeout</span>' +
          unalignedNote +
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
          return '<tr><td>' + label + '</td>' +
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
        hostBlock =
          '<div class="headline-block"><h4>Host · /proc' + sampleNote + '</h4>' +
          statTable([
            ['CPU, cores', 'cpu', cpuHost, hostCapacity('cpu')],
            ['RAM, GB', 'ram', ramHost, hostCapacity('ram')],
            ['Disk read, MB/s', 'disk', diskRead, null],
            ['Disk write, MB/s', 'disk', diskWrite, null],
          ], true) +
          '</div>';
      }}
      grid.innerHTML =
        runLine +
        '<div class="headline-tables">' +
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
    function axisLayout(xsDisp, useFullMakeRange) {{
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
      const layout = {{
        title: axisTitle(),
        type: 'date',
        tickmode: tickVals ? 'array' : 'auto',
        tickvals: tickVals || undefined,
        ticktext: tickText || undefined,
        hoverformat: '',
      }};
      // Prefer the full evlog / ya-make window so suite tiles line up with
      // the top concurrency chart (build starts before tests).
      if (useFullMakeRange) {{
        const full = fullEvlogRangeMs();
        if (full) {{
          layout.range = [new Date(full[0]).toISOString(), new Date(full[1]).toISOString()];
        }}
      }}
      return layout;
    }}
    function fullEvlogRangeMs() {{
      const fullRange = data.resources_overlay && data.resources_overlay.evlog_range_sec;
      if (Array.isArray(fullRange) && fullRange.length >= 2) {{
        const a = _toMs(xToDisplay(Number(fullRange[0])));
        const b = _toMs(xToDisplay(Number(fullRange[1])));
        if (a != null && b != null && a < b) return [a, b];
      }}
      // Fallback: union of ya_activity series (same wall clock as evlog).
      const activity = data.ya_activity || {{}};
      let minMs = Number.POSITIVE_INFINITY;
      let maxMs = Number.NEGATIVE_INFINITY;
      for (const kind of Object.keys(activity)) {{
        for (const x of (activity[kind] && activity[kind].xs) || []) {{
          const n = _toMs(xToDisplay(Number(x)));
          if (n == null) continue;
          if (n < minMs) minMs = n;
          if (n > maxMs) maxMs = n;
        }}
      }}
      if (minMs < maxMs) return [minMs, maxMs];
      return null;
    }}
    function _collectPlotMs(el) {{
      const xs = [];
      if (!el || !el.data) return xs;
      for (const tr of el.data) {{
        if (!tr || !Array.isArray(tr.x)) continue;
        for (const v of tr.x) {{
          const n = _toMs(v);
          if (n != null) xs.push(n);
        }}
      }}
      return xs;
    }}
    function syncChartGroupXRange(ids) {{
      // Intentional: all time-series tiles share the full evlog window
      // (build → tests), matching ya make concurrency. A long build compresses
      // the test segment — that alignment is preferred over per-chart spans.
      if (!window.Plotly) return;
      let span = fullEvlogRangeMs();
      if (!span) {{
        let minMs = Number.POSITIVE_INFINITY;
        let maxMs = Number.NEGATIVE_INFINITY;
        for (const id of ids) {{
          for (const n of _collectPlotMs(document.getElementById(id))) {{
            if (n < minMs) minMs = n;
            if (n > maxMs) maxMs = n;
          }}
        }}
        if (!(minMs < maxMs)) return;
        span = [minMs, maxMs];
      }}
      const range = [new Date(span[0]).toISOString(), new Date(span[1]).toISOString()];
      const ticks = _buildTickVals(span[0], span[1], 8);
      const tickVals = ticks.map(ms => new Date(ms).toISOString());
      const tickText = ticks.map(ms => _formatTick(ms));
      for (const id of ids) {{
        const el = document.getElementById(id);
        if (!el || !el.data || !el.layout) continue;
        Plotly.relayout(el, {{
          'xaxis.range': range,
          'xaxis.tickmode': 'array',
          'xaxis.tickvals': tickVals,
          'xaxis.ticktext': tickText,
        }});
      }}
    }}
    const ALL_TIME_CHART_IDS = ['yaActivity', 'activeLayerChunk', 'activeLayerSuite', 'testsLayerSuite', 'cpuLayer', 'ramLayer', 'cpuLayerSuite', 'ramLayerSuite', 'diskLayer', 'diskLayerSuite'];
    function applyTimezoneToAllCharts() {{
      for (const id of ALL_TIME_CHART_IDS) {{
        const el = document.getElementById(id);
        if (!el || !el.data || !el.layout || !window.Plotly) continue;
        const xs = _collectPlotMs(el);
        const ax = axisLayout(xs.map(ms => new Date(ms).toISOString()), true);
        const patch = {{
          'xaxis.title': ax.title,
          'xaxis.type': 'date',
          'xaxis.tickmode': ax.tickmode,
          'xaxis.tickvals': ax.tickvals,
          'xaxis.ticktext': ax.ticktext,
          'xaxis.hoverformat': '',
          'hoverdistance': -1,
          'spikedistance': -1,
        }};
        if (ax.range) patch['xaxis.range'] = ax.range;
        Plotly.relayout(el, patch);
      }}
      syncChartGroupXRange(ALL_TIME_CHART_IDS);
      for (const id of ALL_TIME_CHART_IDS) {{
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
    const prTitle = cfg.pr_title != null && String(cfg.pr_title).trim() !== '' ? String(cfg.pr_title).trim() : null;
    const branch = cfg.branch != null && String(cfg.branch).trim() !== '' ? String(cfg.branch).trim() : null;
    const commit = cfg.commit != null && String(cfg.commit).trim() !== '' ? String(cfg.commit).trim() : null;
      const artifactsUrl = (cfg.artifacts_url || '').trim();
    const tryLinks = Array.isArray(cfg.try_links) ? cfg.try_links : [];
    const hasCiData = pr || prTitle || branch || commit || artifactsUrl;
    const repo = escapeHtml((cfg.repo || 'ydb-platform/ydb').trim());
    if (pr || prTitle) {{
      const el = document.getElementById('headerPr');
      el.style.display = '';
      let html = '';
      if (pr) {{
        const prSafe = escapeHtml(pr);
        html += '<a href="https://github.com/' + repo + '/pull/' + prSafe + '" target="_blank" rel="noopener" style="color:#2563eb;font-weight:600;text-decoration:none;">PR #' + prSafe + '</a>';
      }} else {{
        html += '<span style="color:#2563eb;font-weight:600;">PR</span>';
      }}
      if (prTitle) {{
        html += ' <span style="color:#0f172a;font-weight:600;">' + escapeHtml(prTitle) + '</span>';
      }}
      el.innerHTML = html;
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
    // Browser tab title: make it obvious which PR/dashboard this is.
    if (pr && prTitle) {{
      document.title = 'PR #' + pr + ': ' + prTitle + ' · Resource dashboard';
    }} else if (prTitle) {{
      document.title = prTitle + ' · Resource dashboard';
    }} else if (pr) {{
      document.title = 'PR #' + pr + ' · Resource dashboard';
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
    const _markerChartIds = ['yaActivity', 'activeLayerChunk', 'activeLayerSuite', 'testsLayerSuite', 'cpuLayer', 'ramLayer', 'cpuLayerSuite', 'ramLayerSuite', 'diskLayer', 'diskLayerSuite'];
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
        const heavySummaryLabel = document.querySelector('#heavyTestsDetails summary');
        const heavyMarker = (col) => (col === heavySortCol ? (heavySortAsc ? ' ▲' : ' ▼') : '');
        const heavyCount = heavyTestsFlat.length;
        if (heavySummaryLabel) {{
          heavySummaryLabel.textContent = 'Long/heavy near timeout (' + heavyCount + ')';
        }}
        if (heavyDetails) {{
          // Stay invisible when empty so the closed row does not eat vertical space.
          heavyDetails.classList.toggle('is-visible', heavyCount > 0);
          if (!heavyDetails.dataset.initCollapsed) {{
            heavyDetails.open = false;
            heavyDetails.dataset.initCollapsed = '1';
          }}
          if (!heavyCount) heavyDetails.open = false;
        }}
        if (heavyWrap) {{
          if (!heavyCount) {{
            heavyWrap.innerHTML = '';
          }} else {{
            heavyWrap.innerHTML =
              '<div style=\"font-size:11px;color:#8c959f;margin-bottom:4px;\">&gt;=97% of the chunk timeout budget</div>' +
              '<table style=\"width:100%;border-collapse:collapse;margin-top:4px;\">' +
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
      if (helpBtn) {{
        helpBtn.addEventListener('click', () => {{
          const footer = document.getElementById('docsFooter');
          const cpuHelp = document.getElementById('cpuHelpDetails');
          if (footer) footer.open = true;
          if (cpuHelp) cpuHelp.open = true;
          (cpuHelp || footer)?.scrollIntoView({{ behavior: 'smooth', block: 'nearest' }});
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
      // Tile header names the chart; skip Plotly title to avoid double headings.
      // Full evlog window so suite/chunk tiles match the yaActivity build chart.
      const ax = axisLayout(xDisp, true);
      Plotly.newPlot(divId, traces, {{
        title: false,
        xaxis: Object.assign(ax, {{
          titlefont: {{size: 11, color: '#8c959f'}},
          tickfont: {{size: 10, color: '#656d76'}},
          gridcolor: '#eef1f4',
          zerolinecolor: '#eaeef2',
          linecolor: '#d8dee4',
        }}),
        yaxis: {{
          title: yTitle,
          titlefont: {{size: 11, color: '#8c959f'}},
          tickfont: {{size: 10, color: '#656d76'}},
          gridcolor: '#eef1f4',
          zerolinecolor: '#eaeef2',
          linecolor: '#d8dee4',
          rangemode: 'tozero',
        }},
        hovermode: 'x unified',
        showlegend: false,
        paper_bgcolor: 'rgba(0,0,0,0)',
        plot_bgcolor: '#fcfcfd',
        margin: {{l: 48, r: 8, t: 8, b: 36}},
      }}, {{responsive: true, displayModeBar: 'hover', displaylogo: false}});
    }}

    function formatValue(y, unit) {{
      if (unit === 'active') return String(Math.round(y));
      if (unit === 'tests') return String(Math.round(y));
      if (unit === 'GB') return y.toFixed(3);
      return y.toFixed(3);
    }}

    function minVisibleValue(unit) {{
      if (unit === 'active') return 0.5;
      if (unit === 'tests') return 0.5;
      if (unit === 'GB') return 0.001;   // ~1 MB
      if (unit === 'cores') return 0.01; // suppress tiny CPU noise shown as 0.000
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
    function showChartTip(ev) {{
      const tip = document.getElementById('chartTip');
      if (!tip || !ev || !ev.points || !ev.points.length) return;
      clearTimeout(chartTipHideTimer);
      const label = formatPointTime(ev.points[0]);
      const seen = new Set();
      const lines = [];
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
      const xs = Array.isArray(data.xs_active_exact) && data.xs_active_exact.length
        ? data.xs_active_exact : data.xs_active;
      const ys = Array.isArray(data.ys_active_exact) && data.ys_active_exact.length
        ? data.ys_active_exact : data.ys_active;
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
        const eps = minVisibleValue(unit);
        const rows = ev.points
          .map(p => ({{name: p.data.name, y: Number(p.y || 0)}}))
          .filter(p => p.y > eps && isSuiteContribution(p.name))
          .sort((a, b) => b.y - a.y);
        const top = rows.slice(0, 40);
        const lines = top.map(r => `${{r.name}}: ${{formatValue(r.y, unit)}} ${{unit}}`);
        panel.textContent = `t=${{label}}\\n` + lines.join('\\n');
        showChartTip(ev);
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

    (function() {{
      const activity = data.ya_activity || {{}};
      const specs = [
        ['build', 'Build/codegen (CC/AS/PY/…)', '#2563eb', 'solid'],
        ['archive', 'Archives (AR)', '#0891b2', 'solid'],
        ['cache', 'Cache (From/Put cache)', '#f59e0b', 'solid'],
        ['test_report', 'Test chunks (report)', '#16a34a', 'solid'],
        ['test_tm', 'Test workers (TM)', '#4ade80', 'dash'],
        ['link', 'Links (LD)', '#9333ea', 'solid'],
      ];
      const traces = [];
      const allX = [];
      for (const [kind, name, color, dash] of specs) {{
        const series = activity[kind] || {{}};
        const xs = (series.xs || []).map(xToDisplay);
        if (!xs.length) continue;
        allX.push(...xs);
        traces.push({{
          x: xs,
          y: series.ys || [],
          mode: 'lines',
          name,
          line: {{color, width: 2, shape: 'hv', dash}},
          hovertemplate: name + ': %{{y:.0f}}<extra></extra>',
        }});
      }}
      const el = document.getElementById('yaActivity');
      if (!traces.length) {{
        const tile = el && el.closest ? el.closest('.chart-tile') : null;
        if (tile) tile.style.display = 'none';
        else if (el) el.style.display = 'none';
        return;
      }}
      const activityUnaligned = !!(data.stats && data.stats.evlog_activity_offset_unaligned);
      // Put clock warning into the tile label when needed; keep the plot clean.
      const titleEl = document.querySelector('#yaActivityGrid .chart-tile-title');
      if (titleEl) {{
        const main = activityUnaligned ? 'ya make concurrency (clock unaligned)' : 'ya make concurrency';
        titleEl.innerHTML = main + '<span class="chart-tile-sub">build, archives, cache, test workers, links</span>';
      }}
      const ax = axisLayout(allX, true);
      Plotly.newPlot('yaActivity', traces, {{
        title: false,
        xaxis: Object.assign(ax, {{
          titlefont: {{size: 11, color: '#8c959f'}},
          tickfont: {{size: 10, color: '#656d76'}},
          gridcolor: '#eef1f4',
          zerolinecolor: '#eaeef2',
          linecolor: '#d8dee4',
        }}),
        yaxis: {{
          title: 'active jobs',
          rangemode: 'tozero',
          titlefont: {{size: 11, color: '#8c959f'}},
          tickfont: {{size: 10, color: '#656d76'}},
          gridcolor: '#eef1f4',
          zerolinecolor: '#eaeef2',
          linecolor: '#d8dee4',
        }},
        hovermode: 'x unified',
        legend: {{orientation: 'h', x: 0, y: -0.28, xanchor: 'left', yanchor: 'top', font: {{size: 11}}}},
        paper_bgcolor: 'rgba(0,0,0,0)',
        plot_bgcolor: '#fcfcfd',
        margin: {{l: 48, r: 8, t: 8, b: 72}},
      }}, {{responsive: true, displayModeBar: 'hover', displaylogo: false}});
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
        const diskTile = document.getElementById('diskTileChunk');
        if (diskEl) {{
          if (diskTile) diskTile.style.display = '';
          diskEl.style.display = '';
          const diskAx = axisLayout(xDisp, true);
          Plotly.newPlot('diskLayer', [
            {{ x: xDisp, y: (ro.disk_write_mb || []).map(v => Number(v) || 0), mode: 'lines', name: 'Disk write (+MB/s)', line: {{ color: '#65a30d' }}, hovertemplate: 'Disk write: %{{y:.2f}} MB/s<extra></extra>' }},
            {{ x: xDisp, y: (ro.disk_read_mb || []).map(v => -(Number(v) || 0)), mode: 'lines', name: 'Disk read (-MB/s)', line: {{ color: '#0284c7' }}, hovertemplate: 'Disk read: %{{y:.2f}} MB/s<extra></extra>' }}
          ], {{
            title: false,
            xaxis: Object.assign(diskAx, {{
              titlefont: {{size: 11, color: '#8c959f'}},
              tickfont: {{size: 10, color: '#656d76'}},
              gridcolor: '#eef1f4',
              zerolinecolor: '#eaeef2',
              linecolor: '#d8dee4',
            }}),
            yaxis: {{
              title: 'MB/s',
              titlefont: {{size: 11, color: '#8c959f'}},
              tickfont: {{size: 10, color: '#656d76'}},
              gridcolor: '#eef1f4',
              zerolinecolor: '#eaeef2',
              linecolor: '#d8dee4',
            }},
            hovermode: 'x unified',
            hoverlabel: {{ namelength: -1 }},
            legend: {{orientation: 'h', x: 0, y: -0.28, xanchor: 'left', yanchor: 'top', font: {{size: 11}}}},
            paper_bgcolor: 'rgba(0,0,0,0)',
            plot_bgcolor: '#fcfcfd',
            margin: {{l: 48, r: 8, t: 8, b: 72}},
          }}, {{ responsive: true, displayModeBar: 'hover', displaylogo: false }});
        }}
      }}
      attachSortedHover('cpuLayer', 'cpuHover', 'cores');
      attachSortedHover('ramLayer', 'ramHover', 'GB');
      attachSortedClick('cpuLayer', 'cpuClick', 'cores');
      attachSortedClick('ramLayer', 'ramClick', 'GB');
    }}

    ensureDetailsPlot('activeBySuiteDetails', 'activeLayerSuite', () => {{
      stackedArea('activeLayerSuite', data.xs_active_suite, data.active_tracks_suite, 'Layered active chunks by suite', 'active chunks', true);
      attachSortedHover('activeLayerSuite', 'activeHoverSuite', 'active');
      attachSortedClick('activeLayerSuite', 'activeClickSuite', 'active');
    }});
    ensureDetailsPlot('testsBySuiteDetails', 'testsLayerSuite', () => {{
      stackedArea('testsLayerSuite', data.xs_tests_suite, data.tests_tracks_suite, 'Layered tests running by suite', 'tests', true);
      attachSortedHover('testsLayerSuite', 'testsHoverSuite', 'tests');
      attachSortedClick('testsLayerSuite', 'testsClickSuite', 'tests');
    }});

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
      const diskTile = document.getElementById('diskTileSuite');
      if (diskEl) {{
        if (diskTile) diskTile.style.display = '';
        diskEl.style.display = '';
        const diskAx = axisLayout(xDisp, true);
        Plotly.newPlot('diskLayerSuite', [
          {{ x: xDisp, y: (ro.disk_write_mb || []).map(v => Number(v) || 0), mode: 'lines', name: 'Disk write (+MB/s)', line: {{ color: '#65a30d' }}, hovertemplate: 'Disk write: %{{y:.2f}} MB/s<extra></extra>' }},
          {{ x: xDisp, y: (ro.disk_read_mb || []).map(v => -(Number(v) || 0)), mode: 'lines', name: 'Disk read (-MB/s)', line: {{ color: '#0284c7' }}, hovertemplate: 'Disk read: %{{y:.2f}} MB/s<extra></extra>' }}
        ], {{
          title: false,
          xaxis: Object.assign(diskAx, {{
            titlefont: {{size: 11, color: '#8c959f'}},
            tickfont: {{size: 10, color: '#656d76'}},
            gridcolor: '#eef1f4',
            zerolinecolor: '#eaeef2',
            linecolor: '#d8dee4',
          }}),
          yaxis: {{
            title: 'MB/s',
            titlefont: {{size: 11, color: '#8c959f'}},
            tickfont: {{size: 10, color: '#656d76'}},
            gridcolor: '#eef1f4',
            zerolinecolor: '#eaeef2',
            linecolor: '#d8dee4',
          }},
          hovermode: 'x unified',
          hoverlabel: {{ namelength: -1 }},
          legend: {{orientation: 'h', x: 0, y: -0.28, xanchor: 'left', yanchor: 'top', font: {{size: 11}}}},
          paper_bgcolor: 'rgba(0,0,0,0)',
          plot_bgcolor: '#fcfcfd',
          margin: {{l: 48, r: 8, t: 8, b: 72}},
        }}, {{ responsive: true, displayModeBar: 'hover', displaylogo: false }});
      }}
    }}
    attachSortedHover('cpuLayerSuite', 'cpuHoverSuite', 'cores');
    attachSortedHover('ramLayerSuite', 'ramHoverSuite', 'GB');
    attachSortedClick('cpuLayerSuite', 'cpuClickSuite', 'cores');
    attachSortedClick('ramLayerSuite', 'ramClickSuite', 'GB');
    syncChartGroupXRange(ALL_TIME_CHART_IDS);

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

    const suiteSearchEl = document.getElementById('suiteSearch');
    if (suiteSearchEl) {{
      suiteSearchEl.addEventListener('input', applySuiteSearch);
    }}
    applySuiteSearch();

    // onclick/onchange handlers in the HTML markup need globals.
    window.showTab = showTab;
    window.clearSuiteSearch = clearSuiteSearch;
    window.clearAllMarkers = clearAllMarkers;
    window.applyTimezoneToAllCharts = applyTimezoneToAllCharts;
    window.applyMarkersToCharts = applyMarkersToCharts;
    window.toggleSuiteMarkers = toggleSuiteMarkers;
    window.applyLayerToggles = applyLayerToggles;
    window.toggleChartTile = toggleChartTile;
    }})();
  </script>
</body>
</html>
"""
    out_html.write_text(html, encoding="utf-8")
