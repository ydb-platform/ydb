from __future__ import annotations

import json
import re
from collections import defaultdict
from pathlib import Path
from typing import Any, Optional

CHUNK_FROM_SUBTEST_RE = re.compile(r"\[(?:[^\]]*?\s)?(\d+)/(?:\d+)\]\s+chunk")
CHUNK_SOLE_RE = re.compile(r"^\s*sole\s+chunk\s*$", re.IGNORECASE)
CHUNK_BRACKET_ONLY_RE = re.compile(r"^\s*\[[^\]]+\]\s+chunk\s*$", re.IGNORECASE)
CHUNK_GROUP_FROM_SUBTEST_RE = re.compile(r"\[([^\]\s]+)\s+\d+/\d+\]\s+chunk", re.IGNORECASE)
CHUNK_GROUP_BRACKET_ONLY_RE = re.compile(r"\[([^\]]+)\]\s+chunk", re.IGNORECASE)
PART_SUFFIX_RE = re.compile(r"/part\d+$")


def normalize_suite_path(path: str) -> str:
    return PART_SUFFIX_RE.sub("", path)


def chunk_group_from_subtest(subtest_name: str) -> Optional[str]:
    s = subtest_name or ""
    m = CHUNK_GROUP_FROM_SUBTEST_RE.search(s)
    if m:
        return m.group(1)
    m2 = CHUNK_GROUP_BRACKET_ONLY_RE.search(s)
    if m2:
        content = m2.group(1).strip()
        if re.search(r"^\d+/\d+$", content):
            return None
        return content or None
    return None


def cpu_seconds(metrics: dict[str, Any]) -> float:
    vals: list[float] = []
    for k in ("ru_utime", "ru_stime"):
        v = metrics.get(k)
        if v is None:
            continue
        try:
            fv = float(v)
        except (TypeError, ValueError):
            continue
        if fv > 1000:
            fv /= 1_000_000.0
        vals.append(fv)
    return sum(vals)


def ram_kb(metrics: dict[str, Any]) -> float:
    mx = metrics.get("ru_maxrss")
    if mx is not None:
        try:
            return float(mx)
        except (TypeError, ValueError):
            pass
    rss = metrics.get("ru_rss")
    if rss is not None:
        try:
            return float(rss) / 1024.0
        except (TypeError, ValueError):
            pass
    return 0.0


def build_report_table_html(report_path: Path, out_html: Path, suite_filter: Optional[str]) -> None:
    report = json.loads(report_path.read_text(encoding="utf-8", errors="replace"))
    results = report.get("results", []) if isinstance(report, dict) else []
    rows: list[dict[str, Any]] = []
    for item in results:
        if not isinstance(item, dict):
            continue
        if item.get("type") != "test":
            continue
        suite_raw = str(item.get("path", "") or "")
        if not suite_raw:
            continue
        suite = normalize_suite_path(suite_raw)
        if suite_filter and suite != suite_filter:
            continue

        metrics = item.get("metrics") if isinstance(item.get("metrics"), dict) else {}
        sub = str(item.get("subtest_name", "") or "")
        chunk_idx: Optional[int] = None
        m_idx = CHUNK_FROM_SUBTEST_RE.search(sub)
        if m_idx:
            chunk_idx = int(m_idx.group(1))
        elif CHUNK_SOLE_RE.search(sub) or CHUNK_BRACKET_ONLY_RE.search(sub):
            chunk_idx = 0
        chunk_group = chunk_group_from_subtest(sub)

        rows.append(
            {
                "suite_path": suite,
                "suite_path_raw": suite_raw,
                "test_name": str(item.get("name", "") or ""),
                "subtest_name": sub,
                "status": str(item.get("status", "") or ""),
                "error_type": str(item.get("error_type", "") or ""),
                "is_muted": bool(item.get("is_muted") or item.get("muted")),
                "chunk": bool(item.get("chunk")),
                "chunk_idx": chunk_idx,
                "chunk_group": chunk_group,
                "duration_sec": float(item.get("duration") or 0.0),
                "cpu_sec": cpu_seconds(metrics),
                "ram_kb": ram_kb(metrics),
                "ru_utime": metrics.get("ru_utime"),
                "ru_stime": metrics.get("ru_stime"),
                "ru_maxrss": metrics.get("ru_maxrss"),
                "ru_rss": metrics.get("ru_rss"),
                "wall_time": metrics.get("wall_time"),
                "id": item.get("id"),
                "hid": item.get("hid"),
                "size": item.get("size"),
                "tags": ", ".join(item.get("tags", [])) if isinstance(item.get("tags"), list) else "",
                "metrics_json": json.dumps(metrics, ensure_ascii=False, sort_keys=True),
            }
        )

    tests_per_suite: dict[str, int] = defaultdict(int)
    for r in rows:
        tests_per_suite[r["suite_path"]] += 1
    for r in rows:
        r["tests_in_suite"] = tests_per_suite[r["suite_path"]]

    suite_summary = "; ".join(f"{s}: {c} tests" for s, c in sorted(tests_per_suite.items()))
    payload = {
        "suite_filter": suite_filter,
        "report_path": str(report_path),
        "rows_count": len(rows),
        "suite_summary": suite_summary,
        "rows": rows,
    }
    html = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Report table: suite/chunk/test</title>
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, Segoe UI, Roboto, Arial, sans-serif; margin: 12px; }}
    .toolbar {{ position: sticky; top: 0; background: #fff; z-index: 3; border-bottom: 1px solid #eee; padding: 8px 0; display: flex; gap: 8px; align-items: center; flex-wrap: wrap; }}
    .toolbar input, .toolbar select {{ padding: 4px 8px; }}
    .table-wrap {{ overflow-x: auto; border: 1px solid #eee; border-radius: 6px; }}
    table {{ width: 100%; min-width: 2650px; border-collapse: collapse; font-size: 12px; table-layout: fixed; }}
    th, td {{ border: 1px solid #eee; padding: 4px 6px; vertical-align: top; }}
    th {{ position: sticky; top: 48px; background: #fafafa; z-index: 2; cursor: pointer; user-select: none; }}
    td pre {{ margin: 0; white-space: pre-wrap; word-break: break-word; max-width: 620px; }}
    #tbody tr {{ content-visibility: auto; contain-intrinsic-size: 28px; }}
    th:nth-child(1), td:nth-child(1) {{ width: 320px; }}
    th:nth-child(2), td:nth-child(2) {{ width: 90px; }}
    th:nth-child(3), td:nth-child(3) {{ width: 70px; }}
    th:nth-child(4), td:nth-child(4) {{ width: 130px; }}
    th:nth-child(5), td:nth-child(5) {{ width: 70px; }}
    th:nth-child(6), td:nth-child(6) {{ width: 220px; }}
    th:nth-child(7), td:nth-child(7) {{ width: 260px; }}
    th:nth-child(23), td:nth-child(23) {{ width: 130px; }}
    .mono {{ font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; }}
    .muted {{ color: #6a737d; }}
    .ok {{ color: #155724; }}
    .fail {{ color: #721c24; }}
    .sidepanel {{ position: fixed; top: 0; right: 0; width: min(52vw, 820px); height: 100vh; background: #fff; border-left: 1px solid #ddd; box-shadow: -8px 0 24px rgba(0,0,0,0.08); z-index: 20; display: none; }}
    .sidepanel .head {{ display: flex; justify-content: space-between; align-items: center; padding: 10px 12px; border-bottom: 1px solid #eee; }}
    .sidepanel .body {{ padding: 10px 12px; height: calc(100vh - 56px); overflow: auto; }}
  </style>
</head>
<body>
  <h2>Report table: suite/chunk/test</h2>
  <div class="muted">suite filter: {suite_filter or 'ALL SUITES'} | rows: <span id="rowsCount"></span> | <span id="suiteSummary"></span></div>
  <div class="toolbar">
    <label>Search:</label>
    <input id="q" type="text" placeholder="suite/test/subtest/status/tags" style="min-width: 360px;" />
    <label>Status:</label>
    <select id="statusSel">
      <option value="">all</option>
    </select>
    <label>Mode:</label>
    <select id="chunkSel">
      <option value="">all</option>
      <option value="chunk">chunk=true</option>
      <option value="test">chunk=false</option>
    </select>
    <button type="button" onclick="clearFilters()">Clear</button>
  </div>
  <div class="table-wrap">
    <table id="tbl">
      <thead id="thead"></thead>
      <tbody id="tbody"></tbody>
    </table>
  </div>
  <div id="metricsPanel" class="sidepanel" role="dialog" aria-modal="true">
    <div class="head">
      <b id="metricsPanelTitle">metrics</b>
      <button type="button" onclick="closeMetricsPanel()">Close</button>
    </div>
    <div class="body">
      <pre id="metricsPanelBody" class="mono"></pre>
    </div>
  </div>
  <script>
    const data = {json.dumps(payload, ensure_ascii=False)};
    const cols = [
      ['suite_path', 'suite_path'],
      ['tests_in_suite', 'tests in suite'],
      ['chunk', 'chunk'],
      ['chunk_group', 'chunk_group'],
      ['chunk_idx', 'chunk_idx'],
      ['test_name', 'test_name'],
      ['subtest_name', 'subtest_name'],
      ['status', 'status'],
      ['error_type', 'error_type'],
      ['is_muted', 'is_muted'],
      ['duration_sec', 'duration_sec'],
      ['cpu_sec', 'cpu_sec'],
      ['ram_kb', 'ram_kb'],
      ['ru_utime', 'ru_utime'],
      ['ru_stime', 'ru_stime'],
      ['ru_maxrss', 'ru_maxrss'],
      ['ru_rss', 'ru_rss'],
      ['wall_time', 'wall_time'],
      ['size', 'size'],
      ['id', 'id'],
      ['hid', 'hid'],
      ['tags', 'tags'],
      ['metrics_json', 'metrics'],
    ];

    let sortCol = 'suite_path';
    let sortAsc = true;

    function valueForSort(v) {{
      if (v === null || v === undefined) return '';
      if (typeof v === 'boolean') return v ? 1 : 0;
      const n = Number(v);
      if (!Number.isNaN(n) && String(v).trim() !== '') return n;
      return String(v).toLowerCase();
    }}

    function esc(s) {{
      return String(s ?? '')
        .replaceAll('&', '&amp;')
        .replaceAll('<', '&lt;')
        .replaceAll('>', '&gt;')
        .replaceAll('"', '&quot;');
    }}

    function render() {{
      const q = (document.getElementById('q').value || '').toLowerCase().trim();
      const st = document.getElementById('statusSel').value;
      const mode = document.getElementById('chunkSel').value;

      const filtered = data.rows.map((r, idx) => ({{r, idx}})).filter(({{r}}) => {{
        if (st && String(r.status || '') !== st) return false;
        if (mode === 'chunk' && !r.chunk) return false;
        if (mode === 'test' && r.chunk) return false;
        if (!q) return true;
        const hay = [
          r.suite_path, r.test_name, r.subtest_name, r.status, r.error_type, r.tags, r.chunk_group, r.id, r.hid
        ].map(x => String(x || '').toLowerCase()).join(' ');
        return hay.includes(q);
      }});

      filtered.sort((a, b) => {{
        const av = valueForSort(a.r[sortCol]);
        const bv = valueForSort(b.r[sortCol]);
        if (av < bv) return sortAsc ? -1 : 1;
        if (av > bv) return sortAsc ? 1 : -1;
        return 0;
      }});

      const head = '<tr>' + cols.map(([k, title]) => {{
        const marker = sortCol === k ? (sortAsc ? ' ▲' : ' ▼') : '';
        return '<th data-col="' + k + '">' + esc(title) + marker + '</th>';
      }}).join('') + '</tr>';
      document.getElementById('thead').innerHTML = head;
      document.querySelectorAll('#thead th').forEach(th => th.addEventListener('click', () => {{
        const c = th.getAttribute('data-col');
        if (sortCol === c) sortAsc = !sortAsc;
        else {{ sortCol = c; sortAsc = true; }}
        render();
      }}));

      const body = filtered.map(({{r, idx}}) => {{
        const statusCls = /^(OK|PASS)$/i.test(String(r.status || '')) ? 'ok' : (/^(FAILED|ERROR|TIMEOUT|INTERNAL|MUTE)$/i.test(String(r.status || '')) ? 'fail' : '');
        const metricsCell = '<button type="button" class="metrics-btn" data-idx="' + String(idx) + '">open metrics</button>';
        const vals = {{
          suite_path: esc(r.suite_path),
          chunk: r.chunk ? 'true' : 'false',
          chunk_group: esc(r.chunk_group || ''),
          tests_in_suite: String(r.tests_in_suite ?? ''),
          chunk_idx: r.chunk_idx == null ? '' : String(r.chunk_idx),
          test_name: esc(r.test_name || ''),
          subtest_name: esc(r.subtest_name || ''),
          status: '<span class="' + statusCls + '">' + esc(r.status || '') + '</span>',
          error_type: esc(r.error_type || ''),
          is_muted: r.is_muted ? 'true' : 'false',
          duration_sec: Number(r.duration_sec || 0).toFixed(3),
          cpu_sec: Number(r.cpu_sec || 0).toFixed(6),
          ram_kb: Number(r.ram_kb || 0).toFixed(0),
          ru_utime: esc(r.ru_utime ?? ''),
          ru_stime: esc(r.ru_stime ?? ''),
          ru_maxrss: esc(r.ru_maxrss ?? ''),
          ru_rss: esc(r.ru_rss ?? ''),
          wall_time: esc(r.wall_time ?? ''),
          size: esc(r.size ?? ''),
          id: '<span class="mono">' + esc(r.id ?? '') + '</span>',
          hid: '<span class="mono">' + esc(r.hid ?? '') + '</span>',
          tags: esc(r.tags || ''),
          metrics_json: metricsCell,
        }};
        return '<tr>' + cols.map(([k]) => '<td>' + (vals[k] ?? '') + '</td>').join('') + '</tr>';
      }}).join('');

      document.getElementById('tbody').innerHTML = body;
      document.querySelectorAll('.metrics-btn').forEach(btn => btn.addEventListener('click', () => {{
        const idx = Number(btn.getAttribute('data-idx'));
        openMetricsPanel(idx);
      }}));
      document.getElementById('rowsCount').textContent = String(filtered.length);
      const summaryEl = document.getElementById('suiteSummary');
      if (summaryEl) summaryEl.textContent = data.suite_summary || ('total: ' + data.rows_count + ' tests');
    }}

    function openMetricsPanel(idx) {{
      const row = data.rows[idx];
      if (!row) return;
      const title = (row.suite_path || '') + ' | ' + (row.test_name || '') + (row.subtest_name ? (' | ' + row.subtest_name) : '');
      const body = row.metrics_json || '{{}}';
      const panel = document.getElementById('metricsPanel');
      document.getElementById('metricsPanelTitle').textContent = title;
      document.getElementById('metricsPanelBody').textContent = body;
      panel.style.display = 'block';
    }}

    function closeMetricsPanel() {{
      const panel = document.getElementById('metricsPanel');
      panel.style.display = 'none';
    }}

    function clearFilters() {{
      document.getElementById('q').value = '';
      document.getElementById('statusSel').value = '';
      document.getElementById('chunkSel').value = '';
      render();
    }}

    const statuses = Array.from(new Set(data.rows.map(r => String(r.status || '')).filter(Boolean))).sort();
    const stSel = document.getElementById('statusSel');
    statuses.forEach(s => {{
      const opt = document.createElement('option');
      opt.value = s;
      opt.textContent = s;
      stSel.appendChild(opt);
    }});
    document.getElementById('q').addEventListener('input', render);
    document.getElementById('statusSel').addEventListener('change', render);
    document.getElementById('chunkSel').addEventListener('change', render);
    render();
  </script>
</body>
</html>
"""
    out_html.write_text(html, encoding="utf-8")
