from __future__ import annotations

import html as html_lib
from collections import defaultdict
from pathlib import Path
from typing import Any, Optional

try:
    from .html_embed import js_script_json
    from .ya_make_requirements import normalize_suite_path
except ImportError:
    from html_embed import js_script_json
    from ya_make_requirements import normalize_suite_path


def build_report_table_html(
    enriched_runs: list[dict[str, Any]],
    out_html: Path,
    suite_filter: Optional[str],
) -> None:
    """Chunk-level rows only: the payload is built from enriched chunk runs,
    per-test report rows are not loaded here."""
    suite_filter = normalize_suite_path(suite_filter) if suite_filter else None
    rows: list[dict[str, Any]] = []
    for run in enriched_runs:
        if not isinstance(run, dict):
            continue
        suite = normalize_suite_path(str(run.get("suite_path", "") or ""))
        if not suite:
            continue
        if suite_filter and suite != suite_filter:
            continue
        rows.append(
            {
                "suite_path": suite,
                "suite_path_raw": str(run.get("suite_path_raw", "") or suite),
                "subtest_name": str(run.get("raw_name", "") or ""),
                "status": str(run.get("status", "") or ""),
                "error_type": str(run.get("error_type", "") or ""),
                "is_muted": bool(run.get("is_muted")),
                "chunk_idx": run.get("chunk"),
                "chunk_group": run.get("chunk_group"),
                "duration_sec": float(run.get("duration_used_sec") or 0.0),
                "cpu_sec": float(run.get("cpu_sec_report") or 0.0),
                "ram_kb": float(run.get("ram_kb_report") or 0.0),
                "id": run.get("uid"),
            }
        )

    chunks_per_suite: dict[str, int] = defaultdict(int)
    for r in rows:
        chunks_per_suite[r["suite_path"]] += 1
    for r in rows:
        r["chunks_in_suite"] = chunks_per_suite[r["suite_path"]]

    suite_summary = "; ".join(f"{s}: {c} chunks" for s, c in sorted(chunks_per_suite.items()))
    payload = {
        "suite_filter": suite_filter,
        "rows_count": len(rows),
        "suite_summary": suite_summary,
        "rows": rows,
    }
    payload_js = js_script_json(payload)
    suite_filter_html = html_lib.escape(suite_filter or "ALL SUITES", quote=True)
    html = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Report table: suite/chunk</title>
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, Segoe UI, Roboto, Arial, sans-serif; margin: 12px; }}
    .toolbar {{ position: sticky; top: 0; background: #fff; z-index: 3; border-bottom: 1px solid #eee; padding: 8px 0; display: flex; gap: 8px; align-items: center; flex-wrap: wrap; }}
    .toolbar input, .toolbar select {{ padding: 4px 8px; }}
    .table-wrap {{ overflow-x: auto; border: 1px solid #eee; border-radius: 6px; }}
    table {{ width: 100%; min-width: 1500px; border-collapse: collapse; font-size: 12px; table-layout: fixed; }}
    th, td {{ border: 1px solid #eee; padding: 4px 6px; vertical-align: top; }}
    th {{ position: sticky; top: 48px; background: #fafafa; z-index: 2; cursor: pointer; user-select: none; }}
    td pre {{ margin: 0; white-space: pre-wrap; word-break: break-word; max-width: 620px; }}
    #tbody tr {{ content-visibility: auto; contain-intrinsic-size: 28px; }}
    th:nth-child(1), td:nth-child(1) {{ width: 320px; }}
    th:nth-child(2), td:nth-child(2) {{ width: 110px; }}
    th:nth-child(3), td:nth-child(3) {{ width: 130px; }}
    th:nth-child(4), td:nth-child(4) {{ width: 70px; }}
    th:nth-child(5), td:nth-child(5) {{ width: 260px; }}
    .mono {{ font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; }}
    .muted {{ color: #6a737d; }}
    .ok {{ color: #155724; }}
    .fail {{ color: #721c24; }}
  </style>
</head>
<body>
  <h2>Report table: suite/chunk</h2>
  <div class="muted">chunk-level rows (no per-test rows) | suite filter: {suite_filter_html} | rows: <span id="rowsCount"></span> | <span id="suiteSummary"></span></div>
  <div class="toolbar">
    <label>Search:</label>
    <input id="q" type="text" placeholder="suite/chunk/status" style="min-width: 360px;" />
    <label>Status:</label>
    <select id="statusSel">
      <option value="">all</option>
    </select>
    <button type="button" onclick="clearFilters()">Clear</button>
  </div>
  <div class="table-wrap">
    <table id="tbl">
      <thead id="thead"></thead>
      <tbody id="tbody"></tbody>
    </table>
  </div>
  <script>
    const data = {payload_js};
    const cols = [
      ['suite_path', 'suite_path'],
      ['chunks_in_suite', 'chunks in suite'],
      ['chunk_group', 'chunk_group'],
      ['chunk_idx', 'chunk_idx'],
      ['subtest_name', 'chunk name'],
      ['status', 'status'],
      ['error_type', 'error_type'],
      ['is_muted', 'is_muted'],
      ['duration_sec', 'duration_sec'],
      ['cpu_sec', 'cpu_sec'],
      ['ram_kb', 'ram_kb'],
      ['id', 'id'],
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

      const filtered = data.rows.filter(r => {{
        if (st && String(r.status || '') !== st) return false;
        if (!q) return true;
        const hay = [
          r.suite_path, r.subtest_name, r.status, r.error_type, r.chunk_group, r.id
        ].map(x => String(x || '').toLowerCase()).join(' ');
        return hay.includes(q);
      }});

      filtered.sort((a, b) => {{
        const av = valueForSort(a[sortCol]);
        const bv = valueForSort(b[sortCol]);
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

      const body = filtered.map(r => {{
        const statusCls = /^(OK|PASS)$/i.test(String(r.status || '')) ? 'ok' : (/^(FAILED|ERROR|TIMEOUT|INTERNAL|MUTE)$/i.test(String(r.status || '')) ? 'fail' : '');
        const vals = {{
          suite_path: esc(r.suite_path),
          chunks_in_suite: String(r.chunks_in_suite ?? ''),
          chunk_group: esc(r.chunk_group || ''),
          chunk_idx: r.chunk_idx == null ? '' : String(r.chunk_idx),
          subtest_name: esc(r.subtest_name || ''),
          status: '<span class="' + statusCls + '">' + esc(r.status || '') + '</span>',
          error_type: esc(r.error_type || ''),
          is_muted: r.is_muted ? 'true' : 'false',
          duration_sec: Number(r.duration_sec || 0).toFixed(3),
          cpu_sec: Number(r.cpu_sec || 0).toFixed(6),
          ram_kb: Number(r.ram_kb || 0).toFixed(0),
          id: '<span class="mono">' + esc(r.id ?? '') + '</span>',
        }};
        return '<tr>' + cols.map(([k]) => '<td>' + (vals[k] ?? '') + '</td>').join('') + '</tr>';
      }}).join('');

      document.getElementById('tbody').innerHTML = body;
      document.getElementById('rowsCount').textContent = String(filtered.length);
      const summaryEl = document.getElementById('suiteSummary');
      if (summaryEl) summaryEl.textContent = data.suite_summary || ('total: ' + data.rows_count + ' chunks');
    }}

    function clearFilters() {{
      document.getElementById('q').value = '';
      document.getElementById('statusSel').value = '';
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
    render();
  </script>
</body>
</html>
"""
    out_html.write_text(html, encoding="utf-8")
