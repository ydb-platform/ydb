"""Offline, read-only HTTP views of schema-v4 benchmark manifests."""
import json
import webbrowser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import unquote, urlparse

from ydb.tools.ydb_bench.lib.common import BenchmarkError
from ydb.tools.ydb_bench.lib.results import load_manifest


_CSP = "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self'; font-src 'self'; connect-src 'self'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'"
_HTML = """<!doctype html><meta charset=utf-8><title>YDB benchmark runs</title><link rel=stylesheet href=/app.css><main><h1>YDB benchmark runs</h1><nav><a href=/#runs>Runs</a> <a href=/#topology>System topology</a></nav><section id=app>Loading…</section><script src=/app.js></script></main>"""
_CSS = "body{font:16px system-ui,sans-serif;margin:2rem;max-width:70rem}table{border-collapse:collapse;width:100%}th,td{border:1px solid #ccc;padding:.4rem;text-align:left}a{color:#0759a5}code{white-space:pre-wrap}"
_JS = """const app=document.querySelector('#app');const esc=s=>String(s??'').replace(/[&<>]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;'}[c]));async function api(p){let r=await fetch(p);if(!r.ok)throw Error(r.status);return r.json()}async function show(){try{let h=location.hash.slice(1),runs=await api('/api/runs');if(h==='topology'){let r=runs[0];if(!r)return app.textContent='No manifests found.';let d=await api('/api/runs/'+encodeURIComponent(r.id));return app.innerHTML='<h2>System topology</h2><p>'+esc(r.id)+'</p><code>'+esc(JSON.stringify(d.topology,null,2))+'</code>'}let id=h.replace(/^run\//,'');if(id){let d=await api('/api/runs/'+encodeURIComponent(id));return app.innerHTML='<h2>Run detail</h2><code>'+esc(JSON.stringify(d,null,2))+'</code>'}app.innerHTML='<h2>Runs</h2><table><tr><th>Run</th><th>Status</th><th>Profiles</th><th>Source</th></tr>'+runs.map(r=>'<tr><td><a href="#run/'+encodeURIComponent(r.id)+'">'+esc(r.id)+'</a></td><td>'+esc(r.status)+'</td><td>'+r.profiles+'</td><td>'+esc(r.source)+'</td></tr>').join('')+'</table>'}catch(e){app.textContent='Unable to read benchmark manifests: '+e}}addEventListener('hashchange',show);show();"""


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
        # Profile manifests have no topology or top-level run list.
        if "topology" not in manifest or "runs" not in manifest:
            continue
        records.append((str(candidate.parent.relative_to(root)) or ".", manifest))
    return sorted(records, key=lambda value: value[0])


def read_model(output):
    """Return a normalized, manifest-only model suitable for a future UI/API."""
    result = {}
    for run_id, manifest in _manifests(output):
        result[run_id] = {
            "id": run_id, "status": manifest.get("status", "unknown"),
            "state": manifest.get("state", "unknown"),
            "source": "imported" if (
                manifest.get("imported") or manifest.get("source") == "imported" or manifest.get("origin")
            ) else "local",
            "started_at": manifest.get("started_at"), "finished_at": manifest.get("finished_at"),
            "profiles": len(manifest.get("runs", [])), "runs": manifest.get("runs", []),
            "steps": manifest.get("steps", []), "topology": manifest.get("topology"),
        }
    return result


def _handler(model):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_args):
            pass
        def _send(self, status, content_type, body):
            self.send_response(status); self.send_header("Content-Type", content_type)
            self.send_header("Content-Security-Policy", _CSP); self.send_header("X-Content-Type-Options", "nosniff")
            self.send_header("Content-Length", str(len(body))); self.end_headers(); self.wfile.write(body)
        def do_GET(self):
            path = urlparse(self.path).path
            if path == "/": return self._send(200, "text/html; charset=utf-8", _HTML.encode())
            if path == "/app.css": return self._send(200, "text/css; charset=utf-8", _CSS.encode())
            if path == "/app.js": return self._send(200, "application/javascript; charset=utf-8", _JS.encode())
            if path == "/api/runs":
                values = [{key: item[key] for key in ("id", "status", "state", "source", "started_at", "finished_at", "profiles")} for item in model().values()]
                return self._send(200, "application/json", json.dumps(values).encode())
            if path.startswith("/api/runs/"):
                item = model().get(unquote(path[len("/api/runs/"):]))
                return self._send(200 if item else 404, "application/json", json.dumps(item or {"error": "run not found"}).encode())
            return self._send(404, "application/json", b'{"error":"not found"}')
        def do_POST(self):
            self._send(405, "application/json", b'{"error":"read-only API"}')
    return Handler


def serve(listen, port, output, no_open=False, allow_remote=False):
    if not _is_loopback(listen) and not allow_remote:
        raise BenchmarkError("non-loopback --listen requires --allow-remote")
    read_model(output)  # validate the result root before listening
    server = ThreadingHTTPServer((listen, port), _handler(lambda: read_model(output)))
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


def make_server(listen, port, output):
    """Test hook: create a server without entering its serving loop."""
    if not _is_loopback(listen):
        raise BenchmarkError("non-loopback --listen requires --allow-remote")
    return ThreadingHTTPServer((listen, port), _handler(lambda: read_model(output)))
