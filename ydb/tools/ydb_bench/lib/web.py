"""Local web UI and durable application service for benchmark runs.

The HTTP handlers in this module deliberately only translate requests.  A
``RunService`` owns workers, manifests and the replayable event log, so closing
a browser connection cannot stop a benchmark.
"""
import hashlib
import json
import tempfile
import threading
import uuid
import webbrowser
from collections import deque
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlparse

from ydb.tools.ydb_bench.lib.common import BenchmarkError, atomic_write_json
from ydb.tools.ydb_bench.lib.config import build_run_plan, load_config
from ydb.tools.ydb_bench.lib.results import ResultStore, load_manifest
from ydb.tools.ydb_bench.lib.actors_core import run_actors_core
from ydb.tools.ydb_bench.lib.common import extract_executable


_CSP = "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self'; font-src 'self'; connect-src 'self'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'"
_HTML = """<!doctype html><meta charset=utf-8><title>YDB benchmark runs</title><link rel=stylesheet href=/app.css><main><h1>YDB benchmark runs</h1><nav><a href=/#runs>Runs</a> <a href=/#builder>Builder</a> <a href=/#yaml>YAML</a></nav><section id=app>Loading…</section><script src=/app.js></script></main>"""
_CSS = "body{font:16px system-ui,sans-serif;margin:2rem;max-width:70rem}table{border-collapse:collapse;width:100%}th,td{border:1px solid #ccc;padding:.4rem;text-align:left}a{color:#0759a5}code,textarea{white-space:pre-wrap;width:100%;min-height:14rem}button{margin:.3rem}"
_JS = r"""const app=document.querySelector('#app'),esc=s=>String(s??'').replace(/[&<>]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;'}[c]));let yaml='ping-bench:\n  baseline:\n    threads: [1]\n    duration: 1\n    repetitions: 1\n    affinity: [none]\n';async function api(p,o){let r=await fetch(p,o);let v=await r.json();if(!r.ok)throw Error(v.error||r.status);return v}function editor(){return '<textarea id=y>'+esc(yaml)+'</textarea><p><button id=validate>Validate</button><button id=preview>Preview</button><button id=start>Start</button></p><pre id=result></pre>'}async function compose(){let h=location.hash.slice(1);if(h==='builder'||h==='yaml'){app.innerHTML='<h2>'+h+'</h2>'+editor();let get=()=>yaml=document.querySelector('#y').value;validate.onclick=async()=>{get();result.textContent=JSON.stringify(await api('/api/validate',{method:'POST',body:yaml}),null,2)};preview.onclick=async()=>{get();result.textContent=JSON.stringify(await api('/api/plan',{method:'POST',body:yaml}),null,2)};start.onclick=async()=>{get();let r=await api('/api/runs',{method:'POST',body:yaml});location.hash='run/'+encodeURIComponent(r.id)};return}if(h.startsWith('run/')){let id=decodeURIComponent(h.slice(4));let draw=async()=>{let r=await api('/api/runs/'+encodeURIComponent(id));app.innerHTML='<h2>Run '+esc(id)+'</h2><button id=cancel>Cancel</button><pre>'+esc(JSON.stringify(r,null,2))+'</pre>';cancel.onclick=()=>api('/api/runs/'+encodeURIComponent(id)+'/cancel',{method:'POST'}).then(draw)};await draw();let e=new EventSource('/api/runs/'+encodeURIComponent(id)+'/events');e.onmessage=draw;return}let runs=await api('/api/runs');app.innerHTML='<h2>Runs</h2><table><tr><th>Run</th><th>Status</th><th>Profiles</th></tr>'+runs.map(r=>'<tr><td><a href="#run/'+encodeURIComponent(r.id)+'">'+esc(r.id)+'</a></td><td>'+esc(r.status)+'</td><td>'+r.profiles+'</td></tr>').join('')+'</table>'}addEventListener('hashchange',compose);compose();"""


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
        try: manifest = load_manifest(candidate)
        except BenchmarkError: continue
        if "topology" not in manifest and "steps" not in manifest: continue
        records.append((str(candidate.parent.relative_to(root)) or ".", manifest))
    return sorted(records, key=lambda value: value[0])


def read_model(output):
    result = {}
    for run_id, manifest in _manifests(output):
        result[run_id] = {"id": run_id, "status": manifest.get("status", "unknown"), "state": manifest.get("state", "unknown"), "source": "imported" if (manifest.get("imported") or manifest.get("source") == "imported" or manifest.get("origin")) else "local", "started_at": manifest.get("started_at"), "finished_at": manifest.get("finished_at"), "profiles": len(manifest.get("runs", [])), "runs": manifest.get("runs", []), "steps": manifest.get("steps", []), "topology": manifest.get("topology"), "events": manifest.get("events", 0)}
    return result


def _load_yaml(yaml_text):
    """Use the CLI parser/validator without allocating a result directory."""
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", suffix=".yaml", delete=False) as stream:
        stream.write(yaml_text); path = Path(stream.name)
    try: return load_config(path)
    finally: path.unlink(missing_ok=True)


class RunService:
    """Own running jobs, bounded live tails, and durable event replay.

    ``executor`` is an adapter callable ``(run, emit, cancelled)``. It may emit
    ``step-started``, ``step-finished``, ``stdout``, ``stderr`` and arbitrary
    progress dictionaries. This small boundary makes web integration testable
    without a real benchmark binary.
    """
    def __init__(self, output, executor=None, event_limit=256, tail_limit=65536):
        self.output = Path(output).resolve(); self.output.mkdir(parents=True, exist_ok=True)
        self.executor = executor or self._unsupported_executor
        self.event_limit, self.tail_limit = event_limit, tail_limit
        self._runs, self._lock = {}, threading.RLock()
        self._recover()

    def _recover(self):
        for run_id, manifest in _manifests(self.output):
            # A process may still be live after a server restart. Never restart
            # it without an adapter-specific proof that it is gone.
            if manifest.get("state") == "running":
                manifest["status"] = "recovery_required"; manifest["state"] = "recovery_required"
                atomic_write_json(self.output / run_id / "run.json", manifest)

    def validate(self, yaml_text):
        try: loaded = _load_yaml(yaml_text)
        except BenchmarkError as error: return {"valid": False, "error": str(error)}
        return {"valid": True, "sha256": hashlib.sha256(yaml_text.encode()).hexdigest(), "steps": len(build_run_plan(loaded).steps)}

    def plan(self, yaml_text):
        validation = self.validate(yaml_text)
        if not validation["valid"]: return validation
        plan = build_run_plan(_load_yaml(yaml_text))
        validation["plan"] = [{"id": s.id, "benchmark": s.benchmark, "profile": s.profile, "affinity": s.affinity, "repeat": s.repeat, "timeout": s.configuration.timeout_seconds} for s in plan.steps]
        return validation

    def start(self, yaml_text):
        plan_result = self.plan(yaml_text)
        if not plan_result["valid"]: raise BenchmarkError(plan_result["error"])
        run_id = "{}-web".format(datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"))
        while (self.output / run_id).exists(): run_id = "{}-{}".format(run_id, uuid.uuid4().hex[:6])
        root = self.output / run_id; root.mkdir(); (root / "config.yaml").write_text(yaml_text, encoding="utf-8")
        manifest = {"schema_version": 4, "status": "running", "state": "running", "started_at": _utc_now(), "config": {"snapshot": yaml_text, "sha256": plan_result["sha256"]}, "runs": [], "steps": [dict(item, state="pending", artifacts=[]) for item in plan_result["plan"]], "events": 0}
        run = {"id": run_id, "root": root, "loaded": _load_yaml(yaml_text), "store": ResultStore(root / "run.json", manifest), "events": deque(maxlen=self.event_limit), "tail": {"stdout": "", "stderr": ""}, "cancel": threading.Event()}
        run["store"].write()
        with self._lock: self._runs[run_id] = run
        threading.Thread(target=self._run, args=(run,), daemon=True, name="ydb-bench-" + run_id).start()
        return {"id": run_id, "state": "running"}

    def _emit(self, run, event):
        event = dict(event); event["sequence"] = run["store"].manifest.get("events", 0) + 1; event["at"] = _utc_now()
        if event.get("type") in ("stdout", "stderr"):
            key = event["type"]; run["tail"][key] = (run["tail"][key] + str(event.get("data", "")))[-self.tail_limit:]
        step_id = event.get("step_id")
        if event.get("type") == "step-started" and step_id: run["store"].transition_step(step_id, "running")
        if event.get("type") == "step-artifacts" and step_id:
            run["store"].add_artifacts(step_id, event.get("artifacts", []))
            for artifact in event.get("artifacts", []):
                if str(artifact).endswith(("stdout.txt", "stderr.txt")):
                    key = "stdout" if str(artifact).endswith("stdout.txt") else "stderr"
                    try:
                        run["tail"][key] = (run["tail"][key] + (run["root"] / artifact).read_text(encoding="utf-8"))[-self.tail_limit:]
                    except OSError: pass
        if event.get("type") == "step-finished" and step_id: run["store"].transition_step(step_id, event.get("state", "passed"), **event.get("fields", {}))
        run["events"].append(event); run["store"].manifest["events"] = event["sequence"]
        with (run["root"] / "events.jsonl").open("a", encoding="utf-8") as stream: stream.write(json.dumps(event, sort_keys=True) + "\n")
        run["store"].write()

    def _unsupported_executor(self, run, emit, cancelled):
        raise BenchmarkError("web execution adapter is not configured")

    def _run(self, run):
        try:
            self.executor(run, lambda event: self._emit(run, event), run["cancel"])
            if run["cancel"].is_set():
                for step in run["store"].manifest["steps"]:
                    if step["state"] in ("pending", "running"): run["store"].transition_step(step["id"], "cancelled")
                state, status = "cancelled", "cancelled"
            else: state, status = "passed", "completed"
        except Exception as error:
            state, status = ("cancelled", "cancelled") if run["cancel"].is_set() else ("failed", "failed")
            run["store"].manifest["error"] = str(error)
        run["store"].manifest.update({"state": state, "status": status, "finished_at": _utc_now()}); self._emit(run, {"type": "run-finished", "state": state})

    def cancel(self, run_id):
        with self._lock: run = self._runs.get(run_id)
        if not run: return {"id": run_id, "cancelled": True, "state": "not-running"}
        run["cancel"].set(); self._emit(run, {"type": "cancel-requested"})
        return {"id": run_id, "cancelled": True, "state": run["store"].manifest["state"]}

    def model(self): return read_model(self.output)
    def detail(self, run_id):
        item = self.model().get(run_id)
        if item and run_id in self._runs: item.update({"tail": self._runs[run_id]["tail"]})
        return item
    def events(self, run_id, after=0):
        run = self._runs.get(run_id)
        if run: return [e for e in run["events"] if e["sequence"] > after]
        path = self.output / run_id / "events.jsonl"
        if not path.is_file(): return []
        return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if json.loads(line)["sequence"] > after]


def production_executor(resource_loader, tool_revision):
    """Adapt the existing actors-core executor to the durable web service."""
    def execute(run, emit, cancelled):
        if resource_loader is None:
            raise BenchmarkError("the benchmark executable resource loader is not configured")
        with tempfile.TemporaryDirectory(prefix="ydb-bench-web-") as work:
            binary = extract_executable(resource_loader("actors_core_ut_fat"), work, "actors_core_ut_fat")
            for configuration in run["loaded"].runs:
                if cancelled.is_set(): return
                relative = Path(configuration.benchmark.name) / configuration.profile
                directory = run["root"] / relative; directory.mkdir(parents=True, exist_ok=True)
                run["store"].manifest["runs"].append({"benchmark": configuration.benchmark.name, "profile": configuration.profile, "status": "running", "directory": str(relative)})
                run["store"].write()
                def event(event):
                    item = dict(event)
                    if "affinity" in item:
                        item["step_id"] = next(step["id"] for step in run["store"].manifest["steps"] if step["benchmark"] == configuration.benchmark.name and step["profile"] == configuration.profile and step["affinity"] == item["affinity"] and step["repeat"] == item["repeat"])
                    if item.get("type") == "step-artifacts":
                        item["artifacts"] = [str(relative / artifact) for artifact in item["artifacts"]]
                    emit(item)
                profile = run_actors_core(binary, configuration, directory, tool_revision, work_dir_hint=work, event_sink=event, cancel_event=cancelled)
                run["store"].manifest["runs"][-1].update({"status": "completed", "manifest": str(relative / "run.json"), "summary": str(relative / profile["summary"])})
                run["store"].write()
    return execute


def _handler(service):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_args): pass
        def _send(self, status, content_type, body):
            self.send_response(status); self.send_header("Content-Type", content_type); self.send_header("Content-Security-Policy", _CSP); self.send_header("X-Content-Type-Options", "nosniff"); self.send_header("Content-Length", str(len(body))); self.end_headers(); self.wfile.write(body)
        def _json(self, status, value): self._send(status, "application/json", json.dumps(value).encode())
        def _body(self): return self.rfile.read(int(self.headers.get("Content-Length", 0))).decode("utf-8")
        def do_GET(self):
            parsed = urlparse(self.path); path = parsed.path
            if path == "/": return self._send(200, "text/html; charset=utf-8", _HTML.encode())
            if path == "/app.css": return self._send(200, "text/css; charset=utf-8", _CSS.encode())
            if path == "/app.js": return self._send(200, "application/javascript; charset=utf-8", _JS.encode())
            if path == "/api/runs": return self._json(200, [{k: v[k] for k in ("id", "status", "state", "source", "started_at", "finished_at", "profiles")} for v in service.model().values()])
            if path.endswith("/events") and path.startswith("/api/runs/"):
                run_id = unquote(path[len("/api/runs/"):-len("/events")]); after = int(parse_qs(parsed.query).get("after", [0])[0]); events = service.events(run_id, after)
                payload = b"".join(("id: %s\ndata: %s\n\n" % (e["sequence"], json.dumps(e))).encode() for e in events) or b": connected\n\n"
                return self._send(200, "text/event-stream", payload)
            if path.startswith("/api/runs/"):
                item = service.detail(unquote(path[len("/api/runs/"):]))
                return self._json(200 if item else 404, item or {"error": "run not found"})
            return self._json(404, {"error": "not found"})
        def do_POST(self):
            path = urlparse(self.path).path; body = self._body()
            try:
                if path == "/api/validate": return self._json(200, service.validate(body))
                if path == "/api/plan": return self._json(200, service.plan(body))
                if path == "/api/runs": return self._json(201, service.start(body))
                if path.startswith("/api/runs/") and path.endswith("/cancel"): return self._json(200, service.cancel(unquote(path[len("/api/runs/"):-len("/cancel")])))
            except BenchmarkError as error: return self._json(400, {"error": str(error)})
            return self._json(404, {"error": "not found"})
    return Handler


def make_server(listen, port, output, allow_remote=False, executor=None):
    if not _is_loopback(listen) and not allow_remote: raise BenchmarkError("non-loopback --listen requires --allow-remote")
    return ThreadingHTTPServer((listen, port), _handler(RunService(output, executor=executor)))


def serve(listen, port, output, no_open=False, allow_remote=False, executor=None):
    server = make_server(listen, port, output, allow_remote, executor); url_host = "[{}]".format(listen) if ":" in listen else listen; url = "http://{}:{}/".format(url_host, server.server_port); print(url)
    if not no_open: webbrowser.open(url)
    try: server.serve_forever()
    except KeyboardInterrupt: pass
    finally: server.server_close()
