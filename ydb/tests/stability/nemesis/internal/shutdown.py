"""Bounded, idempotent SIGTERM shutdown, independent of Flask request teardown."""
import json
import logging
from pathlib import Path
import signal
import threading

from werkzeug.serving import make_server

logger = logging.getLogger(__name__)


class ShutdownCoordinator:
    def __init__(self, begin, finish, *, report_path, timeout=120):
        self._begin = begin
        self._finish = finish
        self._path = Path(report_path)
        self._timeout = timeout
        self._lock = threading.Lock()
        self._done = threading.Event()
        self._started = False
        self._report = None

    def _write(self, report):
        self._path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self._path.with_suffix(".tmp")
        temporary.write_text(json.dumps(report, indent=2) + "\n")
        temporary.replace(self._path)

    def _run(self):
        try:
            report = self._finish()
        except Exception as error:
            logger.exception("Nemesis shutdown failed")
            report = {"status": "error", "message": str(error)}
        with self._lock:
            if self._report is None:
                self._report = report
                try:
                    self._write(report)
                except Exception as error:
                    logger.exception("Cannot persist shutdown report")
                    self._report = {"status": "error", "message": str(error), "restore": report}
            self._done.set()

    def shutdown(self):
        with self._lock:
            if self._report is not None:
                return self._report
            if not self._started:
                self._begin()
                self._started = True
                threading.Thread(target=self._run, daemon=True).start()
        if not self._done.wait(self._timeout):
            with self._lock:
                if self._report is None:
                    self._report = {"status": "error", "message": "Shutdown deadline exceeded; restore incomplete"}
                    try:
                        self._write(self._report)
                    except Exception:
                        logger.exception("Cannot persist shutdown timeout")
        return self._report


def serve_until_stopped(app, cleanup, *, host, port):
    stop = threading.Event()
    previous = {sig: signal.signal(sig, lambda *_: stop.set()) for sig in (signal.SIGTERM, signal.SIGINT)}
    server = make_server(host, port, app, threaded=True)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        while thread.is_alive() and not stop.wait(0.2):
            pass
        app.config["NEMESIS_STOPPING"] = True
        report = cleanup()
        return 0 if report.get("status") == "ok" else 1
    finally:
        server.shutdown()
        server.server_close()
        for sig, handler in previous.items():
            signal.signal(sig, handler)
