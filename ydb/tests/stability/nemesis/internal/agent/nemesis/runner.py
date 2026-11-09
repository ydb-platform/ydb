import io
import json
import logging
import threading
import time
import traceback

from ydb.tests.stability.nemesis.internal.nemesis.monitored_actor import NEMESIS_EXECUTION_LOGGER


def _invoke_inject(runner, payload):
    runner.inject_fault({} if payload is None else payload)


def _invoke_extract(runner, payload):
    runner.extract_fault({} if payload is None else payload)


class NemesisManager:
    def __init__(self):
        self.processes = []
        self.processes_lock = threading.RLock()
        self._shutdown_lock = threading.Lock()
        self._closing = False
        self._closed = False
        self._terminating = False
        self._extracts = {}
        self._threads = []
        self._effects = {}
        self._reversible_types = set()
        self._runner_locks = {}

    def configure_recovery(self, types):
        self._reversible_types = set(types)

    @staticmethod
    def _effect_key(type_name, payload):
        payload = payload or {}
        if payload.get("chaos_target"):
            from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.chaos_target import ChaosTarget
            return type_name, ChaosTarget.from_dict(payload["chaos_target"]).identity_key()
        return type_name, json.dumps(payload, sort_keys=True)

    def begin_shutdown(self, *, permanent=False):
        with self.processes_lock:
            self._closing = True
            self._terminating |= permanent

    def resume(self):
        with self.processes_lock:
            if self._terminating or self._effects or any(t.is_alive() for t in self._threads):
                raise RuntimeError("Agent cannot resume while stopping or restoring")
            self._closing = False
            self._closed = False

    def shutdown(self, timeout=60):
        deadline = time.monotonic() + timeout
        self.begin_shutdown()
        with self._shutdown_lock:
            def join_actions():
                with self.processes_lock:
                    threads = list(self._threads)
                for thread in threads:
                    thread.join(max(0, deadline - time.monotonic()))
                return sum(thread.is_alive() for thread in threads)

            running = join_actions()
            if running:
                return {"status": "error", "running": running, "message": "Actions did not finish before restore"}
            with self.processes_lock:
                effects = list(self._effects.items())
            for (type_name, _), (runner, payload) in effects:
                self.start_process(type_name, runner, "extract", payload=payload)
            running = join_actions()
            with self.processes_lock:
                remaining = [key[0] for key in self._effects]
                if not running and not remaining:
                    self._closed = True
            return {"status": "error" if running or remaining else "ok",
                    "running": running, "unrestored": remaining}


    def start_process(
        self,
        type_name: str,
        runner,
        action='inject',
        payload=None,
    ):
        with self.processes_lock:
            if action not in ("inject", "extract"):
                raise ValueError("Unknown action type")
            if self._closed or (self._closing and action == "inject"):
                raise RuntimeError("Nemesis agent is stopping; injections are disabled")
            key = self._effect_key(type_name, payload)
            if action == "extract" and key in self._extracts:
                previous = self._extracts[key]
                if previous["status"] != "error":
                    return dict(previous)
            if action == "inject":
                self._extracts.pop(key, None)
                if key in self._effects:
                    raise RuntimeError("Target already has an unfinished agent action")
                self._effects[key] = (runner, payload)
            self._runner_locks.setdefault(id(runner), threading.Lock())
            proc_id = len(self.processes)
            proc_data = {
                "id": proc_id,
                "type": type_name,
                "command": f"{type_name} ({action})",
                "logs": "",
                "ret_code": None,
                "status": "running"
            }
            self.processes.append(proc_data)
            if action == "extract":
                self._extracts[key] = proc_data

            thread = threading.Thread(
                target=self._run, args=(proc_id, runner, action, type_name, payload), daemon=True,
            )
            self._threads.append(thread)
            thread.start()
        return proc_data

    def _run(self, proc_id, runner, action, type_name, payload):
        try:
            # It's a nemesis runner
            log_capture_string = io.StringIO()

            # Custom handler that filters logs based on thread ID
            class ThreadLocalHandler(logging.StreamHandler):
                def __init__(self, stream, thread_id):
                    super().__init__(stream)
                    self.thread_id = thread_id

                def filter(self, record):
                    return record.thread == self.thread_id

            def execute_with_logging():
                thread_id = threading.get_ident()
                handler = ThreadLocalHandler(log_capture_string, thread_id)
                handler.setLevel(logging.DEBUG)

                exec_logger = logging.getLogger(NEMESIS_EXECUTION_LOGGER)
                original_level = exec_logger.level
                exec_logger.setLevel(logging.DEBUG)
                exec_logger.addHandler(handler)

                try:
                    if action == 'inject':
                        _invoke_inject(runner, payload)
                    elif action == 'extract':
                        _invoke_extract(runner, payload)
                    else:
                        raise Exception('Unknown action type')
                except Exception:
                    if action == 'inject':
                        runner.on_failed_inject_fault()
                    elif action == 'extract':
                        runner.on_failed_extract_fault()
                    raise
                finally:
                    exec_logger.removeHandler(handler)
                    exec_logger.setLevel(original_level)

            # Background task to flush logs using threading.Timer
            stop_flushing = threading.Event()

            def flush_logs():
                while not stop_flushing.is_set():
                    with self.processes_lock:
                        if proc_id < len(self.processes):
                            self.processes[proc_id]['logs'] = log_capture_string.getvalue()
                    stop_flushing.wait(1)
                # Final flush
                with self.processes_lock:
                    if proc_id < len(self.processes):
                        self.processes[proc_id]['logs'] = log_capture_string.getvalue()

            flush_thread = threading.Thread(target=flush_logs)
            flush_thread.daemon = True
            flush_thread.start()

            try:
                with self._runner_locks[id(runner)]:
                    execute_with_logging()
                with self.processes_lock:
                    if action == "extract" or type_name not in self._reversible_types:
                        self._effects.pop(self._effect_key(type_name, payload), None)
                # Force final log capture
                final_logs = log_capture_string.getvalue()
                with self.processes_lock:
                    if proc_id < len(self.processes):
                        self.processes[proc_id]['logs'] = final_logs
                        self.processes[proc_id]['status'] = 'finished'
                        self.processes[proc_id]['ret_code'] = 0
                stop_flushing.set()
                flush_thread.join(timeout=2)
            except Exception as e:
                final_logs = log_capture_string.getvalue()
                stop_flushing.set()
                flush_thread.join(timeout=2)
                with self.processes_lock:
                    if proc_id < len(self.processes):
                        self.processes[proc_id]['logs'] = final_logs + f"\nError executing process: {str(e)}\n{traceback.format_exc()}"
                        self.processes[proc_id]['status'] = 'error'
                        self.processes[proc_id]['ret_code'] = 1
            log_capture_string.close()
        except Exception as e:
            with self.processes_lock:
                if proc_id < len(self.processes):
                    self.processes[proc_id]['logs'] += f"\nError setting up process: {str(e)}\n{traceback.format_exc()}"
                    self.processes[proc_id]['status'] = 'error'
                    self.processes[proc_id]['ret_code'] = 1

    def get_all(self):
        with self.processes_lock:
            return [dict(row) for row in self.processes]
