import io
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
        self.processes_lock = threading.Lock()

    def start_process(
        self,
        type_name: str,
        runner,
        action='inject',
        payload=None,
    ):
        with self.processes_lock:
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

        # Start process in a daemon thread
        thread = threading.Thread(
            target=self._run,
            args=(proc_id, runner, action, type_name, payload),
        )
        thread.daemon = True
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
                    time.sleep(1)
                # Final flush
                with self.processes_lock:
                    if proc_id < len(self.processes):
                        self.processes[proc_id]['logs'] = log_capture_string.getvalue()

            flush_thread = threading.Thread(target=flush_logs)
            flush_thread.daemon = True
            flush_thread.start()

            try:
                execute_with_logging()
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
            return self.processes.copy()
