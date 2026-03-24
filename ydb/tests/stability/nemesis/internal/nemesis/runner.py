import io
import logging
import threading
import time


class NemesisManager:
    def __init__(self):
        self.processes = []
        self.processes_lock = threading.Lock()

    def start_process(self, type_name: str, runner, action='inject'):
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
        thread = threading.Thread(target=self._run, args=(proc_id, runner, action))
        thread.daemon = True
        thread.start()
        return proc_data

    def _run(self, proc_id, runner, action):
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
                handler.setLevel(logging.DEBUG)  # Capture all levels

                # Configure root logger to ensure it processes all logs
                root_logger = logging.getLogger()
                original_level = root_logger.level
                root_logger.setLevel(logging.DEBUG)

                # Add handler to root logger
                root_logger.addHandler(handler)

                try:
                    if action == 'inject':
                        runner.inject_fault()
                    elif action == 'extract':
                        runner.extract_fault()
                    else:
                        raise Exception('Unknown action type')
                finally:
                    root_logger.removeHandler(handler)
                    root_logger.setLevel(original_level)

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
                import traceback
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
            import traceback
            with self.processes_lock:
                if proc_id < len(self.processes):
                    self.processes[proc_id]['logs'] += f"\nError setting up process: {str(e)}\n{traceback.format_exc()}"
                    self.processes[proc_id]['status'] = 'error'
                    self.processes[proc_id]['ret_code'] = 1

    def get_all(self):
        with self.processes_lock:
            return self.processes.copy()
