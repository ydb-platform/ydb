"""Measure TA dependency preparation without stopping runner threads.

One record per dependency, no per-file logging or additional directory walks.
Wall times include waits; thread CPU times exclude waits. Preparation excluding
cleanup still includes output scanning and parent-directory handling, not just link().
"""
import functools
import json
import os
import sys
import threading
import time


def install(output):
    from yalibrary.runner.build_root import BuildRoot
    from yalibrary.runner.tasks.run import RunNodeTask

    fd = os.open(output, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
    local = threading.local()
    original_call = RunNodeTask.__call__
    original_steal = BuildRoot.steal
    original_delete = BuildRoot.delete

    @functools.wraps(original_call)
    def call(task, *args, **kwargs):
        previous = getattr(local, 'task', None)
        outputs = task._node.outputs
        ta_output = next((x for x in outputs if 'results_accumulator.log' in x), None)
        local.task = (task._node.uid, ta_output) if ta_output else None
        try:
            return original_call(task, *args, **kwargs)
        finally:
            local.task = previous

    @functools.wraps(original_delete)
    def delete(root, *args, **kwargs):
        record = getattr(local, 'record', None)
        if record is None:
            return original_delete(root, *args, **kwargs)
        record['delete_start_ts'] = time.time()
        start = time.perf_counter()
        cpu = time.thread_time()
        try:
            return original_delete(root, *args, **kwargs)
        finally:
            record['delete_wall_s'] += time.perf_counter() - start
            record['delete_thread_cpu_s'] += time.thread_time() - cpu
            record['delete_count'] += 1
            record['delete_end_ts'] = time.time()

    @functools.wraps(original_steal)
    def steal(root, *args, **kwargs):
        task = getattr(local, 'task', None)
        if task is None:
            return original_steal(root, *args, **kwargs)
        previous = getattr(local, 'record', None)
        record = dict(task_uid=task[0], ta_output=task[1],
                      root=os.path.basename(root.path), tid=threading.get_native_id(),
                      outputs=len(root._outputs), start_ts=time.time(),
                      delete_count=0, delete_wall_s=0.0, delete_thread_cpu_s=0.0)
        local.record = record
        start = time.perf_counter()
        cpu = time.thread_time()
        try:
            result = original_steal(root, *args, **kwargs)
            record['ok'] = bool(result)
            return result
        finally:
            record['wall_s'] = time.perf_counter() - start
            record['thread_cpu_s'] = time.thread_time() - cpu
            record['end_ts'] = time.time()
            local.record = previous
            # One append after the measured section; no paths/content from file reads.
            os.write(fd, (json.dumps(record) + '\n').encode())

    RunNodeTask.__call__ = call
    BuildRoot.steal = steal
    BuildRoot.delete = delete


if __name__ == '__main__':
    # Do not propagate the custom entry point to ya's subprocesses.
    os.environ.pop('Y_PYTHON_ENTRY_POINT', None)
    install(os.environ.pop('TA_TIMINGS_FILE'))
    sys.argv[0] = sys.executable
    from entry.main import main
    main()
