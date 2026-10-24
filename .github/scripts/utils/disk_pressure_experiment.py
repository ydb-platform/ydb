"""Temporary CI experiment: bounded direct writes during each test attempt."""

import mmap
import multiprocessing as mp
import os
from pathlib import Path
import shutil
import signal
import sys
import tempfile
import time


WORKERS = 4
FILE_BYTES = 1024**3
BLOCK_BYTES = 4 * 1024**2
MAX_SECONDS = 6 * 3600


def write_loop(path, stop, written):
    signal.signal(signal.SIGTERM, signal.SIG_DFL)
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    # Anonymous mmap provides the alignment required by O_DIRECT. This avoids
    # filling the page cache and turning the disk experiment into RAM pressure.
    with mmap.mmap(-1, BLOCK_BYTES) as block:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_DIRECT, 0o600)
        try:
            os.posix_fallocate(fd, 0, FILE_BYTES)
            offset = 0
            while not stop.is_set():
                size = os.pwrite(fd, block, offset)
                if size != BLOCK_BYTES:
                    raise RuntimeError("Short direct write: {}".format(size))
                written.value += size
                offset = (offset + size) % FILE_BYTES
        finally:
            os.close(fd)


def main():
    directory, ready_path = map(Path, sys.argv[1:])
    if shutil.disk_usage(directory).free < 2 * WORKERS * FILE_BYTES:
        raise RuntimeError("Not enough free space for the bounded disk experiment")

    stopping = False

    def request_stop(signum, frame):
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)
    parent_pid = os.getppid()
    stop = mp.Event()
    # One writer per counter; approximate telemetry must not block cleanup if a
    # worker is killed. The experiment runs on 64-bit Linux.
    counters = [mp.Value("Q", 0, lock=False) for _ in range(WORKERS)]
    started = time.monotonic()
    previous_time, previous_bytes = started, 0
    ready = False
    print("O_DIRECT writers={} footprint_GiB={} max_seconds={}".format(
        WORKERS, WORKERS * FILE_BYTES / 1024**3, MAX_SECONDS), flush=True)

    with tempfile.TemporaryDirectory(prefix="disk-pressure-", dir=directory) as work:
        workers = []
        try:
            for i in range(WORKERS):
                worker = mp.Process(target=write_loop, args=(
                    str(Path(work) / str(i)), stop, counters[i]))
                worker.start()
                workers.append(worker)
            while not stopping and os.getppid() == parent_pid:
                if any(not worker.is_alive() for worker in workers):
                    raise RuntimeError("A disk writer exited unexpectedly")
                now = time.monotonic()
                if now - started >= MAX_SECONDS:
                    raise RuntimeError("Disk pressure reached its runtime limit")
                amounts = [counter.value for counter in counters]
                if not ready and all(amounts):
                    ready_path.touch()
                    ready = True
                    print("ready timestamp={:.3f}".format(time.time()), flush=True)
                if now - previous_time >= 10:
                    total = sum(amounts)
                    print("timestamp={:.3f} write_MiBs={:.1f} total_GiB={:.2f}".format(
                        time.time(), (total - previous_bytes) / (now - previous_time) / 1024**2,
                        total / 1024**3), flush=True)
                    previous_time, previous_bytes = now, total
                time.sleep(0.2)
        finally:
            stop.set()
            for worker in workers:
                worker.join(timeout=3)
                if worker.is_alive():
                    worker.kill()
                    worker.join()
            ready_path.unlink(missing_ok=True)
            print("stopped timestamp={:.3f} elapsed_seconds={:.1f} written_GiB={:.2f}".format(
                time.time(), time.monotonic() - started,
                sum(counter.value for counter in counters) / 1024**3), flush=True)


if __name__ == "__main__":
    main()
