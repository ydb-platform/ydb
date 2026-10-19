"""Linux local-build guard: bounded CPUs, hard allocation cap, monitored tree RSS.

The aggregate RSS cutoff is sampled every 100 ms; it is not a kernel cgroup
limit. Descendants of every thread are tracked, including reparented children.
Pass ya's worker limits explicitly as well. The default is two CPUs;
RBO_BUILD_CPUS may select one through four after checking memory headroom.
"""

import glob
import os
import resource
import signal
import subprocess
import sys
import time

GIB = 1024 ** 3
PROCESS_LIMIT = 8 * GIB
ADDRESS_LIMIT = 64 * GIB  # Linkers map large read-only archives and debug sections.
TREE_LIMIT = 16 * GIB
cpu_count = int(os.environ.get("RBO_BUILD_CPUS", "2"))
if not 1 <= cpu_count <= 4:
    raise SystemExit("RBO_BUILD_CPUS must be between one and four")
cpus = set(sorted(os.sched_getaffinity(0))[:cpu_count])

def child_limits():
    os.sched_setaffinity(0, cpus)
    resource.setrlimit(resource.RLIMIT_DATA, (PROCESS_LIMIT, PROCESS_LIMIT))
    resource.setrlimit(resource.RLIMIT_AS, (ADDRESS_LIMIT, ADDRESS_LIMIT))
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))

command = sys.argv[1:]
if not command:
    raise SystemExit('Missing command')
process = subprocess.Popen(command, start_new_session=True, preexec_fn=child_limits)
known = {}
peak = 0

def inspect(pid):
    try:
        stat = open(f'/proc/{pid}/stat').read().rsplit(')', 1)[1].split()
        start = stat[19]
        rss = int(stat[21]) * os.sysconf('SC_PAGE_SIZE')
        children = []
        for task_children in glob.glob(f'/proc/{pid}/task/*/children'):
            try:
                with open(task_children) as stream:
                    children.extend(int(value) for value in stream.read().split())
            except OSError:
                pass
        return start, rss, children
    except (OSError, ValueError, IndexError):
        return None

def stop(signum=signal.SIGTERM):
    try:
        os.killpg(process.pid, signum)
    except ProcessLookupError:
        pass
    for pid, start in list(known.items()):
        current = inspect(pid)
        if current and current[0] == start:
            try:
                os.kill(pid, signum)
            except ProcessLookupError:
                pass

interrupted = False

def interrupt(signum, frame):
    global interrupted
    interrupted = True
    stop()

signal.signal(signal.SIGTERM, interrupt)
signal.signal(signal.SIGINT, interrupt)
print(f'BUILD LIMITS: CPUs={sorted(cpus)}, data/process=8 GiB, address-space/process=64 GiB, process-tree RSS guard=16 GiB', flush=True)
while process.poll() is None:
    pending = [process.pid, *known]
    seen = set()
    total = 0
    while pending:
        pid = pending.pop()
        if pid in seen:
            continue
        seen.add(pid)
        current = inspect(pid)
        if current is None:
            known.pop(pid, None)
            continue
        start, rss, children = current
        if pid in known and known[pid] != start:
            known.pop(pid, None)
            continue
        known[pid] = start
        total += rss
        pending.extend(children)
    peak = max(peak, total)
    available = next((int(line.split()[1]) * 1024 for line in open("/proc/meminfo") if line.startswith("MemAvailable:")), 0)
    if total > TREE_LIMIT or available < 16 * GIB or interrupted:
        print(f'BUILD STOPPED: tree RSS={total / GIB:.2f} GiB; available={available / GIB:.2f} GiB; interrupted={interrupted}', flush=True)
        stop()
        try:
            process.wait(timeout=2)
        except subprocess.TimeoutExpired:
            stop(signal.SIGKILL)
            process.wait()
        raise SystemExit(130 if interrupted else 137)
    time.sleep(0.1)
print(f'BUILD PEAK PROCESS-TREE RSS: {peak / GIB:.2f} GiB', flush=True)
raise SystemExit(process.returncode if process.returncode >= 0 else 128 - process.returncode)
