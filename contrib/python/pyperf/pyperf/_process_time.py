"""
Similar to UNIX time command: measure the execution time of a command.

Minimum Python script spawning a program, wait until it completes, and then
write the elapsed time into stdout. Time is measured by the time.perf_counter()
timer.

Python subprocess.Popen() is implemented with fork()+exec(). Minimize the
Python imports to reduce the memory footprint, to reduce the cost of
fork()+exec().

Measure wall-time, not CPU time.

If resource.getrusage() is available: compute the maximum RSS memory in bytes
per process and writes it into stdout as a second line.
"""
import contextlib
import json
import os
import subprocess
import sys
import tempfile
import time

try:
    import resource
except ImportError:
    resource = None


def get_max_rss(*, children):
    if resource is not None:
        if children:
            resource_type = resource.RUSAGE_CHILDREN
        else:
            resource_type = resource.RUSAGE_SELF
        usage = resource.getrusage(resource_type)
        if sys.platform == 'darwin':
            return usage.ru_maxrss
        return usage.ru_maxrss * 1024
    else:
        return 0


def merge_profile_stats_files(src, dst):
    """
    Merging one existing pstats file into another.
    """
    import pstats
    if os.path.isfile(dst):
        src_stats = pstats.Stats(src)
        dst_stats = pstats.Stats(dst)
        dst_stats.add(src_stats)
        dst_stats.dump_stats(dst)
        os.unlink(src)
    else:
        os.rename(src, dst)


def bench_process(loops, args, kw, profile_filename=None):
    max_rss = 0
    range_it = range(loops)
    start_time = time.perf_counter()

    if profile_filename:
        temp_profile_filename = tempfile.mktemp()
        args = [args[0], "-m", "cProfile", "-o", temp_profile_filename] + args[1:]

    for _ in range_it:
        start_rss = get_max_rss(children=True)

        proc = subprocess.Popen(args, **kw)
        with proc:
            proc.wait()

        exitcode = proc.returncode
        if exitcode != 0:
            print("Command failed with exit code %s" % exitcode,
                  file=sys.stderr)
            if profile_filename:
                os.unlink(temp_profile_filename)
            sys.exit(exitcode)

        rss = get_max_rss(children=True) - start_rss
        max_rss = max(max_rss, rss)

        if profile_filename:
            merge_profile_stats_files(
                temp_profile_filename, profile_filename
            )

    dt = time.perf_counter() - start_time
    return (dt, max_rss)


def load_hooks(metadata):
    hook_names = []
    while "--hook" in sys.argv:
        hook_idx = sys.argv.index("--hook")
        hook_name = sys.argv[hook_idx + 1]
        hook_names.append(hook_name)
        del sys.argv[hook_idx]
        del sys.argv[hook_idx]

    if len(hook_names):
        # Only import pyperf if we know we have hooks
        import pyperf._hooks

        hook_managers = pyperf._hooks.instantiate_selected_hooks(hook_names)
        metadata["hooks"] = ", ".join(hook_managers.keys())
    else:
        hook_managers = {}

    return hook_managers


def write_data(dt, max_rss, metadata, out=sys.stdout):
    # Write the data that is communicated back to the main orchestration process.
    # It is three lines containing:
    #    - The runtime (in seconds)
    #    - max_rss (or -1, if not able to compute)
    #    - The metadata to add to the benchmark entry, as a JSON dictionary
    print(dt, file=out)
    print(max_rss or -1, file=out)
    json.dump(metadata, fp=out)
    print(file=out)


def main():
    # Make sure that the pyperf module wasn't imported
    if 'pyperf' in sys.modules:
        print("ERROR: don't run %s -m pyperf._process, run the .py script"
              % os.path.basename(sys.executable))
        sys.exit(1)

    if len(sys.argv) < 3:
        print("Usage: %s %s loops program [arg1 arg2 ...] [--profile profile]"
              % (os.path.basename(sys.executable), __file__))
        sys.exit(1)

    if "--profile" in sys.argv:
        profile_idx = sys.argv.index("--profile")
        profile_filename = sys.argv[profile_idx + 1]
        del sys.argv[profile_idx]
        del sys.argv[profile_idx]
    else:
        profile_filename = None

    metadata = {}
    hook_managers = load_hooks(metadata)

    loops = int(sys.argv[1])
    args = sys.argv[2:]

    kw = {}
    if hasattr(subprocess, 'DEVNULL'):
        devnull = None
        kw['stdin'] = subprocess.DEVNULL
        kw['stdout'] = subprocess.DEVNULL
    else:
        devnull = open(os.devnull, 'w+', 0)
        kw['stdin'] = devnull
        kw['stdout'] = devnull
    kw['stderr'] = subprocess.STDOUT

    with contextlib.ExitStack() as stack:
        for hook in hook_managers.values():
            stack.enter_context(hook)
        dt, max_rss = bench_process(loops, args, kw, profile_filename)

    if devnull is not None:
        devnull.close()

    for hook in hook_managers.values():
        hook.teardown(metadata)

    write_data(dt, max_rss, metadata)


if __name__ == "__main__":
    main()
