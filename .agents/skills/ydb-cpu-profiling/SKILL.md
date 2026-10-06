---
name: ydb-cpu-profiling
description: Collect and inspect Linux perf CPU profiles for YDB test binaries and running processes through ssh_ya.
---

# YDB CPU profiling

Use this workflow for sampled CPU call stacks. If the request concerns heap usage, coverage, or off-CPU latency, establish that scope before choosing a profiler.

Root placement: profiling can start in any YDB component; a nested skill would hide this shared workflow. Only this description needs discovery in unrelated sessions.

## Prepare the workload

1. Choose whether to launch a test or attach to a running process. For a test, identify the target, exact case, and workload arguments; for attachment, verify the PID, owner, and executable. Set a bounded measurement interval. Read its nearest AGENTS.md. Separate setup and warmup from the measured work when the workload supports it; otherwise report that the profile includes them.
2. Use the installed `ssh_ya` from this checkout. Inspect `ssh_ya --help` and `ssh_ya execute --help`. Global options precede the subcommand. Use the selected host consistently; if no host is configured, obtain one before running remote commands.
3. Check Linux, perf, and recording permissions without syncing: `ssh_ya execute --skip-sync sh -c 'uname -s; perf --version; cat /proc/sys/kernel/perf_event_paranoid'`. A sysctl value alone does not prove recording works. If recording fails, preserve the error and resolve access with the user; do not change machine-wide settings automatically.
4. When launching a test that needs rebuilding, build only the required target through `ssh_ya make <target>`, which uses relwithdebinfo and syncs local changes. Follow root AGENTS.md build constraints. Do not edit sources during the build.
5. For a test, locate the produced executable on that host and inspect its help to choose a single case. The `ya make -F` filter is not necessarily the executable's filter. Run the chosen case once without perf to confirm it works.
6. For attachment, use the existing executable and its matching symbols without rebuilding or syncing. Create a unique remote artifact directory outside the synced checkout. Record the host, local commit and dirty diff, remote revision, build options, executable path and build ID, workload command, kernel, and perf version. Keep the matching executable and debug symbols available for later symbolization.

## Record

Run perf around the built executable, rather than around `ya make`, so the profile excludes compilation and test-runner work. After the build, use `execute --skip-sync` to preserve that exact checkout and executable.

The following commands are templates: replace `<run-dir>`, `<binary>`, `<case-args>`, and `<pid>` with verified values and quote paths and arguments for both shells.

```bash
# One selected test or benchmark, user-space CPU cycles, starting at 99 Hz.
ssh_ya execute --skip-sync sh -c 'perf record -e cycles:u -F 99 --call-graph fp -o <run-dir>/perf.data -- <binary> <case-args>'

# Attach to a verified running process for a bounded interval.
ssh_ya execute --skip-sync sh -c 'perf record -e cycles:u -F 99 --call-graph fp -p <pid> -o <run-dir>/perf.data -- sleep 30'
```

1. Verify frame pointers for the actual build and inspect captured stacks. `fp` needs frame pointers; relwithdebinfo alone is not proof. For truncated stacks or binaries without frame pointers, retry with `--call-graph dwarf,16384` and keep its larger artifact separately.
2. If hardware cycles are unavailable on the host, check `perf list` and use `cpu-clock:u` when supported. Record the event change; do not compare unlike events as equivalent measurements.
3. Preserve perf stderr and the workload's exit status. Check for lost samples and unresolved symbols. Increase the interval or repeat the workload for short tests before raising the sample rate. Bound repeats for workloads that may hang.
4. Keep every run separately. For comparisons use the same host, event, build options, workload, and sample settings. Measure runtime without perf too; sampled shares are not absolute timings. Do not select only the fastest run unless the benchmark explicitly requires that policy.

## Export and inspect

```bash
ssh_ya execute --skip-sync sh -c 'perf script --no-demangle -i <run-dir>/perf.data > <run-dir>/perf.script'
```

1. Symbolize on the recording host while matching binaries and symbols remain available. Check export exit status and nonempty output.
2. For a flame graph, use an available FlameGraph checkout: pipe perf.script through `c++filt -n` and `stackcollapse-perf.pl`, then pass the folded stacks to `flamegraph.pl`. Resolve tool paths first; install tools only within the authorized task scope.
3. Use `ydb/core/nbs/flamegraph_analysis/analyze_folded.py` on the folded stacks for thread, self, and inclusive sample shares. Read its `--help` before selecting filters. Do not apply NBS-specific symbol interpretations or discard scheduler samples blindly; state any exclusions and their denominator.
4. Preserve perf.data, perf.script, folded stacks, optional SVG, recording stderr, workload output, and run metadata. Report the remote host and absolute artifact paths; copy requested artifacts locally using the available SSH file-transfer tools.
5. Report the selected case, event, sample count, stack quality, hottest paths, and limitations. Do not claim a performance regression from one sampled run or mark a failed test as a successful profiling result.

## Existing examples and flag references

- `ydb/core/nbs/flamegraph_analysis/README.md`: attach, fold, and analyze an NBS process. Its observations about scheduler samples and libc symbols describe that workload.
- `ydb/core/kqp/tools/combiner_perf/bin/perf_driver.py`: a specialized JSON benchmark runner using DWARF and fastest-of-five selection. Reuse only when the command emits integer `resultTime`; it is not a generic test runner.
- [perf record manual](https://man7.org/linux/man-pages/man1/perf-record.1.html): event, frequency, call graph, output, and PID options.
- [perf script manual](https://man7.org/linux/man-pages/man1/perf-script.1.html): export and symbolization options. Check the installed version's help when flags differ.
