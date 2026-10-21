# Profiling an nbs2 partition dyn node with perf + FlameGraph

## Prerequisites

`FlameGraph` must be checked out next to the work dir:

```bash
ssh <host>
mkdir -p ~/flamegraphs && cd ~/flamegraphs
git clone https://github.com/brendangregg/FlameGraph.git ../FlameGraph   # -> ~/FlameGraph
mkdir -p work && cd work
```

Only `flamegraph.pl` and `stackcollapse-perf.pl` are actually used.

## 1. Find the process

```bash
ps aux | grep -E 'kikimr.*NBS' | grep -v grep
# sanity-check it is the right one and see how busy it is:
ps -p <pid> -o pid,etime,nlwp,pcpu,rss,cmd --no-headers
top -b -n1 -H -p <pid> | head -25      # per-thread CPU — tells you which pools are hot
```

## 2. Record

```bash
cd ~/flamegraphs/work
sudo perf record -F 99 -g -p <pid> -o perf_analysis.data sleep 30
```

* `-F 99` — 99 Hz, deliberately not 100 to avoid beating against periodic timers.
* `-g` — call graphs. The binary is built with frame pointers, so the default
  fp unwinding works; if stacks come out truncated add `--call-graph dwarf`
  (much larger `perf.data`, ~10x).
* `-p <pid>` samples **all threads** of the process. That is what you want here —
  the interesting question is how work splits across the actor pools.
* Bump `sleep` to 60–120 s if the load is bursty. 30 s at 99 Hz across ~3 busy
  threads gives ~8 k samples, which is enough to resolve anything above ~0.5 %.

## 3. Fold the stacks (this is the artifact you actually analyse)

```bash
sudo perf script --no-demangle -i perf_analysis.data > perf_analysis.script
sudo chown $USER perf_analysis.script

c++filt -n < perf_analysis.script \
  | ~/FlameGraph/stackcollapse-perf.pl \
  > folded.txt
```

`--no-demangle` + external `c++filt -n` is much faster and more reliable than
perf's built-in demangler for Arcadia's very long template symbols.

`folded.txt` is one line per unique stack: `frame;frame;...;leaf <count>`.
It is small (a few thousand lines), greppable, and is the input to everything below.

## 4a. SVG flamegraph

```bash
~/FlameGraph/flamegraph.pl \
  --title "nbs2 partition dyn node (pid <pid>) 30s @99Hz" \
  folded.txt > nbs2_analysis.svg
```

## 4b. Text summary (preferred for analysis)

The SVG is good for exploring, bad for answering "where does the time go".
[analyze_folded.py](analyze_folded.py) turns `folded.txt` into ranked tables:

```bash
python3 analyze_folded.py folded.txt --top 30
```

Prints, each as a percentage with a bar:

* **threads** — samples grouped by root frame (= thread comm = actor pool)
* **self time** — leaf frames, i.e. where cycles are actually burnt
* **inclusive time** — every frame anywhere in a stack
* **hottest full stacks** — the top complete stacks, indented

Useful flags:

```bash
# Drop blocked/idle time. Every stack that went to sleep has finish_task_switch
# in it, so excluding it leaves a pure on-CPU profile. Do this first — otherwise
# futex_wait/epoll_wait/__schedule dominate and hide the real work.
python3 analyze_folded.py folded.txt --exclude finish_task_switch

# One actor pool at a time (root frame match)
python3 analyze_folded.py folded.txt --exclude finish_task_switch --thread kikimr.NBS_0

# Everything touching one symbol, with the call paths that reach it
python3 analyze_folded.py folded.txt --grep TTimePredictor --exclude finish_task_switch
```

Inclusive samples for an arbitrary pattern, straight from `folded.txt`:

```bash
grep -v finish_task_switch folded.txt \
  | awk -F' ' '/TTimePredictor/ { s += $NF } END { print s }'
```

## Reading the numbers

* **Always exclude `finish_task_switch` first.** In the raw profile ~27 % of
  samples are stacks caught going to sleep, and `perf_ctx_enable` /
  `__perf_event_task_sched_in` / `_raw_spin_unlock` (perf's own
  context-switch bookkeeping, ~25 % of raw self time) sit on top of them.
  Those are measurement artefacts, not the workload.
* **`__nss_database_lookup` is a lie.** glibc ifunc'd `memcpy`/`strlen` resolve
  to that symbol. Its callers here are tcmalloc refill and string append —
  read it as "libc mem/string ops".
* Percentages in the per-pool reports are relative to that pool, not the process.

