# Local TPC-C LBR profile

`TestTpccLocalLbr` is a LARGE profiling test tagged `ya:manual`.
It runs in a single test process, without splitting into files or test cases. It starts three static and three
dynamic YDB processes on the local machine, with `mirror-3-dc` and file-backed
PDisks. `YDB_USE_IN_MEMORY_PDISKS=true` is rejected before starting the cluster;
unset it or set it to `false`. A nonempty `YDB_PDISKS_DIRECTORY` is also rejected:
use `tpc-lbr-pdisk-directory` to select storage explicitly. Ordinary TPC tests keep their existing configuration.

Supply external executables for both the server and CLI. To reproduce the baseline
experiment, use Sandbox resources **14259363241** (ydbd) and **14258473698** (CLI).
The server revision is `640467cc0f2b88a6fecdd597739acbd50b8fb4c0`; the CLI revision
is `d91aeff7eda9a2c8aa106787d4d847d7759e3d25`.

For example, after unpacking the resources into `/absolute/path/baseline/`:

```bash
./ya make --build=relwithdebinfo -tA ydb/tests/functional/lbr \
  --test-filter='*TestTpccLocalLbr*' \
  --test-tag=ya:manual \
  --test-param=tpc-lbr-ydbd=/absolute/path/baseline/ydbd \
  --test-param=ydb-cli=/absolute/path/baseline/ydb \
  --test-param=tpc-lbr-llvm-directory=/absolute/path/llvm/bin \
  --test-param=tpc-lbr-warehouses=1000 \
  --test-param=tpc-lbr-seconds=1800
```

The test uses Serializable transactions, automatic CLI thread count and legacy
compaction. It records import, legacy compaction and workload into separate
`lbr/import`, `lbr/compaction` and `lbr/workload` directories, each with its own
`manifest.json`, `snap*.perf.data` and `snap*.log` files.

Import and compaction are sampled immediately and then every 60 seconds until
the phase ends. A final shorter snapshot is stopped gracefully and flushed.
The collector allows 60 seconds for perf to flush after SIGINT; if it still
does not exit, it is killed and the snapshot is rejected. Reaping after SIGKILL
is limited to another 10 seconds; waiting for the collector thread is bounded
at 75 seconds. An unreaped process never makes a snapshot valid.
Its manifest records the actual duration and `stopped_on_phase_end`. A failed
phase remains marked invalid. The default record duration is 30 seconds.
SDK compaction is not a separate phase: it happens inside CLI import; the local
training test deliberately uses legacy compaction.

Workload collection starts after an explicit warmup. The default warmup is 600
seconds, configurable with `--test-param=tpc-lbr-warmup-seconds=300` for the
100-warehouse experiment. Three snapshots are spaced evenly inside the measured
workload, after that delay. A workload that ends before its scheduled snapshots
finish is rejected with an incomplete-workload error, including interruption
of an active snapshot at phase end. If the workload fails before collection
starts, its manifest has `started_at: null`, no records and `valid: false`;
the original test error is preserved. The CLI may enforce a minimum warmup for terminal startup;
the parameter must be at least `warehouses // 10 + 1` seconds for the baseline CLI.
This bound comes from ten terminals per warehouse, started at 10 ms intervals.
Another CLI build may enforce a different minimum and silently extend warmup,
shifting workload snapshots into warmup. When changing CLI builds, check its
reported effective warmup and choose a sufficiently large explicit value; the
collector does not derive its schedule from the CLI's effective warmup.
Both workload and preparation sampling schedules are validated before cluster startup.

The command is `perf record -m 64 -F 99 -e cycles:u -b -p <six server PIDs>`.
No system-wide or CLI sampling is performed. The smaller per-CPU mmap buffer
avoids locked-memory allocation failures. The user must have permission to record
hardware events and LBR for these processes. A failure to record makes the test fail.

Use `--test-param=tpc-lbr-pdisk-directory=/absolute/writable/directory` to place
file-backed PDisks on a particular filesystem. For the baseline server, plain tmpfs does not support the required `O_DIRECT`
mode on older kernels. Use an ext4 loop filesystem backed by a private image on
tmpfs instead. RAM-backed storage changes the storage characteristics relative
to the cluster; record this difference when comparing profiles. The directory must already exist.

Parameters `tpc-lbr-record-seconds`, `tpc-lbr-snapshots` and `tpc-lbr-perf` override
the recording duration, workload snapshot count and perf executable.
`tpc-lbr-phase-interval-seconds` sets the preparation-phase sampling interval
(at least the record duration); `tpc-lbr-mmap-pages` sets the perf buffer size.
For a smoke run use one warehouse, a two-second explicit warmup, a 12-second
workload, one workload snapshot and a one-second recording.
The profiling default is 256 GiB per sparse PDisk file, overridable with
`tpc-lbr-pdisk-size-gib`. The three-zone topology uses three PDisks per static
node, with an expected slot count of four. Both static storage and the tenant
storage pool use `mirror-3-dc`. Account for
available disk space when increasing the number of warehouses.

Results are stored in the test output directory: `lbr-run.json`, binary version
outputs, and the separate `lbr/{import,compaction,workload}/` directories. Failed phases and incomplete workload snapshots must not be merged into a training
profile. A gracefully stopped partial snapshot of a successful preparation phase
is valid; its shorter duration is preserved in the manifest.

For comparison with the cluster profile, use the same LLVM 20 tools as the
baseline experiment and the exact server executable that was sampled. As in
that experiment, retain binary mappings and samples with a nonempty branch stack:


```bash
# Convert each phase separately using the exact executable that was sampled.
export BINARY=/absolute/path/baseline/ydbd
perf script -G --show-mmap-events -F ip,brstack -i snap1.perf.data | \
  python3 -c '
import os
from pathlib import Path
import re
import sys

binary = Path(os.environ["BINARY"])
paths = {os.fsencode(binary.absolute()), os.fsencode(binary.resolve())}
mmap = re.compile(rb"\bPERF_RECORD_MMAP2?\s+\d+/\d+:\s+\[[^\]]+\]:\s+(?:[-rwxps]{4}\s+)?(.+)$")
branch = re.compile(rb"0x[0-9a-fA-F]+/0x[0-9a-fA-F]+")
for line in sys.stdin.buffer:
    event = mmap.search(line.rstrip())
    if branch.search(line) or (event is not None and event[1] in paths):
        sys.stdout.buffer.write(line)
' > snap1.lbr
llvm-profgen --binary="$BINARY" \
  --perfscript=snap1.lbr --output=snap1.prof --use-md5
# Repeat for the remaining snapshots, then:
llvm-profdata merge --sample -o local.prof snap1.prof snap2.prof snap3.prof
llvm-profdata overlap --sample cluster-589.prof local.prof
```

Keep the resulting phase profiles separate. Explicit weights can be supplied to
`llvm-profdata merge --sample --weighted-input=<weight>,<phase.prof>` when building
an experimental mixture; phase durations should not implicitly choose those weights.

A smaller warehouse count and colocated processes change the workload and
resource contention. Preserve those parameters alongside the comparison; do not
interpret profile similarity as a direct performance comparison.

The 1000-warehouse training run used about 142 GiB for the server/test processes,
in addition to RAM-backed storage. This opt-in mode is intended for a suitably
sized local host; the test suite RAM requirement does not describe its
training resource needs. The suite timeout is four hours, including import,
compaction, workload, conversion and merging. Reduce the training parameters or
raise the runner timeout for longer runs; a suite timeout can terminate the
process before manifests and conversion reports are finalized.

The collector unit tests are in `ut/`: a SMALL suite without `ya:manual`.
They use a fake perf executable and do not start YDB or collect hardware profiles.


## Conversion during collection

Each closed, valid snapshot immediately starts its own conversion in a background
thread, without a worker limit or a waiting queue. Provision memory for the
cluster and concurrent converters; if conversions fall behind recording, their
combined memory use grows. An open perf file is never submitted. Import and compaction
conversion overlaps the later phases; the test waits for the remaining jobs
at the end of the test and merges each successful phase separately.

Pass `--test-param=tpc-lbr-llvm-directory=/absolute/path/to/llvm/bin` to select
`llvm-profgen` and `llvm-profdata`; otherwise they are found on PATH. If the
chosen LLVM needs shared libraries, supply its `LD_LIBRARY_PATH` in the test
execution environment. Conversion uses MD5 names without profile compression.
The exact sampled ydbd is used, without binary rewriting or an LLVM patch.

`profiles/<phase>/snap*.prof` are published after successful conversion by an
atomic rename. Command lines, logs, wall time, peak RSS and file sizes are kept
next to each snapshot. `profiles/conversion.json` records phase validity and
errors. A failed or incomplete phase is not merged, even if some of its
snapshots converted successfully. Per-snapshot files from invalid phases remain
available for diagnostics; their existence does not certify phase validity.
For training use only the final `profiles/<phase>.prof` files, produced by a
successful phase merge, and check `conversion.json` and the phase manifest.
Do not glob `profiles/<phase>/snap*.prof` into a training profile.
Conversion errors in successful phases fail
the test. Sampling errors keep their original exception; setup failure drains
or cancels the pending conversion jobs. Each conversion command has a one-hour
timeout; expiry and cancellation terminate its process group. Failed conversion
or merging of one phase does not prevent publishing other successful phases.

The collector checks that `perf` is executable before starting a phase. Hardware
LBR support and permissions are checked by the actual recording. GNU time at
`/usr/bin/time` is optional: without it conversion still runs, but
`peak_rss_kib` is null. Merge-error report rows have `index: null`.
