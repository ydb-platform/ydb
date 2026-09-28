# YDB Device Test Tool

## Description

The utility is designed for testing and evaluating the performance of storage devices and allows applying load in several different ways, including those characteristic of the YDB storage layer, as well as performing performance parameter assessment.

**WARNING! During testing, data on the tested storage device will be overwritten with a test pattern.**

The series of experiments can be described in a configuration file (default cfg.txt for the legacy invocation). DDisk tests also support a config-free `ddisk` command. The tool supports rotary hard drives (ROT), solid-state drives (SSD), and NVMe. Test results can be output in wiki markup format, human-readable format, or as a JSON document.

## DDisk command

Run a random write workload, or select measured reads with `--read-only`:

```bash
ydb_stress_tool ddisk --path /dev/nvme0n1 --type NVME --output-format human
ydb_stress_tool ddisk --path /dev/nvme0n1 --type NVME --read-only --inflight 128
```

Without `--cfg`, the defaults are 32 equally weighted areas per device, 128 MiB per
area, 10 seconds of load, 128 requests in flight, random access, and 4096-byte I/O.
Initialization takes additional time; there is no measurement warm-up delay.
The command formats local and server PDisks with exactly 128 MiB physical chunks,
without the extra space used by legacy PDisk sector-metadata sizing.

| Workload option | Default | Meaning |
| --- | --- | --- |
| `--areas N` | 32 | Number of 128 MiB areas per device |
| `--duration SECONDS` | 10 | Load duration, excluding initialization |
| `--read-only` | Off | Measure reads instead of writes |
| `--io-size BYTES` | 4096 | Request size; power of two from 4096 through 134217728 |
| `--sequential` | Off | Sequential access within each area |
| `--background-write-ratio R` | 0 | Unmeasured writes per measured read, from 0 to 1; nonzero requires `--read-only` |
| `--background-write-size-kib N` | 4 | Background-write size; power of two from 4 through 131072 KiB |

`--read-only` describes the measured workload: initialization still writes every
I/O slot, and an explicit `--background-write-ratio` enables unmeasured writes.
Area size and measurement delay have no CLI options.

Use common `--inflight N` for one queue depth, or both `--inflight-from N` and
`--inflight-to N` for a doubling sweep. These forms are mutually exclusive.
The `ddisk` command requires positive values and an ordered, complete range.

```bash
ydb_stress_tool ddisk --path /dev/nvme0n1 --read-only --areas 16 --duration 20 \
    --inflight-from 1 --inflight-to 128 --run-count 3 --output-format human
```

Repeat `--path` for multiple local devices. Existing checksum, PDisk fallback,
encryption, output, and monitoring options remain available. `ddisk --help`
groups workload, DDisk runtime, and client/server flags separately from common options.

For a remote DDisk test, start the server and then the client:

```bash
# Server host (node 1; expects client node 2)
ydb_stress_tool ddisk --server 1 --client 2 --ic-port 19001 --path /dev/nvme0n1
# Client host
ydb_stress_tool ddisk --client 2 --endpoint server-host:19001 --read-only
```

Repeat `--endpoint` for multiple servers; endpoints receive node IDs starting at 1.
Use `--num-server-devices N` on the client when each server has multiple devices.
The client's node ID must differ from every server's node ID.

`ddisk --cfg FILE` uses DDisk workloads from a config file. It cannot be combined
with the workload options in the table; common inflight overrides and existing
runtime/network options remain available. Config values, including measurement
delay, are preserved unless overridden by the common inflight controls.

The legacy invocation, `ydb_stress_tool --cfg FILE <options>`, remains supported,
including implicit `cfg.txt` loading, its existing inflight-range parsing, and its
disk formatting behavior. The new `ddisk` command never implicitly reads `cfg.txt`.

## Configuration File

The following types of test loads are supported:
- `AioTestList` - submitting read/write requests using libaio.
- `TrimTestList` - submitting trim (blkdiscard) requests in specified block sizes, preceded by data writing.
- `PDiksTestList` - applying load through YDB storage layer code (PDisk).

### Parameters for `AioTestList`
These describe read/write requests using libaio.

- `DurationSeconds` - the duration of the load application in seconds.
- `RequestSize` - the size of the requests in bytes.
- `QueueDepth` - the number of concurrently sent requests.
- `ReadProportion` - the proportion of read requests, with the proportion of write requests defined as (1.0 - ReadProportion). For instance, to submit an equal number of read and write requests, use a ReadProportion value of 0.5.

### Parameters for `TrimTestList`
These describe trim (blkdiscard) requests in specified block sizes, preceded by data writing.

- `DurationSeconds` - the duration of the load application in seconds.
- `RequestSize` - the size of the requests in bytes.

### Parameters for `PDiksTestList`
These describe the load applied through the YDB storage layer code (PDisk).

Multiple load sources can be running simultaneously.
- `PDiskReadLoad` - description of the read load source.
- `PDiskWriteLoad` - description of the write load source.

During normal operation, PDisk sends Trim requests to SSDs. This behavior can be enabled/disabled in the test using the EnableTrim parameter.

#### Parameters for `PDiskReadLoad`
These describe the read load source.

- `Tag` - a unique numeric identifier for the load source.
- `PDiskId` - the address of the PDisk, must be set to 1.
- `PDiskGuid` - a globally unique identifier for the PDisk, must be set to 12345.
- `VDiskId` - the address of the VDisk used for load application, must be set to { GroupID: 1 GroupGeneration: 5 Ring: 1 Domain: 1 VDisk: 1 }.
- `Chunks` - a set of entries that specifies the number of chunks used (by default, a chunk size is 128 megabytes). Each chunk contains a configurable number of slots (blocks, whose entire write is performed by one request), set by the Slots parameter. Requests are sent to different chunks, with requests either targeting random chunks or sequentially.
- `DurationSeconds` - the duration of the load application in seconds.
- `IntervalMsMin` - a reserved parameter for future use, must be set to 0.
- `IntervalMsMax` - a reserved parameter for future use, must be set to 0.
- `InFlightWrites` - the number of concurrently sent requests.
- `Sequential` - a parameter that controls the addresses to which requests are made. When true, requests are made to sequentially located data blocks. When false, requests are made to randomly located data blocks.
- `IsWardenlessTest` - a reserved parameter for future use, must be set to true.

#### Parameters for `PDiskWriteLoad`
These describe the write load source.
It has the same settings as PDiskReadLoad, with an additional parameter `LogMode`.

- `LogMode` - the mode of logging metadata about the write into a "chunk". The following modes are available:
  - `LOG_PARALLEL` - logging is performed concurrently with the write to the chunk; the operation is considered complete after both the logging and the write to the chunk are finished.
  - `LOG_SEQUENTIAL` - logging is performed after the successful completion of the write to the chunk; the entire operation is considered complete after the logging is finished, similar to what the current version of VDisk does.
  - `LOG_NONE` - logging is not performed; the operation is considered complete after the write to the chunk is finished.

### Parameters for `DDiskTestList`
These describe the load applied through the YDB storage layer code (DDisk).

Each `DDiskTestList` entry contains one or more `DDiskLoad` sources.

DDisk and Persistent Buffer checksums are enabled by default. Pass
`--disable-ddisk-checksums` to disable both checksum generation in the load
actors and checksum handling in DDisk. For client/server DDisk tests, pass the
option to both processes so the client and server use the same mode.

Pass `--ddisk-checksums-cache-size N` to set the checksum array cache size per
DDisk in MiB (1 MiB = 1024 * 1024 bytes; default: 64). Set it to `0` to disable
caching while keeping checksums enabled; necessary in-flight state is retained.
For client/server DDisk tests, pass this option to the server process.

Pass `--force-ddisk-pdisk-fallback` to route DDisk direct I/O through the PDisk
actor instead of io_uring.

#### Parameters for `DDiskLoad`
- `Tag` - a unique numeric identifier for the load source.
- `DDiskId` - the DDisk address `{ NodeId, PDiskId, DDiskSlotId }`.
- `DurationSeconds` - the duration of the load application in seconds.
- `InFlight` - the number of concurrently sent requests. Background writes share this queue-depth budget with measured reads.
- `InitInFlight` - the maximum number of concurrent initialization requests (defaults to `InFlight` if omitted).
- `IntervalMsMin` and `IntervalMsMax` - interval mode settings; `0/0` means continuous load.
- `ExpectedChunkSize` - expected chunk size in bytes; must be divisible by `IoSizeBytes`.
- `IoSizeBytes` - request size for DDisk read/write I/O in bytes (default `4096`).
- `Areas` - the set of DDisk areas used by the load source; each `AreaSize` must be divisible by `IoSizeBytes`.
  Per-area `InitType` defaults to `INIT_ZEROES_FULL` (every slot is written before the measured load). Use `INIT_ZEROES_FIRST_BLOCK` only to preallocate chunks; with checksums enabled, unread blocks are then served from RAM as zeros and do not hit the device. `INIT_NONE` is rejected for read load.
- `IsReadLoad` - if `true`, run read load; if `false`, run write load.
- `BackgroundWriteRatio` - unmeasured background writes per measured read during read load; `0` disables them and `1.0` issues one write per read. Background writes share `InFlight` and interval pacing with measured reads.
- `BackgroundWriteSizeKiB` - background write size in KiB (default `4`); must be a power of two and at least 4. When background writes are enabled, `ExpectedChunkSize` and each `AreaSize` must be divisible by this size.

### Parameters for `InterconnectTestList`
These describe network load generated through the actors library interconnect
subsystem (`ydb/library/actors/interconnect/load.h`), reusing the same
`TInterconnectLoad` load actor used by the `InterconnectLoad` load type of the
regular YDB load actor service.

By default (no `--server`/`--client` options), the stress tool runs a single
node; the load actor and its counterpart (the load responder) both live in the
same process, and traffic is routed through loopback using `NodeHops`, so no
real network connection is required to generate and measure interconnect
traffic.

To measure interconnect performance over a **real network** between two
hosts, run two copies of `ydb_stress_tool`:

- On the **second** (remote) host, start a responder-only instance. Its
  `--server NODE_ID` must match the NodeId that the client will assign this
  server via `--endpoint` (see below) -- for a single server this is `1`:
  ```
  ydb_stress_tool --cfg cfg.txt --server 1 --client 2 --ic-port 19001
  ```
  This registers the interconnect load responder for node 1 and listens on
  port 19001 for an incoming connection from the client (node 2). No device
  `--path` is required.

- On the **first** host, run the actual `InterconnectTestList` from the config
  as a client that connects out to the responder:
  ```
  ydb_stress_tool --cfg cfg.txt --client 2 --endpoint remote-host:19001
  ```
  The client's own NodeId is `2` (from `--client`, must differ from all
  server NodeIds); the server passed via `--endpoint` is assigned NodeId `1`
  (the i-th `--endpoint` gets NodeId `i + 1`, matching the `--server NODE_ID`
  used on the remote host). The config's `InterconnectLoad.NodeHops` must
  reference this NodeId (e.g. `NodeHops: [1]`) so that load messages are
  routed to the remote responder over the network instead of looping back
  locally.

Each `InterconnectTestList` entry contains one or more `InterconnectLoad`
sources, run sequentially. Detailed periodic throughput/RTT statistics are
logged to stderr via the `INTERCONNECT_SPEED_TEST` log component at `NOTICE`
level; the tool's own result table only prints a short summary row per test.

#### Parameters for `InterconnectLoad`
- `Tag` - a unique numeric identifier for the load source.
- `Name` - a human readable name for the load, shown in the results table.
- `DurationSeconds` - the duration of the load application in seconds.
- `DelayBeforeMeasurementsSeconds` - warm-up period (default 15s); throughput/RTT
  samples generated before this delay has elapsed are excluded from the
  reported statistics.
- `InFlyMax` - the maximum number of concurrently in-flight messages.
- `NodeHops` - the sequence of node ids the message is routed through before
  coming back to the load actor. For a single-node run use `[1]` (loopback to
  the local node).
- `SizeMin` / `SizeMax` - the min/max payload size in bytes for each message.
- `IntervalMinUs` / `IntervalMaxUs` - min/max interval between sent messages,
  in microseconds; `0/0` means messages are sent as fast as `InFlyMax` allows.
- `SoftLoad` - if `true`, keeps a steady send rate regardless of response
  latency; if `false`, waits for a response (or timeout) before scheduling the
  next send interval.
- `UseProtobufWithPayload` - if `true`, stores the payload in a separate rope
  buffer instead of inline in the protobuf message.
