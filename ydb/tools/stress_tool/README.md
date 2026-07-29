# YDB Device Test Tool

## Description

The utility is designed for testing and evaluating the performance of storage devices and allows applying load in several different ways, including those characteristic of the YDB storage layer, as well as performing performance parameter assessment.

**WARNING! During testing, data on the tested storage device will be overwritten with a test pattern.**

The series of experiments is described in a configuration file (default cfg.txt). It supports working with rotary hard drives (ROT), solid-state drives (SSD), and NVMe. Test results can be output in wiki markup format, human-readable format, or as a JSON document.

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

#### Parameters for `DDiskLoad`
- `Tag` - a unique numeric identifier for the load source.
- `DDiskId` - the DDisk address `{ NodeId, PDiskId, DDiskSlotId }`.
- `DurationSeconds` - the duration of the load application in seconds.
- `InFlight` - the number of concurrently sent requests.
- `InitInFlight` - the maximum number of concurrent initialization requests (defaults to `InFlight` if omitted).
- `IntervalMsMin` and `IntervalMsMax` - interval mode settings; `0/0` means continuous load.
- `ExpectedChunkSize` - expected chunk size in bytes; must be divisible by `IoSizeBytes`.
- `IoSizeBytes` - request size for DDisk read/write I/O in bytes (default `4096`).
- `Areas` - the set of DDisk areas used by the load source; each `AreaSize` must be divisible by `IoSizeBytes`.
- `IsReadLoad` - if `true`, run read load; if `false`, run write load.
- `SQPoll` / `IOPoll` - enable io_uring SQPOLL / IOPOLL for direct I/O.

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

  Optional: `--use-uring` selects the io_uring transport instead of epoll for
  the interconnect connection itself (Linux 5.19+).

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

