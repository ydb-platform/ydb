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

