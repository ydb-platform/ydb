# StorageLoad

Tests the read/write performance to and from Distributed Storage. The load is generated on Distributed Storage directly without using any tablet and Query Processor layers. When testing write performance, the actor writes data to the specified storage group. To test read performance, the actor first writes data to the specified storage group and then reads the data. After the load is removed, all the data written by the actor is deleted.

You can generate three types of load:

* _Continuous_: The actor ensures that the specified number of requests are running concurrently. To generate a continuous load, set a zero interval between requests (e.g., `WriteIntervals: { Weight: 1.0 Uniform: { MinUs: 0 MaxUs: 0 } }`), while keeping the `MaxInFlightWriteRequests` parameter value different from zero and omit the `WriteHardRateDispatcher` parameter.
* _Interval_: The actor runs requests at specific intervals. To generate an interval load, set a non-zero interval between requests, e.g., `WriteIntervals: { Weight: 1.0 Uniform: { MinUs: 50000 MaxUs: 50000 } }` and don't set the `WriteHardRateDispatcher` parameter. The maximum number of in-flight requests is set by the `InFlightWrites` parameter (0 means unlimited).
* _Hard rate_: The actor runs requests at certain intervals, but the interval length is adjusted to maintain a configured request rate per second. If the duration of the load is limited by `LoadDuration` than the request rate may differ between start and finish of the workload and will adjust gradually throughout all the main load cycle. To generate a load of this type, set the [parameters of hard rate load](#hardRateDispatcher) (parameter `WriteHardRateDispatcher`). Note that if this parameter is set, the hard rate type of load will be launched, regardless the value of the `WriteIntervals` parameter. The maximum number of in-flight requests is set by the `InFlightWrites` parameter (0 means unlimited).

## Actor parameters {#options}

{% include [load-actors-params](../_includes/load-actors-params.md) %}

| Parameter | Description |
--- | ---
| `DurationSeconds` | Load duration. The timer starts upon completion of the initial data allocation. |
| `Tablets` | The load is generated on behalf of a tablet with the following parameters:<ul><li>`TabletId`: Tablet ID. It must be unique for each load actor across all the cluster nodes. This parameter and `TabletName` are mutually exclusive.</li><li>`TabletName`: Tablet name. If the parameter is set, tablets' IDs will be assigned automatically, tablets launched on the same node with the same name will be given the same ID, tablets launched on different nodes will be given different IDs.</li><li>`Channel`: Tablet channel.</li><li>`GroupId`: ID of the storage group to get loaded.</li><li>`Generation`: Tablet generation.</li></ul> |
| `WriteSizes` | Size of the data to write. It is selected randomly for each request from the `Min`-`Max` range. You can set multiple `WriteSizes` ranges, in which case a value from a specific range will be selected based on its `Weight`. |
| `WriteHardRateDispatcher` | Setting up the [parameters of load with hard rate](#hardRateDispatcher) for write requests. If this parameter is set than the value of `WriteIntervals` is ignored. |
| `WriteIntervals` | Setting up the [parameters for probabilistic distribution](#params) of intervals between the records loaded at intervals (in milliseconds). You can set multiple `WriteIntervals` ranges, in which case a value from a specific range will be selected based on its `Weight`. |
| `MaxInFlightWriteRequests` | The maximum number of write requests being processed simultaneously. |
| `ReadSizes` | Size of the data to read. It is selected randomly for each request from the `Min`-`Max` range. You can set multiple `ReadSizes` ranges, in which case a value from a specific range will be selected based on its `Weight`. |
| `WriteHardRateDispatcher` | Setting up the [parameters of load with hard rate](#hardRateDispatcher) for read requests. If this parameter is set than the value of `ReadIntervals` is ignored. |
| `ReadIntervals` | Setting up the [parameters for probabilistic distribution](#params) of intervals between the queries loaded by intervals (in milliseconds). You can set multiple `ReadIntervals` ranges, in which case a value from a specific range will be selected based on its `Weight`. |
| `MaxInFlightReadRequests` | The maximum number of read requests being processed simultaneously. |
| `FlushIntervals` | Setting up the [parameters for probabilistic distribution](#params) of intervals (in microseconds) between the queries used to delete data written by the write requests in the main load cycle of the StorageLoad actor. You can set multiple `FlushIntervals` ranges, in which case a value from a specific range will be selected based on its `Weight`. Only one flush request will be processed concurrently. |
| `PutHandleClass` | [Class of data writes](#writeClass) to the disk subsystem. If the `TabletLog` value is set, the write operation has the highest priority. |
| `GetHandleClass` | [Class of data reads](#readClass) from the disk subsystem. If the `FastRead` is set, the read operation is performed with the highest speed possible. |
| `Initial allocation` |  Setting up the [parameters for initial data allocation](#initialAllocation). It defines the amount of data to be written before the start of the main load cycle. This data can be read by read requests along with the data written in the main load cycle.  |

### Write requests class {#writeClass}
| Class | Description |
--- | ---
| `TabletLog` | The highest priority of write operation. |
| `AsyncBlob` | Used for writing SSTables and their parts. |
| `UserData` | Used for writing user data as separate blobs. |

### Read requests class {#readClass}
| Class | Description |
--- | ---
| `AsyncRead` | Used for reading compacted tablets' data. |
| `FastRead` | Used for fast reads initiated by user. |
| `Discover` | Reads from Discover query. |
| `LowRead` | Low priority reads executed on the background. |

### Parameters of probabilistic distribution {#params}

{% include [load-actors-params](../_includes/load-actors-interval.md) %}

### Parameters of load with hard rate {#hardRateDispatcher}

| Parameter | Description |
--- | ---
| `RequestRateAtStart` | Requests per second at the moment of load start. If load duration limit is not set then the request rate will remain the same and equal to the value of this parameter. |
| `RequestRateOnFinish` | Requests per second at the moment of load finish. |

### Parameters of initial data allocation {#initialAllocation}

| Parameter | Description |
--- | ---
| `TotalSize` | Total size of allocated data. This parameter and `BlobsNumber` are mutually exclusive. |
| `BlobsNumber` | Total number of allocated blobs. |
| `BlobSizes` | Size of the blobs to write. It is selected randomly for each request from the `Min`-`Max` range. You can set multiple `WriteSizes` ranges, in which case a value from a specific range will be selected based on its `Weight`. |
| `MaxWritesInFlight` | Maximum number of simultaneously processed write requests. If this parameter is not set then the number of simultaneously processed requests is not limited. |
| `MaxWriteBytesInFlight` | Maximum number of total amount of simultaneously processed write requests' data. If this parameter is not set then the total amount of data being written concurrently is unlimited. |
| `PutHandleClass` | [Class of data writes](#writeClass) to the disk subsystem. |
| `DelayAfterCompletionSec` | The amount of time in seconds the actor will wait upon completing the initial data allocation before starting the main load cycle. If its value is `0` or not set the load will start immediately after the completion of the data allocaion. |

{% include [load-actors-params](../_includes/load-actors-interval.md) %}

## Examples {#examples}

### Write load {#write}

The following actor writes data to the group with the ID `2181038080` during `60` seconds. The size per write is `4096` bytes, the number of in-flight requests is no more than `256` (continuous load):

```proto
StorageLoad: {
    DurationSeconds: 60
    Tablets: {
        Tablets: { TabletId: 1000 Channel: 0 GroupId: 2181038080 Generation: 1 }
        WriteSizes: { Weight: 1.0 Min: 4096 Max: 4096 }
        WriteIntervals: { Weight: 1.0 Uniform: { MinUs: 0 MaxUs: 0 } }
        MaxInFlightWriteRequests: 256
        FlushIntervals: { Weight: 1.0 Uniform: { MinUs: 10000000 MaxUs: 10000000 } }
        PutHandleClass: TabletLog
    }
}
```

When viewing test results, the following values should be of most interest to you:

* `Writes per second`: Number of writes per second, e.g., `28690.29`.
* `Speed@ 100%`: 100 percentile of write speed in MB/s, e.g., `108.84`.

### Read load {#read}

To generate a read load, you need to write data first. Data is written by requests of `4096` bytes every `50` ms with no more than `1` in-flight request (interval load). If a request fails to complete within `50` ms, the actor will wait until it is complete and run another request in `50` ms. Data older than `10`s is deleted. Data reads are performed by requests of `4096` bytes with `16` in-flight requests allowed (continuous load):

```proto
StorageLoad: {
    DurationSeconds: 60
    Tablets: {
        Tablets: { TabletId: 5000 Channel: 0 GroupId: 2181038080 Generation: 1 }
        WriteSizes: { Weight: 1.0 Min: 4096 Max: 4096}
        WriteIntervals: { Weight: 1.0 Uniform: { MinUs: 50000 MaxUs: 50000 } }
        MaxInFlightWriteRequests: 1

        ReadSizes: { Weight: 1.0 Min: 4096 Max: 4096 }
        ReadIntervals: { Weight: 1.0 Uniform: { MinUs: 0 MaxUs: 0 } }
        MaxInFlightReadRequests: 16
        FlushIntervals: { Weight: 1.0 Uniform: { MinUs: 10000000 MaxUs: 10000000 } }
        PutHandleClass: TabletLog
        GetHandleClass: FastRead
    }
}
```

When viewing test results, the following value should be of most interest to you:

* `ReadSpeed@ 100%`: 100 percentile of read speed in MB/s, e.g., `60.86`.

### Read only {#readonly}

Before the start of the main load cycle the `1 GB` data block of blobs with sizes between `1 MB` and `5 MB` is allocated. To avoid overloading the system with write requests the number of simultaneously processed requests is limited by the value of `5`. After completing the initial data allocation the main cycle is launched. It consists of read requests sent with increasing rate: from `10` to `50` requests per second, the rate will increase gradually for `300` seconds.

```proto
StorageLoad: {
    DurationSeconds: 300
    Tablets: {
        Tablets: { TabletId: 5000 Channel: 0 GroupId: 2181038080 Generation: 1 }

        MaxInFlightReadRequests: 10
        GetHandleClass: FastRead
        ReadHardRateDispatcher {
                RequestsPerSecondAtStart: 10
                RequestsPerSecondOnFinish: 50
        }

        InitialAllocation {
                TotalSize: 1000000000
                BlobSizes: { Weight: 1.0 Min: 1000000 Max: 5000000 }
                MaxWritesInFlight: 5
        }
    }
}
```
Calculated percentiles will only represent the requests of the main load cycle and won't include write requests sent during the initial data allocation. The [graphs in Monitoring](../reference/observability/metrics/grafana-dashboards.md) should be of interest, for example, they allow to trace the request latency degradation caused by the increasing load.
