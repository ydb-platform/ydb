# StorageLoad

Tests the read/write performance to and from Distributed Storage. The load is generated on Distributed Storage directly without using any tablet and Query Processor layers. When testing write performance, the actor writes data to the specified VDisk group. To test read performance, the actor first writes data to the specified VDisk group and then reads the data. After the load is removed, all the data written by the actor is deleted.

You can generate two types of load:

* _Continuous_: The actor ensures the specified number of requests are run concurrently. To generate a continuous load, set a zero interval between requests (e.g., `WriteIntervals: { Weight: 1.0 Uniform: { MinUs: 0 MaxUs: 0 } }`), while keeping the `MaxInFlightWriteRequests` parameter value different from zero.
* _Interval_: The actor runs requests at preset intervals. To generate an interval load, set a non-zero interval between requests, e.g., `WriteIntervals: { Weight: 1.0 Uniform: { MinUs: 50000 MaxUs: 50000 } }`. Maximum number of in-flight requests is set by the `InFlightReads` parameter. If its value is `0`, their number is unlimited.

## Actor parameters {#options}

{% include [load-actors-params](../_includes/load-actors-params.md) %}

| Parameter | Description |
--- | ---
| ` DurationSeconds` | Load duration. |
| ` Tablets` | The load is generated on behalf of a tablet with the following parameters:<ul><li>` TabletId`: Tablet ID. It must be unique for each load actor.</li><li>` Channel`: Tablet channel.</li><li>` GroupId`: ID of the VDisk group to get loaded.</li><li>` Generation`: Tablet generation.</li></ul> |
| ` WriteSizes` | Size of the data to write. It is selected randomly for each request from the `Min`-`Max` range. You can set multiple `WriteSizes` ranges, in which case a value from a specific range will be selected based on its `Weight`. |
| ` WriteIntervals` | Setting up the [parameters for probabilistic distribution](#params) of intervals between the records loaded at intervals (in milliseconds). You can set multiple `WriteIntervals` ranges, in which case a value from a specific range will be selected based on its `Weight`. |
| ` MaxInFlightWriteRequests` | Maximum number of simultaneously processed write requests. |
| ` ReadSizes` | Size of the data to read. It is selected randomly for each request from the `Min`-`Max` range. You can set multiple `ReadSizes` ranges, in which case a value from a specific range will be selected based on its `Weight`. |
| ` ReadIntervals` | Setting up the [parameters for probabilistic distribution](#params) of intervals between the queries loaded by intervals (in milliseconds). You can set multiple `ReadIntervals` ranges, in which case a value from a specific range will be selected based on its `Weight`. |
| ` MaxInFlightReadRequests` | Maximum number of simultaneously processed read requests. |
| ` FlushIntervals` | Setting up the [parameters for probabilistic distribution](#params) of intervals (in microseconds) between the queries used to delete data written by the StorageLoad actor. You can set multiple `FlushIntervals` ranges, in which case a value from a specific range will be selected based on its `Weight`. |
| ` PutHandleClass` | Class of data writes to the disk subsystem. If the `TabletLog` value is set, the write operation has the highest priority. |
| ` GetHandleClass` | Class of data reads from the disk subsystem. If the `FastRead` is set, the read operation is performed with the highest speed possible. |

### Parameters of probabilistic distribution {#params}

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

* ` Writes per second`: Number of writes per second, e.g., `28690.29`.
* ` Speed@ 100%`: 100 percentile of write speed in MB/s, e.g., `108.84`.

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

* ` ReadSpeed@ 100%`: 100 percentile of read speed in MB/s, e.g., `60.86`.
