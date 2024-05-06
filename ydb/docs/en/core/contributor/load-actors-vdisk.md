# VDiskLoad

Generates a write-only load on the VDisk. Simulates a Distributed Storage Proxy. The test outputs the VDisk write performance in operations per second.

{% include notitle [addition](../_includes/addition.md) %}

## Actor parameters {#options}

{% include [load-actors-params](../_includes/load-actors-params.md) %}

| Parameter | Description |
--- | ---
| `VDiskId` | Parameters of the VDisk used to generate load.<ul><li>`GroupID`: Group ID.</li><li>`GroupGeneration`: Group generation.</li><li>`Ring`: Group ring ID.</li><li>`Domain`: Ring fail domain ID.</li><li>`VDisk`: Index of the VDisk in the fail domain.</li></ul> |
| `GroupInfo` | Description of the group hosting the loaded VDisk (of the appropriate generation). |
| `TabletId` | ID of the tablet that generates the load. It must be unique for each load actor. |
| `Channel` | ID of the channel inside the tablet that will be specified in the BLOB write and garbage collection commands. |
| `DurationSeconds` | The total test time in seconds; when it expires, the load stops automatically. |
| `WriteIntervals` | Setting up the [parameters for probabilistic distribution](#params) of intervals between the records. |
| `WriteSizes` | Size of the data to write. It is selected randomly for each request from the `Min`-`Max` range. You can set multiple `WriteSizes` ranges, in which case a value from a specific range will be selected based on its `Weight`. |
| `InFlightPutsMax` | Maximum number of concurrent BLOB write queries against the VDisk (TEvVPut queries); if omitted, the number of queries is unlimited. |
| `InFlightPutBytesMax` | Maximum number of bytes in the concurrent BLOB write queries against the VDisk (TEvVPut-requests). |
| `PutHandleClass` | Class of data writes to the disk subsystem. If the `TabletLog` value is set, the write operation has the highest priority. |
| `BarrierAdvanceIntervals` | Setting up the [parameters for probabilistic distribution](#params) of intervals between the advance of the garbage collection barrier and the write step. |
| `StepDistance` | Distance between the currently written step `Gen:Step` of the BLOB and its currently collected step. The higher is the value, the more data is stored. Data is written from `Step = X` and deleted from all the BLOBs where `Step = X - StepDistance`. The `Step` is periodically incremented by one (with the `BarrierAdvanceIntervals` period). |

### Parameters of probabilistic distribution {#params}

{% include [load-actors-params](../_includes/load-actors-interval.md) %}

<!-- ## Примеры {#examples} -->
