# CPU bottleneck

High CPU usage can lead to slow query processing and increased response times. When CPU resources are constrained, the database may have difficulty handling complex queries or large transaction volumes.

The CPU resources are mainly used by the actor system. Depending on the type, all actors run in one of the following pools:

- **System**: A pool that is designed for running quick internal operations in YDB (it serves system tablets, state storage, distributed storage I/O, and erasure coding).

- **User**: A pool that serves the user load (user tablets, queries run in the Query Processor).

- **Batch**: A pool that serves tasks with no strict limit on the execution time, background operations like backups, garbage collection, and heavy queries run in the Query Processor.

- **IO**: A pool responsible for performing any tasks with blocking operations (such as authentication or writing logs to a file).

- **IC**: Interconnect, it serves the load related to internode communication (system calls to wait for sending and send data across the network, data serialization, as well as message splits and merges).

## Diagnostics

<!-- The include is added to allow partial overrides in overlays  -->
{% include notitle [#](_includes/cpu-bottleneck.md) %}

<!-- If the spikes on these charts align, the increased latencies may be related to the higher number of rows being read from the database. In this case, the available database nodes might not be sufficient to handle the increased load. -->

## Recommendation

Add additional [database nodes](../../../../concepts/glossary.md#database-node) or allocate more CPU cores to the existing nodes.
