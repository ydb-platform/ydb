# Service Node, Worker Node, and dq_cli Utility

`service_node` and `worker_node` are testing utilities designed for debugging a distributed SQL engine in a distributed configuration. These utilities can be run on a single host or multiple hosts. Query plan nodes are executed inside `worker_node`, and multiple tasks are created for each node. Each task is executed within a `ComputeActor` inside a specific `worker_node`. `service_node` is responsible for launching `ComputeActor` on `worker_node`, managing them, retrieving results, and sending them back to the user. You should run one instance of `service_node`, and multiple instances of `worker_node` can be launched.

## Running `service_node`

Parameters for running `service_node`:

- `--id`: a unique process ID (a number greater than 0).
- `--port`: the base port for the interconnect protocol.
- `--grpcport`: the client port (dqrun or dq_cli can connect to this port).

Example command to start `service_node`:

```bash
service_node --id 1 --port 5555 --grpcport 8080
```

## Running `worker_node`

Parameters for running `worker_node`:

- `--id`: a unique process ID (a number greater than 0).
- `--port`: the base port for the interconnect protocol.
- `--service_addr`: the connection string to `service_node` using the grpc protocol (host:port).
- `--workers`: the number of `ComputeActors` that can be launched inside `worker_node`.

Example command to start `worker_node`:

```bash
worker_node --id 2 --port 5556 --service_addr localhost:8080 --workers 4
```

**Note:** The `--port` option sets the base port, and upon launch, each utility will check if it is available. If the base port is occupied, the utility will attempt to use the next port in sequence, repeating this process for 100 ports starting from the base port.

## dq_cli Utility

The `dq_cli` utility is used to retrieve information from `service_node`.

Parameters for running `dq_cli`:

- `-h`: host.
- `-p`: port.
- `command`: as a command, you can pass `info`, in which case information about all `worker_node`s and their states will be displayed.

Example command to use `dq_cli`:

```bash
dq_cli -h localhost -p 8080 info
```

**Example Output of `dq_cli info`:**

```plaintext
Revision: "0577215664901532860606512090082402431042"
Worker {
  Guid: "dae96494-424f5c0c-740f1a75-fcb4e42f"
  NodeId: 4
  Address: "::1"
  Port: 1025
  Epoch: 1
  LastPingTime: "2023-10-25T21:37:02.020298Z"
  Revision: "0577215664901532860606512090082402431042"
  ClusterName: "hume"
  Resource: "0577215664901532860606512090082402431042"
  FreeDiskSize: 16000000000
  Capacity: 1
  StartTime: "2023-10-25T21:35:02.652718Z"
  Rusage {
    Stime: 1868530
    Utime: 8320152
  }
}
Worker {
  Guid: "581aea43-74251e94-8ddde277-882bdaf6"
  NodeId: 3
  Address: "::1"
  Port: 1024
  Epoch: 1
  LastPingTime: "2023-10-25T21:37:02.019857Z"
  Revision: "0577215664901532860606512090082402431042"
  ClusterName: "hume"
  Resource: "0577215664901532860606512090082402431042"
  FreeDiskSize: 16000000000
  Capacity: 1
  StartTime: "2023-10-25T21:34:56.530492Z"
  Rusage {
    Stime: 2866575
    Utime: 8012477
    MajorPageFaults: 916
  }
}
FreeListSize: 2
Capacity: 2
```

In this example, `NodeId` corresponds to the IDs set in the command line, and `Capacity` indicates how many `ComputeActors` are allowed to run (corresponds to the `--workers` option).
