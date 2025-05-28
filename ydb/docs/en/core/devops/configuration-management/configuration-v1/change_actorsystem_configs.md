# Changing Actor System Configuration

{% include [deprecated](_includes/deprecated.md) %}

The actor system is the foundation of {{ ydb-short-name }} as most of system components are represented by one or more actors.
Each actor is distributed to a specific ExecutorPool corresponding to the actor's task.
Changing the configuration helps more precisely distribute the number of reserved cores for each type of task.

## Actor System Configuration Description

The actor system configuration consists of an enumeration of ExecutorPools, mapping ExecutorPools to task types, and actor system scheduler configurations.

Currently, there are the following task types and their corresponding pools:

* System - designed for executing fast internal {{ ydb-short-name }} operations;
* User - includes all user load for processing and executing incoming requests;
* Batch - tasks that do not have strict execution time limits, mainly executing background operations;
* IO - responsible for executing all tasks with blocking operations (such as writing logs to a file);
* IC - Interconnect, includes all load related to communication between nodes.

Each pool is described by an Executor field, as in the example below.

```proto
Executor {
  Type: BASIC
  Threads: 9
  SpinThreshold: 1
  Name: "System"
}
```

Description of main fields:

* **Type** - currently can have two types: **BASIC** and **IO**. All pools except **IO** have type **BASIC**;
* **Threads** - number of threads (number of actors working in parallel) in this pool;
* **SpinThreshold** - number of processor cycles before going to sleep when there are no tasks, which the thread executing actor work will perform (affects CPU consumption and request latency during light load);
* **Name** - pool name that will be displayed in node monitoring.

Pool mapping to task types is done by setting the pool's ordinal number in special fields. Pools are numbered from zero, and multiple task types can be assigned to one pool.

List of fields with their tasks:

* **SysExecutor** - System
* **UserExecutor** - User
* **BatchExecutor** - Batch
* **IoExecutor** - IO

Example:

```proto
SysExecutor: 0
UserExecutor: 1
BatchExecutor: 2
IoExecutor: 3
```

The IC pool is set differently, through ServiceExecutor, as in the example below.

```proto
ServiceExecutor {
  ServiceName: "Interconnect"
  ExecutorId: 4
}
```

The actor system scheduler is responsible for delivering delayed messages between actors and is set by the following parameters:

* **Resolution** - minimum time offset step in microseconds;
* **SpinThreshold** - similar to the pool parameter, number of processor cycles before going to sleep when there are no messages;
* **ProgressThreshold** - maximum time offset step in microseconds.

If for unknown reasons the scheduler thread gets stuck, it will send messages according to lagging time, shifting it each time by **ProgressThreshold**.

It is not recommended to change the scheduler configuration. In pool configurations, it is recommended to change only the number of threads.

Example of default actor system configuration:

```proto
Executor {
  Type: BASIC
  Threads: 9
  SpinThreshold: 1
  Name: "System"
}
Executor {
  Type: BASIC
  Threads: 16
  SpinThreshold: 1
  Name: "User"
}
Executor {
  Type: BASIC
  Threads: 7
  SpinThreshold: 1
  Name: "Batch"
}
Executor {
  Type: IO
  Threads: 1
  Name: "IO"
}
Executor {
  Type: BASIC
  Threads: 3
  SpinThreshold: 10
  Name: "IC"
  TimePerMailboxMicroSecs: 100
}
SysExecutor: 0
UserExecutor: 1
IoExecutor: 3
BatchExecutor: 2
ServiceExecutor {
  ServiceName: "Interconnect"
  ExecutorId: 4
}
```

## On Static Nodes

Static nodes take actor system configuration from the file `/opt/ydb/cfg/config.yaml`.

After replacing the configuration, the node needs to be restarted.

## On Dynamic Nodes

Dynamic nodes take configuration from [CMS](cms.md). To change it, you can use the following command:

```proto
ConfigureRequest {
  Actions {
    AddConfigItem {
      ConfigItem {
        // UsageScope: { ... }
        Config {
          ActorSystemConfig {
            <actor system configuration>
          }
        }
        MergeStrategy: 3
      }
    }
  }
}

```bash
ydbd -s <endpoint> admin console execute --domain=<domain> --retry=10 actorsystem.txt
```