# NYT::NProfiling

`yt/yt/library/profiling` is a monitoring library.

Main features:
- Local calculation of aggregates makes it easy to work with multidimensional counters.
- Lock-free registration of counters allows new counters to be created during process execution without worrying about unnecessary locks.
- Internal buffer of points allows saving high-resolution values. YTsaurus uses a grid step of 5 seconds, and the metric collection interval is usually 30 seconds.
- Sparse counters and deregistration of old counters allow tags with high cardinality, such as `user` or `table_path`, to be used.
- Optional global registry lets you add counters in code without necessity to pass parameters through constructors.
- Separation of interface and implementation allows the interface to be used in other core libraries, preventing dependency hell.
- Built-in debug page for troubleshooting sensor overflow and monitoring metric collection.
- Can mark sensors for which it makes sense to compute an aggregate on the Monitoring side. This option prevents "sum of maximums across the cluster" graphs from appearing.

## Comparison with monlib

|                           | YTsaurus  | monlib |
| ------------------------- | --------- | ------ |
| Spack                     | Yes       | Yes    |
| Debug Page                | Yes       | Yes    |
| 5s Grid Step              | Yes       | No     |
| Local Projections         | Yes       | No     |
| Sparse Sensors            | Yes       | No     |
| Sensor Deregistration     | Yes       | No     |
| MemOnly Metrics           | Yes       | Yes    |

## Separation of interface and implementation

`yt/yt/library/profiling` contains a minimal interface for adding sensors in code. Other `core` libraries depend on this library.

The interface implementation is in `yt/yt/library/profiling/solomon`. The interface finds the implementation via the weak symbol `GetGlobalRegistry`. If implementation is not linked, the library becomes a no-op.

## Sparse sensors

By default, all sensors are dense. The counter value will be exported to Solomon even if the sensor hasn't changed or is zero. This is true even for sensors created via `ISensorProducer`.

In some cases, this creates excessive load on Solomon. For example, among `request_count` sensors of users, only a few hundred out of ten thousand are nonzero at any given time. In such cases, the sensor can be switched to sparse mode.

Sparse sensors are not exported while their value is zero. After the sensor value becomes nonzero, the sensor is exported for LingerTimeout (default: 5 minutes). This reduces holes in the graphs.

## Global sensors

By default, a sensor belongs to the current process. When exported, it gets a `host=` tag or another process identifier.

A sensor can be made "global." In this case, process tags are not added during export, but the client must guarantee that such a sensor is active only on one process in the cluster at a time.

## Tags and projections

Before exporting sensors to Solomon, YT computes local aggregates.

All sensors with the same name should be annotated with tags having the same keys. It's possible to break this rule, but it's highly discouraged.

```
# Incorrect. Same name, different tag keys.
request_count{user=foo} 1
request_count{command=get} 1

# Correct
request_count_by_user{user=foo} 1
request_count_by_command{command=get} 1

# Also correct
request_count{user=foo;command=get} 1
```

By default, aggregation is performed over all subsets of the set of keys. Only aggregates are exported, not the raw sensors.

In some cases, this leads to combinatorial explosion or useless computation.

For example:

```
# Original sensor
request_count{bundle=sys;table_path=//sys/operations}

# The value of the aggregate with tag table_path will always equal
# the original sensor. This projection doubles Solomon load with no added value.
request_count{table_path=//sys/operations}

# The aggregate across all tables in the bundle is very useful and must be kept.
request_count{bundle=sys}
```

To avoid this problem, some sets of keys can be excluded from aggregation.

- The `Required` tag must be present in the set of keys for aggregation.
- The `Excluded` tag excludes all key sets where it appears.
- The `Alternative(other)` tag excludes all subsets where both the tag and its alternative are present.
- A tag with a defined `Parent` excludes all subsets containing the tag but not its parent.

Example: Suppose we have a counter with tags `a` and `b`.
The table shows which aggregate subsets are exported depending on tag settings.

| Aggregate       | A is required | B is excluded | A is parent of B | A is alternative to B |
|-----------------|---------------|---------------|------------------|-----------------------|
| `{}`            | -             | +             | +                | +                     |
| `{a=foo}`       | +             | +             | +                | +                     |
| `{b=bar}`       | -             | -             | -                | +                     |
| `{a=foo;b=bar}` | +             | -             | +                | -                     |

## Monitoring aggregates

Current Monitoring aggregates can compute metric sums during writing.

Summing only makes sense for some sensor types. To avoid pointless graphs in Solomon, the library marks all values for which aggregates make sense with the tag `yt_aggr=1`.

In shard settings, there must be a single rule enabled: `host=*, yt_aggr=1 -> host=Aggr, yt_aggr=1`.
