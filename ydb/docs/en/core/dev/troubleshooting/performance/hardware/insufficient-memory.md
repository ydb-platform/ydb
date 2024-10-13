# Insufficient memory (RAM)

If [swap](https://www.kernel.org/doc/gorman/html/understand/understand014.html) (paging of anonymous memory) is disabled on the server running {{ ydb-short-name }}, insufficient memory activates another kernel feature called the [OOM killer](https://en.wikipedia.org/wiki/Out_of_memory), which terminates the most memory-intensive processes (often the database itself). This feature also interacts with [cgroups](https://en.wikipedia.org/wiki/Cgroups) if multiple cgroups are configured.

If swap is enabled, insufficient memory may cause the database to rely heavily on disk I/O, which is significantly slower than accessing data directly from memory. This can result in increased latencies during query execution and data retrieval.

Additionally, which components within the  {{ ydb-short-name }} process consume memory may also be significant.

## Diagnostics

## Recommendation