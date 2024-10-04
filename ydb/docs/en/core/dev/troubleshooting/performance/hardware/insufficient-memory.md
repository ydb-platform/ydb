# Insufficient memory (RAM)

If support for paging of anonymous memory ([swap](https://www.kernel.org/doc/gorman/html/understand/understand014.html)) is enabled on the database server, insufficient memory can cause the database to rely heavily on disk I/O for operations, which is significantly slower than accessing data from memory. This can lead to increased latencies in query execution and data retrieval.

However, in production environments, swap is often disabled to trade performance issues for availability issues, which are easier to notice and mitigate.

If the swap is disabled, insufficient memory triggers another kernel feature called [OOM killer](https://en.wikipedia.org/wiki/Out_of_memory) that terminates the most memory-hungry processes (for servers running databases, it's often the database itself). This feature also interacts with [cgroups](https://en.wikipedia.org/wiki/Cgroups) if multiple are configured.

Which components inside the YDB process consume the memory might matter as well.

## Diagnostics

## Recommendation