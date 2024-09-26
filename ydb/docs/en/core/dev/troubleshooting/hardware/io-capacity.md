# Disk space and I/O capacity

## Description

High read/write operations can overwhelm disk subsystems, causing latencies in data access. When the system cannot read or write data quickly enough, queries that require disk access will be delayed.

Lack of available disk space can prevent the database from storing new data, leading to write failures or the inability to create necessary temporary files. This can also cause slowdowns as the system attempts to manage space constraints.