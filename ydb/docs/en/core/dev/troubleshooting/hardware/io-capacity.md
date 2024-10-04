# Disk space and I/O capacity

## Description

High read/write operations can overwhelm disk subsystems, leading to increased latencies in data access. When the system cannot read or write data quickly enough, queries that require disk access will be delayed.

A lack of available disk space can prevent the database from storing new data, resulting in write failures or the inability to create necessary temporary files. This can also cause slowdowns as the system tries to manage space constraints.