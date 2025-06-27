Etcd over YDB. Zero version.

Main restriction: Can work only in single instance mode.
To remove the restriction we have to use CDC for implement the watches instead of self-notifications.

And other todo's:
- Add merics.
- Add logging.
- Add retry policies.
- Implement compaction with control of a requested revision.
- Implement the watches for ranges. (Now the watches work only with a single key or a prefix.)
- Add unit tests for watches.
