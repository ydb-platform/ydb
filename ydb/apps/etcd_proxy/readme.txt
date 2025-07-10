Etcd over YDB. Zero version.

TODO's:
- Add metrics.
- Add logging.
- Add retry policies.
- Implement compaction trought many stages and with control of a requested revision.
- Implement the watches for ranges. (Now the watches work only with a single key or a prefix.)
- Add unit tests for watches.
- Support many partitions in the cahangefeed. (Now supported only single partition.)
- Support iam autorization.
- Optimize some requests (check leases as example) to reduce count of TLI errors.
