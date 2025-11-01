# YDB Topics

This directory contains the implementation of YDB topics.

* public - these are utilitarian methods and actors that are supposed to be worked with outside this directory.
* common - common utilitarian methods and actors that are a closed part of the implementation. Their use is acceptable in the implementation of tablets inside persqueue.
* pqrb is a tablet that controls the topic as a whole. Implements the following functions:
  * balancing of reading sessions (partitioning by readers);
  * aggregation of statistics for all partitions;
  * coordination of topic autopartitioning;
  * and others.
* pqtablet - a tablet responsible for the operation of one or more partitions (depending on the cluster configuration).
* dread_cache_service is a service for transmitting topic messages during direct reading.
