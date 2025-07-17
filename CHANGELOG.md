## Unreleased

### Functionality

* 15570:Allow creation of views that use UDFs in their queries. [#15570](https://github.com/ydb-platform/ydb/pull/15570) ([Daniil Demin](https://github.com/jepett0))
* 19327:Enable kafka port in local-ydb docker container [#19327](https://github.com/ydb-platform/ydb/pull/19327) ([Timofey Koolin](https://github.com/rekby))

### Bug fixes

* 16649:Fixed an issue where distconf would issue a VERIFY error and crash when nodes are removed through the legacy Console configuration management interface. [#16649](https://github.com/ydb-platform/ydb/pull/16649) ([Alexander Rutkovsky](https://github.com/alexvru))
* 16635:Fixed an [error](https://github.com/ydb-platform/ydb/issues/15551) that caused the `RETURNING` clause to work incorrectly with `INSERT/UPSERT` operations. [#16635](https://github.com/ydb-platform/ydb/pull/16635) ([Vitalii Gridnev](https://github.com/gridnevvvit))
* 16420:Improved secondary index build reliability - the system now automatically retries on certain errors instead of aborting the process. [#16420](https://github.com/ydb-platform/ydb/pull/16420) ([azevaykin](https://github.com/azevaykin))
* 16269:Fixed the issue of the Drop Table operation hanging in the PQ tablet, especially during delays in the Interconnect operation. [#16269](https://github.com/ydb-platform/ydb/pull/16269) ([Alek5andr-Kotov](https://github.com/Alek5andr-Kotov))
* 16194:Fixed a verification failure that occurred during VDisk compaction. [#16194](https://github.com/ydb-platform/ydb/pull/16194) ([Alexander Rutkovsky](https://github.com/alexvru))
* 15515:Fixed a topic reading hang that occurred when at least one partition had no incoming data but was being read by multiple consumers. [#15515](https://github.com/ydb-platform/ydb/pull/15515) ([FloatingCrowbar](https://github.com/FloatingCrowbar))
* 18302:Optimized memory usage in transactions with a large number of participants by changing the storage and resending mechanism for TEvReadSet messages. [#18302](https://github.com/ydb-platform/ydb/pull/18302) ([Alek5andr-Kotov](https://github.com/Alek5andr-Kotov))
* 19013:Fixed low performance in stream lookup on many reads https://github.com/ydb-platform/ydb/issues/19010, added simple overload logic https://github.com/ydb-platform/ydb/issues/19011 [#19013](https://github.com/ydb-platform/ydb/pull/19013) ([Vitalii Gridnev](https://github.com/gridnevvvit))
* 18905:Fix segfault that could happen while retrying Whiteboard requests https://github.com/ydb-platform/ydb/issues/18145 [#18905](https://github.com/ydb-platform/ydb/pull/18905) ([vporyadke](https://github.com/vporyadke))
* 18899:Table auto partitioning: Fixed crash when selecting split key from access samples containing a mix of full key and key prefix operations (e.g. exact/range reads). [#18899](https://github.com/ydb-platform/ydb/pull/18899) ([Ilnaz Nizametdinov](https://github.com/CyberROFL))
* 18717:Fixed an issue in the read session balancing of the pqv0 protocol that occurred when storage nodes of version 24.4 and dynamic nodes of version 25.1. [#18717](https://github.com/ydb-platform/ydb/pull/18717) ([Nikolay Shestakov](https://github.com/nshestakov))
* 18625:Fixed [an issue](https://github.com/ydb-platform/ydb/issues/18576) that node memory usage was not tracked [#18625](https://github.com/ydb-platform/ydb/pull/18625) ([vporyadke](https://github.com/vporyadke))
* 18621:[Fixed]((https://github.com/ydb-platform/ydb/pull/18621)) a [bug](https://github.com/ydb-platform/ydb/issues/16462) that caused tablet downloads to pause due to tablets that could not allowed to be launched. ([vporyadke](https://github.com/vporyadke))
* 18614:[Fixed](https://github.com/ydb-platform/ydb/pull/18614) a rare [issue](https://github.com/ydb-platform/ydb/issues/18615) of tablet restart due to the difference between `StartOffset` and the offset of the first blob. ([Alek5andr-Kotov](https://github.com/Alek5andr-Kotov))
* 18577:[Fixed](https://github.com/ydb-platform/ydb/pull/19152) typing [errors](https://github.com/ydb-platform/ydb/issues/18487) in UDF. ([Filitov Mikhail](https://github.com/lll-phill-lll))
* 18507:[Fixed]((https://github.com/ydb-platform/ydb/pull/18507)) issues with processing [optional columns](https://github.com/ydb-platform/ydb/issues/15701) and [UUID columns](https://github.com/ydb-platform/ydb/issues/15697) in row tables. ([Ivan Nikolaev](https://github.com/lex007in))
* 18378:[Fixed](https://github.com/ydb-platform/ydb/pull/18378) [a bug](https://github.com/ydb-platform/ydb/issues/16000) for handling per-dc followers created on older YDB versions. ([vporyadke](https://github.com/vporyadke))
* 17629:Fixed early object deletion in wide combiner. [#17629](https://github.com/ydb-platform/ydb/pull/17629) ([Filitov Mikhail](https://github.com/lll-phill-lll))
* 18077:The metric value is reset to zero when the `TEvPQ::TEvPartitionCounters` event arrives. Added a re-calculation of the values. [#18077](https://github.com/ydb-platform/ydb/pull/18077) ([Alek5andr-Kotov](https://github.com/Alek5andr-Kotov))
* 19057:Fixed "Failed to set up listener on port 9092 errno# 98 (Address already in use)" [#19057](https://github.com/ydb-platform/ydb/pull/19057) ([Nikolay Shestakov](https://github.com/nshestakov))
* 19115:Fixed an [issue](https://github.com/ydb-platform/ydb/issues/19083) in stream lookup join [#19115](https://github.com/ydb-platform/ydb/pull/19115) ([Vitalii Gridnev](https://github.com/gridnevvvit))
* 20240:If the CDC stream was recorded in an auto-partitioned topic, then it could stop after several splits of the topic. In this case, modification of rows in the table would result in the error that the table is overloaded. [#20240](https://github.com/ydb-platform/ydb/pull/20240) ([Nikolay Shestakov](https://github.com/nshestakov))
* 20072:[Fixed](https://github.com/ydb-platform/ydb/pull/20072) rare freezes of the topic table transaction. [#20072]([Alek5andr-Kotov](https://github.com/Alek5andr-Kotov))
* 19907:When transaction duration exceeds the topic's message retention period, writing to the topic may result in inconsistent data in the partition. [#19907](https://github.com/ydb-platform/ydb/pull/19907) ([Nikolay Shestakov](https://github.com/nshestakov))

