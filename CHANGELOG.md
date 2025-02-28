## Unreleased

### Bug fixes
* 15160:The partition actor puts messages in the queue until it enters the `StateIdle` state. Then he sends himself these messages again. There may be a situation when new messages will be processed before the accumulated ones.

#15070 [#15160](https://github.com/ydb-platform/ydb/pull/15160) ([None](https://github.com/Alek5andr-Kotov))

### Functionality
* 15143:add kind options to storage pools for create specific set of pdisks


... [#15143](https://github.com/ydb-platform/ydb/pull/15143) ([Sergey J](https://github.com/sourcecd))

