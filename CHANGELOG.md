## Unreleased

### Functionality
* 15186:Increase query text limit size in query stats system views. [#15186](https://github.com/ydb-platform/ydb/pull/15186) ([spuchin](https://github.com/spuchin))
* 15157:Switch to library/cpp/charset/lite to avoid unneded dependency on libiconv [#15157](https://github.com/ydb-platform/ydb/pull/15157) ([Andrey Neporada](https://github.com/nepal))

### Bug fixes
* 15160:The partition actor puts messages in the queue until it enters the `StateIdle` state. Then he sends himself these messages again. There may be a situation when new messages will be processed before the accumulated ones. [#15070](https://github.com/ydb-platform/ydb/issues/15070) [#15160](https://github.com/ydb-platform/ydb/pull/15160) ([Alek5andr-Kotov](https://github.com/Alek5andr-Kotov))

