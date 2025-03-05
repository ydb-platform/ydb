## Unreleased

### Functionality
* 15186:Increase query text limit size in query stats system views. [#15186](https://github.com/ydb-platform/ydb/pull/15186) ([spuchin](https://github.com/spuchin))
* 15157:Switch to library/cpp/charset/lite to avoid unneded dependency on libiconv [#15157](https://github.com/ydb-platform/ydb/pull/15157) ([Andrey Neporada](https://github.com/nepal))

### Bug fixes
* 15160:If the user performs a control plane operation with a topic (for example, add partitions or a consumer) and at an unsuccessful moment the PQ tablet moves to another node, the transaction may hang. To the user, it will look like "the operation is frozen".. [#15070](https://github.com/ydb-platform/ydb/issues/15070) [#15160](https://github.com/ydb-platform/ydb/pull/15160) ([Alek5andr-Kotov](https://github.com/Alek5andr-Kotov))

