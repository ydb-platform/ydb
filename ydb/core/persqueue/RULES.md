# YDB Topics — shared rules

## Guidelines

* Large logic → separate actors/classes.
* `*_fwd.h` for forwards; `.pb.h` only when needed; config: [`public/config.h`](public/config.h).
* Implement methods in `.cpp`, not headers (except templates).

## Tests

When changing topic read/write/offset/blob logic, cover:

* **Small and large messages.** Large = >512 KiB (split into parts).
* **Large-message layout:** all parts in one blob; parts across different blobs.
* **Formats:** at least native and Kafka (single and multi-message batch).
* **Offset gaps:** missing offsets / holes (e.g. mirroring).

## Review

* Flag missing coverage for the cases above when the change touches message/blob/offset handling.
* Flag redundant or unreachable branches.
* [`agents/BACKWARD_COMPATIBILITY.md`](../../agents/BACKWARD_COMPATIBILITY.md)
* [`agents/NO_ABORT.md`](../../agents/NO_ABORT.md)

## Monorepo

[`agents/CODESTYLE.md`](../../agents/CODESTYLE.md) ·
[`agents/GUIDE.md`](../../agents/GUIDE.md) ·
[`agents/GTEST_PREFER.md`](../../agents/GTEST_PREFER.md)
