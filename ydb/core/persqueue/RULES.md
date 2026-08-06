# YDB Topics — shared rules

## Guidelines

* Large logic → separate actors/classes.
* `*_fwd.h` for forwards; `.pb.h` only when needed; config: [`public/config.h`](public/config.h).
* Implement methods in `.cpp`, not headers (except templates).

## Tests

Cover cases:
* small and large messages
* kafka batch message format with single and multiple messages
* holes in the offset (mirroring)

## Review

* Flag redundant or unreachable branches.
* [`agents/BACKWARD_COMPATIBILITY.md`](../../agents/BACKWARD_COMPATIBILITY.md)
* [`agents/NO_ABORT.md`](../../agents/NO_ABORT.md)

## Monorepo

[`agents/CODESTYLE.md`](../../agents/CODESTYLE.md) ·
[`agents/GUIDE.md`](../../agents/GUIDE.md) ·
[`agents/GTEST_PREFER.md`](../../agents/GTEST_PREFER.md)
