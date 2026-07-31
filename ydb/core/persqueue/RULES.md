# YDB Topics — shared rules

Applies to Topics-owned trees (see `.github/CODEOWNERS` `@ydb-platform/Topics`):
`persqueue`, `library/persqueue`, `persqueue_v1`, `persqueue_v0`,
`kafka_proxy`, `http_proxy`, `datastreams`, `sqs_topic`, `transfer`.

## Layering

* External / protocol code uses `persqueue/public/` (and `events/` where
  needed), not `pqtablet/` / `pqrb/` internals. Layering: `public` → tablets.

## Guidelines

* In `pqrb` / `pqtablet`: batch when persisting; minimize persists and
  inter-actor messages; extract large logic into separate actors or classes.
* `*_fwd.h` for forward declarations; `.pb.h` only when needed.
  Config helpers: [`public/config.h`](public/config.h).
* Put method implementations in `.cpp` files, not in headers — except
  template functions and template classes.

## Monorepo

[`agents/CODESTYLE.md`](../../agents/CODESTYLE.md) ·
[`agents/GUIDE.md`](../../agents/GUIDE.md) ·
[`agents/GTEST_PREFER.md`](../../agents/GTEST_PREFER.md) ·
[`agents/NO_ABORT.md`](../../agents/NO_ABORT.md)
