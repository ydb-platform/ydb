# YDB Topics — shared rules

## Guidelines

* Extract large logic into separate actors or classes.
* `*_fwd.h` for forward declarations; `.pb.h` only when needed.
  Config helpers: [`public/config.h`](public/config.h).
* Put method implementations in `.cpp` files, not in headers — except
  template functions and template classes.

## Monorepo

[`agents/CODESTYLE.md`](../../agents/CODESTYLE.md) ·
[`agents/GUIDE.md`](../../agents/GUIDE.md) ·
[`agents/GTEST_PREFER.md`](../../agents/GTEST_PREFER.md) ·
[`agents/NO_ABORT.md`](../../agents/NO_ABORT.md)
