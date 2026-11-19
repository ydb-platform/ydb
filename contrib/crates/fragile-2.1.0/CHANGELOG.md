# Changelog

All notable changes to similar are documented here.

## 2.1.0

* Implement `Future` and `Stream` for `Fragile`, `Sticky` and `SemiSticky`.
  [#38](https://github.com/mitsuhiko/fragile/pull/38), [#40](https://github.com/mitsuhiko/fragile/pull/40)
* Better panic error reporting by adding `#[track_caller]`.
* Fragile now internally uses the stdlib's thread IDs instead of its own counter.  [#39](https://github.com/mitsuhiko/fragile/pull/39)

## 2.0.1

* Fixed a soundness issue with `Sticky` if the `slab` variant was enabled.
  This caused a use after free if the type was freed in the wrong thread.
  [#37](https://github.com/mitsuhiko/fragile/pull/37)

## 2.0.0

* `Fragile` no longer boxes internally.
* `Sticky` and `SemiSticky` now require the use of stack tokens.
  For more information see [#26](https://github.com/mitsuhiko/fragile/issues/26)
* `Sticky` now tries to drop entries from the thread local registry eagerly
  if it's dropped on the right thread.

## 1.2.1

* Fixed non slab versions only allowing a single sticky.

## 1.2.0

Note on safety: the `Sticky` and `SemiSticky` types allow data to live
longer than the wrapper type which is why they are now requiring a `'static`
bound.  Previously it was possible to create a sticky containing a bare
reference which permitted unsafe access.

* `Sticky` now requires `'static`.
* Added the `slab` feature for an internal optimization for `Sticky` to use
  a slab instead of a `HashMap`.

## Older Releases

Older releases were yanked due to the insufficient trait bound on `Sticky`.
