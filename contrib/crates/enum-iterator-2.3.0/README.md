<!-- cargo-sync-readme start -->

# Overview
- [📦 crates.io](https://crates.io/crates/enum-iterator)
- [📖 Documentation](https://docs.rs/enum-iterator)
- [⚖ 0BSD license](https://spdx.org/licenses/0BSD.html)

Tools to iterate over the values of a type.

# Examples
```rust
use enum_iterator::{all, cardinality, first, last, next, previous, reverse_all, Sequence};

#[derive(Debug, PartialEq, Sequence)]
enum Day { Monday, Tuesday, Wednesday, Thursday, Friday, Saturday, Sunday }

assert_eq!(cardinality::<Day>(), 7);
assert_eq!(all::<Day>().collect::<Vec<_>>(), [
    Day::Monday,
    Day::Tuesday,
    Day::Wednesday,
    Day::Thursday,
    Day::Friday,
    Day::Saturday,
    Day::Sunday,
]);
assert_eq!(first::<Day>(), Some(Day::Monday));
assert_eq!(last::<Day>(), Some(Day::Sunday));
assert_eq!(next(&Day::Tuesday), Some(Day::Wednesday));
assert_eq!(previous(&Day::Wednesday), Some(Day::Tuesday));
assert_eq!(reverse_all::<Day>().collect::<Vec<_>>(), [
    Day::Sunday,
    Day::Saturday,
    Day::Friday,
    Day::Thursday,
    Day::Wednesday,
    Day::Tuesday,
    Day::Monday,
]);
```

```rust
use enum_iterator::{cardinality, first, last, Sequence};

#[derive(Debug, PartialEq, Sequence)]
struct Foo {
    a: bool,
    b: u8,
}

assert_eq!(cardinality::<Foo>(), 512);
assert_eq!(first::<Foo>(), Some(Foo { a: false, b: 0 }));
assert_eq!(last::<Foo>(), Some(Foo { a: true, b: 255 }));
```

# Rust version
This crate tracks stable Rust. Minor releases may require a newer Rust version. Patch releases
must not require a newer Rust version.

# Contribute
All contributions shall be licensed under the [0BSD license](https://spdx.org/licenses/0BSD.html).

<!-- cargo-sync-readme end -->
