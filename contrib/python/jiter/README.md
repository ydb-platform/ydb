# jiter

[![CI](https://github.com/pydantic/jiter/actions/workflows/ci.yml/badge.svg?event=push)](https://github.com/pydantic/jiter/actions/workflows/ci.yml?query=branch%3Amain)
[![Crates.io](https://img.shields.io/crates/v/jiter?color=green)](https://crates.io/crates/jiter)
[![CodSpeed Badge](https://img.shields.io/endpoint?url=https://codspeed.io/badge.json)](https://codspeed.io/pydantic/jiter)

Fast iterable JSON parser.

Documentation is available at [docs.rs/jiter](https://docs.rs/jiter).

jiter has three interfaces:
* `JsonValue` an enum representing JSON data
* `Jiter` an iterator over JSON data
* `PythonParse` which parses a JSON string into a Python object

## JsonValue Example

See [the `JsonValue` docs](https://docs.rs/jiter/latest/jiter/enum.JsonValue.html) for more details.

```rust
use jiter::JsonValue;

let json_data = r#"
    {
        "name": "John Doe",
        "age": 43,
        "phones": [
            "+44 1234567",
            "+44 2345678"
        ]
    }"#;
let json_value = JsonValue::parse(json_data.as_bytes(), true).unwrap();
println!("{:#?}", json_value);
```

returns:

```text
Object(
    {
        "name": Str("John Doe"),
        "age": Int(43),
        "phones": Array(
            [
                Str("+44 1234567"),
                Str("+44 2345678"),
            ],
        ),
    },
)
```

## Jiter Example

To use [Jiter](https://docs.rs/jiter/latest/jiter/struct.Jiter.html), you need to know what schema you're expecting:

```rust
use jiter::{Jiter, NumberInt, Peek};

let json_data = r#"
    {
        "name": "John Doe",
        "age": 43,
        "phones": [
            "+44 1234567",
            "+44 2345678"
        ]
    }"#;
let mut jiter = Jiter::new(json_data.as_bytes());
assert_eq!(jiter.next_object().unwrap(), Some("name"));
assert_eq!(jiter.next_str().unwrap(), "John Doe");
assert_eq!(jiter.next_key().unwrap(), Some("age"));
assert_eq!(jiter.next_int().unwrap(), NumberInt::Int(43));
assert_eq!(jiter.next_key().unwrap(), Some("phones"));
assert_eq!(jiter.next_array().unwrap(), Some(Peek::String));
// we know the next value is a string as we just asserted so
assert_eq!(jiter.known_str().unwrap(), "+44 1234567");
assert_eq!(jiter.array_step().unwrap(), Some(Peek::String));
// same again
assert_eq!(jiter.known_str().unwrap(), "+44 2345678");
// next we'll get `None` from `array_step` as the array is finished
assert_eq!(jiter.array_step().unwrap(), None);
// and `None` from `next_key` as the object is finished
assert_eq!(jiter.next_key().unwrap(), None);
// and we check there's nothing else in the input
jiter.finish().unwrap();
```

## Benchmarks

_There are lies, damned lies and benchmarks._

In particular, serde-json benchmarks use `serde_json::Value` which is significantly slower than deserializing
to a string.

For more details, see [the benchmarks](https://github.com/pydantic/jiter/tree/main/crates/jiter/benches).

```text
running 48 tests
test big_jiter_iter                    ... bench:   3,662,616 ns/iter (+/- 88,878)
test big_jiter_value                   ... bench:   6,998,605 ns/iter (+/- 292,383)
test big_serde_value                   ... bench:  29,793,191 ns/iter (+/- 576,173)
test bigints_array_jiter_iter          ... bench:      11,836 ns/iter (+/- 414)
test bigints_array_jiter_value         ... bench:      28,979 ns/iter (+/- 938)
test bigints_array_serde_value         ... bench:     129,797 ns/iter (+/- 5,096)
test floats_array_jiter_iter           ... bench:      19,302 ns/iter (+/- 631)
test floats_array_jiter_value          ... bench:      31,083 ns/iter (+/- 921)
test floats_array_serde_value          ... bench:     208,932 ns/iter (+/- 6,167)
test lazy_map_lookup_1_10              ... bench:         615 ns/iter (+/- 15)
test lazy_map_lookup_2_20              ... bench:       1,776 ns/iter (+/- 36)
test lazy_map_lookup_3_50              ... bench:       4,291 ns/iter (+/- 77)
test massive_ints_array_jiter_iter     ... bench:      62,244 ns/iter (+/- 1,616)
test massive_ints_array_jiter_value    ... bench:      82,889 ns/iter (+/- 1,916)
test massive_ints_array_serde_value    ... bench:     498,650 ns/iter (+/- 47,759)
test medium_response_jiter_iter        ... bench:           0 ns/iter (+/- 0)
test medium_response_jiter_value       ... bench:       3,521 ns/iter (+/- 101)
test medium_response_jiter_value_owned ... bench:       6,088 ns/iter (+/- 180)
test medium_response_serde_value       ... bench:       9,383 ns/iter (+/- 342)
test pass1_jiter_iter                  ... bench:           0 ns/iter (+/- 0)
test pass1_jiter_value                 ... bench:       3,048 ns/iter (+/- 79)
test pass1_serde_value                 ... bench:       6,588 ns/iter (+/- 232)
test pass2_jiter_iter                  ... bench:         384 ns/iter (+/- 9)
test pass2_jiter_value                 ... bench:       1,259 ns/iter (+/- 44)
test pass2_serde_value                 ... bench:       1,237 ns/iter (+/- 38)
test sentence_jiter_iter               ... bench:         283 ns/iter (+/- 10)
test sentence_jiter_value              ... bench:         357 ns/iter (+/- 15)
test sentence_serde_value              ... bench:         428 ns/iter (+/- 9)
test short_numbers_jiter_iter          ... bench:           0 ns/iter (+/- 0)
test short_numbers_jiter_value         ... bench:      18,085 ns/iter (+/- 613)
test short_numbers_serde_value         ... bench:      87,253 ns/iter (+/- 1,506)
test string_array_jiter_iter           ... bench:         615 ns/iter (+/- 18)
test string_array_jiter_value          ... bench:       1,410 ns/iter (+/- 44)
test string_array_jiter_value_owned    ... bench:       2,863 ns/iter (+/- 151)
test string_array_serde_value          ... bench:       3,467 ns/iter (+/- 60)
test true_array_jiter_iter             ... bench:         299 ns/iter (+/- 8)
test true_array_jiter_value            ... bench:         995 ns/iter (+/- 29)
test true_array_serde_value            ... bench:       1,207 ns/iter (+/- 36)
test true_object_jiter_iter            ... bench:       2,482 ns/iter (+/- 84)
test true_object_jiter_value           ... bench:       2,058 ns/iter (+/- 45)
test true_object_serde_value           ... bench:       7,991 ns/iter (+/- 370)
test unicode_jiter_iter                ... bench:         315 ns/iter (+/- 7)
test unicode_jiter_value               ... bench:         389 ns/iter (+/- 6)
test unicode_serde_value               ... bench:         445 ns/iter (+/- 6)
test x100_jiter_iter                   ... bench:          12 ns/iter (+/- 0)
test x100_jiter_value                  ... bench:          20 ns/iter (+/- 1)
test x100_serde_iter                   ... bench:          72 ns/iter (+/- 3)
test x100_serde_value                  ... bench:          83 ns/iter (+/- 3)
```
