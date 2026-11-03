# valuable-serde

[Valuable][`valuable`] provides object-safe value inspection. Use cases include passing
structured data to trait objects and object-safe serialization.

This crate provides a bridge between [`valuable`] and the [`serde`]
serialization ecosystem. Using [`valuable_serde::Serializable`] allows any type
that implements `valuable`'s [`Valuable`] trait to be serialized by any
[`serde::ser::Serializer`].

[`valuable`]: https://crates.io/crates/valuable
[`serde`]: https://crates.io/crates/serde
[`valuable_serde::Serializable`]: https://docs.rs/valuable-serde/latest/valuable_serde/struct.Serializable.html
[`Valuable`]: https://docs.rs/valuable/latest/valuable/trait.Valuable.html
[`serde::ser::Serializer`]:  https://docs.rs/serde/latest/serde/ser/trait.Serializer.html

## License

This project is licensed under the [MIT license](LICENSE).

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in Valuable by you, shall be licensed as MIT, without any additional
terms or conditions.