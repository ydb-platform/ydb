[![Crates.io](https://img.shields.io/crates/v/pulldown-cmark-to-cmark)](https://crates.io/crates/pulldown-cmark-to-cmark)
![Rust](https://github.com/Byron/pulldown-cmark-to-cmark/workflows/Rust/badge.svg)

A utility library which translates [`Event`][pdcm-event] back to markdown.
It's the prerequisite for writing markdown filters which can work as
[mdbook-preprocessors][mdbook-prep].

This library takes great pride in supporting **everything that `pulldown-cmark`** supports,
including *tables* and *footnotes* and *codeblocks in codeblocks*,
while assuring *quality* with a powerful test suite.

[pdcm-event]: https://docs.rs/pulldown-cmark/latest/pulldown_cmark/enum.Event.html
[mdbook-prep]: https://rust-lang.github.io/mdBook/for_developers/preprocessors.html

### How to use

Please have a look at the [`stupicat`-example][sc-example] for a complete tour
of the API, or have a look at the [api-docs][api].

It's easiest to get this library into your `Cargo.toml` using `cargo-add`:
```
cargo add pulldown-cmark-to-cmark
```

[sc-example]: https://github.com/Byron/pulldown-cmark-to-cmark/blob/76667725b61be24890fbdfed5e7ecdb4c1ad1dc8/examples/stupicat.rs#L21
[api]: https://docs.rs/crate/pulldown-cmark-to-cmark

### Supported Rust Versions

`pulldown-cmark-to-cmark` follows the MSRV (minimum supported rust version) policy of [`pulldown-cmark`]. The current MSRV is 1.71.1.

[`pulldown-cmark`]: https://github.com/pulldown-cmark/pulldown-cmark

### Friends of this project

 * [**termbook**](https://github.com/Byron/termbook)
   * A runner for `mdbooks` to keep your documentation tested.  
 * [**Share Secrets Safely**](https://github.com/Byron/share-secrets-safely)
   * share secrets within teams to avoid plain-text secrets from day one 

### Maintenance Guide

#### Making a new release

 * **Assure all documentation is up-to-date and tests are green**
 * update the `version` in `Cargo.toml` and `git commit`
 * run `cargo release --no-dev-version`
