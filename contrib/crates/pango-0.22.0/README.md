# Rust Pango bindings

__Rust__ bindings and wrappers for [Pango](https://docs.gtk.org/Pango/), part of [gtk-rs-core](https://github.com/gtk-rs/gtk-rs-core).

Pango __1.40__ is the lowest supported version for the underlying library.

## Minimum supported Rust version

Currently, the minimum supported Rust version is `1.83.0`.

## Documentation

 * [Rust API - Stable](https://gtk-rs.org/gtk-rs-core/stable/latest/docs/pango/)
 * [Rust API - Development](https://gtk-rs.org/gtk-rs-core/git/docs/pango)
 * [C API](https://developer.gnome.org/platform-overview/unstable/tech-pango.html.en)
 * [GTK Installation instructions](https://www.gtk.org/docs/installations/)

## Using

We recommend using [crates from crates.io](https://crates.io/keywords/gtk-rs),
as [demonstrated here](https://gtk-rs.org/#using).

If you want to track the bleeding edge, use the git dependency instead:

```toml
[dependencies]
pango = { git = "https://github.com/gtk-rs/gtk-rs-core.git", package = "pango" }
```

Avoid mixing versioned and git crates like this:

```toml
# This will not compile
[dependencies]
pango = "0.13"
pango = { git = "https://github.com/gtk-rs/gtk-rs-core.git", package = "pango" }
```

### See Also

 * [glib](https://crates.io/crates/glib)

## License

__pango__ is available under the MIT License, please refer to it.
