# Cairo bindings

__Rust__ bindings for Rust and wrappers for [Cairo](https://www.cairographics.org/), part of [gtk-rs-core](https://github.com/gtk-rs/gtk-rs-core).

![screenshot](https://guillaume-gomez.fr/image/cairo.png)

Cairo __1.14__ is the lowest supported version for the underlying library.

## Minimum supported Rust version

Currently, the minimum supported Rust version is `1.83.0`.

## Default-on features

* **use_glib** - Use with [glib](mod@glib)

## Fileformat features

 * **png** - Reading and writing PNG images
 * **pdf** - Rendering PDF documents
 * **svg** - Rendering SVG documents
 * **ps** - Rendering PostScript documents

## Cairo API version features

 * **v1_16** - Use Cairo 1.16 APIs

## Documentation

 * [Rust API - Stable](https://gtk-rs.org/gtk-rs-core/stable/latest/docs/cairo)
 * [Rust API - Development](https://gtk-rs.org/gtk-rs-core/git/docs/cairo)
 * [C API](https://www.cairographics.org/documentation/)

## X Window features

 * **xcb** - X Window System rendering using the XCB library
 * **xlib** - X Window System rendering using XLib

## Windows API features

 * **win32-surface** - Microsoft Windows surface support

## Using

We recommend using [crates from crates.io](https://crates.io/keywords/gtk-rs),
as [demonstrated here](https://gtk-rs.org/#using).

If you want to track the bleeding edge, use the git dependency instead:

```toml
[dependencies]
cairo-rs = { git = "https://github.com/gtk-rs/gtk-rs-core.git", package = "cairo-rs" }
```

Avoid mixing versioned and git crates like this:

```toml
# This will not compile
[dependencies]
cairo-rs = "0.13"
cairo-rs = { git = "https://github.com/gtk-rs/gtk-rs-core.git", package = "cairo-rs" }
```

### See Also

 * [glib](https://crates.io/crates/glib)

## License

__cairo__ is available under the MIT License, please refer to it.
