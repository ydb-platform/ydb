# Rust GDK-PixBuf bindings

__Rust__ bindings and wrappers for [GDK-Pixbuf](https://docs.gtk.org/gdk-pixbuf), part of [gtk-rs-core](https://github.com/gtk-rs/gtk-rs-core).

GDK-PixBuf __2.36.8__ is the lowest supported version for the underlying library.

## Minimum supported Rust version

Currently, the minimum supported Rust version is `1.83.0`.

## Documentation

 * [Rust API - Stable](https://gtk-rs.org/gtk-rs-core/stable/latest/docs/gdk_pixbuf/)
 * [Rust API - Development](https://gtk-rs.org/gtk-rs-core/git/docs/gdk_pixbuf)
 * [C API](https://developer.gnome.org/gdk-pixbuf/stable/)
 * [GTK Installation instructions](https://www.gtk.org/docs/installations/)

## Using

We recommend using [crates from crates.io](https://crates.io/keywords/gtk-rs),
as [demonstrated here](https://gtk-rs.org/#using).

If you want to track the bleeding edge, use the git dependency instead:

```toml
[dependencies]
gdk-pixbuf = { git = "https://github.com/gtk-rs/gtk-rs-core.git", package = "gdk-pixbuf" }
```

Avoid mixing versioned and git crates like this:

```toml
# This will not compile
[dependencies]
gdk-pixbuf = "0.13"
gdk-pixbuf = { git = "https://github.com/gtk-rs/gtk-rs-core.git", package = "gdk-pixbuf" }
```

### See Also

 * [glib](https://crates.io/crates/glib)
 * [gio](https://crates.io/crates/gio)

## License

__gdk-pixbuf__ is available under the MIT License, please refer to it.
