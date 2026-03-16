Compiling librsvg
=================

**For the impatient:**

First, install librsvg's dependencies; see ["Installing dependencies for
building"](#installing-dependencies-for-building) below for
instructions for various operating systems.

For everyday hacking, librsvg looks like a regular Rust project.  You
can `cargo build` to compile the main user-visible artifact for the
`rsvg-convert` program:

* `cargo build --release` - will compile `rsvg-convert` and put it in
  `./target/release/rsvg-convert`.  You can use that binary directly.
  If you need a debug build, use `cargo build --debug` instead; note
  that debug binaries run *much* slower!

* `cargo test` - this runs almost all of librsvg's test suite, which
  includes the Rust API and `rsvg-convert`, and is enough for regular
  hacking.  The full test suite includes tests for the C API; see the
  section "Compiling the C API" below.

* `cargo doc` - will produce the documentation for the Rust crate.

# Installing dependencies for building

To compile librsvg, you need the following packages installed.  The
minimum version is listed here; you may use a newer version instead.

**Compilers:**

* a C compiler and `make` tool; we recommend GNU `make`.
* rust 1.56 or later
* cargo

**Mandatory dependencies:**

* Cairo 1.16.0 with PNG support
* Freetype2 2.8.0
* Gdk-pixbuf 2.20.0
* GIO 2.24.0
* GObject-Introspection 0.10.8
* gi-docgen
* python3-docutils
* Libxml2 2.9.0
* Pango 1.46.0

The following sections describe how to install these dependencies on
several systems.

### Debian based systems

As of 2018/Feb/22, librsvg cannot be built in `debian stable` and
`ubuntu 18.04`, as they have packages that are too old.

**Build dependencies on Debian Testing or Ubuntu 18.10:**

```sh
apt-get install -y gcc make rustc cargo \
automake autoconf libtool gi-docgen python3-docutils git \
libgdk-pixbuf2.0-dev libgirepository1.0-dev \
libxml2-dev libcairo2-dev libpango1.0-dev
```

Additionally, as of September 2018 you need to add `gdk-pixbuf` utilities to your path, see #331 for more.

```sh
PATH="$PATH:/usr/lib/x86_64-linux-gnu/gdk-pixbuf-2.0"
```

### Fedora based systems

```sh
dnf install -y gcc rust rust-std-static cargo make \
automake autoconf libtool gi-docgen python3-docutils git redhat-rpm-config \
gdk-pixbuf2-devel gobject-introspection-devel \
libxml2-devel cairo-devel cairo-gobject-devel pango-devel
```

### openSUSE based systems

```sh
zypper install -y gcc rust rust-std cargo make \
automake autoconf libtool python3-gi-docgen python38-docutils git \
gdk-pixbuf-devel gobject-introspection-devel \
libxml2-devel cairo-devel pango-devel
```

### macOS systems

Dependencies may be installed using [Homebrew](https://brew.sh) or another
package manager.

```sh
brew install automake gi-docgen pkgconfig libtool gobject-introspection gdk-pixbuf pango
```

# Detailed compilation instructions

A full build of librsvg requires [autotools].  A full build will produce these:

* `rsvg-convert` binary.
* librsvg shared library with the GObject-based API.
* Gdk-pixbuf loader for SVG files.
* HTML documentation for the GObject-based API, with `gi-docgen`.
* GObject-introspection information for language bindings.
* Vala language bindings.

Librsvg uses a mostly normal [autotools] setup.  The details of how
librsvg integrates Cargo and Rust into its autotools setup are
described in [this blog post][blog], although hopefully you will not
need to refer to it.

It is perfectly fine to [ask the maintainer][maintainer] if you have
questions about the Autotools setup; it's a tricky bit of machinery,
and we are glad to help.

There are generic compilation/installation instructions in the
[`INSTALL`][install] file, which comes from Autotools.  The following
explains librsvg's peculiarities.

* [Basic compilation instructions](#basic-compilation-instructions)
* [Verbosity](#verbosity)
* [Debug or release builds](#debug-or-release-builds)
* [Selecting a Rust toolchain](#selecting-a-rust-toolchain)
* [Cross-compilation](#cross-compilation)
* [Building with no network access](#building-with-no-network-access)
* [Running `make distcheck`](#running-make-distcheck)

# Basic compilation instructions

If you are compiling a tarball:

```sh
./configure --enable-vala
make
make install
```

See the [`INSTALL`][install] file for details on options you can pass
to the `configure` script to select where to install the compiled
library.

If you are compiling from a git checkout:

```sh
./autogen.sh --enable-vala
make
make install
```

The `--enable-vala` argument is optional.  It will check that you have
the Vala compiler installed.

# Verbosity

By default the compilation process is quiet, and it just tells you
which files it is compiling.

If you wish to see the full compilation command lines, use "`make V=1`"
instead of plain "`make`".

# Debug or release builds

Librsvg's artifacts have code both in C and Rust, and each language
has a different way of specifying compilation options to select
compiler optimizations, or whether debug information should be
included.

* **Rust code:** the librsvg shared library, and the `rsvg-convert`
  binary.  See below.

* **C code:** the gdk-pixbuf loader; you should set the `CFLAGS`
  environment variable with compiler flags that you want to pass to
  the C compiler.

## Controlling debug or release mode for Rust

* With a `configure` option: `--enable-debug` or `--disable-debug`
* With an environment variable: `LIBRSVG_DEBUG=yes` or `LIBRSVG_DEBUG=no`

For the Rust part of librsvg, we have a flag that
you can pass at `configure` time.  When enabled, the Rust
sub-library will have debugging information and no compiler
optimizations.  **This flag is off by default:** if the flag is not
specified, the Rust sub-library will be built in release mode (no
debug information, full compiler optimizations).

The rationale is that people who already had scripts in place to build
binary packages for librsvg, generally from release tarballs, are
already using conventional machinery to specify C compiler options,
such as that in RPM specfiles or Debian source packages.  However,
they may not contemplate Rust libraries and they will certainly
not want to modify their existing packaging scripts too much.

So, by default, the Rust library builds in **release mode**, to make
life easier to binary distributions.  Librsvg's build scripts will add
`--release` to the Cargo command line by default.

Developers can request a debug build of the Rust code by
passing `--enable-debug` to the `configure` script, or by setting the
`LIBRSVG_DEBUG=yes` environment variable before calling `configure`.
This will omit the `--release` option from Cargo, so that it will
build the Rust sub-library in debug mode.

In case both the environment variable and the command-line option are
specified, the command-line option overrides the env var.

# Selecting a Rust toolchain

By default, the configure/make steps will use the `cargo` binary that
is found in your `$PATH`.  If you have a system installation of Rust
and one in your home directory, or for special build systems, you may
need to override the locations of `cargo` and/or `rustc`.  In this
case, you can set any of these environment variables before running
`configure` or `autogen.sh`:

* `RUSTC` - path to the `rustc` compiler
* `CARGO` - path to `cargo`

Note that `$RUSTC` only gets used in the `configure` script to ensure
that there is a Rust compiler installed with an appropriate version.
The actual compilation process just uses `$CARGO`, and assumes that
that `cargo` binary will use the same Rust compiler as the other
variable.

# Cross-compilation

If you need to cross-compile librsvg, specify the `--host=TRIPLE` to
the `configure` script as usual with Autotools.  This will cause
librsvg's build scripts to automatically pass `--target=TRIPLE` to
`cargo`.

Note, however, that Rust may support different targets than the C
compiler on your system.  Rust's supported targets can be found in the
[rustc manual](https://doc.rust-lang.org/nightly/rustc/platform-support.html)

You can check Jorge Aparicio's [guide on cross-compilation for
Rust][rust-cross] for more details.

## Overriding the Rust target name

If you need `cargo --target=FOO` to obtain a different value from the
one you specified for `--host=TRIPLE`, you can use the `RUST_TARGET`
variable, and this will be passed to `cargo`.  For example,

```sh
RUST_TARGET=aarch64-unknown-linux-gnu ./configure --host=aarch64-buildroot-linux-gnu
# will run "cargo --target=aarch64-unknown-linux-gnu" for the Rust part
```

## Cross-compiling to a target not supported by Rust out of the box

When building with a target that is not supported out of the box by
Rust, you have to do this:

1. Create a [target JSON definition file][target-json].

2. Set the environment variable `RUST_TARGET_PATH` to its directory
   for the `make` command.

Example:

```sh
cd /my/target/definition
echo "JSON goes here" > MYMACHINE-VENDOR-OS.json
cd /source/tree/for/librsvg
./configure --host=MYMACHINE-VENDOR-OS
make RUST_TARGET_PATH=/my/target/definition
```

## Cross-compiling for win32 target

You can also cross-compile to win32 (Microsoft Windows) target by using
[MinGW-w64][mingw-w64]. You need to specify the appropriate target in the same way
as usual:

* Set an appropriate target via the `--host` configure option:
    * `i686-w64-mingw32` for 32-bit target
    * `x86_64-w64-mingw32` for 64-bit target
* Set an appropriate RUST_TARGET:
    * `i686-pc-windows-gnu` for 32-bit target
    * `x86_64-pc-windows-gnu` for 64-bit target

In addition you may need to link with some win32 specific libraries like
`LIBS="-lws2_32 -luserenv"`.

Example:

```sh
./configure \
  --host=x86_64-w64-mingw32 \
  RUST_TARGET=x86_64-pc-windows-gnu \
  LIBS="-lws2_32 -luserenv"
make
```

The most painful aspect of this way of building is preparing a win32
build for each of librsvg's dependencies. [MXE][mxe] may help you on
this work.



# Building with no network access

Automated build systems generally avoid network access so that they
can compile from known-good sources, instead of pulling random updates
from the net every time.  However, normally Cargo likes to download
dependencies when it first compiles a Rust project.

We use [`cargo vendor`][cargo-vendor] to ship librsvg release tarballs
with the source code for Rust dependencies **embedded within the
tarball**.  If you unpack a librsvg tarball, these sources will appear
in the `vendor/` subdirectory.  If you build librsvg from a
tarball, instead of git, it should not need to access the network to
download extra sources at all.

Build systems can use [Cargo's source replacement
mechanism][cargo-source-replacement] to override the location of the
source code for the Rust dependencies, for example, in order to patch
one of the Rust crates that librsvg uses internally.

The source replacement information is in `rust/.cargo/config` in the
unpacked tarball.  Your build system can patch this file as needed.

# Running `make distcheck`

The `make distcheck` command will built a release tarball, extract it,
compile it and test it.  However, part of the `make install` process
within that command will try to install the gdk-pixbuf loader in your
system location, and it will fail.

Please run `make distcheck` like this:

```
$ make distcheck DESTDIR=/tmp/foo
```

That `DESTDIR` will keep the gdk-pixbuf loader installation from
trying to modify your system locations.

[autotools]: https://autotools.io/index.html
[blog]: https://people.gnome.org/~federico/blog/librsvg-build-infrastructure.html
[maintainer]: README.md#maintainers
[install]: INSTALL
[cargo-vendor]: https://crates.io/crates/cargo-vendor
[cargo-source-replacement]: http://doc.crates.io/source-replacement.html
[rust-cross]: https://github.com/japaric/rust-cross
[target-json]: https://github.com/japaric/rust-cross#target-specification-files
[mingw-w64]: https://mingw-w64.org/
[mxe]: https://mxe.cc/
