# Release process checklist for librsvg

Feel free to print this document or copy it to a text editor to check
off items while making a release.

- [ ] Refresh your memory with https://wiki.gnome.org/MaintainersCorner/Releasing
- [ ] Increase the package version number in `configure.ac` (it may
      already be increased but not released; double-check it).
- [ ] Copy version number to `Cargo.toml`.
- [ ] Copy version number to `doc/librsvg.toml`.
- [ ] `cargo update` - needed because you tweaked `Cargo.toml`, and
      also to get new dependencies.
- [ ] Tweak the library version number in `configure.ac` if the API changed; follow the steps there.
- [ ] Update `NEWS`, see below for the preferred format.
- [ ] Commit the changes above.
- [ ] Make a tarball with `./autogen.sh --enable-vala && make distcheck DESTDIR=/tmp/foo` - fix things until it passes.
- [ ] Create a signed tag - `git tag -s x.y.z` with the version number.
- [ ] `git push` to the appropriate branch to gitlab.gnome.org/GNOME/librsvg
- [ ] `git push` the signed tag to gitlab.gnome.org/GNOME/librsvg
- [ ] `scp librsvg-x.y.z.tar.xz master.gnome.org:`
- [ ] `ssh master.gnome.org` and then `ftpadmin install librsvg-x.y.z.tar.xz`
- [ ] Create a [release in Gitlab](#gitlab-release).
      
For `x.y.0` releases, at least, do the following:

- [ ] [Notify the release team][release-team] on whether to use this
      `librsvg-x.y.0` for the next GNOME version via an issue on their
      `GNOME/releng` project.
      
- [ ] `cargo-audit audit` and ensure we don't have vulnerable dependencies.

## Gitlab release

- [ ] Go to https://gitlab.gnome.org/GNOME/librsvg/-/releases and click the **New release** button.

- [ ] Select the tag `x.y.z` you created as part of the release steps.

- [ ] If there is an associated milestone, select it too.

- [ ] Fill in the release title - `x.y.z - stable` or `x.y.z - development`.

- [ ] Copy the release notes from NEWS.

- [ ] Add a release asset link to
      `https://download.gnome.org/sources/librsvg/x.y/librsvg-x.y.z.tar.xz`
      and call it `librsvg-x.y.z.tar.xz - release tarball`.

- [ ] Add a release asset link to
      `https://download.gnome.org/sources/librsvg/x.y/librsvg-x.y.z.sha256sum`
      and call it `librsvg-x.y.z.sha256sum - release tarball
      sha256sum`.

## Version numbers

`configure.ac` and `Cargo.toml` must have the same **package version**
number - this is the number that users of the library see.

`configure.ac` is where the **library version** is defined; this is
what gets encoded in the SONAME of `librsvg.so`.

Librsvg follows an even/odd numbering scheme for the **package
version**.  For example, the 2.50.x series is for stable releases, and
2.51.x is for unstable/development ones.  The [release-team] needs to
be notified when a new series comes about, so they can adjust their
tooling for the stable or development GNOME releases.  File an issue
in their [repository][release-team] to indicate whether the new
`librsvg-x.y.0` is a stable or development series.

## Minimum supported Rust version (MSRV)

While it may seem desirable to always require the latest released
version of the Rust toolchain, to get new language features and such,
this is really inconvenient for distributors of librsvg which do not
update Rust all the time.  So, we make a compromise.

The `configure.ac` script defines `MININUM_RUST_MAJOR` and
`MINIMUM_RUST_MINOR` variables with librsvg's minimum supported Rust
version (MSRV).  These ensure that distros will get an early failure during a
build, at the `configure` step, if they have a version of Rust that is
too old — instead of getting an obscure error message from `rustc` in
the middle of the build when it finds an unsupported language
construct.

As of March 2021, Cargo does not allow setting a minimum supported
Rust version; you may want to keep an eye on [the MSRV RFC][msrv-rfc].

Sometimes librsvg's dependencies update their MSRV and librsvg may
need to increase it as well.  Please consider the following before
doing this:

* Absolutely do not require a nightly snapshot of the compiler, or
  crates that only build on nightly.

* Distributions with rolling releases usually keep their Rust
  toolchains fairly well updated, maybe not always at the latest, but
  within two or three releases earlier than the latest.  If the MSRV
  you want is within about six months of the latest, things are
  probably safe.
  
* Enterprise distributions update more slowly.  It is useful to watch
  for the MSRV that Firefox requires, although sometimes Firefox
  updates Rust very slowly as well.  Now that distributions are
  shipping packages other than Firefox that require Rust, they will
  probably start updating more frequently.
  
Generally — two or three releases earlier than the latest stable Rust
is OK for rolling distros, probably perilous for enterprise distros.
Releases within a year of an enterprise distro's shipping date are
probably OK.

If you are not sure, ask on the [forum for GNOME
distributors][distributors] about their plans!  (That is, posts on
`discourse.gnome.org` with the `distributor` tag.)

[msrv-rfc]: https://github.com/rust-lang/rfcs/pull/2495
[distributors]: https://discourse.gnome.org/tag/distributor

## Format for release notes in NEWS

The `NEWS` file contains the release notes.  Please use something
close to this format; it is not mandatory, but makes the formatting
consistent, and is what tooling expects elsewhere - also by writing
Markdown, you can just cut&paste it into a Gitlab release.  You can
skim bits of the NEWS file for examples on style and content.

New entries go at the **top** of the file.

```
=============
Version x.y.z
=============

Commentary on the release; put anything here that you want to
highlight.  Note changes in the build process, if any, or any other
things that may trip up distributors.

## Description of a special feature

You can include headings with `##` in Markdown syntax.

Blah blah blah.


Next is a list of features added and issues fixed; use gitlab's issue
numbers. I tend to use this order: first security bugs, then new
features and user-visible changes, finally regular bugs.  The
rationale is that if people stop reading early, at least they will
have seen the most important stuff first.

## Changes:

- #123 - title of the issue, or short summary if it warrants more
  discussion than just the title.

- #456 - fix blah blah (Contributor's Name).

## Special thanks for this release:

- Any people that you want to highlight.  Feel free to omit this
  section if the release is otherwise unremarkable.
```

## Making a tarball

```
make distcheck DESTDIR=/tmp/foo
```

The `DESTDIR` is a quirk, required because otherwise the gdk-pixbuf
loader will try to install itself into the system's location for
pixbuf loaders, and it won't work.  The `DESTDIR` is what Linux
distribution packaging scripts use to `make install` the compiled
artifacts to a temporary location before building a system package.

## Copying the tarball to master.gnome.org

If you don't have a maintainer account there, ask federico@gnome.org
to do it or [ask the release team][release-team] to do it by filing an
issue on their `GNOME/releng` project.

[release-team]: https://gitlab.gnome.org/GNOME/releng/-/issues

## Rust dependencies

Release tarballs get generated with *vendored dependencies*, that is,
the source code for all the crates that librsvg depends on gets bundled
into the tarball itself.  It is important to keep these dependencies
updated; you can do that regularly with the `cargo update` step listed
in the checklist above.

[`cargo-audit`][cargo-audit] is very useful to scan the list of
dependencies for registered vulnerabilities in the [RustSec
vulnerability database][rustsec].  Run it especially before making a
new `x.y.0` release.

Sometimes cargo-audit will report crates that are not vulnerable, but
that are unmaintained.  Keep an eye of those; you may want to file
bugs upstream to see if the crates are really unmaintained or if they
should be substituted for something else.

[cargo-audit]: https://github.com/RustSec/cargo-audit
[rustsec]: https://rustsec.org/
