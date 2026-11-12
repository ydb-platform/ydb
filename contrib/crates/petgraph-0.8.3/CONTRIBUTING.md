# Contributing to petgraph

🦕 Thanks for your help improving the project! We are so happy to have
you!

There are opportunities to contribute to petgraph at any level. It
doesn't matter which background you have in either Rust or graph
algorithms, we would love to have you and can use your help!

**No contribution is too small and all contributions are valued.**

There is a [discord server][discord-url] where you can ask questions,
get help, and
chat with other contributors.

This guide will go through the different information you might need to
contribute to petgraph. **Do not let this guide intimidate you.** It
should simply be a reference for you, which you can refer to when
contributing. The structure of this guide is as follows:

- [Conduct](#-conduct)

- [Where We Need Help](#-where-we-need-help)
    - [Issues](#-issues)
        - [Tackling existing issues](#tackling-existing-issues)
        - [Creating new issues](#creating-new-issues)
    - [Pull Requests](#-pull-requests)
        - [Creating pull requests](#creating-pull-requests)
        - [Reviewing pull requests](#reviewing-pull-requests)

- [Setup](#%EF%B8%8F-setup)
    - [Building](#%EF%B8%8F-building)
    - [Testing](#-testing)
    - [Lints](#-lints)
    - [Benchmarks](#%EF%B8%8F-benchmarks)

- [Contributors](#-contributors)

## 🫂 Conduct

The petgraph project adheres to
the [Rust Code of Conduct][code-of-conduct-url]. This
describes
the minimum behavior expected from all contributors.

## 🧩 Where We Need Help

### 🐞 Issues

#### Tackling existing issues

We have an entire category of issues which need help.
These issues are [labeled
with <span style="background-color:#142f21; color:#14c321; padding:2px 6px; border-radius:2em;">
P-help-wanted</span>][github-help-wanted-issue-label] where the P
stands for
Call for (P)articipation. We further categorize these issues
into [<span style="background-color:#142f21; color:#14c321; padding:2px 6px; border-radius:2em;">
P-easy</span>][github-easy-issue-label], [<span style="background-color:#142f21; color:#14c321; padding:2px 6px; border-radius:2em;">
P-medium</span>][github-medium-issue-label]
and [<span style="background-color:#142f21; color:#14c321; padding:2px 6px; border-radius:2em;">
P-hard</span>][github-hard-issue-label].
issues, so that you can find an issue which you feel confident
tackling 🦾

Additionally, there is
the [<span style="background-color:#112542; color:#4988dd; padding:2px 6px; border-radius:2em;">
C-feature-accepted</span> label][github-help-wanted-issue-label],
which marks
feature requests that have been deemed useful and fitting. They
are just waiting for someone to implement them! 👀

#### Creating new issues

If you have an idea for a new feature, or if you found a bug, please
open a new issue on the [GitHub issues page][github-new-issue].

### 📥 Pull Requests

Pull Requests are the way concrete changes are made to the code,
documentation, and dependencies in petgraph.

Even tiny pull requests (e.g., one character pull request fixing a
typo in API documentation) are greatly appreciated. Before making a
large change, it is usually a good idea to first open an issue
describing the change to get feedback and guidance. This will
increase the likelihood of the PR getting merged.

#### Creating pull requests

Pull requests address different kinds of changes to the codebase.
We are working on templates for the different kinds of pull requests,
which will explain teh requirements for each kind of pull request.

Until then, the different kinds of pull requests are described in the
[old section below](#-old-section-on-pull-requests).

#### Reviewing pull requests

Reviewing pull requests is a great way to help out the project. When
doing so, please keep in mind the following:

- **Be kind**: Remember that the person who submitted the pull request
  is a human being, and that they are trying to help out the project.
- **Be constructive**: If you find something that you think could be
  improved, explain why you think it should be improved and how it can
  be improved. This will help the person who submitted the pull
  request
  to learn and improve their skills.

Regarding the content of the pull request:

- **Tests**: Make sure that the pull request includes tests for the
  changes made. In particular, the pull request should include
  quickcheck tests if they are applicable.
- **Documentation**: Make sure that the pull request includes
  documentation for the changes made. If it is a new algorithm,
  it should comply with
  the [algorithm documentation defaults][algo-docs-template].
- **Traits**: Make sure that the pull request uses the existing
  graph traits. This will help to ensure that the code is
  generic and can be used with the different graph types.
- **Performance**: If the pull request includes a new algorithm,
  make sure that it is well documented, and that it includes
  performance benchmarks. The benchmarks should be included in the
  `benches` directory, and should be run with `cargo bench`.

## ⚙️ Setup

petgraph does not have any special setup requirements, other than
having a working Rust toolchain. The project is built using
[Cargo](https://doc.rust-lang.org/cargo/).

For running the benchmarks, as well as [miri][miri-url], you will
need to switch to the `nightly`
toolchain. You can do this by running:

```commandline
rustup default nightly
```

Which will install the nightly toolchain if it is not already
installed, and set it as the default toolchain. You can switch back
to the stable toolchain by running:

```commandline
rustup default stable
```

We use [just][just-url] as a command runner
to simplify the commands you need to run. You can install it using
cargo:

```commandline
cargo install just
```

To understand which commands are available, and what they do, you can
run:

```commandline
just
```

or have a look at the [`justfile`](justfile).

To run a basic test suite, as it is run in CI (using the stable
toolchain), you can run:

```commandline
just ci
```

Which will run the (cargo) tests and lints. More detailed commands
will be explained in the following sections.

### 🏗️ Building

Building petgraph is as simple as running:

```commandline
just build
```

### 🧪 Testing

Testing petgraph is similarly simple:

```commandline
just test
```

In CI, we run the tests on different rust versions and toolchains,
including nightly, as well as our current minimum supported Rust
version (MSRV). You can see which versions are tested exactly in
[`.github/workflows/ci.yml`](.github/workflows/ci.yml).

Similarly, we also run [cargo miri][miri-url] in CI, which is a tool
for detecting undefined behavior in Rust code. This will however
again require the nightly toolchain:

```commandline
just miri
```

This might, however, take a long time to run, so consider running
`miri-fast` instead, which uses [nextest][nextest-url],
a faster test runner, and skips some tests which take very long:

```commandline
just miri-fast
```

Nextest can again be installed using cargo:

```commandline
cargo install cargo-nextest --locked
```

### 🧹 Lints

We use [clippy][clippy-url] and [rustfmt][rustfmt-url] to ensure that
the code is formatted and linted correctly. You can run all linting
at once by running:

```commandline
just lint
```

Or individually, by running:

```commandline
just fmt
```

and

```commandline
just clippy
```

### ⏱️ Benchmarks

Benchmarks can be run by running:

```commandline
cargo bench
```

## 🙌 Contributors

Currently, the petgraph crate is being maintained by:

- `@ABorgna`
- `@starovoid`
- `@XVilka`
- `@RaoulLuque`
- `@indietyp`

However, much of the initial development of petgraph was done by:

* ``@bluss``

We always need more people helping maintain the project, so if
you are interested in helping out on a more long-term basis, feel free
to introduce yourself on [discord][discord-url] or start
a [discussion][github-discussions-url].


[algo-docs-template]: https://github.com/petgraph/petgraph/blob/master/assets/guide/algodocs.md

[code-of-conduct-url]: https://github.com/rust-lang/rust/blob/master/CODE_OF_CONDUCT.md

[clippy-url]: https://github.com/rust-lang/rust-clippy

[discord-url]: https://discord.gg/n2tc79tJ4e

[just-url]: https://github.com/casey/just

[rustfmt-url]: https://github.com/rust-lang/rustfmt

[github-discussions-url]: https://github.com/petgraph/petgraph/discussions

[github-easy-issue-label]: https://github.com/petgraph/petgraph/labels/P-easy

[github-hard-issue-label]: https://github.com/petgraph/petgraph/labels/P-hard

[github-help-wanted-issue-label]: https://github.com/petgraph/petgraph/labels/P-help-wanted

[github-medium-issue-label]: https://github.com/petgraph/petgraph/labels/P-medium

[github-new-issue]: https://github.com/petgraph/petgraph/issues/new

[miri-url]: https://github.com/rust-lang/miri

[nextest-url]: https://github.com/nextest-rs/nextest

## 🏛️ Old Section on Pull Requests

The following section is just here for historical reasons, until
the information on the different kinds of pull requests is included
in the different pull request templates.

All pull requests are reviewed by a team member before merging.

Additionally, different kinds of pull requests have different
requirements.

### Bug Fixes

We love getting bug fixes!

Make sure to include a regression test, so that we can be sure that we
never
accidentally re-introduce the bug again.

### Performance Improvements

You made an algorithm faster? Awesome.

When submitting performance improvement, include the following:

* A new ``#[bench]`` function that exercises this code path, if one
  doesn't
  already exist

* Before and after ``cargo bench`` scores, optionally formatted using
  [`cargo-benchcmp`][cargo-benchcmp-url]

### Implementing New Algorithms

Implementing new graph algorithms is encouraged!

If you're going to implement a new algorithm, make sure that you do
the
following:

* Add a ``quickcheck`` property test for the new algorithm

* Add a ``benchmark`` test for measuring performance of the new
  algorithm

* Document what the algorithm does and in what situations it should be
  used

* Document the big-O running time of the algorithm

* Include links to relevant reading materials, such as a paper or
  Wikipedia

* Make the algorithm work with generic graphs, constraining the
  generic graph
  type parameter with our existing graph traits, like ``Visitable``,
  or with new
  graph traits

Anyone can review a pull request implementing a new
algorithm, but the
final decision whether or not the algorithm is appropriate for
inclusion in the
``petgraph`` crate is left to team members.

Additionally, assuming that the new algorithm is merged into
``petgraph``, you are *strongly* encouraged to join the ``petgraph``
team! *You* are the best person to review any future bug fixes,
performance improvements, and whatever other changes that affect
this new algorithm.

[cargo-benchcmp-url]: https://github.com/BurntSushi/cargo-benchcmp