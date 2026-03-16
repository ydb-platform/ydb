Contributing to librsvg
=======================

Thank you for looking in this file!  There are different ways of
contributing to librsvg, and we appreciate all of them.

* [Source repository](#source-repository)
* [Reporting bugs](#reporting-bugs)
* [Feature requests](#feature-requests)
* [Hacking on librsvg](#hacking-on-librsvg)

There is a **code of conduct** for contributors to librsvg; please see the
file [`code-of-conduct.md`][coc].

## Source repository

Librsvg's main source repository is at gitlab.gnome.org.  You can view
the web interface here:

https://gitlab.gnome.org/GNOME/librsvg

Development happens in the `main` branch.  There are also branches for
stable releases.

Alternatively, you can use the mirror at Github:

https://github.com/GNOME/librsvg

Note that we don't do bug tracking in the Github mirror; see the next
section.

If you need to publish a branch, feel free to do it at any
publically-accessible Git hosting service, although gitlab.gnome.org
makes things easier for the maintainers of librsvg.

To work on the source code, you may find the "[Hacking on
librsvg](#hacking-on-librsvg)" section helpful.

## Reporting bugs

Please report bugs at https://gitlab.gnome.org/GNOME/librsvg/issues

If you want to report a rendering bug, or a missing SVG feature,
please provide an example SVG file as an attachment to your bug
report.  It really helps if you can minimize the SVG to only the
elements required to reproduce the bug or see the missing feature, but
it is not absolutely required.  **Please be careful** of publishing
SVG images that you don't want other people to see, or images whose
copyright does not allow redistribution; the bug tracker is a public
resource and attachments are visible to everyone.

You can also [browse the existing bugs][bugs-browse].

### Obtaining debug logs

Librsvg can be asked to output debug logs.  Set the `RSVG_LOG`
environment variable, and then librsvg will print some 
information to stdout:

```
$ RSVG_LOG=1 some-program-that-uses-librsvg
... debug output goes here ...
```

As of librsvg 2.43.5, there are no options you can set in the
`RSVG_LOG` variable; the library just checks whether that environment
variable is present or not.

## Feature requests

Librsvg aims to be a small and relatively simple SVG rendering
library.  Currently we do not plan to support scripting, animation, or
interactive features like mouse events on SVG elements.

However, we *do* aim go provide good support for SVG's graphical
features.  Please see the "[reporting bugs](#reporting-bugs)" section for
information about our bug tracking system; feature requests should be
directed there.

It is especially helpful if you file bug for a feature request along
with a sample SVG file.

## Hacking on librsvg

The library's internals are being documented at
https://gnome.pages.gitlab.gnome.org/librsvg/internals/librsvg/index.html

Please see that documentation for common tasks like adding support for
a new CSS property.

What can you hack on?

* [Bugs for newcomers](https://gitlab.gnome.org/GNOME/librsvg/-/issues?label_name%5B%5D=4.+Newcomers)
* Pick something from the [development roadmap](devel-docs/README.md)!

### Working on the source

Librsvg uses an autotools setup, which is described in detail [in this
blog post][blog].

If you need to **add a new source file**, you need to do it in the
toplevel [`Makefile.am`][toplevel-makefile].  *Note that this is for
both C and Rust sources*, since `make(1)` needs to know when a Rust
file changed so it can call `cargo` as appropriate.

It is perfectly fine to [ask the maintainer][maintainer] if you have
questions about the Autotools setup; it's a tricky bit of machinery,
and we are glad to help.

Please read the file [`ARCHITECTURE.md`][arch]; this describes the
overall flow of the source code, so hopefully it will be easier for
you to navigate.

### Taking advantage of Continuous Integration

If you fork librsvg in `gitlab.gnome.org` and push commits to your
forked version, the Continuous Integration machinery (CI) will run
automatically.

A little glossary:

* ***Continuous Integration (CI)*** - A tireless robot that builds
  librsvg on every push, and runs various kinds of tests.
  
* ***Pipeline*** - A set of ***jobs*** that may happen on every push.
  Every pipeline has ***stages*** of things that get run.  You can
  [view recent
  pipelines](https://gitlab.gnome.org/GNOME/librsvg/pipelines) and
  examine their status.
  
* ***Stages*** - Each stage runs some kind of test on librsvg, and
  depends on the previous stages succeeding.  We have a *Test* stage
  that just builds librsvg as quickly as possible and runs its test
  suite.  If that succeeds, then it will go to a *Lint* stage which
  runs `rustfmt` to ensure that the coding style remains consistent.
  Finally, there is a `Cross_distro` stage that tries to build/test
  librsvg on various operating systems and configurations.
  
* ***Jobs*** - You can think of a job as "something that runs in a
  specific container image, and emits a success/failure result".  For
  example, the `Test` stage runs a job in a fast Fedora container.
  The `Lint` stage runs `rustfmt` in a Rust-specific container that
  always contains a recent version of `rustfmt`.  Distro-specific jobs
  run on container images for each distro we support.
  
The default CI pipeline for people's branches is set up to build your
branch and to run the test suite and lints.  If any tests fail, the
pipeline will fail and you can then examine the job's build
artifacts.  If the lint stage fails, you will have to reindent your
code.

***Automating the code formatting:*** You may want to enable a
[client-side git
hook](https://git-scm.com/book/en/v2/Customizing-Git-Git-Hooks) to run
`rustfmt` before you can commit something; otherwise the `Lint` stage
of CI pipelines will fail:

1. `cd librsvg`

1. `mv .git/hooks/pre-commit.sample .git/hooks/pre-commit`

1. Edit `.git/hooks/pre-commit` and put in one of the following
   commands:

  * If you want code reformatted automatically, no questions asked:
    `cargo fmt`
    ***Note:*** if this actually reformats your code while committing,
    you'll have to re-stage the new changes and `git commit --amend`.  Be
    careful if you had unstaged changes that got reformatted!

  * If you want to examine errors if rustfmt doesn't like your
    indentation, but don't want it to make changes on its own:  
    `cargo fmt --all -- --check`

### Test suite

Please make sure that the test suite passes with the changes in your
branch.  The easiest way to run all the tests is to go to librsvg's
toplevel directory and run `make check`.  This will run both the small
unit tests and the black box tests in the `librsvg/tests` directory.

It's possible that a wide swath of tests will fail when you do this,
don't panic! that's likely just text rendering, which varies a bit
from distro to distro. To run the test suite on the same OS as the 
CI, first install docker then see this file 
[`tools/docker/README.md`][docker-tests-readme].

If you need to add new tests (you should, for new features, or for
things that we weren't testing!), or for additional information on how
the test suite works, please see the file
[`tests/README.md`][tests-readme].

In addition, the CI machinery will run librsvg's test suite
automatically when you push some commits.  You can tweak what happens
during CI for your branch in the [`.gitlab-ci.yml`
file](.gitlab-ci.yml) — for example, if you want to enable
distro-specific tests to test things on a system that you don't have.

### Testing changes

The most direct way to test a change is to have an example SVG file
that exercises the code you want to test.  Then you can rebuild
librsvg, and run this:

```
cd /src/librsvg
cargo build
./target/debug/rsvg-convert -o foo.png foo.svg
```

Then you can view the resulting `foo.png` image.

**Please update the test suite** with a suitable example file once you
have things working (or before even writing code, if you like
test-driven development), so we can avoid regressions later.  The test
suite is documented in [`tests/README.md`][tests-readme].

### Creating a merge request

You may create a forked version of librsvg in [GNOME's Gitlab
instance][gitlab], or any other publically-accesible Git hosting
service.  You can register an account there, or log in with your
account from other OAuth services.

Note that the maintainers of librsvg only get notified about merge
requests (or pull requests) if your fork is in
[gitlab.gnome.org][gitlab].

For technical reasons, the maintainers of librsvg do not get
automatically notified if you submit a pull request through the GNOME
mirror in Github.  [Please contact the maintainer][maintainer] directly if you
have a pull request there or a branch that you would like to
contribute.

### Formatting commit messages

If a commit fixes a bug, please format its commit message like this:

```
(#123): Don't crash when foo is bar

Explanation for why the crash happened, or anything that is not
obvious from looking at the diff.

https://gitlab.gnome.org/GNOME/librsvg/issues/123
```

Note the `(#123)` in the first line.  This is the line that shows up
in single-line git logs, and having the bug number there makes it
easier to write the release notes later — one does not have to read
all the commit messages to find the ids of fixed bugs.

Also, please paste the complete URL to the bug report somewhere in the
commit message, so that it's easier to visit when reading the commit
logs.

Generally, commit messages should summarize *what* you did, and *why*.
Think of someone doing `git blame` in the future when trying to figure
out how some code works:  they will want to see *why* a certain line
of source code is there.  The commit where that line was introduced
should explain it.

### Testing performance-related changes

You can use the [rsvg-bench] tool to benchmark librsvg.  It lets you
run a benchmarking program **on an already-installed librsvg
library**.  For example, you can ask rsvg-bench to render one or more
SVGs hundreds of times in a row, so you can take accurate timings or
run a sampling profiler and get enough samples.

**Why is rsvg-bench not integrated in librsvg's sources?**  Because
rsvg-bench depends on the [rsvg-rs] Rust bindings, and these are
shipped outside of librsvg.  This requires you to first install
librsvg, and then compile rsvg-bench.  We aim to make this easier in
the future.  Of course all help is appreciated!

### Included benchmarks

The [`rsvg_internals/benches`][benches] directory has a
couple of benchmarks for functions related to SVG filter effects.  You
can run them with `cargo bench`.

These benchmarks use the [Criterion] crate, which supports some
interesting options to generate plots and such.  You can see the
[Criterion command line options][criterion-options].

[coc]: code-of-conduct.md
[gitlab]: https://gitlab.gnome.org/GNOME/librsvg
[bugs-browse]: https://gitlab.gnome.org/GNOME/librsvg/issues
[maintainer]: README.md#maintainers
[tests-readme]: tests/README.md
[blog]: https://people.gnome.org/~federico/blog/librsvg-build-infrastructure.html
[toplevel-makefile]: Makefile.am
[tests-readme]: tests/README.md
[rsvg-bench]: https://gitlab.gnome.org/federico/rsvg-bench
[rsvg-rs]: https://github.com/selaux/rsvg-rs
[arch]: ARCHITECTURE.md
[benches]: rsvg-internals/benches
[Criterion]: https://crates.io/crates/criterion
[criterion-options]: https://japaric.github.io/criterion.rs/book/user_guide/command_line_options.html
[docker-tests-readme]: tools/docker/README.md
