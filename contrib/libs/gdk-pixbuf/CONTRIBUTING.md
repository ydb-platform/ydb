Contributing to GdkPixbuf
=========================

Thank you for considering contributing to GdkPixbuf!

**NOTE**: GdkPixbuf is an old library, and its data format is
not equipped to support all the features of modern image formats.
It is also using many image libraries that are written in C,
in-process. For these reasons, we don't encourage adding new
features or support for more image formats to GdkPixbuf at this
point. You should probably look at using a modern image loading
framework such as [glycin](https://gitlab.gnome.org/GNOME/glycin/)
instead.

These guidelines are meant for new contributors, regardless of their level
of proficiency; following them allows the core developers of GdkPixbuf to
more effectively evaluate your contribution, and provide prompt feedback to
you. Additionally, by following these guidelines you clearly communicate
that you respect the time and effort that the people developing GdkPixbuf
put into managing the project.

GdkPixbuf is a free software utility library, and it would not exist without
contributions from the free and open source software community. There are
many things that we value:

 - bug reporting and fixing
 - documentation and examples
 - tests
 - testing and support for other platforms
 - new features

Please, do not use the issue tracker for support questions. If you have
questions on how to use GdkPixbuf effectively, you can use:

 - the `#gtk+` IRC channel on irc.gnome.org
 - the [gtk](https://mail.gnome.org/mailman/listinfo/gtk-list) mailing list,
   for general questions on GdkPixbuf
 - the [gtk-devel](https://mail.gnome.org/mailman/listinfo/gtk-devel-list)
   mailing list, for questions on developing GdkPixbuf itself

The issue tracker is meant to be used for actionable issues only.

GdkPixbuf is a library with a long history, and it has been incrementally
modified over years, so it may retain some older coding practices alongside
newer ones.

As it deals with loading image data into user processes, it's also important
to note that GdkPixbuf must always deal with potential security issues.

## How to report bugs

### Security issues

You should not open a new issue for security related questions.

When in doubt, send an email to the [security](mailto:security@gnome.org)
mailing list.

### Bug reports

If you’re reporting a bug make sure to list:

 1. which version of GdkPixbuf (and its dependencies) are you using?
 2. which operating system are you using?
 3. the necessary steps to reproduce the issue
 4. the expected outcome
 5. a description of the behavior
 6. a small, self-contained example exhibiting the behavior

If the issue includes a crash, you should also include:

 1. the eventual warnings printed on the terminal
 2. a backtrace, obtained with tools such as GDB or LLDB

If the issue includes a memory leak, you should also include:

 - a log of definite leaks from a tool such as [valgrind’s memcheck](http://valgrind.org/docs/manual/mc-manual.html)

For small issues, such as:

 - spelling/grammar fixes in the documentation,
 - typo correction,
 - comment clean ups,
 - changes to metadata files (CI, `.gitignore`),
 - build system changes, or
 - source tree clean ups and reorganizations;

or for self-contained bug fixes where you have implemented and tested a solution
already, you should directly open a merge request instead of filing a new issue.

## Your first contribution

### Requirements

If you wish to contribute to GdkPixbuf you will need to install the
appropriate development tools for your operating system, including:

 - Python 3.x
 - Meson
 - Ninja
 - Gettext
 - a C99 compatible compiler

### Getting started

You can start by cloning the Git repository:

```sh
$ git clone https://gitlab.gnome.org/GNOME/gdk-pixbuf.git
$ cd gdk-pixbuf
```

Then you should build GdkPixbuf locally:

```sh
$ meson _build .
$ cd _build
$ ninja
```

Once you built GdkPixbuf, you should create a new branch in order
to work on your bug fix, or your feature, undisturbed:

```sh
$ git checkout -b my-amazing-feature
[ work work work ... ]
```

You should run the test suite locally, to verify you are not introducing a
regression; if you are fixing a bug, you should also add a test case to
verify that the fix works and to avoid future regressions; if you are
introducing a new feature, you should write a comprehensive test suite:

```sh
$ cd _build
$ meson test
```

When introducing a new feature or new API, you should document it using the
[gtk-doc](https://developer.gnome.org/gtk-doc-manual/stable/) format. You
can build the GdkPixbuf API reference locally by enabling the `docs`
configuration option and building the `gdk-pixbuf-doc` target:

```sh
$ cd _build
$ meson configure -Ddocs=true
$ ninja
$ ninja gdk-pixbuf-doc
```

### Commit messages

The expected format for git commit messages is as follows:

```plain
Short explanation of the commit

Longer explanation explaining exactly what’s changed, whether any
external or private interfaces changed, what bugs were fixed (with bug
tracker reference if applicable) and so forth. Be concise but not too
brief.

Closes #1234
```

 - Always add a brief description of the commit to the _first_ line of
 the commit and terminate by two newlines (it will work without the
 second newline, but that is not nice for the interfaces).

 - First line (the brief description) must only be one sentence and
 should start with a capital letter unless it starts with a lowercase
 symbol or identifier. Don’t use a trailing period either. Don’t exceed
 72 characters.

 - The main description (the body) is normal prose and should use normal
 punctuation and capital letters where appropriate. Consider the commit
 message as an email sent to the developers (or yourself, six months
 down the line) detailing **why** you changed something. There’s no need
 to specify the **how**: the changes can be inlined.

 - When committing code on behalf of others use the `--author` option, e.g.
 `git commit -a --author "Joe Coder <joe@coder.org>"` and `--signoff`.

 - If your commit is addressing an issue, use the
 [GitLab syntax](https://docs.gitlab.com/ce/user/project/issues/managing_issues.html#default-closing-pattern)
 to automatically close the issue when merging the commit with the upstream
 repository:

```plain
Closes #1234
Fixes #1234
Closes: https://gitlab.gnome.org/GNOME/glib/issues/1234
```

 - If you have a merge request with multiple commits and none of them
 completely fixes an issue, you should add a reference to the issue in
 the commit message, e.g. `Bug: #1234`, and use the automatic issue
 closing syntax in the description of the merge request.

### Submitting your contribution for review

Once you're done with your work, you should commit it, push it to a remote
repository, and open a Merge Request against the GdkPixbuf upstream
repository. Follow the [GitLab workflow page](https://wiki.gnome.org/GitLab/)
on the GNOME wiki for further instructions.

Once you opened a Merge Request, the GdkPixbuf maintainers will review your
contribution.

## Project layout

```
├── build-aux
├── docs
├── gdk-pixbuf
│   └── pixops
├── m4
├── po
├── tests
│   └── test-images
│       ├── fail
│       ├── randomly-modified
│       └── reftests
│           └── tga
└── thumbnailer
```

 - `build-aux`: Ancillary files, necessary to build GdkPixbuf
 - `docs`: The GdkPixbuf API reference
 - `gdk-pixbuf`: The core GdkPixbuf source
  - `pixops`: Platform-specific code for pixel operations
 - `po`: Localization files
 - `tests`: Test suite
  - `test-images`: Reference images for the test suite
 - `thumbnailer`: Helper binary for generating thumbnails with GdkPixbuf

### Architecture

GdkPixbuf is divided into logical sections:

 - Core: the [GdkPixbuf][gdkpixbuf-api-core] object and its properties
 - Construction: [creating][gdkpixbuf-api-ctor] a new GdkPixbuf instance from a buffer
 - I/O: [Loading][gdkpixbuf-api-load] and [Saving][gdkpixbuf-api-save] image
   data in different formats
 - Image transformations: [Scaling and compositing][gdkpixbuf-api-ops] image
   data inside GdkPixbuf instances
 - The [GdkPixbuf loader][gdkpixbuf-api-loader] API, for incremental
   asynchronous loading of image data in a GdkPixbuf
 - The [loadable module interface][gdkpixbuf-api-module] for writing out of
   tree image loaders
 - The [animated image][gdkpixbuf-api-animation] API, for image formats
   that support animations

[gdkpixbuf-api-core]: https://developer.gnome.org/gdk-pixbuf/stable/gdk-pixbuf-The-GdkPixbuf-Structure.html
[gdkpixbuf-api-ctor]: https://developer.gnome.org/gdk-pixbuf/stable/gdk-pixbuf-Image-Data-in-Memory.html
[gdkpixbuf-api-load]: https://developer.gnome.org/gdk-pixbuf/stable/gdk-pixbuf-File-Loading.html
[gdkpixbuf-api-save]: https://developer.gnome.org/gdk-pixbuf/stable/gdk-pixbuf-File-saving.html
[gdkpixbuf-api-ops]: https://developer.gnome.org/gdk-pixbuf/stable/gdk-pixbuf-Scaling.html
[gdkpixbuf-api-loader]: https://developer.gnome.org/gdk-pixbuf/stable/GdkPixbufLoader.html
[gdkpixbuf-api-module]: https://developer.gnome.org/gdk-pixbuf/stable/gdk-pixbuf-Module-Interface.html
[gdkpixbuf-api-animation]: https://developer.gnome.org/gdk-pixbuf/stable/gdk-pixbuf-Animations.html
