# Contribution guidelines

Thank you for considering contributing to the GLib project!

These guidelines are meant for new contributors, regardless of their level
of proficiency; following them allows the core developers of the GLib project to
more effectively evaluate your contribution, and provide prompt feedback to
you. Additionally, by following these guidelines you clearly communicate
that you respect the time and effort that the people developing GLib put into
managing the project.

GLib is a complex free software utility library, and it would not exist without
contributions from the free and open source software community. There are
many things that we value:

 - bug reporting and fixing
 - documentation and examples
 - tests
 - testing and support for other platforms
 - new features

Please, do not use the issue tracker for support questions. If you have
questions on how to use GLib effectively, you can use:

 - the `#gtk` IRC channel on irc.gnome.org
 - the [`glib` tag on GNOME's Discourse](https://discourse.gnome.org/tags/glib)

You can also look at the [`glib` tag on Stack
Overflow](https://stackoverflow.com/questions/tagged/glib).

The issue tracker is meant to be used for actionable issues only.

## How to report bugs

### Security issues

You should not open a new issue for security related questions.

When in doubt, send an email to the [security](mailto:security@gnome.org)
mailing list.

### Bug reports

If you’re reporting a bug make sure to list:

 0. which version of GLib are you using?
 0. which operating system are you using?
 0. the necessary steps to reproduce the issue
 0. the expected outcome
 0. a description of the behavior
 0. a small, self-contained example exhibiting the behavior

If the issue includes a crash, you should also include:

 0. the eventual warnings printed on the terminal
 0. a backtrace, obtained with tools such as GDB or LLDB

If the issue includes a memory leak, you should also include:

 0. a log of definite leaks from a tool such as [valgrind’s
    memcheck](http://valgrind.org/docs/manual/mc-manual.html)

For small issues, such as:

 - spelling/grammar fixes in the documentation,
 - typo correction,
 - comment clean ups,
 - changes to metadata files (CI, `.gitignore`),
 - build system changes, or
 - source tree clean ups and reorganizations;

or for self-contained bug fixes where you have implemented and tested a solution
already, you should directly open a merge request instead of filing a new issue.

### Features and enhancements

Feature discussion can be open ended and require high bandwidth channels; if
you are proposing a new feature on the issue tracker, make sure to make
an actionable proposal, and list:

 0. what you’re trying to achieve and the problem it solves
 0. three (or more) existing pieces of software which would benefit from the
    new feature
 0. how the feature is implementable on platforms other than Linux

New APIs, in particular, should follow the ‘rule of three’, where there should
be three (or more) pieces of software which are ready to use the new APIs. This
allows us to check that the new APIs are usable in real-life code, and fit well
with related APIs. This reduces the chances of awkward or unusable APIs becoming
stable in GLib and having to be supported forever.

A common way to introduce new APIs or data types to GLib is to prototype them in
another code base for a while, to gain real-life experience with them before
they are imported into GLib and marked as stable.

Each feature should also come fully documented, and with tests which approach
full branch coverage of the new code. GLib’s CI system generates code coverage
reports which are viewable for each merge request.

If proposing a large feature or change, it’s better to discuss it (on the
`#gtk` IRC channel or on [Discourse](https://discourse.gnome.org) before
putting time into writing an actionable issue — and certainly before putting
time into writing a merge request.

## Your first contribution

### Prerequisites

If you want to contribute to the GLib project, you will need to have the
development tools appropriate for your operating system, including:

 - Python 3.x
 - Meson
 - Ninja
 - Gettext (19.7 or newer)
 - a [C99 compatible compiler](https://wiki.gnome.org/Projects/GLib/CompilerRequirements)

Up-to-date instructions about developing GNOME applications and libraries
can be found on [the GNOME Developer Center](https://developer.gnome.org).

The [GLib project uses GitLab](https://gitlab.gnome.org/GNOME/glib/) for code
hosting and for tracking issues. More information about using GitLab can be
found [on the GNOME wiki](https://wiki.gnome.org/GitLab).

### Getting started

You should start by forking the GLib repository from the GitLab web UI, and
cloning from your fork:

```sh
$ git clone https://gitlab.gnome.org/yourusername/glib.git
$ cd glib
```

**Note**: if you plan to push changes to back to the main repository and
have a GNOME account, you can skip the fork, and use the following instead:

```sh
$ git clone git@gitlab.gnome.org:GNOME/glib.git
$ cd glib
```

To compile the Git version of GLib on your system, you will need to
configure your build using Meson:

```sh
$ meson _builddir .
$ cd _builddir
$ ninja
```

Typically, you should work on your own branch:

```sh
$ git checkout -b your-branch
```

Once you’ve finished working on the bug fix or feature, push the branch
to the Git repository and open a new merge request, to let the GLib
core developers review your contribution.

### Code reviews

Each contribution is reviewed by the core developers of the GLib project.

The [CODEOWNERS](./docs/CODEOWNERS) document contains the list of core
contributors to GLib and the areas for which they are responsible; you
should ensure to receive their review and signoff on your changes.

It is our intention that every commit to GLib is reviewed by at least one other
person, including commits from core developers. We all make mistakes and can
always learn from each other, and code review allows that. It also reduces
[bus factor](https://en.wikipedia.org/wiki/Bus_factor) by spreading knowledge
of each commit between at least two people.

With each code review, we intend to:

 0. Identify if this is a desirable change or new feature. Ideally for larger
    features this will have been discussed (in an issue, on IRC, or on Discourse)
    already, so that effort isn’t wasted on putting together merge requests
    which will be rejected.
 0. Check the design of any new API.
 0. Provide realistic estimates of how long a review might take, if it can’t
    happen immediately.
 0. Ensure that all significant contributions of new code, or bug fixes, are
    adequately tested, either through requiring tests to be submitted at the
    same time, or as a follow-up.
 0. Ensure that all new APIs are documented and have [introspection
    annotations](https://wiki.gnome.org/Projects/GObjectIntrospection/Annotations).
 0. Check that the contribution is split into logically separate commits, each
    with a good commit message.
 0. Encourage further high quality contributions.
 0. Ensure code style and quality is upheld.

If a code review is stalled (due to not receiving comments for two or more
weeks; or due to a technical disagreement), please ping another GLib core
developer on the merge request, or on IRC, to ask for a second opinion.

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
 [GitLab syntax](https://docs.gitlab.com/ce/user/project/issues/automatic_issue_closing.html)
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

### Merge access to the GLib repository

GLib is part of the GNOME infrastructure. At the current time, any
person with write access to the GNOME repository can merge merge requests to
GLib. This is a good thing, in that it allows maintainership to be delegated
and shared as needed. However, GLib is a fairly large and complicated package
that many other things depend on, and which has platform specific behavior — so
to avoid unnecessary breakage, and to take advantage of the knowledge about GLib
that has been built up over the years, we’d like to ask people contributing to
GLib to follow a few rules:

0. Never push to the `main` branch, or any stable branches, directly; you
   should always go through a merge request, to ensure that the code is
   tested on the CI infrastructure at the very least. A merge request is
   also the proper place to get a comprehensive code review from the core
   developers of GLib.

0. Always get a code review, even for seemingly trivial changes.

0. Pay attention to the CI results. Merge requests cannot be merged until the
   CI passes. If they consistently fail, either something is wrong with the
   change, or the CI tests need fixing — in either case, please bring this to
   the attention of a core developer rather than overriding the CI.

If you have been contributing to GLib for a while and you don’t have commit
access to the repository, you may ask to obtain it following the [GNOME account
process](https://wiki.gnome.org/AccountsTeam/NewAccounts).
