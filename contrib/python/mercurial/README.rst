Mercurial
=========

Mercurial is a fast, easy to use, distributed revision control tool
for software developers.

Basic install::

 $ make            # see install targets
 $ make install    # do a system-wide install
 $ hg debuginstall # sanity-check setup
 $ hg              # see help

Running without installing::

 $ make local      # build for inplace usage
 $ ./hg --version  # should show the latest version

See https://mercurial-scm.org/ for detailed installation
instructions, platform-specific notes, and Mercurial user information.

Notes for packagers
===================

Mercurial ships a copy of the python-zstandard sources. This is used to
provide support for zstd compression and decompression functionality. The
module is not intended to be replaced by the plain python-zstandard nor
is it intended to use a system zstd library. Patches can result in hard
to diagnose errors and are explicitly discouraged as unsupported
configuration.
