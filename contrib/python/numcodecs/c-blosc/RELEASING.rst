================
Releasing Blosc
================

:Author: Francesc Alted
:Contact: francesc@blosc.org
:Date: 2014-01-15


Preliminaries
-------------

- Switch to main branch::

    $ git switch main

- Make sure that ``RELEASE_NOTES.rst`` and ``ANNOUNCE.rst`` are up to
  date with the latest news in the release.

- Check that *VERSION* symbols in blosc/blosc.h contains the correct info.

- Commit the changes::

    $ git commit -a -m"Getting ready for X.Y.Z release"
    $ git push


Testing
-------

Create a new build/ directory, change into it and issue::

  $ cmake ..
  $ cmake --build .
  $ ctest

To actually test Blosc the hard way, look at the end of:

https://www.blosc.org/pages/synthetic-benchmarks/

where instructions on how to intensively test (and benchmark) Blosc
are given.

Forward compatibility testing
-----------------------------

First, go to the compat/ directory and generate a file with the current
version::

  $ cd ../compat
  $ export LD_LIBRARY_PATH=../build/blosc
  $ gcc -o filegen filegen.c -L$LD_LIBRARY_PATH -lblosc -I../blosc
  $ ./filegen compress lz4 blosc-1.y.z-lz4.cdata

In order to make sure that we are not breaking forward compatibility,
link and run the `compat/filegen` utility against different versions of
the Blosc library (suggestion: 1.3.0, 1.7.0, 1.11.1, 1.14.1).

You can compile the utility with different blosc shared libraries with::

  $ export LD_LIBRARY_PATH=shared_blosc_library_path
  $ gcc -o filegen filegen.c -L$LD_LIBRARY_PATH -lblosc -Iblosc.h_include_path

Then, test the file created with the new version with::

  $ ./filegen decompress blosc-1.y.z-lz4.cdata

If that works and you want to keep track of this for future compatibility checks
just add the new file to the suite::

  $ git add blosc-1.y.z-lz4.cdata
  $ git commit -m"Add a new cdata file for compatibility checks"

Repeat this for every codec shipped with Blosc (blosclz, lz4, lz4hc, snappy,
zlib and zstd).

Tagging
-------

- Create a tag ``X.Y.Z`` from ``main``::

    $ git tag -a vX.Y.Z -m "Tagging version X.Y.Z"

- Push the previous commits and tag to the github repo::

    $ git push
    $ git push --tags


Announcing
----------

- Send an announcement to the blosc and pytables-dev.  Use the
  ``ANNOUNCE.rst`` file as skeleton (possibly as the definitive version).


Post-release actions
--------------------

- Edit *VERSION* symbols in blosc/blosc.h in main to increment the
  version to the next minor one (i.e. X.Y.Z --> X.Y.(Z+1).dev).

- Create new headers for adding new features in ``RELEASE_NOTES.rst``
  and add this place-holder instead:

  #XXX version-specific blurb XXX#

- Commit the changes::

    $ git commit -a -m"Post X.Y.Z release actions done"
    $ git push


That's all folks!
