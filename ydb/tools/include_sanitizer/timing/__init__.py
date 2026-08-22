"""Build timing instrumentation via clang -ftime-trace.

Two stages, mirroring compdb:

- ``collect``: run a (rebuild) ya make with the compile shim injecting
  ``-ftime-trace`` so every TU emits a Chrome-trace JSON into a
  persistent directory.
- ``report`` (the ``timing`` subcommand): aggregate those traces into
  per-TU and global stats — frontend vs backend split, the headers that
  cost the most cumulative parse time across the build, and the most
  expensive template instantiations.
"""
