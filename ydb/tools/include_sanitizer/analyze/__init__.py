"""Per-file include analysis via clang-include-cleaner.

We treat both ``.cpp`` translation units and ``.h`` headers as analysis
inputs. For ``.cpp`` files the compile command comes straight from
``compile_commands.json``. For headers we synthesize a probe TU that does
``#include "H"`` and inherit a compile command from a TU known to include
``H``.

The output is a per-file JSON document (see ``schema.py``) that captures:

- the set of ``#include`` lines we consider "removable" according to
  clang-include-cleaner,
- the set of headers that the file ought to insert directly,
- the full include tree as observed by clang's ``-H`` output,
- the resolved path of every spelled include.
"""
