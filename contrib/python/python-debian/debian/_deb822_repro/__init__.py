# The "from X import Y as Y" looks weird, but we are stuck in a fight
# between mypy and pylint in the CI.
#
#    mypy --strict insists on either of following for re-exporting
#       1) Do a "from debian._deb822_repro.X import *"
#       2) Do a "from .X import Y"
#       3) Do a "from debian._deb822_repro.X import Y as Z"
#
#    pylint on the CI fails on relative imports (it assumes "lib" is a
#    part of the python package name in relative imports).  This rules
#    out 2) from the mypy list.  The use of 1) would cause overlapping
#    imports (and also it felt prudent to import only what was exported).
#
# This left 3) as the only option for now, which pylint then complains
# about (not unreasonably in general).  Unfortunately, we can disable
# that warning in this work around.  But once 2) becomes an option
# without pylint tripping over itself on the CI, then it considerably
# better than this approach.
#

"""Round-trip safe dictionary-like interfaces to RFC822-like files

This module is a round-trip safe API for working with RFC822-like Debian data
formats. It is primarily aimed files managed by humans, like debian/control.
While it is be able to process any Deb822 file, you might find the debian.deb822
module better suited for larger files such as the `Packages` and `Sources`
from the Debian archive due to reasons explained below.

Being round-trip safe means that this module will faithfully preserve the original
formatting including whitespace and comments from the input where not modified.
A concrete example::

    >>> from debian._deb822_repro import parse_deb822_file
    >>> example_deb822_paragraph = '''
    ... Package: foo
    ... # Field comment (because it becomes just before a field)
    ... Section: main/devel
    ... Depends: libfoo,
    ... # Inline comment (associated with the next line)
    ...          libbar,
    ... '''
    >>> deb822_file = parse_deb822_file(example_deb822_paragraph.splitlines())
    >>> paragraph = next(iter(deb822_file))
    >>> paragraph['Section'] = 'devel'
    >>> output = deb822_file.dump()
    >>> output == example_deb822_paragraph.replace('Section: main/devel', 'Section: devel')
    True

This makes it particularly good for automated changes/corrections to files (partly)
maintained by humans.

Compared to debian.deb822
-------------------------

The round-trip safe API is primarily useful when your program is editing files
and the file in question is (likely) to be hand-edited or formatted directly by
human maintainers.  This includes files like debian/control and the
debian/copyright using the "DEP-5" format.

The round-trip safe API also supports parsing and working with invalid files.
This enables programs to work on the file in cases where the file was a left
with an error in an attempt to correct it (or ignore it).

On the flip side, the debian.deb822 module generally uses less memory than the
round trip safe API. In some cases, it will also have faster data structures
because its internal data structures are simpler. Accordingly, when you are doing
read-only work or/and working with large files a la the Packages or Sources
files from the Debian archive, then the round-trip safe API either provides no
advantages or its trade-offs might show up in performance statistics.

The memory and runtime performance difference should generally be constant for
valid files but not necessarily a small one.  For invalid files, some operations
can degrade in runtime performance in particular cases (memory performance for
invalid files are comparable to that of valid files).

Converting from debian.deb822
=============================

The following is a short example for how to migrate from debian.deb822 to
the round-trip safe API. Given the following source text::

    >>> dctrl_input = b'''
    ... Source: foo
    ... Build-Depends: debhelper-compat (= 13)
    ...
    ... Package: bar
    ... Architecture: any
    ... Depends: ${misc:Depends},
    ...          ${shlibs:Depends},
    ... Description: provides some exciting feature
    ...  yada yada yada
    ...  .
    ...  more deskription with a misspelling
    ... '''.lstrip()  # To remove the leading newline
    >>> # A few definitions to emulate file I/O (would be different in the program)
    >>> import contextlib, os
    >>> @contextlib.contextmanager
    ... def open_input():
    ...     # Works with and without keepends=True.
    ...     # Keep the ends here to truly emulate an open file.
    ...     yield dctrl_input.splitlines(keepends=True)
    >>> def open_output():
    ...    return open(os.devnull, 'wb')

With debian.deb822, your code might look like this::

    >>> from debian.deb822 import Deb822
    >>> with open_input() as in_fd, open_output() as out_fd:
    ...     for paragraph in Deb822.iter_paragraphs(in_fd):
    ...         if 'Description' not in paragraph:
    ...             continue
    ...         description = paragraph['Description']
    ...         # Fix typo
    ...         paragraph['Description'] = description.replace('deskription', 'description')
    ...         paragraph.dump(out_fd)

With the round-trip safe API, the rewrite would look like this::

    >>> from debian._deb822_repro import parse_deb822_file
    >>> with open_input() as in_fd, open_output() as out_fd:
    ...     parsed_file = parse_deb822_file(in_fd)
    ...     for paragraph in parsed_file:
    ...         if 'Description' not in paragraph:
    ...             continue
    ...         description = paragraph['Description']
    ...         # Fix typo
    ...         paragraph['Description'] = description.replace('deskription', 'description')
    ...     parsed_file.dump(out_fd)

Key changes are:

 1. Imports are different.
 2. Deb822.iter_paragraphs is replaced by parse_deb822_file and a reference to
    its return value is kept for later.
 3. Instead of dumping paragraphs one by one, the return value from
    parse_deb822_file is dumped at the end.

    -  The round-trip safe api does support "per-paragraph" but formatting
       and comments between paragraphs would be lost in the output. This may
       be an acceptable tradeoff or desired for some cases.

Note that the round trip safe API does not accept all the same parameters as the
debian.deb822 module does.  Often this is because the feature is not relevant for
the round-trip safe API (e.g., python-apt cannot be used as it discard comments)
or is obsolete in the debian.deb822 module and therefore omitted.

For list based fields, you may want to have a look at the
Deb822ParagraphElement.as_interpreted_dict_view method.

Stability of this API
---------------------

The API is subject to change based on feedback from early adopters and beta
testers.  That said, the code for valid files is unlikely to change in
a backwards incompatible way.

Things that might change in an incompatible way include:
 * Whether invalid files are accepted (parsed without errors) by default.
   (currently they are)
 * How invalid files are parsed.  As an example, currently a syntax error acts
   as a paragraph separator. Whether it should is open to debate.

"""

# pylint: disable=useless-import-alias
from debian._deb822_repro.parsing import (
    parse_deb822_file as parse_deb822_file,
    LIST_SPACE_SEPARATED_INTERPRETATION as LIST_SPACE_SEPARATED_INTERPRETATION,
    LIST_COMMA_SEPARATED_INTERPRETATION as LIST_COMMA_SEPARATED_INTERPRETATION,
    Interpretation as Interpretation,
    # Primarily for documentation purposes / help()
    Deb822FileElement as Deb822FileElement,
    Deb822NoDuplicateFieldsParagraphElement,
    Deb822ParagraphElement as Deb822ParagraphElement,
)
from debian._deb822_repro.types import (
    AmbiguousDeb822FieldKeyError as AmbiguousDeb822FieldKeyError,
    SyntaxOrParseError,
)

__all__ = [
    "parse_deb822_file",
    "AmbiguousDeb822FieldKeyError",
    "LIST_SPACE_SEPARATED_INTERPRETATION",
    "LIST_COMMA_SEPARATED_INTERPRETATION",
    "Interpretation",
    "Deb822FileElement",
    "Deb822NoDuplicateFieldsParagraphElement",
    "Deb822ParagraphElement",
    "SyntaxOrParseError",
]
