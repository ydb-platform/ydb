#
# Copyright (c) nexB Inc. and others. All rights reserved.
# ScanCode is a trademark of nexB Inc.
# SPDX-License-Identifier: Apache-2.0
# See http://www.apache.org/licenses/LICENSE-2.0 for the license text.
# See https://github.com/aboutcode-org/typecode for support or download.
# See https://aboutcode.org for more information about nexB OSS projects.
#

import contextlib
import io
import os

import attr
from binaryornot.helpers import get_starting_chunk
from binaryornot.helpers import is_binary_string
from pdfminer.pdfparser import PDFParser
from pdfminer.pdfdocument import PDFDocument
from pdfminer.pdfparser import PDFSyntaxError
from pdfminer.psparser import PSSyntaxError
from pdfminer.pdfdocument import PDFEncryptionError
from pdfminer.pdftypes import PDFException

from commoncode import filetype
from commoncode import fileutils
from commoncode.datautils import Boolean
from commoncode.datautils import List
from commoncode.datautils import String
from commoncode import text
from typecode import entropy
from typecode import extractible
from typecode import magic2
from typecode import mimetypes
from typecode.pygments_lexers import ClassNotFound as LexerClassNotFound
from typecode.pygments_lexers import get_lexer_for_filename
from typecode.pygments_lexers import guess_lexer

"""
Utilities to detect and report the type of a file or path based on its name,
extension and mostly its content.


TODO: consider adding support for these

https://www.freedesktop.org/wiki/Specifications/shared-mime-info-spec/
 https://github.com/freedesktop/xdg-shared-mime-info
 https://pypi.python.org/pypi/z3c.sharedmimeinfo/0.1.0
 https://github.com/chesstrian/mimetype-description

and :
 https://github.com/mime-types/mime-types-data

"""

# Tracing flag
TRACE = False


def logger_debug(*args):
    pass


if TRACE:
    import logging
    import sys

    logger = logging.getLogger(__name__)
    logging.basicConfig(stream=sys.stdout)
    logger.setLevel(logging.DEBUG)

    def logger_debug(*args):
        return logger.debug(" ".join(isinstance(a, str) and a or repr(a) for a in args))


# Ensure that all dates are UTC, especially for fine free file.
os.environ["TZ"] = "UTC"

ELF_EXE = "executable"
ELF_SHARED = "shared object"
ELF_RELOC = "relocatable"
ELF_UNKNOWN = "unknown"
elf_types = (
    ELF_EXE,
    ELF_SHARED,
    ELF_RELOC,
)

PLAIN_TEXT_EXTENSIONS = (
    # docs
    ".rst",
    ".rest",
    ".md",
    ".txt",
    # This one is actually not handled by Pygments. There are probably more.
    ".log",
    # various data
    ".json",
    ".xml",
)

MAKEFILE_EXTENSIONS = (
    "Makefile",
    "Makefile.inc",
)

# Global registry of Type objects, keyed by location
# FIXME: can this be a memory hog for very large scans?
_registry = {}


def get_type(location):
    """
    Return a Type object for location.
    """
    abs_loc = os.path.abspath(location)
    try:
        return _registry[abs_loc]
    except KeyError:
        t = Type(abs_loc)
        _registry[abs_loc] = t
        return t


# TODO: simplify code using a cached property decorator


class Type(object):
    """
    Content, media and mimetype information about a file.
    All flags and values are tri-booleans. You can test a value state with
    `is`:

     - if the value is None, it has not been computed yet
     - if the value is False or has some true-ish value it has been computed

    Raise an IOError if the location does not exists.
    """

    __slots__ = (
        "location",
        "is_file",
        "is_dir",
        "is_regular",
        "is_special",
        "date",
        "is_link",
        "is_broken_link",
        "_size",
        "_link_target",
        "_mimetype_python",
        "_filetype_file",
        "_mimetype_file",
        "_filetype_pygment",
        "_is_pdf_with_text",
        "_is_text",
        "_is_text_with_long_lines",
        "_is_compact_js",
        "_is_js_map",
        "_is_binary",
        "_is_data",
        "_is_archive",
        "_contains_text",
    )

    # FIXME: we should use an introspectable attrs class instead
    # ATTENTION: keep this in sync with sloats and properties
    text_attributes = [
        "filetype_file",
        "mimetype_file",
        "mimetype_python",
        "filetype_pygment",
        "elf_type",
        "programming_language",
        "link_target",
    ]

    numeric_attributes = [
        "size",
    ]
    date_attributes = [
        "date",
    ]

    boolean_attributes = [
        "is_file",
        "is_dir",
        "is_regular",
        "is_special",
        "is_link",
        "is_broken_link",
        "is_pdf_with_text",
        "is_text",
        "is_text_with_long_lines",
        "is_compact_js",
        "is_js_map",
        "is_binary",
        "is_data",
        "is_archive",
        "contains_text",
        "is_compressed",
        "is_c_source",
        "is_c_source",
        "is_elf",
        "is_elf",
        "is_filesystem",
        "is_java_class",
        "is_java_source",
        "is_media",
        "is_media_with_meta",
        "is_office_doc",
        "is_package",
        "is_pdf",
        "is_script",
        "is_source",
        "is_stripped_elf",
        "is_winexe",
        "is_makefile",
    ]
    exportable_attributes = (
        text_attributes + numeric_attributes + date_attributes + boolean_attributes
    )

    def __init__(self, location):
        if not location or (not os.path.exists(location) and not filetype.is_broken_link(location)):
            raise IOError("[Errno 2] No such file or directory: '%(location)r'" % locals())
        self.location = location
        # flags and values
        self.is_file = filetype.is_file(location)
        self.is_dir = filetype.is_dir(location)
        self.is_regular = filetype.is_regular(location)
        self.is_special = filetype.is_special(location)

        self.date = filetype.get_last_modified_date(location)

        self.is_link = filetype.is_link(location)
        self.is_broken_link = bool(filetype.is_broken_link(location))

        # FIXME: the way the True and False values are checked in properties is verbose and contrived at best
        # and is due to use None/True/False as different values
        # computed on demand
        self._size = None
        self._link_target = None

        self._mimetype_python = None
        self._filetype_file = None
        self._mimetype_file = None
        self._filetype_pygment = None
        self._is_pdf_with_text = None
        self._is_text = None
        self._is_text_with_long_lines = None
        self._is_compact_js = None
        self._is_js_map = None
        self._is_binary = None
        self._is_data = None
        self._is_archive = None
        self._contains_text = None

    def __repr__(self):
        return "Type(ftf=%r, mtf=%r, ftpyg=%r, mtpy=%r)" % (
            self.filetype_file,
            self.mimetype_file,
            self.filetype_pygment,
            self.mimetype_python,
        )

    def to_dict(self, include_date=True):
        """
        Return a mapping of attributes.
        """
        nv = ((n, getattr(self, n)) for n in self.exportable_attributes)
        if not include_date:
            nv = ((n, v) for n, v in nv if n not in self.date_attributes)

        return dict(nv)

    @property
    def size(self):
        """
        Return the size of a file or directory
        """
        if self._size is None:
            self._size = 0
            if self.is_file or self.is_dir:
                self._size = filetype.get_size(self.location)
        return self._size

    @property
    def link_target(self):
        """
        Return a link target for symlinks or an empty string otherwise.
        """
        if self._link_target is None:
            self._link_target = ""
            if self.is_link or self.is_broken_link:
                self._link_target = filetype.get_link_target(self.location)
        return self._link_target

    @property
    def mimetype_python(self):
        """
        Return the mimetype using the a map of mimetypes by file extension.
        """
        if self._mimetype_python is None:
            self._mimetype_python = ""
            if self.is_file is True:
                self._mimetype_python = mimetypes.guess_type(self.location) or ""
        return self._mimetype_python

    @property
    def filetype_file(self):
        """
        Return the filetype using the fine free file library.
        """
        if self._filetype_file is None:
            self._filetype_file = ""
            if self.is_file is True:
                self._filetype_file = magic2.file_type(self.location)
        return self._filetype_file

    @property
    def mimetype_file(self):
        """
        Return the mimetype using the fine free file library.
        """
        if self._mimetype_file is None:
            self._mimetype_file = ""
            if self.is_file is True:
                self._mimetype_file = magic2.mime_type(self.location)
        return self._mimetype_file

    @property
    def filetype_pygment(self):
        """
        Return the filetype guessed using Pygments lexer, mostly for source code.
        """
        if self._filetype_pygment is None:
            self._filetype_pygment = ""
            if self.is_text and not self.is_media:
                lexer = get_pygments_lexer(self.location)
                if lexer and not lexer.name.startswith("JSON"):
                    self._filetype_pygment = lexer.name or ""
                else:
                    self._filetype_pygment = ""
        return self._filetype_pygment

    @property
    def file_name(self):
        """
        Return the file name for this location.
        """
        # TODO: cache me
        return fileutils.file_name(self.location)

    # FIXME: we way we use tri booleans is a tad ugly

    @property
    def is_binary(self):
        """
        Return True is the file at location is likely to be a binary file.
        """
        if self._is_binary is None:
            self._is_binary = False
            if self.is_file is True:
                self._is_binary = is_binary(self.location)
        return self._is_binary

    @property
    def is_text(self):
        """
        Return True is the file at location is likely to be a text file.
        """
        if self._is_text is None:
            self._is_text = self.is_file is True and self.is_binary is False
        return self._is_text

    @property
    def is_text_with_long_lines(self):
        """
        Return True is the file at location is likely to be a text file.
        """
        if self._is_text_with_long_lines is None:
            self._is_text_with_long_lines = (
                self.is_text is True and "long lines" in self.filetype_file.lower()
            )
        return self._is_text_with_long_lines

    @property
    def is_compact_js(self):
        """
        Return True is the file at location is likely to be a compact JavaScript
        (e.g. map or minified) or JSON file.
        """
        if self._is_compact_js is None:
            # FIXME: when moving to Python 3
            extensions = (
                ".min.js",
                ".typeface.json",
            )
            json_ext = ".json"

            self._is_compact_js = (
                self.is_js_map
                or (self.is_text is True and self.location.endswith(extensions))
                or (
                    self.filetype_file.lower() == "data"
                    and (
                        self.programming_language == "JavaScript"
                        or self.location.endswith(json_ext)
                    )
                )
            )
        return self._is_compact_js

    @property
    def is_js_map(self):
        """
        Return True is the file at location is likely to be a CSS or JavaScript
        map file.
        """
        if self._is_js_map is None:
            # FIXME: when moving to Python 3
            extensions = (
                ".js.map",
                ".css.map",
            )
            self._is_js_map = self.is_text is True and self.location.endswith(extensions)
        return self._is_js_map

    @property
    def is_archive(self):
        """
        Return True if the file is some kind of archive or compressed file.
        """
        if self._is_archive is not None:
            return self._is_archive

        self._is_archive = False
        docx_type_end = "2007+"

        ft = self.filetype_file.lower()

        if self.is_text:
            self._is_archive = False
        elif ft.startswith("gem image data"):
            self._is_archive = False
        elif self.is_compressed:
            self._is_archive = True
        elif "archive" in ft:
            self._is_archive = True
        elif self.is_package:
            self._is_archive = True
        elif self.is_filesystem:
            self._is_archive = True
        elif self.is_office_doc and ft.endswith(docx_type_end):
            self._is_archive = True
        elif "(zip)" in ft:
            # FIXME: is this really correct???
            self._is_archive = True
        elif extractible.can_extract(self.location):
            self._is_archive = True

        return self._is_archive

    @property
    def is_office_doc(self):
        loc = self.location.lower()
        # FIXME: add open office extensions and other extensions for other docs
        msoffice_exts = (
            ".doc",
            ".docx",
            ".xlsx",
            ".xlsx",
            ".ppt",
            ".pptx",
        )

        if loc.endswith(msoffice_exts):
            return True
        else:
            ft = self.filetype_file.lower()
            if ft.startswith("microsoft") and ft.endswith("2007+"):
                return True
            return False

    @property
    def is_package(self):
        """
        Return True if the file is some kind of packaged archive.
        """
        # FIXME: this should beased on proper package recognition, not this simplistic check
        ft = self.filetype_file.lower()
        loc = self.location.lower()
        package_archive_extensions = ".jar", ".war", ".ear", ".zip", ".whl", ".egg"
        gem_extension = ".gem"

        # FIXME: this is grossly under specified and is missing many packages
        if (
            "debian binary package" in ft
            or ft.startswith("rpm ")
            or (ft == "posix tar archive" and loc.endswith(gem_extension))
            or (
                ft.startswith(("zip archive", "java archive"))
                and loc.endswith(package_archive_extensions)
            )
        ):
            return True
        else:
            return False

    @property
    def is_compressed(self):
        """
        Return True if the file is some kind of compressed file.
        """
        ft = self.filetype_file.lower()

        docx_ext = "x"

        if not self.is_text and (
            "(zip)" in ft
            or ft.startswith(("zip archive", "java archive"))
            or self.is_package
            or any(x in ft for x in ("squashfs filesystem", "compressed"))
            or (self.is_office_doc and self.location.endswith(docx_ext))
        ):
            return True
        else:
            return False

    @property
    def is_filesystem(self):
        """
        Return True if the file is some kind of file system or disk image.
        """
        ft = self.filetype_file.lower()
        if "squashfs filesystem" in ft:
            return True
        else:
            return False

    @property
    def is_media(self):
        """
        Return True if the file is likely to be a media file.
        """
        # TODO: fonts?
        mt = self.mimetype_file
        mimes = (
            "image",
            "picture",
            "audio",
            "video",
            "graphic",
            "sound",
        )

        ft = self.filetype_file.lower()
        types = (
            "image data",
            "graphics image",
            "ms-windows metafont .wmf",
            "windows enhanced metafile",
            "png image",
            "interleaved image",
            "microsoft asf",
            "image text",
            "photoshop image",
            "shop pro image",
            "ogg data",
            "vorbis",
            "mpeg",
            "theora",
            "bitmap",
            "audio",
            "video",
            "sound",
            "riff",
            "icon",
            "pc bitmap",
            "image data",
            "netpbm",
        )

        if any(m in mt for m in mimes) or any(t in ft for t in types):
            return True

        tga_ext = ".tga"

        if (
            ft == "data"
            and mt == "application/octet-stream"
            and self.location.lower().endswith(tga_ext)
        ):
            # there is a regression in libmagic 5.38 https://bugs.astron.com/view.php?id=161
            # this is a targe image
            return True

        return False

    @property
    def is_media_with_meta(self):
        """
        Return True if the file is a media file that may contain text metadata.
        """
        # For now we only exclude PNGs, JEPG and Gifs, though there are likely
        # several other. mp(1,2,3,4), jpeg, gif all have support for metadata
        # but we exclude some.

        # FIXME: only include types that are known to have metadata
        if not self.is_media:
            return False
        if self.filetype_file.lower().startswith(
            ("gif image", "png image", "jpeg image", "netpbm", "mpeg")
        ):
            return False
        else:
            return True

    @property
    def is_pdf(self):
        """
        Return True if the file is highly likely to be a pdf file.
        """
        if "pdf" in self.mimetype_file:
            return True
        else:
            return False

    @property
    def is_pdf_with_text(self):
        """
        Return True if the file is a pdf file from which we can extract text.
        """
        if self._is_pdf_with_text is None:
            self._is_pdf_with_text = False
            if not self.is_file is True and not self.is_pdf is True:
                self._is_pdf_with_text = False
            else:
                with open(self.location, "rb") as pf:
                    try:
                        parser = PDFParser(pf)
                        doc = PDFDocument(parser)
                        self._is_pdf_with_text = doc.is_extractable
                    except (PDFSyntaxError, PSSyntaxError, PDFException, PDFEncryptionError):
                        self._is_pdf_with_text = False
        return self._is_pdf_with_text

    @property
    def contains_text(self):
        """
        Return True if a file possibly contains some text.
        """
        if self._contains_text is None:
            svg_ext = ".svg"

            if not self.is_file:
                self._contains_text = False

            elif self.is_media and not self.location.lower().endswith(svg_ext):
                # and not self.is_media_with_meta:
                self._contains_text = False

            elif self.is_text:
                self._contains_text = True

            elif self.is_pdf and not self.is_pdf_with_text:
                self._contains_text = False

            elif self.is_compressed:
                self._contains_text = False

            elif self.is_archive and not self.is_compressed:
                self._contains_text = True

            # TODO: exclude all binaries??
            # elif self.is_binary:
            #     self._contains_text = False

            else:
                self._contains_text = True
        return self._contains_text

    @property
    def is_data(self):
        """
        Return True if the file is some kind of data file.
        """
        if self._is_data is None:
            if not self.is_file:
                self._is_data = False

            large_file = 5 * 1000 * 1000
            large_text_file = 2 * 1000 * 1000

            ft = self.filetype_file.lower()

            size = self.size
            max_entropy = 1.3

            if (
                ft == "data"
                or is_data(self.location)
                or ("data" in ft and size > large_file)
                or (self.is_text and size > large_text_file)
                or (self.is_text and size > large_text_file)
                or (entropy.entropy(self.location, length=5000) < max_entropy)
            ):
                self._is_data = True
            else:
                self._is_data = False
        return self._is_data

    @property
    def is_script(self):
        """
        Return True if the file is script-like.
        """
        ft = self.filetype_file.lower()
        if self.is_text is True and "script" in ft and not "makefile" in ft:
            return True
        else:
            return False

    def _is_plain_text(self, _pte=PLAIN_TEXT_EXTENSIONS):
        return self.location.endswith(_pte)

    @property
    def is_makefile(self):
        return self.location.endswith(MAKEFILE_EXTENSIONS)

    @property
    def is_source(self):
        """
        Return True if the file is source code.
        """
        if self.is_text is False:
            return False

        elif self._is_plain_text():
            return False

        elif self.is_makefile or self.is_js_map:
            return False

        elif self.is_java_source is True or self.is_c_source is True:
            return True

        elif self.filetype_pygment or self.is_script is True:
            return True

        else:
            return False

    @property
    def programming_language(self):
        """
        Return the programming language if the file is source code or an empty
        string.
        """
        if self.is_source:
            return self.filetype_pygment or ""
        return ""

    @property
    def is_c_source(self):
        C_EXTENSIONS = set(
            [
                ".c",
                ".cc",
                ".cp",
                ".cpp",
                ".cxx",
                ".c++",
                ".h",
                ".hh",
                ".s",
                ".asm",
                ".hpp",
                ".hxx",
                ".h++",
                ".i",
                ".ii",
                ".m",
            ]
        )

        ext = fileutils.file_extension(self.location)
        return self.is_text is True and ext.lower() in C_EXTENSIONS

    @property
    def is_winexe(self):
        """
        Return True if a the file is a windows executable.
        """
        ft = self.filetype_file.lower()
        return "for ms windows" in ft or ft.startswith("pe32")

    @property
    def is_elf(self):
        ft = self.filetype_file.lower()
        if ft.startswith("elf") and (ELF_EXE in ft or ELF_SHARED in ft or ELF_RELOC in ft):
            return True
        else:
            return False

    @property
    def elf_type(self):
        if self.is_elf is True:
            ft = self.filetype_file.lower()
            for t in elf_types:
                if t in ft:
                    return t
            return ELF_UNKNOWN
        else:
            return ""

    @property
    def is_stripped_elf(self):
        if self.is_elf is True:
            return "not stripped" not in self.filetype_file.lower()
        else:
            return False

    @property
    def is_java_source(self):
        """
        FIXME: Check the filetype.
        """
        return self.is_file and self.file_name.lower().endswith((".java", ".aj", ".jad", ".ajt"))

    @property
    def is_java_class(self):
        """
        FIXME: Check the filetype.
        """
        return self.is_file and self.file_name.lower().endswith(".class")


@attr.attributes
class TypeDefinition(object):
    name = String(repr=True)
    filetypes = List(repr=True)
    mimetypes = List(repr=True)
    extensions = List(repr=True)
    strict = Boolean(
        repr=True, help=" if True, all criteria must be matched to select this detector."
    )


DATA_TYPE_DEFINITIONS = tuple(
    [
        TypeDefinition(
            name="MySQL ARCHIVE Storage Engine data files",
            filetypes=("mysql table definition file",),
            extensions=(
                ".arm",
                ".arz",
                ".arn",
            ),
        ),
    ]
)


def is_data(location, definitions=DATA_TYPE_DEFINITIONS):
    """
    Return True isthe file at `location` is a data file.
    """
    if not filetype.is_file(location):
        return False

    T = get_type(location)
    ftype = T.filetype_file.lower()
    mtype = T.mimetype_file.lower()

    for ddef in definitions:
        type_matched = ddef.filetypes and any(t in ftype for t in ddef.filetypes)
        mime_matched = ddef.mimetypes and any(m in mtype for m in ddef.mimetypes)

        exts = ddef.extensions
        if exts:
            extension_matched = exts and location.lower().endswith(exts)

        if TRACE:
            logger_debug("is_data: considering def: %(ddef)r for %(location)s" % locals())
            logger_debug(
                "matched type: %(type_matched)s, mime: %(mime_matched)s, ext: %(extension_matched)s"
                % locals()
            )

        if ddef.strict and not all([type_matched, mime_matched, extension_matched]):
            continue

        if type_matched or mime_matched or extension_matched:
            if TRACE:
                logger_debug("is_data: True: %(location)s: " % locals())
            return True

    return False


def get_pygments_lexer(location):
    """
    Given an input file location, return a Pygments lexer appropriate for
    lexing this file content.
    """
    try:
        T = _registry[location]
        if T.is_binary:
            return
    except KeyError:
        if is_binary(location):
            return

    # We first try to get a lexer using
    #  - the filename
    #  - then the lowercased filename
    #  - and finally the begining of the file content.
    # We try with lowercase as detection is skewed otherwise (e.g. .java vs .JAVA)

    try:
        return get_lexer_for_filename(location)
    except LexerClassNotFound:
        try:
            return get_lexer_for_filename(location.lower())
        except LexerClassNotFound:
            # only try content-based detection if we do not have an extension
            ext = fileutils.file_extension(location)
            if not ext:
                try:
                    # if Pygments does not guess we should not carry forward
                    content = get_text_file_start(location)
                    return guess_lexer(content)
                except LexerClassNotFound:
                    return


def get_text_file_start(location, length=4096):
    """
    Return a unicode string with up the first "length" characters from the text
    file at location.
    """
    content = None
    # read the first 4K of the file
    try:
        with io.open(location, "r") as f:
            content = f.read(length)
    except:
        # try again as bytes and force unicode
        with open(location, "rb") as f:
            content = text.as_unicode(f.read(length))
    finally:
        return content


def get_filetype(location):
    """
    LEGACY: Return the best filetype for location using multiple tools.
    """
    T = get_type(location)
    return T.filetype_file.lower()


def is_standard_include(location):
    """
    Return True if the `location` file path refers to something that looks like
    a standard C/C++ include.
    """
    STD_INCLUDES = (
        "/usr/lib/gcc",
        "/usr/lib",
        "/usr/include",
        "<built-in>",
        "/tmp/glibc-",
    )

    if location.startswith(STD_INCLUDES) or location.endswith(STD_INCLUDES):
        return True
    else:
        return False


def is_binary(location):
    """
    Retrun True if the file at `location` is a binary file.
    """
    known_extensions = (
        ".pyc",
        ".pgm",
        ".mp3",
        ".mp4",
        ".mpeg",
        ".mpg",
        ".emf",
        ".pgm",
        ".pbm",
        ".ppm",
    )
    if location.endswith(known_extensions):
        return True
    return is_binary_string(get_starting_chunk(location))
