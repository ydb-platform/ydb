"""Provides the PackURI value type and known pack-URI strings such as PACKAGE_URI."""

from __future__ import annotations

import posixpath
import re


class PackURI(str):
    """Proxy for a pack URI (partname).

    Provides utility properties the baseURI and the filename slice. Behaves as |str| otherwise.
    """

    _filename_re = re.compile("([a-zA-Z]+)([0-9][0-9]*)?")

    def __new__(cls, pack_uri_str: str):
        if not pack_uri_str[0] == "/":
            raise ValueError(f"PackURI must begin with slash, got {repr(pack_uri_str)}")
        return str.__new__(cls, pack_uri_str)

    @staticmethod
    def from_rel_ref(baseURI: str, relative_ref: str) -> PackURI:
        """Construct an absolute pack URI formed by translating `relative_ref` onto `baseURI`."""
        joined_uri = posixpath.join(baseURI, relative_ref)
        abs_uri = posixpath.abspath(joined_uri)
        return PackURI(abs_uri)

    @property
    def baseURI(self) -> str:
        """The base URI of this pack URI; the directory portion, roughly speaking.

        E.g. `"/ppt/slides"` for `"/ppt/slides/slide1.xml"`.

        For the package pseudo-partname "/", the baseURI is "/".
        """
        return posixpath.split(self)[0]

    @property
    def ext(self) -> str:
        """The extension portion of this pack URI.

        E.g. `"xml"` for `"/ppt/slides/slide1.xml"`. Note the leading period is not included.
        """
        # -- raw_ext is either empty string or starts with period, e.g. ".xml" --
        raw_ext = posixpath.splitext(self)[1]
        return raw_ext[1:] if raw_ext.startswith(".") else raw_ext

    @property
    def filename(self) -> str:
        """The "filename" portion of this pack URI.

        E.g. `"slide1.xml"` for `"/ppt/slides/slide1.xml"`.

        For the package pseudo-partname "/", `filename` is ''.
        """
        return posixpath.split(self)[1]

    @property
    def idx(self) -> int | None:
        """Optional int partname index.

        Value is an integer for an "array" partname or None for singleton partname, e.g. `21` for
        `"/ppt/slides/slide21.xml"` and |None| for `"/ppt/presentation.xml"`.
        """
        filename = self.filename
        if not filename:
            return None
        name_part = posixpath.splitext(filename)[0]  # filename w/ext removed
        match = self._filename_re.match(name_part)
        if match is None:
            return None
        if match.group(2):
            return int(match.group(2))
        return None

    @property
    def membername(self) -> str:
        """The pack URI with the leading slash stripped off.

        This is the form used as the Zip file membername for the package item. Returns "" for the
        package pseudo-partname "/".
        """
        return self[1:]

    def relative_ref(self, baseURI: str) -> str:
        """Return string containing relative reference to package item from `baseURI`.

        E.g. PackURI("/ppt/slideLayouts/slideLayout1.xml") would return
        "../slideLayouts/slideLayout1.xml" for baseURI "/ppt/slides".
        """
        # workaround for posixpath bug in 2.6, doesn't generate correct
        # relative path when `start` (second) parameter is root ("/")
        return self[1:] if baseURI == "/" else posixpath.relpath(self, baseURI)

    @property
    def rels_uri(self) -> PackURI:
        """The pack URI of the .rels part corresponding to the current pack URI.

        Only produces sensible output if the pack URI is a partname or the package pseudo-partname
        "/".
        """
        rels_filename = "%s.rels" % self.filename
        rels_uri_str = posixpath.join(self.baseURI, "_rels", rels_filename)
        return PackURI(rels_uri_str)


PACKAGE_URI = PackURI("/")
CONTENT_TYPES_URI = PackURI("/[Content_Types].xml")
