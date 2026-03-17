# encoding: utf-8

"""API for reading/writing serialized Open Packaging Convention (OPC) package."""

import os
import posixpath
import zipfile

from pptx.compat import Container, is_string
from pptx.exceptions import PackageNotFoundError
from pptx.opc.constants import CONTENT_TYPE as CT
from pptx.opc.oxml import CT_Types, serialize_part_xml
from pptx.opc.packuri import CONTENT_TYPES_URI, PACKAGE_URI, PackURI
from pptx.opc.shared import CaseInsensitiveDict
from pptx.opc.spec import default_content_types
from pptx.util import lazyproperty


class PackageReader(Container):
    """Provides access to package-parts of an OPC package with dict semantics.

    The package may be in zip-format (a .pptx file) or expanded into a directory
    structure, perhaps by unzipping a .pptx file.
    """

    def __init__(self, pkg_file):
        self._pkg_file = pkg_file

    def __contains__(self, pack_uri):
        """Return True when part identified by `pack_uri` is present in package."""
        return pack_uri in self._blob_reader

    def __getitem__(self, pack_uri):
        """Return bytes for part corresponding to `pack_uri`."""
        return self._blob_reader[pack_uri]

    def rels_xml_for(self, partname):
        """Return optional rels item XML for `partname`.

        Returns `None` if no rels item is present for `partname`. `partname` is a
        |PackURI| instance.
        """
        blob_reader, uri = self._blob_reader, partname.rels_uri
        return blob_reader[uri] if uri in blob_reader else None

    @lazyproperty
    def _blob_reader(self):
        """|_PhysPkgReader| subtype providing read access to the package file."""
        return _PhysPkgReader.factory(self._pkg_file)


class PackageWriter(object):
    """Writes a zip-format OPC package to `pkg_file`.

    `pkg_file` can be either a path to a zip file (a string) or a file-like object.
    `pkg_rels` is the |_Relationships| object containing relationships for the package.
    `parts` is a sequence of |Part| subtype instance to be written to the package.

    Its single API classmethod is :meth:`write`. This class is not intended to be
    instantiated.
    """

    def __init__(self, pkg_file, pkg_rels, parts):
        self._pkg_file = pkg_file
        self._pkg_rels = pkg_rels
        self._parts = parts

    @classmethod
    def write(cls, pkg_file, pkg_rels, parts):
        """Write a physical package (.pptx file) to `pkg_file`.

        The serialized package contains `pkg_rels` and `parts`, a content-types stream
        based on the content type of each part, and a .rels file for each part that has
        relationships.
        """
        cls(pkg_file, pkg_rels, parts)._write()

    def _write(self):
        """Write physical package (.pptx file)."""
        with _PhysPkgWriter.factory(self._pkg_file) as phys_writer:
            self._write_content_types_stream(phys_writer)
            self._write_pkg_rels(phys_writer)
            self._write_parts(phys_writer)

    def _write_content_types_stream(self, phys_writer):
        """Write `[Content_Types].xml` part to the physical package.

        This part must contain an appropriate content type lookup target for each part
        in the package.
        """
        phys_writer.write(
            CONTENT_TYPES_URI,
            serialize_part_xml(_ContentTypesItem.xml_for(self._parts)),
        )

    def _write_parts(self, phys_writer):
        """Write blob of each part in `parts` to the package.

        A rels item for each part is also written when the part has relationships.
        """
        for part in self._parts:
            phys_writer.write(part.partname, part.blob)
            if part._rels:
                phys_writer.write(part.partname.rels_uri, part.rels.xml)

    def _write_pkg_rels(self, phys_writer):
        """Write the XML rels item for *pkg_rels* ('/_rels/.rels') to the package."""
        phys_writer.write(PACKAGE_URI.rels_uri, self._pkg_rels.xml)


class _PhysPkgReader(Container):
    """Base class for physical package reader objects."""

    def __contains__(self, item):
        """Must be implemented by each subclass."""
        raise NotImplementedError(  # pragma: no cover
            "`%s` must implement `.__contains__()`" % type(self).__name__
        )

    @classmethod
    def factory(cls, pkg_file):
        """Return |_PhysPkgReader| subtype instance appropriage for `pkg_file`."""
        # --- for pkg_file other than str, assume it's a stream and pass it to Zip
        # --- reader to sort out
        if not is_string(pkg_file):
            return _ZipPkgReader(pkg_file)

        # --- otherwise we treat `pkg_file` as a path ---
        if os.path.isdir(pkg_file):
            return _DirPkgReader(pkg_file)

        if zipfile.is_zipfile(pkg_file):
            return _ZipPkgReader(pkg_file)

        raise PackageNotFoundError("Package not found at '%s'" % pkg_file)


class _DirPkgReader(_PhysPkgReader):
    """Implements |PhysPkgReader| interface for OPC package extracted into directory.

    `path` is the path to a directory containing an expanded package.
    """

    def __init__(self, path):
        self._path = os.path.abspath(path)

    def __contains__(self, pack_uri):
        """Return True when part identified by `pack_uri` is present in zip archive."""
        return os.path.exists(posixpath.join(self._path, pack_uri.membername))

    def __getitem__(self, pack_uri):
        """Return bytes of file corresponding to `pack_uri` in package directory."""
        path = os.path.join(self._path, pack_uri.membername)
        try:
            with open(path, "rb") as f:
                return f.read()
        except IOError:
            raise KeyError("no member '%s' in package" % pack_uri)


class _ZipPkgReader(_PhysPkgReader):
    """Implements |PhysPkgReader| interface for a zip-file OPC package."""

    def __init__(self, pkg_file):
        self._pkg_file = pkg_file

    def __contains__(self, pack_uri):
        """Return True when part identified by `pack_uri` is present in zip archive."""
        return pack_uri in self._blobs

    def __getitem__(self, pack_uri):
        """Return bytes for part corresponding to `pack_uri`.

        Raises |KeyError| if no matching member is present in zip archive.
        """
        if pack_uri not in self._blobs:
            raise KeyError("no member '%s' in package" % pack_uri)
        return self._blobs[pack_uri]

    @lazyproperty
    def _blobs(self):
        """dict mapping partname to package part binaries."""
        with zipfile.ZipFile(self._pkg_file, "r") as z:
            return {PackURI("/%s" % name): z.read(name) for name in z.namelist()}


class _PhysPkgWriter(object):
    """Base class for physical package writer objects."""

    @classmethod
    def factory(cls, pkg_file):
        """Return |_PhysPkgWriter| subtype instance appropriage for `pkg_file`.

        Currently the only subtype is `_ZipPkgWriter`, but a `_DirPkgWriter` could be
        implemented or even a `_StreamPkgWriter`.
        """
        return _ZipPkgWriter(pkg_file)


class _ZipPkgWriter(_PhysPkgWriter):
    """Implements |PhysPkgWriter| interface for a zip-file (.pptx file) OPC package."""

    def __init__(self, pkg_file):
        self._pkg_file = pkg_file

    def __enter__(self):
        """Enable use as a context-manager. Opening zip for writing happens here."""
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        """Close the zip archive on exit from context.

        Closing flushes any pending physical writes and releasing any resources it's
        using.
        """
        self._zipf.close()

    def write(self, pack_uri, blob):
        """Write `blob` to zip package with membername corresponding to `pack_uri`."""
        self._zipf.writestr(pack_uri.membername, blob)

    @lazyproperty
    def _zipf(self):
        """`ZipFile` instance open for writing."""
        return zipfile.ZipFile(self._pkg_file, "w", compression=zipfile.ZIP_DEFLATED)


class _ContentTypesItem(object):
    """Composes content-types "part" ([Content_Types].xml) for a collection of parts."""

    def __init__(self, parts):
        self._parts = parts

    @classmethod
    def xml_for(cls, parts):
        """Return content-types XML mapping each part in `parts` to a content-type.

        The resulting XML is suitable for storage as `[Content_Types].xml` in an OPC
        package.
        """
        return cls(parts)._xml

    @lazyproperty
    def _xml(self):
        """lxml.etree._Element containing the content-types item.

        This XML object is suitable for serialization to the `[Content_Types].xml` item
        for an OPC package. Although the sequence of elements is not strictly
        significant, as an aid to testing and readability Default elements are sorted by
        extension and Override elements are sorted by partname.
        """
        defaults, overrides = self._defaults_and_overrides
        _types_elm = CT_Types.new()

        for ext, content_type in sorted(defaults.items()):
            _types_elm.add_default(ext, content_type)
        for partname, content_type in sorted(overrides.items()):
            _types_elm.add_override(partname, content_type)

        return _types_elm

    @lazyproperty
    def _defaults_and_overrides(self):
        """pair of dict (defaults, overrides) accounting for all parts.

        `defaults` is {ext: content_type} and overrides is {partname: content_type}.
        """
        defaults = CaseInsensitiveDict(rels=CT.OPC_RELATIONSHIPS, xml=CT.XML)
        overrides = dict()

        for part in self._parts:
            partname, content_type = part.partname, part.content_type
            ext = partname.ext
            if (ext.lower(), content_type) in default_content_types:
                defaults[ext] = content_type
            else:
                overrides[partname] = content_type

        return defaults, overrides
