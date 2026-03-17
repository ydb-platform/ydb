# encoding: utf-8

"""Fundamental Open Packaging Convention (OPC) objects.

The :mod:`pptx.packaging` module coheres around the concerns of reading and writing
presentations to and from a .pptx file.
"""

import collections

from pptx.compat import is_string, Mapping
from pptx.opc.constants import RELATIONSHIP_TARGET_MODE as RTM, RELATIONSHIP_TYPE as RT
from pptx.opc.oxml import CT_Relationships, serialize_part_xml
from pptx.opc.packuri import CONTENT_TYPES_URI, PACKAGE_URI, PackURI
from pptx.opc.serialized import PackageReader, PackageWriter
from pptx.opc.shared import CaseInsensitiveDict
from pptx.oxml import parse_xml
from pptx.util import lazyproperty


class _RelatableMixin(object):
    """Provide relationship methods required by both the package and each part."""

    def part_related_by(self, reltype):
        """Return (single) part having relationship to this package of `reltype`.

        Raises |KeyError| if no such relationship is found and |ValueError| if more than
        one such relationship is found.
        """
        return self._rels.part_with_reltype(reltype)

    def relate_to(self, target, reltype, is_external=False):
        """Return rId key of relationship of `reltype` to `target`.

        If such a relationship already exists, its rId is returned. Otherwise the
        relationship is added and its new rId returned.
        """
        return (
            self._rels.get_or_add_ext_rel(reltype, target)
            if is_external
            else self._rels.get_or_add(reltype, target)
        )

    def related_part(self, rId):
        """Return related |Part| subtype identified by `rId`."""
        return self._rels[rId].target_part

    def target_ref(self, rId):
        """Return URL contained in target ref of relationship identified by `rId`."""
        return self._rels[rId].target_ref

    @lazyproperty
    def _rels(self):
        """|Relationships| object containing relationships from this part to others."""
        raise NotImplementedError(  # pragma: no cover
            "`%s` must implement `.rels`" % type(self).__name__
        )


class OpcPackage(_RelatableMixin):
    """Main API class for |python-opc|.

    A new instance is constructed by calling the :meth:`open` classmethod with a path
    to a package file or file-like object containing a package (.pptx file).
    """

    def __init__(self, pkg_file):
        self._pkg_file = pkg_file

    @classmethod
    def open(cls, pkg_file):
        """Return an |OpcPackage| instance loaded with the contents of `pkg_file`."""
        return cls(pkg_file)._load()

    def drop_rel(self, rId):
        """Remove relationship identified by `rId`."""
        self._rels.pop(rId)

    def iter_parts(self):
        """Generate exactly one reference to each part in the package."""
        visited = set()
        for rel in self.iter_rels():
            if rel.is_external:
                continue
            part = rel.target_part
            if part in visited:
                continue
            yield part
            visited.add(part)

    def iter_rels(self):
        """Generate exactly one reference to each relationship in package.

        Performs a depth-first traversal of the rels graph.
        """
        visited = set()

        def walk_rels(rels):
            for rel in rels.values():
                yield rel
                # --- external items can have no relationships ---
                if rel.is_external:
                    continue
                # --- all relationships other than those for the package belong to a
                # --- part. Once that part has been processed, processing it again
                # --- would lead to the same relationships appearing more than once.
                part = rel.target_part
                if part in visited:
                    continue
                visited.add(part)
                # --- recurse into relationships of each unvisited target-part ---
                for rel in walk_rels(part.rels):
                    yield rel

        for rel in walk_rels(self._rels):
            yield rel

    @property
    def main_document_part(self):
        """Return |Part| subtype serving as the main document part for this package.

        In this case it will be a |Presentation| part.
        """
        return self.part_related_by(RT.OFFICE_DOCUMENT)

    def next_partname(self, tmpl):
        """Return |PackURI| next available partname matching `tmpl`.

        `tmpl` is a printf (%)-style template string containing a single replacement
        item, a '%d' to be used to insert the integer portion of the partname.
        Example: '/ppt/slides/slide%d.xml'
        """
        # --- expected next partname is tmpl % n where n is one greater than the number
        # --- of existing partnames that match tmpl. Speed up finding the next one
        # --- (maybe) by searching from the end downward rather than from 1 upward.
        prefix = tmpl[: (tmpl % 42).find("42")]
        partnames = set(
            p.partname for p in self.iter_parts() if p.partname.startswith(prefix)
        )
        for n in range(len(partnames) + 1, 0, -1):
            candidate_partname = tmpl % n
            if candidate_partname not in partnames:
                return PackURI(candidate_partname)
        raise Exception(  # pragma: no cover
            "ProgrammingError: ran out of candidate_partnames"
        )

    def save(self, pkg_file):
        """Save this package to `pkg_file`.

        `file` can be either a path to a file (a string) or a file-like object.
        """
        PackageWriter.write(pkg_file, self._rels, tuple(self.iter_parts()))

    def _load(self):
        """Return the package after loading all parts and relationships."""
        pkg_xml_rels, parts = _PackageLoader.load(self._pkg_file, self)
        self._rels.load_from_xml(PACKAGE_URI, pkg_xml_rels, parts)
        return self

    @lazyproperty
    def _rels(self):
        """|Relationships| object containing relationships of this package."""
        return _Relationships(PACKAGE_URI.baseURI)


class _PackageLoader(object):
    """Function-object that loads a package from disk (or other store)."""

    def __init__(self, pkg_file, package):
        self._pkg_file = pkg_file
        self._package = package

    @classmethod
    def load(cls, pkg_file, package):
        """Return (pkg_xml_rels, parts) pair resulting from loading `pkg_file`.

        The returned `parts` value is a {partname: part} mapping with each part in the
        package included and constructed complete with its relationships to other parts
        in the package.

        The returned `pkg_xml_rels` value is a `CT_Relationships` object containing the
        parsed package relationships. It is the caller's responsibility (the package
        object) to load those relationships into its |_Relationships| object.
        """
        return cls(pkg_file, package)._load()

    def _load(self):
        """Return (pkg_xml_rels, parts) pair resulting from loading pkg_file."""
        parts, xml_rels = self._parts, self._xml_rels

        for partname, part in parts.items():
            part.load_rels_from_xml(xml_rels[partname], parts)

        return xml_rels["/"], parts

    @lazyproperty
    def _content_types(self):
        """|_ContentTypeMap| object providing content-types for items of this package.

        Provides a content-type (MIME-type) for any given partname.
        """
        return _ContentTypeMap.from_xml(self._package_reader[CONTENT_TYPES_URI])

    @lazyproperty
    def _package_reader(self):
        """|PackageReader| object providing access to package-items in pkg_file."""
        return PackageReader(self._pkg_file)

    @lazyproperty
    def _parts(self):
        """dict {partname: Part} populated with parts loading from package.

        Among other duties, this collection is passed to each relationships collection
        so each relationship can resolve a reference to its target part when required.
        This reference can only be reliably carried out once the all parts have been
        loaded.
        """
        content_types = self._content_types
        package = self._package
        package_reader = self._package_reader

        return {
            partname: PartFactory(
                partname,
                content_types[partname],
                package,
                blob=package_reader[partname],
            )
            for partname in (p for p in self._xml_rels.keys() if p != "/")
            # --- invalid partnames can arise in some packages; ignore those rather
            # --- than raise an exception.
            if partname in package_reader
        }

    @lazyproperty
    def _xml_rels(self):
        """dict {partname: xml_rels} for package and all package parts.

        This is used as the basis for other loading operations such as loading parts and
        populating their relationships.
        """
        xml_rels = {}
        visited_partnames = set()

        def load_rels(source_partname, rels):
            """Populate `xml_rels` dict by traversing relationships depth-first."""
            xml_rels[source_partname] = rels
            visited_partnames.add(source_partname)
            base_uri = source_partname.baseURI

            # --- recursion stops when there are no unvisited partnames in rels ---
            for rel in rels:
                if rel.targetMode == RTM.EXTERNAL:
                    continue
                target_partname = PackURI.from_rel_ref(base_uri, rel.target_ref)
                if target_partname in visited_partnames:
                    continue
                load_rels(target_partname, self._xml_rels_for(target_partname))

        load_rels(PACKAGE_URI, self._xml_rels_for(PACKAGE_URI))
        return xml_rels

    def _xml_rels_for(self, partname):
        """Return CT_Relationships object formed by parsing rels XML for `partname`.

        A CT_Relationships object is returned in all cases. A part that has no
        relationships receives an "empty" CT_Relationships object, i.e. containing no
        `CT_Relationship` objects.
        """
        rels_xml = self._package_reader.rels_xml_for(partname)
        return CT_Relationships.new() if rels_xml is None else parse_xml(rels_xml)


class Part(_RelatableMixin):
    """Base class for package parts.

    Provides common properties and methods, but intended to be subclassed in client code
    to implement specific part behaviors. Also serves as the default class for parts
    that are not yet given specific behaviors.
    """

    def __init__(self, partname, content_type, package, blob=None):
        # --- XmlPart subtypes, don't store a blob (the original XML) ---
        self._partname = partname
        self._content_type = content_type
        self._package = package
        self._blob = blob

    @classmethod
    def load(cls, partname, content_type, package, blob):
        """Return `cls` instance loaded from arguments.

        This one is a straight pass-through, but subtypes may do some pre-processing,
        see XmlPart for an example.
        """
        return cls(partname, content_type, package, blob)

    @property
    def blob(self):
        """Contents of this package part as a sequence of bytes.

        May be text (XML generally) or binary. Intended to be overridden by subclasses.
        Default behavior is to return the blob initial loaded during `Package.open()`
        operation.
        """
        return self._blob

    @blob.setter
    def blob(self, bytes_):
        """Note that not all subclasses use the part blob as their blob source.

        In particular, the |XmlPart| subclass uses its `self._element` to serialize a
        blob on demand. This works fine for binary parts though.
        """
        self._blob = bytes_

    @lazyproperty
    def content_type(self):
        """Content-type (MIME-type) of this part."""
        return self._content_type

    def drop_rel(self, rId):
        """Remove relationship identified by `rId` if its reference count is under 2.

        Relationships with a reference count of 0 are implicit relationships. Note that
        only XML parts can drop relationships.
        """
        if self._rel_ref_count(rId) < 2:
            self._rels.pop(rId)

    def load_rels_from_xml(self, xml_rels, parts):
        """load _Relationships for this part from `xml_rels`.

        Part references are resolved using the `parts` dict that maps each partname to
        the loaded part with that partname. These relationships are loaded from a
        serialized package and so already have assigned rIds. This method is only used
        during package loading.
        """
        self._rels.load_from_xml(self._partname.baseURI, xml_rels, parts)

    @lazyproperty
    def package(self):
        """|OpcPackage| instance this part belongs to."""
        return self._package

    @property
    def partname(self):
        """|PackURI| partname for this part, e.g. "/ppt/slides/slide1.xml"."""
        return self._partname

    @partname.setter
    def partname(self, partname):
        if not isinstance(partname, PackURI):
            raise TypeError(  # pragma: no cover
                "partname must be instance of PackURI, got '%s'"
                % type(partname).__name__
            )
        self._partname = partname

    @lazyproperty
    def rels(self):
        """|Relationships| collection of relationships from this part to other parts."""
        # --- this must be public to allow the part graph to be traversed ---
        return self._rels

    def _blob_from_file(self, file):
        """Return bytes of `file`, which is either a str path or a file-like object."""
        # --- a str `file` is assumed to be a path ---
        if is_string(file):
            with open(file, "rb") as f:
                return f.read()

        # --- otherwise, assume `file` is a file-like object
        # --- reposition file cursor if it has one
        if callable(getattr(file, "seek")):
            file.seek(0)
        return file.read()

    def _rel_ref_count(self, rId):
        """Return int count of references in this part's XML to `rId`."""
        return len([r for r in self._element.xpath("//@r:id") if r == rId])

    @lazyproperty
    def _rels(self):
        """|Relationships| object containing relationships from this part to others."""
        return _Relationships(self._partname.baseURI)


class XmlPart(Part):
    """Base class for package parts containing an XML payload, which is most of them.

    Provides additional methods to the |Part| base class that take care of parsing and
    reserializing the XML payload and managing relationships to other parts.
    """

    def __init__(self, partname, content_type, package, element):
        super(XmlPart, self).__init__(partname, content_type, package)
        self._element = element

    @classmethod
    def load(cls, partname, content_type, package, blob):
        """Return instance of `cls` loaded with parsed XML from `blob`."""
        return cls(partname, content_type, package, element=parse_xml(blob))

    @property
    def blob(self):
        """bytes XML serialization of this part."""
        return serialize_part_xml(self._element)

    @property
    def part(self):
        """This part.

        This is part of the parent protocol, "children" of the document will not know
        the part that contains them so must ask their parent object. That chain of
        delegation ends here for child objects.
        """
        return self


class PartFactory(object):
    """Constructs a registered subtype of |Part|.

    Client code can register a subclass of |Part| to be used for a package blob based on
    its content type.
    """

    part_type_for = {}

    def __new__(cls, partname, content_type, package, blob):
        PartClass = cls._part_cls_for(content_type)
        return PartClass.load(partname, content_type, package, blob)

    @classmethod
    def _part_cls_for(cls, content_type):
        """Return the custom part class registered for `content_type`.

        Returns |Part| if no custom class is registered for `content_type`.
        """
        if content_type in cls.part_type_for:
            return cls.part_type_for[content_type]
        return Part


class _ContentTypeMap(object):
    """Value type providing dict semantics for looking up content type by partname."""

    def __init__(self, overrides, defaults):
        self._overrides = overrides
        self._defaults = defaults

    def __getitem__(self, partname):
        """Return content-type (MIME-type) for part identified by *partname*."""
        if not isinstance(partname, PackURI):
            raise TypeError(
                "_ContentTypeMap key must be <type 'PackURI'>, got %s"
                % type(partname).__name__
            )

        if partname in self._overrides:
            return self._overrides[partname]

        if partname.ext in self._defaults:
            return self._defaults[partname.ext]

        raise KeyError(
            "no content-type for partname '%s' in [Content_Types].xml" % partname
        )

    @classmethod
    def from_xml(cls, content_types_xml):
        """Return |_ContentTypeMap| instance populated from `content_types_xml`."""
        types_elm = parse_xml(content_types_xml)
        overrides = CaseInsensitiveDict(
            (o.partName.lower(), o.contentType) for o in types_elm.override_lst
        )
        defaults = CaseInsensitiveDict(
            (d.extension.lower(), d.contentType) for d in types_elm.default_lst
        )
        return cls(overrides, defaults)


class _Relationships(Mapping):
    """Collection of |_Relationship| instances having `dict` semantics.

    Relationships are keyed by their rId, but may also be found in other ways, such as
    by their relationship type. |Relationship| objects are keyed by their rId.

    Iterating this collection has normal mapping semantics, generating the keys (rIds)
    of the mapping. `rels.keys()`, `rels.values()`, and `rels.items() can be used as
    they would be for a `dict`.
    """

    def __init__(self, base_uri):
        self._base_uri = base_uri

    def __contains__(self, rId):
        """Implement 'in' operation, like `"rId7" in relationships`."""
        return rId in self._rels

    def __getitem__(self, rId):
        """Implement relationship lookup by rId using indexed access, like rels[rId]."""
        try:
            return self._rels[rId]
        except KeyError:
            raise KeyError("no relationship with key '%s'" % rId)

    def __iter__(self):
        """Implement iteration of rIds (iterating a mapping produces its keys)."""
        return iter(self._rels)

    def __len__(self):
        """Return count of relationships in collection."""
        return len(self._rels)

    def get_or_add(self, reltype, target_part):
        """Return str rId of `reltype` to `target_part`.

        The rId of an existing matching relationship is used if present. Otherwise, a
        new relationship is added and that rId is returned.
        """
        existing_rId = self._get_matching(reltype, target_part)
        return (
            self._add_relationship(reltype, target_part)
            if existing_rId is None
            else existing_rId
        )

    def get_or_add_ext_rel(self, reltype, target_ref):
        """Return str rId of external relationship of `reltype` to `target_ref`.

        The rId of an existing matching relationship is used if present. Otherwise, a
        new relationship is added and that rId is returned.
        """
        existing_rId = self._get_matching(reltype, target_ref, is_external=True)
        return (
            self._add_relationship(reltype, target_ref, is_external=True)
            if existing_rId is None
            else existing_rId
        )

    def load_from_xml(self, base_uri, xml_rels, parts):
        """Replace any relationships in this collection with those from `xml_rels`."""

        def iter_valid_rels():
            """Filter out broken relationships such as those pointing to NULL."""
            for rel_elm in xml_rels.relationship_lst:
                # --- Occasionally a PowerPoint plugin or other client will "remove"
                # --- a relationship simply by "voiding" its Target value, like making
                # --- it "/ppt/slides/NULL". Skip any relationships linking to a
                # --- partname that is not present in the package.
                if rel_elm.targetMode == RTM.INTERNAL:
                    partname = PackURI.from_rel_ref(base_uri, rel_elm.target_ref)
                    if partname not in parts:
                        continue
                yield _Relationship.from_xml(base_uri, rel_elm, parts)

        self._rels.clear()
        self._rels.update((rel.rId, rel) for rel in iter_valid_rels())

    def part_with_reltype(self, reltype):
        """Return target part of relationship with matching `reltype`.

        Raises |KeyError| if not found and |ValueError| if more than one matching
        relationship is found.
        """
        rels_of_reltype = self._rels_by_reltype[reltype]

        if len(rels_of_reltype) == 0:
            raise KeyError("no relationship of type '%s' in collection" % reltype)

        if len(rels_of_reltype) > 1:
            raise ValueError(
                "multiple relationships of type '%s' in collection" % reltype
            )

        return rels_of_reltype[0].target_part

    def pop(self, rId):
        """Return |Relationship| identified by `rId` after removing it from collection.

        The caller is responsible for ensuring it is no longer required.
        """
        return self._rels.pop(rId)

    @property
    def xml(self):
        """bytes XML serialization of this relationship collection.

        This value is suitable for storage as a .rels file in an OPC package. Includes
        a `<?xml` header with encoding as UTF-8.
        """
        rels_elm = CT_Relationships.new()

        # -- Sequence <Relationship> elements deterministically (in numerical order) to
        # -- simplify testing and manual inspection.
        def iter_rels_in_numerical_order():
            sorted_num_rId_pairs = sorted(
                (
                    int(rId[3:]) if rId.startswith("rId") and rId[3:].isdigit() else 0,
                    rId,
                )
                for rId in self.keys()
            )
            return (self[rId] for _, rId in sorted_num_rId_pairs)

        for rel in iter_rels_in_numerical_order():
            rels_elm.add_rel(rel.rId, rel.reltype, rel.target_ref, rel.is_external)

        return rels_elm.xml

    def _add_relationship(self, reltype, target, is_external=False):
        """Return str rId of |_Relationship| newly added to spec."""
        rId = self._next_rId
        self._rels[rId] = _Relationship(
            self._base_uri,
            rId,
            reltype,
            target_mode=RTM.EXTERNAL if is_external else RTM.INTERNAL,
            target=target,
        )
        return rId

    def _get_matching(self, reltype, target, is_external=False):
        """Return optional str rId of rel of `reltype`, `target`, and `is_external`.

        Returns `None` on no matching relationship
        """
        for rel in self._rels_by_reltype[reltype]:
            if rel.is_external != is_external:
                continue
            rel_target = rel.target_ref if rel.is_external else rel.target_part
            if rel_target != target:
                continue
            return rel.rId

        return None

    @property
    def _next_rId(self):
        """Next str rId available in collection.

        The next rId is the first unused key starting from "rId1" and making use of any
        gaps in numbering, e.g. 'rId2' for rIds ['rId1', 'rId3'].
        """
        # --- The common case is where all sequential numbers starting at "rId1" are
        # --- used and the next available rId is "rId%d" % (len(rels)+1). So we start
        # --- there and count down to produce the best performance.
        for n in range(len(self) + 1, 0, -1):
            rId_candidate = "rId%d" % n  # like 'rId19'
            if rId_candidate not in self._rels:
                return rId_candidate

    @lazyproperty
    def _rels(self):
        """dict {rId: _Relationship} containing relationships of this collection."""
        return dict()

    @property
    def _rels_by_reltype(self):
        """defaultdict {reltype: [rels]} for all relationships in collection."""
        D = collections.defaultdict(list)
        for rel in self.values():
            D[rel.reltype].append(rel)
        return D


class _Relationship(object):
    """Value object describing link from a part or package to another part."""

    def __init__(self, base_uri, rId, reltype, target_mode, target):
        self._base_uri = base_uri
        self._rId = rId
        self._reltype = reltype
        self._target_mode = target_mode
        self._target = target

    @classmethod
    def from_xml(cls, base_uri, rel, parts):
        """Return |_Relationship| object based on CT_Relationship element `rel`."""
        target = (
            rel.target_ref
            if rel.targetMode == RTM.EXTERNAL
            else parts[PackURI.from_rel_ref(base_uri, rel.target_ref)]
        )
        return cls(base_uri, rel.rId, rel.reltype, rel.targetMode, target)

    @lazyproperty
    def is_external(self):
        """True if target_mode is `RTM.EXTERNAL`.

        An external relationship is a link to a resource outside the package, such as
        a web-resource (URL).
        """
        return self._target_mode == RTM.EXTERNAL

    @lazyproperty
    def reltype(self):
        """Member of RELATIONSHIP_TYPE describing relationship of target to source."""
        return self._reltype

    @lazyproperty
    def rId(self):
        """str relationship-id, like 'rId9'.

        Corresponds to the `Id` attribute on the `CT_Relationship` element and
        uniquely identifies this relationship within its peers for the source-part or
        package.
        """
        return self._rId

    @lazyproperty
    def target_part(self):
        """|Part| or subtype referred to by this relationship."""
        if self.is_external:
            raise ValueError(
                "`.target_part` property on _Relationship is undefined when "
                "target-mode is external"
            )
        return self._target

    @lazyproperty
    def target_partname(self):
        """|PackURI| instance containing partname targeted by this relationship.

        Raises `ValueError` on reference if target_mode is external. Use
        :attr:`target_mode` to check before referencing.
        """
        if self.is_external:
            raise ValueError(
                "`.target_partname` property on _Relationship is undefined when "
                "target-mode is external"
            )
        return self._target.partname

    @lazyproperty
    def target_ref(self):
        """str reference to relationship target.

        For internal relationships this is the relative partname, suitable for
        serialization purposes. For an external relationship it is typically a URL.
        """
        return (
            self._target
            if self.is_external
            else self.target_partname.relative_ref(self._base_uri)
        )
