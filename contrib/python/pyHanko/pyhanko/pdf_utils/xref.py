"""
Internal utilities to handle the processing of cross-reference data and
document trailer data.

This entire module is considered internal API.
"""

import enum
import logging
import os
import struct
from dataclasses import dataclass
from io import BytesIO
from itertools import chain
from typing import Dict, Iterator, List, Optional, Set, Tuple, Union

from pyhanko.pdf_utils import generic, misc
from pyhanko.pdf_utils.generic import EncryptedObjAccess, pdf_name
from pyhanko.pdf_utils.misc import PdfReadError, PdfStrictReadError, peek
from pyhanko.pdf_utils.rw_common import PdfHandler

__all__ = [
    'XRefCache',
    'XRefBuilder',
    'XRefType',
    'XRefEntry',
    'ObjStreamRef',
    'ObjectHeaderReadError',
    'XRefSection',
    'XRefSectionData',
    'XRefSectionType',
    'XRefSectionMetaInfo',
    'TrailerDictionary',
    'read_object_header',
    'parse_xref_stream',
    'parse_xref_table',
    'write_xref_table',
    'ObjectStream',
    'XRefStream',
    'OBJSTREAM_FORBIDDEN',
    'PositionDict',
]

logger = logging.getLogger(__name__)


@enum.unique
class XRefType(enum.Enum):
    """
    Different types of cross-reference entries.
    """

    FREE = enum.auto()
    """
    A freeing instruction.
    """

    STANDARD = enum.auto()
    """
    A regular top-level object.
    """

    IN_OBJ_STREAM = enum.auto()
    """
    An object that is part of an object stream.
    """


@dataclass(frozen=True)
class ObjStreamRef:
    """
    Identifies an object that's part of an object stream.
    """

    obj_stream_id: int
    """
    The ID number of the object stream (its generation number is presumed zero).
    """

    ix_in_stream: int
    """
    The index of the object in the stream.
    """


@dataclass(frozen=True)
class XRefEntry:
    """
    Value type representing a single cross-reference entry.
    """

    xref_type: XRefType
    """
    The type of cross-reference entry.
    """

    location: Optional[Union[int, ObjStreamRef]]
    """
    Location the cross-reference points to.
    """

    idnum: int
    """
    The ID of the object being referenced.
    """

    generation: int = 0
    """
    The generation number of the object being referenced.
    """


def parse_xref_table(stream) -> Iterator[XRefEntry]:
    """
    Parse a single cross-reference table and yield its entries one by one.

    This is internal API.

    :param stream:
        A file-like object pointed to the start of the cross-reference table.
    :return:
        A generator object yielding :class:`.XRefEntry` objects.
    """

    misc.read_non_whitespace(stream)
    stream.seek(-1, os.SEEK_CUR)
    while True:
        num = generic.NumberObject.read_from_stream(stream)
        misc.read_non_whitespace(stream)
        stream.seek(-1, os.SEEK_CUR)
        size = generic.NumberObject.read_from_stream(stream)
        misc.read_non_whitespace(stream)
        stream.seek(-1, os.SEEK_CUR)
        for cnt in range(0, size):
            line = stream.read(20)

            # It's very clear in section 3.4.3 of the PDF spec
            # that all cross-reference table lines are a fixed
            # 20 bytes (as of PDF 1.7). However, some files have
            # 21-byte entries (or more) due to the use of \r\n
            # (CRLF) EOL's. Detect that case, and adjust the line
            # until it does not begin with a \r (CR) or \n (LF).
            while line[0] in b"\x0D\x0A":
                stream.seek(-20 + 1, os.SEEK_CUR)
                line = stream.read(20)

            # On the other hand, some malformed PDF files
            # use a single character EOL without a preceding
            # space.  Detect that case, and seek the stream
            # back one character.  (0-9 means we've bled into
            # the next xref entry, t means we've bled into the
            # text "trailer"):
            if line[-1] in b"0123456789t":
                stream.seek(-1, os.SEEK_CUR)

            offset, generation, marker = line[:18].split(b" ")
            if marker == b'n':
                yield XRefEntry(
                    xref_type=XRefType.STANDARD,
                    location=int(offset),
                    idnum=num,
                    generation=int(generation),
                )
            elif marker == b'f':
                yield XRefEntry(
                    xref_type=XRefType.FREE,
                    location=None,
                    idnum=num,
                    generation=int(generation),
                )
            num += 1
        misc.read_non_whitespace(stream)
        stream.seek(-1, os.SEEK_CUR)
        trailertag = stream.read(7)
        if trailertag == b"trailer":
            # we're done, finish by reading to the first non-whitespace char
            misc.read_non_whitespace(stream)
            stream.seek(-1, os.SEEK_CUR)
            return
        else:
            # more xrefs!
            stream.seek(-7, os.SEEK_CUR)


def parse_xref_stream(
    xref_stream: generic.StreamObject, strict: bool = True
) -> Iterator[XRefEntry]:
    """
    Parse a single cross-reference stream and yield its entries one by one.

    This is internal API.

    :param xref_stream:
        A :class:`~generic.StreamObject`.
    :param strict:
        Boolean indicating whether we're running in strict mode.
    :return:
        A generator object yielding :class:`.XRefEntry` objects.
    """

    stream_data = BytesIO(xref_stream.data)
    # Index pairs specify the subsections in the dictionary. If
    # none create one subsection that spans everything.
    idx_pairs = xref_stream.get("/Index", [0, xref_stream.get("/Size")])
    entry_sizes = xref_stream.get("/W")

    def get_entry(ix):
        # Reads the correct number of bytes for each entry. See the
        # discussion of the W parameter in ISO 32000-1 table 17.
        entry_width = entry_sizes[ix]
        if entry_width > 0:
            d = stream_data.read(entry_width)
            if len(d) >= entry_width:
                return convert_to_int(d, entry_width)
            elif strict:
                raise misc.PdfStrictReadError(
                    "XRef stream ended prematurely; incomplete entry: "
                    f"expected to read {entry_width} bytes, but only got "
                    f"{len(d)}."
                )

        # ISO 32000-1 Table 17: A value of zero for an element in the
        # W array indicates...the default value shall be used
        if ix == 0:
            return 1  # First value defaults to 1
        else:
            return 0

    # Iterate through each subsection
    last_end = 0
    for start, size in misc.pair_iter(idx_pairs):
        # The subsections must increase
        assert start >= last_end
        last_end = start + size
        for num in range(start, start + size):
            # The first entry is the type
            xref_type = get_entry(0)
            # The rest of the elements depend on the xref_type
            if xref_type == 1:
                # objects that are in use but are not compressed
                location = get_entry(1)
                generation = get_entry(2)
                yield XRefEntry(
                    xref_type=XRefType.STANDARD,
                    idnum=num,
                    location=location,
                    generation=generation,
                )
            elif xref_type == 2:
                # compressed objects
                objstr_num = get_entry(1)
                objstr_idx = get_entry(2)
                location = ObjStreamRef(objstr_num, objstr_idx)
                yield XRefEntry(
                    xref_type=XRefType.IN_OBJ_STREAM,
                    idnum=num,
                    location=location,
                )
            elif xref_type == 0:
                # freed object
                # we ignore the linked list aspect anyway, so discard first
                get_entry(1)
                next_generation = get_entry(2)
                yield XRefEntry(
                    xref_type=XRefType.FREE,
                    idnum=num,
                    generation=next_generation,
                    location=None,
                )
            else:
                # unknown type (=> ignore).
                get_entry(1)
                get_entry(2)

    if len(stream_data.read(1)) > 0 and strict:
        raise misc.PdfStrictReadError("Trailing data in cross-reference stream")


@enum.unique
class XRefSectionType(enum.Enum):
    STANDARD = enum.auto()
    STREAM = enum.auto()
    HYBRID_MAIN = enum.auto()
    HYBRID_STREAM = enum.auto()


@dataclass(frozen=True)
class XRefSectionMetaInfo:
    xref_section_type: XRefSectionType
    """
    The type of cross-reference section.
    """

    size: int
    """
    The highest object ID in scope for this xref section.
    """

    declared_startxref: int
    """
    Location pointed to by the startxref pointer in that revision.
    """

    start_location: int
    """
    Actual start location of the xref data. This should be equal
    to `declared_startxref`, but in broken files that may not be the case.
    """

    end_location: int
    """
    Location where the xref data ended.
    """

    stream_ref: Optional[generic.Reference]
    """
    Reference to the relevant xref stream, if applicable.
    """


class XRefSectionData:
    """
    Internal class for bookkeeping on a single cross-reference section,
    independently of the others.
    """

    def __init__(self: 'XRefSectionData'):
        # TODO Food for thought: is there an efficient IntMap implementation out
        #  there that can beat generic python dicts in real-world scenarios?
        self.freed: Dict[int, int] = {}
        self.standard_xrefs: Dict[int, Tuple[int, int]] = {}
        self.xrefs_in_objstm: Dict[int, ObjStreamRef] = {}
        self.explicit_refs_in_revision: Set[Tuple[int, int]] = set()
        self.obj_streams_used: Set[int] = set()
        self.hybrid: Optional[XRefSection] = None

    def try_resolve(
        self, ref: Union[generic.Reference, generic.IndirectObject]
    ) -> Optional[Union[int, ObjStreamRef]]:
        # The lookups are ordered more or less in the order we expect
        # them to be queried most frequently in a given file.

        # In files that use object streams in a context where it provides
        #  significant savings, we also expect _most_ objects (esp. lots of
        #  small ones) to be stored in object streams (case in point: tagged
        #  documents). As such, it makes sense to look there first.
        # Note: generation must be zero
        if ref.generation == 0:
            try:
                return self.xrefs_in_objstm[ref.idnum]
            except KeyError:
                pass

        std_ref = self.standard_xrefs.get(ref.idnum, None)
        if std_ref is not None:
            # check if the generations match
            if std_ref[0] == ref.generation:
                return std_ref[1]
            else:
                raise KeyError(ref)

        freed_next_generation = self.freed.get(ref.idnum, None)
        # The generation in a free entry is the _next_ gen number at which
        # the reference may be used, hence the -1
        # Otherwise, the freed entry is irrelevant, so we fall through.
        if (
            freed_next_generation is not None
            and ref.generation == freed_next_generation - 1
        ):
            # note: 7.5.8.4 is confusingly worded, but we do _not_ need to
            # fall back to the hybrid ref here, because the main section
            # still takes precedence!
            return None

        # ISO 32000-2:2020, 7.5.8.4 says that we have to look in the hybrid
        # stream section before moving on now.
        if self.hybrid is not None:
            return self.hybrid.xref_data.try_resolve(ref)
        else:
            raise KeyError(ref)

    def process_entries(self, entries: Iterator[XRefEntry], strict: bool):
        highest_id = 0
        for xref_entry in entries:
            idnum = xref_entry.idnum
            generation = xref_entry.generation
            highest_id = max(idnum, highest_id)
            if generation > 0xFFFF:
                if strict:
                    raise PdfStrictReadError(
                        f"Illegal generation {generation} for "
                        f"object ID {idnum}."
                    )
                continue
            if xref_entry.idnum == 0:
                continue  # don't bother
            if xref_entry.xref_type == XRefType.STANDARD:
                offset = xref_entry.location
                assert isinstance(offset, int)
                self.standard_xrefs[idnum] = (generation, offset)
                self.explicit_refs_in_revision.add((idnum, generation))
            elif xref_entry.xref_type == XRefType.IN_OBJ_STREAM:
                assert generation == 0
                loc = xref_entry.location
                assert isinstance(loc, ObjStreamRef)
                self.xrefs_in_objstm[idnum] = loc
                self.obj_streams_used.add(loc.obj_stream_id)
                self.explicit_refs_in_revision.add((idnum, 0))
            elif xref_entry.xref_type == XRefType.FREE:
                self.freed[idnum] = generation
                # subtract one, since we're removing one generation before
                #  this one.
                self.explicit_refs_in_revision.add((idnum, generation - 1))
        return highest_id

    def process_hybrid_entries(
        self,
        entries: Iterator[XRefEntry],
        xref_meta_info: XRefSectionMetaInfo,
        strict: bool,
    ):
        hybrid = XRefSectionData()
        hybrid.process_entries(entries, strict=strict)
        self.hybrid = XRefSection(xref_meta_info, hybrid)

    def higher_generation_refs(self):
        for idnum, (generation, _) in self.standard_xrefs.items():
            if generation > 0:
                yield idnum, generation


@dataclass(frozen=True)
class XRefSection:
    """
    Describes a cross-reference section and describes how it is serialised into
    the PDF file.
    """

    meta_info: XRefSectionMetaInfo
    """
    Metadata about the cross-reference section.
    """

    xref_data: XRefSectionData
    """
    A description of the actual object pointer definitions.
    """


def _check_freed_refs(
    ix: int, section: XRefSection, all_sections: List[XRefSection]
):
    # Prevent xref stream objects from being overwritten.
    # (stricter than the spec, but it makes our lives easier at the cost
    # of rejecting some theoretically valid files)
    # The reason is because of the potential for caching issues with
    # encrypted files.
    xstream_ref = section.meta_info.stream_ref
    if xstream_ref:
        as_tuple = (xstream_ref.idnum, xstream_ref.generation)
        for section_ in all_sections[ix + 1 :]:
            data = section_.xref_data
            if as_tuple in data.explicit_refs_in_revision:
                raise misc.PdfStrictReadError(
                    "XRef stream objects must not be clobbered in strict "
                    "mode."
                )

    # For all free refs, check that _if_ redefined, they're
    # redefined with a proper generation number
    for idnum, expected_next_generation in section.xref_data.freed.items():
        # When rewriting files & removing dead objects, Acrobat will
        # enter the deleted reference into the Xref table/stream with
        # a 'next generation' ID of 0. It doesn't contradict the spec
        # directly, but I assumed that this was the way to indicate that
        # generation 0xffff had just been freed. Apparently not, because
        # I've seen Acrobat put that same freed reference in later revisions
        # with a next_gen number of 1. Bizarre.
        #
        # Anyhow, given the ubiquity of Adobe (Acrobat|Reader), it's
        # probably prudent to special-case this one.
        # In doing so, we're probably not dealing correctly with the case
        # where the 0xffff'th generation of an object is freed, but I'm
        # happy to assume that that will never happen in a legitimate file.

        # We will raise an error on any reuse of such "dead" objects
        #  in anything else than a 'free'.
        #
        # To be explicit (while still accepting legitimate files)
        #  we'll throw an error if this happens in a non-initial revision,
        #  until someone complains.
        if expected_next_generation == 0 and ix > 0:
            raise misc.PdfStrictReadError(
                "In strict mode, a free xref with next generation 0 is only"
                "permitted in an initial revision due to unclear semantics."
            )

        # We have to exempt free refs in sections preceding a HYBRID_MAIN
        #  section from conflicting with later overrides in HYBRID_STREAM
        #  sections. But also this provision is interpreted more strictly in
        #  pyHanko:
        #   we only allow the hybrid stream associated with the next section
        #   to be used to lift the restriction.
        # This is sufficient to enable the subset of uses of hybrid references
        # that I've seen in the wild.

        if ix < len(all_sections) - 1:
            next_section = all_sections[ix + 1]
            hybrid: Optional[XRefSectionData] = (
                next_section.xref_data.hybrid.xref_data
                if next_section.xref_data.hybrid is not None
                else None
            )
            if hybrid is not None:
                if (
                    idnum in hybrid.standard_xrefs
                    or idnum in hybrid.xrefs_in_objstm
                ):
                    # exemption!
                    continue

        improper_generation = None
        for succ in all_sections[ix + 1 :]:
            data = succ.xref_data
            # don't enforce
            if idnum in data.xrefs_in_objstm:
                improper_generation = 0
            else:
                try:
                    next_generation, _ = data.standard_xrefs[idnum]
                    # We compare using < instead of forcing equality.
                    # Jumps in the generation number will be detected
                    # by the higher_gen check further down.
                    # For the == 0 check, see comment about dead objects
                    # further up.
                    if (
                        expected_next_generation == 0
                        or next_generation < expected_next_generation
                    ):
                        improper_generation = next_generation
                except KeyError:
                    pass

            if improper_generation is not None:
                if expected_next_generation == 0:
                    raise misc.PdfStrictReadError(
                        f"Object with id {idnum} was listed as dead, "
                        f"but is reused later, with generation "
                        f"number {improper_generation}."
                    )
                else:
                    raise misc.PdfStrictReadError(
                        f"Object with id {idnum} and generation "
                        f"{improper_generation} was found after "
                        f"{expected_next_generation - 1} was freed."
                    )


def _with_hybrids(sections: List[XRefSection]):
    for section in sections:
        if section.xref_data.hybrid is not None:
            yield section.xref_data.hybrid
        yield section


def _check_xref_consistency(all_sections: List[XRefSection]):
    # expand out the hybrid sections as separate sections for the purposes
    # of the xref consistency check (try_resolve takes hybrids into account,
    # but

    # put the sections in chronological order for code readability reasons
    for ix, section in enumerate(all_sections):
        # check freed refs
        _check_freed_refs(ix, section, all_sections)

    expanded_sections = list(_with_hybrids(all_sections))
    prev_size = 0
    for ix, section in enumerate(expanded_sections):
        # collect all higher-generation refs
        higher_gen = set(section.xref_data.higher_generation_refs())
        sz = section.meta_info.size
        xref_type = section.meta_info.xref_section_type
        if sz < prev_size and xref_type != XRefSectionType.HYBRID_STREAM:
            # We allow stream sections in hybrid files to declare a smaller
            # xref size. I've seen this happen in cases where the xref stream
            # in a hybrid file doesn't contain itself.
            # Since this is common (*cough*MS Word*cough*) and not explicitly
            # prohibited by the standard, we let that slide.
            raise misc.PdfStrictReadError(
                f"XRef section sizes must be nondecreasing; found XRef section "
                f"of size {sz} after section of size {prev_size}."
            )
        prev_size = max(sz, prev_size)

        # Verify that all such higher-generation refs are
        # preceded by an appropriate free instruction of the previous generation
        # HOWEVER: the generation match is disabled when matching against
        #  freeings in the previous revision, to allow room for the way
        #  the free placeholders are commonly declared in hybrid reference
        #  files (i.e. as xxxxxxxxx 65535 f)
        is_hybrid_stream = (
            section.meta_info.xref_section_type == XRefSectionType.HYBRID_STREAM
        )
        for idnum, generation in higher_gen:
            preceding = reversed(expanded_sections[:ix])

            # this is the hybrid stream special case (see above)
            if is_hybrid_stream:
                prec = next(preceding)
                # don't apply generation check, just verify whether a free
                #  was present.
                if idnum in prec.xref_data.freed:
                    break

            for prec in preceding:
                try:
                    next_generation = prec.xref_data.freed[idnum]
                    if next_generation == generation:
                        break  # we've found the appropriate 'free'
                except KeyError:
                    continue
            else:
                raise misc.PdfStrictReadError(
                    f"Object with id {idnum} has an orphaned "
                    f"generation: generation {generation} was "
                    f"not preceded by a free instruction for "
                    f"generation {generation - 1}."
                )


class XRefBuilder:
    err_limit = 10

    def __init__(
        self, handler: PdfHandler, stream, strict: bool, last_startxref: int
    ):
        self.handler = handler
        self.stream = stream
        self.strict = strict
        self.last_startxref = last_startxref
        self.sections: List[XRefSection] = []

        self.trailer = TrailerDictionary()
        self.trailer.container_ref = generic.TrailerReference(handler)
        self.has_xref_stream = False

    def _read_xref_stream_object(self):
        stream = self.stream
        idnum, generation = read_object_header(stream, strict=self.strict)
        xrefstream_ref = generic.Reference(idnum, generation, pdf=self.handler)
        xrefstream = generic.StreamObject.read_from_stream(
            stream, xrefstream_ref
        )
        xrefstream.container_ref = xrefstream_ref
        assert xrefstream.raw_get("/Type") == "/XRef"
        return xrefstream_ref, xrefstream

    def _read_xref_stream(self, declared_startxref: int):
        stream = self.stream
        start_location = stream.tell()
        xrefstream_ref, xrefstream = self._read_xref_stream_object()

        xref_section_data = XRefSectionData()
        xref_section_data.process_entries(
            parse_xref_stream(xrefstream, strict=self.strict),
            strict=self.strict,
        )
        xref_meta_info = XRefSectionMetaInfo(
            xref_section_type=XRefSectionType.STREAM,
            # in a stream, the number of entries is enforced, so we don't need
            # to check it explicitly
            size=int(xrefstream.raw_get('/Size')),
            declared_startxref=declared_startxref,
            start_location=start_location,
            end_location=stream.tell(),
            stream_ref=xrefstream_ref,
        )
        self.sections.append(XRefSection(xref_meta_info, xref_section_data))

        self.trailer.add_trailer_revision(xrefstream)
        return xrefstream.get('/Prev')

    def _read_xref_table(self, declared_startxref: int):
        stream = self.stream
        xref_start = stream.tell()
        xref_section_data = XRefSectionData()
        highest = xref_section_data.process_entries(
            parse_xref_table(stream), strict=self.strict
        )
        xref_end = stream.tell()

        new_trailer = generic.DictionaryObject.read_from_stream(
            stream, generic.TrailerReference(self.handler)
        )
        assert isinstance(new_trailer, generic.DictionaryObject)
        declared_size = int(new_trailer.raw_get('/Size'))

        if self.strict and highest >= declared_size:
            raise misc.PdfStrictReadError(
                f"Xref table size mismatch: table allocated object with id "
                f"{highest}, but according to the trailer {declared_size - 1} "
                f"is the maximal allowed object id."
            )

        try:
            hybrid_xref_stm_loc = int(new_trailer.raw_get('/XRefStm'))
        except (KeyError, ValueError):
            hybrid_xref_stm_loc = None

        if hybrid_xref_stm_loc is not None:
            stream_pos = stream.tell()

            # TODO: employ similar off-by-n tolerances for hybrid xref sections
            #  as for regular ones? Probably YAGNI, but putting it out there
            #  for future review.
            stream.seek(hybrid_xref_stm_loc)
            stream_ref, xrefstream = self._read_xref_stream_object()

            # we'll treat those as regular XRef streams in terms of metadata
            hybrid_stream_meta = XRefSectionMetaInfo(
                xref_section_type=XRefSectionType.HYBRID_STREAM,
                size=int(xrefstream.raw_get('/Size')),
                declared_startxref=hybrid_xref_stm_loc,
                start_location=hybrid_xref_stm_loc,
                end_location=stream.tell(),
                stream_ref=stream_ref,
            )
            xref_section_data.process_hybrid_entries(
                parse_xref_stream(xrefstream),
                hybrid_stream_meta,
                strict=self.strict,
            )
            stream.seek(stream_pos)
            xref_type = XRefSectionType.HYBRID_MAIN
        else:
            stream_ref = None
            xref_type = XRefSectionType.STANDARD

        xref_meta_info = XRefSectionMetaInfo(
            xref_section_type=xref_type,
            size=declared_size,
            declared_startxref=declared_startxref,
            start_location=xref_start,
            end_location=xref_end,
            stream_ref=stream_ref,
        )
        self.sections.append(XRefSection(xref_meta_info, xref_section_data))

        self.trailer.add_trailer_revision(new_trailer)
        return new_trailer.get('/Prev')

    def read_xrefs(self):
        # read all cross reference tables and their trailers
        stream = self.stream
        declared_startxref = startxref = self.last_startxref
        # Tracks the number of times we retried to read a particular xref
        # section
        err_count = 0
        while startxref is not None:
            if (self.strict and err_count) or err_count > self.err_limit:
                raise PdfStrictReadError("Failed to locate xref section")
            if not err_count:
                declared_startxref = startxref
            stream.seek(startxref)
            if misc.skip_over_whitespace(stream):
                # This is common in linearised files, so we're not marking
                # this as an error, even in strict mode.
                logger.debug(
                    "Encountered unexpected whitespace when looking "
                    "for xref stream"
                )
                startxref = stream.tell()
            x = stream.read(1)
            if x == b"x":
                # standard cross-reference table
                ref = stream.read(4)
                if ref[:3] != b"ref":
                    raise PdfReadError("xref table read error")
                startxref = self._read_xref_table(declared_startxref)
            elif x.isdigit():
                # PDF 1.5+ Cross-Reference Stream
                stream.seek(-1, os.SEEK_CUR)
                try:
                    startxref = self._read_xref_stream(declared_startxref)
                except ObjectHeaderReadError as e:
                    logger.debug(
                        "Failed to read xref stream header, attempting to "
                        "correct...",
                        exc_info=e,
                    )
                    # can be caused by an OBO error (pointing too far)
                    startxref = _attempt_startxref_correction(stream, startxref)
                    err_count += 1
                    continue
                self.has_xref_stream = True
            else:
                startxref = _attempt_startxref_correction(stream, startxref)
                err_count += 1
                continue
            err_count = 0

        # put the sections in chronological order from this point onwards
        #  (better readability)
        chrono_sections = list(reversed(self.sections))
        if self.strict:
            _check_xref_consistency(chrono_sections)
        return chrono_sections


class ObjectHeaderReadError(misc.PdfReadError):
    pass


def _read_object_header(stream, strict):
    # Should never be necessary to read out whitespace, since the
    # cross-reference table should put us in the right spot to read the
    # object header.  In reality... some files have stupid cross reference
    # tables that are off by whitespace bytes.
    extra = False
    misc.skip_over_comment(stream)
    extra |= misc.skip_over_whitespace(stream)
    idnum_bytes = misc.read_until_whitespace(stream)
    idnum = int(idnum_bytes)
    extra |= misc.skip_over_whitespace(stream)
    generation_bytes = misc.read_until_whitespace(stream)
    generation = int(generation_bytes)
    stream.read(3)
    misc.read_non_whitespace(stream, seek_back=True)

    if extra and strict:
        logger.warning(
            f"Superfluous whitespace found in object header "
            f"{idnum} {generation}"
        )
    return idnum, generation


def read_object_header(stream, strict):
    pos = stream.tell()
    try:
        return _read_object_header(stream, strict)
    except ValueError as e:
        raise ObjectHeaderReadError(
            f"Failed to read object header at {pos}"
        ) from e


class TrailerDictionary(generic.PdfObject):
    """
    The standard mandates that each trailer shall contain
    at least all keys used in the preceding trailer, even if unmodified.
    Of course, we cannot trust documents to actually follow this rule, so
    this class implements fallbacks.
    """

    # These keys shouldn't really be considered part of the trailer dictionary,
    # and in particular are not subject to inheritance rules.
    non_trailer_keys = {
        '/Length',
        '/Filter',
        '/DecodeParms',
        '/W',
        '/Type',
        '/Index',
        '/XRefStm',
    }

    def __init__(self: 'TrailerDictionary'):
        # trailer revisions, numbered backwards (i.e. in processing order)
        # The element at index 0 is the most recent one.
        self._trailer_revisions: List[generic.DictionaryObject] = []
        self._new_changes = generic.DictionaryObject()

    def add_trailer_revision(self, trailer_dict: generic.DictionaryObject):
        self._trailer_revisions.append(trailer_dict)

    def __getitem__(self, item):
        # the decrypt parameter doesn't matter, get_object() decrypts
        # as necessary.
        return self.raw_get(item).get_object()

    def raw_get(
        self,
        key,
        decrypt: EncryptedObjAccess = EncryptedObjAccess.TRANSPARENT,
        revision=None,
    ):
        revisions = self._trailer_revisions
        if revision is None:
            try:
                return self._new_changes.raw_get(key, decrypt)
            except KeyError:
                pass
        else:
            # xref sections are numbered backwards
            section = len(revisions) - 1 - revision
            revisions = revisions[section:]

        if key in self.non_trailer_keys:
            # These are not subject to trailer inheritance;
            # only look in the most recent revision
            return revisions[0].raw_get(key, decrypt)

        for revision in revisions:
            try:
                return revision.raw_get(key, decrypt)
            except KeyError:
                continue
        raise KeyError(key)

    def __setitem__(self, item, value):
        self._new_changes[item] = value

    def __delitem__(self, item):
        try:
            # deleting stuff from _new_changes should be OK.
            del self._new_changes[item]
        except KeyError:
            raise misc.PdfError(
                "Cannot remove existing entries from trailer dictionary, only "
                "update them."
            )

    def flatten(self, revision=None) -> generic.DictionaryObject:
        relevant_revisions = self._trailer_revisions
        if revision is not None:
            relevant_revisions = relevant_revisions[-revision - 1 :]
        trailer = generic.DictionaryObject(
            {
                k: v
                for revision in reversed(relevant_revisions)
                for k, v in revision.items()
            }
        )
        if revision is None:
            trailer.update(self._new_changes)

        # ensure that the trailer isn't polluted using stream
        # compression / XRef parameters
        for key in self.non_trailer_keys:
            trailer.pop(key, None)
        return trailer

    def __contains__(self, item):
        try:
            self.raw_get(item, decrypt=EncryptedObjAccess.PROXY)
            return True
        except KeyError:
            return False

    def keys(self):
        return frozenset(chain(self._new_changes, *self._trailer_revisions))

    def __iter__(self):
        return iter(self.keys())

    def items(self):
        return self.flatten().items()

    def write_to_stream(self, stream, handler=None, container_ref=None):
        raise NotImplementedError(
            "TrailerDictionary object cannot be written directly"
        )


def _attempt_startxref_correction(stream, startxref):
    # couldn't process data at location pointed to by startxref.
    # Let's see if we can find the xref table nearby, as we've observed this
    # error with an off-by-one before.
    stream.seek(-11, os.SEEK_CUR)
    tmp = stream.read(20)
    xref_loc = tmp.find(b"xref")
    if xref_loc != -1:
        return startxref - (10 - xref_loc)
    # No explicit xref table, try finding a cross-reference stream, with a
    # negative offset to account for the startxref being wrong in either
    # direction
    xrefstm_readback = 5
    stream.seek(startxref - xrefstm_readback)
    tmp = stream.read(2 * xrefstm_readback)
    newline_loc = tmp.rfind(b"\n")
    if newline_loc != -1:
        stream.seek(startxref - xrefstm_readback + newline_loc - 1)
        misc.skip_over_whitespace(stream)
        line_start = stream.tell()
        for look in range(5):
            if stream.read(1).isdigit():
                # This is not a standard PDF, consider adding a warning
                return line_start + look

    # no xref table found at specified location
    raise PdfReadError("Could not find xref table at specified location")


def convert_to_int(d, size):
    if size <= 8:
        padding = bytes(8 - size)
        return struct.unpack(">q", padding + d)[0]
    else:
        return sum(digit * 256 ** (size - ix - 1) for ix, digit in enumerate(d))


class XRefCache:
    """
    Internal class to parse & store information from the xref section(s) of a
    PDF document.

    Stores both the most recent status of all xrefs in addition to their
    historical values.

    All members of this class are considered internal API and are subject
    to change without notice.
    """

    def __init__(self, reader, xref_sections: List[XRefSection]):
        self.reader = reader
        self._xref_sections = xref_sections

        # Our consistency checker forbids these from being clobbered in strict
        # mode even though the spec allows it. It's too much of a pain
        # to deal with the impact on encryption semantics correctly, and
        # for the time being, I'm willing to deal with the possibility of
        # having to reject a few potentially legitimate files because of this.
        self.xref_stream_refs = {
            section.meta_info.stream_ref
            for section in xref_sections
            if section.meta_info.stream_ref is not None
        }

    @property
    def total_revisions(self):
        return len(self._xref_sections)

    def get_last_change(self, ref: generic.Reference):
        for ix, section in enumerate(reversed(self._xref_sections)):
            try:
                section.xref_data.try_resolve(ref)
                return len(self._xref_sections) - 1 - ix
            except KeyError:
                # nothing in this section
                pass
        raise KeyError(ref)

    def object_streams_used_in(self, revision):
        section = self._xref_sections[revision]
        return {
            generic.Reference(objstm_id, pdf=self.reader)
            for objstm_id in section.xref_data.obj_streams_used
        }

    def get_introducing_revision(self, ref: generic.Reference):
        for ix, section in enumerate(self._xref_sections):
            try:
                result = section.xref_data.try_resolve(ref)
                if result is not None:
                    return ix
            except KeyError:
                # nothing in this section
                pass
        raise KeyError(ref)

    def get_xref_container_info(self, revision) -> XRefSectionMetaInfo:
        section = self._xref_sections[revision]
        return section.meta_info

    def get_xref_data(self, revision) -> XRefSectionData:
        section = self._xref_sections[revision]
        return section.xref_data

    def explicit_refs_in_revision(self, revision) -> Set[generic.Reference]:
        """
        Look up the object refs for all objects explicitly added or overwritten
        in a given revision.

        :param revision:
            A revision number. The oldest revision is zero.
        :return:
            A set of Reference objects.
        """
        section = self._xref_sections[revision]
        result = {
            generic.Reference(idnum, generation, pdf=self.reader)
            for idnum, generation in section.xref_data.explicit_refs_in_revision
        }
        hybrid = section.xref_data.hybrid
        if hybrid is not None:
            # make sure we also account for refs in hybrid sections
            result |= {
                generic.Reference(idnum, generation, pdf=self.reader)
                for idnum, generation in hybrid.xref_data.explicit_refs_in_revision
            }
        return result

    def refs_freed_in_revision(self, revision) -> Set[generic.Reference]:
        """
        Look up the object refs for all objects explicitly freed
        in a given revision.

        :param revision:
            A revision number. The oldest revision is zero.
        :return:
            A set of Reference objects.
        """
        section = self._xref_sections[revision]
        return {
            generic.Reference(idnum, gen - 1, pdf=self.reader)
            for idnum, gen in section.xref_data.freed.items()
            if gen > 0  # don't acknowledge "dead" objects as freeings
        }

    def get_startxref_for_revision(self, revision) -> int:
        """
        Look up the location of the XRef table/stream associated with a specific
        revision, as indicated by startxref or /Prev.

        :param revision:
            A revision number. The oldest revision is zero.
        :return:
            An integer pointer
        """
        section = self._xref_sections[revision]
        return section.meta_info.declared_startxref

    def get_historical_ref(
        self, ref, revision
    ) -> Optional[Union[int, ObjStreamRef]]:
        """
        Look up the location of the historical value of an object.

        .. note::
            This method is not suitable for determining whether or not
            a particular object ID is available in a given revision, since
            it treats unused objects and freed objects the same way.

        :param ref:
            An object reference.
        :param revision:
            A revision number. The oldest revision is zero.
        :return:
            An integer offset, an object stream reference, or ``None`` if
            the reference does not resolve in the specified revision.
        """
        for section in reversed(self._xref_sections[: revision + 1]):
            try:
                result = section.xref_data.try_resolve(ref)
                return result
            except KeyError:
                continue

        return None

    def __getitem__(self, ref):
        # No need to make this more efficient, since the reader caches
        # objects for us.
        return self.get_historical_ref(ref, len(self._xref_sections) - 1)

    @property
    def hybrid_xrefs_present(self) -> bool:
        """
        Determine if a file uses hybrid references anywhere.

        :return:
            ``True`` if hybrid references were detected, ``False`` otherwise.
        """
        return any(
            section.meta_info.xref_section_type == XRefSectionType.HYBRID_MAIN
            for section in self._xref_sections
        )


PositionDict = Dict[Tuple[int, int], Union[int, Tuple[int, int]]]
# Note: position_dict is (generation, objId) -> pos
# for historical reasons (inherited from refactored PyPDF2 logic).
# Position can be an int (absolute file offset) or a
# tuple (object stream reference)


OBJSTREAM_FORBIDDEN = (generic.IndirectObject, generic.StreamObject)


class ObjectStream:
    """
    Utility class to collect objects into a PDF object stream.

    Object streams are mainly useful for space efficiency reasons.
    They allow related objects to be grouped & compressed together in a
    more flexible manner.


    .. warning::
        Object streams can only be used in files with a cross-reference
        stream, as opposed to a classical XRef table.
        In particular, this means that incremental updates to files with a
        legacy XRef table cannot contain object streams either.
        See § 7.5.7 in ISO 32000-1 for further details.

    .. danger::
        Use :meth:`.BasePdfFileWriter.prepare_object_stream` to create instances
        of object streams. The `__init__` function is internal API.

    """

    def __init__(self, compress=True):
        self._obj_refs = {}
        self.compress = compress
        self.ref = None

    def __bool__(self):
        return bool(self._obj_refs)

    def __iter__(self):
        return iter(self._obj_refs.items())

    def add_object(self, idnum: int, obj: generic.PdfObject):
        """
        Add an object to an object stream.
        Note that objects in object streams always have their generation number
        set to `0` by definition.

        :param idnum:
            The object's ID number.
        :param obj:
            The object to embed into the object stream.
        :raise TypeError:
            Raised if ``obj`` is an instance of :class:`~.generic.StreamObject`
            or :class:`~.generic.IndirectObject`.
        """

        if isinstance(obj, OBJSTREAM_FORBIDDEN):
            raise TypeError(
                'Stream objects and bare references cannot be embedded into '
                'object streams.'
            )
        self._obj_refs[idnum] = obj

    def as_pdf_object(self) -> generic.StreamObject:
        """
        Render the object stream to a PDF stream object

        :return: An instance of :class:`~.generic.StreamObject`.
        """
        stream_header = BytesIO()
        main_body = BytesIO()
        for idnum, obj in self._obj_refs.items():
            offset = main_body.tell()
            obj.write_to_stream(main_body, None)
            stream_header.write(b'%d %d ' % (idnum, offset))

        first_obj_offset = stream_header.tell()
        stream_header.seek(0)
        sh_bytes = stream_header.read(first_obj_offset)
        stream_data = sh_bytes + main_body.getvalue()
        stream_object = generic.StreamObject(
            {
                pdf_name('/Type'): pdf_name('/ObjStm'),
                pdf_name('/N'): generic.NumberObject(len(self._obj_refs)),
                pdf_name('/First'): generic.NumberObject(first_obj_offset),
            },
            stream_data=stream_data,
        )
        if self.compress:
            stream_object.compress()
        return stream_object


def _contiguous_xref_chunks(position_dict):
    """
    Helper method to divide the XRef table (or stream) into contiguous chunks.
    """
    previous_idnum = None
    current_chunk = []

    if not position_dict.keys():
        # return immediately, there are no objects
        return

    # iterate over keys in object ID order
    key_iter = sorted(position_dict.keys(), key=lambda t: t[1])
    (_, first_idnum), key_iter = peek(key_iter)
    for ix in key_iter:
        generation, idnum = ix

        # the idnum jumped, so yield the current chunk
        # and start a new one
        if current_chunk and idnum != previous_idnum + 1:
            yield first_idnum, current_chunk
            current_chunk = []
            first_idnum = idnum

        # append the object reference to the current chunk
        # (xref table requires position and generation entries)
        current_chunk.append((position_dict[ix], generation))
        previous_idnum = idnum

    # there is always at least one chunk, so this is fine
    yield first_idnum, current_chunk


def write_xref_table(stream, position_dict: Dict[Tuple[int, int], int]):
    xref_location = stream.tell()
    stream.write(b'xref\n')
    # Insert xref table subsections in contiguous chunks.
    # This is necessarily more complicated than the implementation
    # in PyPDF2 (see ISO 32000 § 7.5.4, esp. on updates), since
    # we need to handle incremental updates correctly.
    subsections = _contiguous_xref_chunks(position_dict)

    def write_header(idnum, length):
        header = '%d %d\n' % (idnum, length)
        stream.write(header.encode('ascii'))

    def write_subsection(chunk):
        for position, generation in chunk:
            entry = "%010d %05d n \n" % (position, generation)
            stream.write(entry.encode('ascii'))

    try:
        first_idnum, subsection = next(subsections)
    except StopIteration:
        # no updates, just write '0 0' and be done with it
        stream.write(b'0 0\n')
        return xref_location
    # TODO support deleting objects
    # case distinction: in contrast with the above we have to ensure that
    # everything is written in one chunk when *not* doing incremental updates.
    # In particular, this applies to the null object
    null_obj_ref = b'0000000000 65535 f \n'
    if first_idnum == 1:
        # integrate the null object into the first subsection
        write_header(0, len(subsection) + 1)
        stream.write(null_obj_ref)
        write_subsection(subsection)
    else:
        # insert origin of linked list of freed objects, and then the first
        # subsection, as usual
        stream.write(b'0 1\n')
        stream.write(null_obj_ref)
        write_header(first_idnum, len(subsection))
        write_subsection(subsection)
    for first_idnum, subsection in subsections:
        # subsection header: list first object ID + length of subsection
        write_header(first_idnum, len(subsection))
        write_subsection(subsection)

    return xref_location


class XRefStream(generic.StreamObject):
    def __init__(self, position_dict: PositionDict):
        super().__init__()
        self.position_dict = position_dict

        # type indicator is one byte wide
        # we use longs to indicate positions of objects (>Q)
        # two more bytes for the generation number of an uncompressed object
        widths = map(generic.NumberObject, (1, 8, 2))
        self.update(
            {
                pdf_name('/W'): generic.ArrayObject(widths),
                pdf_name('/Type'): pdf_name('/XRef'),
            }
        )

    def write_to_stream(self, stream, handler=None, container_ref=None):
        # the caller is responsible for making sure that the stream
        # is registered in the position dictionary

        index = [0, 1]
        subsections = _contiguous_xref_chunks(self.position_dict)
        stream_content = BytesIO()
        # write null object
        stream_content.write(b'\x00' * 9 + b'\xff\xff')
        for first_idnum, subsection in subsections:
            index += [first_idnum, len(subsection)]
            for position, generation in subsection:
                if isinstance(position, tuple):
                    # reference to object in object stream
                    assert generation == 0
                    obj_stream_num, ix = position
                    stream_content.write(b'\x02')
                    stream_content.write(struct.pack('>Q', obj_stream_num))
                    stream_content.write(struct.pack('>H', ix))
                else:
                    stream_content.write(b'\x01')
                    stream_content.write(struct.pack('>Q', position))
                    stream_content.write(struct.pack('>H', generation))
        index_entry = generic.ArrayObject(map(generic.NumberObject, index))

        self[pdf_name('/Index')] = index_entry
        self._data = stream_content.getbuffer()
        super().write_to_stream(stream, None)
