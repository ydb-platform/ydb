"""Basic support for OpenType/TrueType font handling & subsetting.

This module relies on `fontTools <https://pypi.org/project/fonttools/>`_ for
OTF parsing and subsetting, and on HarfBuzz (via ``uharfbuzz``) for shaping.
"""

import logging
from binascii import hexlify
from dataclasses import dataclass
from io import BytesIO
from typing import Callable, Dict, Iterable, List, Optional, Tuple

from pyhanko.pdf_utils import generic
from pyhanko.pdf_utils.font.api import (
    FontEngine,
    FontEngineFactory,
    ShapeResult,
)
from pyhanko.pdf_utils.misc import peek
from pyhanko.pdf_utils.writer import BasePdfFileWriter

try:
    import uharfbuzz as hb
    from fontTools import subset, ttLib
except ImportError as import_err:  # pragma: nocover
    raise ImportError(
        "pyhanko.pdf_utils.font.opentype requires pyHanko to be installed with "
        "the [opentype] option. You can install missing "
        "dependencies by running \"pip install 'pyHanko[opentype]'\".",
        import_err,
    )


__all__ = ['GlyphAccumulator', 'GlyphAccumulatorFactory']

# TODO: the holy grail would be to integrate PDF font resource management
#  and rendering with a battle-tested text layout engine like Pango.
#  Not sure how easy that is, though.


logger = logging.getLogger(__name__)

pdf_name = generic.NameObject
pdf_string = generic.pdf_string


def _format_simple_glyphline_from_buffer(
    buf, cid_width_callback: Callable[[int], Tuple[int, int]]
):
    total_len = 0
    info: hb.GlyphInfo
    pos: hb.GlyphPosition
    subsgmt_cids: List[int] = []
    tj_adjust = 0
    current_tj_segments: List[str] = []

    def _emit_subsegment():
        nonlocal subsgmt_cids
        current_tj_segments.append(
            f"<{''.join('%04x' % cid for cid in subsgmt_cids)}>"
        )
        subsgmt_cids = []
        if tj_adjust:
            current_tj_segments.append(str(tj_adjust))

    # Horizontal text with horizontal kerning
    for info, pos in zip(buf.glyph_infos, buf.glyph_positions):
        # the TJ operator is weird like that, we have to know both
        # the glyph length and the advance reported by Harfbuzz
        current_cid, width = cid_width_callback(info.codepoint)

        # make sure the current x_offset is included in tj_adjust
        # before the current CID can be emitted
        # Note: sign convention for offsets is opposite in HarfBuzz and in
        #  PDF, hence the minus sign
        tj_adjust -= pos.x_offset
        if tj_adjust:
            # if tj_adjust is nonzero, emit a new subsegment,
            # and put in the adjustment
            _emit_subsegment()
        subsgmt_cids.append(current_cid)
        # reset tj_adjust for the next iteration
        #  (the x_offset shouldn't affect the current glyphline position,
        #  hence why we add it back)
        tj_adjust = width - pos.x_advance + pos.x_offset
        total_len += pos.x_advance

    if subsgmt_cids:
        _emit_subsegment()
    return f'[{" ".join(current_tj_segments)}] TJ'.encode('ascii'), total_len


def _format_cid_glyphline_from_buffer(
    buf,
    cid_width_callback: Callable[[int], Tuple[int, int]],
    units_per_em: int,
    font_size: float,
    vertical: bool,
):
    no_complex_positioning = not vertical and all(
        not pos.y_offset for pos in buf.glyph_positions
    )

    def _glyph_to_user(num):
        return (num / units_per_em) * font_size

    # simple case where we can do it in one TJ
    if no_complex_positioning:
        ops, total_len = _format_simple_glyphline_from_buffer(
            buf, cid_width_callback
        )

        # do a Td to put the newline cursor at the end of our output,
        # for compatibility with the complex positioning layout code
        text_ops = b'%s %g 0 Td' % (ops, _glyph_to_user(total_len))
        return text_ops, (total_len, 0)

    info: hb.GlyphInfo
    pos: hb.GlyphPosition
    commands = []

    # use manual Td's and Tj's to position glyphs
    # This routine assumes that the current Td "origin" coincides
    # with the current cursor position.
    total_x_len = 0
    total_y_len = 0
    x_pos = 0
    y_pos = 0
    for info, pos in zip(buf.glyph_infos, buf.glyph_positions):
        current_cid, _ = cid_width_callback(info.codepoint)

        x_pos += pos.x_offset
        y_pos += pos.y_offset

        # position cursor
        if x_pos or y_pos:
            # this also updates the line matrix so the next Td will be in
            # coordinates relative to this point
            commands.append(
                b'%g %g Td' % (_glyph_to_user(x_pos), _glyph_to_user(y_pos))
            )

        # emit one character
        commands.append(f'<{current_cid:04x}> Tj'.encode('ascii'))

        # note: we don't care about the width & auto-advance in PDF-land,
        # just compute everything using positioning data from HarfBuzz

        # have to compensate for x_offset / y_offset
        # since it's all relative to the origin of the current glyph
        x_pos = pos.x_advance - pos.x_offset
        y_pos = pos.y_advance - pos.y_offset

        total_x_len += pos.x_advance
        total_y_len += pos.y_advance

    # do a final Td to put the newline cursor at the end of our output
    if x_pos or y_pos:
        commands.append(
            b'%g %g Td' % (_glyph_to_user(x_pos), _glyph_to_user(y_pos))
        )

    return b' '.join(commands), (total_x_len, total_y_len)


def _gids_by_cluster(buf) -> Iterable[Tuple[int, Optional[int], List[int]]]:
    # assumes that the first cluster is 0

    cur_cluster = 0
    cur_cluster_glyphs: List[int] = []
    gi: hb.GlyphInfo
    for gi in buf.glyph_infos:
        if gi.cluster != cur_cluster:
            yield cur_cluster, gi.cluster, cur_cluster_glyphs
            cur_cluster_glyphs = []
            cur_cluster = gi.cluster
        cur_cluster_glyphs.append(gi.codepoint)

    yield cur_cluster, None, cur_cluster_glyphs


def _build_type0_font_from_cidfont(
    writer,
    cidfont_obj: 'CIDFont',
    widths_by_cid_iter,
    vertical,
    obj_stream=None,
):
    # take the Identity-* encoding to inherit from the /Encoding
    # entry specified in our CIDSystemInfo dict
    encoding = 'Identity-V' if vertical else 'Identity-H'

    cidfont_obj.embed(writer, obj_stream=obj_stream)
    cidfont_ref = writer.add_object(cidfont_obj, obj_stream=obj_stream)
    type0 = generic.DictionaryObject(
        {
            pdf_name('/Type'): pdf_name('/Font'),
            pdf_name('/Subtype'): pdf_name('/Type0'),
            pdf_name('/DescendantFonts'): generic.ArrayObject([cidfont_ref]),
            pdf_name('/Encoding'): pdf_name('/' + encoding),
            pdf_name('/BaseFont'): pdf_name(f'/{cidfont_obj.name}-{encoding}'),
        }
    )
    # compute widths entry

    def _widths():
        current_chunk = []
        prev_cid = None
        (first_cid, _), itr = peek(widths_by_cid_iter)
        for cid, width in itr:
            if current_chunk and cid != prev_cid + 1:
                yield generic.NumberObject(first_cid)
                yield generic.ArrayObject(current_chunk)
                current_chunk = []
                first_cid = cid

            current_chunk.append(generic.NumberObject(width))
            prev_cid = cid
        if current_chunk:
            yield generic.NumberObject(first_cid)
            yield generic.ArrayObject(current_chunk)

    cidfont_obj[pdf_name('/W')] = generic.ArrayObject(list(_widths()))
    return type0


def _breakdown_cmap(mappings):
    # group contiguous mappings in a cmap

    sorted_pairs = iter(sorted(mappings, key=lambda t: t[0]))

    # use the first item of the iterator to initialise the state
    source, target = next(sorted_pairs)
    cur_segment_start = prev = source
    cur_segment = [target]

    for source, target in sorted_pairs:
        # max segment length is 100
        if not cur_segment or source == prev + 1:
            # extend current segment
            cur_segment.append(target)
        else:
            # emit previous segment, and start a new one
            yield cur_segment_start, cur_segment
            cur_segment = [target]
            cur_segment_start = source
        prev = source

    if cur_segment:
        yield cur_segment_start, cur_segment


def _segment_cmap(mappings):
    current_heading = 'char'
    decls = []

    def _emit():
        yield f'{len(decls)} beginbf{current_heading}'
        use_bfrange = current_heading == 'range'
        for source_start, targets in decls:
            if use_bfrange:
                source_end = source_start + len(targets) - 1
                # TODO use short form for bfrange targets if they're all
                #  contiguous
                target_values = ' '.join(
                    f'<{hexlify(target).decode("ascii")}>' for target in targets
                )
                yield (
                    f'<{source_start:04x}> <{source_end:04x}> [{target_values}]'
                )
            else:
                target = targets[0]
                yield (
                    f'<{source_start:04x}> <{hexlify(target).decode("ascii")}>'
                )
        yield f'endbf{current_heading}'

    for pair in _breakdown_cmap(mappings):
        current_type = 'char' if len(pair[1]) == 1 else 'range'
        if (decls and current_heading != current_type) or len(decls) >= 100:
            yield from _emit()
            decls = []
        current_heading = current_type
        decls.append(pair)

    if decls:
        yield from _emit()


def _check_ot_tag(tag):
    if tag is None:
        return
    if len(tag) != 4:
        raise ValueError("OpenType tags must be 4 bytes long")
    try:
        tag.encode('ascii')
    except UnicodeEncodeError as e:
        raise ValueError("OpenType tags must be ASCII-encodable") from e
    return tag


def _read_ps_name(tt: ttLib.TTFont) -> str:
    ps_name = None
    try:
        name_table = tt['name']
        # extract PostScript name from the font's name table
        nr = next(nr for nr in name_table.names if nr.nameID == 6)
        ps_name = nr.toUnicode()
    except StopIteration:  # pragma: nocover
        pass

    if ps_name is None:
        raise NotImplementedError(
            "Could not deduce PostScript name for font; only "
            "unicode-compatible encodings in the name table are supported"
        )
    return ps_name


class GlyphAccumulator(FontEngine):
    """
    Utility to collect & measure glyphs from OpenType/TrueType fonts.

    :param writer:
        A PDF writer.
    :param font_handle:
        File-like object
    :param font_size:
        Font size in pt units.

        .. note::
            This is only relevant for some positioning intricacies (or hacks,
            depending on your perspective) that may not matter for your use
            case.
    :param features:
        Features to use. If ``None``, use HarfBuzz defaults.
    :param ot_script_tag:
        OpenType script tag to use. Will be guessed by HarfBuzz if not
        specified.
    :param ot_language_tag:
        OpenType language tag to use. Defaults to the default language system
        for the current script.
    :param writing_direction:
        Writing direction, one of 'ltr', 'rtl', 'ttb' or 'btt'.
        Will be guessed by HarfBuzz if not specified.
    :param bcp47_lang_code:
        BCP 47 language code. Used to mark the text's language in the PDF
        content stream, if specified.
    :param obj_stream:
        Try to put font-related objects into a particular object stream, if
        specified.
    """

    def __init__(
        self,
        writer: BasePdfFileWriter,
        font_handle,
        font_size,
        features=None,
        ot_language_tag=None,
        ot_script_tag=None,
        writing_direction=None,
        bcp47_lang_code=None,
        obj_stream=None,
    ):
        # harfbuzz expects bytes
        font_handle.seek(0)
        font_bytes = font_handle.read()
        font_handle.seek(0)
        face = hb.Face(font_bytes)
        self.font_size = font_size
        self.hb_font = hb.Font(face)
        self.tt = tt = ttLib.TTFont(font_handle)
        base_ps_name = _read_ps_name(tt)
        super().__init__(
            writer, base_ps_name, embedded_subset=True, obj_stream=obj_stream
        )
        self._font_ref = writer.allocate_placeholder()
        self.cidfont_obj: CIDFont
        try:
            cff = self.tt['CFF ']
            self.cff_charset = cff.cff[0].charset

            # CFF font programs are embedded differently
            #  (in a more Adobe-native way)
            self.cidfont_obj = cidfont_obj = CIDFontType0(
                tt, base_ps_name, self.subset_prefix
            )
            self.use_raw_gids = cidfont_obj.use_raw_gids
        except KeyError:
            self.cff_charset = None
            self.use_raw_gids = True
            self.cidfont_obj = CIDFontType2(
                tt, base_ps_name, self.subset_prefix
            )

        self.features = features

        # the 'head' table is mandatory
        self.units_per_em = tt['head'].unitsPerEm

        self._glyphs: Dict[int, Tuple[int, int]] = {}
        self._glyph_set = tt.getGlyphSet(preferCFF=True)

        self._cid_to_unicode: Dict[int, str] = {}
        self.__reverse_cmap = None
        self.ot_language_tag = _check_ot_tag(ot_language_tag)
        self.ot_script_tag = _check_ot_tag(ot_script_tag)
        if writing_direction is not None and writing_direction not in (
            'ltr',
            'rtl',
            'ttb',
            'btt',
        ):
            raise ValueError(
                "Writing direction must be one of 'ltr', 'rtl', 'ttb' or 'btt'."
            )
        self.writing_direction = writing_direction
        self.bcp47_lang_code = bcp47_lang_code
        self._subset_created = self._write_prepared = False

    @property
    def _reverse_cmap(self):
        if self.__reverse_cmap is None:
            self.__reverse_cmap = self.tt['cmap'].buildReversed()
        return self.__reverse_cmap

    def _get_cid_and_width(self, glyph_id: int) -> Tuple[int, int]:
        try:
            return self._glyphs[glyph_id]
        except KeyError:
            pass

        if not self.use_raw_gids:
            cid_str = self.cff_charset[glyph_id]
            current_cid = int(cid_str[3:])
            glyph = self._glyph_set.get(cid_str)
        else:
            # current_cid = glyph_id in the Type2 case
            # (for our subsetting setup)
            current_cid = glyph_id
            glyph_name = self.tt.getGlyphName(glyph_id)
            glyph = self._glyph_set.get(glyph_name)
        self._glyphs[glyph_id] = result = current_cid, glyph.width
        return result

    def marked_content_property_list(self, txt) -> generic.DictionaryObject:
        result = generic.DictionaryObject(
            {pdf_name('/ActualText'): generic.TextStringObject(txt)}
        )
        if self.bcp47_lang_code is not None:
            result['/Lang'] = pdf_string(self.bcp47_lang_code)
        return result

    def shape(self, txt: str, with_actual_text: bool = True) -> ShapeResult:
        if not txt:
            # currently, uharfbuzz will return buf.glyph_positions=None when
            # passed an empty string, so we handle that as a special case
            return ShapeResult(b'', 0, 0)
        buf = hb.Buffer()
        buf.add_str(txt)

        if self.ot_script_tag is not None:
            buf.set_script_from_ot_tag(self.ot_script_tag)
        if self.ot_language_tag is not None:
            buf.set_language_from_ot_tag(self.ot_language_tag)
        if self.writing_direction is not None:
            buf.direction = self.writing_direction

        # guess any remaining unset segment properties
        buf.guess_segment_properties()

        hb.shape(self.hb_font, buf, self.features)

        vertical = buf.direction in ('ttb', 'btt')
        text_ops, (total_x, total_y) = _format_cid_glyphline_from_buffer(
            buf,
            cid_width_callback=self._get_cid_and_width,
            units_per_em=self.units_per_em,
            font_size=self.font_size,
            vertical=vertical,
        )

        # the original buffer's cluster values are just character indexes
        # so calculating cluster extents is not hard
        # We'll use that information to put together a ToUnicode CMap
        for cluster, next_cluster, glyph_ids in _gids_by_cluster(buf):
            # CMaps need individual CIDs, so we cannot deal with multi-glyph
            # clusters (e.g. g̈, which is g + a combining diaeresis, so two
            # unicode code points --- and often represented as two separate
            # glyphs in a font)
            # https://www.unicode.org/reports/tr29/#Grapheme_Cluster_Boundaries
            #  TODO figure out how much of a limitation that is in practice.
            #   And if so, think about other ways to handle ToUnicode
            #  TODO things like diacritic stacks can be solved by individual
            #   'cmap' table lookups, as a fallback
            if next_cluster is None:
                unicode_str = txt[cluster:]
            else:
                unicode_str = txt[cluster:next_cluster]
            if len(glyph_ids) == 1:
                gid = glyph_ids[0]
                if gid == 0:
                    continue
                cid, _ = self._get_cid_and_width(gid)
                self._cid_to_unicode[cid] = unicode_str
            else:
                # fallback for many-to-many clusters: try to look up the glyphs
                # one by one
                relevant_codepoints = frozenset(ord(x) for x in unicode_str)
                for gid in glyph_ids:
                    if gid == 0:
                        continue
                    cid, _ = self._get_cid_and_width(gid)
                    glyph_name = self.tt.getGlyphName(gid)
                    # since this is a fallback, we don't allow clobbering
                    # of existing values
                    if cid not in self._cid_to_unicode:
                        codepoints = self._reverse_cmap.get(glyph_name, ())
                        for codepoint in codepoints:
                            # only allow unicode codepoints that actually occur
                            # in the substring
                            if codepoint in relevant_codepoints:
                                self._cid_to_unicode[cid] = chr(codepoint)

        if with_actual_text:
            # wrap the text rendering operations in a Span
            marked_content_buf = BytesIO()
            marked_content_buf.write(b'/Span ')
            mc_properties = self.marked_content_property_list(txt)
            mc_properties.write_to_stream(marked_content_buf)
            marked_content_buf.write(b' BDC ')
            marked_content_buf.write(text_ops)
            marked_content_buf.write(b' EMC')

            marked_content_buf.seek(0)
            graphics_ops = marked_content_buf.read()
        else:
            graphics_ops = text_ops

        return ShapeResult(
            graphics_ops=graphics_ops,
            x_advance=total_x / self.units_per_em,
            y_advance=total_y / self.units_per_em,
        )

    def _format_tounicode_cmap(self):
        # ToUnicode is always Adobe-UCS2-0 in our case,
        # since we use the fixed-with 2-byte UCS2 encoding for the BMP
        header = (
            '/CIDInit /ProcSet findresource begin\n'
            '12 dict begin\n'
            'begincmap\n'
            '/CIDSystemInfo <<\n'
            '/Registry (Adobe)\n'
            '/Ordering (UCS2)\n'
            '/Supplement 0\n'
            '>> def\n'
            '/CMapName /Adobe-Identity-UCS2 def\n'
            '/CMapType 2 def\n'
            '1 begincodespacerange\n'
            '<0000> <FFFF>\n'
            'endcodespacerange\n'
        )
        to_segment = (
            (cid, codepoints.encode('utf-16be'))
            for cid, codepoints in self._cid_to_unicode.items()
        )
        body = '\n'.join(_segment_cmap(to_segment))

        footer = (
            '\nendcmap\n'
            'CMapName\n'
            'currentdict\n'
            '/CMap defineresource\n'
            'pop\nend\nend'
        )
        stream = generic.StreamObject(
            stream_data=(header + body + footer).encode('ascii')
        )
        stream.compress()
        return stream

    def _extract_subset(self, options: Optional[subset.Options] = None):
        if options is None:
            options = subset.Options(layout_closure=False)
            options.drop_tables += ['GPOS', 'GDEF', 'GSUB']

        if self.use_raw_gids:
            # Have to retain GIDs in the Type2 (non-CFF) case, since we don't
            # have a predefined character set available (i.e. the ROS ordering
            # param is 'Identity')
            # This ensures that the PDF operators we output will keep working
            # with the subsetted font, at the cost of a slight space overhead in
            # the output.
            # This is fine, because fonts with a number of glyphs where this
            # would matter (i.e. large CJK fonts, basically), are subsetted as
            # CID-keyed CFF fonts anyway (and based on a predetermined charset),
            # so this subtlety doesn't apply and the space overhead is very
            # small.

            # NOTE: we apply the same procedure to string-keyed CFF fonts,
            # even though this is rather inefficient... XeTeX can subset these
            # much more compactly, presumably by rewriting the CFF charset data.
            # TODO look into how that works
            options.retain_gids = True

        subsetter: subset.Subsetter = subset.Subsetter(options=options)
        subsetter.populate(gids=list(self._glyphs.keys()))
        subsetter.subset(self.tt)

    def prepare_write(self):
        """
        This implementation of ``prepare_write`` will embed a subset of this
        glyph accumulator's font into the PDF writer it belongs to.
        Said subset will include all glyphs necessary to render the
        strings provided to the accumulator via :meth:`feed_string`.

        .. danger::
            Due to the way ``fontTools`` handles subsetting, this is a
            destructive operation. The in-memory representation of the original
            font will be overwritten by the generated subset.
        """

        if self._write_prepared:
            return
        if not self._subset_created:
            self._extract_subset()
            self._subset_created = True
        writer = self.writer
        obj_stream = self.obj_stream
        by_cid = iter(sorted(self._glyphs.values(), key=lambda t: t[0]))
        type0 = _build_type0_font_from_cidfont(
            writer=writer,
            cidfont_obj=self.cidfont_obj,
            widths_by_cid_iter=by_cid,
            obj_stream=obj_stream,
            vertical=False,
        )
        type0['/ToUnicode'] = writer.add_object(self._format_tounicode_cmap())
        # use our preallocated font ref
        writer.add_object(type0, obj_stream, idnum=self._font_ref.idnum)
        self._write_prepared = True

    def as_resource(self) -> generic.IndirectObject:
        return self._font_ref


class CIDFont(generic.DictionaryObject):
    def __init__(
        self,
        tt: ttLib.TTFont,
        subtype,
        registry,
        ordering,
        supplement,
        base_ps_name,
        subset_prefix,
    ):
        self._tt = tt

        self.subset_prefix = subset_prefix

        ps_name = '%s+%s' % (subset_prefix, base_ps_name)

        self.name = ps_name
        self.ros = registry, ordering, supplement

        super().__init__(
            {
                pdf_name('/Type'): pdf_name('/Font'),
                pdf_name('/Subtype'): pdf_name(subtype),
                pdf_name('/CIDSystemInfo'): generic.DictionaryObject(
                    {
                        pdf_name('/Registry'): pdf_string(registry),
                        pdf_name('/Ordering'): pdf_string(ordering),
                        pdf_name('/Supplement'): generic.NumberObject(
                            supplement
                        ),
                    }
                ),
                pdf_name('/BaseFont'): pdf_name('/' + ps_name),
            }
        )
        self._font_descriptor = FontDescriptor(self)

    def embed(self, writer: BasePdfFileWriter, obj_stream=None):
        fd = self._font_descriptor
        self[pdf_name('/FontDescriptor')] = fd_ref = writer.add_object(
            fd, obj_stream=obj_stream
        )
        font_stream_ref = self.set_font_file(writer)
        return fd_ref, font_stream_ref

    def set_font_file(self, writer: BasePdfFileWriter):
        raise NotImplementedError

    @property
    def tt_font(self):
        return self._tt


class CIDFontType0(CIDFont):
    def __init__(self, tt: ttLib.TTFont, base_ps_name, subset_prefix):
        # We assume that this font set (in the CFF sense) contains
        # only one font. This is fairly safe according to the fontTools docs.
        self.cff = cff = tt['CFF '].cff
        td = cff[0]
        try:
            registry, ordering, supplement = td.ROS
            self.use_raw_gids = False
        except (AttributeError, ValueError):
            self.use_raw_gids = True
            # String-keyed CFF font
            logger.warning("No ROS metadata. Assuming identity.")
            registry = "Adobe"
            ordering = "Identity"
            supplement = 0
        super().__init__(
            tt,
            '/CIDFontType0',
            registry,
            ordering,
            supplement,
            base_ps_name,
            subset_prefix,
        )
        td.rawDict['FullName'] = '%s+%s' % (self.subset_prefix, self.name)

    def set_font_file(self, writer: BasePdfFileWriter):
        stream_buf = BytesIO()
        # write the CFF table to the stream
        self.cff.compile(stream_buf, self.tt_font)
        stream_buf.seek(0)
        font_stream = generic.StreamObject(
            {
                # this is a Type0 CFF font program (see Table 126 in ISO 32000)
                pdf_name('/Subtype'): pdf_name('/CIDFontType0C'),
            },
            stream_data=stream_buf.read(),
        )
        font_stream.compress()
        font_stream_ref = writer.add_object(font_stream)
        self._font_descriptor[pdf_name('/FontFile3')] = font_stream_ref
        return font_stream_ref


class CIDFontType2(CIDFont):
    def __init__(self, tt: ttLib.TTFont, base_ps_name, subset_prefix):
        super().__init__(
            tt,
            '/CIDFontType2',
            registry="Adobe",
            # i.e. "no defined character set, just do whatever"
            # This makes sense because there's no native notion of character
            # sets in OTF/TTF fonts without a CFF font program.
            # (since we also put CIDToGIDMap = /Identity, this effectively means
            # that CIDs correspond to GIDs in the font)
            ordering="Identity",
            supplement=0,
            base_ps_name=base_ps_name,
            subset_prefix=subset_prefix,
        )
        self['/CIDToGIDMap'] = pdf_name('/Identity')

    def set_font_file(self, writer: BasePdfFileWriter):
        stream_buf = BytesIO()
        self.tt_font.save(stream_buf)
        stream_buf.seek(0)

        font_stream = generic.StreamObject(stream_data=stream_buf.read())
        font_stream.compress()
        font_stream_ref = writer.add_object(font_stream)
        self._font_descriptor[pdf_name('/FontFile2')] = font_stream_ref
        return font_stream_ref


class FontDescriptor(generic.DictionaryObject):
    """
    Lazy way to embed a font descriptor. It assumes all sorts of metadata
    to be present. If not, it'll probably fail with a gnarly error.
    """

    def __init__(self, cf: CIDFont):
        tt = cf.tt_font

        # Some metrics
        hhea = tt['hhea']
        head = tt['head']
        bbox = [head.xMin, head.yMin, head.xMax, head.yMax]
        os2 = tt['OS/2']
        weight = os2.usWeightClass
        stemv = int(10 + 220 * (weight - 50) / 900)
        super().__init__(
            {
                pdf_name('/Type'): pdf_name('/FontDescriptor'),
                pdf_name('/FontName'): pdf_name('/' + cf.name),
                pdf_name('/Ascent'): generic.NumberObject(hhea.ascent),
                pdf_name('/Descent'): generic.NumberObject(hhea.descent),
                pdf_name('/FontBBox'): generic.ArrayObject(
                    map(generic.NumberObject, bbox)
                ),
                # FIXME I'm setting the Serif and Symbolic flags here, but
                #  is there any way we can read/infer those from the TTF metadata?
                pdf_name('/Flags'): generic.NumberObject(0b110),
                pdf_name('/StemV'): generic.NumberObject(stemv),
                pdf_name('/ItalicAngle'): generic.FloatObject(
                    getattr(tt['post'], 'italicAngle', 0)
                ),
                pdf_name('/CapHeight'): generic.NumberObject(
                    getattr(os2, 'sCapHeight', 750)
                ),
            }
        )


@dataclass(frozen=True)
class GlyphAccumulatorFactory(FontEngineFactory):
    """
    Stateless callable helper class to instantiate :class:`.GlyphAccumulator`
    objects.
    """

    font_file: str
    """
    Path to the OTF/TTF font to load.
    """

    font_size: int = 10
    """
    Font size.
    """

    ot_script_tag: Optional[str] = None
    """
    OpenType script tag to use. Will be guessed by HarfBuzz if not
    specified.
    """

    ot_language_tag: Optional[str] = None
    """
    OpenType language tag to use. Defaults to the default language system
    for the current script.
    """

    writing_direction: Optional[str] = None
    """
    Writing direction, one of 'ltr', 'rtl', 'ttb' or 'btt'.
    Will be guessed by HarfBuzz if not specified.
    """

    bcp47_lang_code: Optional[str] = None
    """
    BCP 47 language code to tag strings with.
    """

    create_objstream_if_needed: bool = True
    """
    Create an object stream to hold this glyph accumulator's assets if no
    object stream is passed in, and the writer supports object streams.
    """

    def create_font_engine(
        self, writer: 'BasePdfFileWriter', obj_stream=None
    ) -> GlyphAccumulator:
        fh = open(self.font_file, 'rb')
        if (
            obj_stream is None
            and writer.stream_xrefs
            and self.create_objstream_if_needed
        ):
            obj_stream = writer.prepare_object_stream()
        return GlyphAccumulator(
            writer=writer,
            font_handle=fh,
            font_size=self.font_size,
            ot_script_tag=self.ot_script_tag,
            ot_language_tag=self.ot_language_tag,
            writing_direction=self.writing_direction,
            bcp47_lang_code=self.bcp47_lang_code,
            obj_stream=obj_stream,
        )
