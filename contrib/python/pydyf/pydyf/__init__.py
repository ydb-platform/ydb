"""
A low-level PDF generator.

"""

import base64
import re
import zlib
from codecs import BOM_UTF16_BE
from hashlib import md5
from math import ceil, log

VERSION = __version__ = '0.12.1'


def _to_bytes(item):
    """Convert item to bytes."""
    if isinstance(item, bytes):
        return item
    elif isinstance(item, float):
        if item.is_integer():
            return str(int(item)).encode('ascii')
        else:
            return f'{item:f}'.rstrip('0').encode('ascii')
    elif isinstance(item, Object):
        return item.data
    return str(item).encode('ascii')


class Object:
    """Base class for PDF objects."""
    def __init__(self):
        #: Number of the object.
        self.number = None
        #: Position in the PDF of the object.
        self.offset = 0
        #: Version number of the object, non-negative.
        self.generation = 0
        #: Indicate if an object is used (``'n'``), or has been deleted
        #: and therefore is free (``'f'``).
        self.free = 'n'

    @property
    def indirect(self):
        """Indirect representation of an object."""
        header = f'{self.number} {self.generation} obj\n'.encode()
        return header + self.data + b'\nendobj'

    @property
    def reference(self):
        """Object identifier."""
        return f'{self.number} {self.generation} R'.encode()

    @property
    def data(self):
        """Data contained in the object. Shall be defined in each subclass."""
        raise NotImplementedError()

    @property
    def compressible(self):
        """Whether the object can be included in an object stream."""
        return not self.generation and not isinstance(self, Stream)


class Dictionary(Object, dict):
    """PDF Dictionary object."""
    def __init__(self, values=None):
        Object.__init__(self)
        dict.__init__(self, values or {})

    @property
    def data(self):
        result = [
            b'/' + _to_bytes(key) + b' ' + _to_bytes(value)
            for key, value in self.items()]
        return b'<<' + b''.join(result) + b'>>'


class Stream(Object):
    """PDF Stream object."""
    def __init__(self, stream=None, extra=None, compress=False):
        super().__init__()
        #: Python array of data composing stream.
        self.stream = stream or []
        #: Metadata containing at least the length of the Stream.
        self.extra = extra or {}
        #: Compress the stream data if set to ``True``. Default is ``False``.
        self.compress = compress

    def begin_marked_content(self, tag, property_list=None):
        """Begin marked-content sequence."""
        self.stream.append(f'/{tag}')
        if property_list is None:
            self.stream.append(b'BMC')
        else:
            self.stream.append(property_list)
            self.stream.append(b'BDC')

    def begin_text(self):
        """Begin a text object."""
        self.stream.append(b'BT')

    def clip(self, even_odd=False):
        """Modify current clipping path by intersecting it with current path.

        Use the nonzero winding number rule to determine which regions lie
        inside the clipping path by default.

        Use the even-odd rule if ``even_odd`` set to ``True``.

        """
        self.stream.append(b'W*' if even_odd else b'W')

    def close(self):
        """Close current subpath.

        Append a straight line segment from the current point to the starting
        point of the subpath.

        """
        self.stream.append(b'h')

    def curve_to(self, x1, y1, x2, y2, x3, y3):
        """Add cubic Bézier curve to current path.

        The curve shall extend from ``(x3, y3)`` using ``(x1, y1)`` and ``(x2,
        y2)`` as the Bézier control points.

        """
        self.stream.append(b' '.join((
            _to_bytes(x1), _to_bytes(y1),
            _to_bytes(x2), _to_bytes(y2),
            _to_bytes(x3), _to_bytes(y3), b'c')))

    def curve_start_to(self, x2, y2, x3, y3):
        """Add cubic Bézier curve to current path

        The curve shall extend to ``(x3, y3)`` using the current point and
        ``(x2, y2)`` as the Bézier control points.

        """
        self.stream.append(b' '.join((
            _to_bytes(x2), _to_bytes(y2),
            _to_bytes(x3), _to_bytes(y3), b'v')))

    def curve_end_to(self, x1, y1, x3, y3):
        """Add cubic Bézier curve to current path

        The curve shall extend to ``(x3, y3)`` using `(x1, y1)`` and ``(x3,
        y3)`` as the Bézier control points.

        """
        self.stream.append(b' '.join((
            _to_bytes(x1), _to_bytes(y1),
            _to_bytes(x3), _to_bytes(y3), b'y')))

    def draw_x_object(self, reference):
        """Draw object given by reference."""
        self.stream.append(b'/' + _to_bytes(reference) + b' Do')

    def end(self):
        """End path without filling or stroking."""
        self.stream.append(b'n')

    def end_marked_content(self):
        """End marked-content sequence."""
        self.stream.append(b'EMC')

    def end_text(self):
        """End text object."""
        self.stream.append(b'ET')

    def fill(self, even_odd=False):
        """Fill path using nonzero winding rule.

        Use even-odd rule if ``even_odd`` is set to ``True``.

        """
        self.stream.append(b'f*' if even_odd else b'f')

    def fill_and_stroke(self, even_odd=False):
        """Fill and stroke path usign nonzero winding rule.

        Use even-odd rule if ``even_odd`` is set to ``True``.

        """
        self.stream.append(b'B*' if even_odd else b'B')

    def fill_stroke_and_close(self, even_odd=False):
        """Fill, stroke and close path using nonzero winding rule.

        Use even-odd rule if ``even_odd`` is set to ``True``.

        """
        self.stream.append(b'b*' if even_odd else b'b')

    def inline_image(self, width, height, color_space, bpc, raw_data):
        """Add an inline image.

        :param width: The width of the image.
        :type width: :obj:`int`
        :param height: The height of the image.
        :type height: :obj:`int`
        :param colorspace: The color space of the image, f.e. RGB, Gray.
        :type colorspace: :obj:`str`
        :param bpc: The bits per component. 1 for BW, 8 for grayscale.
        :type bpc: :obj:`int`
        :param raw_data: The raw pixel data.

        """
        data = zlib.compress(raw_data) if self.compress else raw_data
        a85_data = base64.a85encode(data) + b'~>'
        self.stream.append(b' '.join((
            b'BI',
            b'/W', _to_bytes(width),
            b'/H', _to_bytes(height),
            b'/BPC', _to_bytes(bpc),
            b'/CS',
            b'/Device' + _to_bytes(color_space),
            b'/F',
            b'[/A85 /Fl]' if self.compress else b'/A85',
            b'/L', _to_bytes(len(a85_data)),
            b'ID',
            a85_data,
            b'EI',
        )))

    def line_to(self, x, y):
        """Add line from current point to point ``(x, y)``."""
        self.stream.append(b' '.join((_to_bytes(x), _to_bytes(y), b'l')))

    def move_to(self, x, y):
        """Begin new subpath by moving current point to ``(x, y)``."""
        self.stream.append(b' '.join((_to_bytes(x), _to_bytes(y), b'm')))

    def move_text_to(self, x, y):
        """Move text to next line at ``(x, y)`` distance from previous line."""
        self.stream.append(b' '.join((_to_bytes(x), _to_bytes(y), b'Td')))

    def paint_shading(self, name):
        """Paint shape and color shading using shading dictionary ``name``."""
        self.stream.append(b'/' + _to_bytes(name) + b' sh')

    def pop_state(self):
        """Restore graphic state."""
        self.stream.append(b'Q')

    def push_state(self):
        """Save graphic state."""
        self.stream.append(b'q')

    def rectangle(self, x, y, width, height):
        """Add rectangle to current path as complete subpath.

        ``(x, y)`` is the lower-left corner and width and height the
        dimensions.

        """
        self.stream.append(b' '.join((
            _to_bytes(x), _to_bytes(y),
            _to_bytes(width), _to_bytes(height), b're')))

    def set_color_rgb(self, r, g, b, stroke=False):
        """Set RGB color for nonstroking operations.

        Set RGB color for stroking operations instead if ``stroke`` is set to
        ``True``.

        """
        self.stream.append(b' '.join((
            _to_bytes(r), _to_bytes(g), _to_bytes(b),
            (b'RG' if stroke else b'rg'))))

    def set_color_space(self, space, stroke=False):
        """Set the nonstroking color space.

        If stroke is set to ``True``, set the stroking color space instead.

        """
        self.stream.append(
            b'/' + _to_bytes(space) + b' ' + (b'CS' if stroke else b'cs'))

    def set_color_special(self, name, stroke=False, *operands):
        """Set special color for nonstroking operations.

        Set special color for stroking operation if ``stroke`` is set to ``True``.

        """
        if name:
            operands = (*operands, b'/' + _to_bytes(name))
        self.stream.append(
            b' '.join(_to_bytes(operand) for operand in operands) + b' ' +
            (b'SCN' if stroke else b'scn'))

    def set_dash(self, dash_array, dash_phase):
        """Set dash line pattern.

        :param dash_array: Dash pattern.
        :type dash_array: :term:`iterable`
        :param dash_phase: Start of dash phase.
        :type dash_phase: :obj:`int`

        """
        self.stream.append(b' '.join((
            Array(dash_array).data, _to_bytes(dash_phase), b'd')))

    def set_font_size(self, font, size):
        """Set font name and size."""
        self.stream.append(
            b'/' + _to_bytes(font) + b' ' + _to_bytes(size) + b' Tf')

    def set_text_rendering(self, mode):
        """Set text rendering mode."""
        self.stream.append(_to_bytes(mode) + b' Tr')

    def set_text_rise(self, height):
        """Set text rise."""
        self.stream.append(_to_bytes(height) + b' Ts')

    def set_line_cap(self, line_cap):
        """Set line cap style."""
        self.stream.append(_to_bytes(line_cap) + b' J')

    def set_line_join(self, line_join):
        """Set line join style."""
        self.stream.append(_to_bytes(line_join) + b' j')

    def set_line_width(self, width):
        """Set line width."""
        self.stream.append(_to_bytes(width) + b' w')

    def set_matrix(self, a, b, c, d, e, f):
        """Set current transformation matrix.

        :param a: Top left number in the matrix.
        :type a: :obj:`int` or :obj:`float`
        :param b: Top middle number in the matrix.
        :type b: :obj:`int` or :obj:`float`
        :param c: Middle left number in the matrix.
        :type c: :obj:`int` or :obj:`float`
        :param d: Middle middle number in the matrix.
        :type d: :obj:`int` or :obj:`float`
        :param e: Bottom left number in the matrix.
        :type e: :obj:`int` or :obj:`float`
        :param f: Bottom middle number in the matrix.
        :type f: :obj:`int` or :obj:`float`

        """
        self.stream.append(b' '.join((
            _to_bytes(a), _to_bytes(b), _to_bytes(c),
            _to_bytes(d), _to_bytes(e), _to_bytes(f), b'cm')))

    def set_miter_limit(self, miter_limit):
        """Set miter limit."""
        self.stream.append(_to_bytes(miter_limit) + b' M')

    def set_state(self, state_name):
        """Set specified parameters in graphic state.

        :param state_name: Name of the graphic state.

        """
        self.stream.append(b'/' + _to_bytes(state_name) + b' gs')

    def set_text_matrix(self, a, b, c, d, e, f):
        """Set current text and text line transformation matrix.

        :param a: Top left number in the matrix.
        :type a: :obj:`int` or :obj:`float`
        :param b: Top middle number in the matrix.
        :type b: :obj:`int` or :obj:`float`
        :param c: Middle left number in the matrix.
        :type c: :obj:`int` or :obj:`float`
        :param d: Middle middle number in the matrix.
        :type d: :obj:`int` or :obj:`float`
        :param e: Bottom left number in the matrix.
        :type e: :obj:`int` or :obj:`float`
        :param f: Bottom middle number in the matrix.
        :type f: :obj:`int` or :obj:`float`

        """
        self.stream.append(b' '.join((
            _to_bytes(a), _to_bytes(b), _to_bytes(c),
            _to_bytes(d), _to_bytes(e), _to_bytes(f), b'Tm')))

    def show_text(self, text):
        """Show text strings with individual glyph positioning."""
        self.stream.append(b'[' + _to_bytes(text) + b'] TJ')

    def show_text_string(self, text):
        """Show single text string."""
        self.stream.append(String(text).data + b' Tj')

    def stroke(self):
        """Stroke path."""
        self.stream.append(b'S')

    def stroke_and_close(self):
        """Stroke and close path."""
        self.stream.append(b's')

    @property
    def data(self):
        stream = b'\n'.join(_to_bytes(item) for item in self.stream)
        extra = Dictionary(self.extra.copy())
        if self.compress:
            extra['Filter'] = '/FlateDecode'
            compressobj = zlib.compressobj(level=9)
            stream = compressobj.compress(stream)
            stream += compressobj.flush()
        extra['Length'] = len(stream)
        return b'\n'.join((extra.data, b'stream', stream, b'endstream'))


class String(Object):
    """PDF String object."""
    def __init__(self, string=''):
        super().__init__()
        #: Unicode string.
        self.string = string

    @property
    def data(self):
        try:
            # "A literal string is written as an arbitrary number of characters
            # enclosed in parentheses. Any characters may appear in a string
            # except unbalanced parentheses and the backslash, which must be
            # treated specially."
            escaped = re.sub(rb'([\\\(\)])', rb'\\\1', _to_bytes(self.string))
            return b'(' + escaped + b')'
        except UnicodeEncodeError:
            encoded = BOM_UTF16_BE + str(self.string).encode('utf-16-be')
            return b'<' + encoded.hex().encode() + b'>'


class Array(Object, list):
    """PDF Array object."""
    def __init__(self, array=None):
        Object.__init__(self)
        list.__init__(self, array or [])

    @property
    def data(self):
        return b'[' + b' '.join(_to_bytes(child) for child in self) + b']'


class PDF:
    """PDF document."""
    def __init__(self):
        """Create a PDF document."""

        #: Python :obj:`list` containing the PDF’s objects.
        self.objects = []

        zero_object = Object()
        zero_object.generation = 65535
        zero_object.free = 'f'
        self.add_object(zero_object)

        #: PDF :class:`Dictionary` containing the PDF’s pages.
        self.pages = Dictionary({
            'Type': '/Pages',
            'Kids': Array([]),
            'Count': 0,
        })
        self.add_object(self.pages)

        #: PDF :class:`Dictionary` containing the PDF’s metadata.
        self.info = Dictionary({})

        #: PDF :class:`Dictionary` containing references to the other objects.
        self.catalog = Dictionary({
            'Type': '/Catalog',
            'Pages': self.pages.reference,
        })
        self.add_object(self.catalog)

        #: Current position in the PDF.
        self.current_position = 0
        #: Position of the cross reference table.
        self.xref_position = None

    def add_page(self, page):
        """Add page to the PDF.

        :param page: New page.
        :type page: :class:`Dictionary`

        """
        self.pages['Count'] += 1
        self.add_object(page)
        self.pages['Kids'].extend([page.number, 0, 'R'])

    def add_object(self, object_):
        """Add object to the PDF."""
        object_.number = len(self.objects)
        self.objects.append(object_)

    @property
    def page_references(self):
        return tuple(
            f'{object_number} 0 R'.encode('ascii')
            for object_number in self.pages['Kids'][::3])

    def write_line(self, content, output):
        """Write line to output.

        :param content: Content to write.
        :type content: :obj:`bytes`
        :param output: Output stream.
        :type output: binary :term:`file object`

        """
        self.current_position += len(content) + 1
        output.write(content + b'\n')

    def write(self, output, version=b'1.7', identifier=False, compress=False):
        """Write PDF to output.

        :param output: Output stream.
        :type output: binary :term:`file object`
        :param bytes version: PDF version.
        :param identifier: PDF file identifier. Default is :obj:`False`
          to include no identifier, can be set to :obj:`True` to generate an
          automatic identifier.
        :type identifier: :obj:`bytes` or :obj:`bool`
        :param bool compress: whether the PDF uses a compressed object stream.

        """
        # Convert version and identifier to bytes
        version = _to_bytes(version or b'1.7')  # Force 1.7 when None
        if identifier not in (False, True, None):
            identifier = _to_bytes(identifier)

        # Add info object if needed
        if self.info:
            self.add_object(self.info)

        # Write header
        self.write_line(b'%PDF-' + version, output)
        self.write_line(b'%\xf0\x9f\x96\xa4', output)

        if version >= b'1.5' and compress:
            # Store compressed objects for later and write other ones in PDF
            compressed_objects = []
            for object_ in self.objects:
                if object_.free == 'f':
                    continue
                if object_.compressible:
                    compressed_objects.append(object_)
                else:
                    object_.offset = self.current_position
                    self.write_line(object_.indirect, output)

            # Write compressed objects in object stream
            stream = [[]]
            position = 0
            for i, object_ in enumerate(compressed_objects):
                data = object_.data
                stream.append(data)
                stream[0].append(object_.number)
                stream[0].append(position)
                position += len(data) + 1
            stream[0] = ' '.join(str(i) for i in stream[0])
            extra = {
                'Type': '/ObjStm',
                'N': len(compressed_objects),
                'First': len(stream[0]) + 1,
            }
            object_stream = Stream(stream, extra, compress)
            object_stream.offset = self.current_position
            self.add_object(object_stream)
            self.write_line(object_stream.indirect, output)

            # Write cross-reference stream
            xref = []
            dict_index = 0
            for object_ in self.objects:
                if object_.compressible:
                    xref.append((2, object_stream.number, dict_index))
                    dict_index += 1
                else:
                    xref.append((
                        bool(object_.number), object_.offset, object_.generation))
            xref.append((1, self.current_position, 0))

            field2_size = ceil(log(self.current_position + 1, 256))
            max_generation = max(
                object_.generation for object_ in self.objects)
            field3_size = ceil(log(
                max(max_generation, len(compressed_objects)) + 1, 256))
            xref_lengths = (1, field2_size, field3_size)
            xref_stream = b''.join(
                value.to_bytes(length, 'big')
                for line in xref for length, value in zip(xref_lengths, line))
            extra = {
                'Type': '/XRef',
                'Index': Array((0, len(self.objects) + 1)),
                'W': Array(xref_lengths),
                'Size': len(self.objects) + 1,
                'Root': self.catalog.reference,
            }
            if self.info:
                extra['Info'] = self.info.reference
            if identifier:
                data = b''.join(obj.data for obj in self.objects if obj.free != 'f')
                data_hash = md5(data).hexdigest().encode()
                if identifier is True:
                    identifier = data_hash
                extra['ID'] = Array((String(identifier).data, String(data_hash).data))
            dict_stream = Stream([xref_stream], extra, compress)
            self.xref_position = dict_stream.offset = self.current_position
            self.add_object(dict_stream)
            self.write_line(dict_stream.indirect, output)
        else:
            # Write all non-free PDF objects
            for object_ in self.objects:
                if object_.free == 'f':
                    continue
                object_.offset = self.current_position
                self.write_line(object_.indirect, output)

            # Write cross-reference table
            self.xref_position = self.current_position
            self.write_line(b'xref', output)
            self.write_line(f'0 {len(self.objects)}'.encode(), output)
            for object_ in self.objects:
                self.write_line(
                    (f'{object_.offset:010} {object_.generation:05} '
                     f'{object_.free} ').encode(), output)

            # Write trailer
            self.write_line(b'trailer', output)
            self.write_line(b'<<', output)
            self.write_line(f'/Size {len(self.objects)}'.encode(), output)
            self.write_line(b'/Root ' + self.catalog.reference, output)
            if self.info:
                self.write_line(b'/Info ' + self.info.reference, output)
            if identifier:
                data = b''.join(
                    obj.data for obj in self.objects if obj.free != 'f')
                data_hash = md5(data).hexdigest().encode()
                if identifier is True:
                    identifier = data_hash
                self.write_line(
                    b'/ID [' + String(identifier).data + b' ' +
                    String(data_hash).data + b']', output)
            self.write_line(b'>>', output)

        self.write_line(b'startxref', output)
        self.write_line(f'{self.xref_position}'.encode(), output)
        self.write_line(b'%%EOF', output)
