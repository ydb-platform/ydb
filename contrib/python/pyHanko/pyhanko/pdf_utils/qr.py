import itertools
import math
from typing import List, Optional

import qrcode.util
from qrcode.image.base import BaseImage

from pyhanko.pdf_utils.content import PdfContent
from pyhanko.pdf_utils.misc import rd


class PdfStreamQRImage(BaseImage):
    """
    Quick-and-dirty implementation of the Image interface required
    by the qrcode package.
    """

    kind = "PDF"
    allowed_kinds = ("PDF",)
    qr_color = (0, 0, 0)

    def new_image(self, **kwargs):
        return []

    def drawrect(self, row, col):
        self._img.append((row, col))

    def append_single_rect(self, command_stream, row, col):
        command_stream.append(b'%g %g 1 1 re' % (col, row))

    def format_qr_color(self):
        return (b"%g %g %g rg\n" % self.qr_color) + (
            b"%g %g %g RG" % self.qr_color
        )

    def setup_drawing_area(self):
        # start a command stream with fill colour set to black (default)
        # and transform the coordinate system to line up with our grid
        brd = rd(self.border * self.box_size)
        ydiff = rd(self.width * self.box_size)
        cm = f"{rd(self.box_size)} 0 0 {-rd(self.box_size)} {brd} {brd + ydiff} cm"
        return b"%s\n%s" % (self.format_qr_color(), cm.encode('ascii'))

    def render_command_stream(self):
        command_stream = [self.setup_drawing_area()]
        for row, col in self._img:
            # paint a rectangle
            self.append_single_rect(command_stream, row, col)
        command_stream.append(b"f")
        return b'\n'.join(command_stream)

    def save(self, stream, kind=None):
        raise NotImplementedError

    def process(self):
        raise NotImplementedError

    def drawrect_context(self, row, col, active, context):
        return self.drawrect(row, col)  # pragma: nocover


class PdfFancyQRImage(PdfStreamQRImage):
    centerpiece_corner_radius = 0.2

    def __init__(
        self,
        border,
        width,
        box_size,
        *_args,
        version,
        center_image: Optional[PdfContent] = None,
        **kwargs,
    ):
        super().__init__(border, width, box_size, **kwargs)
        self._version = version
        self._centerpiece = center_image

    def save(self, stream, kind=None):
        raise NotImplementedError

    def process(self):
        raise NotImplementedError

    def append_single_rect(self, command_stream, row, col):
        if self.is_position_pattern(row, col):
            return
        command_stream.extend(rounded_square(col, row, 0.9, 0.3))

    def is_major_position_pattern(self, row, col):
        return (
            (row < 7 and col < 7)
            or ((row > self.width - 8) and col < 7)
            or (row < 7 and (col > self.width - 8))
        )

    def _enumerate_alignment_patterns(self):
        adj_ptns = qrcode.util.pattern_position(self._version)
        for pr, pc in itertools.product(adj_ptns, adj_ptns):
            # don't consider the bits that fall within the "big" position
            # patterns
            if self.is_major_position_pattern(pr, pc):
                continue
            yield pr, pc

    def is_position_pattern(self, row, col):
        if self.is_major_position_pattern(row, col):
            return True
        # check whether this pixel is part of an alignment pattern
        return any(
            abs(row - pr) <= 2 and abs(col - pc) <= 2
            for pr, pc in self._enumerate_alignment_patterns()
        )

    def draw_position_patterns(self):
        # draw the surrounding squares of the location patterns first
        #  -> stroked paths (outer squares)
        # (these require drawing at module midpoints)
        command_stream = [b'q\n1 0 0 1 0.5 0.5 cm\n0.7 w']
        sz = self.width
        command_stream.extend(rounded_square(0, 0, 6, 1))
        command_stream.extend(rounded_square(0, sz - 7, 6, 1))
        command_stream.extend(rounded_square(sz - 7, 0, 6, 1))
        for pr, pc in self._enumerate_alignment_patterns():
            command_stream.extend(rounded_square(pr - 2, pc - 2, 4, 0.7))
        command_stream.append(b"S\nQ")

        # draw the inner squares of the location patterns
        command_stream.extend(rounded_square(2, 2, 3, 0.6))
        command_stream.extend(rounded_square(2, sz - 7 + 2, 3, 0.6))
        command_stream.extend(rounded_square(sz - 7 + 2, 2, 3, 0.6))

        for pr, pc in self._enumerate_alignment_patterns():
            command_stream.extend(rounded_square(pr, pc, 1, 0.1))
        command_stream.append(b"f")
        return b"\n".join(command_stream)

    def draw_centerpiece(self):
        c_x, c_y, c_sz = self._measure_out_centerpiece()
        # better hope it's square
        c_w = self._centerpiece.box.width
        c_h = self._centerpiece.box.height

        # draw the border of the centerpiece
        centerpiece_commands = [b"q", b"0.2 w"]
        centerpiece_commands.extend(
            rounded_square(
                c_x, c_y, c_sz, self.centerpiece_corner_radius * c_sz
            )
        )
        centerpiece_commands.append(b"S\nQ\nq")
        # transform back into the internal coordinates of the centerpiece
        # (including any y-axis reversals)
        # -> f"{x_scale} 0 0 {-y_scale} {c_x} {c_y + c_sz} cm"
        x_scale = rd(c_sz / c_w)
        y_scale = rd(c_sz / c_h)
        # COMBINED WITH:
        # we slightly shrink and offset the centerpiece to create
        # a border of sorts
        # -> f"{shrink} 0 0 {shrink} {x_shift} {y_shift} cm"
        shrink = 0.85
        x_shift = rd((1 - shrink) * c_w / 2)
        y_shift = rd((1 - shrink) * c_h / 2)
        centerpiece_commands.append(
            f"{x_scale * shrink} 0 0 {-y_scale * shrink} "
            f"{c_x + x_shift * x_scale} {c_y + c_sz - y_shift * y_scale} "
            f"cm".encode('ascii')
        )

        # resource management left up to the caller
        centerpiece_commands.append(self._centerpiece.render())
        centerpiece_commands.append(b"Q")
        return b"\n".join(centerpiece_commands)

    def _measure_out_centerpiece(self):
        # Centerpiece area takes up a square in the QR code with a side of
        # about 28% the total size of the QR code itself
        c_sz = 0.28 * self.width
        c_x = (self.width - c_sz) / 2
        c_y = (self.width - c_sz) / 2
        return rd(c_x), rd(c_y), rd(c_sz)

    def setup_drawing_area(self):
        basic_setup = super().setup_drawing_area()
        commands = [basic_setup]
        if self._centerpiece is not None:
            # we need to clip out the centerpiece area.
            # we'll do that by creating a clip path that goes around the
            # entire QR code, and a rounded rectangle where the centerpiece
            # would go using opposite orientation.

            # -> clockwise rectangle
            w = rd(self.width)
            # save the state to undo the clipping later
            commands.append(b"q")
            commands.append(b"0.2 w")
            commands.append(
                f"0 0 m 0 {w} l {w} {w} l {w} 0 l h".encode("ascii"),
            )
            c_x, c_y, c_sz = self._measure_out_centerpiece()
            commands.extend(
                rounded_square(
                    c_x, c_y, c_sz, self.centerpiece_corner_radius * c_sz
                )
            )
            commands.append(b"W n")
        return b"\n".join(commands)

    def render_command_stream(self):
        parts = [super().render_command_stream(), self.draw_position_patterns()]
        if self._centerpiece:
            parts.append(b"Q")  # undo clipping
            parts.append(self.draw_centerpiece())

        return b"\n".join(parts)


def rounded_square(
    x_pos: float, y_pos: float, sz: float, rad: float
) -> List[bytes]:
    """
    Add a subpath of a square with rounded corners at the given position.
    Doesn't include any painting or clipping operations.

    The path is drawn counterclockwise.

    :param x_pos:
        The x-coordinate of the enveloping square's lower left corner.
    :param y_pos:
        The y-coordinate of the enveloping square's lower left corner.
    :param sz:
        The side length of the enveloping square.
    :param rad:
        The corner radius.

    :return:
        A list of graphics operators.
    """

    c_off = (4 * (math.sqrt(2) - 1) / 3) * rad

    result = []

    def fmt(x, y):
        px = rd(x_pos + x)
        py = rd(y_pos + y)
        result.append(f"{px} {py} ".encode('ascii'))

    def op(pts, opc: str):
        for x, y in pts:
            fmt(x, y)
        result.append(opc.encode('ascii'))

    def uop(x, y, opc):
        op([(x, y)], opc)

    uop(rad, 0, "m")
    uop(sz - rad, 0, "l")
    op([(sz - c_off, 0), (sz, c_off), (sz, rad)], "c")
    uop(sz, sz - rad, "l")
    op([(sz, sz - c_off), (sz - c_off, sz), (sz - rad, sz)], "c")
    uop(rad, sz, "l")
    op([(c_off, sz), (0, sz - c_off), (0, sz - rad)], "c")
    uop(0, rad, "l")
    op([(0, c_off), (c_off, 0), (rad, 0)], "c")
    result.append(b"h")
    return result
