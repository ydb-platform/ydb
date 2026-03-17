"""Utilities related to text rendering & layout."""

from dataclasses import dataclass, field
from typing import Optional

from pyhanko.config.api import ConfigurableMixin
from pyhanko.config.errors import ConfigurationError
from pyhanko.pdf_utils import layout
from pyhanko.pdf_utils.content import PdfContent, PdfResources, ResourceType
from pyhanko.pdf_utils.font import FontEngineFactory, SimpleFontEngineFactory
from pyhanko.pdf_utils.generic import pdf_name


@dataclass(frozen=True)
class TextStyle(ConfigurableMixin):
    """Container for basic test styling settings."""

    font: FontEngineFactory = field(
        default_factory=SimpleFontEngineFactory.default_factory
    )
    """
    The :class:`.FontEngineFactory` to be used for this text style.
    Defaults to Courier (as a non-embedded standard font).
    """

    font_size: int = 10
    """
    Font size to be used.
    """

    leading: Optional[int] = None
    """
    Text leading. If ``None``, the :attr:`font_size` parameter is used instead.
    """

    @classmethod
    def process_entries(cls, config_dict):
        super().process_entries(config_dict)
        try:
            fc = config_dict['font']
            if not isinstance(fc, str) or not (
                fc.endswith('.otf') or fc.endswith('.ttf')
            ):
                raise ConfigurationError(
                    "'font' must be a path to an OpenType or "
                    "TrueType font file."
                )

            from pyhanko.pdf_utils.font.opentype import GlyphAccumulatorFactory

            config_dict['font'] = GlyphAccumulatorFactory(fc)
        except KeyError:
            pass


DEFAULT_BOX_LAYOUT = layout.SimpleBoxLayoutRule(
    x_align=layout.AxisAlignment.ALIGN_MID,
    y_align=layout.AxisAlignment.ALIGN_MID,
)

DEFAULT_TEXT_BOX_MARGIN = 10


@dataclass(frozen=True)
class TextBoxStyle(TextStyle):
    """Extension of :class:`.TextStyle` for use in text boxes."""

    border_width: int = 0
    """
    Border width, if applicable.
    """

    box_layout_rule: Optional[layout.SimpleBoxLayoutRule] = None
    """
    Layout rule to nest the text within its containing box.
    
    .. warning::
        This only affects the position of the text object, not the alignment of
        the text within.
    """

    vertical_text: bool = False
    """
    Switch layout code to vertical mode instead of horizontal mode.
    """


class TextBox(PdfContent):
    """Implementation of a text box that implements the :class:`.PdfContent`
    interface.

    .. note::
        Text boxes currently don't offer automatic word wrapping.
    """

    def __init__(
        self,
        style: TextBoxStyle,
        writer,
        resources: Optional[PdfResources] = None,
        box: Optional[layout.BoxConstraints] = None,
        font_name='F1',
    ):
        super().__init__(resources, writer=writer, box=box)
        self.style = style
        self._content = None
        self._content_lines = self._wrapped_lines = None
        self.font_name = font_name
        self.font_engine = style.font.create_font_engine(writer)
        self._nat_text_height = self._nat_text_width = 0

    def put_string_line(self, txt):
        font_engine = self.font_engine
        shape_result = font_engine.shape(txt)
        x_advance = shape_result.x_advance * self.style.font_size
        y_advance = shape_result.y_advance * self.style.font_size
        ops = shape_result.graphics_ops
        if font_engine.uses_complex_positioning:
            # Tm and Tlm will be at the same position, after the last glyph
            newline_x = -x_advance
            newline_y = -y_advance

            # TODO deal with TTB scripts with LTR line order too, like Mongolian
            vertical = self.style.vertical_text
            leading = self.leading
            if vertical:
                newline_x -= leading
                extent = abs(y_advance)
            else:
                newline_y -= leading
                extent = abs(x_advance)

            ops += b' %g %g Td' % (newline_x, newline_y)
        else:
            ops += b' T*'
            extent = abs(x_advance)

        return ops, extent

    @property
    def content_lines(self):
        """
        :return:
            Text content of the text box, broken up into lines.
        """
        return self._content_lines

    @property
    def content(self):
        """
        :return:
            The actual text content of the text box.
            This is a modifiable property.

            In textboxes that don't have a fixed size, setting this property
            can cause the text box to be resized.
        """
        return self._content

    @content.setter
    def content(self, content):
        # TODO text reflowing logic goes here
        #  (with option to either scale things, or do word wrapping)
        self._content = content

        natural_text_width = 0
        natural_text_height = 0
        leading = self.leading
        lines = []
        vertical = self.style.vertical_text
        for line in content.split('\n'):
            wrapped_line, extent = self.put_string_line(line)
            rounded_extent = int(round(extent))
            if vertical:
                natural_text_height = max(natural_text_height, rounded_extent)
                natural_text_width += leading
            else:
                natural_text_width = max(natural_text_width, rounded_extent)
                natural_text_height += leading
            lines.append(wrapped_line)
        self._wrapped_lines = lines
        self._content_lines = content.split('\n')

        self._nat_text_width = natural_text_width
        self._nat_text_height = natural_text_height

    @property
    def leading(self):
        """
        :return:
            The effective leading value, i.e. the
            :attr:`~.TextStyle.leading` attribute of the associated
            :class:`.TextBoxStyle`, or :attr:`~.TextStyle.font_size` if
            not specified.
        """
        style = self.style
        return style.font_size if style.leading is None else style.leading

    def render(self):
        style = self.style

        self.set_resource(
            category=ResourceType.FONT,
            name=pdf_name('/' + self.font_name),
            value=self.font_engine.as_resource(),
        )
        leading = self.leading

        nat_text_width = self._nat_text_width
        nat_text_height = self._nat_text_height

        vertical = self.style.vertical_text

        box_layout = self.style.box_layout_rule
        if box_layout is None:
            margins = layout.Margins.uniform(DEFAULT_TEXT_BOX_MARGIN)
            box_layout = DEFAULT_BOX_LAYOUT.substitute_margins(margins)

        positioning = box_layout.fit(self.box, nat_text_width, nat_text_height)

        command_stream = []

        # draw border before scaling
        if style.border_width:
            command_stream.append(
                b'q %g w 0 0 %g %g re S Q'
                % (style.border_width, self.box.width, self.box.height)
            )

        # reposition cursor
        command_stream.append(positioning.as_cm())

        command_stream += [
            b'BT',
            b'/%s %d Tf %d TL'
            % (self.font_name.encode('latin1'), style.font_size, leading),
        ]

        # start by moving the cursor to the starting position.
        # In horizontal mode, that's the top left, accounting for leading.
        # In vertical mode, we need the top right.
        if vertical:
            text_cursor_start = b'%g %g Td' % (
                # V-mode leading/baselining is weird like that---the glyph
                # origin is in the middle, so we chop off half of the leading
                nat_text_width * positioning.x_scale - leading / 2,
                nat_text_height * positioning.y_scale,
            )
        else:
            text_cursor_start = b'0 %g Td' % (
                nat_text_height * positioning.y_scale - leading
            )
        command_stream.append(text_cursor_start)

        command_stream.extend(self._wrapped_lines)
        command_stream.append(b'ET')
        return b' '.join(command_stream)
