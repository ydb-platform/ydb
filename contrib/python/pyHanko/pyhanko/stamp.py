"""
Utilities for stamping PDF files.

Here 'stamping' loosely refers to adding small overlays (QR codes, text boxes,
etc.) on top of already existing content in PDF files.

The code in this module is also used by the :mod:`.sign` module to render
signature appearances.
"""

import enum
import uuid
from binascii import hexlify
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional, Tuple

import qrcode
import tzlocal

from pyhanko.config.api import ConfigurableMixin
from pyhanko.config.errors import ConfigurationError
from pyhanko.pdf_utils import content, generic, layout
from pyhanko.pdf_utils.generic import IndirectObject, pdf_name, pdf_string
from pyhanko.pdf_utils.incremental_writer import IncrementalPdfFileWriter
from pyhanko.pdf_utils.layout import LayoutError
from pyhanko.pdf_utils.misc import rd
from pyhanko.pdf_utils.qr import PdfFancyQRImage, PdfStreamQRImage
from pyhanko.pdf_utils.text import DEFAULT_BOX_LAYOUT, TextBox, TextBoxStyle
from pyhanko.pdf_utils.writer import BasePdfFileWriter, init_xobject_dictionary

__all__ = [
    "AnnotAppearances",
    "BaseStampStyle",
    "TextStampStyle",
    "QRStampStyle",
    "StaticStampStyle",
    "QRPosition",
    "BaseStamp",
    "TextStamp",
    "QRStamp",
    "StaticContentStamp",
    "text_stamp_file",
    "qr_stamp_file",
    "STAMP_ART_CONTENT",
]


class AnnotAppearances:
    """
    Convenience abstraction to set up an appearance dictionary for a PDF
    annotation.

    Annotations can have three appearance streams, which can be roughly
    characterised as follows:

    * *normal*: the only required one, and the default one;
    * *rollover*: used when mousing over the annotation;
    * *down*: used when clicking the annotation.

    These are given as references to form XObjects.

    .. note::
        This class only covers the simple case of an appearance dictionary
        for an annotation with only one appearance state.

    See § 12.5.5 in ISO 32000-1 for further information.
    """

    def __init__(
        self,
        normal: generic.IndirectObject,
        rollover: Optional[generic.IndirectObject] = None,
        down: Optional[generic.IndirectObject] = None,
    ):
        self.normal = normal
        self.rollover = rollover
        self.down = down

    def as_pdf_object(self) -> generic.DictionaryObject:
        """
        Convert the :class:`.AnnotationAppearances` instance to a PDF
        dictionary.

        :return:
            A :class:`~.pdf_utils.generic.DictionaryObject` that can be plugged
            into the ``/AP`` entry of an annotation dictionary.
        """

        res = generic.DictionaryObject({pdf_name('/N'): self.normal})
        if self.rollover is not None:
            res[pdf_name('/R')] = self.rollover
        if self.down is not None:
            res[pdf_name('/D')] = self.down
        return res


def _get_background_content(bg_spec) -> content.PdfContent:
    if not isinstance(bg_spec, str):
        raise ConfigurationError("Background specification must be a string")
    # 'special' value to use the stamp vector image baked into
    # the module
    if bg_spec == '__stamp__':
        return STAMP_ART_CONTENT
    elif bg_spec.endswith('.pdf'):
        # import first page of PDF as background
        return content.ImportedPdfPage(bg_spec)
    else:
        from PIL import Image

        from pyhanko.pdf_utils.images import PdfImage

        img = Image.open(bg_spec)
        # Setting the writer can be delayed
        return PdfImage(img, writer=None)


@dataclass(frozen=True)
class BaseStampStyle(ConfigurableMixin):
    """
    Base class for stamp styles.
    """

    border_width: int = 3
    """
    Border width in user units (for the stamp, not the text box).
    """

    background: Optional[content.PdfContent] = None
    """
    :class:`~.pdf_utils.content.PdfContent` instance that will be used to render
    the stamp's background.
    """

    background_layout: layout.SimpleBoxLayoutRule = layout.SimpleBoxLayoutRule(
        x_align=layout.AxisAlignment.ALIGN_MID,
        y_align=layout.AxisAlignment.ALIGN_MID,
        margins=layout.Margins.uniform(5),
    )
    """
    Layout rule to render the background inside the stamp's bounding box.
    Only used if the background has a fully specified :attr:`PdfContent.box`.

    Otherwise, the renderer will position the cursor at
    ``(left_margin, bottom_margin)`` and render the content as-is.
    """

    background_opacity: float = 0.6
    """
    Opacity value to render the background at. This should be a floating-point
    number between `0` and `1`.
    """

    @classmethod
    def process_entries(cls, config_dict):
        """
        This implementation of :meth:`process_entries` processes the
        :attr:`background` configuration value.
        This can either be a path to an image file, in which case it will
        be turned into an instance of :class:`~.pdf_utils.images.PdfImage`,
        or the special value ``__stamp__``, which is an alias for
        :const:`~pyhanko.stamp.STAMP_ART_CONTENT`.
        """

        super().process_entries(config_dict)
        bg_spec = None
        try:
            bg_spec = config_dict['background']
        except KeyError:
            pass
        if bg_spec is not None:
            config_dict['background'] = _get_background_content(bg_spec)

    def create_stamp(
        self,
        writer: BasePdfFileWriter,
        box: layout.BoxConstraints,
        text_params: dict,
    ) -> 'BaseStamp':
        raise NotImplementedError


@dataclass(frozen=True)
class StaticStampStyle(BaseStampStyle):
    """
    Stamp style that does not include any custom parts; it only renders
    the background.
    """

    background_opacity: float = 1.0
    """
    Opacity value to render the background at. This should be a floating-point
    number between `0` and `1`.
    """

    @classmethod
    def from_pdf_file(
        cls, file_name, page_ix=0, **kwargs
    ) -> 'StaticStampStyle':
        """
        Create a :class:`StaticStampStyle` from a page from an external PDF
        document. This is a convenience wrapper around
        :class:`~content.ImportedPdfContent`.

        The remaining keyword arguments are passed to
        :class:`StaticStampStyle`'s init method.

        :param file_name:
            File name of the external PDF document.
        :param page_ix:
            Page index to import. The default is ``0``, i.e. the first page.
        """
        return StaticStampStyle(
            background=content.ImportedPdfPage(file_name, page_ix=page_ix),
            **kwargs,
        )

    def create_stamp(
        self,
        writer: BasePdfFileWriter,
        box: layout.BoxConstraints,
        text_params: dict,
    ) -> 'StaticContentStamp':
        return StaticContentStamp(writer=writer, style=self, box=box)


@dataclass(frozen=True)
class TextStampStyle(BaseStampStyle):
    """
    Style for text-based stamps.

    Roughly speaking, this stamp type renders some predefined (but parametrised)
    piece of text inside a text box, and possibly applies a background to it.
    """

    text_box_style: TextBoxStyle = TextBoxStyle()
    """
    The text box style for the internal text box used.
    """

    inner_content_layout: Optional[layout.SimpleBoxLayoutRule] = None
    """
    Rule determining the position and alignment of the inner text box within
    the stamp.
    
    .. warning::
        This only affects the position of the box, not the alignment of the
        text within.
    """

    stamp_text: str = '%(ts)s'
    """
    Text template for the stamp. The template can contain an interpolation
    parameter ``ts`` that will be replaced by the stamping time.
    
    Additional parameters may be added if necessary. Values for these must be
    passed to the :meth:`~.TextStamp.__init__` method of the 
    :class:`.TextStamp` class in the ``text_params`` argument.
    """

    timestamp_format: str = '%Y-%m-%d %H:%M:%S %Z'
    """
    Datetime format used to render the timestamp.
    """

    def create_stamp(
        self,
        writer: BasePdfFileWriter,
        box: layout.BoxConstraints,
        text_params: dict,
    ) -> 'TextStamp':
        return TextStamp(
            writer=writer, style=self, box=box, text_params=text_params
        )


class QRPosition(enum.Enum):
    """
    QR positioning constants, with the corresponding default content layout
    rule.
    """

    LEFT_OF_TEXT = layout.SimpleBoxLayoutRule(
        x_align=layout.AxisAlignment.ALIGN_MIN,
        y_align=layout.AxisAlignment.ALIGN_MID,
    )
    RIGHT_OF_TEXT = layout.SimpleBoxLayoutRule(
        x_align=layout.AxisAlignment.ALIGN_MAX,
        y_align=layout.AxisAlignment.ALIGN_MID,
    )
    ABOVE_TEXT = layout.SimpleBoxLayoutRule(
        y_align=layout.AxisAlignment.ALIGN_MAX,
        x_align=layout.AxisAlignment.ALIGN_MID,
    )
    BELOW_TEXT = layout.SimpleBoxLayoutRule(
        y_align=layout.AxisAlignment.ALIGN_MIN,
        x_align=layout.AxisAlignment.ALIGN_MID,
    )

    @property
    def horizontal_flow(self):
        return self in (QRPosition.LEFT_OF_TEXT, QRPosition.RIGHT_OF_TEXT)

    @classmethod
    def from_config(cls, config_str) -> 'QRPosition':
        """
        Convert from a configuration string.

        :param config_str:
            A string: 'left', 'right', 'top', 'bottom'
        :return:
            An :class:`.QRPosition` value.
        :raise ConfigurationError: on unexpected string inputs.
        """
        try:
            return {
                'left': QRPosition.LEFT_OF_TEXT,
                'right': QRPosition.RIGHT_OF_TEXT,
                'top': QRPosition.ABOVE_TEXT,
                'bottom': QRPosition.BELOW_TEXT,
            }[config_str.lower()]
        except KeyError:
            raise ConfigurationError(
                f"'{config_str}' is not a valid QR position setting; valid "
                f"values are 'left', 'right', 'top', 'bottom'"
            )


DEFAULT_QR_SCALE = 0.2
"""
If the layout & other bounding boxes don't impose another size requirement,
render QR codes at ~20% of their natural size in QR canvas units. At scale 1,
this produces codes of about 2cm x 2cm for a 25-module QR code, which is
probably OK.
"""


@dataclass(frozen=True)
class QRStampStyle(TextStampStyle):
    """
    Style for text-based stamps together with a QR code.

    This is exactly the same as a text stamp, except that the text box
    is rendered with a QR code to the left of it.
    """

    innsep: int = 3
    """
    Inner separation inside the stamp.
    """

    stamp_text: str = (
        "Digital version available at\n"
        "this url: %(url)s\n"
        "Timestamp: %(ts)s"
    )
    """
    Text template for the stamp.
    The description of :attr:`.TextStampStyle.stamp_text` still applies, but
    an additional default interpolation parameter ``url`` is available.
    This parameter will be replaced with the URL that the QR code points to.
    """

    qr_inner_size: Optional[int] = None
    """
    Size of the QR code in the inner layout. By default, this is in user units,
    but if the stamp has a fully defined bounding box, it may be rescaled
    depending on :attr:`inner_content_layout`.
    
    If unspecified, a reasonable default will be used.
    """

    qr_position: QRPosition = QRPosition.LEFT_OF_TEXT
    """
    Position of the QR code relative to the text box.
    """

    qr_inner_content: Optional[content.PdfContent] = None
    """
    Inner graphics content to be included in the QR code (experimental).
    """

    @classmethod
    def process_entries(cls, config_dict):
        super().process_entries(config_dict)

        try:
            qr_pos = config_dict['qr_position']
            config_dict['qr_position'] = QRPosition.from_config(qr_pos)
        except KeyError:
            pass

    def create_stamp(
        self,
        writer: BasePdfFileWriter,
        box: layout.BoxConstraints,
        text_params: dict,
    ) -> 'QRStamp':
        # extract the URL parameter
        try:
            url = text_params.pop('url')
        except KeyError:
            raise layout.LayoutError(
                "Using a QR stamp style requires a 'url' text parameter."
            )
        return QRStamp(
            writer, style=self, url=url, text_params=text_params, box=box
        )


class BaseStamp(content.PdfContent):
    def __init__(
        self,
        writer: BasePdfFileWriter,
        style,
        box: Optional[layout.BoxConstraints] = None,
    ):
        super().__init__(box=box, writer=writer)
        self.style = style
        self._resources_ready = False
        self._stamp_ref: Optional[IndirectObject] = None

    def _render_background(self):
        bg = self.style.background
        bg.set_writer(self.writer)
        bg_content = bg.render()  # render first, in case the BBox is lazy

        bg_box = bg.box
        if bg_box.width_defined and bg_box.height_defined:
            # apply layout rule
            positioning = self.style.background_layout.fit(
                self.box, bg_box.width, bg_box.height
            )
        else:
            # No idea about the background dimensions, so just use
            # the left/bottom margins and hope for the best
            margins = self.style.background_layout.margins
            positioning = layout.Positioning(
                x_scale=1, y_scale=1, x_pos=margins.left, y_pos=margins.bottom
            )

        # set opacity in graphics state
        opacity = generic.FloatObject(self.style.background_opacity)
        self.set_resource(
            category=content.ResourceType.EXT_G_STATE,
            name=pdf_name('/BackgroundGS'),
            value=generic.DictionaryObject(
                {pdf_name('/CA'): opacity, pdf_name('/ca'): opacity}
            ),
        )

        # Position & render the background
        command = b'q /BackgroundGS gs %s %s Q' % (
            positioning.as_cm(),
            bg_content,
        )
        # we do this after render(), just in case our background resource
        # decides to pull in extra stuff during rendering
        self.import_resources(bg.resources)
        return command

    def _render_inner_content(self):
        raise NotImplementedError

    def render(self):
        command_stream = [b'q']

        inner_content = self._render_inner_content()

        # Now that the inner layout is done, the dimensions of our bounding
        # box should all have been reified. Let's put in the background,
        # if there is one
        if self.style.background:
            command_stream.append(self._render_background())

        # put in the inner content
        if inner_content:
            command_stream.extend(inner_content)

        # draw the border around the stamp
        bbox = self.box
        border_width = self.style.border_width
        if border_width:
            command_stream.append(
                b'%g w 0 0 %g %g re S' % (border_width, bbox.width, bbox.height)
            )
        command_stream.append(b'Q')
        return b' '.join(command_stream)

    def register(self) -> generic.IndirectObject:
        """
        Register the stamp with the writer coupled to this instance, and
        cache the returned reference.

        This works by calling :meth:`.PdfContent.as_form_xobject`.

        :return:
            An indirect reference to the form XObject containing the stamp.
        """
        stamp_ref = self._stamp_ref
        if stamp_ref is None:
            wr = self._ensure_writer
            form_xobj = self.as_form_xobject()
            self._stamp_ref = stamp_ref = wr.add_object(form_xobj)
        return stamp_ref

    def apply(self, dest_page: int, x: int, y: int):
        """
        Apply a stamp to a particular page in the PDF writer attached to this
        :class:`.BaseStamp` instance.

        :param dest_page:
            Index of the page to which the stamp is to be applied
            (starting at `0`).
        :param x:
            Horizontal position of the stamp's lower left corner on the page.
        :param y:
            Vertical position of the stamp's lower left corner on the page.
        :return:
            A reference to the affected page object, together with
            a ``(width, height)`` tuple describing the dimensions of the stamp.
        """
        stamp_ref = self.register()
        resource_name = b'/Stamp' + hexlify(uuid.uuid4().bytes)
        stamp_paint = b'q 1 0 0 1 %g %g cm %s Do Q' % (
            rd(x),
            rd(y),
            resource_name,
        )
        stamp_wrapper_stream = generic.StreamObject(stream_data=stamp_paint)
        resources = generic.DictionaryObject(
            {
                pdf_name('/XObject'): generic.DictionaryObject(
                    {pdf_name(resource_name.decode('ascii')): stamp_ref}
                )
            }
        )
        wr = self.writer
        assert wr is not None
        page_ref = wr.add_stream_to_page(
            dest_page, wr.add_object(stamp_wrapper_stream), resources
        )
        dims = (self.box.width, self.box.height)
        return page_ref, dims

    def as_appearances(self) -> AnnotAppearances:
        """
        Turn this stamp into an appearance dictionary for an annotation
        (or a form field widget), after rendering it.
        Only the normal appearance will be defined.

        :return:
            An instance of :class:`.AnnotAppearances`.
        """
        # TODO support defining overrides/extra's for the rollover/down
        #  appearances in some form
        stamp_ref = self.register()
        return AnnotAppearances(normal=stamp_ref)


class StaticContentStamp(BaseStamp):
    """Class representing stamps with static content."""

    def __init__(
        self,
        writer: BasePdfFileWriter,
        style: StaticStampStyle,
        box: layout.BoxConstraints,
    ):
        if not (box and box.height_defined and box.width_defined):
            raise layout.LayoutError(
                "StaticContentStamp requires a predetermined bounding box."
            )
        super().__init__(box=box, style=style, writer=writer)

    def _render_inner_content(self):
        return []


class TextStamp(BaseStamp):
    """
    Class that renders a text stamp as specified by an instance
    of :class:`.TextStampStyle`.
    """

    def __init__(
        self,
        writer: BasePdfFileWriter,
        style,
        text_params=None,
        box: Optional[layout.BoxConstraints] = None,
    ):
        super().__init__(box=box, style=style, writer=writer)
        self.text_params = text_params

        self.text_box: Optional[TextBox] = None

    def get_default_text_params(self):
        """
        Compute values for the default string interpolation parameters
        to be applied to the template string specified in the stamp
        style. This method does not take into account the ``text_params``
        init parameter yet.

        :return:
            A dictionary containing the parameters and their values.
        """
        ts = datetime.now(tz=tzlocal.get_localzone())
        return {
            'ts': ts.strftime(self.style.timestamp_format),
        }

    def _text_layout(self):
        # Set the contents of the text box
        self.text_box = tb = TextBox(
            self.style.text_box_style,
            writer=self.writer,
            resources=self.resources,
            box=None,
        )
        _text_params = self.get_default_text_params()
        if self.text_params is not None:
            _text_params.update(self.text_params)
        try:
            text = self.style.stamp_text % _text_params
        except KeyError as e:
            raise LayoutError(f"Stamp text parameter '{e.args[0]}' is missing")
        tb.content = text

        # Render the text box in its natural size, we'll deal with
        # the minutiae later
        return tb.render()

    def _inner_layout_natural_size(self):
        # render text
        text_commands = self._text_layout()

        inn_box = self.text_box.box
        return [text_commands], (inn_box.width, inn_box.height)

    def _inner_content_layout_rule(self):
        return self.style.inner_content_layout or DEFAULT_BOX_LAYOUT

    def _render_inner_content(self):
        command_stream = [b'q']

        # compute the inner bounding box
        inn_commands, (
            inn_width,
            inn_height,
        ) = self._inner_layout_natural_size()

        inner_layout = self._inner_content_layout_rule()

        bbox = self.box

        # position the inner box
        inn_position = inner_layout.fit(bbox, inn_width, inn_height)

        command_stream.append(inn_position.as_cm())
        command_stream.extend(inn_commands)
        command_stream.append(b'Q')

        return command_stream


class QRStamp(TextStamp):
    def __init__(
        self,
        writer: BasePdfFileWriter,
        url: str,
        style: QRStampStyle,
        text_params=None,
        box: Optional[layout.BoxConstraints] = None,
    ):
        super().__init__(writer, style, text_params=text_params, box=box)
        self.url = url
        self._qr_size = None

    def _inner_content_layout_rule(self):
        style = self.style
        if style.inner_content_layout is not None:
            return style.inner_content_layout

        # choose a reasonable default based on the QR code's relative position
        return style.qr_position.value

    def _inner_layout_natural_size(self) -> Tuple[List[bytes], Tuple[int, int]]:
        text_commands, (
            text_width,
            text_height,
        ) = super()._inner_layout_natural_size()

        qr_ref, natural_qr_size = self._qr_xobject()
        self.set_resource(
            category=content.ResourceType.XOBJECT,
            name=pdf_name('/QR'),
            value=qr_ref,
        )

        style = self.style
        stamp_box = self.box

        # To size the QR code, we proceed as follows:
        #  - If qr_inner_size is not None, use it
        #  - If the stamp has a fully defined bbox already,
        #    make sure it fits within the innseps, and it's not too much smaller
        #    than the text box
        #  - Else, scale down by DEFAULT_QR_SCALE and use that value
        #
        # Note: if qr_inner_size is defined AND the stamp bbox is available
        # already, scaling might still take effect depending on the inner layout
        # rule.
        innsep = style.innsep
        if style.qr_inner_size is not None:
            qr_size = style.qr_inner_size
        elif stamp_box.width_defined and stamp_box.height_defined:
            # ensure that the QR code doesn't shrink too much if the text
            # box is too tall.
            min_dim = min(
                max(stamp_box.height, text_height),
                max(stamp_box.width, text_width),
            )
            qr_size = min_dim - 2 * innsep
        else:
            qr_size = int(round(DEFAULT_QR_SCALE * natural_qr_size))

        qr_innunits_scale = qr_size / natural_qr_size
        qr_padded = qr_size + 2 * innsep
        # Next up: put the QR code and the text box together to get the
        # inner layout bounding box
        if style.qr_position.horizontal_flow:
            inn_width = qr_padded + text_width
            inn_height = max(qr_padded, text_height)
        else:
            inn_width = max(qr_padded, text_width)
            inn_height = qr_padded + text_height
        # grab the base layout rule from the QR position setting
        default_layout: layout.SimpleBoxLayoutRule = style.qr_position.value

        # Fill in the margins
        qr_layout_rule = layout.SimpleBoxLayoutRule(
            x_align=default_layout.x_align,
            y_align=default_layout.y_align,
            margins=layout.Margins.uniform(innsep),
            # There's no point in scaling here, the inner content canvas
            # is always big enough
            inner_content_scaling=layout.InnerScaling.NO_SCALING,
        )

        inner_box = layout.BoxConstraints(inn_width, inn_height)
        qr_inn_pos = qr_layout_rule.fit(inner_box, qr_size, qr_size)

        # we still need to take the axis reversal into account
        # (which also implies an adjustment in the y displacement)
        draw_qr_command = b'q %g 0 0 %g %g %g cm /QR Do Q' % (
            qr_inn_pos.x_scale * qr_innunits_scale,
            qr_inn_pos.y_scale * qr_innunits_scale,
            qr_inn_pos.x_pos,
            qr_inn_pos.y_pos,
        )

        # Time to put in the text box now
        if style.qr_position == QRPosition.LEFT_OF_TEXT:
            tb_margins = layout.Margins(
                left=qr_padded, right=0, top=0, bottom=0
            )
        elif style.qr_position == QRPosition.RIGHT_OF_TEXT:
            tb_margins = layout.Margins(
                right=qr_padded, left=0, top=0, bottom=0
            )
        elif style.qr_position == QRPosition.BELOW_TEXT:
            tb_margins = layout.Margins(
                bottom=qr_padded, right=0, left=0, top=0
            )
        else:
            tb_margins = layout.Margins(
                top=qr_padded, right=0, left=0, bottom=0
            )

        tb_layout_rule = layout.SimpleBoxLayoutRule(
            # flip around the alignment conventions of the default layout
            # to position the text box on the other side
            x_align=default_layout.x_align.flipped,
            y_align=default_layout.y_align.flipped,
            margins=tb_margins,
            inner_content_scaling=layout.InnerScaling.NO_SCALING,
        )

        # position the text box
        text_inn_pos = tb_layout_rule.fit(inner_box, text_width, text_height)

        commands = [draw_qr_command, b'q', text_inn_pos.as_cm()]
        commands.extend(text_commands)
        commands.append(b'Q')
        return commands, (inn_width, inn_height)

    def _qr_xobject(self):
        is_fancy = self.style.qr_inner_content is not None
        err_corr = (
            qrcode.constants.ERROR_CORRECT_H
            if is_fancy
            else qrcode.constants.ERROR_CORRECT_M
        )
        qr = qrcode.QRCode(error_correction=err_corr)
        qr.add_data(self.url)
        qr.make()

        if is_fancy:
            img = qr.make_image(
                image_factory=PdfFancyQRImage,
                version=qr.version,
                center_image=self.style.qr_inner_content,
            )
        else:
            img = qr.make_image(image_factory=PdfStreamQRImage)
        command_stream = img.render_command_stream()

        bbox_size = (qr.modules_count + 2 * qr.border) * qr.box_size
        qr_xobj = init_xobject_dictionary(command_stream, bbox_size, bbox_size)
        qr_xobj.compress()
        return self.writer.add_object(qr_xobj), bbox_size

    def get_default_text_params(self):
        tp = super().get_default_text_params()
        tp['url'] = self.url
        return tp

    def apply(self, dest_page, x, y):
        page_ref, (w, h) = super().apply(dest_page, x, y)
        link_rect = (x, y, x + w, y + h)
        link_annot = generic.DictionaryObject(
            {
                pdf_name('/Type'): pdf_name('/Annot'),
                pdf_name('/Subtype'): pdf_name('/Link'),
                pdf_name('/Rect'): generic.ArrayObject(
                    list(map(generic.FloatObject, link_rect))
                ),
                pdf_name('/A'): generic.DictionaryObject(
                    {
                        pdf_name('/S'): pdf_name('/URI'),
                        pdf_name('/URI'): pdf_string(self.url),
                    }
                ),
            }
        )
        wr = self.writer
        wr.register_annotation(page_ref, wr.add_object(link_annot))
        return page_ref, (w, h)


def _stamp_file(
    input_name: str,
    output_name: str,
    style: TextStampStyle,
    stamp_class,
    dest_page: int,
    x: int,
    y: int,
    **stamp_kwargs,
):
    with open(input_name, 'rb') as fin:
        pdf_out = IncrementalPdfFileWriter(fin, strict=False)
        stamp = stamp_class(writer=pdf_out, style=style, **stamp_kwargs)
        stamp.apply(dest_page, x, y)

        with open(output_name, 'wb') as out:
            pdf_out.write(out)


def text_stamp_file(
    input_name: str,
    output_name: str,
    style: TextStampStyle,
    dest_page: int,
    x: int,
    y: int,
    text_params=None,
):
    """
    Add a text stamp to a file.

    :param input_name:
        Path to the input file.
    :param output_name:
        Path to the output file.
    :param style:
        Text stamp style to use.
    :param dest_page:
        Index of the page to which the stamp is to be applied (starting at `0`).
    :param x:
        Horizontal position of the stamp's lower left corner on the page.
    :param y:
        Vertical position of the stamp's lower left corner on the page.
    :param text_params:
        Additional parameters for text template interpolation.
    """
    _stamp_file(
        input_name,
        output_name,
        style,
        TextStamp,
        dest_page,
        x,
        y,
        text_params=text_params,
    )


def qr_stamp_file(
    input_name: str,
    output_name: str,
    style: QRStampStyle,
    dest_page: int,
    x: int,
    y: int,
    url: str,
    text_params=None,
):
    """
    Add a QR stamp to a file.

    :param input_name:
        Path to the input file.
    :param output_name:
        Path to the output file.
    :param style:
        QR stamp style to use.
    :param dest_page:
        Index of the page to which the stamp is to be applied (starting at `0`).
    :param x:
        Horizontal position of the stamp's lower left corner on the page.
    :param y:
        Vertical position of the stamp's lower left corner on the page.
    :param url:
        URL for the QR code to point to.
    :param text_params:
        Additional parameters for text template interpolation.
    """

    _stamp_file(
        input_name,
        output_name,
        style,
        QRStamp,
        dest_page,
        x,
        y,
        url=url,
        text_params=text_params,
    )


STAMP_ART_CONTENT = content.RawContent(
    box=layout.BoxConstraints(width=100, height=100),
    data=b'''
q 1 0 0 -1 0 100 cm 
0.603922 0.345098 0.54902 rg
3.699 65.215 m 3.699 65.215 2.375 57.277 7.668 51.984 c 12.957 46.695 27.512
 49.34 39.418 41.402 c 39.418 41.402 31.48 40.078 32.801 33.465 c 34.125
 26.852 39.418 28.172 39.418 24.203 c 39.418 20.234 30.156 17.59 30.156
14.945 c 30.156 12.297 28.465 1.715 50 1.715 c 71.535 1.715 69.844 12.297
 69.844 14.945 c 69.844 17.59 60.582 20.234 60.582 24.203 c 60.582 28.172
 65.875 26.852 67.199 33.465 c 68.52 40.078 60.582 41.402 60.582 41.402
c 72.488 49.34 87.043 46.695 92.332 51.984 c 97.625 57.277 96.301 65.215
 96.301 65.215 c h f
3.801 68.734 92.398 7.391 re f
3.801 79.512 92.398 7.391 re f
3.801 90.289 92.398 7.391 re f
Q
''',
)
"""
Hardcoded stamp background that will render a stylised image of a stamp using 
PDF graphics operators (see below).

.. image:: images/stamp-background.svg
   :alt: Standard stamp background
   :align: center
   
"""
