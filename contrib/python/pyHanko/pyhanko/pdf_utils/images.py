"""Utilities for embedding bitmap image data into PDF files.

The image data handling is done by
`Pillow <https://github.com/python-pillow/Pillow>`_.

.. note::
    Note that also here we only support a subset of what the PDF standard
    provides for. Most RGB and grayscale images (with or without transparency)
    that can be read by PIL/Pillow can be used without issue.
    PNG images with an indexed palette backed by one of these colour spaces
    can also be used.

    Currently there is no support for CMYK images or (direct) support for
    embedding JPEG-encoded image data as such, but these features may be added
    later.

"""

import uuid
from typing import Optional, Union

from . import generic
from .content import PdfContent, PdfResources, ResourceType
from .generic import IndirectObject, pdf_name
from .layout import BoxConstraints
from .writer import BasePdfFileWriter

try:
    from PIL import Image
    from PIL.ImagePalette import ImagePalette
except ImportError as e:  # pragma: nocover
    raise ImportError(
        "pyhanko.pdf_utils.images requires pyHanko to be installed with "
        "the [image-support] option. You can install missing "
        "dependencies by running "
        "\"pip install 'pyHanko[image-support]'\".",
        e,
    )


__all__ = ['pil_image', 'PdfImage']


def pil_image(img: Image.Image, writer: BasePdfFileWriter):
    """
    This function writes a PIL/Pillow :class:`.Image` object to a PDF file
    writer, as an image XObject.

    :param img:
        A Pillow :class:`.Image` object
    :param writer:
        A PDF file writer
    :return:
        A reference to the image XObject written.
    """

    if img.mode not in ('RGB', 'RGBA', 'P', 'PA', 'L', 'LA'):  # pragma: nocover
        raise NotImplementedError

    dict_data = {
        pdf_name('/Type'): pdf_name('/XObject'),
        pdf_name('/Subtype'): pdf_name('/Image'),
        pdf_name('/Width'): generic.NumberObject(img.width),
        pdf_name('/Height'): generic.NumberObject(img.height),
    }

    bpc = generic.NumberObject(8)

    smask_image = None
    if img.mode.endswith('A'):
        # extract the alpha channel, and save it as a separate image object
        smask_pil_image = img.split()[-1]
        assert smask_pil_image.mode == 'L'
        smask_image = pil_image(smask_pil_image, writer)
        # finally, convert to RBG or L as appropriate
        img = img.convert(img.mode[:-1])

    clr_space: Union[generic.NameObject, generic.ArrayObject]
    clr_space = (
        pdf_name('/DeviceGray') if img.mode == 'L' else pdf_name('/DeviceRGB')
    )
    if img.mode == 'P':
        palette: ImagePalette = img.palette
        palette_arr = palette.palette
        if palette.mode != 'RGB':  # pragma: nocover
            raise NotImplementedError
        palette_size = len(palette_arr) // 3
        # declare an indexed colour space based on /DeviceRGB
        # with 'palette_size' colours, with mapping defined as
        # a byte string
        clr_space = generic.ArrayObject(
            [
                pdf_name('/Indexed'),
                pdf_name('/DeviceRGB'),
                generic.NumberObject(palette_size - 1),
                generic.ByteStringObject(palette_arr),
            ]
        )

    if smask_image is not None:
        dict_data[pdf_name('/SMask')] = smask_image

    dict_data[pdf_name('/ColorSpace')] = clr_space
    dict_data[pdf_name('/BitsPerComponent')] = bpc
    # TODO nice to have: I'd like to pack everything into minimal space here
    #   (but the flate compression should deal with it pretty nicely)
    #  NOTE the BPC values allowed by the standard are limited to 1, 2, 4, 8
    #   (and 12, 16, but those aren't allowed by PIL anyway in this context)
    image_bytes = img.tobytes()

    stream = generic.StreamObject(dict_data, stream_data=image_bytes)
    stream.compress()
    return writer.add_object(stream)


class PdfImage(PdfContent):
    """Wrapper class that implements the :class:`.PdfContent` interface for
    image objects.

    .. note::
        Instances of this class are reusable, in the sense that the
        implementation is aware of changes to the associated :attr:`writer`
        object. This allows the same image to be embedded into multiple files
        without instantiating a new :class:`.PdfImage` every time.

    """

    def __init__(
        self,
        image: Union[Image.Image, str],
        writer: Optional[BasePdfFileWriter] = None,
        resources: Optional[PdfResources] = None,
        name: Optional[str] = None,
        opacity=None,
        box: Optional[BoxConstraints] = None,
    ):
        if isinstance(image, str):
            image = Image.open(image)

        self.image: Image.Image = image
        self.name = name or str(uuid.uuid4())
        self.opacity = opacity

        if box is None:
            # assume square pixels
            box = BoxConstraints(self.image.width, self.image.height)
        super().__init__(resources, writer=writer, box=box)
        self._image_ref: Optional[IndirectObject] = None

    @property
    def image_ref(self) -> generic.IndirectObject:
        """Return a reference to the image XObject associated with this
        :class:`.PdfImage` instance.
        If no such reference is available, it will be created using
        :func:`.pil_image`, and the result will be cached until the
        :attr:`writer` attribute changes
        (see :meth:`~pyhanko.pdf_utils.content.PdfContent.set_writer`).

        :return: An indirect reference to an image XObject.
        """
        assert self.writer is not None
        # cache is invalidated if the writer changed
        image_ref = self._image_ref
        if image_ref is None or image_ref.get_pdf_handler() is not self.writer:
            self._image_ref = image_ref = pil_image(self.image, self.writer)
        assert image_ref is not None
        return image_ref

    def render(self) -> bytes:
        img_ref_name = '/Img' + self.name
        self.set_resource(
            category=ResourceType.XOBJECT,
            name=pdf_name(img_ref_name),
            value=self.image_ref,
        )

        opacity = b''
        if self.opacity is not None:
            gs_name = '/GS' + str(uuid.uuid4())
            self.set_resource(
                category=ResourceType.EXT_G_STATE,
                name=pdf_name(gs_name),
                value=generic.DictionaryObject(
                    {pdf_name('/ca'): generic.FloatObject(self.opacity)}
                ),
            )
            opacity = gs_name.encode('ascii') + b' gs'

        # Internally, the image is mapped to the unit square in
        # user coordinates, irrespective of width/height.
        # In particular, we might have to scale the x and y axes differently.
        if not self.box.height_defined:
            self.box.height = self.image.height
        if not self.box.width_defined:
            self.box.width = self.image.width

        draw = b'%g 0 0 %g 0 0 cm %s Do' % (
            self.box.width,
            self.box.height,
            img_ref_name.encode('ascii'),
        )
        return b'q %s %s Q' % (opacity, draw)
