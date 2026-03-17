# encoding: utf-8

"""ImagePart and related objects."""

from __future__ import division

import hashlib
import os

try:
    from PIL import Image as PIL_Image
except ImportError:
    import Image as PIL_Image

from pptx.compat import BytesIO, is_string
from pptx.opc.package import Part
from pptx.opc.spec import image_content_types
from pptx.util import lazyproperty


class ImagePart(Part):
    """An image part.

    An image part generally has a partname matching the regex
    `ppt/media/image[1-9][0-9]*.*`.
    """

    def __init__(self, partname, content_type, package, blob, filename=None):
        super(ImagePart, self).__init__(partname, content_type, package, blob)
        self._filename = filename

    @classmethod
    def new(cls, package, image):
        """Return new |ImagePart| instance containing `image`.

        `image` is an |Image| object.
        """
        return cls(
            package.next_image_partname(image.ext),
            image.content_type,
            package,
            image.blob,
            image.filename,
        )

    @property
    def desc(self):
        """
        The filename associated with this image, either the filename of
        the original image or a generic name of the form ``image.ext``
        where ``ext`` is appropriate to the image file format, e.g.
        ``'jpg'``. An image created using a path will have that filename; one
        created with a file-like object will have a generic name.
        """
        # return generic filename if original filename is unknown
        if self._filename is None:
            return "image.%s" % self.ext
        return self._filename

    @property
    def ext(self):
        """
        Return file extension for this image e.g. ``'png'``.
        """
        return self.partname.ext

    @property
    def image(self):
        """
        An |Image| object containing the image in this image part.
        """
        return Image(self.blob, self.desc)

    def scale(self, scaled_cx, scaled_cy):
        """
        Return scaled image dimensions in EMU based on the combination of
        parameters supplied. If *scaled_cx* and *scaled_cy* are both |None|,
        the native image size is returned. If neither *scaled_cx* nor
        *scaled_cy* is |None|, their values are returned unchanged. If
        a value is provided for either *scaled_cx* or *scaled_cy* and the
        other is |None|, the missing value is calculated such that the
        image's aspect ratio is preserved.
        """
        image_cx, image_cy = self._native_size

        if scaled_cx is None and scaled_cy is None:
            scaled_cx = image_cx
            scaled_cy = image_cy
        elif scaled_cx is None:
            scaling_factor = float(scaled_cy) / float(image_cy)
            scaled_cx = int(round(image_cx * scaling_factor))
        elif scaled_cy is None:
            scaling_factor = float(scaled_cx) / float(image_cx)
            scaled_cy = int(round(image_cy * scaling_factor))

        return scaled_cx, scaled_cy

    @lazyproperty
    def sha1(self):
        """
        The SHA1 hash digest for the image binary of this image part, like:
        ``'1be010ea47803b00e140b852765cdf84f491da47'``.
        """
        return hashlib.sha1(self._blob).hexdigest()

    @property
    def _dpi(self):
        """
        A (horz_dpi, vert_dpi) 2-tuple (ints) representing the dots-per-inch
        property of this image.
        """
        image = Image.from_blob(self.blob)
        return image.dpi

    @property
    def _native_size(self):
        """
        A (width, height) 2-tuple representing the native dimensions of the
        image in EMU, calculated based on the image DPI value, if present,
        assuming 72 dpi as a default.
        """
        EMU_PER_INCH = 914400
        horz_dpi, vert_dpi = self._dpi
        width_px, height_px = self._px_size

        width = EMU_PER_INCH * width_px / horz_dpi
        height = EMU_PER_INCH * height_px / vert_dpi

        return width, height

    @property
    def _px_size(self):
        """
        A (width, height) 2-tuple representing the dimensions of this image
        in pixels.
        """
        image = Image.from_blob(self.blob)
        return image.size


class Image(object):
    """Immutable value object representing an image such as a JPEG, PNG, or GIF."""

    def __init__(self, blob, filename):
        super(Image, self).__init__()
        self._blob = blob
        self._filename = filename

    @classmethod
    def from_blob(cls, blob, filename=None):
        """Return a new |Image| object loaded from the image binary in *blob*."""
        return cls(blob, filename)

    @classmethod
    def from_file(cls, image_file):
        """
        Return a new |Image| object loaded from *image_file*, which can be
        either a path (string) or a file-like object.
        """
        if is_string(image_file):
            # treat image_file as a path
            with open(image_file, "rb") as f:
                blob = f.read()
            filename = os.path.basename(image_file)
        else:
            # assume image_file is a file-like object
            # ---reposition file cursor if it has one---
            if callable(getattr(image_file, "seek")):
                image_file.seek(0)
            blob = image_file.read()
            filename = None

        return cls.from_blob(blob, filename)

    @property
    def blob(self):
        """
        The binary image bytestream of this image.
        """
        return self._blob

    @lazyproperty
    def content_type(self):
        """
        MIME-type of this image, e.g. ``'image/jpeg'``.
        """
        return image_content_types[self.ext]

    @lazyproperty
    def dpi(self):
        """
        A (horz_dpi, vert_dpi) 2-tuple specifying the dots-per-inch
        resolution of this image. A default value of (72, 72) is used if the
        dpi is not specified in the image file.
        """

        def int_dpi(dpi):
            """
            Return an integer dots-per-inch value corresponding to *dpi*. If
            *dpi* is |None|, a non-numeric type, less than 1 or greater than
            2048, 72 is returned.
            """
            try:
                int_dpi = int(round(float(dpi)))
                if int_dpi < 1 or int_dpi > 2048:
                    int_dpi = 72
            except (TypeError, ValueError):
                int_dpi = 72
            return int_dpi

        def normalize_pil_dpi(pil_dpi):
            """
            Return a (horz_dpi, vert_dpi) 2-tuple corresponding to *pil_dpi*,
            the value for the 'dpi' key in the ``info`` dict of a PIL image.
            If the 'dpi' key is not present or contains an invalid value,
            ``(72, 72)`` is returned.
            """
            if isinstance(pil_dpi, tuple):
                return (int_dpi(pil_dpi[0]), int_dpi(pil_dpi[1]))
            return (72, 72)

        return normalize_pil_dpi(self._pil_props[2])

    @lazyproperty
    def ext(self):
        """
        Canonical file extension for this image e.g. ``'png'``. The returned
        extension is all lowercase and is the canonical extension for the
        content type of this image, regardless of what extension may have
        been used in its filename, if any.
        """
        ext_map = {
            "BMP": "bmp",
            "GIF": "gif",
            "JPEG": "jpg",
            "PNG": "png",
            "TIFF": "tiff",
            "WMF": "wmf",
        }
        format = self._format
        if format not in ext_map:
            tmpl = "unsupported image format, expected one of: %s, got '%s'"
            raise ValueError(tmpl % (ext_map.keys(), format))
        return ext_map[format]

    @property
    def filename(self):
        """
        The filename from the path from which this image was loaded, if
        loaded from the filesystem. |None| if no filename was used in
        loading, such as when loaded from an in-memory stream.
        """
        return self._filename

    @lazyproperty
    def sha1(self):
        """
        SHA1 hash digest of the image blob
        """
        return hashlib.sha1(self._blob).hexdigest()

    @lazyproperty
    def size(self):
        """
        A (width, height) 2-tuple specifying the dimensions of this image in
        pixels.
        """
        return self._pil_props[1]

    @property
    def _format(self):
        """
        The PIL Image format of this image, e.g. 'PNG'.
        """
        return self._pil_props[0]

    @lazyproperty
    def _pil_props(self):
        """
        A tuple containing useful image properties extracted from this image
        using Pillow (Python Imaging Library, or 'PIL').
        """
        stream = BytesIO(self._blob)
        pil_image = PIL_Image.open(stream)
        format = pil_image.format
        width_px, height_px = pil_image.size
        dpi = pil_image.info.get("dpi")
        stream.close()
        return (format, (width_px, height_px), dpi)
