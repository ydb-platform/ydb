"""ImagePart and related objects."""

from __future__ import annotations

import hashlib
import io
import os
from typing import IO, TYPE_CHECKING, Any, cast

from PIL import Image as PIL_Image

from pptx.opc.package import Part
from pptx.opc.spec import image_content_types
from pptx.util import Emu, lazyproperty

if TYPE_CHECKING:
    from pptx.opc.packuri import PackURI
    from pptx.package import Package
    from pptx.util import Length


class ImagePart(Part):
    """An image part.

    An image part generally has a partname matching the regex `ppt/media/image[1-9][0-9]*.*`.
    """

    def __init__(
        self,
        partname: PackURI,
        content_type: str,
        package: Package,
        blob: bytes,
        filename: str | None = None,
    ):
        super(ImagePart, self).__init__(partname, content_type, package, blob)
        self._blob = blob
        self._filename = filename

    @classmethod
    def new(cls, package: Package, image: Image) -> ImagePart:
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
    def desc(self) -> str:
        """The filename associated with this image.

        Either the filename of the original image or a generic name of the form `image.ext` where
        `ext` is appropriate to the image file format, e.g. `'jpg'`. An image created using a path
        will have that filename; one created with a file-like object will have a generic name.
        """
        # -- return generic filename if original filename is unknown --
        if self._filename is None:
            return f"image.{self.ext}"
        return self._filename

    @property
    def ext(self) -> str:
        """File-name extension for this image e.g. `'png'`."""
        return self.partname.ext

    @property
    def image(self) -> Image:
        """An |Image| object containing the image in this image part.

        Note this is a `pptx.image.Image` object, not a PIL Image.
        """
        return Image(self._blob, self.desc)

    def scale(self, scaled_cx: int | None, scaled_cy: int | None) -> tuple[int, int]:
        """Return scaled image dimensions in EMU based on the combination of parameters supplied.

        If `scaled_cx` and `scaled_cy` are both |None|, the native image size is returned. If
        neither `scaled_cx` nor `scaled_cy` is |None|, their values are returned unchanged. If a
        value is provided for either `scaled_cx` or `scaled_cy` and the other is |None|, the
        missing value is calculated such that the image's aspect ratio is preserved.
        """
        image_cx, image_cy = self._native_size

        if scaled_cx and scaled_cy:
            return scaled_cx, scaled_cy

        if scaled_cx and not scaled_cy:
            scaling_factor = float(scaled_cx) / float(image_cx)
            scaled_cy = int(round(image_cy * scaling_factor))
            return scaled_cx, scaled_cy

        if not scaled_cx and scaled_cy:
            scaling_factor = float(scaled_cy) / float(image_cy)
            scaled_cx = int(round(image_cx * scaling_factor))
            return scaled_cx, scaled_cy

        # -- only remaining case is both `scaled_cx` and `scaled_cy` are `None` --
        return image_cx, image_cy

    @lazyproperty
    def sha1(self) -> str:
        """The 40-character SHA1 hash digest for the image binary of this image part.

        like: `"1be010ea47803b00e140b852765cdf84f491da47"`.
        """
        return hashlib.sha1(self._blob).hexdigest()

    @property
    def _dpi(self) -> tuple[int, int]:
        """(horz_dpi, vert_dpi) pair representing the dots-per-inch resolution of this image."""
        image = Image.from_blob(self._blob)
        return image.dpi

    @property
    def _native_size(self) -> tuple[Length, Length]:
        """A (width, height) 2-tuple representing the native dimensions of the image in EMU.

        Calculated based on the image DPI value, if present, assuming 72 dpi as a default.
        """
        EMU_PER_INCH = 914400
        horz_dpi, vert_dpi = self._dpi
        width_px, height_px = self._px_size

        width = EMU_PER_INCH * width_px / horz_dpi
        height = EMU_PER_INCH * height_px / vert_dpi

        return Emu(int(width)), Emu(int(height))

    @property
    def _px_size(self) -> tuple[int, int]:
        """A (width, height) 2-tuple representing the dimensions of this image in pixels."""
        image = Image.from_blob(self._blob)
        return image.size


class Image(object):
    """Immutable value object representing an image such as a JPEG, PNG, or GIF."""

    def __init__(self, blob: bytes, filename: str | None):
        super(Image, self).__init__()
        self._blob = blob
        self._filename = filename

    @classmethod
    def from_blob(cls, blob: bytes, filename: str | None = None) -> Image:
        """Return a new |Image| object loaded from the image binary in `blob`."""
        return cls(blob, filename)

    @classmethod
    def from_file(cls, image_file: str | IO[bytes]) -> Image:
        """Return a new |Image| object loaded from `image_file`.

        `image_file` can be either a path (str) or a file-like object.
        """
        if isinstance(image_file, str):
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
    def blob(self) -> bytes:
        """The binary image bytestream of this image."""
        return self._blob

    @lazyproperty
    def content_type(self) -> str:
        """MIME-type of this image, e.g. `"image/jpeg"`."""
        return image_content_types[self.ext]

    @lazyproperty
    def dpi(self) -> tuple[int, int]:
        """A (horz_dpi, vert_dpi) 2-tuple specifying the dots-per-inch resolution of this image.

        A default value of (72, 72) is used if the dpi is not specified in the image file.
        """

        def int_dpi(dpi: Any):
            """Return an integer dots-per-inch value corresponding to `dpi`.

            If `dpi` is |None|, a non-numeric type, less than 1 or greater than 2048, 72 is
            returned.
            """
            try:
                int_dpi = int(round(float(dpi)))
                if int_dpi < 1 or int_dpi > 2048:
                    int_dpi = 72
            except (TypeError, ValueError):
                int_dpi = 72
            return int_dpi

        def normalize_pil_dpi(pil_dpi: tuple[int, int] | None):
            """Return a (horz_dpi, vert_dpi) 2-tuple corresponding to `pil_dpi`.

            The value for the 'dpi' key in the `info` dict of a PIL image. If the 'dpi' key is not
            present or contains an invalid value, `(72, 72)` is returned.
            """
            if isinstance(pil_dpi, tuple):
                return (int_dpi(pil_dpi[0]), int_dpi(pil_dpi[1]))
            return (72, 72)

        return normalize_pil_dpi(self._pil_props[2])

    @lazyproperty
    def ext(self) -> str:
        """Canonical file extension for this image e.g. `'png'`.

        The returned extension is all lowercase and is the canonical extension for the content type
        of this image, regardless of what extension may have been used in its filename, if any.
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
    def filename(self) -> str | None:
        """Filename from path used to load this image, if loaded from the filesystem.

        |None| if no filename was used in loading, such as when loaded from an in-memory stream.
        """
        return self._filename

    @lazyproperty
    def sha1(self) -> str:
        """SHA1 hash digest of the image blob."""
        return hashlib.sha1(self._blob).hexdigest()

    @lazyproperty
    def size(self) -> tuple[int, int]:
        """A (width, height) 2-tuple specifying the dimensions of this image in pixels."""
        return self._pil_props[1]

    @property
    def _format(self) -> str | None:
        """The PIL Image format of this image, e.g. 'PNG'."""
        return self._pil_props[0]

    @lazyproperty
    def _pil_props(self) -> tuple[str | None, tuple[int, int], tuple[int, int] | None]:
        """tuple of image properties extracted from this image using Pillow."""
        stream = io.BytesIO(self._blob)
        pil_image = PIL_Image.open(stream)  # pyright: ignore[reportUnknownMemberType]
        format = pil_image.format
        width_px, height_px = pil_image.size
        dpi = cast(
            "tuple[int, int] | None",
            pil_image.info.get("dpi"),  # pyright: ignore[reportUnknownMemberType]
        )
        stream.close()
        return (format, (width_px, height_px), dpi)
