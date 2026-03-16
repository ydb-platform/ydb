"""Plugins for the Pillow library."""

from itertools import chain
from typing import IO
from warnings import warn

from PIL import Image, ImageFile, ImageSequence

from . import options
from .constants import HeifCompressionFormat
from .heif import HeifFile
from .misc import (
    CtxEncode,
    _exif_from_pillow,
    _get_bytes,
    _get_orientation_for_encoder,
    _get_primary_index,
    _pil_to_supported_mode,
    _xmp_from_pillow,
    set_orientation,
)

try:
    import _pillow_heif
except ImportError as ex:
    from ._deffered_error import DeferredError

    _pillow_heif = DeferredError(ex)


class _LibHeifImageFile(ImageFile.ImageFile):
    """Base class with all functionality for ``HeifImageFile`` class."""

    _heif_file: HeifFile | None = None
    _close_exclusive_fp_after_loading = True
    _mode: str

    def __init__(self, *args, **kwargs):
        self.__frame = 0
        super().__init__(*args, **kwargs)

    def _open(self):
        try:
            # when Pillow starts supporting 16-bit multichannel images change `convert_hdr_to_8bit` to False
            heif_file = HeifFile(self.fp, convert_hdr_to_8bit=True, hdr_to_16bit=True, remove_stride=False)
        except (OSError, ValueError, SyntaxError, RuntimeError, EOFError) as exception:
            raise SyntaxError(str(exception)) from None
        self.custom_mimetype = heif_file.mimetype
        self._heif_file = heif_file
        self.__frame = heif_file.primary_index
        self._init_from_heif_file(self.__frame)
        self.tile = []

    def load(self):
        if self._heif_file:
            frame_heif = self._heif_file[self.tell()]
            try:
                data = frame_heif.data  # Size of Image can change during decoding
                self._size = frame_heif.size  # noqa
                self.load_prepare()
                self.frombytes(data, "raw", (frame_heif.mode, frame_heif.stride))
            except (EOFError, ValueError):
                if not ImageFile.LOAD_TRUNCATED_IMAGES:
                    raise
                self.load_prepare()
            # In any case, we close `fp`, since the input data bytes are held by the `HeifFile` class.
            if self.fp and getattr(self, "_exclusive_fp", False) and hasattr(self.fp, "close"):
                self.fp.close()
            self.fp = None
            if not self.is_animated:
                self._heif_file = None
        return super().load()

    def seek(self, frame: int):
        if not self._seek_check(frame):
            return
        self.__frame = frame
        self._init_from_heif_file(frame)

        # Pillow 11.0+
        # We need to create a new core image object on second and
        # subsequent frames in the image. Image may be different size/mode.
        # https://github.com/python-pillow/Pillow/issues/8439
        self.im = Image.core.new(self._mode, self._size)  # pylint: disable=too-many-function-args

        exif = getattr(self, "_exif", None)  # Pillow 9.2+ do no reload exif between frames.
        if exif is not None and getattr(exif, "_loaded", None):
            exif._loaded = False  # pylint: disable=protected-access

    def tell(self) -> int:
        return self.__frame

    def verify(self) -> None:
        pass

    @property
    def n_frames(self) -> int:
        """Returns the number of available frames.

        :returns: Frame number, starting with 0.
        """
        return len(self._heif_file) if self._heif_file else 1

    @property
    def is_animated(self) -> bool:
        """Returns ``True`` if this image contains more than one frame, or ``False`` otherwise."""
        return self.n_frames > 1

    def _seek_check(self, frame: int):
        if frame < 0 or frame >= self.n_frames:
            raise EOFError("attempt to seek outside sequence")
        return self.tell() != frame

    def _init_from_heif_file(self, img_index: int) -> None:
        if self._heif_file:
            self._size = self._heif_file[img_index].size
            self._mode = self._heif_file[img_index].mode
            self.info = self._heif_file[img_index].info
            self.info["original_orientation"] = set_orientation(self.info)


class HeifImageFile(_LibHeifImageFile):
    """Pillow plugin class type for a HEIF image format."""

    format = "HEIF"  # noqa
    format_description = "HEIF container"


def _is_supported_heif(fp) -> bool:
    magic = _get_bytes(fp, 12)
    if magic[4:8] != b"ftyp":
        return False
    return magic[8:12] in (b"heic", b"heix", b"heim", b"heis", b"hevc", b"hevx", b"hevm", b"hevs", b"mif1", b"msf1")


def _save_heif(im: Image.Image, fp: IO[bytes], _filename: str | bytes):
    __save_one(im, fp, HeifCompressionFormat.HEVC)


def _save_all_heif(im: Image.Image, fp: IO[bytes], _filename: str | bytes):
    __save_all(im, fp, HeifCompressionFormat.HEVC)


def register_heif_opener(**kwargs) -> None:
    """Registers a Pillow plugin for HEIF format.

    :param kwargs: dictionary with values to set in options. See: :ref:`options`.
    """
    __options_update(**kwargs)
    Image.register_open(HeifImageFile.format, HeifImageFile, _is_supported_heif)
    if _pillow_heif.get_lib_info()["HEIF"]:
        Image.register_save(HeifImageFile.format, _save_heif)
        Image.register_save_all(HeifImageFile.format, _save_all_heif)
    extensions = [".heic", ".heics", ".heif", ".heifs", ".hif"]
    Image.register_mime(HeifImageFile.format, "image/heif")
    Image.register_extensions(HeifImageFile.format, extensions)


def __options_update(**kwargs):
    """Internal function to set options from `register_heif_opener` method."""
    for k, v in kwargs.items():
        if k == "thumbnails":
            options.THUMBNAILS = v
        elif k == "depth_images":
            options.DEPTH_IMAGES = v
        elif k == "aux_images":
            options.AUX_IMAGES = v
        elif k == "quality":
            options.QUALITY = v
        elif k == "save_to_12bit":
            options.SAVE_HDR_TO_12_BIT = v
        elif k == "decode_threads":
            options.DECODE_THREADS = v
        elif k == "allow_incorrect_headers":
            options.ALLOW_INCORRECT_HEADERS = v
        elif k == "save_nclx_profile":
            options.SAVE_NCLX_PROFILE = v
        elif k == "preferred_encoder":
            options.PREFERRED_ENCODER = v
        elif k == "preferred_decoder":
            options.PREFERRED_DECODER = v
        else:
            warn(f"Unknown option: {k}", stacklevel=1)


def __save_one(im: Image.Image, fp: IO[bytes], compression_format: HeifCompressionFormat):
    ctx_write = CtxEncode(compression_format, **im.encoderinfo)
    _pil_encode_image(ctx_write, im, True, **im.encoderinfo)
    ctx_write.save(fp)


def __save_all(im: Image.Image, fp: IO[bytes], compression_format: HeifCompressionFormat):
    ctx_write = CtxEncode(compression_format, **im.encoderinfo)
    current_frame = im.tell() if hasattr(im, "tell") else None
    append_images = im.encoderinfo.get("append_images", [])
    primary_index = _get_primary_index(
        chain(ImageSequence.Iterator(im), append_images), im.encoderinfo.get("primary_index", None)
    )
    for i, frame in enumerate(chain(ImageSequence.Iterator(im), append_images)):
        _pil_encode_image(ctx_write, frame, i == primary_index, **im.encoderinfo)
    if current_frame is not None and hasattr(im, "seek"):
        im.seek(current_frame)
    ctx_write.save(fp)


def _pil_encode_image(ctx: CtxEncode, img: Image.Image, primary: bool, **kwargs) -> None:
    if img.size[0] <= 0 or img.size[1] <= 0:
        raise ValueError("Empty images are not supported.")
    info = img.info.copy()
    info["exif"] = _exif_from_pillow(img)
    info["xmp"] = _xmp_from_pillow(img)
    info.update(**kwargs)
    info["primary"] = primary
    if img.mode == "YCbCr":
        ctx.add_image_ycbcr(img, image_orientation=_get_orientation_for_encoder(info), **info)
    else:
        img = _pil_to_supported_mode(img)
        ctx.add_image(img.size, img.mode, img.tobytes(), image_orientation=_get_orientation_for_encoder(info), **info)
