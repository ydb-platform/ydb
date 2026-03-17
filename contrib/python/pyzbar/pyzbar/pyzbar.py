from collections import namedtuple
from contextlib import contextmanager
from ctypes import cast, c_void_p, string_at

from .locations import bounding_box, convex_hull, Point, Rect
from .pyzbar_error import PyZbarError
from .wrapper import (
    zbar_image_scanner_set_config,
    zbar_image_scanner_create, zbar_image_scanner_destroy,
    zbar_image_create, zbar_image_destroy, zbar_image_set_format,
    zbar_image_set_size, zbar_image_set_data, zbar_scan_image,
    zbar_image_first_symbol, zbar_symbol_get_data_length,
    zbar_symbol_get_data, zbar_symbol_get_orientation,
    zbar_symbol_get_loc_size, zbar_symbol_get_loc_x, zbar_symbol_get_loc_y,
    zbar_symbol_get_quality, zbar_symbol_next, ZBarConfig, ZBarOrientation,
    ZBarSymbol, EXTERNAL_DEPENDENCIES,
)

__all__ = [
    'decode', 'Point', 'Rect', 'Decoded', 'ZBarSymbol', 'EXTERNAL_DEPENDENCIES', 'ORIENTATION_AVAILABLE'
]


ORIENTATION_AVAILABLE = zbar_symbol_get_orientation is not None

Decoded = namedtuple('Decoded', 'data type rect polygon quality orientation')

# ZBar's magic 'fourcc' numbers that represent image formats
_FOURCC = {
    'L800': 808466521,
    'GRAY': 1497715271
}

_RANGEFN = getattr(globals(), 'xrange', range)


@contextmanager
def _image():
    """A context manager for `zbar_image`, created and destoyed by
    `zbar_image_create` and `zbar_image_destroy`.

    Yields:
        POINTER(zbar_image): The created image

    Raises:
        PyZbarError: If the image could not be created.
    """
    image = zbar_image_create()
    if not image:
        raise PyZbarError('Could not create zbar image')
    else:
        try:
            yield image
        finally:
            zbar_image_destroy(image)


@contextmanager
def _image_scanner():
    """A context manager for `zbar_image_scanner`, created and destroyed by
    `zbar_image_scanner_create` and `zbar_image_scanner_destroy`.

    Yields:
        POINTER(zbar_image_scanner): The created scanner

    Raises:
        PyZbarError: If the decoder could not be created.
    """
    scanner = zbar_image_scanner_create()
    if not scanner:
        raise PyZbarError('Could not create image scanner')
    else:
        try:
            yield scanner
        finally:
            zbar_image_scanner_destroy(scanner)


def _symbols_for_image(image):
    """Generator of symbols.

    Args:
        image: `zbar_image`

    Yields:
        POINTER(zbar_symbol): Symbol
    """
    symbol = zbar_image_first_symbol(image)
    while symbol:
        yield symbol
        symbol = zbar_symbol_next(symbol)


def _decode_symbols(symbols):
    """Generator of decoded symbol information.

    Args:
        symbols: iterable of instances of `POINTER(zbar_symbol)`

    Yields:
        Decoded: decoded symbol
    """
    for symbol in symbols:
        data = string_at(
            zbar_symbol_get_data(symbol),
            zbar_symbol_get_data_length(symbol)
        )
        # The 'type' int should be a value in the ZBarSymbol enumeration
        try:
            symbol_type = ZBarSymbol(symbol.contents.type)
        except ValueError:
            # This release of zbar supports a type that pyzbar does not know about
            symbol_type = "Unrecognised type [{0}]".format(symbol.contents.type)
        else:
            symbol_type = symbol_type.name

        quality = zbar_symbol_get_quality(symbol)
        polygon = convex_hull(
            (
                zbar_symbol_get_loc_x(symbol, index),
                zbar_symbol_get_loc_y(symbol, index)
            )
            for index in _RANGEFN(zbar_symbol_get_loc_size(symbol))
        )

        if zbar_symbol_get_orientation:
            orientation = ZBarOrientation(zbar_symbol_get_orientation(symbol)).name
        else:
            orientation = None

        yield Decoded(
            data=data,
            type=symbol_type,
            rect=bounding_box(polygon),
            polygon=polygon,
            orientation=orientation,
            quality=quality,
        )


def _pixel_data(image):
    """Returns (pixels, width, height)

    Returns:
        :obj: `tuple` (pixels, width, height)
    """
    # Test for PIL.Image, numpy.ndarray, and imageio.core.util without
    # requiring that cv2, PIL, or imageio are installed.

    image_type = str(type(image))
    if 'PIL.' in image_type:
        if 'L' != image.mode:
            image = image.convert('L')
        pixels = image.tobytes()
        width, height = image.size
    elif 'numpy.ndarray' in image_type or 'imageio.core.util' in image_type:
        # Different versions of imageio use a subclass of numpy.ndarray
        # called either imageio.core.util.Image or imageio.core.util.Array.
        if 3 == len(image.shape):
            # Take just the first channel
            image = image[:, :, 0]
        if 'uint8' != str(image.dtype):
            image = image.astype('uint8')
        try:
            pixels = image.tobytes()
        except AttributeError:
            # `numpy.ndarray.tobytes()` introduced in `numpy` 1.9.0 - use the
            # older `tostring` method.
            pixels = image.tostring()
        height, width = image.shape[:2]
    else:
        # image should be a tuple (pixels, width, height)
        pixels, width, height = image

        # Check dimensions
        if 0 != len(pixels) % (width * height):
            raise PyZbarError(
                (
                    'Inconsistent dimensions: image data of {0} bytes is not '
                    'divisible by (width x height = {1})'
                ).format(len(pixels), (width * height))
            )

    # Compute bits-per-pixel
    bpp = 8 * len(pixels) // (width * height)
    if 8 != bpp:
        raise PyZbarError(
            'Unsupported bits-per-pixel [{0}]. Only [8] is supported.'.format(
                bpp
            )
        )

    return pixels, width, height


def decode(image, symbols=None):
    """Decodes datamatrix barcodes in `image`.

    Args:
        image: `numpy.ndarray`, `PIL.Image` or tuple (pixels, width, height)
        symbols: iter(ZBarSymbol) the symbol types to decode; if `None`, uses
            `zbar`'s default behaviour, which is to decode all symbol types.

    Returns:
        :obj:`list` of :obj:`Decoded`: The values decoded from barcodes.
    """
    pixels, width, height = _pixel_data(image)

    results = []
    with _image_scanner() as scanner:
        if symbols:
            # Disable all but the symbols of interest
            disable = set(ZBarSymbol).difference(symbols)
            for symbol in disable:
                zbar_image_scanner_set_config(
                    scanner, symbol, ZBarConfig.CFG_ENABLE, 0
                )
            # I think it likely that zbar will detect all symbol types by
            # default, in which case enabling the types of interest is
            # redundant but it seems sensible to be over-cautious and enable
            # them.
            for symbol in symbols:
                zbar_image_scanner_set_config(
                    scanner, symbol, ZBarConfig.CFG_ENABLE, 1
                )
        with _image() as img:
            zbar_image_set_format(img, _FOURCC['L800'])
            zbar_image_set_size(img, width, height)
            zbar_image_set_data(img, cast(pixels, c_void_p), len(pixels), None)
            decoded = zbar_scan_image(scanner, img)
            if decoded < 0:
                raise PyZbarError('Unsupported image format')
            else:
                results.extend(_decode_symbols(_symbols_for_image(img)))

    return results
