"""Enumerations."""

from enum import Enum, IntEnum

class TransformDirection(IntEnum):
    """Coordinate transform direction

    Forward transform direction defined as image pixel (row, col) to
    geographic/projected (x, y) coordinates. Reverse transform direction defined as
    geographic/projected (x, y) to image pixel (row, col) coordinates.

    Notes
    -----
    The convention for transform direction for RPC based coordinate transform is
    typically the opposite of what is previously described. For consistency
    all coordinate transforms methods use the same convention.
    """
    forward = 1
    reverse = 0

class TransformMethod(Enum):
    affine = 'transform'
    gcps = 'gcps'
    rpcs = 'rpcs'

class ColorInterp(IntEnum):
    """Raster band color interpretation."""
    undefined = 0
    gray = 1
    grey = 1
    palette = 2
    red = 3
    green = 4
    blue = 5
    alpha = 6
    hue = 7
    saturation = 8
    lightness = 9
    cyan = 10
    magenta = 11
    yellow = 12
    black = 13
    Y = 14
    Cb = 15
    Cr = 16
    # Below values since GDAL 3.10
    pan = 17
    coastal = 18
    rededge = 19
    nir = 20
    swir = 21
    mwir = 22
    lwir = 23
    tir = 24
    other_ir = 25
    # GCI_IR_Reserved_1 = 26
    # GCI_IR_Reserved_2 = 27
    # GCI_IR_Reserved_3 = 28
    # GCI_IR_Reserved_4 = 29
    sar_ka = 30
    sar_k = 31
    sar_ku = 32
    sar_x = 33
    sar_c = 34
    sar_s = 35
    sar_l = 36
    sar_p = 37


class Resampling(IntEnum):
    """Available warp resampling algorithms.

    Notes
    ----------
    The first 8, 'nearest', 'bilinear', 'cubic', 'cubic_spline',
    'lanczos', 'average', 'mode', and 'gauss', are available for making
    dataset overviews.

    'max', 'min', 'med', 'q1', 'q3' are only supported in GDAL >= 2.0.0.

    'nearest', 'bilinear', 'cubic', 'cubic_spline', 'lanczos',
    'average', 'mode' are always available (GDAL >= 1.10).

    'sum' is only supported in GDAL >= 3.1.

    'rms' is only supported in GDAL >= 3.3.

    Note: 'gauss' is not available to the functions in rio.warp.
    """
    nearest = 0
    """Nearest neighbor resampling (default, fastest algorithm, worst interpolation quality)."""
    bilinear = 1
    """Bilinear resampling."""
    cubic = 2
    """Cubic resampling."""
    cubic_spline = 3
    """Cubic spline resampling."""
    lanczos = 4
    """Lanczos windowed sinc resampling."""
    average = 5
    """Average resampling, computes the weighted average of all non-NODATA contributing pixels."""
    mode = 6
    """Mode resampling, selects the value which appears most often of all the sampled points."""
    gauss = 7
    """Gaussian resampling, Note: not available to the functions in rio.warp."""
    max = 8
    """Maximum resampling, selects the maximum value from all non-NODATA contributing pixels. (GDAL >= 2.0)"""
    min = 9
    """Minimum resampling, selects the minimum value from all non-NODATA contributing pixels. (GDAL >= 2.0)"""
    med = 10
    """Median resampling, selects the median value of all non-NODATA contributing pixels. (GDAL >= 2.0)"""
    q1 = 11
    """Q1, first quartile resampling, selects the first quartile value of all non-NODATA contributing pixels. (GDAL >= 2.0)"""
    q3 = 12
    """Q3, third quartile resampling, selects the third quartile value of all non-NODATA contributing pixels. (GDAL >= 2.0)"""
    sum = 13
    """Sum, compute the weighted sum of all non-NODATA contributing pixels. (GDAL >= 3.1)"""
    rms = 14
    """RMS, root mean square / quadratic mean of all non-NODATA contributing pixels. (GDAL >= 3.3)"""


class OverviewResampling(IntEnum):
    """Available Overview resampling algorithms.

    The first 8, 'nearest', 'bilinear', 'cubic', 'cubic_spline',
    'lanczos', 'average', 'mode', and 'gauss', are available for making
    dataset overviews.

    'nearest', 'bilinear', 'cubic', 'cubic_spline', 'lanczos',
    'average', 'mode' are always available (GDAL >= 1.10).

    'rms' is only supported in GDAL >= 3.3.

    """
    nearest = 0
    bilinear = 1
    cubic = 2
    cubic_spline = 3
    lanczos = 4
    average = 5
    mode = 6
    gauss = 7
    rms = 14


class Compression(Enum):
    """Available compression algorithms for GeoTIFFs.

    Note that compression options for EXR, MRF, etc are not included
    in this enum.
    """
    jpeg = 'JPEG'
    lzw = 'LZW'
    packbits = 'PACKBITS'
    deflate = 'DEFLATE'
    ccittrle = 'CCITTRLE'
    ccittfax3 = 'CCITTFAX3'
    ccittfax4 = 'CCITTFAX4'
    lzma = 'LZMA'
    none = 'NONE'
    zstd = 'ZSTD'
    lerc = 'LERC'
    lerc_deflate = 'LERC_DEFLATE'
    lerc_zstd = 'LERC_ZSTD'
    webp = 'WEBP'
    jpeg2000 = 'JPEG2000'


class Interleaving(Enum):
    pixel = 'PIXEL'
    line = 'LINE'
    band = 'BAND'
    #: tile requires GDAL 3.11+
    tile = 'TILE'


class MaskFlags(IntEnum):
    all_valid = 1
    per_dataset = 2
    alpha = 4
    nodata = 8


class PhotometricInterp(Enum):
    black = 'MINISBLACK'
    white = 'MINISWHITE'
    rgb = 'RGB'
    cmyk = 'CMYK'
    ycbcr = 'YCbCr'
    cielab = 'CIELAB'
    icclab = 'ICCLAB'
    itulab = 'ITULAB'


class MergeAlg(Enum):
    """Available rasterization algorithms"""
    replace = 'REPLACE'
    add = 'ADD'


class WktVersion(Enum):
    """
     .. versionadded:: 1.3.0

    Supported CRS WKT string versions
    """

    #: WKT Version 2 from 2015
    WKT2_2015 = "WKT2_2015"
    #: Alias for latest WKT Version 2
    WKT2 = "WKT2"
    #: WKT Version 2 from 2019
    WKT2_2019 = "WKT2_2018"
    #: WKT Version 1 GDAL Style
    WKT1_GDAL = "WKT1_GDAL"
    #: Alias for WKT Version 1 GDAL Style
    WKT1 = "WKT1"
    #: WKT Version 1 ESRI Style
    WKT1_ESRI = "WKT1_ESRI"


    @classmethod
    def _missing_(cls, value):
        if value == "WKT2_2019":
            # WKT2_2019 alias added in GDAL 3.2, use WKT2_2018 for compatibility
            return WktVersion.WKT2_2019
        raise ValueError(f"Invalid value for WktVersion: {value}")
