# test_tifffile.py

# Copyright (c) 2008-2026, Christoph Gohlke
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# 1. Redistributions of source code must retain the above copyright notice,
#    this list of conditions and the following disclaimer.
#
# 2. Redistributions in binary form must reproduce the above copyright notice,
#    this list of conditions and the following disclaimer in the documentation
#    and/or other materials provided with the distribution.
#
# 3. Neither the name of the copyright holder nor the names of its
#    contributors may be used to endorse or promote products derived from
#    this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.

"""Unittests for the tifffile package.

:Version: 2026.2.24

"""

from __future__ import annotations

import asyncio
import binascii
import contextlib
import datetime
import glob
import json
import logging
import math
import mmap
import os
import pathlib
import random
import re
import struct
import sys
import sysconfig
import tempfile
import urllib.error
import urllib.request
from io import BytesIO
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from types import ModuleType
    from typing import Any

    from numpy.typing import ArrayLike, DTypeLike, NDArray

import numpy
import pytest
from numpy.testing import assert_allclose, assert_array_equal, assert_raises

import tifffile

try:
    from tifffile import *  # noqa: F403

    STAR_IMPORTED: tuple[object, ...] = (
        TIFF,  # noqa: F405
        imwrite,  # noqa: F405
        imread,  # noqa: F405
        imshow,  # noqa: F405
        TiffWriter,  # noqa: F405
        TiffReader,  # noqa: F405
        TiffFile,  # noqa: F405
        TiffFileError,  # noqa: F405
        TiffSequence,  # noqa: F405
        TiffPage,  # noqa: F405
        TiffFrame,  # noqa: F405
        FileHandle,  # noqa: F405
        FileSequence,  # noqa: F405
        Timer,  # noqa: F405
        logger,  # noqa: F405
        strptime,  # noqa: F405
        natural_sorted,  # noqa: F405
        memmap,  # noqa: F405
        repeat_nd,  # noqa: F405
        format_size,  # noqa: F405
        product,  # noqa: F405
        create_output,  # noqa: F405
        askopenfilename,  # noqa: F405
        read_scanimage_metadata,  # noqa: F405
        read_micromanager_metadata,  # noqa: F405
        OmeXmlError,  # noqa: F405
        OmeXml,  # noqa: F405
    )
except NameError:
    STAR_IMPORTED = ()

from tifffile import (  # noqa: F401
    CHUNKMODE,
    COMPRESSION,
    DATATYPE,
    EXTRASAMPLE,
    FILETYPE,
    FILLORDER,
    OFILETYPE,
    ORIENTATION,
    PHOTOMETRIC,
    PLANARCONFIG,
    PREDICTOR,
    RESUNIT,
    SAMPLEFORMAT,
    TIFF,
    FileCache,
    FileHandle,
    FileSequence,
    OmeXml,
    OmeXmlError,
    TiffFile,
    TiffFileError,
    TiffFrame,
    TiffPage,
    TiffPageSeries,
    TiffReader,
    TiffSequence,
    TiffTag,
    TiffTags,
    TiffWriter,
    Timer,
)
from tifffile.tifffile import (  # noqa: F401
    apply_colormap,
    asbool,
    askopenfilename,
    astrotiff_description_metadata,
    byteorder_compare,
    byteorder_isnative,
    bytes2str,
    check_shape,
    create_output,
    eer_xml_metadata,
    enumarg,
    epics_datetime,
    excel_datetime,
    fluoview_description_metadata,
    format_size,
    hexdump,
    imagej_description,
    imagej_description_metadata,
    imagej_shape,
    imread,
    imshow,
    imwrite,
    julian_datetime,
    logger,
    lsm2bin,
    matlabstr2py,
    memmap,
    metaseries_description_metadata,
    natural_sorted,
    order_axes,
    parse_filenames,
    parse_kwargs,
    pformat,
    pilatus_description_metadata,
    product,
    read_micromanager_metadata,
    read_scanimage_metadata,
    reorient,
    repeat_nd,
    reshape_axes,
    reshape_nd,
    scanimage_artist_metadata,
    scanimage_description_metadata,
    shaped_description,
    shaped_description_metadata,
    snipstr,
    squeeze_axes,
    stk_description_metadata,
    strptime,
    subresolution,
    svs_description_metadata,
    tiff2fsspec,
    tiffcomment,
    transpose_axes,
    unique_strings,
    unpack_rgb,
    update_kwargs,
    validate_jhove,
    xml2dict,
)

HERE = os.path.dirname(__file__)
TEMP_DIR = os.path.join(HERE, '_tmp')
DATA_DIR = os.path.join(HERE, 'data')

IS_BE = sys.byteorder == 'big'
IS_PYPY = 'pypy' in sys.version.lower()
IS_WIN = sys.platform == 'win32'
IS_CG = os.environ.get('COMPUTERNAME', '').startswith('CG-K')

RNG = numpy.random.default_rng(42)


def skip(key: str, default: bool) -> bool:  # noqa: FBT001
    """Return if environment variable is set and true."""
    return os.getenv(key, default) in {True, '1'}


# skip tests requiring large memory
SKIP_LARGE = skip('SKIP_LARGE', default=sys.maxsize < 2**32)
SKIP_SLOW = skip('SKIP_SLOW', default=True)
SKIP_EXTENDED = skip('SKIP_EXTENDED', default=False)
# skip data files
SKIP_FILE = skip('SKIP_FILE', default=not os.path.exists(DATA_DIR))
# skip validate written files with jhove
SKIP_VALIDATE = skip('SKIP_VALIDATE', default=True)
SKIP_CODECS = skip('SKIP_CODECS', default=False)
SKIP_ZARR = skip('SKIP_ZARR', default=False)
SKIP_DASK = skip('SKIP_DASK', default=False)
SKIP_NDTIFF = skip('SKIP_NDTIFF', default=False)
SKIP_HTTP = skip('SKIP_HTTP', default=not IS_CG)
REASON = 'skipped'

FILE_FLAGS = ['is_' + a for a in TIFF.FILE_FLAGS]
FILE_FLAGS += [name for name in dir(TiffFile) if name.startswith('is_')]
PAGE_FLAGS = [name for name in dir(TiffPage) if name.startswith('is_')]

URL = 'http://localhost:8386/'  # TEMP_DIR

if not SKIP_HTTP:
    try:
        urllib.request.urlopen(URL + '/test/test.txt', timeout=0.5)
    except (urllib.error.URLError, TimeoutError, ConnectionError):
        SKIP_HTTP = True

if not os.path.exists(TEMP_DIR):
    TEMP_DIR = tempfile.gettempdir()

if not SKIP_CODECS:
    try:
        import imagecodecs

        SKIP_CODECS = False
    except ImportError:
        SKIP_CODECS = True

zarr: ModuleType | None
fsspec: ModuleType | None

if IS_PYPY:
    SKIP_ZARR = True
    SKIP_DASK = True
    SKIP_HTTP = True
if SKIP_ZARR:
    zarr = None
else:
    try:
        import fsspec
        import zarr
        from kerchunk.utils import refs_as_store

        from tifffile.zarr import (
            ZarrFileSequenceStore,
            ZarrStore,
            ZarrTiffStore,
        )
    except ImportError:
        zarr = None
        fsspec = None
        SKIP_ZARR = True

dask: ModuleType | None
if SKIP_DASK:
    dask = None
else:
    try:
        import dask
        import dask.array
    except ImportError:
        dask = None
        SKIP_DASK = True

ndtiff: ModuleType | None
if SKIP_NDTIFF:
    ndtiff = None
else:
    try:
        import ndtiff  # type: ignore[no-redef]
    except ImportError:
        ndtiff = None
        SKIP_NDTIFF = True


def config() -> str:
    """Return test configuration."""
    this = sys.modules[__name__]
    return ' | '.join(
        a for a in dir(this) if a.startswith('SKIP_') and getattr(this, a)
    )


def _data_file(
    pathname: str, base: str, *, expand: bool = True
) -> str | list[str]:
    """Return path to test file(s)."""
    path = os.path.join(base, *pathname.split('/'))
    if expand and any(i in path for i in '*?'):
        return glob.glob(path)
    return path


def _file(pathname: str, base: str = DATA_DIR) -> str | list[str]:
    """Return path to test file(s)."""
    files = _data_file(pathname, base, expand=False)
    assert isinstance(files, str)
    return files


def _files(pathname: str, base: str = DATA_DIR) -> list[str]:
    """Return path to test files."""
    files = _data_file(pathname, base, expand=True)
    assert isinstance(files, list)
    return files


def random_data(dtype: DTypeLike, shape: tuple[int, ...]) -> NDArray[Any]:
    """Return random numpy array."""
    # TODO: use nd noise
    dtype = numpy.dtype(dtype)
    if dtype.char == '?':
        return RNG.random(shape) < 0.5
    data = RNG.random(shape) * 255
    return data.astype(dtype)


def assert_fsspec(url: str, data: NDArray[Any]) -> None:
    """Assert fsspec ReferenceFileSystem from local http server."""
    assert zarr is not None
    zobj = zarr.open(refs_as_store(url), zarr_format=2)

    if isinstance(zobj, zarr.Group):
        assert_array_equal(zobj['0'][:], data)
        assert_array_equal(zobj['1'][:], data[:, ::2, ::2])
        assert_array_equal(zobj['2'][:], data[:, ::4, ::4])
    else:
        assert_array_equal(zobj[:], data)


def assert_file_flags(tiff_file: TiffFile) -> None:
    """Access all flags of TiffFile."""
    for flag in FILE_FLAGS:
        getattr(tiff_file, flag)


def assert_page_flags(tiff_page: TiffPage) -> None:
    """Access all flags of TiffPage."""
    for flag in PAGE_FLAGS:
        getattr(tiff_page, flag)


def assert__str__(tif: TiffFile, detail: int = 3) -> None:
    """Call TiffFile._str and __repr__ functions."""
    for i in range(detail + 1):
        tif._str(detail=i)
    repr(tif)
    str(tif)
    repr(tif.pages)
    str(tif.pages)
    if len(tif.pages) > 0:
        page = tif.pages.first
        repr(page)
        str(page)
        str(page.tags)
        _ = page.flags
        _ = page.name
        _ = page.dims
        _ = page.sizes
        _ = page.coords
    repr(tif.series)
    str(tif.series)
    if len(tif.series) > 0:
        series = tif.series[0]
        repr(series)
        str(series)


def assert__repr__(obj: object) -> None:
    """Call object's __repr__ and __str__ function."""
    repr(obj)
    str(obj)


def assert_valid_omexml(omexml: str) -> None:
    """Validate OME-XML schema."""
    if not SKIP_HTTP:
        OmeXml.validate(omexml, assert_=True)


def assert_valid_tiff(filename: str, *args: Any, **kwargs: Any) -> None:
    """Validate TIFF file using jhove script."""
    if SKIP_VALIDATE:
        return
    validate_jhove(filename, 'jhove.cmd', *args, **kwargs)


def assert_decode_method(
    page: TiffPage, image: ArrayLike | None = None
) -> None:
    """Call TiffPage.decode on all segments and compare to TiffPage.asarray."""
    fh = page.parent.filehandle
    if page.is_tiled:
        offsets = page.tags['TileOffsets'].value
        bytecounts = page.tags['TileByteCounts'].value
    else:
        offsets = page.tags['StripOffsets'].value
        bytecounts = page.tags['StripByteCounts'].value
    image = page.asarray() if image is None else numpy.asarray(image)
    for i, (o, b) in enumerate(zip(offsets, bytecounts, strict=True)):
        fh.seek(o)
        strile, index, _shape = page.decode(fh.read(b), i)
        assert strile is not None
        assert image.reshape(page.shaped)[index] == strile[0, 0, 0, 0]


def assert_aszarr_method(
    obj: TiffFile | TiffPage,
    image: ArrayLike | None = None,
    chunkmode: int | None = None,
    mode: str = 'r',
    **kwargs: Any,
) -> None:
    """Assert aszarr returns same data as asarray."""
    if SKIP_ZARR or zarr is None:
        return
    if image is None:
        image = obj.asarray(**kwargs)
    with obj.aszarr(
        chunkmode=chunkmode, read_only=mode == 'r', **kwargs
    ) as store:
        data = zarr.open(store, mode=mode)
        if isinstance(data, zarr.Group):
            data = data['0']
        data = data[:]
        assert_array_equal(data, image)
        del data


class TempFileName:
    """Temporary file name context manager."""

    name: str
    remove: bool

    def __init__(
        self,
        name: str | None = None,
        ext: str = '.tif',
        *,
        remove: bool = False,
    ) -> None:
        self.remove = remove or tempfile.gettempdir() == TEMP_DIR
        if not name:
            fh = tempfile.NamedTemporaryFile(prefix='test_')
            self.name = fh.name
            fh.close()
        else:
            self.name = os.path.join(TEMP_DIR, f'test_{name}{ext}')

    def __enter__(self) -> str:
        return self.name

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        if self.remove:
            with contextlib.suppress(Exception):
                os.remove(self.name)


numpy.set_printoptions(suppress=True, precision=5)


###############################################################################

# Tests for specific issues


def test_issue_star_import():
    """Test from tifffile import *."""
    assert len(STAR_IMPORTED) > 0
    assert lsm2bin not in STAR_IMPORTED


@pytest.mark.skipif(__doc__ is None, reason='__doc__ is None')
def test_issue_version_mismatch():
    """Test 'tifffile.__version__' matches docstrings."""
    ver = ':Version: ' + tifffile.__version__
    assert ver in __doc__
    assert ver in tifffile.__doc__


def test_issue_deprecated_import():
    """Test deprecated functions can still be imported."""
    with pytest.raises(ImportError):
        from tifffile import imsave  # noqa: F401


def test_issue_imread_kwargs():
    """Test that is_flags are handled by imread."""
    data = random_data(numpy.uint16, (5, 63, 95))
    with TempFileName('issue_imread_kwargs') as filename:
        with TiffWriter(filename) as tif:
            for image in data:
                tif.write(image)  # create 5 series
        assert_valid_tiff(filename)
        image = imread(filename, pattern=None)  # reads first series
        assert_array_equal(image, data[0])
        image = imread(filename, is_shaped=False)  # reads all pages
        assert_array_equal(image, data)


def test_issue_imread_kwargs_legacy():
    """Test legacy arguments no longer work as of 2022.4.22.

    Specifying 'fastij', 'movie', 'multifile', 'multifile_close', or
    'pages' raises TypeError.
    Specifying 'key' and 'pages' raises TypeError.
    Specifying 'pages' in TiffFile constructor raises TypeError.

    """
    data = random_data(numpy.uint8, (3, 21, 31))
    with TempFileName('issue_imread_kwargs_legacy') as filename:
        imwrite(filename, data, photometric=PHOTOMETRIC.MINISBLACK)
        with pytest.raises(TypeError):
            imread(filename, fastij=True)
        with pytest.raises(TypeError):
            imread(filename, movie=True)
        with pytest.raises(TypeError):
            imread(filename, multifile=True)
        with pytest.raises(TypeError):
            imread(filename, multifile_close=True)

        with pytest.raises(TypeError):
            TiffFile(filename, fastij=True)
        with pytest.raises(TypeError):
            TiffFile(filename, multifile=True)
        with pytest.raises(TypeError):
            TiffFile(filename, multifile_close=True)
        with pytest.raises(TypeError):
            imread(filename, key=0, pages=[1, 2])
        with pytest.raises(TypeError):
            TiffFile(filename, pages=[1, 2])


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_issue_infinite_loop():
    """Test infinite loop reading more than two tags of same code in IFD."""
    # Reported by D. Hughes on 2019.7.26
    # the test file is corrupted but should not cause infinite loop
    filename = _file('gdk-pixbuf/bug784903-overflow-dimensions.tiff')
    with TiffFile(filename) as tif:
        page = tif.pages.first
        assert page.compression == 0  # invalid
        assert__str__(tif)


@pytest.mark.skipif(
    SKIP_FILE or SKIP_CODECS or not imagecodecs.JPEG.available,
    reason=REASON,
)
def test_issue_jpeg_ia():
    """Test JPEG compressed intensity image with alpha channel."""
    # no extrasamples!
    filename = _file('issues/jpeg_ia.tiff')
    with TiffFile(filename) as tif:
        page = tif.pages.first
        assert page.compression == COMPRESSION.JPEG
        assert_array_equal(
            page.asarray(),
            numpy.array([[[0, 0], [255, 255]]], dtype=numpy.uint8),
        )
        assert__str__(tif)


@pytest.mark.skipif(
    SKIP_FILE or SKIP_CODECS or not imagecodecs.JPEG.available,
    reason=REASON,
)
def test_issue_jpeg_palette():
    """Test invalid JPEG compressed intensity image with palette."""
    # https://forum.image.sc/t/viv-and-avivator/45999/24
    filename = _file('issues/FL_cells.ome.tif')
    with TiffFile(filename) as tif:
        page = tif.pages.first
        assert page.compression == COMPRESSION.JPEG
        assert page.colormap is not None
        data = tif.asarray()
        assert data.shape == (4, 1024, 1024)
        assert data.dtype == numpy.uint8
        assert data[2, 512, 512] == 10
        assert_aszarr_method(tif, data)
        assert__str__(tif)


def test_issue_specific_pages():
    """Test read second page."""
    data = random_data(numpy.uint8, (3, 21, 31))
    with TempFileName('issue_specific_pages') as filename:
        imwrite(filename, data, photometric=PHOTOMETRIC.MINISBLACK)
        image = imread(filename)
        assert image.shape == (3, 21, 31)
        # UserWarning: can not reshape (21, 31) to (3, 21, 31)
        image = imread(filename, key=1)
        assert image.shape == (21, 31)
        assert_array_equal(image, data[1])
    with TempFileName('issue_specific_pages_bigtiff') as filename:
        imwrite(
            filename, data, bigtiff=True, photometric=PHOTOMETRIC.MINISBLACK
        )
        image = imread(filename)
        assert image.shape == (3, 21, 31)
        # UserWarning: can not reshape (21, 31) to (3, 21, 31)
        image = imread(filename, key=1)
        assert image.shape == (21, 31)
        assert_array_equal(image, data[1])


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_issue_circular_ifd(caplog):
    """Test circular IFD logs error but is still readable."""
    filename = _file('Tiff-Library-4J/IFD struct/Circular E.tif')
    with TiffFile(filename) as tif:
        assert len(tif.pages) == 2
        assert 'invalid circular reference' in caplog.text
        image = tif.asarray()
        assert image.shape == (2, 1500, 2000, 3)
        assert image[1, 1499, 1999, 2] == 110


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_issue_bad_description(caplog):
    """Test page.description is empty when ImageDescription is not ASCII."""
    # ImageDescription is not ASCII but bytes
    filename = _file('stk/cells in the eye2.stk')
    with TiffFile(filename) as tif:
        page = tif.pages.first
        assert page.description == ''
        assert__str__(tif)
    assert 'coercing invalid ASCII to bytes' in caplog.text


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_issue_bad_ascii(caplog):
    """Test coerce invalid ASCII to bytes."""
    # ImageID is not ASCII but bytes
    # https://github.com/blink1073/tifffile/pull/38
    filename = _file('issues/tifffile_013_tagfail.tif')
    with TiffFile(filename) as tif:
        tags = tif.pages.first.tags
        assert tags['ImageID'].value[-8:] == b'rev 2893'
        assert__str__(tif)
    assert 'coercing invalid ASCII to bytes' in caplog.text


def test_issue_sampleformat():
    """Test write correct number of SampleFormat values."""
    # https://github.com/ngageoint/geopackage-tiff-java/issues/5
    data = random_data(numpy.int16, (256, 256, 4))
    with TempFileName('issue_sampleformat') as filename:
        imwrite(filename, data, photometric=PHOTOMETRIC.RGB)
        with TiffFile(filename) as tif:
            tags = tif.pages.first.tags
            assert tags['SampleFormat'].value == (2, 2, 2, 2)
            assert tags['ExtraSamples'].value == (2,)
            assert__str__(tif)


def test_issue_sampleformat_default():
    """Test SampleFormat are not written for UINT."""
    data = random_data(numpy.uint8, (256, 256, 4))
    with TempFileName('issue_sampleformat_default') as filename:
        imwrite(filename, data, photometric=PHOTOMETRIC.RGB)
        with TiffFile(filename) as tif:
            tags = tif.pages.first.tags
            assert 'SampleFormat' not in tags
            assert tags['ExtraSamples'].value == (2,)
            assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE or SKIP_CODECS, reason=REASON)
def test_issue_palette_with_extrasamples():
    """Test read palette with extra samples."""
    # https://github.com/python-pillow/Pillow/issues/1597
    filename = _file('issues/palette_with_extrasamples.tif')
    with TiffFile(filename) as tif:
        assert len(tif.pages) == 1
        page = tif.pages.first
        assert page.photometric == PHOTOMETRIC.PALETTE
        assert page.compression == COMPRESSION.LZW
        assert page.imagewidth == 518
        assert page.imagelength == 556
        assert page.bitspersample == 8
        assert page.samplesperpixel == 2
        # assert data
        image = page.asrgb()
        assert image.shape == (556, 518, 3)
        assert image.dtype == numpy.uint16
        image = tif.asarray()
        # self.assertEqual(image.shape[-3:], (556, 518, 2))
        assert image.shape == (556, 518, 2)
        assert image.dtype == numpy.uint8
        assert_aszarr_method(tif, image)
        del image
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE or SKIP_CODECS, reason=REASON)
def test_issue_incorrect_rowsperstrip_count():
    """Test read incorrect count for rowsperstrip; bitspersample = 4."""
    # https://github.com/python-pillow/Pillow/issues/1544
    filename = _file('bad/incorrect_count.tiff')
    with TiffFile(filename) as tif:
        assert len(tif.pages) == 1
        page = tif.pages.first
        assert page.photometric == PHOTOMETRIC.PALETTE
        assert page.compression == COMPRESSION.ADOBE_DEFLATE
        assert page.imagewidth == 32
        assert page.imagelength == 32
        assert page.bitspersample == 4
        assert page.samplesperpixel == 1
        assert page.rowsperstrip == 32
        assert page.dataoffsets[0] == 8
        assert page.databytecounts[0] == 89
        # assert data
        image = page.asrgb()
        assert image.shape == (32, 32, 3)
        assert_aszarr_method(page)
        del image
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_issue_extra_strips(caplog):
    """Test read extra strips."""
    # https://github.com/opencv/opencv/issues/17054
    filename = _file('issues/extra_strips.tif')
    with TiffFile(filename) as tif:
        assert not tif.is_bigtiff
        assert len(tif.pages) == 1
        page = tif.pages.first
        assert page.tags['StripOffsets'].value == (8, 0, 0)
        assert page.tags['StripByteCounts'].value == (55064448, 0, 0)
        assert page.dataoffsets[0] == 8
        assert page.databytecounts[0] == 55064448
        assert page.is_contiguous
        # assert data
        image = tif.asarray()
        assert image.shape == (2712, 3384, 3)
        assert_aszarr_method(page, image)
    assert 'incorrect StripOffsets count' in caplog.text
    assert 'incorrect StripByteCounts count' in caplog.text


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_issue_no_bytecounts(caplog):
    """Test read no bytecounts."""
    filename = _file('bad/img2_corrupt.tif')
    with TiffFile(filename) as tif:
        assert not tif.is_bigtiff
        assert len(tif.pages) == 1
        page = tif.pages.first
        assert page.is_contiguous
        assert page.planarconfig == PLANARCONFIG.CONTIG
        assert page.dataoffsets[0] == 512
        assert page.databytecounts[0] == 0
        # assert data
        image = tif.asarray()
        assert image.shape == (800, 1200)
        # fails: assert_aszarr_method(tif, image)
    assert 'invalid value offset 0' in caplog.text
    assert 'invalid data type 31073' in caplog.text
    assert 'invalid page offset 808333686' in caplog.text


@pytest.mark.skipif(SKIP_FILE or SKIP_CODECS, reason=REASON)
def test_issue_missing_eoi_in_strips():
    """Test read LZW strips without EOI."""
    # 256x256 uint16, lzw, imagej
    # Strips do not contain an EOI code as required by the TIFF spec.
    # File generated by `tiffcp -c lzw Z*.tif stack.tif` from
    # Bars-G10-P15.zip
    # Failed with "series 0 failed: string size must be a multiple of
    # element size"
    # Reported by Kai Wohlfahrt on 3/7/2014
    filename = _file('issues/stack.tif')
    with TiffFile(filename) as tif:
        assert tif.is_imagej
        assert tif.byteorder == '<'
        assert len(tif.pages) == 128
        assert len(tif.series) == 1
        # assert page properties
        page = tif.pages.first
        assert page.imagewidth == 256
        assert page.imagelength == 256
        assert page.bitspersample == 16
        # assert series properties
        series = tif.series[0]
        assert series.shape == (128, 256, 256)
        assert series.dtype == numpy.uint16
        assert series.axes == 'IYX'
        # assert ImageJ tags
        ijmeta = tif.imagej_metadata
        assert ijmeta is not None
        assert ijmeta['ImageJ'] == '1.41e'
        # assert data
        data = tif.asarray()
        assert data.shape == (128, 256, 256)
        assert data.dtype == numpy.uint16
        assert data[64, 128, 128] == 19226
        assert_aszarr_method(tif, data)
        del data
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_issue_imagej_grayscalemode():
    """Test read ImageJ grayscale mode RGB image."""
    # https://github.com/cgohlke/tifffile/issues/6
    filename = _file('issues/hela-cells.tif')
    with TiffFile(filename) as tif:
        assert tif.is_imagej
        assert tif.byteorder == '>'
        assert len(tif.pages) == 1
        assert len(tif.series) == 1
        # assert page properties
        page = tif.pages.first
        assert page.photometric == PHOTOMETRIC.RGB
        assert page.imagewidth == 672
        assert page.imagelength == 512
        assert page.bitspersample == 16
        assert page.is_contiguous
        # assert series properties
        series = tif.series[0]
        assert series.shape == (512, 672, 3)
        assert series.dtype == numpy.uint16
        assert series.axes == 'YXS'
        # assert ImageJ tags
        ijmeta = tif.imagej_metadata
        assert ijmeta is not None
        assert ijmeta['ImageJ'] == '1.52p'
        assert ijmeta['channels'] == 3
        # assert data
        data = tif.asarray()
        assert isinstance(data, numpy.ndarray)
        assert data.shape == (512, 672, 3)
        assert data.dtype == numpy.uint16
        assert tuple(data[255, 336]) == (440, 378, 298)
        assert_aszarr_method(tif, data)
        assert__str__(tif)


@pytest.mark.parametrize('byteorder', ['>', '<'])
def test_issue_valueoffset(byteorder):
    """Test read TiffTag.valueoffsets."""
    unpack = struct.unpack
    data = random_data(byteorder + 'u2', (2, 19, 31))
    software = 'test_tifffile'
    bo = {'>': 'be', '<': 'le'}[byteorder]
    with TempFileName(f'issue_valueoffset_{bo}') as filename:
        imwrite(
            filename,
            data,
            software=software,
            photometric=PHOTOMETRIC.MINISBLACK,
            extratags=[(65535, 3, 2, (21, 22), True)],
        )
        with (
            TiffFile(filename, _useframes=True) as tif,
            open(filename, 'rb') as fh,
        ):
            page = tif.pages.first
            # inline value
            fh.seek(page.tags['ImageLength'].valueoffset)
            assert (
                page.imagelength == unpack(tif.byteorder + 'I', fh.read(4))[0]
            )
            # two inline values
            fh.seek(page.tags[65535].valueoffset)
            assert unpack(tif.byteorder + 'H', fh.read(2))[0] == 21
            # separate value
            fh.seek(page.tags['Software'].valueoffset)
            assert page.software == bytes2str(fh.read(13))
            # TiffFrame
            page = tif.pages[1].aspage()
            fh.seek(page.tags['StripOffsets'].valueoffset)
            assert (
                page.dataoffsets[0]
                == unpack(tif.byteorder + 'I', fh.read(4))[0]
            )
            tag = page.tags['ImageLength']
            assert tag.name == 'ImageLength'
            assert tag.dtype_name == 'LONG'
            assert tag.dataformat == '1I'
            assert tag.valuebytecount == 4


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_issue_pages_number():
    """Test number of pages."""
    filename = _file('tifffile/100000_pages.tif')
    with TiffFile(filename) as tif:
        assert len(tif.pages) == 100000
        assert__str__(tif, 0)


def test_issue_pages_iterator():
    """Test iterate over pages in series."""
    data = random_data(numpy.int8, (8, 219, 301))
    with TempFileName('issue_page_iterator') as filename:
        imwrite(filename, data[0])
        imwrite(
            filename,
            data,
            photometric=PHOTOMETRIC.MINISBLACK,
            append=True,
            metadata={'axes': 'ZYX'},
        )
        imwrite(filename, data[-1], append=True)
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 10
            assert len(tif.series) == 3
            page = tif.pages[1]
            assert isinstance(page, TiffPage)
            assert page.is_contiguous
            assert page.photometric == PHOTOMETRIC.MINISBLACK
            assert page.imagewidth == 301
            assert page.imagelength == 219
            assert page.samplesperpixel == 1
            # test read series 1
            series = tif.series[1]
            assert len(series._pages) == 1
            assert len(series.pages) == 8
            image = series.asarray()
            assert_array_equal(data, image)
            for i, page in enumerate(series.pages):
                assert page is not None
                im = page.asarray()
                assert_array_equal(image[i], im)
            assert__str__(tif)


def test_issue_tile_partial():
    """Test write single tiles larger than image data."""
    # https://github.com/cgohlke/tifffile/issues/3
    data = random_data(numpy.uint8, (3, 15, 15, 15))
    with TempFileName('issue_tile_partial_2d') as filename:
        imwrite(filename, data[0, 0], tile=(16, 16))
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 1
            page = tif.pages.first
            assert not page.is_contiguous
            assert page.is_tiled
            assert (
                page.tags['TileOffsets'].value[0]
                + page.tags['TileByteCounts'].value[0]
                == tif.filehandle.size
            )
            assert_array_equal(page.asarray(), data[0, 0])
            assert_aszarr_method(page, data[0, 0])
            assert__str__(tif)

    with TempFileName('issue_tile_partial_3d') as filename:
        imwrite(filename, data[0], tile=(16, 16, 16))
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 1
            page = tif.pages.first
            assert not page.is_contiguous
            assert page.is_tiled
            assert page.is_volumetric
            assert (
                page.tags['TileOffsets'].value[0]
                + page.tags['TileByteCounts'].value[0]
                == tif.filehandle.size
            )
            assert_array_equal(page.asarray(), data[0])
            assert_aszarr_method(page, data[0])
            assert__str__(tif)

    with TempFileName('issue_tile_partial_3d_separate') as filename:
        imwrite(
            filename,
            data,
            tile=(16, 16, 16),
            planarconfig=PLANARCONFIG.SEPARATE,
            photometric=PHOTOMETRIC.RGB,
        )
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 1
            page = tif.pages.first
            assert not page.is_contiguous
            assert page.is_tiled
            assert (
                page.tags['TileOffsets'].value[0]
                + page.tags['TileByteCounts'].value[0] * 3
                == tif.filehandle.size
            )
            assert_array_equal(page.asarray(), data)
            assert_aszarr_method(page, data)
            assert__str__(tif)

    # test complete tile is contiguous
    data = random_data(numpy.uint8, (16, 16))
    with TempFileName('issue_tile_partial_not') as filename:
        imwrite(filename, data, tile=(16, 16))
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 1
            page = tif.pages.first
            assert page.is_contiguous
            assert page.is_memmappable
            assert page.is_tiled
            assert (
                page.tags['TileOffsets'].value[0]
                + page.tags['TileByteCounts'].value[0]
                == tif.filehandle.size
            )
            assert_array_equal(page.asarray(), data)
            assert_aszarr_method(page, data)
            assert__str__(tif)


@pytest.mark.parametrize('compression', [1, 8])
@pytest.mark.parametrize('samples', [1, 3])
def test_issue_tiles_pad(samples, compression):
    """Test tiles from iterator get padded."""
    # https://github.com/cgohlke/tifffile/issues/38
    if samples == 3:
        data = RNG.integers(0, 2**12, (31, 33, 3), numpy.uint16)
        photometric = 'rgb'
    else:
        data = RNG.integers(0, 2**12, (31, 33), numpy.uint16)
        photometric = None

    def tiles(data, tileshape, *, pad=False):
        for y in range(0, data.shape[0], tileshape[0]):
            for x in range(0, data.shape[1], tileshape[1]):
                tile = data[y : y + tileshape[0], x : x + tileshape[1]]
                if pad and tile.shape != tileshape:
                    tile = numpy.pad(
                        tile,
                        (
                            (0, tileshape[0] - tile.shape[0]),
                            (0, tileshape[1] - tile.shape[1]),
                        ),
                    )
                yield tile

    with TempFileName(
        f'issue_issue_tiles_pad_{compression}{samples}'
    ) as filename:
        imwrite(
            filename,
            tiles(data, (16, 16)),
            dtype=data.dtype,
            shape=data.shape,
            tile=(16, 16),
            photometric=photometric,
            compression=compression,
        )
        assert_array_equal(imread(filename), data)
        assert_valid_tiff(filename)


def test_issue_fcontiguous():
    """Test write F-contiguous arrays."""
    # https://github.com/cgohlke/tifffile/issues/24
    data = numpy.asarray(random_data(numpy.uint8, (31, 33)), order='F')
    with TempFileName('issue_fcontiguous') as filename:
        imwrite(filename, data, compression=COMPRESSION.ADOBE_DEFLATE)
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 1
            page = tif.pages.first
            assert_array_equal(page.asarray(), data)
            assert__str__(tif)


def test_issue_pathlib():
    """Test support for pathlib.Path."""
    data = random_data(numpy.uint16, (219, 301))
    with TempFileName('issue_pathlib') as filename_str:
        filename = pathlib.Path(filename_str)
        assert isinstance(filename, os.PathLike)
        # imwrite
        imwrite(filename, data)
        # imread
        im = imread(filename)
        assert_array_equal(im, data)
        # memmap
        im = memmap(filename)
        try:
            assert_array_equal(im, data)
        finally:
            del im
        # TiffFile
        with (
            TiffFile(filename) as tif,
            TempFileName('issue_pathlib_out') as outfilename_str,
        ):
            outfilename = pathlib.Path(outfilename_str)
            # out=file
            im = tif.asarray(out=outfilename)
            try:
                assert isinstance(im, numpy.memmap)
                assert_array_equal(im, data)
                assert os.path.samefile(im.filename, outfilename_str)
            finally:
                del im
        # TiffSequence
        with TiffSequence(filename) as tifs:
            im = tifs.asarray()
            assert_array_equal(im[0], data)
        with TiffSequence([filename]) as tifs:
            im = tifs.asarray()
            assert_array_equal(im[0], data)

    # TiffSequence container
    if SKIP_FILE or SKIP_CODECS:
        pytest.skip(REASON)
    filename = pathlib.Path(_file('TiffSequence.zip'))
    with TiffSequence('*.tif', container=filename, pattern=None) as tifs:
        im = tifs.asarray()
        assert im[9, 256, 256] == 135


@pytest.mark.skipif(SKIP_FILE or SKIP_CODECS, reason=REASON)
def test_issue_lzw_corrupt():
    """Test decode corrupted LZW segment raises RuntimeError."""
    # reported by S Richter on 2020.2.17
    filename = _file('issues/lzw_corrupt.tiff')
    with pytest.raises(RuntimeError), TiffFile(filename) as tif:
        _ = tif.asarray()


def test_issue_iterable_compression():
    """Test write iterable of pages with compression."""
    # https://github.com/cgohlke/tifffile/issues/20
    data = RNG.random((10, 10, 10)) * 127
    data = data.astype(numpy.int8)
    with TempFileName('issue_iterable_compression') as filename:
        with TiffWriter(filename) as tif:
            tif.write(data, shape=(10, 10, 10), dtype=numpy.int8)
            tif.write(
                data,
                shape=(10, 10, 10),
                dtype=numpy.int8,
                compression=COMPRESSION.ADOBE_DEFLATE,
            )
        with TiffFile(filename) as tif:
            assert_array_equal(tif.series[0].asarray(), data)
            assert_array_equal(tif.series[1].asarray(), data)
    # fail with wrong dtype
    with TempFileName('issue_iterable_compression_fail') as filename:
        with TiffWriter(filename) as tif:  # noqa: SIM117
            with pytest.raises(ValueError):
                tif.write(data, shape=(10, 10, 10), dtype=numpy.uint8)
        with TiffWriter(filename) as tif:  # noqa: SIM117
            with pytest.raises(ValueError):
                tif.write(
                    data,
                    shape=(10, 10, 10),
                    dtype=numpy.uint8,
                    compression=COMPRESSION.ADOBE_DEFLATE,
                )


def test_issue_write_separated():
    """Test write SEPARATED colorspace."""
    # https://github.com/cgohlke/tifffile/issues/37
    contig = random_data(numpy.uint8, (63, 95, 4))
    separate = random_data(numpy.uint8, (4, 63, 95))
    extrasample = random_data(numpy.uint8, (63, 95, 5))
    with TempFileName('issue_write_separated') as filename:
        with TiffWriter(filename) as tif:
            tif.write(contig, photometric=PHOTOMETRIC.SEPARATED)
            tif.write(separate, photometric=PHOTOMETRIC.SEPARATED)
            tif.write(
                extrasample,
                photometric=PHOTOMETRIC.SEPARATED,
                extrasamples=[1],
            )
        assert_valid_tiff(filename)
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 3
            assert len(tif.series) == 3
            page = tif.pages.first
            assert page.photometric == PHOTOMETRIC.SEPARATED
            assert_array_equal(page.asarray(), contig)
            page = tif.pages[1]
            assert page.photometric == PHOTOMETRIC.SEPARATED
            assert_array_equal(page.asarray(), separate)
            page = tif.pages[2]
            assert page.photometric == PHOTOMETRIC.SEPARATED
            assert page.extrasamples == (1,)
            assert_array_equal(page.asarray(), extrasample)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_issue_mmap():
    """Test read from mmap object with no readinto function.."""
    filename = _file('OME/bioformats-artificial/4D-series.ome.tiff')
    with open(filename, 'rb') as fh:
        mm = mmap.mmap(fh.fileno(), 0, access=mmap.ACCESS_READ)
        assert_array_equal(imread(mm), imread(filename))
        mm.close()


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_issue_micromanager(caplog):
    """Test fallback to ImageJ metadata if OME series fails."""
    # https://github.com/cgohlke/tifffile/issues/54
    # https://forum.image.sc/t/47567/9
    # OME-XML does not contain reference to master file
    # file has corrupted MicroManager DisplaySettings metadata
    filename = _file(
        'OME/'
        'image_stack_tpzc_50tp_2p_5z_3c_512k_1_MMStack_2-Pos001_000.ome.tif'
    )
    with TiffFile(
        filename, is_mmstack=False, is_ndtiff=False, is_mdgel=False
    ) as tif:
        assert len(tif.pages) == 750
        with caplog.at_level(logging.DEBUG):
            assert len(tif.series) == 1
            assert 'OME series is BinaryOnly' in caplog.text
        assert tif.is_micromanager
        assert tif.is_ome
        assert tif.is_imagej
        assert tif.micromanager_metadata is not None
        assert 'DisplaySettings' not in tif.micromanager_metadata
        assert 'failed to read display settings' not in caplog.text
        series = tif.series[0]
        assert series.shape == (50, 5, 3, 256, 256)


@pytest.mark.skipif(IS_PYPY, reason=REASON)
def test_issue_pickle():
    """Test that TIFF constants are picklable."""
    # https://github.com/cgohlke/tifffile/issues/64
    from pickle import dumps, loads

    with pytest.raises(AttributeError):
        assert loads(dumps(TIFF)).CHUNKMODE.PLANE == TIFF.CHUNKMODE.PLANE
    with pytest.raises(AttributeError):
        assert loads(dumps(TIFF.CHUNKMODE)).PLANE == TIFF.CHUNKMODE.PLANE
    with pytest.raises(AttributeError):
        assert loads(dumps(TIFF.CHUNKMODE.PLANE)) == TIFF.CHUNKMODE.PLANE


def test_issue_imagej_singlet_dimensions():
    """Test that ImageJ files can be read preserving singlet dimensions."""
    # https://github.com/cgohlke/tifffile/issues/19
    # https://github.com/cgohlke/tifffile/issues/66

    data = RNG.integers(0, 2**8, (1, 10, 1, 248, 260, 1), numpy.uint8)

    with TempFileName('issue_imagej_singlet_dimensions') as filename:
        imwrite(filename, data, imagej=True)
        image = imread(filename, squeeze=False)
        assert_array_equal(image, data)

        with TiffFile(filename) as tif:
            assert tif.is_imagej
            series = tif.series[0]
            assert series.axes == 'ZYX'
            assert series.shape == (10, 248, 260)
            assert series.get_axes(squeeze=False) == 'TZCYXS'
            assert series.get_shape(squeeze=False) == (1, 10, 1, 248, 260, 1)
            data = tif.asarray(squeeze=False)
            assert_array_equal(image, data)
            assert_aszarr_method(series, data, squeeze=False)
            assert_aszarr_method(series, data, squeeze=False, chunkmode='page')


@pytest.mark.skipif(
    SKIP_FILE or SKIP_CODECS or not imagecodecs.JPEG.available,
    reason=REASON,
)
def test_issue_cr2_ojpeg():
    """Test read OJPEG image from CR2."""
    # https://github.com/cgohlke/tifffile/issues/75
    filename = _file('CanonCR2/Canon - EOS M6 - RAW (3 2).cr2')
    with TiffFile(filename) as tif:
        assert len(tif.pages) == 4
        page = tif.pages.first
        assert page.compression == 6
        assert page.shape == (4000, 6000, 3)
        assert page.dtype == numpy.uint8
        assert page.photometric == PHOTOMETRIC.YCBCR
        assert page.compression == COMPRESSION.OJPEG
        data = page.asarray()
        assert data.shape == (4000, 6000, 3)
        assert data.dtype == numpy.uint8
        assert tuple(data[1640, 2372]) == (71, 75, 58)
        assert_aszarr_method(page, data)

        page = tif.pages[1]
        assert page.shape == (120, 160, 3)
        assert page.dtype == numpy.uint8
        assert page.photometric == PHOTOMETRIC.YCBCR
        assert page.compression == COMPRESSION.OJPEG
        data = page.asarray()
        assert tuple(data[60, 80]) == (124, 144, 107)
        assert_aszarr_method(page, data)

        page = tif.pages[2]
        assert page.shape == (400, 600, 3)
        assert page.dtype == numpy.uint16
        assert page.photometric == PHOTOMETRIC.RGB
        assert page.compression == COMPRESSION.NONE
        data = page.asarray()
        assert tuple(data[200, 300]) == (1648, 2340, 1348)
        assert_aszarr_method(page, data)

        page = tif.pages[3]
        assert page.shape == (4056, 3144, 2)
        assert page.dtype == numpy.uint16
        assert page.photometric == PHOTOMETRIC.MINISWHITE
        assert page.compression == COMPRESSION.OJPEG  # SOF3
        data = page.asarray()
        assert tuple(data[2000, 1500]) == (1759, 2467)
        assert_aszarr_method(page, data)


@pytest.mark.skipif(
    SKIP_FILE or SKIP_CODECS or not imagecodecs.JPEG.available,
    reason=REASON,
)
def test_issue_ojpeg_preview():
    """Test read JPEGInterchangeFormat from RAW image."""
    # https://github.com/cgohlke/tifffile/issues/93
    filename = _file('RAW/RAW_NIKON_D3X.NEF')
    with TiffFile(filename) as tif:
        assert len(tif.pages) == 1
        page = tif.pages.first
        assert page.compression == COMPRESSION.NONE
        assert page.shape == (120, 160, 3)
        assert page.dtype == numpy.uint8
        assert page.photometric == PHOTOMETRIC.RGB
        data = page.asarray()
        assert data.shape == (120, 160, 3)
        assert data.dtype == numpy.uint8
        assert tuple(data[60, 80]) == (180, 167, 159)
        assert_aszarr_method(page, data)

        page = tif.pages.first.pages[0]
        assert page.shape == (4032, 6048, 3)
        assert page.dtype == numpy.uint8
        assert page.photometric == COMPRESSION.OJPEG
        data = page.asarray()
        assert tuple(data[60, 80]) == (67, 13, 11)
        assert_aszarr_method(page, data)

        page = tif.pages.first.pages[1]
        assert page.shape == (4044, 6080)
        assert page.bitspersample == 14
        assert page.photometric == PHOTOMETRIC.CFA
        assert page.compression == COMPRESSION.NIKON_NEF
        with pytest.raises(ValueError):
            data = page.asarray()


@pytest.mark.skipif(
    SKIP_FILE or SKIP_CODECS or not imagecodecs.JPEG.available,
    reason=REASON,
)
def test_issue_arw(caplog):
    """Test read Sony ARW RAW image."""
    # https://github.com/cgohlke/tifffile/issues/95
    filename = _file('RAW/A1_full_lossless_compressed.ARW')
    with TiffFile(filename) as tif:
        assert len(tif.pages) == 3
        assert len(tif.series) == 4

        page = tif.pages.first
        assert page.compression == COMPRESSION.OJPEG
        assert page.photometric == PHOTOMETRIC.YCBCR
        assert page.shape == (1080, 1616, 3)
        assert page.dtype == numpy.uint8
        data = page.asarray()
        assert data.shape == (1080, 1616, 3)
        assert data.dtype == numpy.uint8
        assert tuple(data[60, 80]) == (122, 119, 104)
        assert_aszarr_method(page, data)
        assert tif.pages.first.pages is not None
        page = tif.pages.first.pages[0]
        assert page.is_tiled
        assert page.compression == COMPRESSION.JPEG
        assert page.photometric == PHOTOMETRIC.CFA
        assert page.bitspersample == 14
        assert page.tags['SonyRawFileType'].value == 4
        assert page.tags['CFARepeatPatternDim'].value == (2, 2)
        assert page.tags['CFAPattern'].value == b'\0\1\1\2'
        assert page.shape == (6144, 8704)
        assert page.dtype == numpy.uint16
        data = page.asarray()
        assert 'SonyRawFileType' in caplog.text
        assert data[60, 80] == 1000  # might not be correct according to #95
        assert_aszarr_method(page, data)

        page = tif.pages[1]
        assert page.compression == COMPRESSION.OJPEG
        assert page.photometric == PHOTOMETRIC.YCBCR
        assert page.shape == (120, 160, 3)
        assert page.dtype == numpy.uint8
        data = page.asarray()
        assert tuple(data[60, 80]) == (56, 54, 29)
        assert_aszarr_method(page, data)

        page = tif.pages[2]
        assert page.compression == COMPRESSION.JPEG
        assert page.photometric == PHOTOMETRIC.YCBCR
        assert page.shape == (5760, 8640, 3)
        assert page.dtype == numpy.uint8
        data = page.asarray()
        assert tuple(data[60, 80]) == (243, 238, 218)
        assert_aszarr_method(page, data)


def test_issue_rational_rounding():
    """Test rational are rounded to 64-bit."""
    # https://github.com/cgohlke/tifffile/issues/81
    # https://github.com/cgohlke/tifffile/issues/318

    data = numpy.array([[255]])

    with TempFileName('issue_rational_rounding') as filename:
        imwrite(
            filename, data, resolution=(7411.824413635355, 7411.824413635355)
        )

        with TiffFile(filename) as tif:
            tags = tif.pages.first.tags
            assert tags['XResolution'].value == (3395816297, 458162)
            assert tags['YResolution'].value == (3395816297, 458162)


def test_issue_omexml_micron():
    """Test OME-TIFF can be created with micron character in XML."""
    # https://forum.image.sc/t/micro-character-in-omexml-from-python/53578/4

    with TempFileName('issue_omexml_micron', ext='.ome.tif') as filename:
        imwrite(
            filename,
            [[0]],
            metadata={'PhysicalSizeX': 1.0, 'PhysicalSizeXUnit': 'µm'},
        )
        with TiffFile(filename) as tif:
            assert tif.is_ome
            assert (
                'PhysicalSizeXUnit="µm"'
                in tif.pages.first.tags['ImageDescription'].value
            )


def test_issue_svs_doubleheader():
    """Test svs_description_metadata for SVS with double header."""
    # https://github.com/cgohlke/tifffile/pull/88
    # https://github.com/bertrandchauveau/DICOMtoSVS/issues/3

    assert svs_description_metadata(
        'Aperio Image Library v11.2.1\r\n'
        '2220x2967 -> 574x768 - ;Aperio Image Library v10.0.51\r\n'
        '46920x33014 [0,100 46000x32914] (256x256) JPEG/RGB Q=30'
        '|AppMag = 20|StripeWidth = 2040|ScanScope ID = CPAPERIOCS'
        '|Filename = CMU-1|Date = 12/29/09|Time = 09:59:15'
        '|User = b414003d-95c6-48b0-9369-8010ed517ba7|Parmset = USM Filter'
        '|MPP = 0.4990|Left = 25.691574|Top = 23.449873'
        '|LineCameraSkew = -0.000424|LineAreaXOffset = 0.019265'
        '|LineAreaYOffset = -0.000313|Focus Offset = 0.000000'
        '|ImageID = 1004486|OriginalWidth = 46920|Originalheight = 33014'
        '|Filtered = 5|OriginalWidth = 46000|OriginalHeight = 32914'
        '|Key'
        '|',
    ) == {
        'Header': (
            'Aperio Image Library v11.2.1\r\n'
            '2220x2967 -> 574x768 - ;Aperio Image Library v10.0.51\r\n'
            '46920x33014 [0,100 46000x32914] (256x256) JPEG/RGB Q=30'
        ),
        'AppMag': 20,
        'StripeWidth': 2040,
        'ScanScope ID': 'CPAPERIOCS',
        'Filename': 'CMU-1',
        'Date': '12/29/09',
        'Time': '09:59:15',
        'User': 'b414003d-95c6-48b0-9369-8010ed517ba7',
        'Parmset': 'USM Filter',
        'MPP': 0.499,
        'Left': 25.691574,
        'Top': 23.449873,
        'LineCameraSkew': -0.000424,
        'LineAreaXOffset': 0.019265,
        'LineAreaYOffset': -0.000313,
        'Focus Offset': 0.0,
        'ImageID': 1004486,
        'OriginalWidth': 46000,
        'Originalheight': 33014,
        'Filtered': 5,
        'OriginalHeight': 32914,
    }


@pytest.mark.skipif(SKIP_FILE or SKIP_CODECS, reason=REASON)
def test_issue_packbits_dtype():
    """Test read and efficiently write PackBits compressed int16 image."""
    # https://github.com/blink1073/tifffile/issues/61
    # requires imagecodecs > 2021.6.8
    filename = _file('packbits/imstack_packbits-int16.tif')
    with TiffFile(filename) as tif:
        assert len(tif.pages) == 519
        page = tif.pages[181]
        assert page.compression == COMPRESSION.PACKBITS
        assert page.photometric == PHOTOMETRIC.MINISBLACK
        assert page.shape == (348, 185)
        assert page.dtype == numpy.int16
        data = page.asarray()
        assert data.shape == (348, 185)
        assert data.dtype == numpy.int16
        assert data[184, 72] == 24
        assert_aszarr_method(page, data)
        data = tif.asarray()
        assert_aszarr_method(tif, data)

    buf = BytesIO()
    imwrite(buf, data, compression='packbits')
    assert buf.seek(0, 2) < 1700000  # efficiently compressed
    buf.seek(0)

    with TiffFile(buf) as tif:
        assert len(tif.pages) == 519
        page = tif.pages[181]
        assert page.compression == COMPRESSION.PACKBITS
        assert page.photometric == PHOTOMETRIC.MINISBLACK
        assert page.shape == (348, 185)
        assert page.dtype == numpy.int16
        data = page.asarray()
        assert data.shape == (348, 185)
        assert data.dtype == numpy.int16
        assert data[184, 72] == 24
        assert_aszarr_method(page, data)
        data = tif.asarray()
        assert_aszarr_method(tif, data)


@pytest.mark.skipif(SKIP_FILE or SKIP_CODECS, reason=REASON)
def test_issue_predictor_byteorder():
    """Test read big-endian uint32 RGB with horizontal predictor."""
    filename = _file('issues/flower-rgb-contig-32_msb_zip_predictor.tiff')
    with TiffFile(filename) as tif:
        assert tif.tiff.byteorder == '>'
        assert len(tif.pages) == 1
        page = tif.pages.first
        assert page.compression == COMPRESSION.ADOBE_DEFLATE
        assert page.photometric == PHOTOMETRIC.RGB
        assert page.predictor == PREDICTOR.HORIZONTAL
        assert page.shape == (43, 73, 3)
        assert page.dtype == numpy.uint32
        data = page.asarray()
        assert data.shape == (43, 73, 3)
        assert data.dtype == numpy.uint32
        assert tuple(data[30, 2]) == (0, 246337650, 191165795)
        assert data.dtype.byteorder == '='
        assert_aszarr_method(page, data)
        data = tif.asarray()
        assert_aszarr_method(tif, data)


@pytest.mark.skipif(SKIP_ZARR or SKIP_DASK, reason=REASON)
@pytest.mark.parametrize('truncate', [False, True])
@pytest.mark.parametrize('chunkmode', [0, 2])
def test_issue_dask_multipage(truncate, chunkmode):
    """Test multi-threaded access of memory-mappable, multi-page Zarr."""
    # https://github.com/cgohlke/tifffile/issues/67#issuecomment-908529425
    data = numpy.arange(5 * 99 * 101, dtype=numpy.uint16).reshape((5, 99, 101))
    with TempFileName(
        f'issue_dask_multipage_{int(truncate)}_{chunkmode}'
    ) as filename:
        kwargs = {'truncate': truncate}
        if not truncate:
            kwargs['tile'] = (32, 32)
        imwrite(filename, data, **kwargs)
        with imread(filename, aszarr=True, chunkmode=chunkmode) as store:
            assert isinstance(store, ZarrStore)
            assert isinstance(store, ZarrTiffStore)
            daskarray = dask.array.from_zarr(store).compute()
            assert_array_equal(data, daskarray)


@pytest.mark.skipif(SKIP_FILE or SKIP_ZARR, reason=REASON)
@pytest.mark.parametrize('chunkmode', [0, 2])
def test_issue_zarr_store_closed(chunkmode):
    """Test Zarr store filehandle is open when reading from unloaded pages."""
    # https://github.com/cgohlke/tifffile/issues/67#issuecomment-2246367891
    filename = _file('ImageJ/_malaria_parasites.tif')
    data = imread(filename)
    store = imread(filename, aszarr=True, chunkmode=chunkmode)
    assert isinstance(store, ZarrStore)
    assert isinstance(store, ZarrTiffStore)
    try:
        z = zarr.open(store, mode='r')
        chunk = z[10:11, 3:-3]  # seek of closed file
    finally:
        store.close()
    assert_array_equal(chunk, data[10:11, 3:-3])

    store = imread(filename, aszarr=True, chunkmode=chunkmode)
    assert isinstance(store, ZarrStore)
    assert isinstance(store, ZarrTiffStore)
    try:
        z = zarr.open(store, mode='r')
        chunk = z[:]
    finally:
        store.close()
    assert_array_equal(chunk, data)


@pytest.mark.skipif(SKIP_FILE or SKIP_ZARR or SKIP_CODECS, reason=REASON)
@pytest.mark.parametrize('chunkmode', [0, 2])
def test_issue_zarr_store_multifile_closed(chunkmode):
    """Test Zarr store can read from closed files."""
    filename = _file('OME/tubhiswt-4D-lzw/tubhiswt_C0_T0.ome.tif')
    data = imread(filename)
    assert data.shape == (43, 10, 2, 512, 512)
    store = imread(filename, aszarr=True, chunkmode=chunkmode)
    try:
        z = zarr.open(store, mode='r')
        chunk = z[32:35, 3:7, :, 31:-31, 33:-33]  # seek of closed file
    finally:
        store.close()
    assert_array_equal(chunk, data[32:35, 3:7, :, 31:-31, 33:-33])


@pytest.mark.skipif(
    SKIP_FILE or SKIP_CODECS or not imagecodecs.LZW.available, reason=REASON
)
def test_issue_read_from_closed_file():
    """Test read from closed file handles."""
    filename = _file('OME/tubhiswt-4D-lzw/tubhiswt_C0_T0.ome.tif')
    with tifffile.TiffFile(filename) as tif:
        count = 0
        for frame in tif.series[0].pages[:10]:
            # most file handles are closed
            if frame is None:
                continue
            isclosed = frame.parent.filehandle.closed
            if not isclosed:
                continue
            count += 1

            if isinstance(frame, TiffFrame):
                with pytest.warns(UserWarning):
                    page = frame.aspage()  # re-load frame as page
                assert isclosed == page.parent.filehandle.closed
            else:
                page = frame

            with pytest.warns(UserWarning):
                _ = page.colormap  # delay load tag value
            assert isclosed == page.parent.filehandle.closed

            with pytest.warns(UserWarning):
                _ = frame.asarray()  # read data
            assert isclosed == page.parent.filehandle.closed
        assert count > 0


@pytest.mark.skipif(
    SKIP_FILE or SKIP_CODECS or not imagecodecs.PNG.available, reason=REASON
)
def test_issue_filesequence_categories():
    """Test FileSequence with categories."""
    # https://github.com/cgohlke/tifffile/issues/76

    with tifffile.FileSequence(
        imagecodecs.imread,
        _files('dataset-A1-20200531/*.png'),
        pattern=(
            r'(?P<sampleid>.{2})-'
            r'(?P<experiment>.+)-\d{8}T\d{6}-PSII0-'
            r'(?P<frameid>\d)'
        ),
        categories={'sampleid': {'A1': 0, 'B1': 1}, 'experiment': {'doi': 0}},
    ) as pngs:
        assert len(pngs) == 2
        assert pngs.files_missing == 2
        assert pngs.shape == (2, 1, 2)
        assert pngs.dims == ('sampleid', 'experiment', 'frameid')
        data = pngs.asarray()
        assert data.shape == (2, 1, 2, 200, 200)
        assert data[1, 0, 1, 100, 100] == 353


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_issue_filesequence_file_parameter():
    """Test FileSequence.asarray with 'file' parameter removed in 2022.4.22."""
    # https://github.com/bluesky/tiled/pull/97

    files = _files('tifffile/temp_C001T00*.tif')
    with TiffSequence(files) as tiffs:
        assert tiffs.shape == (2,)
        with pytest.raises(TypeError):
            assert_array_equal(tiffs.asarray(file=files[0]), imread(files[0]))
        with pytest.raises(TypeError):
            assert_array_equal(tiffs.asarray(file=1), imread(files[1]))


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_issue_imagej_prop():
    """Test read and write ImageJ prop metadata type."""
    # https://github.com/cgohlke/tifffile/issues/103
    # also test write indexed ImageJ file
    filename = _file('issues/triple-sphere-big-distance=035.tif')
    with tifffile.TiffFile(filename) as tif:
        assert tif.is_imagej
        ijmeta = tif.imagej_metadata
        assert ijmeta is not None
        prop = ijmeta['Properties']
        assert ijmeta['slices'] == 500
        assert not ijmeta['loop']
        assert prop['CurrentLUT'] == 'glasbey_on_dark'
        assert tif.pages.first.photometric == PHOTOMETRIC.PALETTE
        colormap = tif.pages.first.colormap
        data = tif.asarray()

    prop['Test'] = 0.1
    with TempFileName('issue_imagej_prop') as filename:
        ijmeta['axes'] = 'ZYX'
        imwrite(
            filename, data, imagej=True, colormap=colormap, metadata=ijmeta
        )

    with tifffile.TiffFile(filename) as tif:
        assert tif.is_imagej
        ijmeta = tif.imagej_metadata
        assert ijmeta is not None
        prop = ijmeta['Properties']
        assert ijmeta['slices'] == 500
        assert not ijmeta['loop']
        assert prop['CurrentLUT'] == 'glasbey_on_dark'
        assert prop['Test'] == '0.1'
        assert tif.pages.first.photometric == PHOTOMETRIC.PALETTE
        colormap = tif.pages.first.colormap
        image = tif.asarray()
        assert_array_equal(image, data)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_issue_missing_dataoffset(caplog):
    """Test read file with missing data offset."""
    filename = _file('gdal/bigtiff_header_extract.tif')
    with tifffile.TiffFile(filename) as tif:
        page = tif.pages.first
        assert page.imagewidth == 100000
        assert page.imagelength == 100000
        assert page.rowsperstrip == 1
        assert page.databytecounts == (10000000000,)
        assert page.dataoffsets == ()
        assert 'incorrect StripOffsets count' in caplog.text
        assert 'incorrect StripByteCounts count' in caplog.text
        assert 'missing data offset tag' in caplog.text
        with pytest.raises(TiffFileError):
            tif.asarray()


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_issue_imagej_metadatabytecounts():
    """Test read ImageJ file with many IJMetadataByteCounts."""
    # https://github.com/cgohlke/tifffile/issues/111
    filename = _file('imagej/issue111.tif')
    with tifffile.TiffFile(filename) as tif:
        assert tif.is_imagej
        page = tif.pages.first
        assert isinstance(page.tags['IJMetadataByteCounts'].value, tuple)
        assert isinstance(page.tags['IJMetadata'].value, dict)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_issue_description_bytes():
    """Test read file with imagedescription bytes."""
    # https://github.com/cgohlke/tifffile/issues/112
    with TempFileName('issue_description_bytes') as filename:
        imwrite(
            filename,
            [[0]],
            description='1st description',
            extratags=[
                ('ImageDescription', 1, None, b'\1\128\0', True),
                ('ImageDescription', 1, None, b'\2\128\0', True),
            ],
            metadata=False,
        )
        with TiffFile(filename) as tif:
            page = tif.pages.first
            assert page.description == '1st description'
            assert page.description1 == ''
            assert page.tags.get(270).value == '1st description'
            assert page.tags.get(270, index=1).value == b'\1\128\0'
            assert page.tags.get(270, index=2).value == b'\2\128\0'


def test_issue_imagej_colormap():
    """Test write 32-bit imagej file with colormap."""
    # https://github.com/cgohlke/tifffile/issues/115
    colormap = numpy.vstack(
        [
            numpy.zeros(256, dtype=numpy.uint16),
            numpy.arange(0, 2**16, 2**8, dtype=numpy.uint16),
            numpy.arange(0, 2**16, 2**8, dtype=numpy.uint16),
        ]
    )
    metadata = {'min': 0.0, 'max': 1.0, 'Properties': {'CurrentLUT': 'cyan'}}
    with TempFileName('issue_imagej_colormap') as filename:
        imwrite(
            filename,
            numpy.zeros((16, 16), numpy.float32),
            imagej=True,
            colormap=colormap,
            metadata=metadata,
        )
        with TiffFile(filename) as tif:
            assert tif.is_imagej
            assert tif.imagej_metadata is not None
            assert tif.imagej_metadata['Properties']['CurrentLUT'] == 'cyan'
            assert tif.pages.first.photometric == PHOTOMETRIC.MINISBLACK
            assert_array_equal(tif.pages.first.colormap, colormap)


@pytest.mark.skipif(
    SKIP_FILE or SKIP_CODECS or not imagecodecs.WEBP.available,
    reason=REASON,
)
@pytest.mark.parametrize('name', ['tile', 'strip'])
def test_issue_webp_rgba(name, caplog):
    """Test read WebP segments with missing alpha channel."""
    # https://github.com/cgohlke/tifffile/issues/122
    filename = _file(f'issues/CMU-1-Small-Region.{name}.webp.tiff')
    with tifffile.TiffFile(filename) as tif:
        page = tif.pages.first
        assert page.compression == COMPRESSION.WEBP
        assert page.shape == (2967, 2220, 4)
        assert tuple(page.asarray()[25, 25]) == (246, 244, 245, 255)
        assert f'corrupted {name}' not in caplog.text


@pytest.mark.skipif(
    SKIP_FILE or SKIP_ZARR or SKIP_CODECS or not imagecodecs.WEBP.available,
    reason=REASON,
)
def test_issue_webp_fsspec():
    """Test read WebP segments with missing alpha channel via fsspec."""
    try:
        from imagecodecs.numcodecs import register_codecs
    except ImportError:
        register_codecs = None
    else:
        register_codecs('imagecodecs_webp', verbose=False)

    filename = _file('issues/CMU-1-Small-Region.tile.webp.tiff')
    url = os.path.dirname(filename).replace('\\', '/')
    data = imread(filename, series=0)
    with TempFileName('issue_webp_fsspec', ext='.json') as jsonfile:
        tiff2fsspec(
            filename,
            url,
            out=jsonfile,
            series=0,
            level=0,
            version=0,
        )
        zobj = zarr.open(refs_as_store(jsonfile), zarr_format=2)
        assert_array_equal(zobj[:], data)


@pytest.mark.skipif(SKIP_FILE or SKIP_ZARR, reason=REASON)
def test_issue_tiffslide():
    """Test no ValueError when closing TiffSlide with Zarr group."""
    # https://github.com/bayer-science-for-a-better-life/tiffslide/issues/25
    tiffslide = pytest.importorskip('tiffslide', exc_type=ImportError)

    filename = _file('AperioSVS/CMU-1.svs')
    with tiffslide.TiffSlide(filename) as slide:
        _ = slide.ts_zarr_grp
        arr = slide.read_region((100, 200), 0, (256, 256), as_array=True)
        assert arr.shape == (256, 256, 3)


@pytest.mark.skipif(SKIP_ZARR, reason=REASON)
def test_issue_xarray():
    """Test read Zarr store with fsspec and xarray."""
    xarray = pytest.importorskip('xarray')

    data = RNG.integers(0, 2**8, (5, 31, 33, 3), numpy.uint8)

    with TempFileName('issue_xarry.ome') as filename:
        with tifffile.TiffWriter(filename) as tif:
            tif.write(
                data,
                photometric='rgb',
                tile=(16, 16),
                metadata={'axes': 'TYXC'},
            )

        for squeeze in (True, False):
            with TempFileName(
                f'issue_xarry_{squeeze}', ext='.json'
            ) as jsonfile:
                with tifffile.TiffFile(filename) as tif:
                    store = tif.series[0].aszarr(squeeze=squeeze)
                    store.write_fsspec(
                        jsonfile,
                        url=os.path.split(jsonfile)[0],
                        groupname='x',
                    )
                    store.close()

                dataset = xarray.open_zarr(
                    refs_as_store(jsonfile),
                    consolidated=False,
                    mask_and_scale=False,
                )

                if squeeze:
                    assert dataset['x'].shape == (5, 31, 33, 3)
                    assert dataset['x'].dims == ('T', 'Y', 'X', 'S')
                else:
                    assert dataset['x'].shape == (5, 1, 1, 31, 33, 3)
                    assert dataset['x'].dims == ('T', 'Z', 'C', 'Y', 'X', 'S')

                assert_array_equal(data, numpy.squeeze(dataset['x'][:]))
                del dataset


@pytest.mark.skipif(SKIP_ZARR, reason=REASON)
def test_issue_xarray_multiscale():
    """Test read multiscale Zarr store with fsspec and xarray."""
    xarray = pytest.importorskip('xarray')

    data = RNG.integers(0, 2**8, (8, 3, 128, 128), numpy.uint8)

    with TempFileName('issue_xarry_multiscale.ome') as filename:
        with tifffile.TiffWriter(filename) as tif:
            tif.write(
                data,
                photometric='rgb',
                planarconfig='separate',
                tile=(32, 32),
                subifds=2,
                metadata={'axes': 'TCYX'},
            )
            tif.write(
                data[:, :, ::2, ::2],
                photometric='rgb',
                planarconfig='separate',
                tile=(32, 32),
            )
            tif.write(
                data[:, :, ::4, ::4],
                photometric='rgb',
                planarconfig='separate',
                tile=(16, 16),
            )

        for squeeze in (True, False):
            with TempFileName(
                f'issue_xarry_multiscale_{squeeze}', ext='.json'
            ) as jsonfile:
                with tifffile.TiffFile(filename) as tif:
                    store = tif.series[0].aszarr(squeeze=squeeze)
                    store.write_fsspec(
                        jsonfile,
                        url=os.path.split(jsonfile)[0],
                        # groupname='test',
                    )
                    store.close()

                    dataset = xarray.open_zarr(
                        refs_as_store(jsonfile),
                        consolidated=False,
                        mask_and_scale=False,
                    )

                if squeeze:
                    assert dataset['0'].shape == (8, 3, 128, 128)
                    assert dataset['0'].dims == ('T', 'S', 'Y', 'X')
                    assert dataset['2'].shape == (8, 3, 32, 32)
                    assert dataset['2'].dims == ('T', 'S', 'Y2', 'X2')
                else:
                    assert dataset['0'].shape == (8, 1, 1, 3, 128, 128)
                    assert dataset['0'].dims == ('T', 'Z', 'C', 'S', 'Y', 'X')
                    assert dataset['2'].shape == (8, 1, 1, 3, 32, 32)
                    assert dataset['2'].dims == (
                        'T',
                        'Z',
                        'C',
                        'S',
                        'Y2',
                        'X2',
                    )

                assert_array_equal(data, numpy.squeeze(dataset['0'][:]))
                assert_array_equal(
                    data[:, :, ::4, ::4], numpy.squeeze(dataset['2'][:])
                )
                del dataset


@pytest.mark.parametrize('resolution', [(1, 0), (0, 0)])
def test_issue_invalid_resolution(resolution):
    """Test invalid resolution tags."""
    # https://github.com/imageio/imageio/blob/master/tests/test_tifffile.py

    data = numpy.zeros((20, 10), dtype=numpy.uint8)

    with TempFileName(f'issue_invalid_resolution{resolution[0]}') as filename:
        imwrite(filename, data)

        with TiffFile(filename, mode='r+') as tif:
            tags = tif.pages.first.tags
            tags['XResolution'].overwrite(resolution)
            tags['YResolution'].overwrite(resolution)

        with tifffile.TiffFile(filename) as tif:
            tags = tif.pages.first.tags
            assert tags['XResolution'].value == resolution
            assert tags['YResolution'].value == resolution
            assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_issue_indexing():
    """Test indexing methods."""
    filename = _file('tifffile/multiscene_pyramidal.ome.tif')
    data0 = imread(filename)
    assert isinstance(data0, numpy.ndarray)
    assert data0.shape == (16, 32, 2, 256, 256)
    level1 = imread(filename, level=1)
    assert isinstance(level1, numpy.ndarray)
    assert level1.shape == (16, 32, 2, 128, 128)
    data1 = imread(filename, series=1)
    assert isinstance(data1, numpy.ndarray)
    assert data1.shape == (128, 128, 3)

    assert_array_equal(data1, imread(filename, key=1024))
    assert_array_equal(data1, imread(filename, key=[1024]))
    assert_array_equal(data1, imread(filename, key=range(1024, 1025)))
    assert_array_equal(data1, imread(filename, series=1, key=0))
    assert_array_equal(data1, imread(filename, series=1, key=[0]))
    assert_array_equal(
        data1, imread(filename, series=1, level=0, key=slice(None))
    )

    assert_array_equal(data0, imread(filename, series=0))
    assert_array_equal(
        data0.reshape((-1, 256, 256)),
        imread(filename, series=0, key=slice(None)),
    )
    assert_array_equal(
        data0.reshape((-1, 256, 256)), imread(filename, key=slice(0, -1, 1))
    )
    assert_array_equal(
        data0.reshape((-1, 256, 256)), imread(filename, key=range(1024))
    )
    assert_array_equal(data0[0, 0], imread(filename, key=[0, 1]))
    assert_array_equal(data0[0, 0], imread(filename, series=0, key=(0, 1)))

    assert_array_equal(
        level1.reshape((-1, 128, 128)),
        imread(filename, series=0, level=1, key=slice(None)),
    )
    assert_array_equal(
        level1.reshape((-1, 128, 128)),
        imread(filename, series=0, level=1, key=range(1024)),
    )


def test_issue_shaped_metadata():
    """Test shaped_metadata property."""
    # https://github.com/cgohlke/tifffile/issues/127
    shapes = ([5, 33, 31], [31, 33, 3])
    with TempFileName('issue_shaped_metadata') as filename:
        with TiffWriter(filename) as tif:
            for shape in shapes:
                tif.write(
                    shape=shape,
                    dtype=numpy.uint8,
                    metadata={'comment': 'a comment', 'number': 42},
                )
        with TiffFile(filename) as tif:
            assert tif.is_shaped
            assert len(tif.series) == 2
            assert tif.series[0].kind == 'shaped'
            assert tif.series[1].kind == 'shaped'
            meta = tif.shaped_metadata
            assert meta is not None
            assert len(meta) == 2
            assert meta[0]['shape'] == shapes[0]
            assert meta[0]['comment'] == 'a comment'
            assert meta[0]['number'] == 42
            assert meta[1]['shape'] == shapes[1]
            assert meta[1]['comment'] == 'a comment'
            assert meta[1]['number'] == 42


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_issue_uic_dates(caplog):
    """Test read MetaMorph STK metadata with invalid julian dates."""
    # https://github.com/cgohlke/tifffile/issues/129
    filename = _file('issues/Cells-003_Cycle00001_Ch1_000001.ome.tif')
    with TiffFile(filename) as tif:
        assert tif.is_stk
        assert tif.is_ome
        assert tif.byteorder == '<'
        assert len(tif.pages) == 1
        # assert page properties
        page = tif.pages.first
        assert page.is_memmappable
        assert page.shape == (256, 256)
        assert page.tags['Software'].value == 'Prairie View 5.4.64.40'
        assert page.tags['DateTime'].value == '2019:03:18 10:13:33'
        # assert uic tags
        with pytest.warns(RuntimeWarning):
            meta = tif.stk_metadata
        assert 'no datetime before year 1' in caplog.text
        assert meta is not None
        assert meta['CreateTime'] is None
        assert meta['LastSavedTime'] is None
        assert meta['DatetimeCreated'] is None
        assert meta['DatetimeModified'] is None
        assert meta['Name'] == 'Gattaca'
        assert meta['NumberPlanes'] == 1
        assert meta['Wavelengths'][0] == 1.7906976744186047


def test_issue_subfiletype_zero():
    """Test write NewSubfileType=0."""
    # https://github.com/cgohlke/tifffile/issues/132
    with TempFileName('issue_subfiletype_zero') as filename:
        imwrite(filename, [[0]], subfiletype=0)
        with TiffFile(filename) as tif:
            assert (
                tif.pages.first.tags['NewSubfileType'].value
                == FILETYPE.UNDEFINED
            )


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_issue_imagej_zct_order(caplog):
    """Test read ImageJ hyperstack with non-TZC order."""
    # https://forum.image.sc/t/69430
    filename = _file(
        'MMStack/mosaic/d220708_HybISS_AS_cycles1to5_NoBridgeProbes_'
        'dim3x3__3_MMStack_2-Pos_000_000.ome.tif'
    )
    data = imread(filename, series=5, is_mmstack=False)

    filename = _file(
        'MMStack/mosaic/d220708_HybISS_AS_cycles1to5_NoBridgeProbes_'
        'dim3x3__3_MMStack_2-Pos_000_001.ome.tif'
    )
    with TiffFile(filename, is_mmstack=False) as tif:
        assert not tif.is_mmstack
        assert tif.is_ome
        assert tif.is_imagej
        assert tif.imagej_metadata is not None
        assert tif.imagej_metadata['order'] == 'zct'
        with caplog.at_level(logging.DEBUG):
            series = tif.series[0]
            assert 'OME series is BinaryOnly' in caplog.text
        assert series.axes == 'CZYX'
        assert series.kind == 'imagej'
        assert_array_equal(series.asarray(), data)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_issue_fei_sfeg_metadata():
    """Test read FEI_SFEG metadata."""
    # https://github.com/cgohlke/tifffile/pull/141
    # FEI_SFEG tag value is a base64 encoded XML string with BOM header
    filename = _file('issues/Helios-AutoSliceAndView.tif')
    with TiffFile(filename) as tif:
        fei = tif.fei_metadata
        assert fei is not None
        assert fei['User']['User'] == 'Supervisor'
        assert fei['System']['DisplayHeight'] == 0.324


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_issue_resolution():
    """Test consistency of reading and writing resolution."""
    resolution = (4294967295 / 3904515723, 4294967295 / 1952257861)  # 1.1, 2.2
    resolutionunit = RESUNIT.CENTIMETER
    scale = 111.111
    with TempFileName('issue_resolution') as filename:
        imwrite(
            filename,
            [[0]],
            resolution=resolution,
            resolutionunit=resolutionunit,
        )
        with TiffFile(filename) as tif:
            page = tif.pages.first
            assert tif.pages.first.tags['XResolution'].value == (
                2863315501,
                2603014092,
            )
            assert tif.pages.first.tags['YResolution'].value == (
                1073742382,
                488064719,
            )
            assert tif.pages.first.tags['ResolutionUnit'].value == (
                resolutionunit
            )

            assert page.resolution == resolution
            assert page.resolutionunit == resolutionunit

            assert page.get_resolution() == resolution
            assert page.get_resolution(resolutionunit) == resolution
            assert_allclose(
                page.get_resolution(RESUNIT.MICROMETER),
                (resolution[0] / 10000, resolution[1] / 10000),
            )
            assert_allclose(
                page.get_resolution(RESUNIT.MICROMETER, 100),
                (resolution[0] / 10000, resolution[1] / 10000),
            )
            assert_allclose(
                page.get_resolution('inch'),
                (resolution[0] * 2.54, resolution[1] * 2.54),
            )
            assert_allclose(
                page.get_resolution(scale=111.111),
                (resolution[0] * scale, resolution[1] * scale),
            )


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_issue_resolutionunit():
    """Test write resolutionunit defaults."""
    # https://github.com/cgohlke/tifffile/issues/145

    with TempFileName('issue_resolutionunit_none') as filename:
        imwrite(filename, [[0]], resolution=None, resolutionunit=None)
        with TiffFile(filename) as tif:
            page = tif.pages.first
            assert tif.pages.first.tags['ResolutionUnit'].value == RESUNIT.NONE
            assert page.resolutionunit == RESUNIT.NONE
            assert page.resolution == (1, 1)

    with TempFileName('issue_resolutionunit_inch') as filename:
        imwrite(filename, [[0]], resolution=(1, 1), resolutionunit=None)
        with TiffFile(filename) as tif:
            page = tif.pages.first
            assert tif.pages.first.tags['ResolutionUnit'].value == RESUNIT.INCH
            assert page.resolutionunit == RESUNIT.INCH
            assert page.resolution == (1, 1)

    with TempFileName('issue_resolutionunit_imagej') as filename:
        imwrite(
            filename,
            [[0]],
            dtype=numpy.float32,
            imagej=True,
            resolution=(1, 1),
        )
        with TiffFile(filename) as tif:
            page = tif.pages.first
            assert tif.pages.first.tags['ResolutionUnit'].value == RESUNIT.NONE
            assert page.resolutionunit == RESUNIT.NONE
            assert page.resolution == (1, 1)


@pytest.mark.skipif(SKIP_FILE or SKIP_CODECS, reason=REASON)
def test_issue_ome_jpeg_colorspace():
    """Test colorspace of JPEG segments encoded by BioFormats."""
    # https://forum.image.sc/t/69862
    # JPEG encoded segments are stored as YCBCR but the
    # PhotometricInterpretation tag is RGB
    # CMU-1.svs exported by QuPath 0.3.2
    filename = _file('ome/CMU-1.ome.tif')
    with TiffFile(filename) as tif:
        assert tif.is_ome
        series = tif.series[0].levels[5]
        assert series.kind == 'ome'
        assert series.keyframe.is_jfif
        assert series.shape == (1028, 1437, 3)
        assert tuple(series.asarray()[800, 200]) == (207, 166, 198)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_issue_imagej_compressed():
    """Test read ImageJ hyperstack with compression."""
    # regression in tifffile 2022.7.28
    filename = _file('imagej/imagej_compressed.tif')
    with TiffFile(filename) as tif:
        assert tif.is_imagej
        assert len(tif.pages) == 120
        series = tif.series[0]
        assert series.kind == 'imagej'
        assert series.axes == 'ZCYX'
        assert series.shape == (60, 2, 256, 256)
        assert series.sizes == {
            'depth': 60,
            'channel': 2,
            'height': 256,
            'width': 256,
        }
        assert series.keyframe.compression == COMPRESSION.ADOBE_DEFLATE
        assert series.asarray()[59, 1, 55, 87] == 5643


@pytest.mark.skipif(SKIP_FILE or SKIP_CODECS, reason=REASON)
def test_issue_jpeg_rgb():
    """Test write JPEG compression in RGB mode."""
    # https://github.com/cgohlke/tifffile/issues/146
    # requires imagecodecs > 2022.7.31
    data = imread(_file('tifffile/rgb.tif'))
    assert data.shape == (32, 31, 3)
    with TempFileName('issue_jpeg_rgb') as filename:
        imwrite(
            filename,
            data,
            photometric='rgb',
            subsampling=(1, 1),
            compression='jpeg',
            compressionargs={'level': 95, 'outcolorspace': 'rgb'},
        )
        with TiffFile(filename) as tif:
            page = tif.pages.first
            assert page.shape == data.shape
            assert page.photometric == PHOTOMETRIC.RGB
            assert page.compression == COMPRESSION.JPEG
            assert not page.is_jfif
            image = page.asarray()
        assert_array_equal(image, imagecodecs.imread(filename))


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_issue_imread_out():
    """Test imread supports out argument."""
    # https://github.com/cgohlke/tifffile/issues/147
    filename = _file('tifffile/rgb.tif')
    image = imread(filename, out=None)
    assert isinstance(image, numpy.ndarray)
    data = imread(filename, out='memmap')
    assert isinstance(data, numpy.memmap)
    assert_array_equal(data, image)

    image = imread([filename, filename], out=None)
    assert isinstance(image, numpy.ndarray)
    assert_array_equal(image[1], data)

    data = imread(
        [filename, filename],
        chunkshape=(32, 31, 3),
        chunkdtype=numpy.uint8,
        out_inplace=True,
        out='memmap',
    )
    assert isinstance(data, numpy.memmap)
    assert_array_equal(data, image)


def test_issue_imagej_hyperstack_arg():
    """Test write ImageJ with hyperstack argument."""
    # https://stackoverflow.com/questions/73279086
    with TempFileName('issue_imagej_hyperstack_arg') as filename:
        data = numpy.zeros((4, 3, 10, 11), dtype=numpy.uint8)
        imwrite(
            filename,
            data,
            imagej=True,
            metadata={'hyperstack': True, 'axes': 'TZYX'},
        )
        with TiffFile(filename) as tif:
            assert tif.is_imagej
            assert 'hyperstack=true' in tif.pages.first.description
            assert tif.imagej_metadata is not None
            assert tif.imagej_metadata['hyperstack']
            assert tif.series[0].axes == 'TZYX'


def test_issue_description_overwrite():
    """Test user description is not overwritten if metadata is disabled."""
    data = numpy.zeros((5, 10, 11), dtype=numpy.uint8)
    omexml = OmeXml()
    omexml.addimage(
        dtype=data.dtype,
        shape=data.shape,
        storedshape=(5, 1, 1, 10, 11, 1),
        axes='ZYX',
    )
    description = omexml.tostring()

    with TempFileName('issue_description_overwrite') as filename:
        with tifffile.TiffWriter(filename, ome=False) as tif:
            for frame in data:
                tif.write(
                    frame,
                    description=description,
                    metadata=None,
                    contiguous=True,
                )
                description = None
        with TiffFile(filename) as tif:
            assert tif.is_ome
            assert tif.pages.first.description == omexml.tostring()
            assert tif.series[0].kind == 'ome'
            assert tif.series[0].axes == 'ZYX'
            assert_array_equal(tif.asarray(), data)


def test_issue_svs_description():
    """Test svs_description_metadata function."""
    # https://github.com/cgohlke/tifffile/issues/149
    assert svs_description_metadata(
        'Aperio Image Library vFS90 01\r\n'
        '159712x44759 [0,100 153271x44659] (256x256) JPEG/RGB Q=30'
        '|AppMag = 40'
        '|StripeWidth = 992'
        '|ScanScope ID = SS1475'
        '|Filename = 12-0893-1'
        '|Title = Mag = 40X, compression quality =30'
        '|Date = 11/20/12'
        '|Time = 01:06:12'
        '|Time Zone = GMT-05:00'
        '|User = 8ce982e3-6ea2-4715-8af3-9874e823e6d9'
        '|MPP = 0.2472'
        '|Left = 19.730396'
        '|Top = 15.537785'
        '|LineCameraSkew = 0.001417'
        '|LineAreaXOffset = 0.014212'
        '|LineAreaYOffset = -0.004733'
        '|Focus Offset = 0.000000'
        '|DSR ID = 152.19.62.167'
        '|ImageID = 311112'
        '|Exposure Time = 109'
        '|Exposure Scale = 0.000001'
        '|DisplayColor = 0'
        '|OriginalWidth = 159712'
        '|OriginalHeight = 44759'
        '|ICC Profile = ScanScope v1'
    ) == {
        'Header': (
            'Aperio Image Library vFS90 01\r\n'
            '159712x44759 [0,100 153271x44659] (256x256) JPEG/RGB Q=30'
        ),
        'AppMag': 40,
        'StripeWidth': 992,
        'ScanScope ID': 'SS1475',
        'Filename': '12-0893-1',
        'Title': 'Mag = 40X, compression quality =30',
        'Date': '11/20/12',
        'Time': '01:06:12',
        'Time Zone': 'GMT-05:00',
        'User': '8ce982e3-6ea2-4715-8af3-9874e823e6d9',
        'MPP': 0.2472,
        'Left': 19.730396,
        'Top': 15.537785,
        'LineCameraSkew': 0.001417,
        'LineAreaXOffset': 0.014212,
        'LineAreaYOffset': -0.004733,
        'Focus Offset': 0.0,
        'DSR ID': '152.19.62.167',
        'ImageID': 311112,
        'Exposure Time': 109,
        'Exposure Scale': 0.000001,
        'DisplayColor': 0,
        'OriginalWidth': 159712,
        'OriginalHeight': 44759,
        'ICC Profile': 'ScanScope v1',
    }

    assert svs_description_metadata(
        'Aperio Image Library v11.0.37\r\n60169x38406 (256x256) JPEG/RGB Q=70'
        '|Patient=CS-10-SI_HE'
        '|Accession='
        '|User='
        '|Date=10/12/2012'
        '|Time=04:55:13 PM'
        '|Copyright=Hamamatsu Photonics KK'
        '|AppMag=20'
        '|Webslide Files=5329'
    ) == {
        'Header': (
            'Aperio Image Library v11.0.37\r\n'
            '60169x38406 (256x256) JPEG/RGB Q=70'
        ),
        'Patient': 'CS-10-SI_HE',
        'Accession': '',
        'User': '',
        'Date': '10/12/2012',
        'Time': '04:55:13 PM',
        'Copyright': 'Hamamatsu Photonics KK',
        'AppMag': 20,
        'Webslide Files': 5329,
    }


def test_issue_iterator_recursion():
    """Test no RecursionError writing large number of tiled pages."""
    with TempFileName('issue_iterator_recursion') as filename:
        imwrite(
            filename, shape=(1024, 54, 64), dtype=numpy.uint8, tile=(32, 32)
        )
        assert os.path.exists(filename)


@pytest.mark.skipif(SKIP_CODECS, reason=REASON)
def test_issue_predictor_floatx2():
    """Test floatx2 predictor."""
    # https://github.com/cgohlke/tifffile/issues/167
    data = random_data(numpy.float32, (219, 302))
    with TempFileName('issue_predictor_floatx2') as filename:
        imwrite(filename, data, predictor=34894, compression=True)
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 1
            assert len(tif.series) == 1
            page = tif.pages.first
            assert page.photometric == PHOTOMETRIC.MINISBLACK
            assert page.imagewidth == 302
            assert page.imagelength == 219
            assert page.samplesperpixel == 1
            assert page.predictor == 34894
            assert page.compression == 8
            image = page.asarray()
            assert_array_equal(data, image)
            assert__str__(tif)


@pytest.mark.skipif(SKIP_CODECS, reason=REASON)
def test_issue_predictor_deltax2():
    """Test deltax2 predictor."""
    # https://github.com/cgohlke/tifffile/issues/167
    data = random_data(numpy.uint8, (219, 302))
    with TempFileName('issue_predictor_deltax2') as filename:  # noqa: SIM117
        with pytest.raises(NotImplementedError):
            imwrite(filename, data, predictor=34892, compression=8)
        # with TiffFile(filename) as tif:
        #     assert len(tif.pages) == 1
        #     assert len(tif.series) == 1
        #     page = tif.pages.first
        #     assert page.photometric == MINISBLACK
        #     assert page.imagewidth == 302
        #     assert page.imagelength == 219
        #     assert page.samplesperpixel == 1
        #     assert page.predictor == 34892
        #     assert page.compression == 8
        #     image = page.asarray()
        #     assert_array_equal(data, image)
        #     assert__str__(tif)


@pytest.mark.skipif(SKIP_CODECS, reason=REASON)
@pytest.mark.parametrize('compression', ['none', 'packbits', 'zlib', 'lzw'])
@pytest.mark.parametrize('predictor', ['none', 'horizontal'])
@pytest.mark.parametrize('samples', [0, 1, 3])
def test_issue_tile_generator(compression, predictor, samples):
    """Test predictor and compression axes with tile generator."""
    # https://github.com/cgohlke/tifffile/issues/185

    if compression == 'none' and predictor != 'none':
        pytest.xfail('cannot use predictor without compression')

    data = numpy.empty(
        (27, 23, samples) if samples else (27, 23, 1), numpy.uint8
    )
    data[:] = 199
    data[7:9, 11:13] = 13
    data[22:25, 19:22] = 11

    def tiles():
        yield data[:16, :16]
        yield data[:16, 16:]
        yield data[16:, :16]
        yield data[16:, 16:]

    with TempFileName(
        f'issue_tile_generator_{compression}_{predictor}_{samples}'
    ) as filename:
        imwrite(
            filename,
            data=tiles(),
            shape=(27, 23, samples) if samples > 1 else (27, 23),
            dtype=numpy.uint8,
            tile=(16, 16),
            compression=compression,
            predictor=predictor,
        )
        assert_array_equal(imread(filename), data.squeeze())
        if (
            SKIP_CODECS
            or not imagecodecs.TIFF.available
            or (compression == 'packbits' and predictor == 'horizontal')
        ):
            return
        assert_array_equal(imagecodecs.imread(filename), data.squeeze())


def test_issue_extratags_filter(caplog):
    """Test filtering extratags."""
    # https://github.com/cgohlke/tifffile/pull/188
    with TempFileName('issue_extratags_filter') as filename:
        imwrite(
            filename,
            shape=(10, 10),
            dtype='u1',
            extratags=[
                (322, 3, 1, 2, False),  # TileWidth is filtered
                (34665, 13, 1, 0, False),  # ExifIFD is filtered
                (270, 2, 0, 'second description', True),  # filtered
                ('FillOrder', 3, 1, 1, True),  # by name should go through
            ],
        )
        assert 'extratag 322' in caplog.text
        assert 'extratag 34665' in caplog.text
        with TiffFile(filename) as tif:
            tags = tif.pages.first.tags
            assert 322 not in tags
            assert 34665 not in tags
            assert tags.get(270, index=1) is None
            assert tags[266].value == 1


@pytest.mark.skipif(
    SKIP_FILE or SKIP_CODECS or not imagecodecs.JPEG.available,
    reason=REASON,
)
def test_issue_invalid_predictor(caplog):
    """Test decoding JPEG compression with invalid predictor tag."""
    filename = _file('issues/invalid_predictor.tiff')
    with TiffFile(filename) as tif:
        page = tif.pages.first
        assert page.predictor == 58240
        assert page.compression == 7
        data = page.asarray()
        assert 'ignoring predictor' in caplog.text
        assert data.shape == (1275, 1650, 4)


@pytest.mark.skipif(SKIP_FILE or SKIP_CODECS, reason=REASON)
def test_issue_ome_missing_frames():
    """Test read OME TIFF with missing pages at end."""
    # https://github.com/cgohlke/tifffile/issues/199
    filename = _file('issues/stack_t24_y2048_x2448.tiff')
    with TiffFile(filename) as tif:
        assert tif.is_ome
        assert tif.byteorder == '<'
        assert len(tif.pages) == 16
        assert len(tif.series) == 1
        # assert page properties
        page = tif.pages.first
        assert page.photometric == PHOTOMETRIC.MINISBLACK
        assert page.imagewidth == 2448
        assert page.imagelength == 2048
        assert page.bitspersample == 8
        assert not page.is_contiguous
        # assert series properties
        series = tif.series[0]
        assert len(series._pages) == 24
        assert len(series.pages) == 24
        assert series[16] is None
        assert series[23] is None
        assert series.shape == (24, 2048, 2448)
        assert series.dtype == numpy.uint8
        assert series.axes == 'TYX'
        assert series.kind == 'ome'
        data = series.asarray()
        assert_aszarr_method(tif, data)
        assert_aszarr_method(tif, data, chunkmode='page')
        assert__str__(tif)


@pytest.mark.skipif(not IS_WIN, reason=REASON)
def test_issue_maxworkers():
    """Test maxworkers defaults."""
    if 'TIFFFILE_NUM_THREADS' in os.environ:
        assert int(os.environ['TIFFFILE_NUM_THREADS']) == TIFF.MAXWORKERS
    else:
        assert max(1, os.cpu_count() // 2) == TIFF.MAXWORKERS

    if 'TIFFFILE_NUM_IOTHREADS' in os.environ:
        assert int(os.environ['TIFFFILE_NUM_IOTHREADS']) == TIFF.MAXIOWORKERS
    else:
        assert os.cpu_count() + 4 == TIFF.MAXIOWORKERS


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_issue_logging_filter(caplog):
    """Test raise an error by filtering logging messages."""
    # https://github.com/cgohlke/tifffile/issues/216
    import logging

    def log_filter(record):
        if record.levelno == logging.ERROR:
            assert record.funcName == '__init__'
            assert 'invalid value offset' in record.msg
            raise ValueError(record.msg)
        return True

    filename = _file('bad/Gel1.tif')

    logger().addFilter(log_filter)
    try:
        with pytest.raises(ValueError):
            imread(filename)
    finally:
        logger().removeFilter(log_filter)

    assert 'invalid value offset' not in caplog.text
    imread(_file(filename))
    assert 'invalid value offset' in caplog.text


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_issue_wrong_shape(caplog):
    """Test rewritten file with wrong shape in metadata."""
    # https://github.com/Kitware/UPennContrast/issues/491
    filename = _file('issues/2023_05_23_pw020_ctrl_well1.tif')
    with TiffFile(filename) as tif:
        assert tif.is_shaped
        page = tif.pages.first
        assert '"shape": [1, 1, 4, 1000, 1000]' in page.description
        assert '"axes": "ZTCYX"' in page.description
        assert page.shape == (1000, 1000, 4)
        series = tif.series[0]
        assert 'shaped series metadata does not match page' in caplog.text
        assert series.shape == (1000, 1000, 4)  # != (1, 1, 4, 1000, 1000)
        assert series.axes == 'YXS'  # != 'ZTCYX'
        assert_array_equal(page.asarray(), series.asarray())


def test_issue_exclusive_creation():
    """Test TiffWriter with exclusive creation mode 'x'."""
    # https://github.com/cgohlke/tifffile/pull/223
    with TempFileName('issue_exclusive_creation') as filename:
        if os.path.exists(filename):
            os.remove(filename)
        with FileHandle(filename, mode='x') as fh:
            assert fh._mode == 'xb'
            with TiffWriter(fh) as tif:
                tif.write(shape=(32, 32), dtype=numpy.uint8)
        with pytest.raises(FileExistsError):  # noqa: SIM117
            with FileHandle(filename, mode='x'):
                pass


def test_issue_non_volumetric():
    """Test writing non-volume data with volumetric tile."""
    # https://github.com/cgohlke/tifffile/pull/225
    data = random_data(numpy.uint8, (3, 13, 17))
    with TempFileName('issue_non_volumetric') as filename:
        with pytest.raises(ValueError):
            imwrite(filename, data[0], tile=(3, 16, 16))
        with pytest.raises(ValueError):
            imwrite(filename, data, photometric='rgb', tile=(3, 16, 16))

        # test some other invalid tiles
        with pytest.raises(ValueError):
            imwrite(
                filename, data, tile=(3, 3, 16, 16), photometric='minisblack'
            )
        with pytest.raises(ValueError):
            imwrite(filename, data, tile=(16,), photometric='minisblack')
        with pytest.raises(ValueError):
            imwrite(filename, data, photometric='rgb', tile=(15, 16))
        with pytest.raises(ValueError):
            imwrite(filename, data, photometric='rgb', tile=(0, 16))

        # test some valid cases
        imwrite(filename, data, tile=(3, 16, 16), photometric='minisblack')
        with TiffFile(filename) as tif:
            page = tif.pages.first
            assert page.is_tiled
            assert page.is_volumetric
            assert page.shape == data.shape

        imwrite(filename, data, photometric='rgb', tile=(1, 16, 16))
        with TiffFile(filename) as tif:
            page = tif.pages.first
            assert page.is_tiled
            assert not page.is_volumetric
            assert page.shape == data.shape

        data = random_data(numpy.uint8, (3, 5, 13, 17))
        imwrite(
            filename,
            data,
            tile=(5, 16, 16),
            photometric='rgb',
            planarconfig='separate',
            metadata=None,
        )
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 1
            page = tif.pages.first
            assert page.is_tiled
            assert page.is_volumetric
            assert page.photometric == PHOTOMETRIC.RGB
            assert page.planarconfig == PLANARCONFIG.SEPARATE
            assert page.shape == data.shape

        data = random_data(numpy.uint8, (5, 13, 17, 3))
        imwrite(
            filename,
            data,
            tile=(5, 16, 16),
            photometric='rgb',
            planarconfig='contig',
            metadata=None,
        )
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 1
            page = tif.pages.first
            assert page.is_tiled
            assert page.is_volumetric
            assert page.photometric == PHOTOMETRIC.RGB
            assert page.planarconfig == PLANARCONFIG.CONTIG
            assert page.shape == data.shape

        data = random_data(numpy.uint8, (5, 13, 17))
        imwrite(
            filename,
            data,
            tile=(16, 16),  # -> (1, 16, 16)
            photometric='miniswhite',
            volumetric=True,
            metadata=None,
        )
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 1
            page = tif.pages.first
            assert page.is_tiled
            assert page.is_volumetric
            assert page.photometric == PHOTOMETRIC.MINISWHITE
            assert page.shape == data.shape


@pytest.mark.skipif(SKIP_ZARR, reason=REASON)
def test_issue_truncated_tileoffsets(caplog):
    """Test reading truncated tile offsets and bytecounts."""
    # https://github.com/cgohlke/tifffile/issues/227
    data = random_data(numpy.uint8, (131, 128))
    with TempFileName('issue_truncated_tileoffsets') as filename:
        imwrite(filename, data, tile=(64, 64))
        with TiffFile(filename, mode='r+') as tif:
            # truncate TileOffsets and TileByteCounts
            tag = tif.pages[0].tags[324]
            assert tag.count == 6
            tag.overwrite(tag.value[:4])
            tag = tif.pages[0].tags[325]
            tag.overwrite(tag.value[:4])

        image = imread(filename)
        assert 'expected 6 segments, got 4' in caplog.text
        assert_raises(AssertionError, assert_array_equal, image, data)
        assert_array_equal(image[:128], data[:128])

        store = imread(filename, aszarr=True)
        za = zarr.open(store, mode='r')[:]
        store.close()
        assert_array_equal(za, image)


@pytest.mark.parametrize('buffersize', [0, 1, 1234])
@pytest.mark.parametrize('tile', [0, 1])
def test_issue_buffersize(buffersize, tile):
    """Test reading and writing with custom buffersize."""
    data = random_data(numpy.uint8, (5, 131, 133))
    with TempFileName(f'issue_buffersize_{buffersize}_{tile}') as filename:
        kwargs = {'tile': (32, 32)} if tile else {'rowsperstrip': 8}
        imwrite(
            filename,
            data,
            maxworkers=2,
            buffersize=buffersize,
            compression='zlib',
            **kwargs,
        )
        im = imread(filename, maxworkers=2, buffersize=buffersize)
    assert_array_equal(im, data)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_issue_ideas(caplog):
    """Test reading invalid TIFF produced by IDEAS."""
    # https://forum.image.sc/t/96103
    # NewSubfileType tag is of type bytes
    # FillOrder tag is zero
    filename = _file('IDEAS/IDEAS_file.ome.tif')
    with TiffFile(filename) as tif:
        assert '0 is not a valid FILLORDER' in caplog.text
        assert 'invalid self.subfiletype=b' in caplog.text
        assert tif.is_ome
        page = tif.pages.first
        assert page.tags['NewSubfileType'].value == b'\x02\x00'
        assert page.tags['FillOrder'].value == 0
        series = tif.series[0]
        assert series.shape == (37, 29)
        assert series.axes == 'YX'
        assert_array_equal(page.asarray(), series.asarray())


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_issue_nodata_invalid(caplog):
    """Test read GeoTIFF with invalid nodata."""
    filename = _file('GeoTIFF/nodata_corrupted.tiff')
    with TiffFile(filename) as tif:
        assert 'GDAL_NODATA tag raised ValueError' in caplog.text
        assert tif.byteorder == '<'
        assert len(tif.pages) == 1
        assert len(tif.series) == 1
        assert tif.is_geotiff
        assert tif.is_gdal
        # assert page properties
        page = tif.pages.first
        assert page.dtype == numpy.uint8
        assert page.nodata == 0
        assert page.tags['GDAL_NODATA'].value == '-99999999'
        # assert series properties
        series = tif.series[0]
        assert series.shape == (920, 1300)
        assert series.dtype == numpy.uint8
        assert series.axes == 'YX'
        assert series.kind == 'uniform'
        # assert data
        data = tif.asarray()
        assert isinstance(data, numpy.ndarray)
        assert data[500, 600] == 85
        assert data[0, 0] == 101
        assert_aszarr_method(tif, data)
        del data
        assert__str__(tif)


@pytest.mark.skipif(SKIP_CODECS or SKIP_FILE, reason=REASON)
def test_issue_tag_readfunc(caplog):
    """Test allow tag reader functions to fail."""
    # file has an OlympusSIS tag, which is pointing to invalid structure
    filename = _file('SIS/cirb_NOT_SIS.tif')
    with TiffFile(filename) as tif:
        # assert 'invalid OlympusSIS structure' in caplog.text
        assert tif.pages.first.tags[33560].value == (1466916540,)
        assert 'invalid OlympusSIS structure' in caplog.text
        assert tif.series[0].kind == 'generic'
        data = tif.asarray()
        assert data.shape == (800, 800, 4)
        assert data[166, 290, 2] == 255


@pytest.mark.skipif(
    SKIP_CODECS
    or not imagecodecs.JPEG8.available
    or not imagecodecs.JPEG2K.available
    or not imagecodecs.JPEGXL.available
    or imagecodecs.JPEG8.legacy,
    reason=REASON,
)
def test_issue_jpeg_bitspersample():
    """Test write JPEG with many bitpersample."""
    # https://github.com/cgohlke/tifffile/pull/265

    data6 = random_data(numpy.uint8, (131, 128, 3)) >> 2
    data12 = random_data(numpy.uint16, (131, 128, 3)) >> 4
    jpeg12 = imagecodecs.jpeg_encode(data12, bitspersample=12, lossless=True)

    with TempFileName('issue_jpeg_bitspersample') as filename:
        with TiffWriter(filename) as tif:
            tif.write(
                data6,
                photometric='rgb',
                compression='jpeg',
                compressionargs={'lossless': True},
                bitspersample=6,
            )
            tif.write(
                data12,
                photometric='rgb',
                compression='jpeg',
                compressionargs={'lossless': True, 'bitspersample': 12},
                # bitspersample=12,
            )
            tif.write(
                iter((jpeg12,)),
                shape=data12.shape,
                dtype=data12.dtype,
                photometric='rgb',
                compression='jpeg',
                bitspersample=14,
            )
            tif.write(
                data12,
                photometric='rgb',
                compression='jpeg2000',
                compressionargs={'bitspersample': 12},
            )
            tif.write(
                data12,
                photometric='rgb',
                compression='jpegxl',
                compressionargs={'bitspersample': 12},
            )
            with pytest.raises(ValueError):
                tif.write(
                    data12,
                    photometric='rgb',
                    compression='jpeg',
                    bitspersample=17,
                )
            with pytest.raises(ValueError):
                tif.write(
                    data12,
                    photometric='rgb',
                    compression='jpeg2000',
                    compressionargs={'bitspersample': 0},
                )

        with TiffFile(filename) as tif:
            assert len(tif.pages) == 5
            assert tif.pages[0].bitspersample == 6
            assert tif.pages[1].bitspersample == 12
            assert tif.pages[2].bitspersample == 14
            assert tif.pages[3].bitspersample == 12
            assert tif.pages[4].bitspersample == 12
            assert_array_equal(tif.series[0].asarray(), data6)
            assert_array_equal(tif.series[1].asarray(), data12)
            assert_array_equal(tif.series[2].asarray(), data12)
            assert_array_equal(tif.series[3].asarray(), data12)
            assert_array_equal(tif.series[4].asarray(), data12)

            assert__str__(tif)


def test_issue_ometiff_modulo():
    """Test writing OME-TIFF with modulo dimension."""
    # required for PhasorPy
    data = random_data(numpy.uint8, (3, 3, 31, 33))
    with TempFileName('issue_ometiff_modulo') as filename:
        with TiffWriter(filename, ome=True) as tif:
            tif.write(
                data, photometric='minisblack', metadata={'axes': 'HTYX'}
            )
            tif.write(
                data, photometric='minisblack', metadata={'axes': 'QZYX'}
            )
        with TiffFile(filename) as tif:
            series = tif.series[0]
            assert series.axes == 'HTYX'
            assert series.shape == (3, 3, 31, 33)
            series = tif.series[1]
            assert series.axes == 'QZYX'
            assert series.shape == (3, 3, 31, 33)


def test_issue_ometiff_pixel():
    """Test writing OME-TIFF three one pixel images."""
    # required for PhasorPy
    data = random_data(numpy.uint8, (3, 1, 1))
    with TempFileName('issue_ometiff_pixel') as filename:
        with TiffWriter(filename, ome=True) as tif:
            tif.write(data, photometric='minisblack', metadata={'axes': 'TYX'})
        with TiffFile(filename) as tif:
            series = tif.series[0]
            assert series.axes == 'TYX'
            assert series.shape == (3, 1, 1)


@pytest.mark.parametrize('arg', [None, 'none', 'None', 'NONE', 1])
def test_issue_none_str(arg):
    """Test predictor and compression none arguments."""
    # https://github.com/cgohlke/tifffile/pull/274

    with TempFileName(f'issue_none_str_{arg}') as filename:
        imwrite(
            filename,
            shape=(27, 23),
            dtype=numpy.uint8,
            compression=arg,
            predictor=arg,
        )
        assert imread(filename).shape == (27, 23)


@pytest.mark.skipif(SKIP_ZARR, reason=REASON)
@pytest.mark.parametrize('shape', [(1, 1), (2, 1), (1, 2)])
def test_issue_zarr_shape_one(shape):
    """Test read images with dimension size one with zarr."""
    # https://github.com/cgohlke/tifffile/issues/303
    with TempFileName(f'issue_zarr_shape_one_{shape}') as filename:
        data = numpy.ones(shape, dtype=numpy.uint8)
        imwrite(filename, data, tile=(16, 16), metadata=None)
        assert_array_equal(imread(filename), data)
        with TiffFile(filename) as tif:
            store = tif.aszarr()
            zarray = zarr.open(store=store, mode='r')
            zdata = zarray[:]
            assert_array_equal(zdata, data)


def test_issue_not_binary():
    """Test read from non-binary open file."""
    # https://github.com/cgohlke/tifffile/issues/304
    with TempFileName('issue_not_binary') as filename:
        imwrite(filename, None, shape=(2, 2), dtype=numpy.uint8)
        with open(filename) as fh:  # noqa: SIM117
            with pytest.raises(TypeError, match='binary mode'):
                imread(fh)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_issue_dcp():
    """Test read DNG DCP file."""
    # https://github.com/cgohlke/tifffile/issues/306
    filename = _file('DCP/example.dcp')
    with TiffFile(filename) as tif:
        assert len(tif.pages) == 1
        page = tif.pages.first
        assert page.tags['UniqueCameraModel'].value == 'Basler acA5472-17uc'
        assert page.dtype == bool
        assert page.shape == (0, 0)
        assert tif.asarray().shape == (0, 0)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_issue_shaped_pyramid():
    """Test read shaped pyramid TIFF file."""
    # https://github.com/cgohlke/tifffile/issues/286
    # shaped description is wrong for pages > 1
    # use is_shaped=False to ignore incorrect metadata
    filename = _file('hest/INT1.tif')

    with TiffFile(filename) as tif:
        assert len(tif.pages) == 8
        assert tif.is_shaped
        for page in tif.pages:
            assert page.is_shaped
            assert page.description == '{"shape": [19968, 19200, 3]}'
        with pytest.raises(RuntimeError, match='incompatible keyframe'):
            tif.series[0]

    with TiffFile(filename, is_shaped=False) as tif:
        assert not tif.is_shaped
        assert len(tif.pages) == 8
        assert len(tif.series) == 1
        series = tif.series[0]
        assert series.shape == (19968, 19200, 3)
        assert series.axes == 'YXS'
        assert series.kind == 'generic'
        assert series.is_pyramidal
        assert len(series.levels) == 8


class TestExceptions:
    """Test various Exceptions and Warnings."""

    data = random_data(numpy.uint16, (5, 13, 17))

    @pytest.fixture(scope='class')
    def filename(self):
        with TempFileName('exceptions') as filename:
            yield filename

    def test_nofiles(self):
        # no files found
        with pytest.raises(ValueError):
            imread('*.exceptions')

    def test_memmap(self, filename):
        # not memory-mappable
        imwrite(filename, self.data, compression=8)
        with pytest.raises(ValueError):
            memmap(filename)
        with pytest.raises(ValueError):
            memmap(filename, page=0)

    def test_dimensions(self, filename):
        # dimensions too large
        with pytest.raises(ValueError):
            imwrite(filename, shape=(4294967296, 32), dtype=numpy.uint8)

    def test_no_shape_dtype_empty(self, filename):
        # shape and dtype missing for empty array
        with pytest.raises(ValueError):
            imwrite(filename)

    def test_no_shape_dtype_iter(self, filename):
        # shape and dtype missing for iterator
        with pytest.raises(ValueError):
            imwrite(filename, iter(self.data))

    def test_no_shape_dtype(self, filename):
        # shape and dtype missing
        with TiffWriter(filename) as tif:  # noqa: SIM117
            with pytest.raises(ValueError):
                tif.write()

    def test_no_shape(self, filename):
        # shape missing
        with TiffWriter(filename) as tif:  # noqa: SIM117
            with pytest.raises(ValueError):
                tif.write(iter(self.data), dtype='u2')

    def test_no_dtype(self, filename):
        # dtype missing
        with TiffWriter(filename) as tif:  # noqa: SIM117
            with pytest.raises(ValueError):
                tif.write(iter(self.data), shape=(5, 13, 17))

    def test_mismatch_dtype(self, filename):
        # dtype wrong
        with pytest.raises(ValueError):
            imwrite(filename, self.data, dtype='f4')

    def test_mismatch_shape(self, filename):
        # shape wrong
        with pytest.raises(ValueError):
            imwrite(filename, self.data, shape=(2, 13, 17))

    def test_byteorder(self, filename):
        # invalid byteorder
        with pytest.raises(ValueError):
            imwrite(filename, self.data, byteorder='?')

    def test_truncate_compression(self, filename):
        # truncate cannot be used with compression, packints, or tiles
        with pytest.raises(ValueError):
            imwrite(filename, self.data, compression=8, truncate=True)

    def test_truncate_ome(self, filename):
        # truncate cannot be used with ome-tiff
        with pytest.raises(ValueError):
            imwrite(filename, self.data, ome=True, truncate=True)

    def test_truncate_noshape(self, filename):
        # truncate cannot be used with shaped=False
        with pytest.raises(ValueError):
            imwrite(filename, self.data, shaped=False, truncate=True)

    def test_compression(self, filename):
        # invalid compression
        with pytest.raises((TypeError, ValueError)):
            imwrite(filename, self.data, compression=(8, None, None, None))

    def test_predictor_dtype(self, filename):
        # cannot apply predictor to dtype complex
        with pytest.raises(ValueError):
            imwrite(
                filename, self.data.astype('F'), predictor=True, compression=8
            )

    def test_predictor_float(self, filename):
        # cannot apply horizontal predictor to float
        with pytest.raises(ValueError):
            imwrite(
                filename, self.data.astype('f'), predictor=2, compression=8
            )

    def test_predictor_uint(self, filename):
        # cannot apply float predictor to uint
        with pytest.raises(ValueError):
            imwrite(
                filename, self.data.astype('H'), predictor=3, compression=8
            )

    def test_predictor_wrong_compression(self, filename):
        # cannot apply predictor to with image compression schemes
        with pytest.raises(ValueError):
            imwrite(
                filename, self.data.astype('B'), predictor=2, compression=7
            )

    def test_predictor_no_compression(self, filename):
        # cannot apply predictor to without compression
        with pytest.raises(ValueError):
            imwrite(
                filename, self.data.astype('B'), predictor=2, compression=1
            )

    def test_ome_imagedepth(self, filename):
        # OME-TIFF does not support ImageDepth
        with pytest.raises(ValueError):
            imwrite(filename, self.data, ome=True, volumetric=True)

    def test_imagej_dtype(self, filename):
        # ImageJ does not support dtype
        with pytest.raises(ValueError):
            imwrite(filename, self.data.astype('f8'), imagej=True)

    def test_imagej_imagedepth(self, filename):
        # ImageJ does not support ImageDepth
        with pytest.raises(ValueError):
            imwrite(filename, self.data, imagej=True, volumetric=True)

    def test_imagej_float_rgb(self, filename):
        # ImageJ does not support float with rgb
        with pytest.raises(ValueError):
            imwrite(
                filename,
                self.data[..., :3].astype('f4'),
                imagej=True,
                photometric='rgb',
            )

    def test_imagej_planar(self, filename):
        # ImageJ does not support planar
        with pytest.raises(ValueError):
            imwrite(filename, self.data, imagej=True, planarconfig='separate')

    def test_colormap_shape(self, filename):
        # invalid colormap shape
        with pytest.raises(ValueError):
            imwrite(
                filename,
                self.data.astype('u1'),
                photometric='palette',
                colormap=numpy.empty((3, 254), 'u2'),
            )

    def test_colormap_shape2(self, filename):
        # invalid colormap shape
        with pytest.raises(ValueError):
            imwrite(
                filename,
                self.data.astype('u1'),
                colormap=numpy.empty((3, 254), 'u2'),
            )

    def test_colormap_shape3(self, filename):
        # invalid dtype for colormap
        with pytest.warns(UserWarning):
            imwrite(
                filename,
                self.data.astype('f2'),
                colormap=numpy.empty((3, 256), 'u2'),
            )
        with TiffFile(filename) as tif:
            assert tif.pages.first.colormap is None

    def test_colormap_dtype(self, filename):
        # invalid colormap dtype
        with pytest.raises(ValueError):
            imwrite(
                filename,
                self.data.astype('u1'),
                photometric='palette',
                colormap=numpy.empty((3, 255), 'i2'),
            )

    def test_palette_dtype(self, filename):
        # invalid dtype for palette mode
        with pytest.raises(ValueError):
            imwrite(
                filename,
                self.data.astype('u4'),
                photometric='palette',
                colormap=numpy.empty((3, 255), 'u2'),
            )

    def test_cfa_shape(self, filename):
        # invalid shape for CFA
        with pytest.raises(ValueError):
            imwrite(filename, self.data, photometric='cfa')

    def test_subfiletype_mask(self, filename):
        # invalid SubfileType MASK
        with pytest.raises(ValueError):
            imwrite(filename, self.data, subfiletype=0b100)

    def test_bitspersample_bilevel(self, filename):
        # invalid bitspersample for bilevel
        with pytest.raises(ValueError):
            imwrite(filename, self.data.astype('?'), bitspersample=2)

    def test_bitspersample_jpeg(self, filename):
        # invalid bitspersample for jpeg
        with pytest.raises(ValueError):
            imwrite(filename, self.data, compression='jpeg', bitspersample=17)

    def test_datetime(self, filename):
        # invalid datetime
        with pytest.raises(ValueError):
            imwrite(filename, self.data, datetime='date')

    def test_rgb(self, filename):
        # not a RGB image
        with pytest.raises(ValueError):
            imwrite(filename, self.data[:2], photometric='rgb')

    def test_extrasamples(self, filename):
        # invalid extrasamples
        with pytest.raises(ValueError):
            imwrite(
                filename, self.data, photometric='rgb', extrasamples=(0, 1, 2)
            )

    def test_subsampling(self, filename):
        # invalid subsampling
        with pytest.raises(ValueError):
            imwrite(
                filename,
                self.data[..., :3],
                photometric='rgb',
                compression=7,
                subsampling=(3, 3),
            )

    def test_compress_bilevel(self, filename):
        # cannot compress bilevel image
        with pytest.raises(NotImplementedError):
            imwrite(filename, self.data.astype('?'), compression='jpeg')

    def test_description_unicode(self, filename):
        # strings must be 7-bit ASCII
        with pytest.raises(ValueError):
            imwrite(filename, self.data, description='mu: \u03bc')

    def test_compression_contiguous(self, filename):
        # contiguous cannot be used with compression, tiles
        with TiffWriter(filename) as tif:
            tif.write(self.data[0])
            with pytest.raises(ValueError):
                tif.write(self.data[1], contiguous=True, compression=8)

    def test_imagej_contiguous(self, filename):
        # ImageJ format does not support non-contiguous series
        with TiffWriter(filename, imagej=True) as tif:
            tif.write(self.data[0])
            with pytest.raises(ValueError):
                tif.write(self.data[1], contiguous=False)

    def test_subifds_subifds(self, filename):
        # SubIFDs in SubIFDs are not supported
        with TiffWriter(filename) as tif:
            tif.write(self.data[0], subifds=1)
            with pytest.raises(ValueError):
                tif.write(self.data[1], subifds=1)

    def test_subifds_truncate(self, filename):
        # SubIFDs cannot be used with truncate
        with TiffWriter(filename) as tif:
            tif.write(self.data, subifds=1, truncate=True)
            with pytest.raises(ValueError):
                tif.write(self.data[:, ::2, ::2])

    def test_subifds_imwrite(self, filename):
        # imwrite cannot be used to write SubIFDs
        with pytest.raises(TypeError):
            imwrite(filename, self.data, subifds=1)

    def test_iter_bytes(self, filename):
        # iterator contains wrong number of bytes
        with TiffWriter(filename) as tif:  # noqa: SIM117
            with pytest.raises(ValueError):
                tif.write(
                    iter([b'abc']),
                    shape=(13, 17),
                    dtype=numpy.uint8,
                    rowsperstrip=13,
                )

    def test_iter_dtype(self, filename):
        # iterator contains wrong dtype
        with TiffWriter(filename) as tif:  # noqa: SIM117
            with pytest.raises(ValueError):
                tif.write(
                    iter(self.data),
                    shape=(5, 13, 17),
                    dtype=numpy.uint8,
                    rowsperstrip=13,
                )

        with TiffWriter(filename) as tif:  # noqa: SIM117
            with pytest.raises(ValueError):
                tif.write(
                    iter(self.data),
                    shape=(5, 13, 17),
                    dtype=numpy.uint8,
                    rowsperstrip=5,
                    compression=8,
                )

    def test_tiff_enums(self):
        # TIFF.COMPRESSION and others are deprecated
        with pytest.raises(AttributeError):
            _ = TIFF.COMPRESSION
        with pytest.raises(AttributeError):
            _ = TIFF.EXTRASAMPLE
        with pytest.raises(AttributeError):
            _ = TIFF.FILLORDER
        with pytest.raises(AttributeError):
            _ = TIFF.PHOTOMETRIC
        with pytest.raises(AttributeError):
            _ = TIFF.PLANARCONFIG
        with pytest.raises(AttributeError):
            _ = TIFF.PREDICTOR
        with pytest.raises(AttributeError):
            _ = TIFF.RESUNIT
        with pytest.raises(AttributeError):
            _ = TIFF.RESUNIT

    # def test_extratags(self, filename):
    #     # invalid dtype or count
    #     with pytest.raises(ValueError):
    #         imwrite(filename, data, extratags=[()])

    def test_container_binaryio(self, filename):
        # container cannot be binaryio
        with pytest.raises(ValueError), FileHandle(filename, 'r+') as fh:
            imread(fh, container=filename)

    def test_imagej_colormap(self, filename):
        # invalid colormap shape
        with pytest.raises(ValueError):
            imwrite(
                filename,
                shape=(31, 33),
                dtype=numpy.float32,
                imagej=True,
                colormap=[1, 2, 3],
            )

    def test_imagej_colormap_u2(self, filename):
        # ImageJ does not support TIFF colormap shape
        with pytest.raises(ValueError):
            imwrite(
                filename,
                self.data.astype('u2'),
                imagej=True,
                colormap=numpy.empty((3, 2**16), 'u2'),
            )

    def test_imagej_colormap_rgb(self, filename):
        # do not write colormap with RGB
        data = self.data.astype('u1')[:3]
        data = numpy.moveaxis(data, 0, -1)
        with pytest.warns(UserWarning):
            imwrite(
                filename,
                data,
                photometric='rgb',
                imagej=True,
                colormap=numpy.empty((3, 256), 'u2'),
            )
        with TiffFile(filename) as tif:
            assert tif.pages.first.colormap is None

    def test_photometric(self, filename):
        # not a RGB image
        with pytest.raises(ValueError):
            imwrite(
                filename,
                shape=(31, 33, 2),
                dtype=numpy.uint8,
                photometric='rgb',
            )

    def test_tile(self, filename):
        # invalid tile in iterator
        def tile():
            yield numpy.empty((32, 32), dtype=numpy.uint16)

        with pytest.raises(ValueError):
            imwrite(
                filename,
                tile(),
                shape=(31, 33),
                dtype=numpy.uint16,
                tile=(16, 16),
            )

        with pytest.raises(ValueError):
            imwrite(
                filename,
                tile(),
                shape=(31, 33),
                dtype=numpy.uint8,
                tile=(32, 32),
            )

    def test_description(self, filename):
        # invalid description type
        with pytest.raises(ValueError):
            imwrite(
                filename,
                shape=(31, 33),
                dtype=numpy.uint8,
                description=32,
                metadata=None,
            )

    def test_omexml(self, filename):
        # invalid OME-XML
        imwrite(filename, shape=(31, 33), dtype=numpy.uint8)
        with pytest.raises(ValueError):
            imread(filename, omexml='OME')

    def test_key(self, filename):
        # invalid key
        imwrite(filename, shape=(31, 33), dtype=numpy.uint8)
        with pytest.raises(TypeError):
            imread(filename, key='1')
        with pytest.raises(TypeError):
            imread(filename, key='1', series=0)

    def test_filemode(self, filename):
        # invalid file open mode
        with pytest.raises(ValueError):
            imread(filename, mode='no')

    @pytest.mark.skipif(
        SKIP_CODECS or not imagecodecs.LERC.available, reason=REASON
    )
    def test_lerc_compression(self, filename):
        # invalid LERC compression
        with pytest.raises(ValueError):
            imwrite(
                filename,
                shape=(31, 33),
                dtype=numpy.uint8,
                compression='LERC',
                compressionargs={'compression': 'jpeg'},
            )

    @pytest.mark.skipif(SKIP_FILE, reason=REASON)
    def test_sequence_imread_dtype(self):
        # dtype argument is deprecated
        files = _files('tifffile/temp_C001T00*.tif')
        with pytest.raises(TypeError):
            imread(files, dtype=numpy.float64)

    @pytest.mark.skipif(SKIP_FILE, reason=REASON)
    def test_filesequence_dtype(self):
        # dtype argument is deprecated
        files = _files('tifffile/temp_C001T00*.tif')
        with pytest.raises(TypeError):
            TiffSequence(files).asarray(dtype=numpy.float64)
        with pytest.raises(TypeError):
            TiffSequence(files).asarray(dtype=numpy.float64)
        if not SKIP_ZARR:
            with pytest.raises(TypeError):
                TiffSequence(files).aszarr(dtype=numpy.float64)


###############################################################################

# Test specific functions and classes


def test_class_tiffformat():
    """Test TiffFormat class."""
    tiff = TIFF.NDPI_LE
    assert not tiff.is_bigtiff
    assert tiff.is_ndpi
    str(tiff)
    repr(tiff)


def test_class_filecache():
    """Test FileCache class."""
    with TempFileName('class_filecache') as filename:
        cache = FileCache(3)

        with open(filename, 'wb') as fh:
            fh.close()

        # create 6 handles, leaving only first one open
        handles = []
        for i in range(6):
            fh = FileHandle(filename)
            if i > 0:
                fh.close()
            handles.append(fh)

        # open all files
        for fh in handles:
            cache.open(fh)
        assert len(cache) == 6
        for i, fh in enumerate(handles):
            assert not fh.closed
            assert cache.files[fh] == 1 if i else 2

        # close all files: only first file and recently used files are open
        for fh in handles:
            cache.close(fh)
        assert len(cache) == 3
        for i, fh in enumerate(handles):
            assert fh.closed == (0 < i < 4)
            if not 0 < i < 4:
                assert cache.files[fh] == 0 if i else 1

        # open all files, then clear cache: only first file is open
        for fh in handles:
            cache.open(fh)
        cache.clear()
        assert len(cache) == 1
        assert handles[0] in cache.files
        for i, fh in enumerate(handles):
            assert fh.closed == (i > 0)

        # randomly open and close files
        for _i in range(13):
            fh = handles[random.randint(0, 5)]
            cache.open(fh)
            cache.close(fh)
            assert len(cache) <= 3
            assert fh in cache.files
            assert handles[0] in cache.files

        # randomly read from files
        for _i in range(13):
            fh = handles[random.randint(0, 5)]
            cache.read(fh, 0, 0)
            assert len(cache) <= 3
            assert fh in cache.files
            assert handles[0] in cache.files

        # clear cache: only first file is open
        cache.clear()
        assert len(cache) == 1
        assert handles[0] in cache.files
        for i, fh in enumerate(handles):
            assert fh.closed == (i > 0)

        # open and close all files twice
        for fh in handles:
            cache.open(fh)
            cache.open(fh)
        assert len(cache) == 6
        for i, fh in enumerate(handles):
            assert not fh.closed
            assert cache.files[fh] == 2 if i else 3
        # close files once
        for fh in handles:
            cache.close(fh)
        assert len(cache) == 6
        for i, fh in enumerate(handles):
            assert not fh.closed
            assert cache.files[fh] == 1 if i else 2
        # close files twice
        for fh in handles:
            cache.close(fh)
        assert len(cache) == 3
        for i, fh in enumerate(handles):
            assert fh.closed == (0 < i < 4)
            if not 0 < i < 4:
                assert cache.files[fh] == 0 if i else 1

        # close all files
        cache.clear()
        handles[0].close()


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_class_tifftag_astuple():
    """Test TiffTag.astuple method."""
    filename = _file('imagej/IJMetadata.tif')
    with TiffFile(filename) as tif:
        tags = tif.pages.first.tags
        assert tags['BitsPerSample'].astuple() == (
            258,
            3,
            1,
            b'\x00\x08',
            True,
        )
        assert tags['ImageDescription'].astuple() == (
            270,
            2,
            60,
            b'ImageJ=1.52b\nimages=3\nchannels=3\nmode=composite\nloop=false'
            b'\x00\x00',
            True,
        )
        assert tags['IJMetadataByteCounts'].astuple() == (
            50838,
            4,
            12,
            b'\x00\x00\x004\x00\x00\x07\xa8\x00\x00\x00\x06\x00\x00\x00\n\x00'
            b'\x00\x00\x08\x00\x00\x000\x00\x00\x03\x00\x00\x00\x03\x00\x00'
            b'\x00\x03\x00\x00\x00\x04\xa4\x00\x00\x00\xc0\x00\x00\x00\x80',
            True,
        )
        assert tags['IJMetadata'].astuple()[:3] == (50839, 1, 5896)


@pytest.mark.parametrize('bigtiff', [False, True])
@pytest.mark.parametrize('byteorder', ['<', '>'])
def test_class_tifftag_overwrite(bigtiff, byteorder):
    """Test TiffTag.overwrite method."""
    data = numpy.ones((16, 16, 3), dtype=byteorder + 'i2')
    bt = '_bigtiff' if bigtiff else ''
    bo = 'be' if byteorder == '>' else 'le'

    with TempFileName(f'class_tifftag_overwrite_{bo}{bt}') as filename:
        imwrite(
            filename,
            data,
            bigtiff=bigtiff,
            photometric=PHOTOMETRIC.RGB,
            software='in',
        )

        with TiffFile(filename, mode='r+') as tif:
            tags = tif.pages.first.tags
            # inline -> inline
            tag = tags[305]
            t305 = tag.overwrite('inl')
            assert tag.valueoffset == t305.valueoffset
            valueoffset = tag.valueoffset
            # xresolution
            tag = tags[282]
            t282 = tag.overwrite((2000, 1000))
            assert tag.valueoffset == t282.valueoffset
            # sampleformat, int -> uint
            tag = tags[339]
            t339 = tags[339].overwrite((1, 1, 1))
            assert tag.valueoffset == t339.valueoffset

        with TiffFile(filename) as tif:
            tags = tif.pages.first.tags
            tag = tags[305]
            assert tag.value == 'inl'
            assert tag.count == t305.count
            tag = tags[282]
            assert tag.value == (2000, 1000)
            assert tag.count == t282.count
            tag = tags[339]
            assert tag.value == (1, 1, 1)
            assert tag.count == t339.count

        # use bytes, specify dtype
        with TiffFile(filename, mode='r+') as tif:
            tags = tif.pages.first.tags
            # xresolution
            tag = tags[282]
            fmt = byteorder + '2I'
            t282 = tag.overwrite(struct.pack(fmt, 2500, 1500), dtype=fmt)
            assert tag.valueoffset == t282.valueoffset

        with TiffFile(filename) as tif:
            tags = tif.pages.first.tags
            tag = tags[282]
            assert tag.value == (2500, 1500)
            assert tag.count == t282.count

        # inline -> separate
        with TiffFile(filename, mode='r+') as tif:
            tag = tif.pages.first.tags[305]
            t305 = tag.overwrite('separate')
            assert tag.valueoffset != t305.valueoffset

        # separate at end -> separate longer
        with TiffFile(filename, mode='r+') as tif:
            tag = tif.pages.first.tags[305]
            assert tag.value == 'separate'
            assert tag.valueoffset == t305.valueoffset
            t305 = tag.overwrite('separate longer')
            assert tag.valueoffset == t305.valueoffset  # overwrite, not append

        # separate -> separate shorter
        with TiffFile(filename, mode='r+') as tif:
            tag = tif.pages.first.tags[305]
            assert tag.value == 'separate longer'
            assert tag.valueoffset == t305.valueoffset
            t305 = tag.overwrite('separate short')
            assert tag.valueoffset == t305.valueoffset

        # separate -> separate longer
        with TiffFile(filename, mode='r+') as tif:
            tag = tif.pages.first.tags[305]
            assert tag.value == 'separate short'
            assert tag.valueoffset == t305.valueoffset
            filesize = tif.filehandle.size
            t305 = tag.overwrite('separate longer')
            assert tag.valueoffset != t305.valueoffset
            assert t305.valueoffset == filesize  # append to end

        # separate -> inline
        with TiffFile(filename, mode='r+') as tif:
            tag = tif.pages.first.tags[305]
            assert tag.value == 'separate longer'
            assert tag.valueoffset == t305.valueoffset
            t305 = tag.overwrite('inl')
            assert tag.valueoffset != t305.valueoffset
            assert t305.valueoffset == valueoffset

        # inline - > erase
        with TiffFile(filename, mode='r+') as tif:
            tag = tif.pages.first.tags[305]
            assert tag.value == 'inl'
            assert tag.valueoffset == t305.valueoffset
            with pytest.raises(TypeError):
                t305 = tag.overwrite(tif, '')
            t305 = tag.overwrite('')
            assert tag.valueoffset == t305.valueoffset

        with TiffFile(filename) as tif:
            tag = tif.pages.first.tags[305]
            assert tag.value == ''
            assert tag.valueoffset == t305.valueoffset

        # change dtype
        with TiffFile(filename, mode='r+') as tif:
            tags = tif.pages.first.tags
            # imagewidth
            tag = tags[256]
            t256 = tag.overwrite(tag.value, dtype=3)
            assert tag.valueoffset == t256.valueoffset

        with TiffFile(filename) as tif:
            tags = tif.pages.first.tags
            tag = tags[256]
            assert tag.value == 16
            assert tag.count == t256.count

        if not bigtiff:
            assert_valid_tiff(filename)


@pytest.mark.skipif(
    SKIP_LARGE or SKIP_FILE or SKIP_CODECS or not imagecodecs.JPEG.available,
    reason=REASON,
)
def test_class_tifftag_overwrite_ndpi():
    """Test TiffTag.overwrite method on 64-bit NDPI file."""
    filename = _file('HamamatsuNDPI/103680x188160.ndpi')
    with TiffFile(filename, mode='r+') as tif:
        assert tif.is_ndpi
        tags = tif.pages.first.tags

        # inline, old value 32-bit
        assert tags['ImageWidth'].value == 188160
        tags['ImageWidth'].overwrite(0)
        tags['ImageWidth'].overwrite(188160)

        # separate, smaller or same length
        assert tags['Model'].value == 'C13220'
        tags['Model'].overwrite('C13220')

        with pytest.raises(struct.error):
            # new offset > 32-bit
            tags['Model'].overwrite('C13220-')

        assert tags['StripByteCounts'].value == (4461521316,)
        with pytest.raises(ValueError):
            # old value > 32-bit
            tags['StripByteCounts'].overwrite(0)

    with TiffFile(filename, mode='r') as tif:
        assert tif.is_ndpi
        tags = tif.pages.first.tags
        assert tags['ImageWidth'].value == 188160
        assert tags['Model'].value == 'C13220'
        assert tags['StripByteCounts'].value == (4461521316,)


def test_class_tifftags():
    """Test TiffTags interface."""
    data = random_data(numpy.uint8, (21, 31))

    with TempFileName('class_tifftags') as filename:
        imwrite(filename, data, description='test', software=False)

        with TiffFile(filename) as tif:
            tags = tif.pages.first.tags
            # assert len(tags) == 14
            assert 270 in tags
            assert 'ImageDescription' in tags
            assert tags[270].value == 'test'
            assert tags['ImageDescription'].value == 'test'
            assert tags.get(270).value == 'test'
            assert tags.get('ImageDescription').value == 'test'
            assert tags.get(270, index=0).value == 'test'
            assert tags.get('ImageDescription', index=0).value == 'test'
            assert tags.get(270, index=1).value.startswith('{')
            assert tags.get('ImageDescription', index=1).value.startswith('{')
            assert tags.get(270, index=2) is None
            assert tags.get('ImageDescription', index=2) is None
            assert tags.getall(270)[0].value == 'test'
            assert tags.getall(270)[1].value.startswith('{')
            assert tags.getall('ImageDescription')[0].value == 'test'
            assert tags.getall('ImageDescription')[1].value.startswith('{')
            assert tags.getall(0, default=1) == 1
            assert tags.getall('0', default=1) == 1
            assert tags.getall(None, default=1) == 1

            assert len(tags.getall(270)) == 2
            assert 305 not in tags
            assert 'Software' not in tags
            assert tags.get(305) is None
            assert tags.get('Software') is None
            with pytest.raises(KeyError):
                _ = tags[305].value
            with pytest.raises(KeyError):
                _ = tags['Software'].value
            assert len(tags.values()) == len(tags.items())
            assert len(tags.keys()) == len(tags.items()) - 1
            assert set(tags.keys()) == {i[0] for i in tags.items()}
            assert list(tags.values()) == [i[1] for i in tags.items()]
            assert list(tags.values()) == list(tags)

            tag270 = tags[270]
            del tags[270]
            assert 270 not in tags
            assert 'ImageDescription' not in tags
            with pytest.raises(KeyError):
                del tags[270]
            with pytest.raises(KeyError):
                del tags['ImageDescription']

            tags.add(tag270)
            assert 270 in tags
            assert 'ImageDescription' in tags
            del tags['ImageDescription']
            assert 270 not in tags
            assert 'ImageDescription' not in tags

            tags[270] = tag270
            assert 270 in tags
            assert 'ImageDescription' in tags

            assert 0 not in tags
            assert 'None' not in tags
            assert None not in tags
            assert 'TiffTags' in repr(tags)


def test_class_tifftagregistry():
    """Test TiffTagRegistry."""
    numtags = 667
    tags = TIFF.TAGS
    assert len(tags) == numtags
    assert tags[11] == 'ProcessingSoftware'
    assert tags['ProcessingSoftware'] == 11
    assert tags.getall(11) == ['ProcessingSoftware']
    assert tags.getall('ProcessingSoftware') == [11]
    tags.add(11, 'ProcessingSoftware')
    assert len(tags) == numtags

    # one code with two names
    assert 34853 in tags
    assert 'GPSTag' in tags
    assert 'OlympusSIS2' in tags
    assert tags[34853] == 'GPSTag'
    assert tags['GPSTag'] == 34853
    assert tags['OlympusSIS2'] == 34853
    assert tags.getall(34853) == ['GPSTag', 'OlympusSIS2']
    assert tags.getall('GPSTag') == [34853]

    del tags[34853]
    assert len(tags) == numtags - 2
    assert 34853 not in tags
    assert 'GPSTag' not in tags
    assert 'OlympusSIS2' not in tags
    tags.add(34853, 'GPSTag')
    tags.add(34853, 'OlympusSIS2')
    assert 34853 in tags
    assert 'GPSTag' in tags
    assert 'OlympusSIS2' in tags

    info = str(tags)
    assert "34853, 'GPSTag'" in info
    assert "34853, 'OlympusSIS2'" in info

    # two codes with same name
    assert 37387 in tags
    assert 41483 in tags
    assert 'FlashEnergy' in tags
    assert tags[37387] == 'FlashEnergy'
    assert tags[41483] == 'FlashEnergy'
    assert tags['FlashEnergy'] == 37387
    assert tags.getall('FlashEnergy') == [37387, 41483]
    assert tags.getall(37387) == ['FlashEnergy']
    assert tags.getall(41483) == ['FlashEnergy']

    del tags['FlashEnergy']
    assert len(tags) == numtags - 2
    assert 37387 not in tags
    assert 41483 not in tags
    assert 'FlashEnergy' not in tags
    tags.add(37387, 'FlashEnergy')
    tags.add(41483, 'FlashEnergy')
    assert 37387 in tags
    assert 41483 in tags
    assert 'FlashEnergy' in tags

    assert "37387, 'FlashEnergy'" in info
    assert "41483, 'FlashEnergy'" in info


@pytest.mark.parametrize(
    ('shape', 'storedshape', 'dtype', 'axes', 'error'),
    [
        # separate and contig
        ((32, 32), (1, 2, 1, 32, 32, 2), numpy.uint8, None, ValueError),
        # depth
        ((32, 32, 32), (1, 1, 32, 32, 32, 1), numpy.uint8, None, OmeXmlError),
        # dtype
        ((32, 32), (1, 1, 1, 32, 32, 1), numpy.float16, None, OmeXmlError),
        # empty
        ((0, 0), (1, 1, 1, 0, 0, 1), numpy.uint8, None, OmeXmlError),
        # not YX
        ((32, 32), (1, 1, 1, 32, 32, 1), numpy.uint8, 'XZ', OmeXmlError),
        # unknown axis
        ((1, 32, 32), (1, 1, 1, 32, 32, 1), numpy.uint8, 'KYX', OmeXmlError),
        # double axis
        ((1, 32, 32), (1, 1, 1, 32, 32, 1), numpy.uint8, 'YYX', OmeXmlError),
        # more than 5 dimensions
        (
            (1, 1, 1, 5, 32, 32),
            (5, 1, 1, 32, 32, 1),
            numpy.uint8,
            None,
            OmeXmlError,
        ),
        # more than 6 dimensions
        (
            (1, 1, 1, 1, 32, 32, 3),
            (1, 1, 1, 32, 32, 3),
            numpy.uint8,
            None,
            OmeXmlError,
        ),
        # more than 8 dimensions
        (
            (1, 1, 1, 1, 1, 1, 1, 32, 32),
            (1, 1, 1, 32, 32, 1),
            numpy.uint8,
            'ARHETZCYX',
            OmeXmlError,
        ),
        # more than 9 dimensions
        (
            (1, 1, 1, 1, 1, 1, 1, 32, 32, 3),
            (1, 1, 1, 32, 32, 3),
            numpy.uint8,
            'ARHETZCYXS',
            OmeXmlError,
        ),
        # double axis
        # ((1, 32, 32), (1, 1, 1, 32, 32, 1), numpy.uint8, 'YYX', OmeXmlError),
        # planecount mismatch
        ((3, 32, 32), (1, 1, 1, 32, 32, 1), numpy.uint8, 'CYX', ValueError),
        # stored shape mismatch
        ((3, 32, 32), (1, 2, 1, 32, 32, 1), numpy.uint8, 'SYX', ValueError),
        ((32, 32, 3), (1, 1, 1, 32, 32, 2), numpy.uint8, 'YXS', ValueError),
        ((3, 32, 32), (1, 3, 1, 31, 31, 1), numpy.uint8, 'SYX', ValueError),
        ((32, 32, 3), (1, 1, 1, 31, 31, 3), numpy.uint8, 'YXS', ValueError),
        ((32, 32), (1, 1, 1, 32, 31, 1), numpy.uint8, None, ValueError),
        # too many modulo dimensions
        (
            (2, 3, 4, 5, 32, 32),
            (60, 1, 1, 32, 32, 1),
            numpy.uint8,
            'RHEQYX',
            OmeXmlError,
        ),
    ],
)
def test_class_omexml_fail(shape, storedshape, dtype, axes, error):
    """Test OmeXml class failures."""
    metadata = {'axes': axes} if axes else {}
    ox = OmeXml()
    with pytest.raises(error):
        ox.addimage(dtype, shape, storedshape, **metadata)


@pytest.mark.parametrize(
    ('axes', 'autoaxes', 'shape', 'storedshape', 'dimorder'),
    [
        ('YX', 'YX', (32, 32), (1, 1, 1, 32, 32, 1), 'XYCZT'),
        ('YXS', 'YXS', (32, 32, 1), (1, 1, 1, 32, 32, 1), 'XYCZT'),
        ('SYX', 'SYX', (1, 32, 32), (1, 1, 1, 32, 32, 1), 'XYCZT'),
        ('YXS', 'YXS', (32, 32, 3), (1, 1, 1, 32, 32, 3), 'XYCZT'),
        ('SYX', 'SYX', (3, 32, 32), (1, 3, 1, 32, 32, 1), 'XYCZT'),
        ('CYX', 'CYX', (5, 32, 32), (5, 1, 1, 32, 32, 1), 'XYCZT'),
        ('CYXS', 'CYXS', (5, 32, 32, 1), (5, 1, 1, 32, 32, 1), 'XYCZT'),
        ('CSYX', 'ZCYX', (5, 1, 32, 32), (5, 1, 1, 32, 32, 1), 'XYCZT'),  # !
        ('CYXS', 'CYXS', (5, 32, 32, 3), (5, 1, 1, 32, 32, 3), 'XYCZT'),
        ('CSYX', 'CSYX', (5, 3, 32, 32), (5, 3, 1, 32, 32, 1), 'XYCZT'),
        ('TZCYX', 'TZCYX', (3, 4, 5, 32, 32), (60, 1, 1, 32, 32, 1), 'XYCZT'),
        (
            'TZCYXS',
            'TZCYXS',
            (3, 4, 5, 32, 32, 1),
            (60, 1, 1, 32, 32, 1),
            'XYCZT',
        ),
        (
            'TZCSYX',
            'TZCSYX',
            (3, 4, 5, 1, 32, 32),
            (60, 1, 1, 32, 32, 1),
            'XYCZT',
        ),
        (
            'TZCYXS',
            'TZCYXS',
            (3, 4, 5, 32, 32, 3),
            (60, 1, 1, 32, 32, 3),
            'XYCZT',
        ),
        ('ZTCSYX', '', (3, 4, 5, 3, 32, 32), (60, 3, 1, 32, 32, 1), 'XYCTZ'),
    ],
)
@pytest.mark.parametrize('metadata', ['axes', None])
def test_class_omexml(axes, autoaxes, shape, storedshape, dimorder, metadata):
    """Test OmeXml class."""
    dtype = numpy.uint8
    if not metadata and dimorder != 'XYCZT':
        pytest.xfail('')
    metadata = {'axes': axes} if metadata else {}
    omexml = OmeXml()
    omexml.addimage(dtype, shape, storedshape, **metadata)
    if IS_WIN:
        assert '\n  ' in str(omexml)
    omexml = omexml.tostring()
    assert dimorder in omexml
    if metadata:
        autoaxes = axes
    for ax in 'XYCZT':
        size = shape[autoaxes.index(ax)] if ax in autoaxes else 1
        if ax == 'C':
            size *= storedshape[1] * storedshape[-1]
        assert f'Size{ax}="{size}"' in omexml
    assert__repr__(omexml)
    assert_valid_omexml(omexml)


@pytest.mark.parametrize(
    ('axes', 'shape', 'storedshape', 'sizetzc', 'dimorder'),
    [
        ('ZAYX', (3, 4, 32, 32), (12, 1, 1, 32, 32, 1), (1, 12, 1), 'XYCZT'),
        ('AYX', (3, 32, 32), (3, 1, 1, 32, 32, 1), (3, 1, 1), 'XYCZT'),
        ('APYX', (3, 4, 32, 32), (12, 1, 1, 32, 32, 1), (3, 4, 1), 'XYCZT'),
        ('TAYX', (3, 4, 32, 32), (12, 1, 1, 32, 32, 1), (12, 1, 1), 'XYCZT'),
        (
            'CHYXS',
            (3, 4, 32, 32, 3),
            (12, 1, 1, 32, 32, 3),
            (1, 1, 36),
            'XYCZT',
        ),
        (
            'CHSYX',
            (3, 4, 3, 32, 32),
            (12, 3, 1, 32, 32, 1),
            (1, 1, 36),
            'XYCZT',
        ),
        (
            'APRYX',
            (3, 4, 5, 32, 32),
            (60, 1, 1, 32, 32, 1),
            (3, 4, 5),
            'XYCZT',
        ),
        (
            'TAPYX',
            (3, 4, 5, 32, 32),
            (60, 1, 1, 32, 32, 1),
            (12, 5, 1),
            'XYCZT',
        ),
        (
            'TZAYX',
            (3, 4, 5, 32, 32),
            (60, 1, 1, 32, 32, 1),
            (3, 20, 1),
            'XYCZT',
        ),
        (
            'ZCHYX',
            (3, 4, 5, 32, 32),
            (60, 1, 1, 32, 32, 1),
            (1, 3, 20),
            'XYCZT',
        ),
        (
            'EPYX',
            (10, 5, 200, 200),
            (50, 1, 1, 200, 200, 1),
            (10, 5, 1),
            'XYCZT',
        ),
        (
            'TQCPZRYX',
            (2, 3, 4, 5, 6, 7, 32, 32),
            (5040, 1, 1, 32, 32, 1),
            (6, 42, 20),
            'XYZCT',
        ),
    ],
)
def test_class_omexml_modulo(axes, shape, storedshape, sizetzc, dimorder):
    """Test OmeXml class with modulo dimensions."""
    dtype = numpy.uint8
    omexml = OmeXml()
    omexml.addimage(dtype, shape, storedshape, axes=axes)
    assert '\n  ' in str(omexml)
    omexml = omexml.tostring()
    assert dimorder in omexml
    for ax, size in zip('TZC', sizetzc, strict=True):
        assert f'Size{ax}="{size}"' in omexml
    assert__repr__(omexml)
    assert_valid_omexml(omexml)


def test_class_omexml_attributes():
    """Test OmeXml class with attributes and elements."""
    from uuid import uuid1

    uuid = str(uuid1())
    metadata = {
        # document
        'UUID': uuid,
        'Creator': f'test_tifffile.py {tifffile.__version__}',
        # image
        'axes': 'QZYXS',
        'Name': 'ImageName',
        'Acquisitiondate': '2011-09-16T10:45:48',
        'Description': 'Image "Description" < & >\n{test}',
        'TypeDescription': {'Q': 'Phasor'},
        'SignificantBits': 12,
        'PhysicalSizeX': 1.1,
        'PhysicalSizeXUnit': 'nm',
        'PhysicalSizeY': 1.2,
        'PhysicalSizeYUnit': '\xb5m',
        'PhysicalSizeZ': 1.3,
        'PhysicalSizeZUnit': '\xc5',
        'TimeIncrement': 1.4,
        'TimeIncrementUnit': '\xb5s',
        'Channel': {'Name': 'ChannelName'},  # one channel with 3 samples
        'Plane': {'PositionZ': [0.0, 0.0, 2.0, 2.0, 4.0, 4.0]},  # 6 TZ-planes
        'CommentAnnotation': 'Tifffile test',
        'BooleanAnnotation': True,
        'LongAnnotation': [1, 2],
        'DoubleAnnotation': {'Description': 'A double', 'Value': 1.0},
        'MapAnnotation': [
            {'Description': 'description', 'key': '<str/>', 'key2': 1.0},
            {'Namespace': 'ns.org', 'key2': 1, 'key3': 2},
        ],
    }

    omexml = OmeXml(**metadata)
    omexml.addimage(
        numpy.uint16, (2, 3, 32, 32, 3), (6, 1, 1, 32, 32, 3), **metadata
    )
    xml = omexml.tostring()
    for value in (
        f'UUID="urn:uuid:{uuid}"',
        'SizeX="32" SizeY="32" SizeC="3" SizeZ="3" SizeT="2"',
        'Interleaved="true" SignificantBits="12"',
        '<Channel ID="Channel:0:0" SamplesPerPixel="3" Name="ChannelName">',
        '<Plane TheC="0" TheZ="2" TheT="1" PositionZ="4.0"/>',
        '<StructuredAnnotations><XMLAnnotation ID="Annotation:0" Namespace',
        '<ModuloAlongT Type="other" TypeDescription="Phasor" '
        'Start="0" End="1"/>',
        '<CommentAnnotation ID="Annotation:1">'
        '<Value>Tifffile test</Value></CommentAnnotation>',
        '<BooleanAnnotation ID="Annotation:2">'
        '<Value>true</Value></BooleanAnnotation>',
        '<LongAnnotation ID="Annotation:3"><Value>1</Value></LongAnnotation>'
        '<LongAnnotation ID="Annotation:4"><Value>2</Value>',
        '<DoubleAnnotation ID="Annotation:5">'
        '<Description>A double</Description>'
        '<Value>1.0</Value></DoubleAnnotation>',
        '<MapAnnotation ID="Annotation:6">'
        '<Description>description</Description>'
        '<Value><M K="key">&lt;str/&gt;</M><M K="key2">1.0</M></Value>'
        '</MapAnnotation>',
        '<MapAnnotation ID="Annotation:7" Namespace="ns.org">'
        '<Value><M K="key2">1</M><M K="key3">2</M></Value></MapAnnotation>',
    ):
        assert value in xml
    if IS_PYPY:
        pytest.xfail('lxml bug?')
    assert__repr__(omexml)
    assert_valid_omexml(xml)
    assert '\n  ' in str(omexml)

    with TempFileName('class_omexml_attributes') as filename:
        imwrite(
            filename,
            shape=(2, 3, 32, 32, 3),
            dtype=numpy.uint16,
            ome=True,
            photometric='rgb',
            metadata=metadata,
        )
        assert imread(filename).shape == (2, 3, 32, 32, 3)


def test_class_omexml_datasets():
    """Test OmeXml class with datasets."""
    args = numpy.uint8, (3, 7), (1, 1, 1, 3, 7, 1)
    kwargs = {'dtype': numpy.uint8, 'shape': (3, 7)}

    omexml = OmeXml()
    # no dataset
    omexml.addimage(*args, CommentAnnotation='0')
    # first dataset explicit
    omexml.addimage(
        *args,
        Dataset={
            'Name': 'First',
            'Description': 'Dataset',
            'CommentAnnotation': '1',
        },
    )
    # first dataset implicit
    omexml.addimage(*args, CommentAnnotation='2')
    # no dataset
    omexml.addimage(*args, Dataset=None, CommentAnnotation='3')
    # first dataset implicit
    omexml.addimage(*args, CommentAnnotation='4')
    # second dataset explicit
    omexml.addimage(*args, Dataset={'CommentAnnotation': '5'})

    xml = omexml.tostring()
    for value in (
        '<Dataset ID="Dataset:0" Name="First">'
        '<Description>Dataset</Description>'
        '<ImageRef ID="Image:1"/>'
        '<ImageRef ID="Image:2"/>'
        '<ImageRef ID="Image:4"/>'
        '<AnnotationRef ID="Annotation:1"/>'
        '</Dataset>',
        '<Dataset ID="Dataset:1">'
        '<ImageRef ID="Image:5"/>'
        '<AnnotationRef ID="Annotation:5"/>'
        '</Dataset>',
        '<AnnotationRef ID="Annotation:4"/></Image>'
        '<Image ID="Image:5" Name="Image5">',
    ):
        assert value in xml
    if IS_PYPY:
        pytest.xfail('lxml bug?')
    assert__repr__(omexml)
    assert_valid_omexml(xml)
    assert '\n  ' in str(omexml)

    with TempFileName('class_omexml_datasets') as filename:
        with TiffWriter(filename, ome=True) as tif:
            tif.write(**kwargs, metadata={'CommentAnnotation': '0'})
            tif.write(
                **kwargs,
                metadata={
                    'Dataset': {
                        'Name': 'First',
                        'Description': 'Dataset',
                        'CommentAnnotation': '1',
                    }
                },
            )
            tif.write(**kwargs, metadata={'CommentAnnotation': '2'})
            tif.write(
                **kwargs, metadata={'Dataset': None, 'CommentAnnotation': '3'}
            )
            tif.write(**kwargs, metadata={'CommentAnnotation': '4'})
            tif.write(
                **kwargs, metadata={'Dataset': {'CommentAnnotation': '5'}}
            )

        with TiffFile(filename) as tif:
            assert len(tif.series) == 6


def test_class_omexml_project():
    """Test OmeXml class with Experimenter and Project elements."""
    args = numpy.uint8, (3, 7), (1, 1, 1, 3, 7, 1)

    # single Experimenter at init level
    omexml = OmeXml(
        Experimenter={
            'FirstName': 'First',
            'LastName': 'Last',
            'Email': 'e@mail.com',
        },
        Project={'Name': 'MyProject', 'Description': 'My project'},
    )
    omexml.addimage(*args)
    xml = omexml.tostring()
    assert '<Experimenter ID="Experimenter:0"' in xml
    assert 'FirstName="First"' in xml
    assert 'LastName="Last"' in xml
    assert 'Email="e@mail.com"' in xml
    assert '<Project ID="Project:0" Name="MyProject">' in xml
    assert '<Description>My project</Description>' in xml
    # ordering: Project before Experimenter before Image
    assert (
        xml.index('<Project')
        < xml.index('<Experimenter')
        < xml.index('<Image')
    )
    # no ExperimenterRef in Image (Experimenter was only at init level)
    assert '<ExperimenterRef' not in xml
    assert_valid_omexml(xml)

    # list of Experimenters and Projects at init level
    omexml2 = OmeXml(
        Experimenter=[
            {'FirstName': 'Alice'},
            {'FirstName': 'Bob', 'Institution': 'Uni'},
        ],
        Project=[
            {'Name': 'Proj1'},
            {'Name': 'Proj2'},
        ],
    )
    omexml2.addimage(*args)
    xml2 = omexml2.tostring()
    assert xml2.count('<Experimenter ') == 2
    assert xml2.count('<Project ') == 2
    assert '<Experimenter ID="Experimenter:0"' in xml2
    assert '<Experimenter ID="Experimenter:1"' in xml2
    assert '<Project ID="Project:0" Name="Proj1">' in xml2
    assert '<Project ID="Project:1" Name="Proj2">' in xml2
    assert_valid_omexml(xml2)

    # Experimenter at per-image level creates ExperimenterRef in Image
    omexml3 = OmeXml()
    omexml3.addimage(
        *args,
        AcquisitionDate='2024-01-01T00:00:00',
        Description='My image',
        Experimenter={'FirstName': 'Eve', 'LastName': 'Jones'},
    )
    xml3 = omexml3.tostring()
    assert '<Experimenter ID="Experimenter:0"' in xml3
    assert 'FirstName="Eve"' in xml3
    assert '<ExperimenterRef ID="Experimenter:0"/>' in xml3
    # schema order: AcquisitionDate < ExperimenterRef < Description
    assert (
        xml3.index('AcquisitionDate')
        < xml3.index('ExperimenterRef')
        < xml3.index('Description')
    )
    assert_valid_omexml(xml3)

    # Project with AnnotationRef
    omexml4 = OmeXml(
        Project={
            'Name': 'Annotated',
            'CommentAnnotation': 'project note',
        }
    )
    omexml4.addimage(*args)
    xml4 = omexml4.tostring()
    assert '<Project ID="Project:0" Name="Annotated">' in xml4
    assert '<AnnotationRef ID="Annotation:0"/>' in xml4
    assert '<CommentAnnotation ID="Annotation:0">' in xml4
    assert_valid_omexml(xml4)

    # Experimenter with AnnotationRef
    omexml5 = OmeXml(
        Experimenter={
            'FirstName': 'Bob',
            'CommentAnnotation': 'experimenter note',
        }
    )
    omexml5.addimage(*args)
    xml5 = omexml5.tostring()
    assert '<Experimenter ID="Experimenter:0"' in xml5
    assert '<AnnotationRef ID="Annotation:0"/>' in xml5
    assert_valid_omexml(xml5)

    assert__repr__(omexml)


def test_class_omexml_multiimage():
    """Test OmeXml class with multiple images."""
    omexml = OmeXml(description='multiimage')
    omexml.addimage(
        numpy.uint8, (32, 32, 3), (1, 1, 1, 32, 32, 3), name='preview'
    )
    omexml.addimage(
        numpy.float32, (4, 256, 256), (4, 1, 1, 256, 256, 1), name='1'
    )
    omexml.addimage('bool', (256, 256), (1, 1, 1, 256, 256, 1), name='mask')
    assert '\n  ' in str(omexml)
    omexml = omexml.tostring()
    assert 'TiffData IFD="0" PlaneCount="1"' in omexml
    assert 'TiffData IFD="1" PlaneCount="4"' in omexml
    assert 'TiffData IFD="5" PlaneCount="1"' in omexml
    assert_valid_omexml(omexml)


def test_class_omexml_escape():
    """Test OmeXml class escaping of special characters in metadata values."""
    # double-quote in attribute and element values
    omexml = OmeXml()
    omexml.addimage(
        numpy.uint8,
        (32, 32),
        (1, 1, 1, 32, 32, 1),
        Name='Image "Name"',
        Channel={'Name': 'Ch "0"'},
        MapAnnotation={'key "1"': 'val "2"'},
    )
    xml = omexml.tostring()
    assert 'Name="Image &quot;Name&quot;"' in xml
    assert 'Name="Ch &quot;0&quot;"' in xml
    assert '<M K="key &quot;1&quot;">val &quot;2&quot;</M>' in xml
    assert_valid_omexml(xml)

    # mixed string: partial pre-escape must NOT suppress escaping of raw chars
    # previously the guard returned 'a > b &amp; c' unchanged, leaving > raw
    assert OmeXml._escape('a > b &amp; c') == 'a &gt; b &amp;amp; c'


def test_class_timer(capsys):
    """Test Timer class."""
    started = Timer.clock()
    with Timer('test_class_timer', started=started) as timer:
        assert timer.started == started
        captured = capsys.readouterr()
        assert captured.out == 'test_class_timer '
        duration = timer.stop()
        assert timer.duration == duration
        assert timer.stopped == started + duration
        timer.duration = 314159.265359
        assert__repr__(timer)
    captured = capsys.readouterr()
    assert captured.out == '3 days, 15:15:59.265359 s\n'


def test_parse_kwargs():
    """Test parse_kwargs function."""
    kwargs = {'one': 1, 'two': 2, 'four': 4}
    kwargs2 = parse_kwargs(kwargs, 'two', 'three', four=None, five=5)
    assert kwargs == {'one': 1}
    assert kwargs2 == {'two': 2, 'four': 4, 'five': 5}

    assert parse_kwargs({}, 'a', b=2) == {'b': 2}

    # _del not supported
    # kwargs = {'one': 1, 'two': 2, 'four': 4}
    # kwargs2 = parse_kwargs(
    #     kwargs, 'two', 'three', four=None, five=5, _del=False
    # )
    # assert kwargs == {'one': 1, 'two': 2, 'four': 4}
    # assert kwargs2 == {'two': 2, 'four': 4, 'five': 5}


def test_update_kwargs():
    """Test update_kwargs function."""
    kwargs = {
        'one': 1,
    }
    update_kwargs(kwargs, one=None, two=2)
    assert kwargs == {'one': 1, 'two': 2}


def test_func_xml2dict():
    """Test xml2dict function."""
    xml = """<?xml version="1.0" ?>
    <root attr="attribute">
        <int>-1</int>
        <ints>-1,2</ints>
        <float>-3.14</float>
        <floats>1.0, -2.0</floats>
        <bool>True</bool>
        <string>Lorem, Ipsum</string>
    </root>
    """
    d = xml2dict(xml)['root']
    assert d['attr'] == 'attribute'
    assert d['int'] == -1
    assert d['ints'] == (-1, 2)
    assert d['float'] == -3.14
    assert d['floats'] == (1.0, -2.0)
    assert d['bool'] is True
    assert d['string'] == 'Lorem, Ipsum'

    d = xml2dict(xml, prefix=('a_', 'b_'), sep='')['root']
    assert d['ints'] == '-1,2'
    assert d['floats'] == '1.0, -2.0'


def test_func_memmap():
    """Test memmap function."""
    with TempFileName('func_memmap_new') as filename:
        # create new file
        im = memmap(
            filename,
            shape=(32, 16),
            dtype=numpy.float32,
            bigtiff=True,
            compression=False,
        )
        im[31, 15] = 1.0
        im.flush()
        assert im.shape == (32, 16)
        assert im.dtype == numpy.float32
        del im
        im = memmap(filename, page=0, mode='r')
        assert im[31, 15] == 1.0
        del im
        im = memmap(filename, series=0, mode='c')
        assert im[31, 15] == 1.0
        del im
        # append to file
        im = memmap(
            filename,
            shape=(3, 64, 64),
            dtype=numpy.uint16,
            append=True,
            photometric=PHOTOMETRIC.MINISBLACK,
        )
        im[2, 63, 63] = 1.0
        im.flush()
        assert im.shape == (3, 64, 64)
        assert im.dtype == numpy.uint16
        del im
        im = memmap(filename, page=3, mode='r')
        assert im[63, 63] == 1
        del im
        im = memmap(filename, series=1, mode='c')
        assert im[2, 63, 63] == 1
        del im
        # can not memory-map compressed array
        with pytest.raises(ValueError):
            memmap(
                filename,
                shape=(16, 16),
                dtype=numpy.float32,
                append=True,
                compression=COMPRESSION.ADOBE_DEFLATE,
            )


@pytest.mark.skipif(IS_BE, reason=REASON)
@pytest.mark.parametrize('byteorder', ['>', '<'])
def test_func_memmap_byteorder(byteorder):
    """Test non-native byteorder can be memory mapped."""
    bo = {'<': 'le', '>': 'be'}[byteorder]

    with TempFileName(f'func_memmap_byteorder_{bo}') as filename:
        if os.path.exists(filename):
            os.remove(filename)

        # new, empty file
        im = memmap(
            filename, shape=(16, 16), dtype=numpy.uint16, byteorder=byteorder
        )
        assert im.dtype.byteorder == byteorder
        im[0, 0] = 253
        im[1, 1] = 257
        del im

        with TiffFile(filename) as tif:
            assert tif.byteorder == byteorder
            im = tif.asarray()
            assert im.dtype.byteorder in {'<', '='}  # native
            assert im[0, 0] == 253
            assert im[1, 1] == 257

        im = memmap(filename, mode='r')
        assert im.dtype.byteorder == '=' if byteorder == '<' else '>'
        assert im[0, 0] == 253
        assert im[1, 1] == 257
        del im


def test_func_repeat_nd():
    """Test repeat_nd function."""
    a = repeat_nd([[0, 1, 2], [3, 4, 5], [6, 7, 8]], (2, 3))
    assert_array_equal(
        a,
        [
            [0, 0, 0, 1, 1, 1, 2, 2, 2],
            [0, 0, 0, 1, 1, 1, 2, 2, 2],
            [3, 3, 3, 4, 4, 4, 5, 5, 5],
            [3, 3, 3, 4, 4, 4, 5, 5, 5],
            [6, 6, 6, 7, 7, 7, 8, 8, 8],
            [6, 6, 6, 7, 7, 7, 8, 8, 8],
        ],
    )


def test_func_byteorder_isnative():
    """Test byteorder_isnative function."""
    assert byteorder_isnative(sys.byteorder)
    assert byteorder_isnative('=')
    if sys.byteorder == 'little':
        assert byteorder_isnative('<')
        assert not byteorder_isnative('>')
    else:
        assert byteorder_isnative('>')
        assert not byteorder_isnative('<')


def test_func_byteorder_compare():
    """Test byteorder_isnative function."""
    assert byteorder_compare('<', '<')
    assert byteorder_compare('>', '>')
    assert byteorder_compare('=', '=')
    assert byteorder_compare('|', '|')
    assert byteorder_compare('>', '|')
    assert byteorder_compare('<', '|')
    assert byteorder_compare('|', '>')
    assert byteorder_compare('|', '<')
    assert byteorder_compare('=', '|')
    assert byteorder_compare('|', '=')
    if sys.byteorder == 'little':
        assert byteorder_compare('<', '=')
    else:
        assert byteorder_compare('>', '=')


def test_func_reshape_nd():
    """Test reshape_nd function."""
    assert reshape_nd(numpy.empty(0), 2).shape == (1, 0)
    assert reshape_nd(numpy.empty(1), 3).shape == (1, 1, 1)
    assert reshape_nd(numpy.empty((2, 3)), 3).shape == (1, 2, 3)
    assert reshape_nd(numpy.empty((2, 3, 4)), 3).shape == (2, 3, 4)

    assert reshape_nd((0,), 2) == (1, 0)
    assert reshape_nd((1,), 3) == (1, 1, 1)
    assert reshape_nd((2, 3), 3) == (1, 2, 3)
    assert reshape_nd((2, 3, 4), 3) == (2, 3, 4)


def test_func_order_axes():
    """Test axis_order function."""
    axes = [(0, 2, 0), (1, 2, 0), (0, 2, 1), (1, 2, 1)]
    # first axis varies fastest, second axis is constant
    assert order_axes(axes) == (1, 2, 0)
    assert order_axes(axes, squeeze=True) == (2, 0)


def test_func_apply_colormap():
    """Test apply_colormap function."""
    image = numpy.arange(256, dtype=numpy.uint8)
    colormap = numpy.vstack([image, image, image]).astype(numpy.uint16) * 256
    assert_array_equal(apply_colormap(image, colormap)[-1], colormap[:, -1])


def test_func_parse_filenames():
    """Test parse_filenames function."""

    def func(*args, **kwargs):
        labels, shape, indices, _ = parse_filenames(*args, **kwargs)
        return ''.join(labels), shape, indices

    files = ['c1t001.ext', 'c1t002.ext', 'c2t002.ext']  # 'c2t001.ext' missing
    # group names
    p = r'(?P<a>\d).[!\d](?P<b>\d+)\.ext'
    assert func(files[:1], p) == ('ab', (1, 1), [(0, 0)])  # (1, 1)
    assert func(files[:2], p) == ('ab', (1, 2), [(0, 0), (0, 1)])  # (1, 1)
    assert func(files, p) == ('ab', (2, 2), [(0, 0), (0, 1), (1, 1)])  # (1, 1)
    # unknown axes
    p = r'(\d)[^\d](\d+)\.ext'
    assert func(files[:1], p) == ('QQ', (1, 1), [(0, 0)])  # (1, 1)
    assert func(files[:2], p) == ('QQ', (1, 2), [(0, 0), (0, 1)])  # (1, 1)
    assert func(files, p) == ('QQ', (2, 2), [(0, 0), (0, 1), (1, 1)])  # (1, 1)
    # match axes
    p = r'([^\d])(\d)([^\d])(\d+)\.ext'
    assert func(files[:1], p) == ('ct', (1, 1), [(0, 0)])  # (1, 1)
    assert func(files[:2], p) == ('ct', (1, 2), [(0, 0), (0, 1)])  # (1, 1)
    assert func(files, p) == ('ct', (2, 2), [(0, 0), (0, 1), (1, 1)])  # (1, 1)
    # misc
    files = ['c0t001.ext', 'c0t002.ext', 'c2t002.ext']  # 'c2t001.ext' missing
    p = r'([^\d])(\d)[^\d](?P<b>\d+)\.ext'
    assert func(files[:1], p) == ('cb', (1, 1), [(0, 0)])  # (0, 1)
    assert func(files[:2], p) == ('cb', (1, 2), [(0, 0), (0, 1)])  # (0, 1)
    assert func(files, p) == ('cb', (3, 2), [(0, 0), (0, 1), (2, 1)])  # (0, 1)

    # BBBC006_v1
    categories = {'p': {chr(i + 97): i for i in range(25)}}
    files = [
        'BBBC006_v1_images_z_00/mcf-z-stacks-03212011_a01_s1_w1a57.tif',
        'BBBC006_v1_images_z_00/mcf-z-stacks-03212011_a03_s2_w1419.tif',
        'BBBC006_v1_images_z_00/mcf-z-stacks-03212011_p24_s2_w2283.tif',
        'BBBC006_v1_images_z_01/mcf-z-stacks-03212011_p24_s2_w11cf.tif',
    ]
    # don't match directory
    p = r'_(?P<p>[a-z])(?P<a>\d+)(?:_(s)(\d))(?:_(w)(\d))'
    assert func(files[:1], p, categories=categories) == (
        'pasw',
        (1, 1, 1, 1),
        [(0, 0, 0, 0)],
        # (97, 1, 1, 1),
    )
    assert func(files[:2], p, categories=categories) == (
        'pasw',
        (1, 3, 2, 1),
        [(0, 0, 0, 0), (0, 2, 1, 0)],
        # (97, 1, 1, 1),
    )
    # match directory
    p = r'(?:_(z)_(\d+)).*_(?P<p>[a-z])(?P<a>\d+)(?:_(s)(\d))(?:_(w)(\d))'
    assert func(files, p, categories=categories) == (
        'zpasw',
        (2, 16, 24, 2, 2),
        [
            (0, 0, 0, 0, 0),
            (0, 0, 2, 1, 0),
            (0, 15, 23, 1, 1),
            (1, 15, 23, 1, 0),
        ],
        # (0, 97, 1, 1, 1),
    )
    # reorder axes
    p = r'(?:_(z)_(\d+)).*_(?P<p>[a-z])(?P<a>\d+)(?:_(s)(\d))(?:_(w)(\d))'
    assert func(
        files, p, axesorder=(2, 0, 1, 3, 4), categories=categories
    ) == (
        'azpsw',
        (24, 2, 16, 2, 2),
        [
            (0, 0, 0, 0, 0),
            (2, 0, 0, 1, 0),
            (23, 0, 15, 1, 1),
            (23, 1, 15, 1, 0),
        ],
        # (1, 0, 97, 1, 1),
    )


def test_func_reshape_axes():
    """Test reshape_axes function."""
    assert reshape_axes('YXS', (219, 301, 1), (219, 301, 1)) == 'YXS'
    assert reshape_axes('YXS', (219, 301, 3), (219, 301, 3)) == 'YXS'
    assert reshape_axes('YXS', (219, 301, 1), (219, 301)) == 'YX'
    assert reshape_axes('YXS', (219, 301, 1), (219, 1, 1, 301, 1)) == 'YQQXS'
    assert reshape_axes('IYX', (12, 219, 301), (3, 4, 219, 301, 1)) == 'QQYXQ'
    assert (
        reshape_axes('IYX', (12, 219, 301), (3, 4, 219, 1, 301, 1)) == 'QQYQXQ'
    )
    assert (
        reshape_axes('IYX', (12, 219, 301), (3, 2, 219, 2, 301, 1)) == 'QQQQXQ'
    )
    with pytest.raises(ValueError):
        reshape_axes('IYX', (12, 219, 301), (3, 4, 219, 2, 301, 1))
    with pytest.raises(ValueError):
        reshape_axes('IYX', (12, 219, 301), (3, 4, 219, 301, 2))


def test_func_julian_datetime():
    """Test julian_datetime function."""
    assert julian_datetime(2451576, 54362783) == (
        datetime.datetime(2000, 2, 2, 15, 6, 2, 783 * 1000)
    )


def test_func_excel_datetime():
    """Test excel_datetime function."""
    assert excel_datetime(40237.029999999795) == (
        datetime.datetime(2010, 2, 28, 0, 43, 11, 999982)
    )


def test_func_natural_sorted():
    """Test natural_sorted function."""
    assert natural_sorted(['f1', 'f2', 'f10']) == ['f1', 'f2', 'f10']


def test_func_bytes2str():
    """Test bytes2str function."""
    # plain ASCII, no null
    assert bytes2str(b'hello') == 'hello'
    # empty bytes
    assert bytes2str(b'') == ''
    # null terminator strips trailing content
    assert bytes2str(b'hello\x00world') == 'hello'
    # null at start
    assert bytes2str(b'\x00hello') == ''
    # valid UTF-8 default encoding
    assert bytes2str('caf\xe9'.encode()) == 'café'
    # valid UTF-8 explicit encoding
    assert bytes2str('caf\xe9'.encode(), encoding='utf-8') == 'café'
    # CP1252 fallback: bytes not valid UTF-8 but valid cp1252
    assert bytes2str(b'\x80') == '\u20ac'  # euro sign in cp1252
    # explicit encoding that fails raises UnicodeDecodeError
    with pytest.raises(UnicodeDecodeError):
        bytes2str(b'\x80', encoding='utf-8')
    # errors argument is passed through
    assert bytes2str(b'\x80', encoding='utf-8', errors='replace') == '\ufffd'
    assert bytes2str(b'\x80', encoding='utf-8', errors='ignore') == ''
    # utf-16-le: basic string
    assert bytes2str('AB'.encode('utf-16-le'), encoding='utf-16-le') == 'AB'
    # utf-16-le: null terminator at aligned boundary
    s = 'AB\x00CD'.encode('utf-16-le')  # AB + null code unit + CD
    assert bytes2str(s, encoding='utf-16-le') == 'AB'
    # utf-16-le: no null terminator — full string returned
    assert bytes2str('AB'.encode('utf-16-le'), encoding='utf-16-le') == 'AB'
    # utf-16-le: garbage bytes after aligned null do not cause decoding error
    s = 'Hi\x00'.encode('utf-16-le') + b'\xff'  # null CU + odd garbage byte
    assert bytes2str(s, encoding='utf-16-le') == 'Hi'
    # utf-16-be: basic string with null terminator
    s = 'XY\x00Z'.encode('utf-16-be')  # XY + null code unit + Z
    assert bytes2str(s, encoding='utf-16-be') == 'XY'
    # utf-16 with BOM
    s = '\ufeffAB\x00CD'.encode('utf-16')
    result = bytes2str(s, encoding='utf-16')
    assert result in {'\ufeffAB', 'AB'}  # BOM handling is codec-defined
    # utf-16: encoding name variants are all recognised as utf-16 family
    s = 'OK\x00'.encode('utf-16-le')
    assert bytes2str(s, encoding='UTF-16-LE') == 'OK'
    assert bytes2str(s, encoding='utf_16_le') == 'OK'


def test_func_product():
    """Test product function."""
    assert product([2**8, 2**30]) == 274877906944
    assert product([]) == 1
    assert product(numpy.array([2**8, 2**30], numpy.int32)) == 274877906944


def test_func_squeeze_axes():
    """Test squeeze_axes function."""
    assert squeeze_axes((5, 1, 2, 1, 1), 'TZYXC') == (
        (5, 2, 1),
        'TYX',
        (True, False, True, True, False),
    )
    assert squeeze_axes((1,), 'Y') == ((1,), 'Y', (True,))
    assert squeeze_axes((1,), 'Q') == ((1,), 'Q', (True,))
    assert squeeze_axes((1, 1), 'PQ') == ((1,), 'Q', (False, True))


def test_func_transpose_axes():
    """Test transpose_axes function."""
    # basic: reorder and add missing axis
    assert transpose_axes(
        numpy.zeros((2, 3, 4, 5)), 'TYXC', asaxes='CTZYX'
    ).shape == (5, 2, 1, 3, 4)
    # no-op: axes already match asaxes
    assert transpose_axes(
        numpy.zeros((5, 2, 1, 3, 4)), 'CTZYX', asaxes='CTZYX'
    ).shape == (5, 2, 1, 3, 4)
    # default asaxes='CTZYX'
    assert transpose_axes(numpy.zeros((3, 4)), 'YX').shape == (1, 1, 1, 3, 4)
    # single axis
    assert transpose_axes(numpy.zeros((7,)), 'X', asaxes='ZYX').shape == (
        1,
        1,
        7,
    )
    # pure reorder, no missing axes
    assert transpose_axes(
        numpy.zeros((2, 3, 4)), 'ZYX', asaxes='XYZ'
    ).shape == (4, 3, 2)
    # all output axes are missing from input (all length-1)
    assert transpose_axes(numpy.zeros((3, 4)), 'YX', asaxes='YXC').shape == (
        3,
        4,
        1,
    )
    # returns a view when no data copy is needed
    arr = numpy.zeros((3, 4))
    result = transpose_axes(arr, 'YX', asaxes='YX')
    assert numpy.shares_memory(arr, result)
    # unknown axis raises ValueError
    with pytest.raises(ValueError, match='unknown axis'):
        transpose_axes(numpy.zeros((3, 4)), 'QX', asaxes='CTZYX')


def test_func_unique_strings():
    """Test unique_strings function."""
    assert list(unique_strings(iter(('a', 'b', 'a')))) == ['a', 'b', 'a2']


def test_func_subresolution():
    """Test subresolution function."""

    class a:
        dtype = numpy.uint8
        axes = 'QzyxS'
        shape = (3, 256, 512, 1024, 4)

    class b:
        dtype = numpy.uint8
        axes = 'QzyxS'
        shape = (3, 128, 256, 512, 4)

    assert subresolution(a, a) == 0
    assert subresolution(a, b) == 1
    assert subresolution(a, b, p=2, n=2) == 1
    assert subresolution(a, b, p=3) is None
    b.shape = (3, 86, 171, 342, 4)
    assert subresolution(a, b, p=3) == 1
    b.shape = (3, 128, 256, 512, 2)
    assert subresolution(a, b) is None
    b.shape = (3, 64, 256, 512, 4)
    assert subresolution(a, b) is None
    b.shape = (3, 128, 64, 512, 4)
    assert subresolution(a, b) is None
    b.shape = (3, 128, 256, 1024, 4)
    assert subresolution(a, b) is None
    b.shape = (3, 32, 64, 128, 4)
    assert subresolution(a, b) == 3


@pytest.mark.skipif(IS_BE, reason=REASON)
def test_func_unpack_rgb():
    """Test unpack_rgb function."""
    data = struct.pack('BBBB', 0x21, 0x08, 0xFF, 0xFF)
    assert_array_equal(
        unpack_rgb(data, '<B', (5, 6, 5), rescale=False), [1, 1, 1, 31, 63, 31]
    )
    assert_array_equal(
        unpack_rgb(data, '<B', (5, 6, 5)), [8, 4, 8, 255, 255, 255]
    )
    assert_array_equal(
        unpack_rgb(data, '<B', (5, 5, 5)), [16, 8, 8, 255, 255, 255]
    )


def test_func_shaped_description():
    """Test shaped_description function."""
    descr = shaped_description((256, 256, 3), axes='YXS')
    assert json.loads(descr) == {'shape': [256, 256, 3], 'axes': 'YXS'}


def test_func_shaped_description_metadata():
    """Test shaped_description_metadata function."""
    assert shaped_description_metadata('shape=(256, 256, 3)') == {
        'shape': (256, 256, 3)
    }
    assert shaped_description_metadata(
        '{"shape": [256, 256, 3], "axes": "YXS"}'
    ) == {'shape': [256, 256, 3], 'axes': 'YXS'}


def test_func_imagej_shape():
    """Test imagej_shape function."""
    for k, v in (
        ((1, None), (1, 1, 1, 1, 1, 1)),
        ((3, None), (1, 1, 1, 1, 3, 1)),
        ((4, 3, None), (1, 1, 1, 4, 3, 1)),
        ((4, 3, True), (1, 1, 1, 1, 4, 3)),
        ((4, 4, None), (1, 1, 1, 4, 4, 1)),
        ((4, 4, True), (1, 1, 1, 1, 4, 4)),
        ((4, 3, 1, None), (1, 1, 1, 4, 3, 1)),
        ((4, 3, 2, None), (1, 1, 4, 3, 2, 1)),
        ((4, 3, 3, None), (1, 1, 1, 4, 3, 3)),
        ((4, 3, 4, None), (1, 1, 1, 4, 3, 4)),
        ((4, 3, 4, True), (1, 1, 1, 4, 3, 4)),
        ((4, 3, 4, False), (1, 1, 4, 3, 4, 1)),
        ((4, 3, 5, None), (1, 1, 4, 3, 5, 1)),
        ((3, 2, 1, 5, 4, None), (1, 3, 2, 1, 5, 4)),
        ((3, 2, 1, 4, 5, None), (3, 2, 1, 4, 5, 1)),
        ((1, 2, 3, 4, 5, None), (1, 2, 3, 4, 5, 1)),
        ((2, 3, 4, 5, 3, None), (1, 2, 3, 4, 5, 3)),
        ((2, 3, 4, 5, 3, True), (1, 2, 3, 4, 5, 3)),
        ((2, 3, 4, 5, 3, False), (2, 3, 4, 5, 3, 1)),
        ((1, 2, 3, 4, 5, 4, None), (1, 2, 3, 4, 5, 4)),
        ((6, 5, 4, 3, 2, 1, None), (6, 5, 4, 3, 2, 1)),
    ):
        assert imagej_shape(k[:-1], rgb=k[-1]) == v


def test_func_imagej_description():
    """Test imagej_description function."""
    expected = (
        'ImageJ=1.11a\nimages=510\nchannels=2\nslices=5\n'
        'frames=51\nhyperstack=true\nmode=grayscale\nloop=false\n'
    )
    assert imagej_description((51, 5, 2, 196, 171)) == expected
    assert imagej_description((51, 5, 2, 196, 171), axes='TZCYX') == expected
    expected = (
        'ImageJ=1.11a\nimages=2\nslices=2\nhyperstack=true\nmode=grayscale\n'
    )
    assert imagej_description((1, 2, 1, 196, 171)) == expected
    assert imagej_description((2, 196, 171), axes='ZYX') == expected
    expected = 'ImageJ=1.11a\nimages=1\nhyperstack=true\nmode=grayscale\n'
    assert imagej_description((196, 171)) == expected
    assert imagej_description((196, 171), axes='YX') == expected
    expected = 'ImageJ=1.11a\nimages=1\nhyperstack=true\n'
    assert imagej_description((196, 171, 3)) == expected
    assert imagej_description((196, 171, 3), axes='YXS') == expected

    # test colormapped parameter
    expected = 'ImageJ=1.11a\nimages=1\nhyperstack=false\n'
    assert imagej_description((196, 171), colormapped=True) == expected

    # test deprecated colormaped parameter
    with pytest.warns(DeprecationWarning, match='colormaped parameter'):
        assert imagej_description((196, 171), colormaped=True) == expected

    with pytest.raises(ValueError):
        imagej_description((196, 171, 3), axes='TYXS')
    with pytest.raises(ValueError):
        imagej_description((196, 171, 2), axes='TYXS')
    with pytest.raises(ValueError):
        imagej_description((3, 196, 171, 3), axes='ZTYX')


def test_func_imagej_description_metadata():
    """Test imagej_description_metadata function."""
    imagej_str = (
        'ImageJ=1.11a\nimages=510\nchannels=2\nslices=5\n'
        'frames=51\nhyperstack=true\nmode=grayscale\nloop=false\n'
    )
    imagej_dict = {
        'ImageJ': '1.11a',
        'images': 510,
        'channels': 2,
        'slices': 5,
        'frames': 51,
        'hyperstack': True,
        'mode': 'grayscale',
        'loop': False,
    }
    assert imagej_description_metadata(imagej_str) == imagej_dict


def test_func_pilatus_header_metadata():
    """Test pilatus_description_metadata function."""
    header = """
        # Detector: PILATUS 300K, 3-0101
        # 2011-07-22T17:33:22.529
        # Pixel_size 172e-6 m x 172e-6 m
        # Silicon sensor, thickness 0.000320 m
        # Exposure_time 0.0970000 s
        # Exposure_period 0.1000000 s
        # Tau = 383.8e-09 s
        # Count_cutoff 126367 counts
        # Threshold_setting: not set
        # Gain_setting: high gain (vrf = -0.150)
        # N_excluded_pixels = 19
        # Excluded_pixels: badpix_mask.tif
        # Flat_field: (nil)
        # Trim_file: p300k0101_E8048_T4024_vrf_m0p15.bin
        #  Beam_xy (243.12, 309.12) pixels
        # Image_path: /ramdisk/
            Invalid
        # Unknown 1 2 3 4 5""".strip().replace('            ', '')
    attr = pilatus_description_metadata(header)
    assert attr['Detector'] == 'PILATUS 300K 3-0101'
    assert attr['Pixel_size'] == (0.000172, 0.000172)
    assert attr['Silicon'] == 0.000320
    # self.assertEqual(attr['Threshold_setting'], float('nan'))
    assert attr['Beam_xy'] == (243.12, 309.12)
    assert attr['Unknown'] == '1 2 3 4 5'


def test_func_astrotiff_description_metadata(caplog):
    """Test astrotiff_description_metadata function."""
    assert (
        astrotiff_description_metadata("""
SIMPLE  =                    T / file does conform to FITS standard
COMMENT 1  First comment.
UNDEF   =                      / undefined
STRING  = 'a string'           / string
STRING1 = ''                   / null string
STRING2 = '    '               / empty string
STRING3 = ' string with / .'   / comment with / . and leading whitespace
STRING4 = 'string longer than  30 characters' / long string
COMMENT 2  Second comment, longer than 30 characters.
COMMENT 3  Third comment with /.
NOCOMMEN=                    1
TRUE    =                    T / True
FALSE   =                    F / False
INT     =                  123 / Integer
FLOAT   =  123.456789123456789 / Float
FLOAT2  =                 123. / Float
FLOAT3  = -123.4564890000E-001 / Scientific
CINT    =            (123, 45) / Complex integer
CFLT    =       (23.23, -45.7) / Complex float
JD      =   2457388.4562152778 / Julian date cannot be represented as float ?
UNIT    =                  123 / [unit] comment
CUSTOM  = '+12 34 56'          / [+dd mm ss] custom unit
DUPLICAT=                    1
DUPLICAT=                    2
INVALID1=                 None / invalid value
INVALID2= '                    / invalid string
END
""")
        == {
            'SIMPLE': True,
            'SIMPLE:COMMENT': 'file does conform to FITS standard',
            'COMMENT:0': '1  First comment.',
            'UNDEF': None,
            'UNDEF:COMMENT': 'undefined',
            'STRING': 'a string',
            'STRING:COMMENT': 'string',
            'STRING1': '',
            'STRING1:COMMENT': 'null string',
            'STRING2': '    ',
            'STRING2:COMMENT': 'empty string',
            'STRING3': ' string with / .',
            'STRING3:COMMENT': 'comment with / . and leading whitespace',
            'STRING4': 'string longer than  30 characters',
            'STRING4:COMMENT': 'long string',
            'COMMENT:1': '2  Second comment, longer than 30 characters.',
            'COMMENT:2': '3  Third comment with /.',
            'NOCOMMEN': 1,
            'TRUE': True,
            'TRUE:COMMENT': 'True',
            'FALSE': False,
            'FALSE:COMMENT': 'False',
            'INT': 123,
            'INT:COMMENT': 'Integer',
            'FLOAT': 123.45678912345679,
            'FLOAT:COMMENT': 'Float',
            'FLOAT2': 123.0,
            'FLOAT2:COMMENT': 'Float',
            'FLOAT3': -12.3456489,
            'FLOAT3:COMMENT': 'Scientific',
            'CINT': (123, 45),
            'CINT:COMMENT': 'Complex integer',
            'CFLT': (23.23, -45.7),
            'CFLT:COMMENT': 'Complex float',
            'JD': 2457388.456215278,
            'JD:COMMENT': 'Julian date cannot be represented as float ?',
            'UNIT': 123,
            'UNIT:COMMENT': '[unit] comment',
            'UNIT:UNIT': 'unit',
            'CUSTOM': '+12 34 56',
            'CUSTOM:COMMENT': '[+dd mm ss] custom unit',
            'CUSTOM:UNIT': '+dd mm ss',
            'DUPLICAT': 2,
            'END:0': '',
        }
    )
    assert 'DUPLICAT: duplicate key' in caplog.text
    assert 'INVALID1: invalid value' in caplog.text
    assert 'INVALID2: invalid string' in caplog.text


def test_eer_xml_metadata():
    """Test eer_xml_metadata function."""
    meta = eer_xml_metadata("""
    <metadata>
    <item name="acquisitionID">TEMApps_20220718_164035</item>
    <item name="cameraName">EF-Falcon</item>
    <item name="commercialName">Falcon 4</item>
    <item name="eerGainReference">20220711_104630_EER_GainReference.gain</item>
    <item name="exposureTime" unit="s">3.2000000000000002</item>
    <item name="meanDoseRate" unit="e/pixel/s">7.5698134278181328</item>
    <item name="numberOfFrames">770</item>
    <item name="sensorImageHeight" unit="pixel">4096</item>
    <item name="sensorImageWidth" unit="pixel">4096</item>
    <item name="sensorPixelSize.height" unit="m">6.42429665e-10</item>
    <item name="sensorPixelSize.width" unit="m">6.42429665e-10</item>
    <item name="serialNumber">20-44-A11-G4H</item>
    <item name="timestamp">2022-07-18T15:40:36.141-08:00</item>
    <item name="totalDose" unit="e/pixel">24.198384735639088</item>
    </metadata>
        """)
    assert meta['acquisitionID'] == 'TEMApps_20220718_164035'
    assert meta['cameraName'] == 'EF-Falcon'
    assert meta['exposureTime'] == 3.2
    assert meta['exposureTime.unit'] == 's'
    assert meta['numberOfFrames'] == 770
    assert meta['timestamp'] == datetime.datetime(
        2022,
        7,
        18,
        15,
        40,
        36,
        141000,
        tzinfo=datetime.timezone(datetime.timedelta(days=-1, seconds=57600)),
    )
    assert meta['totalDose'] == 24.198384735639088
    assert meta['totalDose.unit'] == 'e/pixel'

    meta = eer_xml_metadata("""
    <metadata>
    <item name="decompressionAlgorithmVersion">V1</item>
    <item name="dose" unit="e/pixel">0.023866</item>
    <item name="frameID">10</item>
    <item name="nrOfSubPixelPerDirection">2</item>
    <item name="orientation">2</item>
    <item name="pixelFormat">EerEncoding</item>
    <item name="rleCodeLength">7</item>
    <item name="timestamp">2022-07-18T15:40:36.182-08:00</item>
    </metadata>
        """)
    assert meta['decompressionAlgorithmVersion'] == 'V1'
    assert meta['dose'] == 0.023866
    assert meta['dose.unit'] == 'e/pixel'
    assert meta['frameID'] == 10

    meta = eer_xml_metadata("""
    <metadata>
    <item name="binning">1</item>
    <item name="checksum">Valid</item>
    <item name="countsToElectrons">0.013037</item>
    <item name="darkCorrection">Yes</item>
    <item name="driftCorrectionInformation.clipping">No</item>
    <item name="driftCorrectionInformation.confidence">0.000000</item>
    <item name="driftCorrectionInformation.driftCorrected">No</item>
    <item name="driftCorrectionInformation.vectorXCoordinate">0.000000</item>
    <item name="driftCorrectionInformation.vectorYCoordinate">0.000000</item>
    <item name="exposureTime" unit="s">0.997208</item>
    <item name="gainCorrection">Yes</item>
    <item name="meanPixelValue">144.216678</item>
    <item name="numberOfFrames">238</item>
    <item name="pixelValueToCameraCounts">1</item>
    <item name="roi.bottom">2048</item>
    <item name="roi.left">0</item>
    <item name="roi.right">2048</item>
    <item name="roi.top">0</item>
    <item name="timestamp">2023-05-23T16:18:38.589514+01:00</item>
    </metadata>
        """)
    assert meta['binning'] == 1
    assert meta['checksum'] == 'Valid'
    assert meta['countsToElectrons'] == 0.013037
    assert meta['darkCorrection'] is True
    assert meta['driftCorrectionInformation.clipping'] is False
    assert meta['driftCorrectionInformation.vectorXCoordinate'] == 0.0


def test_func_matlabstr2py():
    """Test matlabstr2py function."""
    assert matlabstr2py('1') == 1
    assert matlabstr2py(
        "['x y z' true false; 1 2.0 -3e4; Inf Inf @class;[1;2;3][1 2] 3]"
    ) == [
        ['x y z', True, False],
        [1, 2.0, -30000.0],
        [float('inf'), float('inf'), '@class'],
        [[[1], [2], [3]], [1, 2], 3],
    ]

    assert matlabstr2py(
        "SI.hChannels.channelType = {'stripe' 'stripe'}\n"
        'SI.hChannels.channelsActive = 2'
    )['SI.hChannels.channelType'] == ['stripe', 'stripe']

    p = matlabstr2py("""
        true = true
        false = false
        True = True
        False = False
        Int = 10
        Float = 3.14
        Float.E = 314.0e-02
        Float.NaN = nan
        Float.Inf = inf
        String = 'string'
        String.Empty = ''
        String.Array = ['ab']
        Array = [1 2]
        Array.2D = [1;2]
        Array.Empty = [[]]
        Transform = [1 0 0;0 1 0;0 0 1]
        Zeros = zeros(1,1)
        Zeros.Empty = zeros(1,0)
        Ones = ones(1,0)
        Filename = C:\\Users\\scanimage.cfg
        Cell = {'' ''}
        Class = @class
        StructObject = <nonscalar struct/object>
        Unknown = unknown

        % Comment
        """)
    assert p['Array'] == [1, 2]
    assert p['Array.2D'] == [[1], [2]]
    assert p['Array.Empty'] == []
    assert p['Cell'] == ['', '']
    assert p['Class'] == '@class'
    assert p['False'] is False
    assert p['Filename'] == 'C:\\Users\\scanimage.cfg'
    assert p['Float'] == 3.14
    assert p['Float.E'] == 3.14
    assert p['Float.Inf'] == float('inf')
    # self.assertEqual(p['Float.NaN'], float('nan'))  # can't compare NaN
    assert p['Int'] == 10
    assert p['StructObject'] == '<nonscalar struct/object>'
    assert p['Ones'] == [[]]
    assert p['String'] == 'string'
    assert p['String.Array'] == 'ab'
    assert p['String.Empty'] == ''
    assert p['Transform'] == [[1, 0, 0], [0, 1, 0], [0, 0, 1]]
    assert p['True'] is True
    assert p['Unknown'] == 'unknown'
    assert p['Zeros'] == [[0.0]]
    assert p['Zeros.Empty'] == [[]]
    assert p['false'] is False
    assert p['true'] is True


def test_func_strptime():
    """Test strptime function."""
    now = datetime.datetime.now().replace(microsecond=0)
    assert strptime(now.isoformat()) == now
    assert strptime(now.strftime('%Y:%m:%d %H:%M:%S')) == now
    assert strptime(now.strftime('%Y%m%d %H:%M:%S.%f')) == now


def test_func_hexdump():
    """Test hexdump function."""
    # test hexdump function
    data = binascii.unhexlify(
        '49492a00080000000e00fe0004000100'
        '00000000000000010400010000000001'
        '00000101040001000000000100000201'
        '030001000000200000000301030001'
    )
    # one line
    assert hexdump(data[:16]) == (
        '49 49 2a 00 08 00 00 00 0e 00 fe 00 04 00 01 00 II*.............'
    )
    # height=1
    assert hexdump(data, width=64, height=1) == (
        '49 49 2a 00 08 00 00 00 0e 00 fe 00 04 00 01 00 II*.............'
    )
    # all lines
    assert hexdump(data) == (
        '00: 49 49 2a 00 08 00 00 00 0e 00 fe 00 04 00 01 00 '
        'II*.............\n'
        '10: 00 00 00 00 00 00 00 01 04 00 01 00 00 00 00 01 '
        '................\n'
        '20: 00 00 01 01 04 00 01 00 00 00 00 01 00 00 02 01 '
        '................\n'
        '30: 03 00 01 00 00 00 20 00 00 00 03 01 03 00 01    '
        '...... ........'
    )
    # skip center
    assert hexdump(data, height=3, snipat=0.5) == (
        '00: 49 49 2a 00 08 00 00 00 0e 00 fe 00 04 00 01 00 '
        'II*.............\n'
        '                          ...\n'
        '30: 03 00 01 00 00 00 20 00 00 00 03 01 03 00 01    '
        '...... ........'
    )
    # skip start
    assert hexdump(data, height=3, snipat=0) == (
        '10: 00 00 00 00 00 00 00 01 04 00 01 00 00 00 00 01 '
        '................\n'
        '20: 00 00 01 01 04 00 01 00 00 00 00 01 00 00 02 01 '
        '................\n'
        '30: 03 00 01 00 00 00 20 00 00 00 03 01 03 00 01    '
        '...... ........'
    )
    # skip end
    assert hexdump(data, height=3, snipat=1) == (
        '00: 49 49 2a 00 08 00 00 00 0e 00 fe 00 04 00 01 00 '
        'II*.............\n'
        '10: 00 00 00 00 00 00 00 01 04 00 01 00 00 00 00 01 '
        '................\n'
        '20: 00 00 01 01 04 00 01 00 00 00 00 01 00 00 02 01 '
        '................'
    )


def test_func_asbool():
    """Test asbool function."""
    for true in ('TRUE', ' True ', 'true '):
        assert asbool(true)
        assert asbool(true.encode())
    for false in ('FALSE', ' False ', 'false '):
        assert not asbool(false)
        assert not asbool(false.encode())
    assert asbool('ON', ['on'], ['off'])
    assert asbool('ON', 'on', 'off')
    with pytest.raises(TypeError):
        assert asbool('Yes')
    with pytest.raises(TypeError):
        assert asbool('True', ['on'], ['off'])


def test_func_snipstr():
    """Test snipstr function."""
    # cut middle
    assert snipstr('abc', 3, ellipsis='...') == 'abc'
    assert snipstr('abc', 3, ellipsis='....') == 'abc'
    assert snipstr('abcdefg', 4, ellipsis='') == 'abcd'
    assert snipstr('abcdefg', 4, ellipsis=None) == 'abc…'
    assert snipstr('abcdefghijklmnop', 8, ellipsis=None) == 'abcd…nop'
    assert snipstr('abcdefghijklmnop', 9, ellipsis=None) == 'abcd…mnop'
    assert snipstr('abcdefghijklmnop', 8, ellipsis='..') == 'abc..nop'
    assert snipstr('abcdefghijklmnop', 8, ellipsis='....') == 'ab....op'
    assert snipstr('abcdefghijklmnop', 8, ellipsis='......') == 'ab......'
    # cut right
    assert snipstr('abc', 3, snipat=1, ellipsis='...') == 'abc'
    assert snipstr('abc', 3, snipat=1, ellipsis='....') == 'abc'
    assert snipstr('abcdefg', 4, snipat=1, ellipsis='') == 'abcd'
    assert snipstr('abcdefg', 4, snipat=1, ellipsis=None) == 'abc…'
    assert (
        snipstr('abcdefghijklmnop', 8, snipat=1, ellipsis=None) == 'abcdefg…'
    )
    assert (
        snipstr('abcdefghijklmnop', 9, snipat=1, ellipsis=None) == 'abcdefgh…'
    )
    assert (
        snipstr('abcdefghijklmnop', 8, snipat=1, ellipsis='..') == 'abcdef..'
    )
    assert (
        snipstr('abcdefghijklmnop', 8, snipat=1, ellipsis='....') == 'abcd....'
    )
    assert (
        snipstr('abcdefghijklmnop', 8, snipat=1, ellipsis='......')
        == 'ab......'
    )
    # cut left
    assert snipstr('abc', 3, snipat=0, ellipsis='...') == 'abc'
    assert snipstr('abc', 3, snipat=0, ellipsis='....') == 'abc'
    assert snipstr('abcdefg', 4, snipat=0, ellipsis='') == 'defg'
    assert snipstr('abcdefg', 4, snipat=0, ellipsis=None) == '…efg'
    assert (
        snipstr('abcdefghijklmnop', 8, snipat=0, ellipsis=None) == '…jklmnop'
    )
    assert (
        snipstr('abcdefghijklmnop', 9, snipat=0, ellipsis=None) == '…ijklmnop'
    )
    assert (
        snipstr('abcdefghijklmnop', 8, snipat=0, ellipsis='..') == '..klmnop'
    )
    assert (
        snipstr('abcdefghijklmnop', 8, snipat=0, ellipsis='....') == '....mnop'
    )
    assert (
        snipstr('abcdefghijklmnop', 8, snipat=0, ellipsis='......')
        == '......op'
    )


def test_func_pformat_printable_bytes():
    """Test pformat function with printable bytes."""
    value = (
        b'0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRST'
        b'UVWXYZ!"#$%&\'()*+,-./:;<=>?@[\\]^_`{|}~ \t\n\r\x0b\x0c'
    )

    assert pformat(value, height=1, width=60, linewidth=None) == (
        '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWX'
    )

    assert pformat(value, height=8, width=60, linewidth=None) == r"""
0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWX
""".strip()
    # YZ!"#$%&'()*+,-./:;<=>?@[\]^_`{|}~


def test_func_pformat_printable_unicode():
    """Test pformat function with printable unicode."""
    value = (
        '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRST'
        'UVWXYZ!"#$%&\'()*+,-./:;<=>?@[\\]^_`{|}~ \t\n\r\x0b\x0c'
    )

    assert pformat(value, height=1, width=60, linewidth=None) == (
        '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWX'
    )

    assert pformat(value, height=8, width=60, linewidth=None) == r"""
0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWX
""".strip()
    # YZ!"#$%&'()*+,-./:;<=>?@[\]^_`{|}~


def test_func_pformat_hexdump():
    """Test pformat function with unprintable bytes."""
    value = binascii.unhexlify(
        '49492a00080000000e00fe0004000100'
        '00000000000000010400010000000001'
        '00000101040001000000000100000201'
        '03000100000020000000030103000100'
    )

    assert pformat(value, height=1, width=60, linewidth=None) == (
        '49 49 2a 00 08 00 00 00 0e 00 fe 00 04 00 01 II*............'
    )

    assert pformat(value, height=8, width=70, linewidth=None) == """
00: 49 49 2a 00 08 00 00 00 0e 00 fe 00 04 00 01 00 II*.............
10: 00 00 00 00 00 00 00 01 04 00 01 00 00 00 00 01 ................
20: 00 00 01 01 04 00 01 00 00 00 00 01 00 00 02 01 ................
30: 03 00 01 00 00 00 20 00 00 00 03 01 03 00 01 00 ...... .........
""".strip()


def test_func_pformat_dict():
    """Test pformat function with dict."""
    value = {
        'GTCitationGeoKey': 'WGS 84 / UTM zone 29N',
        'GTModelTypeGeoKey': 1,
        'GTRasterTypeGeoKey': 1,
        'KeyDirectoryVersion': 1,
        'KeyRevision': 1,
        'KeyRevisionMinor': 2,
        'ModelTransformation': numpy.array(
            [
                [6.00000e01, 0.00000e00, 0.00000e00, 6.00000e05],
                [0.00000e00, -6.00000e01, 0.00000e00, 5.90004e06],
                [0.00000e00, 0.00000e00, 0.00000e00, 0.00000e00],
                [0.00000e00, 0.00000e00, 0.00000e00, 1.00000e00],
            ]
        ),
        'PCSCitationGeoKey': 'WGS 84 / UTM zone 29N',
        'ProjectedCSTypeGeoKey': 32629,
    }

    assert pformat(value, height=1, width=60, linewidth=None) == (
        "{'GTCitationGeoKey': 'WGS 84 / UTM zone 29N', 'GTModelTypeGe"
    )

    assert pformat(value, height=8, width=60, linewidth=None) == (
        """{'GTCitationGeoKey': 'WGS 84 / UTM zone 29N',
 'GTModelTypeGeoKey': 1,
 'GTRasterTypeGeoKey': 1,
 'KeyDirectoryVersion': 1,
...
       [      0.,       0.,       0.,       1.]]),
 'PCSCitationGeoKey': 'WGS 84 / UTM zone 29N',
 'ProjectedCSTypeGeoKey': 32629}"""
    )


def test_func_pformat_list():
    """Test pformat function with list."""
    value = (
        60.0,
        0.0,
        0.0,
        600000.0,
        0.0,
        -60.0,
        0.0,
        5900040.0,
        60.0,
        0.0,
        0.0,
        600000.0,
        0.0,
        -60.0,
        0.0,
        5900040.0,
    )

    assert pformat(value, height=1, width=60, linewidth=None) == (
        '(60.0, 0.0, 0.0, 600000.0, 0.0, -60.0, 0.0, 5900040.0, 60.0,'
    )

    assert pformat(value, height=8, width=60, linewidth=None) == (
        '(60.0, 0.0, 0.0, 600000.0, 0.0, -60.0, 0.0, 5900040.0, 60.0,\n'
        ' 0.0, 0.0, 600000.0, 0.0, -60.0, 0.0, 5900040.0)'
    )


def test_func_pformat_numpy():
    """Test pformat function with numpy array."""
    value = numpy.array(
        (
            60.0,
            0.0,
            0.0,
            600000.0,
            0.0,
            -60.0,
            0.0,
            5900040.0,
            60.0,
            0.0,
            0.0,
            600000.0,
            0.0,
            -60.0,
            0.0,
            5900040.0,
        )
    )

    assert pformat(value, height=1, width=60, linewidth=None) == (
        'array([60., 0., 0., 600000., 0., -60., 0., 5900040., 60., 0.'
    )

    assert pformat(value, height=8, width=60, linewidth=None) == (
        """array([     60.,       0.,       0.,  600000.,       0.,
           -60.,       0., 5900040.,      60.,       0.,
             0.,  600000.,       0.,     -60.,       0.,
       5900040.])"""
    )


@pytest.mark.skipif(not IS_WIN, reason='not reliable on Linux')
def test_func_pformat_xml():
    """Test pformat function with XML."""
    value = """<?xml version="1.0" encoding="ISO-8859-1" ?>
<Dimap_Document name="band2.dim">
  <Metadata_Id>
    <METADATA_FORMAT version="2.12.1">DIMAP</METADATA_FORMAT>
    <METADATA_PROFILE>BEAM-DATAMODEL-V1</METADATA_PROFILE>
  </Metadata_Id>
  <Image_Interpretation>
    <Spectral_Band_Info>
      <BAND_INDEX>0</BAND_INDEX>
    </Spectral_Band_Info>
  </Image_Interpretation>
</Dimap_Document>"""

    assert pformat(value, height=1, width=60, linewidth=None) == (
        '<?xml version="1.0" encoding="ISO-8859-1" ?> <Dimap_Document'
    )

    assert pformat(value, height=8, width=60, linewidth=None) == (
        """<?xml version='1.0' encoding='ISO-8859-1'?>
<Dimap_Document name="band2.dim">
 <Metadata_Id>
  <METADATA_FORMAT version="2.12.1">DIMAP</METADATA_FORMAT>
...
  </Spectral_Band_Info>
 </Image_Interpretation>
</Dimap_Document>"""
    )


@pytest.mark.skipif(SKIP_FILE or SKIP_LARGE, reason=REASON)
def test_func_lsm2bin():
    """Test lsm2bin function."""
    # Convert LSM to BIN
    filename = _file('lsm/Twoareas_Zstacks54slices_3umintervals_5cycles.lsm')
    # filename = _file(
    #     'LSM/fish01-wt-t01-10_ForTest-20zplanes10timepoints.lsm')
    lsm2bin(filename, '', verbose=True)


def test_func_tiffcomment():
    """Test tiffcomment function."""
    data = random_data(numpy.uint8, (33, 31, 3))
    with TempFileName('func_tiffcomment') as filename:
        comment = 'A comment'
        imwrite(
            filename,
            data,
            photometric=PHOTOMETRIC.RGB,
            description=comment,
            metadata=None,
        )
        assert comment == tiffcomment(filename)
        comment = 'changed comment'
        tiffcomment(filename, comment)
        assert comment == tiffcomment(filename)
        assert_valid_tiff(filename)


def test_func_create_output():
    """Test create_output function."""
    shape = (16, 17)
    dtype = numpy.uint16

    # None
    a = create_output(None, shape, dtype)
    assert a.shape == shape
    assert a.dtype == dtype
    # assert_array_equal(a, numpy.zeros(shape, dtype))

    # fillvalue
    a = create_output(None, shape, dtype, fillvalue=42)
    assert_array_equal(a, numpy.full(shape, 42, dtype))

    # existing array
    b = create_output(a, a.shape, a.dtype)
    assert a is b  # a is b.base
    assert_array_equal(a, numpy.full(shape, 42, dtype))

    # existing array with fillvalue
    b = create_output(a, a.shape, a.dtype, fillvalue=1)
    assert a is b  # a is b.base
    assert_array_equal(a, numpy.ones(shape, dtype))

    # reshapeable
    b = create_output(a, (1, 17, 16), a.dtype)
    assert a is b  # a is b.base
    assert b.shape == (1, 17, 16)

    # castable dtype
    b = create_output(a, shape, numpy.uint8)
    assert a is b  # a is b.base
    assert b.dtype == numpy.uint16

    # not contiguous
    a = numpy.ones((32, 31), dtype)
    b = create_output(a[: shape[0], : shape[1]], shape, dtype, fillvalue=0)
    assert b.shape == shape
    assert a[shape[0], shape[1]] == 1
    assert_array_equal(a[: shape[0], : shape[1]], numpy.zeros(shape, dtype))

    # 'memmap'
    a = create_output('memmap', shape, dtype)
    assert isinstance(a, numpy.memmap)
    del a

    # 'memmap:tempdir'
    a = create_output(f'memmap:{os.path.abspath(TEMP_DIR)}', shape, dtype)
    assert isinstance(a, numpy.memmap)
    del a

    # filename
    with TempFileName('nopages') as filename:
        a = create_output(filename, shape, dtype)
        del a

    # wrong shape
    a = create_output(None, shape, dtype)
    with pytest.raises(ValueError):
        create_output(a, (16, 16), dtype)

    # wrong dtype
    with pytest.raises(ValueError):
        create_output(a, shape, numpy.uint32)


def test_func_reorient():
    """Test reoirient func."""
    data = numpy.zeros((2, 3, 31, 33, 3), numpy.uint8)
    for orientation in range(1, 9):
        reorient(data, orientation)  # TODO: assert result


@pytest.mark.parametrize('key', [None, 0, 3, 'series'])
@pytest.mark.parametrize('out', [None, 'empty', 'memmap', 'dir', 'name'])
def test_func_create_output_asarray(out, key):
    """Test create_output function in context of asarray."""
    data = random_data(numpy.uint16, (5, 219, 301))

    with TempFileName(f'func_out_{key}_{out}') as filename:
        imwrite(filename, data)
        # assert file
        with TiffFile(filename) as tif:
            tif.pages.useframes = True
            tif.pages._load()

            if key is None:
                # default
                obj = tif
                dat = data
            elif key == 'series':
                # series
                obj = tif.series[0]
                dat = data
            else:
                # single page/frame
                obj = tif.pages[key]
                dat = data[key]
                if key == 0:
                    assert isinstance(obj, TiffPage)
                else:
                    assert isinstance(obj, TiffFrame)

            if out is None:
                # new array
                image = obj.asarray(out=None)
                assert_array_equal(dat, image)
                del image
            elif out == 'empty':
                # existing array
                image = numpy.empty_like(dat)
                obj.asarray(out=image)
                assert_array_equal(dat, image)
                del image
            elif out == 'memmap':
                # memmap in temp dir
                image = obj.asarray(out='memmap')
                assert isinstance(image, numpy.memmap)
                assert_array_equal(dat, image)
                del image
            elif out == 'dir':
                # memmap in specified dir
                tempdir = os.path.dirname(filename)
                image = obj.asarray(out=f'memmap:{tempdir}')
                assert isinstance(image, numpy.memmap)
                assert_array_equal(dat, image)
                del image
            elif out == 'name':
                # memmap in specified file
                with TempFileName(
                    f'out_{key}_{out}', ext='.memmap'
                ) as fileout:
                    image = obj.asarray(out=fileout)
                    assert isinstance(image, numpy.memmap)
                    assert_array_equal(dat, image)
                    del image


def test_func_bitorder_decode():
    """Test bitorder_decode function."""
    from tifffile._imagecodecs import bitorder_decode

    # bytes
    assert bitorder_decode(b'\x01\x64') == b'\x80&'
    assert bitorder_decode(b'\x01\x00\x9a\x02') == b'\x80\x00Y@'

    # numpy array
    data = numpy.array([1, 666], dtype=numpy.uint16)
    reverse = numpy.array([128, 16473], dtype=numpy.uint16)
    # return new array
    assert_array_equal(bitorder_decode(data), reverse)
    # array view not supported
    data = numpy.array(
        [
            [1, 666, 1431655765, 62],
            [2, 667, 2863311530, 32],
            [3, 668, 1431655765, 30],
        ],
        dtype=numpy.uint32,
    )
    reverse = numpy.array(
        [
            [1, 666, 1431655765, 62],
            [2, 16601, 1431655765, 32],
            [3, 16441, 2863311530, 30],
        ],
        dtype=numpy.uint32,
    )
    # if int(numpy.__version__.split('.')[1]) < 23:
    #     with pytest.raises(NotImplementedError):
    #         bitorder_decode(data[1:, 1:3])
    # else:
    assert_array_equal(bitorder_decode(data[1:, 1:3]), reverse[1:, 1:3])


@pytest.mark.parametrize(
    'kind',
    ['u1', 'u2', 'u4', 'u8', 'i1', 'i2', 'i4', 'i8', 'f4', 'f8', 'B'],
)
@pytest.mark.parametrize('byteorder', ['>', '<'])
def test_func_delta_codec(byteorder, kind):
    """Test delta codec functions."""
    from tifffile._imagecodecs import delta_decode, delta_encode

    # if byteorder == '>' and numpy.dtype(kind).itemsize == 1:
    #     pytest.skip('duplicate test')

    if kind[0] in 'iuB':
        low = numpy.iinfo(kind).min
        high = numpy.iinfo(kind).max
        data = RNG.integers(
            low,
            high,
            size=33 * 31 * 3,
            dtype=kind,
        ).reshape((33, 31, 3))
    else:
        # floating point
        if byteorder == '>':
            pytest.xfail('requires imagecodecs')
        low, high = -1e5, 1e5
        data = RNG.integers(low, high, size=33 * 31 * 3, dtype='i4').reshape(
            (33, 31, 3)
        )
    data = data.astype(byteorder + kind)

    data[16, 14] = [0, 0, 0]
    data[16, 15] = [low, high, low]
    data[16, 16] = [high, low, high]
    data[16, 17] = [low, high, low]
    data[16, 18] = [high, low, high]
    data[16, 19] = [0, 0, 0]

    if kind == 'B':
        # data = data.reshape(-1)
        data = data.tobytes()
        assert delta_decode(delta_encode(data)) == data
    else:
        encoded = delta_encode(data, axis=-2)
        assert encoded.dtype.byteorder == data.dtype.byteorder
        assert_array_equal(data, delta_decode(encoded, axis=-2))
        if not SKIP_CODECS:
            assert_array_equal(
                encoded, imagecodecs.delta_encode(data, axis=-2)
            )


@pytest.mark.parametrize('length', [0, 2, 31 * 33 * 3])
@pytest.mark.parametrize('codec', ['lzma', 'zlib'])
def test_func_zlib_lzma_codecs(codec, length):
    """Test zlib and lzma codec functions."""
    if codec == 'zlib':
        from tifffile._imagecodecs import zlib_decode, zlib_encode

        encode = zlib_encode
        decode = zlib_decode
    elif codec == 'lzma':
        from tifffile._imagecodecs import lzma_decode, lzma_encode

        encode = lzma_encode
        decode = lzma_decode

    if length:
        data = RNG.integers(255, size=length, dtype=numpy.uint8)
        assert decode(encode(data)) == data.tobytes()
    else:
        data = b''
        assert decode(encode(data)) == data


@pytest.mark.parametrize('length', [0, 2, 31 * 33 * 3])
def test_func_zstd_codecs(length):
    """Test zstd codec functions."""
    _ = pytest.importorskip('compression.zstd')

    from tifffile._imagecodecs import zstd_decode, zstd_encode

    if length:
        data = RNG.integers(255, size=length, dtype=numpy.uint8)
        assert zstd_decode(zstd_encode(data)) == data.tobytes()
    else:
        data = b''
        assert zstd_decode(zstd_encode(data)) == data


PACKBITS_DATA = [
    ([], b''),
    ([0] * 1, b'\x00\x00'),  # literal
    ([0] * 2, b'\xff\x00'),  # replicate
    ([0] * 3, b'\xfe\x00'),
    ([0] * 64, b'\xc1\x00'),
    ([0] * 127, b'\x82\x00'),
    ([0] * 128, b'\x81\x00'),  # max replicate
    ([0] * 129, b'\x81\x00\x00\x00'),
    ([0] * 130, b'\x81\x00\xff\x00'),
    ([0] * 128 * 3, b'\x81\x00' * 3),
    ([255] * 1, b'\x00\xff'),  # literal
    ([255] * 2, b'\xff\xff'),  # replicate
    ([0, 1], b'\x01\x00\x01'),
    ([0, 1, 2], b'\x02\x00\x01\x02'),
    ([0, 1] * 32, b'\x3f' + b'\x00\x01' * 32),
    ([0, 1] * 63 + [2], b'\x7e' + b'\x00\x01' * 63 + b'\x02'),
    ([0, 1] * 64, b'\x7f' + b'\x00\x01' * 64),  # max literal
    ([0, 1] * 64 + [2], b'\x7f' + b'\x00\x01' * 64 + b'\x00\x02'),
    ([0, 1] * 64 * 5, (b'\x7f' + b'\x00\x01' * 64) * 5),
    ([0, 1, 1], b'\x00\x00\xff\x01'),  # or b'\x02\x00\x01\x01'
    ([0] + [1] * 128, b'\x00\x00\x81\x01'),  # or b'\x01\x00\x01\x82\x01'
    ([0] + [1] * 129, b'\x00\x00\x81\x01\x00\x01'),  # b'\x01\x00\x01\x81\x01'
    ([0, 1] * 64 + [2] * 2, b'\x7f' + b'\x00\x01' * 64 + b'\xff\x02'),
    ([0, 1] * 64 + [2] * 128, b'\x7f' + b'\x00\x01' * 64 + b'\x81\x02'),
    ([0, 0, 1], b'\x02\x00\x00\x01'),  # or b'\xff\x00\x00\x01'
    ([0, 0] + [1, 2] * 64, b'\xff\x00\x7f' + b'\x01\x02' * 64),
    ([0] * 128 + [1], b'\x81\x00\x00\x01'),
    ([0] * 128 + [1, 2] * 64, b'\x81\x00\x7f' + b'\x01\x02' * 64),
    (
        b'\xaa\xaa\xaa\x80\x00\x2a\xaa\xaa\xaa\xaa\x80\x00'
        b'\x2a\x22\xaa\xaa\xaa\xaa\xaa\xaa\xaa\xaa\xaa\xaa',
        b'\xfe\xaa\x02\x80\x00\x2a\xfd\xaa\x03\x80\x00\x2a\x22\xf7\xaa',
    ),
]


@pytest.mark.parametrize('data', range(len(PACKBITS_DATA)))
def test_func_packbits_decode(data):
    """Test packbits_decode function."""
    from tifffile._imagecodecs import packbits_decode

    uncompressed, compressed = PACKBITS_DATA[data]
    assert packbits_decode(compressed) == bytes(uncompressed)


def test_func_packints_decode():
    """Test packints_decode function."""
    from tifffile._imagecodecs import packints_decode

    decoded = packints_decode(b'', 'B', 1)
    assert len(decoded) == 0
    decoded = packints_decode(b'a', 'B', 1)
    assert tuple(decoded) == (0, 1, 1, 0, 0, 0, 0, 1)
    with pytest.raises(NotImplementedError):
        decoded = packints_decode(b'ab', 'B', 2)
        # assert tuple(decoded) == (1, 2, 0, 1, 1, 2, 0, 2)
    with pytest.raises(NotImplementedError):
        decoded = packints_decode(b'abcd', 'B', 3)
        # assert tuple(decoded) == (3, 0, 2, 6, 1, 1, 4, 3, 3, 1)


def test_func_check_shape():
    """Test check_shape function."""
    assert check_shape((10, 10, 4), (10, 10, 4))
    assert check_shape((10, 10, 4), (1, 1, 10, 10, 4))
    assert not check_shape((4, 10, 10), (10, 10, 4))
    assert not check_shape((10, 10, 4), (4, 10, 10))
    assert not check_shape((10, 10, 4), (1, 1, 4, 10, 10))

    assert check_shape((0,), (0, 0))
    assert check_shape((0, 0), (0,))

    assert check_shape((4,), (4,))
    assert check_shape((1, 4), (4,))
    assert check_shape((1, 4), (4, 1))
    assert check_shape((4, 1), (4, 1))
    assert check_shape((4, 1), (1, 4, 1))
    assert check_shape((4, 5), (4, 5, 1))
    assert check_shape((4, 5), (4, 5, 1, 1))
    assert check_shape((4, 5), (1, 4, 5, 1))

    assert not check_shape((1,), (0, 0))
    assert not check_shape((1, 0), (1,))
    assert not check_shape((4, 1), (1,))
    assert not check_shape((4, 1), (2,))
    assert not check_shape((4, 1), (4,))
    assert not check_shape((4, 1), (2, 2))
    assert not check_shape((4, 1), (2, 1))
    assert not check_shape((3, 4, 5), (4, 5, 3))


###############################################################################

# Test FileHandle class

FILEHANDLE_NAME = _file('tifffile/test_FileHandle.bin')
FILEHANDLE_SIZE = 7937381
FILEHANDLE_OFFSET = 333
FILEHANDLE_LENGTH = 7937381 - 666


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def create_filehandle_file():
    """Write test_FileHandle.bin file."""
    # array start 999
    # array end 1254
    # recarray start 2253
    # recarray end 6078
    # tiff start 7077
    # tiff end 12821
    # mm offset = 13820
    # mm size = 7936382
    with open(FILEHANDLE_NAME, 'wb') as fh:
        # buffer
        numpy.ones(999, dtype=numpy.uint8).tofile(fh)
        # array
        print('array start', fh.tell())
        numpy.arange(255, dtype=numpy.uint8).tofile(fh)
        print('array end', fh.tell())
        # buffer
        numpy.ones(999, dtype=numpy.uint8).tofile(fh)
        # recarray
        print('recarray start', fh.tell())
        a = numpy.recarray(
            (255, 3), dtype=[('x', numpy.float32), ('y', numpy.uint8)]
        )
        for i in range(3):
            a[:, i].x = numpy.arange(255, dtype=numpy.float32)
            a[:, i].y = numpy.arange(255, dtype=numpy.uint8)
        a.tofile(fh)
        print('recarray end', fh.tell())
        # buffer
        numpy.ones(999, dtype=numpy.uint8).tofile(fh)
        # tiff
        print('tiff start', fh.tell())
        with open('data/tifffile/generic_series.tif', 'rb') as tif:
            fh.write(tif.read())
        print('tiff end', fh.tell())
        # buffer
        numpy.ones(999, dtype=numpy.uint8).tofile(fh)
        # micromanager
        print('micromanager start', fh.tell())
        with open('data/tifffile/micromanager.tif', 'rb') as tif:
            fh.write(tif.read())
        print('micromanager end', fh.tell())
        # buffer
        numpy.ones(999, dtype=numpy.uint8).tofile(fh)


def assert_filehandle(fh, offset=0):
    """Assert filehandle can read test_FileHandle.bin."""
    assert__repr__(fh)
    assert not fh.closed
    assert fh.open() is None  # open an open file
    fh.close()
    # assert fh.closed
    assert fh.open() is None  # open a closed file
    size = FILEHANDLE_SIZE - 2 * offset
    pad = 999 - offset
    assert fh.size == size
    assert fh.tell() == 0
    assert fh.read(4) == b'\x01\x01\x01\x01'
    fh.seek(pad - 4)
    assert fh.tell() == pad - 4
    assert fh.read(4) == b'\x01\x01\x01\x01'
    fh.seek(-4, whence=1)
    assert fh.tell() == pad - 4
    assert fh.read(4) == b'\x01\x01\x01\x01'
    fh.seek(-pad, whence=2)
    assert fh.tell() == size - pad
    assert fh.read(4) == b'\x01\x01\x01\x01'
    # assert array
    fh.seek(pad, whence=0)
    assert fh.tell() == pad
    assert_array_equal(
        fh.read_array(numpy.uint8, 255), numpy.arange(255, dtype=numpy.uint8)
    )
    # assert records
    fh.seek(999, whence=1)
    assert fh.tell() == 2253 - offset
    records = fh.read_record(
        [('x', numpy.float32), ('y', numpy.uint8)], (255, 3)
    )
    assert_array_equal(records.y[:, 0], range(255))
    assert_array_equal(records.x, records.y)
    # assert memmap
    if fh.is_file:
        assert_array_equal(
            fh.memmap_array(numpy.uint8, 255, pad),
            numpy.arange(255, dtype=numpy.uint8),
        )


@pytest.mark.skipif(SKIP_HTTP, reason=REASON)
def test_filehandle_seekable():
    """Test FileHandle must be seekable."""
    from urllib.request import HTTPHandler, build_opener

    opener = build_opener(HTTPHandler())
    opener.addheaders = [('User-Agent', 'test_tifffile.py')]
    try:
        fh = opener.open(URL + 'test/test_http.tif')
    except OSError:
        pytest.skip(URL + 'test/test_http.tif')

    with pytest.raises(ValueError):
        FileHandle(fh)


def test_filehandle_write_bytesio():
    """Test write to FileHandle from BytesIO."""
    value = b'123456789'
    buf = BytesIO()
    with FileHandle(buf) as fh:
        fh.write(value)
    assert buf.getvalue() == value


def test_filehandle_write_bytesio_offset():
    """Test write to FileHandle from BytesIO with offset."""
    pad = b'abcd'
    value = b'123456789'
    buf = BytesIO()
    buf.write(pad)
    with FileHandle(buf) as fh:
        fh.write(value)
    buf.write(pad)
    # assert buffer
    buf.seek(len(pad))
    assert buf.read(len(value)) == value
    buf.seek(2)
    with FileHandle(buf, offset=len(pad), size=len(value)) as fh:
        assert fh.read(len(value)) == value


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_filehandle_filename():
    """Test FileHandle from filename."""
    with FileHandle(FILEHANDLE_NAME) as fh:
        assert fh.name == 'test_FileHandle.bin'
        assert fh.is_file
        assert_filehandle(fh)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_filehandle_filename_offset():
    """Test FileHandle from filename with offset."""
    with FileHandle(
        FILEHANDLE_NAME, offset=FILEHANDLE_OFFSET, size=FILEHANDLE_LENGTH
    ) as fh:
        assert fh.name == 'test_FileHandle.bin'
        assert fh.is_file
        assert_filehandle(fh, FILEHANDLE_OFFSET)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_filehandle_bytesio():
    """Test FileHandle from BytesIO."""
    with open(FILEHANDLE_NAME, 'rb') as fh:
        stream = BytesIO(fh.read())
    with FileHandle(stream) as fh:
        assert fh.name == 'Unnamed binary stream'
        assert not fh.is_file
        assert_filehandle(fh)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_filehandle_bytesio_offset():
    """Test FileHandle from BytesIO with offset."""
    with open(FILEHANDLE_NAME, 'rb') as fh:
        stream = BytesIO(fh.read())
    with FileHandle(
        stream, offset=FILEHANDLE_OFFSET, size=FILEHANDLE_LENGTH
    ) as fh:
        assert fh.name == 'Unnamed binary stream'
        assert not fh.is_file
        assert_filehandle(fh, offset=FILEHANDLE_OFFSET)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_filehandle_openfile():
    """Test FileHandle from open file."""
    with open(FILEHANDLE_NAME, 'rb') as fhandle:
        with FileHandle(fhandle) as fh:
            assert fh.name == 'test_FileHandle.bin'
            assert fh.is_file
            assert_filehandle(fh)
        assert not fhandle.closed


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_filehandle_openfile_offset():
    """Test FileHandle from open file with offset."""
    with open(FILEHANDLE_NAME, 'rb') as fhandle:
        with FileHandle(
            fhandle, offset=FILEHANDLE_OFFSET, size=FILEHANDLE_LENGTH
        ) as fh:
            assert fh.name == 'test_FileHandle.bin'
            assert fh.is_file
            assert_filehandle(fh, offset=FILEHANDLE_OFFSET)
        assert not fhandle.closed


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_filehandle_filehandle():
    """Test FileHandle from other FileHandle."""
    with FileHandle(FILEHANDLE_NAME, 'rb') as fhandle:
        with FileHandle(fhandle) as fh:
            assert fh.name == 'test_FileHandle.bin'
            assert fh.is_file
            assert_filehandle(fh)
        assert not fhandle.closed


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_filehandle_offset():
    """Test FileHandle from other FileHandle with offset."""
    with FileHandle(FILEHANDLE_NAME, 'rb') as fhandle:
        with FileHandle(
            fhandle, offset=FILEHANDLE_OFFSET, size=FILEHANDLE_LENGTH
        ) as fh:
            assert fh.name == 'test_FileHandle@333.bin'
            assert fh.is_file
            assert_filehandle(fh, offset=FILEHANDLE_OFFSET)
        assert not fhandle.closed


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_filehandle_reopen():
    """Test FileHandle close and open."""
    try:
        fh = FileHandle(FILEHANDLE_NAME)
        assert not fh.closed
        assert fh.is_file
        fh.close()
        assert fh.closed
        fh.open()
        assert not fh.closed
        assert fh.is_file
        assert fh.name == 'test_FileHandle.bin'
        assert_filehandle(fh)
    finally:
        fh.close()


@pytest.mark.skipif(SKIP_HTTP or not IS_CG, reason=REASON)
def test_filehandle_unc_path():
    """Test FileHandle from UNC path."""
    with FileHandle(r'\\localhost\test$\test_FileHandle.bin') as fh:
        assert fh.name == 'test_FileHandle.bin'
        assert fh.dirname == '\\\\localhost\\test$\\'
        assert_filehandle(fh)


@pytest.mark.skipif(SKIP_FILE or SKIP_ZARR, reason=REASON)
def test_filehandle_fsspec_localfileopener():
    """Test FileHandle from fsspec LocalFileOpener."""
    with fsspec.open(FILEHANDLE_NAME, 'rb') as fhandle:
        with FileHandle(fhandle) as fh:
            assert fh.name == 'test_FileHandle.bin'
            assert fh.is_file  # fails with fsspec 2022.7
            assert_filehandle(fh)
        assert not fhandle.closed


@pytest.mark.skipif(SKIP_FILE or SKIP_ZARR, reason=REASON)
def test_filehandle_fsspec_openfile():
    """Test FileHandle from fsspec OpenFile."""
    fhandle = fsspec.open(FILEHANDLE_NAME, 'rb')
    with FileHandle(fhandle) as fh:
        assert fh.name == 'test_FileHandle.bin'
        assert fh.is_file
        assert_filehandle(fh)
    fhandle.close()


@pytest.mark.skipif(SKIP_FILE or SKIP_HTTP or SKIP_ZARR, reason=REASON)
def test_filehandle_fsspec_http():
    """Test FileHandle from HTTP via fsspec."""
    with fsspec.open(URL + 'test/test_FileHandle.bin', 'rb') as fhandle:
        with FileHandle(fhandle) as fh:
            assert fh.name == 'test_FileHandle.bin'
            assert not fh.is_file
            assert_filehandle(fh)
        assert not fhandle.closed


def test_filehandle_exclusive_creation():
    """Test FileHandle with exclusive creation mode 'x'."""
    # https://github.com/cgohlke/tifffile/issues/221
    with TempFileName('read_filehandle_exclusive', ext='.bin') as filename:
        if os.path.exists(filename):
            os.remove(filename)
        with FileHandle(filename, mode='x'):
            pass
        with pytest.raises(FileExistsError):  # noqa: SIM117
            with FileHandle(filename, mode='x'):
                pass


###############################################################################

# Test read specific files

if SKIP_EXTENDED or SKIP_FILE:
    TIGER_FILES = []
    TIGER_IDS = []
else:
    TIGER_FILES = (
        _files('graphicsmagick.org/be/*.tif')
        + _files('graphicsmagick.org/le/*.tif')
        + _files('graphicsmagick.org/bigtiff-be/*.tif')
        + _files('graphicsmagick.org/bigtiff-le/*.tif')
    )
    TIGER_IDS = [
        '-'.join(f.split(os.path.sep)[-2:])
        .replace('-tiger', '')
        .replace('.tif', '')
        for f in TIGER_FILES
    ]


@pytest.mark.skipif(SKIP_FILE or SKIP_CODECS or SKIP_EXTENDED, reason=REASON)
@pytest.mark.parametrize('filename', TIGER_FILES, ids=TIGER_IDS)
def test_read_tigers(filename):
    """Test tiger images from GraphicsMagick."""
    # ftp://ftp.graphicsmagick.org/pub/tiff-samples
    with TiffFile(filename) as tif:
        byteorder = {'le': '<', 'be': '>'}[os.path.split(filename)[0][-2:]]
        databits = int(filename.rsplit('.tif')[0][-2:])

        # assert file properties
        assert_file_flags(tif)
        assert tif.byteorder == byteorder
        assert tif.is_bigtiff == ('bigtiff' in filename)
        assert len(tif.pages) == 1

        # assert page properties
        page = tif.pages.first
        assert_page_flags(page)
        assert page.tags['DocumentName'].value == os.path.basename(filename)
        assert page.imagewidth == 73
        assert page.imagelength == 76
        assert page.bitspersample == databits
        assert (page.photometric == PHOTOMETRIC.RGB) == ('rgb' in filename)
        assert (page.photometric == PHOTOMETRIC.PALETTE) == (
            'palette' in filename
        )
        assert page.is_tiled == ('tile' in filename)
        assert (page.planarconfig == PLANARCONFIG.CONTIG) == (
            'planar' not in filename
        )
        if 'minisblack' in filename:
            assert page.photometric == PHOTOMETRIC.MINISBLACK

        # float24 not supported
        # if 'float' in filename and databits == 24:
        #     with pytest.raises(ValueError):
        #         data = tif.asarray()
        #     return

        # assert data shapes
        data = tif.asarray()
        assert isinstance(data, numpy.ndarray)
        assert data.flags['C_CONTIGUOUS']
        # if 'palette' in filename:
        #     shape = (76, 73, 3)
        if 'rgb' in filename:
            shape = (3, 76, 73) if 'planar' in filename else (76, 73, 3)
        elif 'separated' in filename:
            shape = (4, 76, 73) if 'planar' in filename else (76, 73, 4)
        else:
            shape = (76, 73)
        assert data.shape == shape

        # assert data types
        if 'float' in filename:
            dtype = numpy.float32 if databits == 24 else f'float{databits}'
        # elif 'palette' in filename:
        #     dtype = numpy.uint16
        elif databits == 1:
            dtype = numpy.bool_
        elif databits <= 8:
            dtype = numpy.uint8
        elif databits <= 16:
            dtype = numpy.uint16
        elif databits <= 32:
            dtype = numpy.uint32
        elif databits <= 64:
            dtype = numpy.uint64
        assert data.dtype == dtype

        assert_decode_method(page, data)
        assert_aszarr_method(page, data)
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_read_exif_paint():
    """Test read EXIF tags."""
    filename = _file('exif/paint.tif')
    with TiffFile(filename) as tif:
        exif = tif.pages.first.tags['ExifTag'].value
        assert exif['ColorSpace'] == 65535
        assert exif['ExifVersion'] == '0230'
        assert exif['UserComment'] == 'paint'
        assert tif.fstat.st_size == 4234366
        assert_aszarr_method(tif)
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE or SKIP_CODECS, reason=REASON)
def test_read_hopper_2bit():
    """Test read 2-bit, fillorder=lsb2msb."""
    # https://github.com/python-pillow/Pillow/pull/1789
    filename = _file('pillow/tiff_gray_2_4_bpp/hopper2.tif')
    with TiffFile(filename) as tif:
        assert tif.byteorder == '<'
        assert len(tif.pages) == 1
        assert len(tif.series) == 1
        # assert page properties
        page = tif.pages.first
        assert page.photometric == PHOTOMETRIC.MINISBLACK
        assert not page.is_contiguous
        assert page.compression == COMPRESSION.NONE
        assert page.imagewidth == 128
        assert page.imagelength == 128
        assert page.bitspersample == 2
        assert page.samplesperpixel == 1
        # assert series properties
        series = tif.series[0]
        assert series.shape == (128, 128)
        assert series.dtype == numpy.uint8
        assert series.axes == 'YX'
        assert series.kind == 'uniform'
        assert series.dataoffset is None
        # assert data
        data = tif.asarray()
        assert isinstance(data, numpy.ndarray)
        assert data.shape == (128, 128)
        assert data[50, 63] == 3
        assert_aszarr_method(tif, data)
        assert__str__(tif)
    # reversed
    filename = _file('pillow/tiff_gray_2_4_bpp/hopper2R.tif')
    with TiffFile(filename) as tif:
        page = tif.pages.first
        assert page.photometric == PHOTOMETRIC.MINISBLACK
        assert page.fillorder == FILLORDER.LSB2MSB
        assert_array_equal(tif.asarray(), data)
        assert_aszarr_method(tif)
        assert__str__(tif)
    # inverted
    filename = _file('pillow/tiff_gray_2_4_bpp/hopper2I.tif')
    with TiffFile(filename) as tif:
        page = tif.pages.first
        assert page.photometric == PHOTOMETRIC.MINISWHITE
        assert_array_equal(tif.asarray(), 3 - data)
        assert_aszarr_method(tif)
        assert__str__(tif)
    # inverted and reversed
    filename = _file('pillow/tiff_gray_2_4_bpp/hopper2IR.tif')
    with TiffFile(filename) as tif:
        page = tif.pages.first
        assert page.photometric == PHOTOMETRIC.MINISWHITE
        assert_array_equal(tif.asarray(), 3 - data)
        assert_aszarr_method(tif)
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE or SKIP_CODECS, reason=REASON)
def test_read_hopper_4bit():
    """Test read 4-bit, fillorder=lsb2msb."""
    # https://github.com/python-pillow/Pillow/pull/1789
    filename = _file('pillow/tiff_gray_2_4_bpp/hopper4.tif')
    with TiffFile(filename) as tif:
        assert tif.byteorder == '<'
        assert len(tif.pages) == 1
        assert len(tif.series) == 1
        # assert page properties
        page = tif.pages.first
        assert page.photometric == PHOTOMETRIC.MINISBLACK
        assert not page.is_contiguous
        assert page.compression == COMPRESSION.NONE
        assert page.imagewidth == 128
        assert page.imagelength == 128
        assert page.bitspersample == 4
        assert page.samplesperpixel == 1
        # assert series properties
        series = tif.series[0]
        assert series.shape == (128, 128)
        assert series.dtype == numpy.uint8
        assert series.axes == 'YX'
        assert series.kind == 'uniform'
        assert series.dataoffset is None
        # assert data
        data = tif.asarray()
        assert isinstance(data, numpy.ndarray)
        assert data.shape == (128, 128)
        assert data[50, 63] == 13
    # reversed
    filename = _file('pillow/tiff_gray_2_4_bpp/hopper4R.tif')
    with TiffFile(filename) as tif:
        page = tif.pages.first
        assert page.photometric == PHOTOMETRIC.MINISBLACK
        assert page.fillorder == FILLORDER.LSB2MSB
        assert_array_equal(tif.asarray(), data)
        assert__str__(tif)
    # inverted
    filename = _file('pillow/tiff_gray_2_4_bpp/hopper4I.tif')
    with TiffFile(filename) as tif:
        page = tif.pages.first
        assert page.photometric == PHOTOMETRIC.MINISWHITE
        assert_array_equal(tif.asarray(), 15 - data)
        assert__str__(tif)
    # inverted and reversed
    filename = _file('pillow/tiff_gray_2_4_bpp/hopper4IR.tif')
    with TiffFile(filename) as tif:
        page = tif.pages.first
        assert page.photometric == PHOTOMETRIC.MINISWHITE
        assert_array_equal(tif.asarray(), 15 - data)
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_read_lsb2msb():
    """Test read fillorder=lsb2msb, 2 series."""
    # http://lists.openmicroscopy.org.uk/pipermail/ome-users
    #   /2015-September/005635.html
    filename = _file('test_lsb2msb.tif')
    with TiffFile(filename) as tif:
        assert tif.byteorder == '<'
        assert len(tif.pages) == 2
        assert len(tif.series) == 2
        # assert page properties
        page = tif.pages.first
        assert page.is_contiguous
        assert page.compression == COMPRESSION.NONE
        assert page.imagewidth == 7100
        assert page.imagelength == 4700
        assert page.bitspersample == 16
        assert page.samplesperpixel == 3
        page = tif.pages[1]
        assert page.is_contiguous
        assert page.compression == COMPRESSION.NONE
        assert page.imagewidth == 7100
        assert page.imagelength == 4700
        assert page.bitspersample == 16
        assert page.samplesperpixel == 1
        # assert series properties
        series = tif.series[0]
        assert series.shape == (4700, 7100, 3)
        assert series.dtype == numpy.uint16
        assert series.axes == 'YXS'
        assert series.dataoffset is None
        series = tif.series[1]
        assert series.shape == (4700, 7100)
        assert series.dtype == numpy.uint16
        assert series.axes == 'YX'
        assert series.kind == 'generic'
        assert series.dataoffset is None
        # assert data
        data = tif.asarray(series=0)
        assert isinstance(data, numpy.ndarray)
        assert data.shape == (4700, 7100, 3)
        assert data[2350, 3550, 1] == 60457
        assert_aszarr_method(tif, data, series=0)
        data = tif.asarray(series=1)
        assert isinstance(data, numpy.ndarray)
        assert data.flags['C_CONTIGUOUS']
        assert data.shape == (4700, 7100)
        assert data[2350, 3550] == 56341
        assert_aszarr_method(tif, data, series=1)
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_read_gimp_u2():
    """Test read uint16 with horizontal predictor by GIMP."""
    filename = _file('tifffile/gimp_u2.tiff')
    with TiffFile(filename) as tif:
        assert len(tif.pages) == 1
        page = tif.pages.first
        assert page.compression == COMPRESSION.ADOBE_DEFLATE
        assert page.photometric == PHOTOMETRIC.RGB
        assert page.predictor == PREDICTOR.HORIZONTAL
        assert page.imagewidth == 333
        assert page.imagelength == 231
        assert page.samplesperpixel == 3
        image = tif.asarray()
        assert image.flags['C_CONTIGUOUS']
        assert tuple(image[110, 110]) == (23308, 17303, 41160)
        assert_aszarr_method(tif, image)
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_read_gimp_f4():
    """Test read float32 with horizontal predictor by GIMP."""
    filename = _file('tifffile/gimp_f4.tiff')
    with TiffFile(filename) as tif:
        assert len(tif.pages) == 1
        page = tif.pages.first
        assert page.compression == COMPRESSION.ADOBE_DEFLATE
        assert page.photometric == PHOTOMETRIC.RGB
        assert page.predictor == PREDICTOR.HORIZONTAL
        assert page.imagewidth == 333
        assert page.imagelength == 231
        assert page.samplesperpixel == 3
        image = tif.asarray()
        assert image.flags['C_CONTIGUOUS']
        assert_allclose(
            image[110, 110], (0.35565534, 0.26402164, 0.6280674), rtol=1e-5
        )
        assert_aszarr_method(tif, image)
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_read_gimp_f2():
    """Test read float16 with horizontal predictor by GIMP."""
    filename = _file('tifffile/gimp_f2.tiff')
    with TiffFile(filename) as tif:
        assert len(tif.pages) == 1
        page = tif.pages.first
        assert page.compression == COMPRESSION.ADOBE_DEFLATE
        assert page.photometric == PHOTOMETRIC.RGB
        assert page.predictor == PREDICTOR.HORIZONTAL
        assert page.imagewidth == 333
        assert page.imagelength == 231
        assert page.samplesperpixel == 3
        image = tif.asarray()
        assert image.flags['C_CONTIGUOUS']
        assert_allclose(
            image[110, 110].astype(numpy.float64),
            (0.35571289, 0.26391602, 0.62792969),
            rtol=1e-5,
        )
        assert_aszarr_method(tif, image)
        assert__str__(tif)


@pytest.mark.skipif(
    SKIP_FILE or SKIP_CODECS or not imagecodecs.JPEG8.available,
    reason=REASON,
)
def test_read_dng_jpeglossy():
    """Test read JPEG_LOSSY in DNG."""
    filename = _file('DNG/Adobe DNG Converter.dng')
    with TiffFile(filename) as tif:
        assert len(tif.pages) == 1
        assert len(tif.series) == 6
        for series in tif.series:
            image = series.asarray()
            assert_aszarr_method(series, image)
        assert__str__(tif)


@pytest.mark.skipif(
    SKIP_FILE or SKIP_CODECS or not imagecodecs.LJPEG.available,
    reason=REASON,
)
def test_read_dng_ljpeg():
    """Test read 14-bit CFA LJPEG in DNG."""
    filename = _file('DNG/uint14_ljpeg_cfa.dng')
    with TiffFile(filename) as tif:
        assert len(tif.pages) == 1
        assert len(tif.series) == 3
        page = tif.pages.first.pages[0]
        assert page.compression == COMPRESSION.JPEG
        assert page.photometric == PHOTOMETRIC.CFA
        assert page.imagewidth == 7392
        assert page.imagelength == 4950
        assert page.bitspersample == 14
        assert page.samplesperpixel == 1
        assert page.tags['CFARepeatPatternDim'].value == (2, 2)
        assert page.tags['CFAPattern'].value == b'\0\1\1\2'
        assert page.tags['CFALayout'].value == 1
        image = page.asarray()
        assert image.shape == (4950, 7392)
        assert image.dtype == numpy.uint16
        assert image[1024, 1024] == 3425
        assert__str__(tif)


@pytest.mark.skipif(
    SKIP_FILE or SKIP_CODECS or not imagecodecs.LJPEG.available,
    reason=REASON,
)
def test_read_dng_linearraw():
    """Test read 12-bit LinearRAW LJPEG in DNG."""
    filename = _file('DNG/LinearRaw.dng')
    with TiffFile(filename) as tif:
        assert len(tif.pages) == 1
        assert len(tif.series) == 2
        page = tif.pages.first.pages[0]
        assert page.compression == COMPRESSION.JPEG
        assert page.photometric == PHOTOMETRIC.LINEAR_RAW
        assert page.imagewidth == 4032
        assert page.imagelength == 3024
        assert page.bitspersample == 12
        assert page.samplesperpixel == 3
        assert page.tile == (378, 504)
        image = page.asarray()
        assert image.shape == (3024, 4032, 3)
        assert image.dtype == numpy.uint16
        assert tuple(image[740, 3660]) == (1474, 3090, 1655)
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE or SKIP_CODECS, reason=REASON)
@pytest.mark.parametrize('fp', ['fp16', 'fp24', 'fp32'])
def test_read_dng_floatpredx2(fp):
    """Test read FLOATINGPOINTX2 predictor in DNG."""
    # <https://raw.pixls.us/data/Canon/EOS%205D%20Mark%20III/>
    filename = _file(f'DNG/fpx2/hdrmerge-bayer-{fp}-w-pred-deflate.dng')
    with TiffFile(filename) as tif:
        assert len(tif.pages) == 1
        assert len(tif.series) == 3
        page = tif.pages.first.pages[0]
        assert page.compression == COMPRESSION.ADOBE_DEFLATE
        assert page.photometric == PHOTOMETRIC.CFA
        assert page.predictor == 34894
        assert page.imagewidth == 5920
        assert page.imagelength == 3950
        assert page.sampleformat == SAMPLEFORMAT.IEEEFP
        assert page.bitspersample == int(fp[2:])
        assert page.samplesperpixel == 1
        if fp == 'fp24':
            with pytest.raises(NotImplementedError):
                image = page.asarray()
        else:
            image = page.asarray()
            assert_aszarr_method(page, image)
        assert__str__(tif)


@pytest.mark.skipif(
    SKIP_FILE or SKIP_CODECS or not imagecodecs.JPEGXL.available,
    reason=REASON,
)
def test_read_dng_jpegxl():
    """Test read JPEGXL in DNG."""
    filename = _file('DNG/20240125_204051_1.dng')
    with TiffFile(filename) as tif:
        assert len(tif.pages) == 1
        assert len(tif.series) == 6

        page = tif.pages.first.pages[0]
        assert not page.is_reduced
        assert page.compression == COMPRESSION.JPEGXL_DNG
        assert page.photometric == PHOTOMETRIC.LINEAR_RAW
        assert page.imagewidth == 5712
        assert page.imagelength == 4284
        assert page.bitspersample == 16
        assert page.samplesperpixel == 3
        image = page.asarray()
        assert image.shape == (4284, 5712, 3)
        assert image.dtype == numpy.uint16
        assert image[1024, 1024, 1] == 36

        page = tif.pages.first.pages[1]
        assert page.is_reduced
        assert page.compression == COMPRESSION.JPEGXL_DNG
        assert page.photometric == PHOTOMETRIC.RGB
        assert page.imagewidth == 1024
        assert page.imagelength == 768
        assert page.bitspersample == 8
        assert page.samplesperpixel == 3
        image = page.asarray()
        assert image.shape == (768, 1024, 3)
        assert image.dtype == numpy.uint8
        assert image[512, 512, 1] in {47, 48}
        assert page.tags['JXLDistance'].value == 2.0
        assert page.tags['JXLEffort'].value == 7
        assert page.tags['JXLDecodeSpeed'].value == 4
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
@pytest.mark.parametrize('filename', ['sample1.orf', 'sample1.rw2'])
def test_read_rawformats(filename, caplog):
    """Test parse unsupported RAW formats."""
    filename = _file(f'RAWformats/{filename}')
    with TiffFile(filename) as tif:
        assert 'RAW format' in caplog.text
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_read_iss_vista():
    """Test read bogus imagedepth tag by ISS Vista."""
    filename = _file('iss/10um_beads_14stacks_ch1.tif')
    with TiffFile(filename) as tif:
        assert tif.byteorder == '<'
        assert len(tif.pages) == 14
        assert len(tif.series) == 1
        # assert page properties
        page = tif.pages.first
        assert not page.is_reduced
        assert not page.is_tiled
        assert page.compression == COMPRESSION.NONE
        assert page.imagewidth == 256
        assert page.imagelength == 256
        assert page.tags['ImageDepth'].value == 14  # bogus
        assert page.bitspersample == 16
        assert page.samplesperpixel == 1
        # assert series properties
        series = tif.series[0]
        assert series.shape == (14, 256, 256)
        assert series.dtype == numpy.int16
        assert series.axes == 'IYX'  # ZYX
        assert series.kind == 'uniform'
        assert isinstance(series.pages[3], TiffFrame)
        assert_aszarr_method(series)
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_read_vips():
    """Test read 347x641 RGB, bigtiff, pyramid, tiled, produced by VIPS."""
    filename = _file('vips.tif')
    with TiffFile(filename) as tif:
        assert tif.byteorder == '<'
        assert len(tif.pages) == 4
        assert len(tif.series) == 1
        # assert page properties
        page = tif.pages.first
        assert not page.is_reduced
        assert page.is_tiled
        assert page.compression == COMPRESSION.ADOBE_DEFLATE
        assert page.imagewidth == 641
        assert page.imagelength == 347
        assert page.bitspersample == 8
        assert page.samplesperpixel == 3
        # assert series properties
        series = tif.series[0]
        assert series.is_pyramidal
        assert len(series.levels) == 4
        assert series.shape == (347, 641, 3)
        assert series.dtype == numpy.uint8
        assert series.axes == 'YXS'
        assert series.kind == 'generic'
        # level 3
        series = series.levels[3]
        page = series.pages[0]
        assert page.is_reduced
        assert page.is_tiled
        assert series.shape == (43, 80, 3)
        assert series.dtype == numpy.uint8
        assert series.axes == 'YXS'
        # assert data
        data = tif.asarray(series=0)
        assert isinstance(data, numpy.ndarray)
        assert data.flags['C_CONTIGUOUS']
        assert data.shape == (347, 641, 3)
        assert data.dtype == numpy.uint8
        assert tuple(data[132, 361]) == (114, 233, 58)
        assert_aszarr_method(tif, data, series=0, level=0)
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_read_volumetric():
    """Test read 128x128x128, float32, tiled SGI."""
    filename = _file('tifffile/sgi_depth.tif')
    with TiffFile(filename) as tif:
        assert tif.byteorder == '<'
        assert len(tif.pages) == 1
        assert len(tif.series) == 1
        # assert page properties
        page = tif.pages.first
        assert page.is_volumetric
        assert page.planarconfig == PLANARCONFIG.CONTIG
        assert page.is_tiled
        assert page.is_contiguous
        assert page.compression == COMPRESSION.NONE
        assert page.imagewidth == 128
        assert page.imagelength == 128
        assert page.imagedepth == 128
        assert page.tilewidth == 128
        assert page.tilelength == 128
        assert page.tiledepth == 1
        assert page.tile == (128, 128)
        assert page.bitspersample == 32
        assert page.samplesperpixel == 1
        assert page.tags['Software'].value == (
            'MFL MeVis File Format Library, TIFF Module'
        )
        # assert series properties
        series = tif.series[0]
        assert series.shape == (128, 128, 128)
        assert series.dtype == numpy.float32
        assert series.axes == 'ZYX'
        assert series.kind == 'uniform'
        # assert data
        data = tif.asarray()
        assert isinstance(data, numpy.ndarray)
        assert data.flags['C_CONTIGUOUS']
        assert data.shape == (128, 128, 128)
        assert data.dtype == numpy.float32
        assert data[64, 64, 64] == 0.0
        assert_decode_method(page)
        assert_aszarr_method(tif, data)
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE or SKIP_CODECS, reason=REASON)
def test_read_oxford():
    """Test read 601x81, uint8, LZW."""
    filename = _file('juicypixels/oxford.tif')
    with TiffFile(filename) as tif:
        assert tif.byteorder == '>'
        assert len(tif.pages) == 1
        assert len(tif.series) == 1
        # assert page properties
        page = tif.pages.first
        assert page.planarconfig == PLANARCONFIG.SEPARATE
        assert page.photometric == PHOTOMETRIC.RGB
        assert page.compression == COMPRESSION.LZW
        assert page.imagewidth == 601
        assert page.imagelength == 81
        assert page.bitspersample == 8
        assert page.samplesperpixel == 3
        # assert series properties
        series = tif.series[0]
        assert series.shape == (3, 81, 601)
        assert series.dtype == numpy.uint8
        assert series.axes == 'SYX'
        assert series.kind == 'uniform'
        # assert data
        data = tif.asarray()
        assert isinstance(data, numpy.ndarray)
        assert data.flags['C_CONTIGUOUS']
        assert data.shape == (3, 81, 601)
        assert data.dtype == numpy.uint8
        assert data[1, 24, 49] == 191
        assert_aszarr_method(tif, data)
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_read_cramps():
    """Test 800x607 uint8, PackBits."""
    filename = _file('juicypixels/cramps.tif')
    with TiffFile(filename) as tif:
        assert tif.byteorder == '>'
        assert len(tif.pages) == 1
        assert len(tif.series) == 1
        # assert page properties
        page = tif.pages.first
        assert page.compression == COMPRESSION.PACKBITS
        assert page.photometric == PHOTOMETRIC.MINISWHITE
        assert page.imagewidth == 800
        assert page.imagelength == 607
        assert page.bitspersample == 8
        assert page.samplesperpixel == 1
        # assert series properties
        series = tif.series[0]
        assert series.shape == (607, 800)
        assert series.dtype == numpy.uint8
        assert series.axes == 'YX'
        assert series.kind == 'uniform'
        # assert data
        data = tif.asarray()
        assert isinstance(data, numpy.ndarray)
        assert data.flags['C_CONTIGUOUS']
        assert data.shape == (607, 800)
        assert data.dtype == numpy.uint8
        assert data[273, 426] == 34
        assert_aszarr_method(tif, data)
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_read_cramps_tile():
    """Test read 800x607 uint8, raw, volumetric, tiled."""
    filename = _file('juicypixels/cramps-tile.tif')
    with TiffFile(filename) as tif:
        assert tif.byteorder == '>'
        assert len(tif.pages) == 1
        assert len(tif.series) == 1
        # assert page properties
        page = tif.pages.first
        assert page.is_tiled
        assert not page.is_volumetric
        assert page.compression == COMPRESSION.NONE
        assert page.photometric == PHOTOMETRIC.MINISWHITE
        assert page.imagewidth == 800
        assert page.imagelength == 607
        assert page.imagedepth == 1
        assert page.tilewidth == 256
        assert page.tilelength == 256
        assert page.tiledepth == 1
        assert page.bitspersample == 8
        assert page.samplesperpixel == 1
        # assert series properties
        series = tif.series[0]
        assert series.shape == (607, 800)
        assert series.dtype == numpy.uint8
        assert series.axes == 'YX'
        assert series.kind == 'uniform'
        # assert data
        data = tif.asarray()
        assert isinstance(data, numpy.ndarray)
        assert data.flags['C_CONTIGUOUS']
        assert data.shape == (607, 800)
        assert data.dtype == numpy.uint8
        assert data[273, 426] == 34
        assert_aszarr_method(tif, data)
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_read_jello():
    """Test read 256x192x3, uint16, palette, PackBits."""
    filename = _file('juicypixels/jello.tif')
    with TiffFile(filename) as tif:
        assert tif.byteorder == '>'
        assert len(tif.pages) == 1
        assert len(tif.series) == 1
        # assert page properties
        page = tif.pages.first
        assert page.photometric == PHOTOMETRIC.PALETTE
        assert page.planarconfig == PLANARCONFIG.CONTIG
        assert page.compression == COMPRESSION.PACKBITS
        assert page.imagewidth == 256
        assert page.imagelength == 192
        assert page.bitspersample == 8
        assert page.samplesperpixel == 1
        # assert series properties
        series = tif.series[0]
        assert series.shape == (192, 256)
        assert series.dtype == numpy.uint8
        assert series.axes == 'YX'
        assert series.kind == 'uniform'
        # assert data
        data = page.asrgb(uint8=False)
        assert isinstance(data, numpy.ndarray)
        assert data.flags['C_CONTIGUOUS']
        assert data.shape == (192, 256, 3)
        assert data.dtype == numpy.uint16
        assert tuple(data[100, 140, :]) == (48895, 65279, 48895)
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE or SKIP_CODECS, reason=REASON)
def test_read_quad_lzw():
    """Test read 384x512 RGB uint8 old style LZW."""
    filename = _file('libtiff/quad-lzw-compat.tiff')
    with TiffFile(filename) as tif:
        assert tif.byteorder == '>'
        assert len(tif.pages) == 1
        assert len(tif.series) == 1
        # assert page properties
        page = tif.pages.first
        assert not page.is_tiled
        assert page.photometric == PHOTOMETRIC.RGB
        assert page.compression == COMPRESSION.LZW
        assert page.imagewidth == 512
        assert page.imagelength == 384
        assert page.bitspersample == 8
        assert page.samplesperpixel == 3
        # assert series properties
        series = tif.series[0]
        assert series.shape == (384, 512, 3)
        assert series.dtype == numpy.uint8
        assert series.axes == 'YXS'
        assert series.kind == 'uniform'
        # assert data
        data = tif.asarray()
        assert isinstance(data, numpy.ndarray)
        assert data.flags['C_CONTIGUOUS']
        assert data.shape == (384, 512, 3)
        assert data.dtype == numpy.uint8
        assert tuple(data[309, 460, :]) == (0, 163, 187)
        assert_aszarr_method(tif, data)


@pytest.mark.skipif(SKIP_FILE or SKIP_CODECS, reason=REASON)
def test_read_quad_lzw_le():
    """Test read 384x512 RGB uint8 LZW."""
    filename = _file('quad-lzw_le.tif')
    with TiffFile(filename) as tif:
        assert tif.byteorder == '<'
        assert len(tif.pages) == 1
        assert len(tif.series) == 1
        # assert page properties
        page = tif.pages.first
        assert page.photometric == PHOTOMETRIC.RGB
        assert not page.is_tiled
        assert page.compression == COMPRESSION.LZW
        assert page.imagewidth == 512
        assert page.imagelength == 384
        assert page.bitspersample == 8
        assert page.samplesperpixel == 3
        # assert series properties
        series = tif.series[0]
        assert series.shape == (384, 512, 3)
        assert series.dtype == numpy.uint8
        assert series.axes == 'YXS'
        assert series.kind == 'uniform'
        # assert data
        data = tif.asarray()
        assert isinstance(data, numpy.ndarray)
        assert data.flags['C_CONTIGUOUS']
        assert data.shape == (384, 512, 3)
        assert data.dtype == numpy.uint8
        assert tuple(data[309, 460, :]) == (0, 163, 187)
        assert_aszarr_method(tif, data)
        assert_decode_method(page)


@pytest.mark.skipif(SKIP_FILE or SKIP_CODECS, reason=REASON)
def test_read_quad_tile():
    """Test read 384x512 RGB uint8 LZW tiled."""
    # Strips and tiles defined in same page
    filename = _file('juicypixels/quad-tile.tif')
    with TiffFile(filename) as tif:
        assert__str__(tif)
        assert tif.byteorder == '>'
        assert len(tif.pages) == 1
        assert len(tif.series) == 1
        # assert page properties
        page = tif.pages.first
        assert page.photometric == PHOTOMETRIC.RGB
        assert page.is_tiled
        assert page.compression == COMPRESSION.LZW
        assert page.imagewidth == 512
        assert page.imagelength == 384
        assert page.imagedepth == 1
        assert page.tilewidth == 128
        assert page.tilelength == 128
        assert page.tiledepth == 1
        assert page.bitspersample == 8
        assert page.samplesperpixel == 3
        # assert series properties
        series = tif.series[0]
        assert series.shape == (384, 512, 3)
        assert series.dtype == numpy.uint8
        assert series.axes == 'YXS'
        assert series.kind == 'uniform'
        # assert data
        data = tif.asarray()
        # assert 'invalid tile data (49153,) (1, 128, 128, 3)' in caplog.text
        assert isinstance(data, numpy.ndarray)
        assert data.flags['C_CONTIGUOUS']
        assert data.shape == (384, 512, 3)
        assert data.dtype == numpy.uint8
        assert tuple(data[309, 460, :]) == (0, 163, 187)
        assert_aszarr_method(tif, data)


@pytest.mark.skipif(SKIP_FILE or SKIP_CODECS, reason=REASON)
def test_read_strike():
    """Test read 256x200 RGBA uint8 LZW."""
    filename = _file('juicypixels/strike.tif')
    with TiffFile(filename) as tif:
        assert__str__(tif)
        assert tif.byteorder == '>'
        assert len(tif.pages) == 1
        assert len(tif.series) == 1
        # assert page properties
        page = tif.pages.first
        assert page.photometric == PHOTOMETRIC.RGB
        assert page.compression == COMPRESSION.LZW
        assert page.imagewidth == 256
        assert page.imagelength == 200
        assert page.bitspersample == 8
        assert page.samplesperpixel == 4
        assert page.extrasamples[0] == EXTRASAMPLE.ASSOCALPHA
        # assert series properties
        series = tif.series[0]
        assert series.shape == (200, 256, 4)
        assert series.dtype == numpy.uint8
        assert series.axes == 'YXS'
        assert series.kind == 'uniform'
        # assert data
        data = tif.asarray()
        assert isinstance(data, numpy.ndarray)
        assert data.flags['C_CONTIGUOUS']
        assert data.shape == (200, 256, 4)
        assert data.dtype == numpy.uint8
        assert tuple(data[65, 139, :]) == (43, 34, 17, 91)
        assert_aszarr_method(tif, data)
        assert_decode_method(page)
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_read_incomplete_tile_contig():
    """Test read PackBits compressed incomplete tile, contig RGB."""
    filename = _file('GDAL/contig_tiled.tif')
    with TiffFile(filename) as tif:
        assert tif.byteorder == '>'
        assert len(tif.pages) == 1
        assert len(tif.series) == 1
        # assert page properties
        page = tif.pages.first
        assert page.photometric == PHOTOMETRIC.RGB
        assert page.planarconfig == PLANARCONFIG.CONTIG
        assert page.compression == COMPRESSION.PACKBITS
        assert page.imagewidth == 35
        assert page.imagelength == 37
        assert page.bitspersample == 8
        assert page.samplesperpixel == 3
        # assert series properties
        series = tif.series[0]
        assert series.shape == (37, 35, 3)
        assert series.dtype == numpy.uint8
        assert series.axes == 'YXS'
        assert series.kind == 'uniform'
        # assert data
        data = page.asarray()
        assert data.flags['C_CONTIGUOUS']
        assert data.shape == (37, 35, 3)
        assert data.dtype == numpy.uint8
        assert tuple(data[19, 31]) == (50, 50, 50)
        assert tuple(data[36, 34]) == (70, 70, 70)
        assert_aszarr_method(page, data)
        assert_decode_method(page)
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_read_incomplete_tile_separate():
    """Test read PackBits compressed incomplete tile, separate RGB."""
    filename = _file('GDAL/separate_tiled.tif')
    with TiffFile(filename) as tif:
        assert tif.byteorder == '>'
        assert len(tif.pages) == 1
        assert len(tif.series) == 1
        # assert page properties
        page = tif.pages.first
        assert page.photometric == PHOTOMETRIC.RGB
        assert page.planarconfig == PLANARCONFIG.SEPARATE
        assert page.compression == COMPRESSION.PACKBITS
        assert page.imagewidth == 35
        assert page.imagelength == 37
        assert page.bitspersample == 8
        assert page.samplesperpixel == 3
        # assert series properties
        series = tif.series[0]
        assert series.shape == (3, 37, 35)
        assert series.dtype == numpy.uint8
        assert series.axes == 'SYX'
        assert series.kind == 'uniform'
        # assert data
        data = page.asarray()
        assert data.flags['C_CONTIGUOUS']
        assert data.shape == (3, 37, 35)
        assert data.dtype == numpy.uint8
        assert tuple(data[:, 19, 31]) == (50, 50, 50)
        assert tuple(data[:, 36, 34]) == (70, 70, 70)
        assert_aszarr_method(page, data)
        assert_decode_method(page)
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_read_django():
    """Test read 3x480x320, uint16, palette, raw."""
    filename = _file('django.tiff')
    with TiffFile(filename) as tif:
        assert tif.byteorder == '<'
        assert len(tif.pages) == 1
        assert len(tif.series) == 1
        # assert page properties
        page = tif.pages.first
        assert page.photometric == PHOTOMETRIC.PALETTE
        assert page.planarconfig == PLANARCONFIG.CONTIG
        assert page.compression == COMPRESSION.NONE
        assert page.imagewidth == 320
        assert page.imagelength == 480
        assert page.bitspersample == 8
        assert page.samplesperpixel == 1
        # assert series properties
        series = tif.series[0]
        assert series.shape == (480, 320)
        assert series.dtype == numpy.uint8
        assert series.axes == 'YX'
        assert series.kind == 'uniform'
        # assert data
        data = page.asrgb(uint8=False)
        assert isinstance(data, numpy.ndarray)
        assert data.flags['C_CONTIGUOUS']
        assert data.shape == (480, 320, 3)
        assert data.dtype == numpy.uint16
        assert tuple(data[64, 64, :]) == (65535, 52171, 63222)
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_read_pygame_icon():
    """Test read 128x128 RGBA uint8 PackBits."""
    filename = _file('pygame_icon.tiff')
    with TiffFile(filename) as tif:
        assert tif.byteorder == '>'
        assert len(tif.pages) == 1
        assert len(tif.series) == 1
        # assert page properties
        page = tif.pages.first
        assert page.photometric == PHOTOMETRIC.RGB
        assert page.compression == COMPRESSION.PACKBITS
        assert page.imagewidth == 128
        assert page.imagelength == 128
        assert page.bitspersample == 8
        assert page.samplesperpixel == 4
        assert page.extrasamples[0] == EXTRASAMPLE.UNASSALPHA  # ?
        assert page.tags['Software'].value == 'QuickTime 5.0.5'
        assert page.tags['HostComputer'].value == 'MacOS 10.1.2'
        assert page.tags['DateTime'].value == '2001:12:21 04:34:56'
        assert page.datetime == datetime.datetime(2001, 12, 21, 4, 34, 56)
        # assert series properties
        series = tif.series[0]
        assert series.shape == (128, 128, 4)
        assert series.dtype == numpy.uint8
        assert series.axes == 'YXS'
        assert series.kind == 'uniform'
        # assert data
        data = tif.asarray()
        assert isinstance(data, numpy.ndarray)
        assert data.flags['C_CONTIGUOUS']
        assert data.shape == (128, 128, 4)
        assert data.dtype == numpy.uint8
        assert tuple(data[22, 112, :]) == (100, 99, 98, 132)
        assert_aszarr_method(tif, data)
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE or SKIP_CODECS, reason=REASON)
def test_read_rgba_wo_extra_samples():
    """Test read 1065x785 RGBA uint8."""
    filename = _file('rgba_wo_extra_samples.tif')
    with TiffFile(filename) as tif:
        assert tif.byteorder == '<'
        assert len(tif.pages) == 1
        assert len(tif.series) == 1
        # assert page properties
        page = tif.pages.first
        assert page.photometric == PHOTOMETRIC.RGB
        assert page.compression == COMPRESSION.LZW
        assert page.imagewidth == 1065
        assert page.imagelength == 785
        assert page.bitspersample == 8
        assert page.samplesperpixel == 4
        # with self.assertRaises(AttributeError):
        #     page.extrasamples
        # assert series properties
        series = tif.series[0]
        assert series.shape == (785, 1065, 4)
        assert series.dtype == numpy.uint8
        assert series.axes == 'YXS'
        assert series.kind == 'uniform'
        # assert data
        data = tif.asarray()
        assert isinstance(data, numpy.ndarray)
        assert data.flags['C_CONTIGUOUS']
        assert data.shape == (785, 1065, 4)
        assert data.dtype == numpy.uint8
        assert tuple(data[560, 412, :]) == (60, 92, 74, 255)
        assert_aszarr_method(tif, data)
        assert_decode_method(page)
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_read_rgb565():
    """Test read 64x64 RGB uint8 5,6,5 bitspersample."""
    filename = _file('rgb565.tif')
    with TiffFile(filename) as tif:
        assert tif.byteorder == '<'
        assert len(tif.pages) == 1
        assert len(tif.series) == 1
        # assert page properties
        page = tif.pages.first
        assert page.photometric == PHOTOMETRIC.RGB
        assert page.compression == COMPRESSION.NONE
        assert page.imagewidth == 64
        assert page.imagelength == 64
        assert page.bitspersample == (5, 6, 5)
        assert page.samplesperpixel == 3
        # assert series properties
        series = tif.series[0]
        assert series.shape == (64, 64, 3)
        assert series.dtype == numpy.uint8
        assert series.axes == 'YXS'
        assert series.kind == 'uniform'
        # assert data
        data = tif.asarray()
        assert isinstance(data, numpy.ndarray)
        assert data.flags['C_CONTIGUOUS']
        assert data.shape == (64, 64, 3)
        assert data.dtype == numpy.uint8
        assert tuple(data[56, 32, :]) == (239, 243, 247)
        assert_aszarr_method(tif, data)
        assert_decode_method(page)
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE or SKIP_CODECS, reason=REASON)
def test_read_generic_series():
    """Test read 4 series in 6 pages."""
    filename = _file('tifffile/generic_series.tif')
    with TiffFile(filename) as tif:
        assert tif.byteorder == '<'
        assert len(tif.pages) == 6
        assert len(tif.series) == 4
        # assert series 0 properties
        series = tif.series[0]
        assert series.shape == (3, 20, 20)
        assert series.dtype == numpy.uint8
        assert series.axes == 'IYX'
        assert series.kind == 'generic'
        page = series.pages[0]
        assert page.compression == COMPRESSION.LZW
        assert page.imagewidth == 20
        assert page.imagelength == 20
        assert page.bitspersample == 8
        assert page.samplesperpixel == 1
        data = tif.asarray(series=0)
        assert data.flags['C_CONTIGUOUS']
        assert data.shape == (3, 20, 20)
        assert data.dtype == numpy.uint8
        assert tuple(data[:, 9, 9]) == (19, 90, 206)
        assert_aszarr_method(tif, data, series=0)
        # assert series 1 properties
        series = tif.series[1]
        assert series.shape == (10, 10, 3)
        assert series.dtype == numpy.float32
        assert series.axes == 'YXS'
        assert series.kind == 'generic'
        page = series.pages[0]
        assert page.photometric == PHOTOMETRIC.RGB
        assert page.compression == COMPRESSION.LZW
        assert page.imagewidth == 10
        assert page.imagelength == 10
        assert page.bitspersample == 32
        assert page.samplesperpixel == 3
        data = tif.asarray(series=1)
        assert isinstance(data, numpy.ndarray)
        assert data.flags['C_CONTIGUOUS']
        assert data.shape == (10, 10, 3)
        assert data.dtype == numpy.float32
        assert round(abs(data[9, 9, 1] - 214.5733642578125), 7) == 0
        assert_aszarr_method(tif, data, series=1)
        # assert series 2 properties
        series = tif.series[2]
        assert series.shape == (20, 20, 3)
        assert series.dtype == numpy.uint8
        assert series.axes == 'YXS'
        assert series.kind == 'generic'
        page = series.pages[0]
        assert page.photometric == PHOTOMETRIC.RGB
        assert page.compression == COMPRESSION.LZW
        assert page.imagewidth == 20
        assert page.imagelength == 20
        assert page.bitspersample == 8
        assert page.samplesperpixel == 3
        data = tif.asarray(series=2)
        assert isinstance(data, numpy.ndarray)
        assert data.flags['C_CONTIGUOUS']
        assert data.shape == (20, 20, 3)
        assert data.dtype == numpy.uint8
        assert tuple(data[9, 9, :]) == (19, 90, 206)
        assert_aszarr_method(tif, data, series=2)
        # assert series 3 properties
        series = tif.series[3]
        assert series.shape == (10, 10)
        assert series.dtype == numpy.float32
        assert series.axes == 'YX'
        assert series.kind == 'generic'
        page = series.pages[0]
        assert page.compression == COMPRESSION.LZW
        assert page.imagewidth == 10
        assert page.imagelength == 10
        assert page.bitspersample == 32
        assert page.samplesperpixel == 1
        data = tif.asarray(series=3)
        assert isinstance(data, numpy.ndarray)
        assert data.flags['C_CONTIGUOUS']
        assert data.shape == (10, 10)
        assert data.dtype == numpy.float32
        assert round(abs(data[9, 9] - 223.1648712158203), 7) == 0
        assert_aszarr_method(tif, data, series=3)
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE or SKIP_CODECS, reason=REASON)
def test_read_freeimage():
    """Test read 3 series in 3 pages RGB LZW."""
    filename = _file('freeimage.tif')
    with TiffFile(filename) as tif:
        assert tif.byteorder == '<'
        assert len(tif.pages) == 3
        assert len(tif.series) == 3
        for i, shape in enumerate(
            ((100, 600, 3), (379, 574, 3), (689, 636, 3))
        ):
            series = tif.series[i]
            assert series.shape == shape
            assert series.dtype == numpy.uint8
            assert series.axes == 'YXS'
            assert series.kind == 'generic'
            page = series.pages[0]
            assert page.photometric == PHOTOMETRIC.RGB
            assert page.compression == COMPRESSION.LZW
            assert page.imagewidth == shape[1]
            assert page.imagelength == shape[0]
            assert page.bitspersample == 8
            assert page.samplesperpixel == 3
            data = tif.asarray(series=i)
            assert isinstance(data, numpy.ndarray)
            assert data.flags['C_CONTIGUOUS']
            assert data.shape == shape
            assert data.dtype == numpy.uint8
            assert_aszarr_method(tif, data, series=i)
            assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE or SKIP_CODECS, reason=REASON)
def test_read_leadtools():
    """Test read LeadTools 11-pages with different compression."""
    # https://www.leadtools.com/support/forum/posts/t10960-
    filename = _file('LeadTools/MultipleFormats.tif')
    with TiffFile(filename) as tif:
        assert tif.byteorder == '<'
        assert len(tif.pages) == 11
        assert len(tif.series) == 11
        assert__str__(tif)
        # 1- Uncompressed bilevel
        page = tif.pages.first
        assert page.photometric == PHOTOMETRIC.MINISBLACK
        assert page.compression == COMPRESSION.NONE
        assert page.imagewidth == 600
        assert page.imagelength == 75
        assert page.bitspersample == 1
        assert page.samplesperpixel == 1
        data = page.asarray()
        assert data.shape == (75, 600)
        assert data.dtype == numpy.bool_
        # 2- Uncompressed CMYK
        page = tif.pages[1]
        assert page.photometric == PHOTOMETRIC.SEPARATED
        assert page.compression == COMPRESSION.NONE
        assert page.imagewidth == 600
        assert page.imagelength == 75
        assert page.bitspersample == 8
        assert page.samplesperpixel == 4
        data = page.asarray()
        assert data.shape == (75, 600, 4)
        assert data.dtype == numpy.uint8
        # 3- JPEG 2000 compression
        page = tif.pages[2]
        assert page.photometric == PHOTOMETRIC.RGB
        assert page.compression == COMPRESSION.JPEG2000
        assert page.imagewidth == 600
        assert page.imagelength == 75
        assert page.bitspersample == 8
        assert page.samplesperpixel == 3
        data = page.asarray()
        assert data.shape == (75, 600, 3)
        assert data.dtype == numpy.uint8
        # 4- JPEG 4:1:1 compression
        page = tif.pages[3]
        assert page.photometric == PHOTOMETRIC.YCBCR
        assert page.compression == COMPRESSION.JPEG
        assert page.imagewidth == 600
        assert page.imagelength == 75
        assert page.bitspersample == 8
        assert page.samplesperpixel == 3
        assert page.tags['YCbCrSubSampling'].value == (2, 2)
        data = page.asarray()
        assert data.shape == (75, 600, 3)
        assert data.dtype == numpy.uint8
        # 5- JBIG compression
        page = tif.pages[4]
        assert page.photometric == PHOTOMETRIC.MINISBLACK
        assert page.compression == COMPRESSION.JBIG
        assert page.imagewidth == 600
        assert page.imagelength == 75
        assert page.bitspersample == 1
        assert page.samplesperpixel == 1
        with pytest.raises(ValueError):
            data = page.asarray()
        # 6- RLE Packbits compression
        page = tif.pages[5]
        assert page.photometric == PHOTOMETRIC.RGB
        assert page.compression == COMPRESSION.PACKBITS
        assert page.imagewidth == 600
        assert page.imagelength == 75
        assert page.bitspersample == 8
        assert page.samplesperpixel == 3
        data = page.asarray()
        assert data.shape == (75, 600, 3)
        assert data.dtype == numpy.uint8
        # 7- CMYK with RLE Packbits compression
        page = tif.pages[6]
        assert page.photometric == PHOTOMETRIC.SEPARATED
        assert page.compression == COMPRESSION.PACKBITS
        assert page.imagewidth == 600
        assert page.imagelength == 75
        assert page.bitspersample == 8
        assert page.samplesperpixel == 4
        data = page.asarray()
        assert data.shape == (75, 600, 4)
        assert data.dtype == numpy.uint8
        # 8- YCC with RLE Packbits compression
        page = tif.pages[7]
        assert page.photometric == PHOTOMETRIC.YCBCR
        assert page.compression == COMPRESSION.PACKBITS
        assert page.imagewidth == 600
        assert page.imagelength == 75
        assert page.bitspersample == 8
        assert page.samplesperpixel == 3
        assert page.tags['YCbCrSubSampling'].value == (2, 1)
        with pytest.raises(NotImplementedError):
            data = page.asarray()
        # 9- LZW compression
        page = tif.pages[8]
        assert page.photometric == PHOTOMETRIC.MINISBLACK
        assert page.compression == COMPRESSION.LZW
        assert page.imagewidth == 600
        assert page.imagelength == 75
        assert page.bitspersample == 1
        assert page.samplesperpixel == 1
        data = page.asarray()
        assert data.shape == (75, 600)
        assert data.dtype == numpy.bool_
        # 10- CCITT Group 4 compression
        page = tif.pages[9]
        assert page.photometric == PHOTOMETRIC.MINISWHITE
        assert page.compression == COMPRESSION.CCITT_T6
        assert page.imagewidth == 600
        assert page.imagelength == 75
        assert page.bitspersample == 1
        assert page.samplesperpixel == 1
        with pytest.raises(ValueError):
            data = page.asarray()
        # 11- CCITT Group 3 2-D compression
        page = tif.pages[10]
        assert page.photometric == PHOTOMETRIC.MINISWHITE
        assert page.compression == COMPRESSION.CCITT_T4
        assert page.imagewidth == 600
        assert page.imagelength == 75
        assert page.bitspersample == 1
        assert page.samplesperpixel == 1
        with pytest.raises(ValueError):
            data = page.asarray()


@pytest.mark.skipif(SKIP_FILE or SKIP_CODECS, reason=REASON)
def test_read_12bit():
    """Test read 12 bit images."""
    filename = _file('12bit.tif')
    with TiffFile(filename) as tif:
        assert tif.byteorder == '<'
        assert len(tif.pages) == 1000
        assert len(tif.series) == 1
        # assert page properties
        page = tif.pages.first
        assert not page.is_contiguous
        assert page.compression == COMPRESSION.NONE
        assert page.imagewidth == 1024
        assert page.imagelength == 304
        assert page.bitspersample == 12
        assert page.samplesperpixel == 1
        # assert series properties
        series = tif.series[0]
        assert series.shape == (1000, 304, 1024)
        assert series.dtype == numpy.uint16
        assert series.axes == 'IYX'
        assert series.kind == 'uniform'
        # assert data
        data = tif.asarray(478)
        assert isinstance(data, numpy.ndarray)
        assert data.flags['C_CONTIGUOUS']
        assert data.shape == (304, 1024)
        assert data.dtype == numpy.uint16
        assert round(abs(data[138, 475] - 40), 7) == 0
        assert_aszarr_method(tif, data, key=478)
        assert__str__(tif, 0)


@pytest.mark.skipif(SKIP_FILE or SKIP_CODECS, reason=REASON)
def test_read_lzw_12bit_table():
    """Test read lzw-full-12-bit-table.tif.

    Also test RowsPerStrip > ImageLength.

    """
    filename = _file('twelvemonkeys/tiff/lzw-full-12-bit-table.tif')
    with TiffFile(filename) as tif:
        assert len(tif.series) == 1
        assert len(tif.pages) == 1
        page = tif.pages.first
        assert page.photometric == PHOTOMETRIC.MINISBLACK
        assert page.imagewidth == 874
        assert page.imagelength == 1240
        assert page.bitspersample == 8
        assert page.samplesperpixel == 1
        assert page.rowsperstrip == 1240
        assert page.tags['RowsPerStrip'].value == 4294967295
        # assert data
        image = page.asarray()
        assert image.flags['C_CONTIGUOUS']
        assert image[434, 588] == 88
        assert image[400, 600] == 255
        assert_aszarr_method(page, image)
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE or SKIP_CODECS or SKIP_LARGE, reason=REASON)
def test_read_lzw_large_buffer():
    """Test read LZW compression which requires large buffer."""
    # https://github.com/groupdocs-viewer/GroupDocs.Viewer-for-.NET-MVC-App
    # /issues/35
    filename = _file('lzw/lzw_large_buffer.tiff')
    with TiffFile(filename) as tif:
        assert len(tif.pages) == 1
        page = tif.pages.first
        assert page.compression == COMPRESSION.LZW
        assert page.imagewidth == 5104
        assert page.imagelength == 8400
        assert page.bitspersample == 8
        assert page.samplesperpixel == 4
        # assert data
        image = page.asarray()
        assert image.shape == (8400, 5104, 4)
        assert image.dtype == numpy.uint8
        image = tif.asarray()
        assert image.shape == (8400, 5104, 4)
        assert image.dtype == numpy.uint8
        assert image[4200, 2550, 0] == 0
        assert image[4200, 2550, 3] == 255
        assert_aszarr_method(tif, image)
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE or SKIP_CODECS, reason=REASON)
def test_read_lzw_ycbcr_subsampling():
    """Test fail LZW compression with subsampling."""
    filename = _file('lzw/lzw_ycbcr_subsampling.tif')
    with TiffFile(filename) as tif:
        assert len(tif.pages) == 1
        page = tif.pages.first
        assert page.compression == COMPRESSION.LZW
        assert page.photometric == PHOTOMETRIC.YCBCR
        assert page.planarconfig == PLANARCONFIG.CONTIG
        assert page.imagewidth == 39
        assert page.imagelength == 39
        assert page.bitspersample == 8
        assert page.samplesperpixel == 3
        # assert data
        with pytest.raises(NotImplementedError):
            page.asarray()
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_read_ycbcr_subsampling():
    """Test fail YCBCR with subsampling."""
    filename = _file('ycbcr_subsampling.tif')
    with TiffFile(filename) as tif:
        assert len(tif.pages) == 2
        page = tif.pages.first
        assert page.compression == COMPRESSION.NONE
        assert page.photometric == PHOTOMETRIC.YCBCR
        assert page.planarconfig == PLANARCONFIG.CONTIG
        assert page.imagewidth == 640
        assert page.imagelength == 480
        assert page.bitspersample == 8
        assert page.samplesperpixel == 3
        # assert data
        with pytest.raises(NotImplementedError):
            page.asarray()
        assert__str__(tif)


@pytest.mark.skipif(
    SKIP_FILE or SKIP_CODECS or not imagecodecs.JPEG.available,
    reason=REASON,
)
def test_read_jpeg_baboon():
    """Test JPEG compression."""
    filename = _file('baboon.tiff')
    with TiffFile(filename) as tif:
        assert tif.byteorder == '<'
        assert len(tif.pages) == 1
        assert len(tif.series) == 1
        # assert page properties
        page = tif.pages.first
        assert 'JPEGTables' in page.tags
        assert not page.is_reduced
        assert not page.is_tiled
        assert page.compression == COMPRESSION.JPEG
        assert page.imagewidth == 512
        assert page.imagelength == 512
        assert page.bitspersample == 8
        # assert series properties
        series = tif.series[0]
        assert series.shape == (512, 512, 3)
        assert series.dtype == numpy.uint8
        assert series.axes == 'YXS'
        assert series.kind == 'uniform'
        # assert data
        # with pytest.raises((ValueError, NotImplementedError)):
        image = tif.asarray()
        assert image.flags['C_CONTIGUOUS']
        assert_aszarr_method(tif, image)
        assert__str__(tif)


@pytest.mark.skipif(
    SKIP_FILE or SKIP_CODECS or not imagecodecs.JPEG.available,
    reason=REASON,
)
def test_read_jpeg_ycbcr():
    """Test read YCBCR JPEG is returned as RGB."""
    filename = _file('jpeg/jpeg_ycbcr.tiff')
    with TiffFile(filename) as tif:
        assert len(tif.pages) == 1
        page = tif.pages.first
        assert page.compression == COMPRESSION.JPEG
        assert page.photometric == PHOTOMETRIC.YCBCR
        assert page.planarconfig == PLANARCONFIG.CONTIG
        assert page.imagewidth == 128
        assert page.imagelength == 80
        assert page.bitspersample == 8
        assert page.samplesperpixel == 3
        # assert data
        image = tif.asarray()
        assert image.flags['C_CONTIGUOUS']
        assert image.shape == (80, 128, 3)
        assert image.dtype == numpy.uint8
        assert tuple(image[50, 50, :]) == (177, 149, 210)
        # YCBCR (164, 154, 137)
        assert_aszarr_method(tif, image)
        assert_decode_method(page)
        assert__str__(tif)


@pytest.mark.skipif(
    SKIP_FILE or SKIP_CODECS or not imagecodecs.JPEG.available,
    reason=REASON,
)
@pytest.mark.parametrize(
    'filename', ['tiff_tiled_cmyk_jpeg.tif', 'tiff_strip_cmyk_jpeg.tif']
)
def test_read_jpeg_cmyk(filename):
    """Test read JPEG compressed CMYK image."""
    with TiffFile(_file(f'pillow/{filename}')) as tif:
        assert len(tif.pages) == 1
        page = tif.pages.first
        assert page.compression == COMPRESSION.JPEG
        assert page.photometric == PHOTOMETRIC.SEPARATED
        assert page.shape == (100, 100, 4)
        assert page.dtype == numpy.uint8
        data = page.asarray()
        assert data.shape == (100, 100, 4)
        assert data.dtype == numpy.uint8
        assert tuple(data[46, 49]) == (79, 230, 222, 77)
        assert_aszarr_method(tif, data)
        # assert_decode_method(page)
        assert__str__(tif)


@pytest.mark.skipif(
    SKIP_FILE or SKIP_CODECS or not imagecodecs.JPEG8.available,
    reason=REASON,
)
def test_read_jpeg12_mandril():
    """Test read JPEG 12-bit compression."""
    # JPEG 12-bit
    filename = _file('jpeg/jpeg12_mandril.tif')
    with TiffFile(filename) as tif:
        assert len(tif.pages) == 1
        page = tif.pages.first
        assert page.compression == COMPRESSION.JPEG
        assert page.photometric == PHOTOMETRIC.YCBCR
        assert page.imagewidth == 512
        assert page.imagelength == 480
        assert page.bitspersample == 12
        assert page.samplesperpixel == 3
        # assert data
        image = tif.asarray()
        assert image.flags['C_CONTIGUOUS']
        assert image.shape == (480, 512, 3)
        assert image.dtype == numpy.uint16
        assert tuple(image[128, 128, :]) == (1685, 1859, 1376)
        # YCBCR (1752, 1836, 2000)
        assert_aszarr_method(tif, image)
        assert__str__(tif)


@pytest.mark.skipif(
    SKIP_FILE or SKIP_CODECS or SKIP_LARGE or not imagecodecs.JPEG.available,
    reason=REASON,
)
def test_read_jpeg_lsb2msb():
    """Test read huge tiled, JPEG compressed, with lsb2msb specified.

    Also test JPEG with RGB photometric.

    """
    filename = _file('large/jpeg_lsb2msb.tif')
    with TiffFile(filename) as tif:
        assert len(tif.pages) == 1
        page = tif.pages.first
        assert page.compression == COMPRESSION.JPEG
        assert page.photometric == PHOTOMETRIC.RGB
        assert not page.is_jfif
        assert page.imagewidth == 49128
        assert page.imagelength == 59683
        assert page.bitspersample == 8
        assert page.samplesperpixel == 3
        # assert data
        image = tif.asarray()
        assert image.flags['C_CONTIGUOUS']
        assert image.shape == (59683, 49128, 3)
        assert image.dtype == numpy.uint8
        assert tuple(image[38520, 43767, :]) == (255, 255, 255)
        assert tuple(image[47866, 30076, :]) == (52, 39, 23)
        assert__str__(tif)


@pytest.mark.skipif(
    SKIP_FILE
    or SKIP_CODECS
    or not imagecodecs.JPEG.available
    or not imagecodecs.JPEG2K.available,
    reason=REASON,
)
def test_read_aperio_j2k():
    """Test read SVS slide with J2K compression."""
    filename = _file('slides/CMU-1-JP2K-33005.tif')
    with TiffFile(filename) as tif:
        assert tif.is_svs
        assert len(tif.pages) == 6
        page = tif.pages.first
        assert page.compression == COMPRESSION.APERIO_JP2000_RGB
        assert page.photometric == PHOTOMETRIC.RGB
        assert page.planarconfig == PLANARCONFIG.CONTIG
        assert page.shape == (32893, 46000, 3)
        assert page.dtype == numpy.uint8
        page = tif.pages[1]
        assert page.compression == COMPRESSION.JPEG
        assert page.photometric == PHOTOMETRIC.RGB
        assert page.planarconfig == PLANARCONFIG.CONTIG
        assert page.shape == (732, 1024, 3)
        assert page.dtype == numpy.uint8
        page = tif.pages[2]
        assert page.compression == COMPRESSION.APERIO_JP2000_RGB
        assert page.photometric == PHOTOMETRIC.RGB
        assert page.planarconfig == PLANARCONFIG.CONTIG
        assert page.shape == (8223, 11500, 3)
        assert page.dtype == numpy.uint8
        page = tif.pages[3]
        assert page.compression == COMPRESSION.APERIO_JP2000_RGB
        assert page.photometric == PHOTOMETRIC.RGB
        assert page.planarconfig == PLANARCONFIG.CONTIG
        assert page.shape == (2055, 2875, 3)
        assert page.dtype == numpy.uint8
        page = tif.pages[4]
        assert page.is_reduced
        assert page.compression == COMPRESSION.LZW
        assert page.photometric == PHOTOMETRIC.RGB
        assert page.planarconfig == PLANARCONFIG.CONTIG
        assert page.shape == (463, 387, 3)
        assert page.dtype == numpy.uint8
        page = tif.pages[5]
        assert page.is_reduced
        assert page.compression == COMPRESSION.JPEG
        assert page.photometric == PHOTOMETRIC.RGB
        assert page.planarconfig == PLANARCONFIG.CONTIG
        assert page.shape == (431, 1280, 3)
        assert page.dtype == numpy.uint8
        # assert data
        image = tif.pages[3].asarray()
        assert image.flags['C_CONTIGUOUS']
        assert image.shape == (2055, 2875, 3)
        assert image.dtype == numpy.uint8
        assert image[512, 1024, 0] == 246
        assert image[512, 1024, 1] == 245
        assert image[512, 1024, 2] == 245

        assert_decode_method(tif.pages[3], image)
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_read_lzma():
    """Test read LZMA compression."""
    # 512x512, uint8, lzma compression
    filename = _file('lzma.tif')
    with TiffFile(filename) as tif:
        assert tif.byteorder == '<'
        assert len(tif.pages) == 1
        assert len(tif.series) == 1
        # assert page properties
        page = tif.pages.first
        assert page.compression == COMPRESSION.LZMA
        assert page.photometric == PHOTOMETRIC.MINISBLACK
        assert page.imagewidth == 512
        assert page.imagelength == 512
        assert page.bitspersample == 8
        assert page.samplesperpixel == 1
        # assert series properties
        series = tif.series[0]
        assert series.shape == (512, 512)
        assert series.dtype == numpy.uint8
        assert series.axes == 'YX'
        assert series.kind == 'shaped'
        # assert data
        data = tif.asarray()
        assert data.flags['C_CONTIGUOUS']
        assert isinstance(data, numpy.ndarray)
        assert data.shape == (512, 512)
        assert data.dtype == numpy.uint8
        assert data[273, 426] == 151
        assert_aszarr_method(tif, data)
        assert__str__(tif)


@pytest.mark.skipif(
    SKIP_FILE or SKIP_CODECS or not imagecodecs.WEBP.available, reason=REASON
)
def test_read_webp():
    """Test read WebP compression."""
    filename = _file('GDAL/tif_webp.tif')
    with TiffFile(filename) as tif:
        assert len(tif.pages) == 1
        page = tif.pages.first
        assert page.compression == COMPRESSION.WEBP
        assert page.photometric == PHOTOMETRIC.RGB
        assert page.planarconfig == PLANARCONFIG.CONTIG
        assert page.imagewidth == 50
        assert page.imagelength == 50
        assert page.bitspersample == 8
        assert page.samplesperpixel == 3
        # assert data
        image = tif.asarray()
        assert image.flags['C_CONTIGUOUS']
        assert image.shape == (50, 50, 3)
        assert image.dtype == numpy.uint8
        assert image[25, 25, 0] == 92
        assert image[25, 25, 1] == 122
        assert image[25, 25, 2] == 37
        assert_aszarr_method(tif, image)
        assert__str__(tif)


@pytest.mark.skipif(
    SKIP_FILE or SKIP_CODECS or not imagecodecs.LERC.available, reason=REASON
)
def test_read_lerc():
    """Test read LERC compression."""
    if not hasattr(imagecodecs, 'LERC'):
        pytest.skip('LERC codec missing')

    filename = _file('imagecodecs/rgb.u2.lerc.tif')
    with TiffFile(filename) as tif:
        assert len(tif.pages) == 1
        page = tif.pages.first
        assert page.compression == COMPRESSION.LERC
        assert page.photometric == PHOTOMETRIC.RGB
        assert page.planarconfig == PLANARCONFIG.CONTIG
        assert page.imagewidth == 31
        assert page.imagelength == 32
        assert page.bitspersample == 16
        assert page.samplesperpixel == 3
        # assert data
        image = tif.asarray()
        assert image.flags['C_CONTIGUOUS']
        assert image.shape == (32, 31, 3)
        assert image.dtype == numpy.uint16
        assert tuple(image[25, 25]) == (3265, 1558, 2811)
        assert_aszarr_method(tif, image)
        assert__str__(tif)


@pytest.mark.skipif(
    SKIP_FILE or SKIP_CODECS or not imagecodecs.ZSTD.available, reason=REASON
)
def test_read_zstd():
    """Test read ZStd compression."""
    filename = _file('GDAL/byte_zstd.tif')
    with TiffFile(filename) as tif:
        assert len(tif.pages) == 1
        page = tif.pages.first
        assert page.compression == COMPRESSION.ZSTD
        assert page.photometric == PHOTOMETRIC.MINISBLACK
        assert page.planarconfig == PLANARCONFIG.CONTIG
        assert page.imagewidth == 20
        assert page.imagelength == 20
        assert page.bitspersample == 8
        assert page.samplesperpixel == 1
        # assert data
        image = tif.asarray()  # fails with imagecodecs <= 2018.11.8
        assert image.flags['C_CONTIGUOUS']
        assert image.shape == (20, 20)
        assert image.dtype == numpy.uint8
        assert image[18, 1] == 247
        assert_aszarr_method(tif, image)
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE or SKIP_CODECS, reason=REASON)
def test_read_jetraw():
    """Test read Jetraw compression."""
    try:
        have_jetraw = imagecodecs.JETRAW.available
    except AttributeError:
        # requires imagecodecs > 2022.22.2
        have_jetraw = False

    filename = _file('jetraw/16ms-1.p.tif')
    with TiffFile(filename) as tif:
        assert len(tif.pages) == 1
        page = tif.pages.first
        assert page.compression == COMPRESSION.JETRAW
        assert page.photometric == PHOTOMETRIC.MINISBLACK
        assert page.planarconfig == PLANARCONFIG.CONTIG
        assert page.imagewidth == 2304
        assert page.imagelength == 2304
        assert page.bitspersample == 16
        assert page.samplesperpixel == 1
        assert__str__(tif)
        # assert data
        if not have_jetraw:
            pytest.skip('Jetraw codec not available')
        image = tif.asarray()
        assert image[1490, 1830] == 36554
        assert_aszarr_method(tif, image)


@pytest.mark.skipif(SKIP_FILE or SKIP_CODECS, reason=REASON)
def test_read_pixtiff():
    """Test read PIXTIFF compression."""
    # https://github.com/haraldk/TwelveMonkeys/issues/307
    filename = _file('PIXTIFF/pixtiff_1bpp.tif')
    with TiffFile(filename) as tif:
        page = tif.pages.first
        assert page.compression == COMPRESSION.PIXTIFF
        assert page.photometric == PHOTOMETRIC.MINISBLACK
        assert page.imagewidth == 801
        assert page.imagelength == 1313
        assert page.bitspersample == 1
        assert page.samplesperpixel == 1
        assert__str__(tif)
        image = tif.asarray()
        assert image.dtype == numpy.bool_
        assert image.shape == (1313, 801)
        assert not image[1000, 700]

    filename = _file('PIXTIFF/pixtiff_4bpp.tif')
    with TiffFile(filename) as tif:
        page = tif.pages.first
        assert page.compression == COMPRESSION.PIXTIFF
        assert page.photometric == PHOTOMETRIC.MINISBLACK
        assert page.imagewidth == 801
        assert page.imagelength == 1313
        assert page.bitspersample == 4
        assert page.samplesperpixel == 1
        assert__str__(tif)
        image = tif.asarray()
        assert image.dtype == numpy.uint8
        assert image.shape == (1313, 801)
        assert image[1000, 700] == 6

    filename = _file('PIXTIFF/pixtiff_8bpp_rgb.tif')
    with TiffFile(filename) as tif:
        page = tif.pages.first
        assert page.compression == COMPRESSION.PIXTIFF
        assert page.photometric == PHOTOMETRIC.RGB
        assert page.imagewidth == 801
        assert page.imagelength == 1313
        assert page.bitspersample == 8
        assert page.samplesperpixel == 3
        assert__str__(tif)
        image = tif.asarray()
        assert image.dtype == numpy.uint8
        assert image.shape == (1313, 801, 3)
        assert tuple(image[1000, 700]) == (89, 70, 80)


@pytest.mark.skipif(
    SKIP_FILE or SKIP_CODECS or not imagecodecs.LJPEG.available,
    reason=REASON,
)
def test_read_dng():
    """Test read JPEG compressed CFA image in SubIFD."""
    filename = _file('DNG/IMG_0793.DNG')
    with TiffFile(filename) as tif:
        assert len(tif.pages) == 1
        assert len(tif.series) == 2
        page = tif.pages.first
        assert page.index == 0
        assert page.shape == (640, 852, 3)
        assert page.bitspersample == 8
        data = page.asarray()
        assert_aszarr_method(tif, data)
        page = tif.pages.first.pages[0]
        assert page.is_tiled
        assert page.treeindex == (0, 0)
        assert page.compression == COMPRESSION.JPEG
        assert page.photometric == PHOTOMETRIC.CFA
        assert page.shape == (3024, 4032)
        assert page.bitspersample == 16
        assert page.tags['CFARepeatPatternDim'].value == (2, 2)
        assert page.tags['CFAPattern'].value == b'\x00\x01\x01\x02'
        data = page.asarray()
        assert_aszarr_method(tif.series[1], data)
        assert__str__(tif)


@pytest.mark.skipif(
    SKIP_FILE or SKIP_CODECS or not imagecodecs.LJPEG.available,
    reason=REASON,
)
def test_read_cfa():
    """Test read 14-bit uncompressed and JPEG compressed CFA image."""
    filename = _file('DNG/cinemadng/M14-1451_000085_cDNG_uncompressed.dng')
    with TiffFile(filename) as tif:
        assert len(tif.pages) == 1
        page = tif.pages.first
        assert page.compression == 1
        assert page.photometric == PHOTOMETRIC.CFA
        assert page.imagewidth == 960
        assert page.imagelength == 540
        assert page.bitspersample == 14
        assert page.tags['CFARepeatPatternDim'].value == (2, 2)
        assert page.tags['CFAPattern'].value == b'\x00\x01\x01\x02'
        data = page.asarray()
        assert_aszarr_method(tif, data)

    filename = _file('DNG/cinemadng/M14-1451_000085_cDNG_compressed.dng')
    with TiffFile(filename) as tif:
        assert len(tif.pages) == 1
        page = tif.pages.first
        assert page.compression == COMPRESSION.JPEG
        assert page.photometric == PHOTOMETRIC.CFA
        assert page.imagewidth == 960
        assert page.imagelength == 540
        assert page.bitspersample == 14
        assert page.tags['CFARepeatPatternDim'].value == (2, 2)
        assert page.tags['CFAPattern'].value == b'\x00\x01\x01\x02'
        image = page.asarray()
        assert_array_equal(image, data)
        assert_aszarr_method(tif, data)


@pytest.mark.skipif(SKIP_FILE or SKIP_CODECS, reason=REASON)
def test_read_lena_be_f16_contig():
    """Test read big endian float16 horizontal differencing."""
    filename = _file('PS/lena_be_f16_contig.tif')
    with TiffFile(filename) as tif:
        assert tif.byteorder == '>'
        assert len(tif.pages) == 1
        assert len(tif.series) == 1
        # assert page properties
        page = tif.pages.first
        assert not page.is_reduced
        assert not page.is_tiled
        assert page.compression == COMPRESSION.NONE
        assert page.imagewidth == 512
        assert page.imagelength == 512
        assert page.bitspersample == 16
        assert page.samplesperpixel == 3
        # assert series properties
        series = tif.series[0]
        assert series.shape == (512, 512, 3)
        assert series.dtype == numpy.float16
        assert series.axes == 'YXS'
        assert series.kind == 'imagej'
        # assert data
        data = tif.asarray(series=0)
        assert isinstance(data, numpy.ndarray)
        assert data.flags['C_CONTIGUOUS']
        assert data.shape == (512, 512, 3)
        assert data.dtype == numpy.float16
        assert_allclose(
            data[256, 256], (0.4563, 0.052856, 0.064819), rtol=1e-4
        )
        assert_aszarr_method(tif, data, series=0)
        assert_decode_method(page)
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE or SKIP_CODECS, reason=REASON)
def test_read_lena_be_f16_lzw_planar():
    """Test read big endian, float16, LZW, horizontal differencing."""
    filename = _file('PS/lena_be_f16_lzw_planar.tif')
    with TiffFile(filename, is_imagej=False) as tif:
        assert tif.byteorder == '>'
        assert len(tif.pages) == 1
        assert len(tif.series) == 1
        assert not tif.is_imagej
        # assert page properties
        page = tif.pages.first
        assert not page.is_reduced
        assert not page.is_tiled
        assert page.compression == COMPRESSION.LZW
        assert page.imagewidth == 512
        assert page.imagelength == 512
        assert page.bitspersample == 16
        assert page.samplesperpixel == 3
        # assert series properties
        series = tif.series[0]
        assert series.shape == (3, 512, 512)
        assert series.dtype == numpy.float16
        assert series.axes == 'SYX'
        assert series.kind == 'uniform'
        # assert data
        data = tif.asarray(series=0)
        assert isinstance(data, numpy.ndarray)
        assert data.flags['C_CONTIGUOUS']
        assert data.shape == (3, 512, 512)
        assert data.dtype == numpy.float16
        assert_allclose(
            data[:, 256, 256], (0.4563, 0.052856, 0.064819), rtol=1e-4
        )
        assert_aszarr_method(tif, data, series=0)
        assert_decode_method(page)
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE or SKIP_CODECS, reason=REASON)
def test_read_lena_be_f32_deflate_contig():
    """Test read big endian, float32 horizontal differencing, deflate."""
    filename = _file('PS/lena_be_f32_deflate_contig.tif')
    with TiffFile(filename, is_imagej=False) as tif:
        assert tif.byteorder == '>'
        assert len(tif.pages) == 1
        assert len(tif.series) == 1
        assert not tif.is_imagej
        # assert page properties
        page = tif.pages.first
        assert not page.is_reduced
        assert not page.is_tiled
        assert page.compression == COMPRESSION.ADOBE_DEFLATE
        assert page.imagewidth == 512
        assert page.imagelength == 512
        assert page.bitspersample == 32
        assert page.samplesperpixel == 3
        # assert series properties
        series = tif.series[0]
        assert series.shape == (512, 512, 3)
        assert series.dtype == numpy.float32
        assert series.axes == 'YXS'
        assert series.kind == 'uniform'
        # assert data
        data = tif.asarray(series=0)
        assert isinstance(data, numpy.ndarray)
        assert data.flags['C_CONTIGUOUS']
        assert data.shape == (512, 512, 3)
        assert data.dtype == numpy.float32
        assert_allclose(
            data[256, 256], (0.456386, 0.052867, 0.064795), rtol=1e-5
        )
        assert_aszarr_method(tif, data, series=0)
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE or SKIP_CODECS, reason=REASON)
def test_read_lena_le_f32_lzw_planar():
    """Test read little endian, LZW, float32 horizontal differencing."""
    filename = _file('PS/lena_le_f32_lzw_planar.tif')
    with TiffFile(filename, is_imagej=False) as tif:
        assert tif.byteorder == '<'
        assert len(tif.pages) == 1
        assert len(tif.series) == 1
        assert not tif.is_imagej
        # assert page properties
        page = tif.pages.first
        assert not page.is_reduced
        assert not page.is_tiled
        assert page.compression == COMPRESSION.LZW
        assert page.imagewidth == 512
        assert page.imagelength == 512
        assert page.bitspersample == 32
        assert page.samplesperpixel == 3
        # assert series properties
        series = tif.series[0]
        assert series.shape == (3, 512, 512)
        assert series.dtype == numpy.float32
        assert series.axes == 'SYX'
        assert series.kind == 'uniform'
        # assert data
        data = tif.asarray(series=0)
        assert isinstance(data, numpy.ndarray)
        assert data.flags['C_CONTIGUOUS']
        assert data.shape == (3, 512, 512)
        assert data.dtype == numpy.float32
        assert_allclose(
            data[:, 256, 256], (0.456386, 0.052867, 0.064795), rtol=1e-5
        )
        assert_aszarr_method(tif, data, series=0)
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_read_lena_be_rgb48():
    """Test read RGB48."""
    filename = _file('PS/lena_be_rgb48.tif')
    with TiffFile(filename) as tif:
        assert tif.byteorder == '>'
        assert len(tif.pages) == 1
        assert len(tif.series) == 1
        # assert page properties
        page = tif.pages.first
        assert not page.is_reduced
        assert not page.is_tiled
        assert page.compression == COMPRESSION.NONE
        assert page.imagewidth == 512
        assert page.imagelength == 512
        assert page.bitspersample == 16
        assert page.samplesperpixel == 3
        # assert series properties
        series = tif.series[0]
        assert series.shape == (512, 512, 3)
        assert series.dtype == numpy.uint16
        assert series.axes == 'YXS'
        assert series.kind == 'imagej'
        # assert data
        data = tif.asarray(series=0)
        assert isinstance(data, numpy.ndarray)
        assert data.flags['C_CONTIGUOUS']
        assert data.shape == (512, 512, 3)
        assert data.dtype == numpy.uint16
        assert_array_equal(data[256, 256], (46259, 16706, 18504))
        assert_aszarr_method(tif, data, series=0)
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE or SKIP_LARGE or IS_PYPY, reason=REASON)
def test_read_huge_ps5_memmap():
    """Test read 30000x30000 float32 contiguous."""
    # TODO: segfault on pypy3.7-v7.3.5rc2-win64
    filename = _file('large/huge_ps5.tif')
    with TiffFile(filename) as tif:
        assert tif.byteorder == '<'
        assert len(tif.pages) == 1
        assert len(tif.series) == 1
        # assert page properties
        page = tif.pages.first
        assert page.dataoffsets[0] == 21890
        assert page.nbytes == 3600000000
        assert not page.is_memmappable  # data not aligned!
        assert page.compression == COMPRESSION.NONE
        assert page.imagewidth == 30000
        assert page.imagelength == 30000
        assert page.bitspersample == 32
        assert page.samplesperpixel == 1
        # assert series properties
        series = tif.series[0]
        assert series.shape == (30000, 30000)
        assert series.dtype == numpy.float32
        assert series.axes == 'YX'
        assert series.kind == 'uniform'
        # assert data
        data = tif.asarray(out='memmap')  # memmap in a temp file
        assert isinstance(data, numpy.memmap)
        assert data.flags['C_CONTIGUOUS']
        assert data.shape == (30000, 30000)
        assert data.dtype == numpy.float32
        assert data[6597, 8135] == 0.008780896663665771
        assert_aszarr_method(tif, data)
        del data
        assert not tif.filehandle.closed
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE or SKIP_LARGE, reason=REASON)
def test_read_movie():
    """Test read 30000 pages, uint16."""
    filename = _file('tifffile/movie.tif')
    with TiffFile(filename) as tif:
        assert tif.byteorder == '<'
        assert len(tif.pages) == 30000
        assert len(tif.series) == 1
        assert tif.is_uniform
        # assert series properties
        series = tif.series[0]
        assert series.shape == (30000, 64, 64)
        assert series.dtype == numpy.uint16
        assert series.axes == 'IYX'
        assert series.kind == 'uniform'
        # assert page properties
        page = tif.pages[-1]
        if tif.pages.cache:
            assert isinstance(page, TiffFrame)
        else:
            assert isinstance(page, TiffPage)
        assert page.shape == (64, 64)
        page = tif.pages[-3]
        if tif.pages.cache:
            assert isinstance(page, TiffFrame)
        else:
            assert isinstance(page, TiffPage)
        # assert data
        data = tif.pages[29999].asarray()  # last frame
        assert isinstance(data, numpy.ndarray)
        assert data.flags['C_CONTIGUOUS']
        assert data.shape == (64, 64)
        assert data.dtype == numpy.uint16
        assert data[32, 32] == 460
        del data
        # read selected pages
        # https://github.com/blink1073/tifffile/issues/51
        data = tif.asarray(key=[31, 999, 29999])
        assert data.flags['C_CONTIGUOUS']
        assert data.shape == (3, 64, 64)
        assert data[2, 32, 32] == 460
        del data
        assert__str__(tif, 0)


@pytest.mark.skipif(SKIP_FILE or SKIP_LARGE, reason=REASON)
def test_read_movie_memmap():
    """Test read 30000 pages memory-mapped."""
    filename = _file('tifffile/movie.tif')
    with TiffFile(filename) as tif:
        # assert data
        data = tif.asarray(out='memmap')
        assert isinstance(data, numpy.memmap)
        assert data.flags['C_CONTIGUOUS']
        assert data.shape == (30000, 64, 64)
        assert data.dtype == numpy.dtype('<u2')
        assert data[29999, 32, 32] == 460
        del data
        assert not tif.filehandle.closed
        assert__str__(tif, 0)


@pytest.mark.skipif(SKIP_FILE or SKIP_LARGE, reason=REASON)
def test_read_100000_pages_movie():
    """Test read 100000x64x64 big endian in memory."""
    filename = _file('tifffile/100000_pages.tif')
    with TiffFile(filename, _useframes=True) as tif:
        assert tif.is_imagej
        assert tif.byteorder == '>'
        assert len(tif.pages) == 100000
        assert len(tif.series) == 1
        # assert series properties
        series = tif.series[0]
        assert series.shape == (100000, 64, 64)
        assert series.dtype == numpy.uint16
        assert series.axes == 'TYX'
        assert series.kind == 'imagej'
        # assert page properties
        frame = tif.pages[100]
        assert isinstance(frame, TiffFrame)  # uniform=True
        assert frame.shape == (64, 64)
        frame = tif.pages.first
        assert frame.imagewidth == 64
        assert frame.imagelength == 64
        assert frame.bitspersample == 16
        assert frame.compression == 1
        assert frame.shape == (64, 64)
        assert frame.shaped == (1, 1, 64, 64, 1)
        assert frame.ndim == 2
        assert frame.size == 4096
        assert frame.nbytes == 8192
        assert frame.axes == 'YX'
        assert frame._nextifd() == 819200206
        assert frame.is_final
        assert frame.is_contiguous
        assert frame.is_memmappable
        assert frame.hash
        assert frame.decode
        if not SKIP_ZARR:
            assert frame.aszarr()
        # assert ImageJ tags
        ijmeta = tif.imagej_metadata
        assert ijmeta is not None
        assert ijmeta['ImageJ'] == '1.48g'
        assert round(abs(ijmeta['max'] - 119.0), 7) == 0
        assert round(abs(ijmeta['min'] - 86.0), 7) == 0
        # assert data
        data = tif.asarray()
        assert data.flags['C_CONTIGUOUS']
        assert data.shape == (100000, 64, 64)
        assert data.dtype == numpy.uint16
        assert round(abs(data[7310, 25, 25] - 100), 7) == 0
        if not SKIP_SLOW:
            assert_aszarr_method(tif, data)
        del data
        assert__str__(tif, 0)


@pytest.mark.skipif(SKIP_FILE or SKIP_LARGE, reason=REASON)
def test_read_chart_bl():
    """Test read 13228x18710, 1 bit, no bitspersample tag."""
    filename = _file('tifffile/chart_bl.tif')
    with TiffFile(filename) as tif:
        assert tif.byteorder == '<'
        assert len(tif.pages) == 1
        assert len(tif.series) == 1
        # assert page properties
        page = tif.pages.first
        assert page.compression == COMPRESSION.NONE
        assert page.imagewidth == 13228
        assert page.imagelength == 18710
        assert page.bitspersample == 1
        assert page.samplesperpixel == 1
        assert page.rowsperstrip == 18710
        # assert series properties
        series = tif.series[0]
        assert series.shape == (18710, 13228)
        assert series.dtype == numpy.bool_
        assert series.axes == 'YX'
        assert series.kind == 'uniform'
        # assert data
        data = tif.asarray()
        assert isinstance(data, numpy.ndarray)
        assert data.flags['C_CONTIGUOUS']
        assert data.shape == (18710, 13228)
        assert data.dtype == numpy.bool_
        assert data[0, 0] is numpy.bool_(True)  # noqa: FBT003
        assert data[5000, 5000] is numpy.bool_(False)  # noqa: FBT003
        if not SKIP_LARGE:
            assert_aszarr_method(tif, data)
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE or SKIP_LARGE, reason=REASON)
def test_read_srtm_20_13():
    """Test read 6000x6000 int16 GDAL."""
    filename = _file('large/srtm_20_13.tif')
    with TiffFile(filename) as tif:
        assert tif.byteorder == '<'
        assert len(tif.pages) == 1
        assert len(tif.series) == 1
        # assert page properties
        page = tif.pages.first
        assert page.is_contiguous
        assert page.compression == COMPRESSION.NONE
        assert page.imagewidth == 6000
        assert page.imagelength == 6000
        assert page.bitspersample == 16
        assert page.samplesperpixel == 1
        assert page.nodata == -32768
        assert page.tags['GDAL_NODATA'].value == '-32768'
        assert page.tags['GeoAsciiParamsTag'].value == 'WGS 84|'
        # assert series properties
        series = tif.series[0]
        assert series.shape == (6000, 6000)
        assert series.dtype == numpy.int16
        assert series.axes == 'YX'
        assert series.kind == 'uniform'
        # assert data
        data = tif.asarray()
        assert isinstance(data, numpy.ndarray)
        assert data.flags['C_CONTIGUOUS']
        assert data.shape == (6000, 6000)
        assert data.dtype == numpy.int16
        assert data[5199, 5107] == 1019
        assert data[0, 0] == -32768
        assert_aszarr_method(tif, data)
        del data
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE or SKIP_CODECS or SKIP_LARGE, reason=REASON)
def test_read_gel_scan():
    """Test read 6976x4992x3 uint8 LZW."""
    filename = _file('large/gel_1-scan2.tif')
    with TiffFile(filename) as tif:
        assert tif.byteorder == '<'
        assert len(tif.pages) == 1
        assert len(tif.series) == 1
        # assert page properties
        page = tif.pages.first
        assert page.photometric == PHOTOMETRIC.RGB
        assert page.compression == COMPRESSION.LZW
        assert page.imagewidth == 4992
        assert page.imagelength == 6976
        assert page.bitspersample == 8
        assert page.samplesperpixel == 3
        # assert series properties
        series = tif.series[0]
        assert series.shape == (6976, 4992, 3)
        assert series.dtype == numpy.uint8
        assert series.axes == 'YXS'
        assert series.kind == 'uniform'
        # assert data
        data = tif.asarray()
        assert isinstance(data, numpy.ndarray)
        assert data.flags['C_CONTIGUOUS']
        assert data.shape == (6976, 4992, 3)
        assert data.dtype == numpy.uint8
        assert tuple(data[2229, 1080, :]) == (164, 164, 164)
        assert_aszarr_method(tif, data)
        del data
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_read_caspian():
    """Test read 3x220x279 float64, RGB, deflate, GDAL."""
    filename = _file('juicypixels/caspian.tif')
    with TiffFile(filename) as tif:
        assert tif.byteorder == '<'
        assert len(tif.pages) == 1
        assert len(tif.series) == 1
        # assert page properties
        page = tif.pages.first
        assert page.photometric == PHOTOMETRIC.RGB
        assert page.planarconfig == PLANARCONFIG.SEPARATE
        assert page.compression == COMPRESSION.DEFLATE
        assert page.imagewidth == 279
        assert page.imagelength == 220
        assert page.bitspersample == 64
        assert page.samplesperpixel == 3
        assert page.tags['GDAL_METADATA'].value.startswith('<GDALMetadata>')
        # assert series properties
        series = tif.series[0]
        assert series.shape == (3, 220, 279)
        assert series.dtype == numpy.float64
        assert series.axes == 'SYX'
        assert series.kind == 'uniform'
        # assert data
        data = tif.asarray()
        assert isinstance(data, numpy.ndarray)
        assert data.flags['C_CONTIGUOUS']
        assert data.shape == (3, 220, 279)
        assert data.dtype == numpy.float64
        assert round(abs(data[2, 100, 140] - 353.0), 7) == 0
        assert_aszarr_method(tif, data)
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_read_subifds_array():
    """Test read SubIFDs."""
    filename = _file('Tiff-Library-4J/IFD struct/SubIFDs array E.tif')
    with TiffFile(filename) as tif:
        assert len(tif.pages) == 1

        # make sure no pyramid was detected
        assert len(tif.series) == 5
        assert tif.series[0].shape == (1500, 2000, 3)
        assert tif.series[1].shape == (1200, 1600, 3)
        assert tif.series[2].shape == (900, 1200, 3)
        assert tif.series[3].shape == (600, 800, 3)
        assert tif.series[4].shape == (300, 400, 3)

        page = tif.pages.first
        assert page.photometric == PHOTOMETRIC.RGB
        assert page.imagewidth == 2000
        assert page.imagelength == 1500
        assert page.bitspersample == 8
        assert page.samplesperpixel == 3
        assert page.tags['SubIFDs'].value == (
            14760220,
            18614796,
            19800716,
            18974964,
        )
        # assert subifds
        assert len(page.pages) == 4
        page = tif.pages.first.pages[0]
        assert page.photometric == PHOTOMETRIC.RGB
        assert page.imagewidth == 1600
        assert page.imagelength == 1200
        assert_aszarr_method(page)
        page = tif.pages.first.pages[1]
        assert page.photometric == PHOTOMETRIC.RGB
        assert page.imagewidth == 1200
        assert page.imagelength == 900
        assert_aszarr_method(page)
        page = tif.pages.first.pages[2]
        assert page.photometric == PHOTOMETRIC.RGB
        assert page.imagewidth == 800
        assert page.imagelength == 600
        assert_aszarr_method(page)
        page = tif.pages.first.pages[3]
        assert page.photometric == PHOTOMETRIC.RGB
        assert page.imagewidth == 400
        assert page.imagelength == 300
        assert_aszarr_method(page)
        # assert data
        image = page.asarray()
        assert image.flags['C_CONTIGUOUS']
        assert image.shape == (300, 400, 3)
        assert image.dtype == numpy.uint8
        assert tuple(image[124, 292]) == (236, 109, 95)
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_read_subifd4():
    """Test read BigTIFFSubIFD4."""
    filename = _file('twelvemonkeys/bigtiff/BigTIFFSubIFD4.tif')
    with TiffFile(filename) as tif:
        assert len(tif.series) == 1
        assert len(tif.pages) == 2
        page = tif.pages.first
        assert page.photometric == PHOTOMETRIC.RGB
        assert page.imagewidth == 64
        assert page.imagelength == 64
        assert page.bitspersample == 8
        assert page.samplesperpixel == 3
        assert page.tags['SubIFDs'].value == (3088,)
        # assert subifd
        page = page.pages[0]
        assert page.photometric == PHOTOMETRIC.RGB
        assert page.imagewidth == 32
        assert page.imagelength == 32
        assert page.bitspersample == 8
        assert page.samplesperpixel == 3
        # assert data
        image = page.asarray()
        assert image.flags['C_CONTIGUOUS']
        assert image.shape == (32, 32, 3)
        assert image.dtype == numpy.uint8
        assert image[15, 15, 0] == 255
        assert image[16, 16, 2] == 0
        assert_aszarr_method(page)
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_read_subifd8():
    """Test read BigTIFFSubIFD8."""
    filename = _file('twelvemonkeys/bigtiff/BigTIFFSubIFD8.tif')
    with TiffFile(filename) as tif:
        assert len(tif.series) == 1
        assert len(tif.pages) == 2
        page = tif.pages.first
        assert page.photometric == PHOTOMETRIC.RGB
        assert page.imagewidth == 64
        assert page.imagelength == 64
        assert page.bitspersample == 8
        assert page.samplesperpixel == 3
        assert page.tags['SubIFDs'].value == (3088,)
        # assert subifd
        page = page.pages[0]
        assert page.photometric == PHOTOMETRIC.RGB
        assert page.imagewidth == 32
        assert page.imagelength == 32
        assert page.bitspersample == 8
        assert page.samplesperpixel == 3
        # assert data
        image = page.asarray()
        assert image.flags['C_CONTIGUOUS']
        assert image.shape == (32, 32, 3)
        assert image.dtype == numpy.uint8
        assert image[15, 15, 0] == 255
        assert image[16, 16, 2] == 0
        assert_aszarr_method(page)
        assert__str__(tif)


@pytest.mark.skipif(
    SKIP_CODECS or not imagecodecs.JPEG.available, reason=REASON
)
def test_read_tiles():
    """Test iteration over tiles, manually and via page.segments."""
    data = numpy.arange(600 * 500 * 3, dtype=numpy.uint8).reshape(
        (600, 500, 3)
    )
    with TempFileName('read_tiles') as filename:
        with TiffWriter(filename) as tif:
            options = {
                'tile': (256, 256),
                'photometric': PHOTOMETRIC.RGB,
                'compression': COMPRESSION.JPEG,
                'metadata': None,
            }
            tif.write(data, **options)
            tif.write(
                data[::2, ::2], subfiletype=FILETYPE.REDUCEDIMAGE, **options
            )
        with TiffFile(filename) as tif:
            fh = tif.filehandle
            for page in tif.pages:
                segments = page.segments()
                jpegtables = page.tags.get('JPEGTables', None)
                if jpegtables is not None:
                    jpegtables = jpegtables.value
                for index, (offset, bytecount) in enumerate(
                    zip(page.dataoffsets, page.databytecounts, strict=True)
                ):
                    fh.seek(offset)
                    data = fh.read(bytecount)
                    tile, _indices, _shape = page.decode(
                        data, index, jpegtables=jpegtables
                    )
                    assert_array_equal(tile, next(segments)[0])


@pytest.mark.skipif(SKIP_FILE or SKIP_LARGE, reason=REASON)
def test_read_lsm_mosaic():
    """Test read LSM: PTZCYX (Mosaic mode), two areas, 32 samples, >4 GB."""
    # LSM files are little endian with two series, one of which is reduced RGB
    # Tags may be unordered or contain bogus values
    filename = _file('lsm/Twoareas_Zstacks54slices_3umintervals_5cycles.lsm')
    with TiffFile(filename) as tif:
        assert tif.is_lsm
        assert tif.byteorder == '<'
        assert len(tif.pages) == 1080
        assert len(tif.series) == 2
        # assert page properties
        page = tif.pages.first
        assert page.is_lsm
        assert page.is_contiguous
        assert page.compression == COMPRESSION.NONE
        assert page.imagewidth == 512
        assert page.imagelength == 512
        assert page.bitspersample == 16
        assert page.samplesperpixel == 32
        # assert strip offsets are corrected
        page = tif.pages[-2]
        assert page.dataoffsets[0] == 9070895981
        # assert series properties
        series = tif.series[0]
        assert series.shape == (2, 5, 54, 32, 512, 512)
        assert series.dtype == numpy.uint16
        assert series.axes == 'PTZCYX'
        assert series.kind == 'lsm'
        if 1:
            series = tif.series[1]
            assert series.shape == (2, 5, 54, 3, 128, 128)
            assert series.dtype == numpy.uint8
            assert series.axes == 'PTZSYX'
            assert series.kind == 'lsm'
        # assert lsm_info tags
        tags = tif.lsm_metadata
        assert tags['DimensionX'] == 512
        assert tags['DimensionY'] == 512
        assert tags['DimensionZ'] == 54
        assert tags['DimensionTime'] == 5
        assert tags['DimensionChannels'] == 32
        # assert lsm_scan_info tags
        tags = tif.lsm_metadata['ScanInformation']
        assert tags['ScanMode'] == 'Stack'
        assert tags['User'] == 'lfdguest1'
        if not SKIP_SLOW:
            assert_aszarr_method(tif)
        assert__str__(tif, 0)


@pytest.mark.skipif(SKIP_FILE or SKIP_LARGE, reason=REASON)
def test_read_lsm_carpet():
    """Test read LSM: TYX (time series x-y), 72000 pages."""
    # reads very slowly, ensure colormap is not applied
    filename = _file('lsm/Cardarelli_carpet_3.lsm')
    with TiffFile(filename) as tif:
        assert tif.is_lsm
        assert tif.byteorder == '<'
        assert len(tif.pages) == 72000
        assert len(tif.series) == 2
        # assert page properties
        page = tif.pages.first
        assert page.is_lsm
        assert 'ColorMap' in page.tags
        assert page.photometric == PHOTOMETRIC.PALETTE
        assert page.compression == COMPRESSION.NONE
        assert page.imagewidth == 32
        assert page.imagelength == 10
        assert page.bitspersample == 8
        assert page.samplesperpixel == 1
        # assert series properties
        series = tif.series[0]
        assert series.dtype == numpy.uint8
        assert series.shape == (36000, 10, 32)
        assert series.axes == 'TYX'
        assert series.kind == 'lsm'
        assert series.get_shape(squeeze=False) == (36000, 1, 10, 32)
        assert series.get_axes(squeeze=False) == 'TCYX'
        if 1:
            series = tif.series[1]
            assert series.dtype == numpy.uint8
            assert series.shape == (36000, 3, 40, 128)
            assert series.axes == 'TSYX'
            assert series.get_shape(squeeze=False) == (36000, 3, 40, 128)
            assert series.get_axes(squeeze=False) == 'TSYX'
            assert series.kind == 'lsm'
        # assert lsm_info tags
        tags = tif.lsm_metadata
        assert tags['DimensionX'] == 32
        assert tags['DimensionY'] == 10
        assert tags['DimensionZ'] == 1
        assert tags['DimensionTime'] == 36000
        assert tags['DimensionChannels'] == 1
        # assert lsm_scan_info tags
        tags = tif.lsm_metadata['ScanInformation']
        assert tags['ScanMode'] == 'Plane'
        assert tags['User'] == 'LSM User'
        # assert_aszarr_method(tif)
        assert__str__(tif, 0)


@pytest.mark.skipif(SKIP_FILE or SKIP_LARGE, reason=REASON)
def test_read_lsm_sfcs():
    """Test read LSM linescan: TX (time series y), 1 page."""
    # second page/series is corrupted: ImageLength=128 but StripByteCounts=1
    filename = _file('lsm/sFCS_780_2069Hz_3p94us.lsm')
    with TiffFile(filename) as tif:
        assert tif.is_lsm
        assert tif.byteorder == '<'
        assert len(tif.pages) == 2
        assert len(tif.series) == 2
        # assert page properties
        page = tif.pages.first
        assert page.is_lsm
        assert 'ColorMap' in page.tags
        assert page.photometric == PHOTOMETRIC.PALETTE
        assert page.compression == COMPRESSION.NONE
        assert page.imagewidth == 52
        assert page.imagelength == 100000
        assert page.bitspersample == 16
        assert page.samplesperpixel == 1
        # assert series properties
        series = tif.series[0]
        assert series.dtype == numpy.uint16
        assert series.shape == (100000, 52)
        assert series.axes == 'TX'
        assert series.kind == 'lsm'
        assert series.get_shape(squeeze=False) == (1, 1, 1, 100000, 52)
        assert series.get_axes(squeeze=False) == 'MPCTX'
        data = series.asarray()
        assert data.shape == (100000, 52)
        assert data.dtype == numpy.uint16
        assert data[1000].sum() == 4

        if 1:
            series = tif.series[1]
            assert series.dtype == numpy.uint8
            assert series.shape == (3, 128, 1)
            assert series.axes == 'SYX'
            assert series.get_shape(squeeze=False) == (3, 128, 1)
            assert series.get_axes(squeeze=False) == 'SYX'
            assert series.kind == 'lsm'
            with pytest.raises(TiffFileError):
                # strip cannot be reshaped from (1,) to (1, 128, 1, 1)
                series.asarray()
        # assert lsm_info tags
        tags = tif.lsm_metadata
        assert tags['DimensionX'] == 52
        assert tags['DimensionY'] == 1
        assert tags['DimensionZ'] == 1
        assert tags['DimensionTime'] == 100000
        assert tags['DimensionChannels'] == 1
        # assert lsm_scan_info tags
        tags = tif.lsm_metadata['ScanInformation']
        assert tags['ScanMode'] == 'Line'
        assert tags['User'] == 'LSM User'
        # assert_aszarr_method(tif)
        assert__str__(tif, 0)


@pytest.mark.skipif(SKIP_FILE or SKIP_LARGE, reason=REASON)
def test_read_lsm_line_2channel():
    """Test read LSM point scan with 2 channels: CTX, 1 page."""
    # https://github.com/cgohlke/tifffile/issues/269
    # second page/series is corrupted: ImageLength=128 but StripByteCounts=1
    filename = _file('lsm/issue269.lsm')
    with TiffFile(filename) as tif:
        assert tif.is_lsm
        assert tif.byteorder == '<'
        assert len(tif.pages) == 2
        assert len(tif.series) == 2
        # assert page properties
        page = tif.pages.first
        assert page.is_lsm
        assert 'ColorMap' not in page.tags
        assert page.photometric == PHOTOMETRIC.RGB
        assert page.compression == COMPRESSION.NONE
        assert page.imagewidth == 512
        assert page.imagelength == 70000
        assert page.bitspersample == 8
        assert page.samplesperpixel == 2
        # assert series properties
        series = tif.series[0]
        assert series.dtype == numpy.uint8
        assert series.shape == (2, 70000, 512)
        assert series.axes == 'CTX'
        assert series.kind == 'lsm'
        assert series.get_axes(squeeze=False) == 'MPCTX'
        assert series.get_shape(squeeze=False) == (1, 1, 2, 70000, 512)
        data = series.asarray()
        assert data.shape == (2, 70000, 512)
        assert data.dtype == numpy.uint8
        assert data[0, 1000].sum() == 13241

        if 1:
            series = tif.series[1]
            assert series.dtype == numpy.uint8
            assert series.shape == (3, 128, 1)
            assert series.axes == 'SYX'
            assert series.get_shape(squeeze=False) == (3, 128, 1)
            assert series.get_axes(squeeze=False) == 'SYX'
            assert series.kind == 'lsm'
            with pytest.raises(TiffFileError):
                # strip cannot be reshaped from (1,) to (1, 128, 1, 1)
                series.asarray()
        # assert lsm_info tags
        tags = tif.lsm_metadata
        assert tags['DimensionX'] == 512
        assert tags['DimensionY'] == 1
        assert tags['DimensionZ'] == 1
        assert tags['DimensionTime'] == 70000
        assert tags['DimensionChannels'] == 2
        # assert lsm_scan_info tags
        tags = tif.lsm_metadata['ScanInformation']
        assert tags['ScanMode'] == 'Line'
        assert tags['User'] == 'LSMUser'
        assert_aszarr_method(tif)
        assert__str__(tif, 0)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_read_lsm_take1():
    """Test read LSM: TZCYX (Plane mode), single image, uint8."""
    filename = _file('lsm/take1.lsm')
    with TiffFile(filename) as tif:
        assert tif.is_lsm
        assert tif.byteorder == '<'
        assert len(tif.pages) == 2
        assert len(tif.series) == 2
        # assert page properties
        page = tif.pages.first
        assert page.is_lsm
        assert page.is_contiguous
        assert page.compression == COMPRESSION.NONE
        assert page.imagewidth == 512
        assert page.imagelength == 512
        assert page.bitspersample == 8
        assert page.samplesperpixel == 1
        page = tif.pages[1]
        assert page.is_reduced
        assert page.photometric == PHOTOMETRIC.RGB
        assert page.planarconfig == PLANARCONFIG.SEPARATE
        assert page.compression == COMPRESSION.NONE
        assert page.imagewidth == 128
        assert page.imagelength == 128
        assert page.samplesperpixel == 3
        assert page.bitspersample == 8
        # assert series properties
        series = tif.series[0]
        assert series.dtype == numpy.uint8
        assert series.shape == (512, 512)
        assert series.axes == 'YX'
        assert series.kind == 'lsm'
        assert series.get_shape(squeeze=False) == (1, 1, 512, 512)
        assert series.get_axes(squeeze=False) == 'ZCYX'
        if 1:
            series = tif.series[1]
            assert series.shape == (3, 128, 128)
            assert series.dtype == numpy.uint8
            assert series.axes == 'SYX'
            assert series.kind == 'lsm'
        # assert data
        data = tif.asarray()
        assert isinstance(data, numpy.ndarray)
        assert data.flags['C_CONTIGUOUS']
        assert data.shape == (512, 512)
        assert data.dtype == numpy.uint8
        assert data[256, 256] == 101
        if 1:
            data = tif.asarray(series=1)
            assert isinstance(data, numpy.ndarray)
            assert data.shape == (3, 128, 128)
            assert data.dtype == numpy.uint8
            assert tuple(data[..., 64, 64]) == (89, 89, 89)
        # assert lsm_info tags
        tags = tif.lsm_metadata
        assert tags['DimensionX'] == 512
        assert tags['DimensionY'] == 512
        assert tags['DimensionZ'] == 1
        assert tags['DimensionTime'] == 1
        assert tags['DimensionChannels'] == 1
        # assert lsm_scan_info tags
        tags = tif.lsm_metadata['ScanInformation']
        assert tags['ScanMode'] == 'Plane'
        assert tags['User'] == 'LSM User'
        assert len(tags['Tracks']) == 1
        assert len(tags['Tracks'][0]['DataChannels']) == 1
        track = tags['Tracks'][0]
        assert track['DataChannels'][0]['Name'] == 'Ch1'
        assert track['DataChannels'][0]['BitsPerSample'] == 8
        assert len(track['IlluminationChannels']) == 1
        assert track['IlluminationChannels'][0]['Name'] == '561'
        assert track['IlluminationChannels'][0]['Wavelength'] == 561.0
        assert_aszarr_method(tif)
        assert_aszarr_method(tif, series=1)
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_read_lsm_2chzt():
    """Test read LSM: ZCYX (Stack mode) uint8."""
    filename = _file('scif.io/2chZT.lsm')
    with TiffFile(filename) as tif:
        assert tif.is_lsm
        assert tif.byteorder == '<'
        assert len(tif.pages) == 798
        assert len(tif.series) == 2
        # assert page properties
        page = tif.pages.first
        assert page.is_lsm
        assert page.is_contiguous
        assert page.photometric == PHOTOMETRIC.RGB
        assert page.tags['StripOffsets'].value[2] == 242632  # bogus offset
        assert page.tags['StripByteCounts'].value[2] == 0  # no strip data
        assert page.compression == COMPRESSION.NONE
        assert page.imagewidth == 400
        assert page.imagelength == 300
        assert page.bitspersample == 8
        assert page.samplesperpixel == 2

        page = tif.pages[1]
        assert page.is_reduced
        assert page.photometric == PHOTOMETRIC.RGB
        assert page.planarconfig == PLANARCONFIG.SEPARATE
        assert page.is_contiguous
        assert page.compression == COMPRESSION.NONE
        assert page.imagewidth == 128
        assert page.imagelength == 96
        assert page.samplesperpixel == 3
        assert page.bitspersample == 8
        # assert series properties
        series = tif.series[0]
        assert series.shape == (19, 21, 2, 300, 400)
        assert series.dtype == numpy.uint8
        assert series.axes == 'TZCYX'
        assert series.kind == 'lsm'
        if 1:
            series = tif.series[1]
            assert series.shape == (19, 21, 3, 96, 128)
            assert series.dtype == numpy.uint8
            assert series.axes == 'TZSYX'
            assert series.kind == 'lsm'
        # assert data
        data = tif.asarray(out='memmap')
        assert isinstance(data, numpy.memmap)
        assert data.flags['C_CONTIGUOUS']
        assert data.shape == (19, 21, 2, 300, 400)
        assert data.dtype == numpy.uint8
        assert data[18, 20, 1, 199, 299] == 39
        if 1:
            data = tif.asarray(series=1)
            assert isinstance(data, numpy.ndarray)
            assert data.shape == (19, 21, 3, 96, 128)
            assert data.dtype == numpy.uint8
            assert tuple(data[18, 20, :, 64, 96]) == (22, 22, 0)
        del data
        # assert lsm_info tags
        tags = tif.lsm_metadata
        assert tags['DimensionX'] == 400
        assert tags['DimensionY'] == 300
        assert tags['DimensionZ'] == 21
        assert tags['DimensionTime'] == 19
        assert tags['DimensionChannels'] == 2
        # assert lsm_scan_info tags
        tags = tif.lsm_metadata['ScanInformation']
        assert tags['ScanMode'] == 'Stack'
        assert tags['User'] == 'zjfhe'
        assert len(tags['Tracks']) == 3
        assert len(tags['Tracks'][0]['DataChannels']) == 1
        track = tags['Tracks'][0]
        assert track['DataChannels'][0]['Name'] == 'Ch3'
        assert track['DataChannels'][0]['BitsPerSample'] == 8
        assert len(track['IlluminationChannels']) == 6
        assert track['IlluminationChannels'][5]['Name'] == '488'
        assert track['IlluminationChannels'][5]['Wavelength'] == 488.0
        assert_aszarr_method(tif)
        assert_aszarr_method(tif, series=1)
        assert__str__(tif, 0)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_read_lsm_unbounderror():
    """Test read LSM: MZSYX (196, 2, 33, 512, 512)."""
    # file is > 4GB with no time axis
    filename = _file('lsm/48h-CM-C-2.lsm')
    with TiffFile(filename) as tif:
        assert tif.is_lsm
        assert tif.byteorder == '<'
        assert len(tif.pages) == 784
        assert len(tif.series) == 2
        # assert page properties
        page = tif.pages.first
        assert page.is_lsm
        assert page.is_contiguous
        assert page.photometric == PHOTOMETRIC.RGB
        assert page.planarconfig == PLANARCONFIG.SEPARATE
        assert page.imagewidth == 512
        assert page.imagelength == 512
        assert page.bitspersample == 16
        assert page.samplesperpixel == 33
        page = tif.pages[1]
        assert page.is_reduced
        assert page.photometric == PHOTOMETRIC.RGB
        assert page.planarconfig == PLANARCONFIG.SEPARATE
        assert page.compression == COMPRESSION.NONE
        assert page.imagewidth == 128
        assert page.imagelength == 128
        assert page.samplesperpixel == 3
        assert page.bitspersample == 8
        # assert series properties
        series = tif.series[0]
        assert series.shape == (196, 2, 33, 512, 512)
        assert series.dtype == numpy.uint16
        assert series.axes == 'MZCYX'
        assert series.get_axes(squeeze=False) == 'MPZCYX'
        assert series.get_shape(squeeze=False) == (196, 1, 2, 33, 512, 512)
        assert series.kind == 'lsm'
        if 1:
            series = tif.series[1]
            assert series.shape == (196, 2, 3, 128, 128)
            assert series.dtype == numpy.uint8
            assert series.axes == 'MZSYX'
            assert series.get_axes(squeeze=False) == 'MPZSYX'
            assert series.get_shape(squeeze=False) == (196, 1, 2, 3, 128, 128)
            assert series.kind == 'lsm'
        # assert data
        data = tif.asarray()
        assert isinstance(data, numpy.ndarray)
        assert data.flags['C_CONTIGUOUS']
        assert data.shape == (196, 2, 33, 512, 512)
        assert data.dtype == numpy.uint16
        assert data[195, 1, 32, 511, 511] == 22088
        # assert_aszarr_method(tif, data)
        if 1:
            data = tif.asarray(series=1)
            assert isinstance(data, numpy.ndarray)
            assert data.shape == (196, 2, 3, 128, 128)
            assert data.dtype == numpy.uint8
            assert tuple(data[195, 1, :, 127, 127]) == (0, 61, 52)
            # assert_aszarr_method(tif, series=1)
        # assert lsm_info tags
        tags = tif.lsm_metadata
        assert tags['DimensionX'] == 512
        assert tags['DimensionY'] == 512
        assert tags['DimensionZ'] == 2
        assert tags['DimensionTime'] == 1
        assert tags['DimensionChannels'] == 33
        # assert lsm_scan_info tags
        tags = tif.lsm_metadata['ScanInformation']
        assert tags['ScanMode'] == 'Stack'
        assert tags['User'] == 'lfdguest1'
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE or SKIP_LARGE, reason=REASON)
def test_read_lsm_incomplete(caplog):
    """Test read LSM: MZSYX (196, 2, 33, 512, 512)."""
    # file is ~18 GB incompletely written, dataoffsets zeroed
    filename = _file('lsm/NTERT_NM-50_x2.lsm')
    with TiffFile(filename) as tif:
        assert 'LSM file incompletely written' in caplog.text
        assert tif.is_lsm
        assert tif.byteorder == '<'
        assert len(tif.pages) == 2450
        assert len(tif.series) == 2
        # assert page properties
        page = tif.pages.first
        assert page.is_lsm
        assert page.is_contiguous
        assert page.photometric == PHOTOMETRIC.RGB
        assert page.planarconfig == PLANARCONFIG.SEPARATE
        assert page.imagewidth == 512
        assert page.imagelength == 512
        assert page.bitspersample == 16
        assert page.samplesperpixel == 33
        page = tif.pages[1]
        assert page.is_reduced
        assert page.photometric == PHOTOMETRIC.RGB
        assert page.planarconfig == PLANARCONFIG.SEPARATE
        assert page.compression == COMPRESSION.NONE
        assert page.imagewidth == 128
        assert page.imagelength == 128
        assert page.samplesperpixel == 3
        assert page.bitspersample == 8
        # assert series properties
        series = tif.series[0]
        assert series.pages[-1].dataoffsets[0] == 0
        assert series.shape == (25, 49, 33, 512, 512)
        assert series.dtype == numpy.uint16
        assert series.axes == 'MTCYX'
        assert series.get_axes(squeeze=False) == 'MPTCYX'
        assert series.get_shape(squeeze=False) == (25, 1, 49, 33, 512, 512)
        assert series.kind == 'lsm'
        if 1:
            series = tif.series[1]
            assert series.shape == (25, 49, 3, 128, 128)
            assert series.get_shape(squeeze=False) == (25, 1, 49, 3, 128, 128)
            assert series.dtype == numpy.uint8
            assert series.axes == 'MTSYX'
            assert series.get_axes(squeeze=False) == 'MPTSYX'
            assert series.kind == 'lsm'
        # assert data
        data = tif.asarray()
        assert isinstance(data, numpy.ndarray)
        assert data.flags['C_CONTIGUOUS']
        assert data.shape == (25, 49, 33, 512, 512)
        assert data.dtype == numpy.uint16
        assert data[24, 45, 32, 511, 511] == 14648
        # assert_aszarr_method(tif, data)
        if 1:
            data = tif.asarray(series=1)
            assert isinstance(data, numpy.ndarray)
            assert data.shape == (25, 49, 3, 128, 128)
            assert data.dtype == numpy.uint8
            assert numpy.sum(data[24, 45]) == 0
            # assert_aszarr_method(tif, series=1)
        # assert lsm_info tags
        tags = tif.lsm_metadata
        assert tags['DimensionX'] == 512
        assert tags['DimensionY'] == 512
        assert tags['DimensionZ'] == 1
        assert tags['DimensionTime'] == 49
        assert tags['DimensionChannels'] == 33
        # assert lsm_scan_info tags
        tags = tif.lsm_metadata['ScanInformation']
        assert tags['ScanMode'] == 'Plane'
        assert tags['User'] == 'lfdguest1'
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE or SKIP_CODECS, reason=REASON)
def test_read_lsm_earpax2isl11():
    """Test read LSM: ZCYX (19, 3, 512, 512) uint8, RGB, LZW."""
    filename = _file('lsm/earpax2isl11.lzw.lsm')
    with TiffFile(filename) as tif:
        assert tif.is_lsm
        assert tif.byteorder == '<'
        assert len(tif.pages) == 38
        assert len(tif.series) == 2
        # assert page properties
        page = tif.pages.first
        assert page.is_lsm
        assert not page.is_contiguous
        assert page.photometric == PHOTOMETRIC.RGB
        assert page.compression == COMPRESSION.LZW
        assert page.imagewidth == 512
        assert page.imagelength == 512
        assert page.bitspersample == 8
        assert page.samplesperpixel == 3
        # assert corrected strip_byte_counts
        assert page.tags['StripByteCounts'].value == (262144, 262144, 262144)
        assert page.databytecounts == (131514, 192933, 167874)
        page = tif.pages[1]
        assert page.is_reduced
        assert page.photometric == PHOTOMETRIC.RGB
        assert page.planarconfig == PLANARCONFIG.SEPARATE
        assert page.compression == COMPRESSION.NONE
        assert page.imagewidth == 128
        assert page.imagelength == 128
        assert page.samplesperpixel == 3
        assert page.bitspersample == 8
        # assert series properties
        series = tif.series[0]
        assert series.shape == (19, 3, 512, 512)
        assert series.dtype == numpy.uint8
        assert series.axes == 'ZCYX'
        assert series.get_axes(squeeze=False) == 'ZCYX'
        assert series.get_shape(squeeze=False) == (19, 3, 512, 512)
        assert series.kind == 'lsm'
        if 1:
            series = tif.series[1]
            assert series.shape == (19, 3, 128, 128)
            assert series.get_shape(squeeze=False) == (19, 3, 128, 128)
            assert series.dtype == numpy.uint8
            assert series.axes == 'ZSYX'
            assert series.get_axes(squeeze=False) == 'ZSYX'
            assert series.kind == 'lsm'
        # assert data
        data = tif.asarray()
        assert isinstance(data, numpy.ndarray)
        assert data.flags['C_CONTIGUOUS']
        assert data.shape == (19, 3, 512, 512)
        assert data.dtype == numpy.uint8
        assert tuple(data[18, :, 200, 320]) == (17, 22, 21)
        assert_aszarr_method(tif, data)
        if 1:
            data = tif.asarray(series=1)
            assert isinstance(data, numpy.ndarray)
            assert data.shape == (19, 3, 128, 128)
            assert data.dtype == numpy.uint8
            assert tuple(data[18, :, 64, 64]) == (25, 5, 33)
            assert_aszarr_method(tif, series=1)
        # assert lsm_info tags
        tags = tif.lsm_metadata
        assert tags['DimensionX'] == 512
        assert tags['DimensionY'] == 512
        assert tags['DimensionZ'] == 19
        assert tags['DimensionTime'] == 1
        assert tags['DimensionChannels'] == 3
        # assert lsm_scan_info tags
        tags = tif.lsm_metadata['ScanInformation']
        assert tags['ScanMode'] == 'Stack'
        assert tags['User'] == 'megason'
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE or SKIP_CODECS or SKIP_LARGE, reason=REASON)
def test_read_lsm_mb231paxgfp_060214():
    """Test read LSM with many LZW compressed pages."""
    # TZCYX (Stack mode), (60, 31, 2, 512, 512), 3720
    filename = _file('tifffile/MB231paxgfp_060214.lzw.lsm')
    with TiffFile(filename) as tif:
        assert tif.is_lsm
        assert tif.byteorder == '<'
        assert len(tif.pages) == 3720
        assert len(tif.series) == 2
        # assert page properties
        page = tif.pages.first
        assert page.is_lsm
        assert not page.is_contiguous
        assert page.compression == COMPRESSION.LZW
        assert page.imagewidth == 512
        assert page.imagelength == 512
        assert page.bitspersample == 16
        assert page.samplesperpixel == 2
        page = tif.pages[1]
        assert page.is_reduced
        assert page.photometric == PHOTOMETRIC.RGB
        assert page.planarconfig == PLANARCONFIG.SEPARATE
        assert page.compression == COMPRESSION.NONE
        assert page.imagewidth == 128
        assert page.imagelength == 128
        assert page.samplesperpixel == 3
        assert page.bitspersample == 8
        # assert series properties
        series = tif.series[0]
        assert series.dtype == numpy.uint16
        assert series.shape == (60, 31, 2, 512, 512)
        assert series.get_shape(squeeze=False) == (60, 31, 2, 512, 512)
        assert series.axes == 'TZCYX'
        assert series.get_axes(squeeze=False) == 'TZCYX'
        assert series.kind == 'lsm'
        if 1:
            series = tif.series[1]
            assert series.dtype == numpy.uint8
            assert series.shape == (60, 31, 3, 128, 128)
            assert series.axes == 'TZSYX'
            assert series.kind == 'lsm'
        # assert data
        data = tif.asarray(out='memmap', maxworkers=None)
        assert isinstance(data, numpy.memmap)
        assert data.flags['C_CONTIGUOUS']
        assert data.shape == (60, 31, 2, 512, 512)
        assert data.dtype == numpy.dtype('<u2')
        assert data[59, 30, 1, 256, 256] == 222
        del data
        # assert lsm_info tags
        tags = tif.lsm_metadata
        assert tags['DimensionX'] == 512
        assert tags['DimensionY'] == 512
        assert tags['DimensionZ'] == 31
        assert tags['DimensionTime'] == 60
        assert tags['DimensionChannels'] == 2
        # assert some lsm_scan_info tags
        tags = tif.lsm_metadata['ScanInformation']
        assert tags['ScanMode'] == 'Stack'
        assert tags['User'] == 'lfdguest1'
        assert__str__(tif, 0)


@pytest.mark.skipif(SKIP_FILE or SKIP_CODECS, reason=REASON)
def test_read_lsm_lzw_no_eoi():
    """Test read LSM with LZW compressed strip without EOI."""
    # The first LZW compressed strip in page 834 has no EOI
    # such that too much data is returned from the decoder and
    # the data of the 2nd channel was getting corrupted
    filename = _file('tifffile/MB231paxgfp_060214.lzw.lsm')
    with TiffFile(filename) as tif:
        assert tif.is_lsm
        assert tif.byteorder == '<'
        assert len(tif.pages) == 3720
        assert len(tif.series) == 2
        # assert page properties
        page = tif.pages.first
        assert not page.is_contiguous
        assert page.photometric == PHOTOMETRIC.RGB
        assert page.compression == COMPRESSION.LZW
        assert page.imagewidth == 512
        assert page.imagelength == 512
        assert page.bitspersample == 16
        assert page.samplesperpixel == 2
        page = tif.pages[834]
        assert isinstance(page, TiffFrame)
        assert page.dataoffsets == (344655101, 345109987)
        assert page.databytecounts == (454886, 326318)
        # assert second channel is not corrupted
        data = page.asarray()
        assert tuple(data[:, 0, 0]) == (288, 238)
        assert__str__(tif, 0)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_stk_nonstack():
    """Test read MetaMorph STK with single plane."""
    # https://github.com/cgohlke/tifffile/pull/217
    filename = _file('stk/test_w1491-Fast_s1_t1.tif')
    with TiffFile(filename) as tif:
        assert tif.is_stk
        assert tif.byteorder == '<'
        assert len(tif.pages) == 1
        assert len(tif.series) == 1
        # assert page properties
        page = tif.pages.first
        assert page.is_contiguous
        assert page.compression == COMPRESSION.NONE
        assert page.imagewidth == 24
        assert page.imagelength == 24
        assert page.bitspersample == 16
        assert page.samplesperpixel == 1
        assert page.tags['Software'].value == 'MetaMorph 7.8.3.0'
        assert page.tags['DateTime'].value == '2023:08:07 15:01:32'
        assert page.datetime == datetime.datetime(2023, 8, 7, 15, 1, 32)
        assert page.description.startswith('Exposure: 150 ms')
        meta = stk_description_metadata(page.description)
        assert meta[0]['Exposure'] == '150 ms'
        # assert uic tags
        tags = tif.stk_metadata
        assert 'ZDistance' not in tags
        assert 'Wavelengths' not in tags
        assert tags['Name'] == '491_Fast'
        assert tags['NumberPlanes'] == 1
        assert ''.join(tags['StageLabel']) == 'position_a'
        assert len(tags['AbsoluteZ']) == 1
        assert tags['AbsoluteZ'][0] == 4294085.491
        assert tuple(tags['StagePosition'][0]) == (16204.6, 444.7)
        assert tuple(tags['CameraChipOffset'][0]) == (192.0, 219.0)
        assert tags['PlaneDescriptions'][0].startswith('Exposure: 150 ms')
        assert len(tags['PlaneProperty']) == 23
        assert tags['PlaneProperty'][-1] == {
            'name': 'Ti TIRF Angle',
            'flags': 0,
            'value': 0.0,
        }
        # assert series properties
        series = tif.series[0]
        assert not series.is_truncated
        assert series.shape == (24, 24)
        assert series.dtype == numpy.uint16
        assert series.axes == 'YX'
        assert series.kind == 'stk'
        # assert data
        data = tif.asarray()
        assert isinstance(data, numpy.ndarray)
        assert data.shape == (24, 24)
        assert data.dtype == numpy.uint16
        assert data[9, 11] == 3453
        assert_aszarr_method(tif, data)
        assert_decode_method(page)
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_read_stk_zseries():
    """Test read MetaMorph STK z-series."""
    filename = _file('stk/zseries.stk')
    with TiffFile(filename) as tif:
        assert tif.is_stk
        assert tif.byteorder == '<'
        assert len(tif.pages) == 1
        assert len(tif.series) == 1
        # assert page properties
        page = tif.pages.first
        assert page.is_contiguous
        assert page.compression == COMPRESSION.NONE
        assert page.imagewidth == 320
        assert page.imagelength == 256
        assert page.bitspersample == 16
        assert page.samplesperpixel == 1
        assert page.tags['Software'].value == 'MetaMorph'
        assert page.tags['DateTime'].value == '2000:01:02 15:06:33'
        assert page.datetime == datetime.datetime(2000, 1, 2, 15, 6, 33)
        assert page.description.startswith('Acquired from MV-1500')
        meta = stk_description_metadata(page.description)
        assert meta[0]['Exposure'] == '2 ms'
        # assert uic tags
        tags = tif.stk_metadata
        assert tags['Name'] == 'Z Series'
        assert tags['NumberPlanes'] == 11
        assert ''.join(tags['StageLabel']) == ''
        assert tags['ZDistance'][10] == 2.5
        assert len(tags['Wavelengths']) == 11
        assert tags['Wavelengths'][10] == 490.0
        assert len(tags['AbsoluteZ']) == 11
        assert tags['AbsoluteZ'][10] == 150.0
        assert tuple(tags['StagePosition'][10]) == (0.0, 0.0)
        assert tuple(tags['CameraChipOffset'][10]) == (0.0, 0.0)
        assert tags['PlaneDescriptions'][0].startswith('Acquired from MV-1500')
        assert str(tags['DatetimeCreated'][0]) == (
            '2000-02-02T15:06:02.783000000'
        )
        # assert series properties
        series = tif.series[0]
        assert series.is_truncated
        assert series.shape == (11, 256, 320)
        assert series.dtype == numpy.uint16
        assert series.axes == 'ZYX'
        assert series.kind == 'stk'
        # assert data
        data = tif.asarray()
        assert isinstance(data, numpy.ndarray)
        assert data.shape == (11, 256, 320)
        assert data.dtype == numpy.uint16
        assert data[8, 159, 255] == 1156
        assert_aszarr_method(tif, data)
        assert_decode_method(page)
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_read_stk_zser24():
    """Test read MetaMorph STK RGB z-series."""
    filename = _file('stk/zser24.stk')
    with TiffFile(filename) as tif:
        assert tif.is_stk
        assert tif.byteorder == '<'
        assert len(tif.pages) == 1
        assert len(tif.series) == 1
        # assert page properties
        page = tif.pages.first
        assert page.is_contiguous
        assert page.photometric == PHOTOMETRIC.RGB
        assert page.compression == COMPRESSION.NONE
        assert page.imagewidth == 160
        assert page.imagelength == 128
        assert page.bitspersample == 8
        assert page.samplesperpixel == 3
        assert page.tags['Software'].value == 'MetaMorph'
        assert page.tags['DateTime'].value == '2000:01:02 15:11:23'
        # assert uic tags
        tags = tif.stk_metadata
        assert tags['Name'] == 'Color Encoded'
        assert tags['NumberPlanes'] == 11
        assert ''.join(tags['StageLabel']) == ''
        assert tags['ZDistance'][10] == 2.5
        assert len(tags['Wavelengths']) == 11
        assert tags['Wavelengths'][10] == 510.0
        assert len(tags['AbsoluteZ']) == 11
        assert tags['AbsoluteZ'][10] == 150.0
        assert tuple(tags['StagePosition'][10]) == (0.0, 0.0)
        assert tuple(tags['CameraChipOffset'][10]) == (320.0, 256.0)
        assert str(tags['DatetimeCreated'][0]) == (
            '2000-02-02T15:10:34.264000000'
        )
        # assert series properties
        series = tif.series[0]
        assert series.is_truncated
        assert series.shape == (11, 128, 160, 3)
        assert series.dtype == numpy.uint8
        assert series.axes == 'ZYXS'
        assert series.kind == 'stk'
        # assert data
        data = tif.asarray()
        assert isinstance(data, numpy.ndarray)
        assert data.shape == (11, 128, 160, 3)
        assert data.dtype == numpy.uint8
        assert tuple(data[8, 100, 135]) == (70, 63, 0)
        assert_aszarr_method(tif, data)
        assert_decode_method(page)
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_read_stk_diatoms3d():
    """Test read MetaMorph STK time-series."""
    filename = _file('stk/diatoms3d.stk')
    with TiffFile(filename) as tif:
        assert tif.is_stk
        assert tif.byteorder == '<'
        assert len(tif.pages) == 1
        assert len(tif.series) == 1
        # assert page properties
        page = tif.pages.first
        assert page.is_contiguous
        assert page.compression == COMPRESSION.NONE
        assert page.imagewidth == 196
        assert page.imagelength == 191
        assert page.bitspersample == 8
        assert page.samplesperpixel == 1
        assert page.tags['Software'].value == 'MetaMorph'
        assert page.tags['DateTime'].value == '2000:01:04 14:57:22'
        # assert uic tags
        tags = tif.stk_metadata
        assert tags['Name'] == 'diatoms3d'
        assert tags['NumberPlanes'] == 10
        assert ''.join(tags['StageLabel']) == ''
        assert tags['ZDistance'][9] == 3.54545
        assert len(tags['Wavelengths']) == 10
        assert tags['Wavelengths'][9] == 440.0
        assert len(tags['AbsoluteZ']) == 10
        assert tags['AbsoluteZ'][9] == 12898.15
        assert tuple(tags['StagePosition'][9]) == (0.0, 0.0)
        assert tuple(tags['CameraChipOffset'][9]) == (206.0, 148.0)
        assert tags['PlaneDescriptions'][0].startswith(
            'Acquired from Flashbus.'
        )
        assert str(tags['DatetimeCreated'][0]) == (
            '2000-02-04T14:38:37.738000000'
        )
        # assert series properties
        series = tif.series[0]
        assert series.shape == (10, 191, 196)
        assert series.dtype == numpy.uint8
        assert series.axes == 'ZYX'
        assert series.kind == 'stk'
        # assert data
        data = tif.asarray()
        assert isinstance(data, numpy.ndarray)
        assert data.shape == (10, 191, 196)
        assert data.dtype == numpy.uint8
        assert data[8, 100, 135] == 223
        assert_aszarr_method(tif, data)
        assert_decode_method(page)
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_read_stk_greenbeads():
    """Test read MetaMorph STK time-series, but time_created is corrupt (?)."""
    # 8bit palette is present but should not be applied
    filename = _file('stk/GreenBeads.stk')
    with TiffFile(filename) as tif:
        assert tif.is_stk
        assert tif.byteorder == '<'
        assert len(tif.pages) == 1
        assert len(tif.series) == 1
        # assert page properties
        page = tif.pages.first
        assert page.is_contiguous
        assert page.photometric == PHOTOMETRIC.PALETTE
        assert page.compression == COMPRESSION.NONE
        assert page.imagewidth == 298
        assert page.imagelength == 322
        assert page.bitspersample == 8
        assert page.samplesperpixel == 1
        assert page.tags['Software'].value == 'MetaMorph 7.5.3.0'
        assert page.tags['DateTime'].value == '2008:05:09 17:35:32'
        # assert uic tags
        tags = tif.stk_metadata
        assert tags['Name'] == 'Green'
        assert tags['NumberPlanes'] == 79
        assert tags['ZDistance'][1] == 0.0
        assert len(tags['Wavelengths']) == 79
        assert tuple(tags['CameraChipOffset'][0]) == (0.0, 0.0)
        assert str(tags['DatetimeModified'][0]) == (
            '2008-05-09T17:35:33.274000000'
        )
        # assert tags['AbsoluteZ'][78] == 1.1672684733218932
        assert 'AbsoluteZ' not in tags
        # assert series properties
        series = tif.series[0]
        assert series.shape == (79, 322, 298)
        assert series.dtype == numpy.uint8
        assert series.axes == 'IYX'  # corrupt time_created
        assert series.kind == 'stk'
        # assert data
        data = tif.asarray()
        assert isinstance(data, numpy.ndarray)
        assert data.shape == (79, 322, 298)
        assert data.dtype == numpy.uint8
        assert data[43, 180, 102] == 205
        assert_aszarr_method(tif, data)
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_read_stk_10xcalib():
    """Test read MetaMorph STK two planes, not Z or T series."""
    filename = _file('stk/10xcalib.stk')
    with TiffFile(filename) as tif:
        assert tif.is_stk
        assert tif.byteorder == '<'
        assert len(tif.pages) == 1
        assert len(tif.series) == 1
        # assert page properties
        page = tif.pages.first
        assert page.is_contiguous
        assert page.photometric != PHOTOMETRIC.PALETTE
        assert page.compression == COMPRESSION.NONE
        assert page.imagewidth == 640
        assert page.imagelength == 480
        assert page.bitspersample == 8
        assert page.samplesperpixel == 1
        assert page.tags['Software'].value == 'MetaMorph'
        assert page.tags['DateTime'].value == '2000:03:28 09:24:37'
        # assert uic tags
        tags = tif.stk_metadata
        assert tags['Name'] == '10xcalib'
        assert tags['NumberPlanes'] == 2
        assert tuple(tags['Wavelengths']) == (440.0, 440.0)
        assert tags['XCalibration'] == 1.24975007
        assert tags['YCalibration'] == 1.24975007
        # assert series properties
        series = tif.series[0]
        assert series.shape == (2, 480, 640)
        assert series.dtype == numpy.uint8
        assert series.axes == 'IYX'
        assert series.kind == 'stk'
        # assert data
        data = tif.asarray()
        assert isinstance(data, numpy.ndarray)
        assert data.shape == (2, 480, 640)
        assert data.dtype == numpy.uint8
        assert data[1, 339, 579] == 56
        assert_aszarr_method(tif, data)
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE or SKIP_LARGE, reason=REASON)
def test_read_stk_112508h100():
    """Test read MetaMorph STK large time-series."""
    filename = _file('stk/112508h100.stk')
    with TiffFile(filename) as tif:
        assert tif.is_stk
        assert tif.byteorder == '<'
        assert len(tif.pages) == 1
        assert len(tif.series) == 1
        # assert page properties
        page = tif.pages.first
        assert page.is_contiguous
        assert page.photometric != PHOTOMETRIC.PALETTE
        assert page.compression == COMPRESSION.NONE
        assert page.imagewidth == 512
        assert page.imagelength == 128
        assert page.bitspersample == 16
        assert page.samplesperpixel == 1
        assert page.tags['Software'].value == 'MetaMorph 7.5.3.0'
        assert page.tags['DateTime'].value == '2008:11:25 18:59:20'
        # assert uic tags
        tags = tif.stk_metadata
        assert tags['Name'] == 'Photometrics'
        assert tags['NumberPlanes'] == 2048
        assert len(tags['PlaneDescriptions']) == 2048
        assert tags['PlaneDescriptions'][0].startswith(
            'Acquired from Photometrics\r\n'
        )
        assert tags['CalibrationUnits'] == 'pixel'
        # assert series properties
        series = tif.series[0]
        assert series.shape == (2048, 128, 512)
        assert series.dtype == numpy.uint16
        assert series.axes == 'TYX'
        assert series.kind == 'stk'
        # assert data
        data = tif.asarray()
        assert isinstance(data, numpy.ndarray)
        assert data.shape == (2048, 128, 512)
        assert data.dtype == numpy.uint16
        assert data[2047, 64, 128] == 7132
        assert_aszarr_method(tif, data)
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE or SKIP_LARGE, reason=REASON)
def test_read_stk_noname():
    """Test read MetaMorph STK with no name in metadata."""
    # https://forum.image.sc/t/metamorph-stack-issue-with-ome-metadata-
    # bioformats-and-omero/48416
    filename = _file('stk/60x_2well_diffexpos1_w1sdcGFP_s1_t1.stk')
    with TiffFile(filename) as tif:
        assert tif.is_stk
        assert tif.byteorder == '<'
        assert len(tif.pages) == 1
        assert len(tif.series) == 1
        # assert page properties
        page = tif.pages.first
        assert page.is_contiguous
        assert page.photometric == PHOTOMETRIC.MINISBLACK
        assert page.compression == COMPRESSION.NONE
        assert page.imagewidth == 1148
        assert page.imagelength == 1112
        assert page.bitspersample == 16
        assert page.samplesperpixel == 1
        assert page.tags['Software'].value == 'VisiView 4.5.0'
        assert page.tags['DateTime'].value == '2021:01:27 13:29:51'
        # assert uic tags
        tags = tif.stk_metadata
        assert 'Name' not in tags
        assert tags['NumberPlanes'] == 5
        assert len(tags['PlaneDescriptions']) == 5
        assert tags['PlaneDescriptions'][0].startswith('Exposure: 50 ms\r\n')
        # assert series properties
        series = tif.series[0]
        assert series.shape == (5, 1112, 1148)
        assert series.dtype == numpy.uint16
        assert series.axes == 'ZYX'
        assert series.kind == 'stk'
        # assert data
        data = tif.asarray()
        assert isinstance(data, numpy.ndarray)
        assert data.shape == (5, 1112, 1148)
        assert data.dtype == numpy.uint16
        assert data[4, 64, 128] == 98
        assert_aszarr_method(tif, data)
        assert__str__(tif)


@pytest.mark.skipif(
    SKIP_FILE or SKIP_CODECS or not imagecodecs.JPEG.available,
    reason=REASON,
)
def test_read_ndpi_cmu1():
    """Test read Hamamatsu NDPI slide, JPEG."""
    filename = _file('HamamatsuNDPI/CMU-1.ndpi')
    with TiffFile(filename) as tif:
        assert tif.is_ndpi
        assert len(tif.pages) == 5
        assert len(tif.series) == 2
        for page in tif.pages:
            assert page.ndpi_tags['Model'] == 'NanoZoomer'
        # first page
        page = tif.pages.first
        assert page.is_ndpi
        assert page.photometric == PHOTOMETRIC.YCBCR
        assert page.compression == COMPRESSION.JPEG
        assert page.shape == (38144, 51200, 3)
        assert page.ndpi_tags['Magnification'] == 20.0
        # page 4
        page = tif.pages[4]
        assert page.is_ndpi
        assert page.photometric == PHOTOMETRIC.YCBCR
        assert page.compression == COMPRESSION.JPEG
        assert page.shape == (408, 1191, 3)
        assert page.ndpi_tags['Magnification'] == -1.0
        assert page.asarray()[226, 629, 0] == 167
        assert_aszarr_method(page)
        assert__str__(tif)


@pytest.mark.skipif(
    SKIP_FILE or SKIP_CODECS or SKIP_LARGE or not imagecodecs.JPEG.available,
    reason=REASON,
)
def test_read_ndpi_cmu2():
    """Test read Hamamatsu NDPI slide, JPEG."""
    # JPEG stream too large to be opened with unpatched libjpeg
    filename = _file('HamamatsuNDPI/CMU-2.ndpi')
    with TiffFile(filename) as tif:
        assert tif.is_ndpi
        assert len(tif.pages) == 6
        assert len(tif.series) == 2
        for page in tif.pages:
            assert page.ndpi_tags['Model'] == 'NanoZoomer'
        # first page
        page = tif.pages.first
        assert page.is_ndpi
        assert page.photometric == PHOTOMETRIC.YCBCR
        assert page.compression == COMPRESSION.JPEG
        assert page.shape == (33792, 79872, 3)
        assert page.ndpi_tags['Magnification'] == 20.0
        # with pytest.raises(RuntimeError):
        if not IS_PYPY:
            data = page.asarray()
            assert data.shape == (33792, 79872, 3)
            assert data.dtype == numpy.uint8
        # page 5
        page = tif.pages[5]
        assert page.is_ndpi
        assert page.photometric == PHOTOMETRIC.YCBCR
        assert page.compression == COMPRESSION.JPEG
        assert page.shape == (408, 1191, 3)
        assert page.ndpi_tags['Magnification'] == -1.0
        assert page.asarray()[226, 629, 0] == 181
        assert_aszarr_method(page)
        assert__str__(tif)


@pytest.mark.skipif(
    SKIP_LARGE or SKIP_FILE or SKIP_CODECS or not imagecodecs.JPEG.available,
    reason=REASON,
)
def test_read_ndpi_4gb():
    """Test read > 4 GB Hamamatsu NDPI slide, JPEG 103680x188160."""
    filename = _file('HamamatsuNDPI/103680x188160.ndpi')
    with TiffFile(filename) as tif:
        assert tif.is_ndpi
        assert len(tif.pages) == 8
        assert len(tif.series) == 3
        for page in tif.pages:
            assert page.ndpi_tags['Model'] == 'C13220'
        # first page
        page = tif.pages.first
        assert page.offset == 4466602683
        assert page.is_ndpi
        assert page.databytecounts[0] == 5105  # not 4461521316
        assert page.photometric == PHOTOMETRIC.YCBCR
        assert page.compression == COMPRESSION.JPEG
        assert page.shape == (103680, 188160, 3)
        assert (
            page.tags['ImageLength'].offset - page.tags['ImageWidth'].offset
            == 12
        )
        assert page.tags['ImageWidth'].offset == 4466602685
        assert page.tags['ImageWidth'].valueoffset == 4466602693
        assert page.tags['ImageLength'].offset == 4466602697
        assert page.tags['ImageLength'].valueoffset == 4466602705
        assert page.tags['ReferenceBlackWhite'].offset == 4466602889
        assert page.tags['ReferenceBlackWhite'].valueoffset == 1003
        assert page.ndpi_tags['Magnification'] == 40.0
        assert page.ndpi_tags['McuStarts'][-1] == 4461516507  # corrected
        with pytest.raises(ValueError):
            page.tags['StripByteCounts'].astuple()
        if not SKIP_ZARR:
            # data = page.asarray()  # 55 GB
            with page.aszarr() as store:
                data = zarr.open(store, mode='r')
                assert data[38061, 121978].tolist() == [220, 167, 187]
        # page 7
        page = tif.pages[7]
        assert page.is_ndpi
        assert page.photometric == PHOTOMETRIC.MINISBLACK
        assert page.compression == COMPRESSION.NONE
        assert page.shape == (200, 600)
        assert page.ndpi_tags['Magnification'] == -2.0
        # assert page.asarray()[226, 629, 0] == 167
        # first series
        series = tif.series[0]
        assert series.kind == 'ndpi'
        assert series.name == 'S10533009'
        assert series.shape == (103680, 188160, 3)
        assert series.is_pyramidal
        assert len(series.levels) == 6
        assert len(series.pages) == 1
        # pyramid levels
        assert series.levels[1].shape == (51840, 94080, 3)
        assert series.levels[2].shape == (25920, 47040, 3)
        assert series.levels[3].shape == (12960, 23520, 3)
        assert series.levels[4].shape == (6480, 11760, 3)
        assert series.levels[5].shape == (3240, 5880, 3)
        data = series.levels[5].asarray(maxworkers=1)
        assert tuple(data[1000, 1000]) == (222, 165, 200)
        with pytest.raises(ValueError):
            page.tags['StripOffsets'].astuple()
        # cannot decode base levels since JPEG compressed size > 2 GB
        # series.levels[0].asarray()
        assert_aszarr_method(series.levels[5], data, maxworkers=1)
        assert__str__(tif)


@pytest.mark.skipif(
    SKIP_FILE or SKIP_CODECS or not imagecodecs.JPEGXR.available,
    reason=REASON,
)
def test_read_ndpi_jpegxr():
    """Test read Hamamatsu NDPI slide with JPEG XR compression."""
    # https://downloads.openmicroscopy.org/images/Hamamatsu-NDPI/hamamatsu/
    filename = _file('HamamatsuNDPI/DM0014 - 2020-04-02 10.25.21.ndpi')
    with TiffFile(filename) as tif:
        assert tif.is_ndpi
        assert len(tif.pages) == 6
        assert len(tif.series) == 3
        for page in tif.pages:
            assert page.ndpi_tags['Model'] == 'C13210'

        for page in tif.pages[:4]:
            # check that all levels are corrected
            assert page.is_ndpi
            assert (
                page.tags['PhotometricInterpretation'].value
                == PHOTOMETRIC.YCBCR
            )
            assert page.tags['BitsPerSample'].value == (8, 8, 8)
            assert page.samplesperpixel == 1  # not 3
            assert page.bitspersample == 16  # not 8
            assert page.photometric == PHOTOMETRIC.MINISBLACK  # not YCBCR
            assert page.compression == COMPRESSION.JPEGXR_NDPI

        # first page
        page = tif.pages.first
        assert page.shape == (34944, 69888)  # not (34944, 69888, 3)
        assert page.databytecounts[0] == 632009
        assert page.ndpi_tags['CaptureMode'] == 17
        assert page.ndpi_tags['Magnification'] == 20.0
        if not SKIP_ZARR:
            with page.aszarr() as store:
                data = zarr.open(store, mode='r')
                assert data[28061, 41978] == 6717
        # page 5
        page = tif.pages[5]
        assert page.is_ndpi
        assert page.photometric == PHOTOMETRIC.MINISBLACK
        assert page.compression == COMPRESSION.NONE
        assert page.shape == (192, 566)
        assert page.ndpi_tags['Magnification'] == -2.0
        # first series
        series = tif.series[0]
        assert series.kind == 'ndpi'
        assert series.name == 'DM0014'
        assert series.shape == (34944, 69888)
        assert series.is_pyramidal
        assert len(series.levels) == 4
        assert len(series.pages) == 1
        # pyramid levels
        assert series.levels[1].shape == (17472, 34944)
        assert series.levels[2].shape == (8736, 17472)
        assert series.levels[3].shape == (4368, 8736)
        data = series.levels[3].asarray(maxworkers=16)
        assert data[1000, 1000] == 1095
        assert_aszarr_method(series.levels[3], data, maxworkers=16)
        assert__str__(tif)


@pytest.mark.skipif(
    SKIP_FILE or SKIP_LARGE or SKIP_CODECS or not imagecodecs.JPEG.available,
    reason=REASON,
)
def test_read_ndpi_databytecounts():
    """Test read Hamamatsu NDPI slide databytecounts do not overflow."""
    # https://forum.image.sc/t/some-ndpi-files-not-opening-in-qupath-v0-4-1
    filename = _file('HamamatsuNDPI/doesnt_work.ndpi')
    with TiffFile(filename) as tif:
        assert tif.is_ndpi
        assert len(tif.pages) == 23
        assert len(tif.series) == 3
        # first series
        series = tif.series[0]
        assert series.kind == 'ndpi'
        assert series.name == 'Baseline'
        assert series.shape == (3, 60928, 155648, 3)
        assert series.is_pyramidal
        assert len(series.levels) == 7
        assert len(series.pages) == 3
        # pyramid levels
        assert series.levels[1].shape == (3, 30464, 77824, 3)
        assert series.levels[2].shape == (3, 15232, 38912, 3)
        assert series.levels[3].shape == (3, 7616, 19456, 3)
        assert series.levels[4].shape == (3, 3808, 9728, 3)
        assert series.levels[5].shape == (3, 1904, 4864, 3)
        assert series.levels[6].shape == (3, 952, 2432, 3)
        # 3rd z-slice in base layer
        page = series.pages[2]
        assert page.index == 14
        assert page.shape == (60928, 155648, 3)
        assert page.dataoffsets[-1] == 4718518695
        assert page.databytecounts[-1] == 4338
        assert page.ndpi_tags['Magnification'] == 40.0
        assert page.ndpi_tags['FocusTime'] == 13
        assert page.ndpi_tags['ScannerSerialNumber'] == '680057'
        assert tuple(page.asarray()[60000, 150000]) == (216, 221, 217)


@pytest.mark.skipif(
    SKIP_FILE or SKIP_CODECS or not imagecodecs.JPEG.available,
    reason=REASON,
)
def test_read_ndpi_layers():
    """Test read Hamamatsu NDPI slide with 5 layers."""
    filename = _file('HamamatsuNDPI/hamamatsu_5-layers.ndpi')
    with TiffFile(filename) as tif:
        assert tif.is_ndpi
        assert len(tif.pages) == 37
        assert len(tif.series) == 3
        for page in tif.pages:
            assert page.ndpi_tags['Model'] == 'C10730-12'

        for page in tif.pages[:4]:
            assert page.is_ndpi
            assert (
                page.tags['PhotometricInterpretation'].value
                == PHOTOMETRIC.YCBCR
            )
            assert page.tags['BitsPerSample'].value == (8, 8, 8)
            assert page.samplesperpixel == 3
            assert page.bitspersample == 8
            assert page.photometric == PHOTOMETRIC.YCBCR
            assert page.compression == COMPRESSION.JPEG

        # first page
        page = tif.pages.first
        assert page.shape == (12032, 11520, 3)
        assert page.databytecounts[0] == 1634
        assert page.ndpi_tags['CaptureMode'] == 0
        assert page.ndpi_tags['Magnification'] == 20.0
        if not SKIP_ZARR:
            with page.aszarr() as store:
                data = zarr.open(store, mode='r')
                assert list(data[10000, 10000]) == [232, 215, 225]
        # last page
        page = tif.pages[-1]
        assert page.is_ndpi
        assert page.photometric == PHOTOMETRIC.MINISBLACK
        assert page.compression == COMPRESSION.NONE
        assert page.shape == (196, 575)
        assert page.ndpi_tags['Magnification'] == -2.0
        # first series
        series = tif.series[0]
        assert series.kind == 'ndpi'
        assert series.name == 'Baseline'
        assert series.shape == (7, 12032, 11520, 3)
        assert series.is_pyramidal
        assert len(series.levels) == 5
        assert len(series.pages) == 7
        # pyramid levels
        assert series.levels[1].shape == (7, 6016, 5760, 3)
        assert series.levels[2].shape == (7, 3008, 2880, 3)
        assert series.levels[3].shape == (7, 1504, 1440, 3)
        assert series.levels[4].shape == (7, 752, 720, 3)
        data = series.levels[3].asarray(maxworkers=1)
        assert_array_equal(
            data[:, 1000, 1000],
            [
                [234, 217, 225],
                [231, 213, 225],
                [233, 215, 227],
                [234, 217, 225],
                [234, 216, 228],
                [233, 216, 224],
                [232, 214, 226],
            ],
        )
        assert_aszarr_method(series.levels[3], data, maxworkers=1)
        assert__str__(tif)


@pytest.mark.skipif(
    SKIP_FILE or SKIP_CODECS or not imagecodecs.JPEG.available,
    reason=REASON,
)
def test_read_svs_cmu1():
    """Test read Aperio SVS slide, JPEG and LZW."""
    filename = _file('AperioSVS/CMU-1.svs')
    with TiffFile(filename) as tif:
        assert tif.is_svs
        assert not tif.is_scanimage
        assert len(tif.pages) == 6
        assert len(tif.series) == 4
        for page in tif.pages:
            svs_description_metadata(page.description)
        # first page
        page = tif.pages.first
        assert page.is_svs
        assert not page.is_jfif
        assert page.is_subsampled
        assert page.photometric == PHOTOMETRIC.RGB
        assert page.is_tiled
        assert page.compression == COMPRESSION.JPEG
        assert page.shape == (32914, 46000, 3)
        metadata = svs_description_metadata(page.description)
        assert metadata['Header'].startswith('Aperio Image Library')
        assert metadata['Originalheight'] == 33014
        # page 4
        page = tif.pages[4]
        assert page.is_svs
        assert page.is_reduced
        assert page.photometric == PHOTOMETRIC.RGB
        assert page.compression == COMPRESSION.LZW
        assert page.shape == (463, 387, 3)
        metadata = svs_description_metadata(page.description)
        assert 'label 387x463' in metadata['Header']
        assert_aszarr_method(page)
        assert__str__(tif)


@pytest.mark.skipif(
    SKIP_FILE or SKIP_CODECS or not imagecodecs.JPEG2K.available,
    reason=REASON,
)
def test_read_svs_jp2k_33003_1():
    """Test read Aperio SVS slide, JP2000 and LZW."""
    filename = _file('AperioSVS/JP2K-33003-1.svs')
    with TiffFile(filename) as tif:
        assert tif.is_svs
        assert not tif.is_scanimage
        assert len(tif.pages) == 6
        assert len(tif.series) == 4
        for page in tif.pages:
            svs_description_metadata(page.description)
        # first page
        page = tif.pages.first
        assert page.is_svs
        assert not page.is_subsampled
        assert page.photometric == PHOTOMETRIC.RGB
        assert page.is_tiled
        assert page.compression == COMPRESSION.APERIO_JP2000_YCBC
        assert page.shape == (17497, 15374, 3)
        metadata = svs_description_metadata(page.description)
        assert metadata['Header'].startswith('Aperio Image Library')
        assert metadata['Originalheight'] == 17597
        # page 4
        page = tif.pages[4]
        assert page.is_svs
        assert page.is_reduced
        assert page.photometric == PHOTOMETRIC.RGB
        assert page.compression == COMPRESSION.LZW
        assert page.shape == (422, 415, 3)
        metadata = svs_description_metadata(page.description)
        assert 'label 415x422' in metadata['Header']
        assert_aszarr_method(page)
        assert__str__(tif)


@pytest.mark.skipif(
    SKIP_FILE or SKIP_CODECS or not imagecodecs.JPEG.available,
    reason=REASON,
)
def test_read_bif(caplog):
    """Test read Ventana BIF slide."""
    filename = _file('VentanaBIF/OS-2.bif')
    with TiffFile(filename) as tif:
        assert tif.is_bif
        assert len(tif.pages) == 12
        assert len(tif.series) == 3
        # first page
        page = tif.pages.first
        assert page.is_bif
        assert page.photometric == PHOTOMETRIC.YCBCR
        assert page.is_tiled
        assert page.compression == COMPRESSION.JPEG
        assert page.shape == (3008, 1008, 3)

        series = tif.series
        assert 'not stitched' in caplog.text
        # baseline
        series = tif.series[0]
        assert series.kind == 'bif'
        assert series.name == 'Baseline'
        assert len(series.levels) == 10
        assert series.shape == (82960, 128000, 3)
        assert series.dtype == numpy.uint8
        # level 0
        page = series.pages[0]
        assert page.is_bif
        assert page.is_tiled
        assert page.photometric == PHOTOMETRIC.YCBCR
        assert page.compression == COMPRESSION.JPEG
        assert page.shape == (82960, 128000, 3)
        assert page.description == 'level=0 mag=40 quality=90'
        # level 5
        page = series.levels[5].pages[0]
        assert not page.is_bif
        assert page.is_tiled
        assert page.photometric == PHOTOMETRIC.YCBCR
        assert page.compression == COMPRESSION.JPEG
        assert page.shape == (2600, 4000, 3)
        assert page.description == 'level=5 mag=1.25 quality=90'

        assert_aszarr_method(page, maxworkers=4)
        assert__str__(tif)


@pytest.mark.skipif(
    SKIP_FILE or SKIP_LARGE or SKIP_CODECS or not imagecodecs.JPEG.available,
    reason=REASON,
)
def test_read_scn_collection():
    """Test read Leica SCN slide, JPEG."""
    # collection of 43 CZYX images
    # https://forum.image.sc/t/43585
    filename = _file(
        'LeicaSCN/19-3-12_b5992c2e-5b6e-46f2-bf9b-d5872bdebdc1.SCN'
    )
    with TiffFile(filename) as tif:
        assert tif.is_scn
        assert tif.is_bigtiff
        assert len(tif.pages) == 5358
        assert len(tif.series) == 46
        # first page
        page = tif.pages.first
        assert page.is_scn
        assert page.is_tiled
        assert page.photometric == PHOTOMETRIC.YCBCR
        assert page.compression == COMPRESSION.JPEG
        assert page.shape == (12990, 5741, 3)
        metadata = tif.scn_metadata
        assert metadata.startswith('<?xml version="1.0" encoding="utf-8"?>')
        for series in tif.series[2:]:
            assert series.kind == 'scn'
            assert series.axes == 'CZYX'
            assert series.shape[:2] == (4, 8)
            assert len(series.levels) in {2, 3, 4, 5}
            assert len(series.pages) == 32
        # third series
        series = tif.series[2]
        assert series.shape == (4, 8, 946, 993)
        assert_aszarr_method(series)
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_read_scanimage_metadata():
    """Test read ScanImage metadata."""
    filename = _file('ScanImage/TS_UnitTestImage_BigTIFF.tif')
    with open(filename, 'rb') as fh:
        frame_data, roi_data, version = read_scanimage_metadata(fh)
    assert version == 3
    assert frame_data['SI.hChannels.channelType'] == ['stripe', 'stripe']
    assert roi_data['RoiGroups']['imagingRoiGroup']['ver'] == 1


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_read_scanimage_2021():
    """Test read ScanImage metadata."""
    # https://github.com/cgohlke/tifffile/issues/46
    filename = _file('ScanImage/ScanImage2021_3frames.tif')
    with open(filename, 'rb') as fh:
        frame_data, roi_data, version = read_scanimage_metadata(fh)
    assert frame_data['SI.hChannels.channelType'] == [
        'stripe',
        'stripe',
        'stripe',
        'stripe',
    ]
    assert version == 4
    assert roi_data['RoiGroups']['imagingRoiGroup']['ver'] == 1

    with TiffFile(filename) as tif:
        assert tif.is_scanimage
        assert len(tif.pages) == 3
        assert len(tif.series) == 1
        assert tif.series[0].shape == (3, 256, 256)
        assert tif.series[0].axes == 'TYX'
        # non-varying scanimage_metadata
        assert tif.scanimage_metadata['version'] == 4
        assert 'FrameData' in tif.scanimage_metadata
        assert 'RoiGroups' in tif.scanimage_metadata
        # assert page properties
        page = tif.pages.first
        assert page.is_scanimage
        assert page.is_contiguous
        assert page.compression == COMPRESSION.NONE
        assert page.imagewidth == 256
        assert page.imagelength == 256
        assert page.bitspersample == 16
        assert page.samplesperpixel == 1
        # description tags
        metadata = scanimage_description_metadata(page.description)
        assert metadata['epoch'] == [2021, 3, 1, 17, 31, 28.047]
        assert_aszarr_method(tif)
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_read_scanimage_no_framedata():
    """Test read ScanImage no FrameData."""
    filename = _file('ScanImage/PSF001_ScanImage36.tif')
    with TiffFile(filename) as tif:
        assert tif.is_scanimage
        assert len(tif.pages) == 100
        assert len(tif.series) == 1
        # no non-tiff scanimage_metadata
        assert not tif.scanimage_metadata
        # assert page properties
        page = tif.pages.first
        assert page.is_scanimage
        assert page.is_contiguous
        assert page.compression == COMPRESSION.NONE
        assert page.imagewidth == 256
        assert page.imagelength == 256
        assert page.bitspersample == 16
        assert page.samplesperpixel == 1
        # description tags
        metadata = scanimage_description_metadata(page.description)
        assert metadata['state.software.version'] == 3.6
        assert_aszarr_method(tif)
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE or SKIP_LARGE, reason=REASON)
def test_read_scanimage_2gb():
    """Test read ScanImage non-BigTIFF > 2 GB.

    https://github.com/MouseLand/suite2p/issues/149

    """
    filename = _file('ScanImage/M161209TH_01__001.tif')
    with TiffFile(filename) as tif:
        assert tif.is_scanimage
        assert len(tif.pages) == 5980
        assert len(tif.series) == 1
        assert tif.series[0].kind == 'scanimage'
        # no non-tiff scanimage_metadata
        assert 'version' not in tif.scanimage_metadata
        assert 'FrameData' not in tif.scanimage_metadata
        assert 'RoiGroups' not in tif.scanimage_metadata
        # assert page properties
        page = tif.pages.first
        assert isinstance(page, TiffPage)
        assert not page.is_frame
        assert not page.is_virtual
        assert not page.is_subifd
        assert page.is_scanimage
        assert page.is_contiguous
        assert page.compression == COMPRESSION.NONE
        assert page.imagewidth == 512
        assert page.imagelength == 512
        assert page.bitspersample == 16
        assert page.samplesperpixel == 1
        # using virtual frames
        frame = tif.pages[-1]
        assert isinstance(frame, TiffFrame)
        assert frame.is_frame
        assert frame.is_virtual
        assert not frame.is_subifd
        assert frame.offset <= 0
        assert frame.index == 5979
        assert frame.dataoffsets[0] == 3163182856
        assert frame.databytecounts[0] == 8192  # 524288
        assert len(frame.dataoffsets) == 64
        assert len(frame.databytecounts) == 64
        # description tags
        metadata = scanimage_description_metadata(page.description)
        assert metadata['scanimage.SI5.VERSION_MAJOR'] == 5
        # assert data
        data = frame.asarray()
        assert data[256, 256] == 71
        data = tif.asarray()
        assert data[5979, 256, 256] == 71
        if not SKIP_SLOW:
            assert_aszarr_method(tif, data)
        else:
            assert_aszarr_method(frame, frame.asarray())
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_read_scanimage_bigtiff():
    """Test read ScanImage BigTIFF."""
    # https://github.com/cgohlke/tifffile/issues/29
    filename = _file('ScanImage/area1__00001.tif')
    with TiffFile(filename) as tif:
        assert tif.is_scanimage
        assert len(tif.pages) == 162
        assert len(tif.series) == 1
        assert tif.series[0].kind == 'scanimage'
        # assert page properties
        page = tif.pages.first
        assert page.is_scanimage
        assert page.is_contiguous
        assert page.compression == COMPRESSION.NONE
        assert page.imagewidth == 512
        assert page.imagelength == 512
        assert page.bitspersample == 16
        assert page.samplesperpixel == 1
        # metadata in description, software, artist tags
        metadata = scanimage_description_metadata(page.description)
        assert metadata['frameNumbers'] == 1
        metadata = scanimage_description_metadata(page.tags['Software'].value)
        assert metadata['SI.TIFF_FORMAT_VERSION'] == 3
        metadata = scanimage_artist_metadata(page.tags['Artist'].value)
        assert metadata['RoiGroups']['imagingRoiGroup']['ver'] == 1
        metadata = tif.scanimage_metadata
        assert metadata['version'] == 3
        assert metadata['FrameData']['SI.TIFF_FORMAT_VERSION'] == 3
        assert metadata['RoiGroups']['imagingRoiGroup']['ver'] == 1
        assert 'Description' not in metadata
        # assert page offsets are correct
        assert tif.pages[-1].offset == 84527590  # not 84526526 (calculated)
        # read image data
        assert_aszarr_method(tif)
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_read_ome_single_channel():
    """Test read OME image."""
    # 2D (single image)
    # OME-TIFF reference images from
    # https://www.openmicroscopy.org/site/support/ome-model/ome-tiff
    filename = _file('OME/bioformats-artificial/single-channel.ome.tif')
    with TiffFile(filename) as tif:
        assert tif.is_ome
        assert tif.byteorder == '>'
        assert len(tif.pages) == 1
        assert len(tif.series) == 1
        # assert page properties
        page = tif.pages.first
        assert page.is_contiguous
        assert page.tags['Software'].value[:15] == 'OME Bio-Formats'
        assert page.compression == COMPRESSION.NONE
        assert page.imagewidth == 439
        assert page.imagelength == 167
        assert page.bitspersample == 8
        assert page.samplesperpixel == 1
        # assert series properties
        series = tif.series[0]
        assert not series.is_multifile
        assert series.dtype == numpy.int8
        assert series.shape == (167, 439)
        assert series.axes == 'YX'
        assert series.kind == 'ome'
        assert series.get_shape(squeeze=False) == (1, 1, 1, 167, 439, 1)
        assert series.get_axes(squeeze=False) == 'TCZYXS'
        # assert data
        data = tif.asarray()
        assert isinstance(data, numpy.ndarray)
        assert data.shape == (167, 439)
        assert data.dtype == numpy.int8
        assert data[158, 428] == 91
        assert_aszarr_method(tif, data)
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_read_ome_multi_channel():
    """Test read OME multi channel image."""
    # 2D (3 channels)
    filename = _file('OME/bioformats-artificial/multi-channel.ome.tiff')
    with TiffFile(filename) as tif:
        assert tif.is_ome
        assert tif.byteorder == '>'
        assert len(tif.pages) == 3
        assert len(tif.series) == 1
        # assert page properties
        page = tif.pages.first
        assert page.is_contiguous
        assert page.tags['Software'].value[:15] == 'OME Bio-Formats'
        assert page.compression == COMPRESSION.NONE
        assert page.imagewidth == 439
        assert page.imagelength == 167
        assert page.bitspersample == 8
        assert page.samplesperpixel == 1
        # assert series properties
        series = tif.series[0]
        assert series.shape == (3, 167, 439)
        assert series.dtype == numpy.int8
        assert series.axes == 'CYX'
        assert series.kind == 'ome'
        assert series.get_shape(squeeze=False) == (1, 3, 1, 167, 439, 1)
        assert series.get_axes(squeeze=False) == 'TCZYXS'
        assert not series.is_multifile
        # assert data
        data = tif.asarray()
        assert isinstance(data, numpy.ndarray)
        assert data.shape == (3, 167, 439)
        assert data.dtype == numpy.int8
        assert data[2, 158, 428] == 91
        assert_aszarr_method(tif, data)
        # don't squeeze
        data = tif.asarray(squeeze=False)
        assert_aszarr_method(tif, data, squeeze=False)
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_read_ome_z_series():
    """Test read OME volume."""
    # 3D (5 focal planes)
    filename = _file('OME/bioformats-artificial/z-series.ome.tif')
    with TiffFile(filename) as tif:
        assert tif.is_ome
        assert tif.byteorder == '>'
        assert len(tif.pages) == 5
        assert len(tif.series) == 1
        # assert page properties
        page = tif.pages.first
        assert page.is_contiguous
        assert page.tags['Software'].value[:15] == 'OME Bio-Formats'
        assert page.compression == COMPRESSION.NONE
        assert page.imagewidth == 439
        assert page.imagelength == 167
        assert page.bitspersample == 8
        assert page.samplesperpixel == 1
        # assert series properties
        series = tif.series[0]
        assert series.shape == (5, 167, 439)
        assert series.dtype == numpy.int8
        assert series.axes == 'ZYX'
        assert series.kind == 'ome'
        assert series.get_shape(squeeze=False) == (1, 1, 5, 167, 439, 1)
        assert series.get_axes(squeeze=False) == 'TCZYXS'
        assert not series.is_multifile
        # assert data
        data = tif.asarray()
        assert isinstance(data, numpy.ndarray)
        assert data.shape == (5, 167, 439)
        assert data.dtype == numpy.int8
        assert data[4, 158, 428] == 91
        assert_aszarr_method(tif, data)
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_read_ome_multi_channel_z_series():
    """Test read OME multi-channel volume."""
    # 3D (5 focal planes, 3 channels)
    filename = _file(
        'OME/bioformats-artificial/multi-channel-z-series.ome.tiff'
    )
    with TiffFile(filename) as tif:
        assert tif.is_ome
        assert tif.byteorder == '>'
        assert len(tif.pages) == 15
        assert len(tif.series) == 1
        # assert page properties
        page = tif.pages.first
        assert page.is_contiguous
        assert page.tags['Software'].value[:15] == 'OME Bio-Formats'
        assert page.compression == COMPRESSION.NONE
        assert page.imagewidth == 439
        assert page.imagelength == 167
        assert page.bitspersample == 8
        assert page.samplesperpixel == 1
        # assert series properties
        series = tif.series[0]
        assert series.shape == (3, 5, 167, 439)
        assert series.dtype == numpy.int8
        assert series.axes == 'CZYX'
        assert series.kind == 'ome'
        assert series.get_shape(squeeze=False) == (1, 3, 5, 167, 439, 1)
        assert series.get_axes(squeeze=False) == 'TCZYXS'
        assert not series.is_multifile
        # assert data
        data = tif.asarray()
        assert isinstance(data, numpy.ndarray)
        assert data.shape == (3, 5, 167, 439)
        assert data.dtype == numpy.int8
        assert data[2, 4, 158, 428] == 91
        assert_aszarr_method(tif, data)
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_read_ome_time_series():
    """Test read OME time-series of images."""
    # 3D (7 time points)
    filename = _file('OME/bioformats-artificial/time-series.ome.tiff')
    with TiffFile(filename) as tif:
        assert tif.is_ome
        assert tif.byteorder == '>'
        assert len(tif.pages) == 7
        assert len(tif.series) == 1
        # assert page properties
        page = tif.pages.first
        assert page.is_contiguous
        assert page.tags['Software'].value[:15] == 'OME Bio-Formats'
        assert page.compression == COMPRESSION.NONE
        assert page.imagewidth == 439
        assert page.imagelength == 167
        assert page.bitspersample == 8
        assert page.samplesperpixel == 1
        # assert series properties
        series = tif.series[0]
        assert series.shape == (7, 167, 439)
        assert series.dtype == numpy.int8
        assert series.axes == 'TYX'
        assert series.kind == 'ome'
        assert series.get_shape(squeeze=False) == (7, 1, 1, 167, 439, 1)
        assert series.get_axes(squeeze=False) == 'TCZYXS'
        assert not series.is_multifile
        # assert data
        data = tif.asarray()
        assert isinstance(data, numpy.ndarray)
        assert data.shape == (7, 167, 439)
        assert data.dtype == numpy.int8
        assert data[6, 158, 428] == 91
        assert_aszarr_method(tif, data)
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_read_ome_multi_channel_time_series():
    """Test read OME time-series of multi-channel images."""
    # 3D (7 time points, 3 channels)
    filename = _file(
        'OME/bioformats-artificial/multi-channel-time-series.ome.tiff'
    )
    with TiffFile(filename) as tif:
        assert tif.is_ome
        assert tif.byteorder == '>'
        assert len(tif.pages) == 21
        assert len(tif.series) == 1
        # assert page properties
        page = tif.pages.first
        assert page.is_contiguous
        assert page.tags['Software'].value[:15] == 'OME Bio-Formats'
        assert page.compression == COMPRESSION.NONE
        assert page.imagewidth == 439
        assert page.imagelength == 167
        assert page.bitspersample == 8
        assert page.samplesperpixel == 1
        # assert series properties
        series = tif.series[0]
        assert series.shape == (7, 3, 167, 439)
        assert series.dtype == numpy.int8
        assert series.axes == 'TCYX'
        assert series.kind == 'ome'
        assert series.get_shape(squeeze=False) == (7, 3, 1, 167, 439, 1)
        assert series.get_axes(squeeze=False) == 'TCZYXS'
        assert not series.is_multifile
        # assert data
        data = tif.asarray()
        assert isinstance(data, numpy.ndarray)
        assert data.shape == (7, 3, 167, 439)
        assert data.dtype == numpy.int8
        assert data[6, 2, 158, 428] == 91
        assert_aszarr_method(tif, data)
        # don't squeeze
        data = tif.asarray(squeeze=False)
        assert_aszarr_method(tif, data, squeeze=False)
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_read_ome_4d_series():
    """Test read OME time-series of volumes."""
    # 4D (7 time points, 5 focal planes)
    filename = _file('OME/bioformats-artificial/4D-series.ome.tiff')
    with TiffFile(filename) as tif:
        assert tif.is_ome
        assert tif.byteorder == '>'
        assert len(tif.pages) == 35
        assert len(tif.series) == 1
        # assert page properties
        page = tif.pages.first
        assert page.is_contiguous
        assert page.tags['Software'].value[:15] == 'OME Bio-Formats'
        assert page.compression == COMPRESSION.NONE
        assert page.imagewidth == 439
        assert page.imagelength == 167
        assert page.bitspersample == 8
        assert page.samplesperpixel == 1
        # assert series properties
        series = tif.series[0]
        assert series.shape == (7, 5, 167, 439)
        assert series.dtype == numpy.int8
        assert series.axes == 'TZYX'
        assert series.kind == 'ome'
        assert not series.is_multifile
        # assert data
        data = tif.asarray()
        assert isinstance(data, numpy.ndarray)
        assert data.shape == (7, 5, 167, 439)
        assert data.dtype == numpy.int8
        assert data[6, 4, 158, 428] == 91
        assert_aszarr_method(tif, data)
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_read_ome_multi_channel_4d_series():
    """Test read OME time-series of multi-channel volumes."""
    # 4D (7 time points, 5 focal planes, 3 channels)
    filename = _file(
        'OME/bioformats-artificial/multi-channel-4D-series.ome.tiff'
    )
    with TiffFile(filename) as tif:
        assert tif.is_ome
        assert tif.byteorder == '>'
        assert len(tif.pages) == 105
        assert len(tif.series) == 1
        # assert page properties
        page = tif.pages.first
        assert page.is_contiguous
        assert page.tags['Software'].value[:15] == 'OME Bio-Formats'
        assert page.compression == COMPRESSION.NONE
        assert page.imagewidth == 439
        assert page.imagelength == 167
        assert page.bitspersample == 8
        assert page.samplesperpixel == 1
        # assert series properties
        series = tif.series[0]
        assert series.shape == (7, 3, 5, 167, 439)
        assert series.dtype == numpy.int8
        assert series.axes == 'TCZYX'
        assert series.kind == 'ome'
        assert series.get_shape(squeeze=False) == (7, 3, 5, 167, 439, 1)
        assert series.get_axes(squeeze=False) == 'TCZYXS'
        assert not series.is_multifile
        # assert data
        data = tif.asarray()
        assert isinstance(data, numpy.ndarray)
        assert data.shape == (7, 3, 5, 167, 439)
        assert data.dtype == numpy.int8
        assert data[6, 0, 4, 158, 428] == 91
        assert_aszarr_method(tif, data)
        # don't squeeze
        data = tif.asarray(squeeze=False)
        assert_aszarr_method(tif, data, squeeze=False)
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_read_ome_modulo_flim():
    """Test read OME modulo FLIM."""
    filename = _file('OME/modulo/FLIM-ModuloAlongC.ome.tiff')
    with TiffFile(filename) as tif:
        assert tif.is_ome
        assert tif.byteorder == '>'
        assert len(tif.pages) == 16
        assert len(tif.series) == 1
        # assert page properties
        page = tif.pages.first
        assert page.is_contiguous
        assert page.tags['Software'].value[:15] == 'OME Bio-Formats'
        assert page.compression == COMPRESSION.NONE
        assert page.imagewidth == 180
        assert page.imagelength == 150
        assert page.bitspersample == 8
        assert page.samplesperpixel == 1
        # assert series properties
        series = tif.series[0]
        assert series.shape == (2, 8, 150, 180)
        assert series.dtype == numpy.int8
        assert series.axes == 'CHYX'
        assert series.kind == 'ome'
        assert series.get_shape(squeeze=False) == (1, 2, 8, 1, 150, 180, 1)
        assert series.get_axes(squeeze=False) == 'TCHZYXS'
        assert not series.is_multifile
        # assert data
        data = tif.asarray()
        assert isinstance(data, numpy.ndarray)
        assert data.shape == (2, 8, 150, 180)
        assert data.dtype == numpy.int8
        assert data[1, 7, 143, 172] == 92
        assert_aszarr_method(tif, data)
        # don't squeeze
        data = tif.asarray(squeeze=False)
        assert_aszarr_method(tif, data, squeeze=False)
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_read_ome_modulo_flim_tcspc():
    """Test read OME modulo FLIM TSCPC."""
    # Two channels each recorded at two timepoints and eight histogram bins
    filename = _file('OME/modulo/FLIM-ModuloAlongT-TSCPC.ome.tiff')
    with TiffFile(filename) as tif:
        assert tif.is_ome
        assert tif.byteorder == '<'
        assert len(tif.pages) == 32
        assert len(tif.series) == 1
        # assert page properties
        page = tif.pages.first
        assert page.is_contiguous
        assert page.tags['Software'].value[:15] == 'OME Bio-Formats'
        assert page.compression == COMPRESSION.NONE
        assert page.imagewidth == 180
        assert page.imagelength == 200
        assert page.bitspersample == 8
        assert page.samplesperpixel == 1
        # assert series properties
        series = tif.series[0]
        assert series.shape == (2, 8, 2, 200, 180)
        assert series.dtype == numpy.int8
        assert series.axes == 'THCYX'
        assert series.kind == 'ome'
        assert not series.is_multifile
        # assert data
        data = tif.asarray()
        assert isinstance(data, numpy.ndarray)
        assert data.shape == (2, 8, 2, 200, 180)
        assert data.dtype == numpy.int8
        assert data[1, 7, 1, 190, 161] == 92
        assert_aszarr_method(tif, data)
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_read_ome_modulo_spim():
    """Test read OME modulo SPIM."""
    # 2x2 tile of planes each recorded at 4 angles
    filename = _file('OME/modulo/SPIM-ModuloAlongZ.ome.tiff')
    with TiffFile(filename) as tif:
        assert tif.is_ome
        assert tif.byteorder == '<'
        assert len(tif.pages) == 192
        assert len(tif.series) == 1
        # assert page properties
        page = tif.pages.first
        assert page.is_contiguous
        assert page.tags['Software'].value == 'OME Bio-Formats 5.2.0-SNAPSHOT'
        assert page.compression == COMPRESSION.NONE
        assert page.imagewidth == 160
        assert page.imagelength == 220
        assert page.bitspersample == 8
        assert page.samplesperpixel == 1
        # assert series properties
        series = tif.series[0]
        assert series.shape == (3, 4, 2, 4, 2, 220, 160)
        assert series.dtype == numpy.uint8
        assert series.axes == 'TRZACYX'
        assert series.kind == 'ome'
        assert not series.is_multifile
        # assert data
        data = tif.asarray()
        assert isinstance(data, numpy.ndarray)
        assert data.shape == (3, 4, 2, 4, 2, 220, 160)
        assert data.dtype == numpy.uint8
        assert data[2, 3, 1, 3, 1, 210, 151] == 92
        assert_aszarr_method(tif, data)
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_read_ome_modulo_lambda():
    """Test read OME modulo LAMBDA."""
    # Excitation of 5 wavelength [big-lambda] each recorded at 10 emission
    # wavelength ranges [lambda].
    filename = _file('OME/modulo/LAMBDA-ModuloAlongZ-ModuloAlongT.ome.tiff')
    with TiffFile(filename) as tif:
        assert tif.is_ome
        assert tif.byteorder == '<'
        assert len(tif.pages) == 50
        assert len(tif.series) == 1
        # assert page properties
        page = tif.pages.first
        assert page.is_contiguous
        assert page.tags['Software'].value == 'OME Bio-Formats 5.2.0-SNAPSHOT'
        assert page.compression == COMPRESSION.NONE
        assert page.imagewidth == 200
        assert page.imagelength == 200
        assert page.bitspersample == 8
        assert page.samplesperpixel == 1
        # assert series properties
        series = tif.series[0]
        assert series.shape == (10, 5, 200, 200)
        assert series.dtype == numpy.uint8
        assert series.axes == 'EPYX'
        assert series.kind == 'ome'
        assert not series.is_multifile
        # assert data
        data = tif.asarray()
        assert isinstance(data, numpy.ndarray)
        assert data.shape == (10, 5, 200, 200)
        assert data.dtype == numpy.uint8
        assert data[9, 4, 190, 192] == 92
        assert_aszarr_method(tif, data)
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_read_ome_multiimage_pixels():
    """Test read OME with three image series."""
    filename = _file('OME/bioformats-artificial/multi-image-pixels.ome.tif')
    with TiffFile(filename) as tif:
        assert tif.is_ome
        assert tif.byteorder == '>'
        assert len(tif.pages) == 86
        assert len(tif.series) == 3
        # assert page properties
        for i, axes, shape in (
            (0, 'CTYX', (2, 7, 555, 431)),
            (1, 'TZYX', (6, 2, 461, 348)),
            (2, 'TZCYX', (4, 5, 3, 239, 517)),
        ):
            series = tif.series[i]
            assert series.kind == 'ome'
            page = series.pages[0]
            assert page.is_contiguous
            assert page.tags['Software'].value == 'LOCI Bio-Formats'
            assert page.compression == COMPRESSION.NONE
            assert page.imagewidth == shape[-1]
            assert page.imagelength == shape[-2]
            assert page.bitspersample == 8
            assert page.samplesperpixel == 1
            # assert series properties
            assert series.shape == shape
            assert series.dtype == numpy.uint8
            assert series.axes == axes
            assert not series.is_multifile
            # assert data
            data = tif.asarray(series=i)
            assert isinstance(data, numpy.ndarray)
            assert data.shape == shape
            assert data.dtype == numpy.uint8
            assert_aszarr_method(series, data)
            assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_read_ome_multiimage_nouuid():
    """Test read single-file, multi-image OME without UUID."""
    filename = _file(
        'OMETIFF.jl/singles/181003_multi_pos_time_course_1_MMStack.ome.tif'
    )
    with TiffFile(filename, is_mmstack=False) as tif:
        assert tif.is_ome
        assert tif.byteorder == '<'
        assert len(tif.pages) == 20
        assert len(tif.series) == 2
        # assert page properties
        for i in (0, 1):
            series = tif.series[i]
            page = series.pages[0]
            assert page.is_imagej == (i == 0)
            assert page.is_ome == (i == 0)
            assert page.is_micromanager
            assert page.is_contiguous
            assert page.compression == COMPRESSION.NONE
            assert page.imagewidth == 256
            assert page.imagelength == 256
            assert page.bitspersample == 16
            assert page.samplesperpixel == 1
            # assert series properties
            assert series.shape == (10, 256, 256)
            assert series.dtype == numpy.uint16
            assert series.axes == 'TYX'
            assert series.kind == 'ome'
            assert not series.is_multifile
            # assert data
            data = tif.asarray(series=i)
            assert_array_equal(
                data, imread(filename, series=i, is_micromanager=False)
            )
            assert isinstance(data, numpy.ndarray)
            assert data.shape == (10, 256, 256)
            assert data.dtype == numpy.uint16
            assert data[5, 128, 128] == (18661, 16235)[i]
            assert_aszarr_method(series, data)
            assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_read_ome_zen_2chzt():
    """Test read OME time-series of two-channel volumes by ZEN 2011."""
    filename = _file('OME/zen_2chzt.ome.tiff')
    with TiffFile(filename) as tif:
        assert tif.is_ome
        assert tif.byteorder == '<'
        assert len(tif.pages) == 798
        assert len(tif.series) == 1
        # assert page properties
        page = tif.pages.first
        assert page.is_contiguous
        assert page.tags['Software'].value == 'ZEN 2011 (blue edition)'
        assert page.compression == COMPRESSION.NONE
        assert page.imagewidth == 400
        assert page.imagelength == 300
        assert page.bitspersample == 8
        assert page.samplesperpixel == 1
        # assert series properties
        series = tif.series[0]
        assert series.shape == (2, 19, 21, 300, 400)
        assert series.dtype == numpy.uint8
        assert series.axes == 'CTZYX'
        assert series.kind == 'ome'
        assert not series.is_multifile
        # assert data
        data = tif.asarray()
        assert isinstance(data, numpy.ndarray)
        assert data.shape == (2, 19, 21, 300, 400)
        assert data.dtype == numpy.uint8
        assert data[1, 10, 10, 100, 245] == 78
        assert_aszarr_method(tif, data)
        assert__str__(tif, 0)


@pytest.mark.skipif(SKIP_FILE or SKIP_LARGE, reason=REASON)
def test_read_ome_multifile():
    """Test read OME CTZYX series in 86 files."""
    # (2, 43, 10, 512, 512) CTZYX uint8 in 86 files, 10 pages each
    filename = _file('OME/tubhiswt-4D/tubhiswt_C0_TP10.ome.tif')
    with TiffFile(filename) as tif:
        assert tif.is_ome
        assert tif.byteorder == '<'
        assert len(tif.pages) == 10
        assert len(tif.series) == 1
        # assert page properties
        page = tif.pages.first
        assert page.is_contiguous
        assert page.tags['Software'].value[:15] == 'OME Bio-Formats'
        assert page.compression == COMPRESSION.NONE
        assert page.imagewidth == 512
        assert page.imagelength == 512
        assert page.bitspersample == 8
        assert page.samplesperpixel == 1
        # assert series properties
        series = tif.series[0]
        assert series.shape == (2, 43, 10, 512, 512)
        assert series.dtype == numpy.uint8
        assert series.axes == 'CTZYX'
        assert series.kind == 'ome'
        assert series.is_multifile
        # assert other files are closed after TiffFile._series_ome
        for page in tif.series[0].pages:
            assert bool(page.parent.filehandle._fh) == (page.parent == tif)
        # assert data
        data = tif.asarray(out='memmap')
        assert isinstance(data, numpy.memmap)
        assert data.shape == (2, 43, 10, 512, 512)
        assert data.dtype == numpy.uint8
        assert data[1, 42, 9, 426, 272] == 123
        # assert other files are still closed after TiffFile.asarray
        for page in tif.series[0].pages:
            assert bool(page.parent.filehandle._fh) == (page.parent == tif)
        assert tif.filehandle._fh
        assert__str__(tif)
        # test aszarr
        if not SKIP_SLOW:
            assert_aszarr_method(tif, data)
        assert_aszarr_method(tif, data, chunkmode='page')
        del data
        # assert other files are still closed after ZarrStore.close
        for page in tif.series[0].pages:
            assert bool(page.parent.filehandle._fh) == (page.parent == tif)

    # assert all files stay open
    # with TiffFile(filename) as tif:
    #     for page in tif.series[0].pages:
    #         self.assertTrue(page.parent.filehandle._fh)
    #     data = tif.asarray(out='memmap')
    #     for page in tif.series[0].pages:
    #         self.assertTrue(page.parent.filehandle._fh)


@pytest.mark.skipif(SKIP_FILE or SKIP_LARGE, reason=REASON)
def test_read_ome_multifile_missing(caplog):
    """Test read OME referencing missing files."""
    # (2, 43, 10, 512, 512) CTZYX uint8, 85 files missing
    filename = _file('OME/tubhiswt_C0_TP34.ome.tif')
    with TiffFile(filename) as tif:
        assert tif.is_ome
        assert tif.byteorder == '<'
        assert len(tif.pages) == 10
        assert len(tif.series) == 1
        assert 'failed to read' in caplog.text
        # assert page properties
        page = tif.pages.first
        TiffPage._str(page, 4)
        assert page.is_contiguous
        assert page.tags['Software'].value[:15] == 'OME Bio-Formats'
        assert page.compression == COMPRESSION.NONE
        assert page.imagewidth == 512
        assert page.imagelength == 512
        assert page.bitspersample == 8
        assert page.samplesperpixel == 1
        page = tif.pages[-1]
        TiffPage._str(page, 4)
        assert page.shape == (512, 512)
        # assert series properties
        series = tif.series[0]
        assert series.shape == (2, 43, 10, 512, 512)
        assert series.dtype == numpy.uint8
        assert series.axes == 'CTZYX'
        assert series.kind == 'ome'
        assert series.is_multifile
        # assert data
        data = tif.asarray(out='memmap')
        assert isinstance(data, numpy.memmap)
        assert data.shape == (2, 43, 10, 512, 512)
        assert data.dtype == numpy.uint8
        assert data[0, 34, 4, 303, 206] == 82
        assert data[1, 25, 2, 425, 272] == 196
        if not SKIP_SLOW:
            assert_aszarr_method(tif, data)
        assert_aszarr_method(tif, data, chunkmode='page')
        del data
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE or SKIP_LARGE, reason=REASON)
def test_read_ome_multifile_bytesio(caplog):
    """Test read OME multifile series with BinaryIO."""
    # (2, 43, 10, 512, 512) CTZYX uint8, 85 files missing
    filename = _file('OME/tubhiswt_C0_TP34.ome.tif')
    with open(filename, 'rb') as fh:
        bytesio = BytesIO(fh.read())
    with TiffFile(bytesio) as tif:
        assert tif.is_ome
        assert tif.byteorder == '<'
        assert len(tif.pages) == 10
        assert len(tif.series) == 1
        assert 'failed to read' in caplog.text
        # assert page properties
        page = tif.pages.first
        TiffPage._str(page, 4)
        assert page.is_contiguous
        assert page.tags['Software'].value[:15] == 'OME Bio-Formats'
        assert page.compression == COMPRESSION.NONE
        assert page.imagewidth == 512
        assert page.imagelength == 512
        assert page.bitspersample == 8
        assert page.samplesperpixel == 1
        page = tif.pages[-1]
        TiffPage._str(page, 4)
        assert page.shape == (512, 512)
        # assert series properties
        series = tif.series[0]
        assert series.shape == (2, 43, 10, 512, 512)
        assert series.dtype == numpy.uint8
        assert series.axes == 'CTZYX'
        assert series.kind == 'ome'
        assert not series.is_multifile
        # assert data
        data = tif.asarray()
        assert data.shape == (2, 43, 10, 512, 512)
        assert data.dtype == numpy.uint8
        assert data[0, 34, 4, 303, 206] == 82
        assert data[1, 25, 2, 425, 272] == 0
        if not SKIP_SLOW:
            assert_aszarr_method(tif, data, mode='r')
        del data
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE or SKIP_LARGE, reason=REASON)
def test_read_ome_multifile_pyramidal():
    """Test read OME pyramidal series in 35 files."""
    # (35, 27328, 54002), 7 levels, CYX uint16 in 35 files, 1 page each
    # https://forum.image.sc/t/119259
    filename = _file(
        'OME/multi-file-pyramid/morphology_focus/ch0000_dapi.ome.tif'
    )
    with TiffFile(filename) as tif:
        assert tif.is_ome
        assert tif.byteorder == '<'
        assert len(tif.pages) == 1
        assert len(tif.series) == 1
        # assert page properties
        page = tif.pages.first
        assert not page.is_contiguous
        assert page.tags['Software'].value[:15] == 'tifffile.py'
        assert page.compression == COMPRESSION.JPEG2000
        assert page.imagewidth == 54002
        assert page.imagelength == 27328
        assert page.bitspersample == 16
        assert page.samplesperpixel == 1

        # assert series properties
        series = tif.series[0]
        assert series.shape == (35, 27328, 54002)
        assert series.dtype == numpy.uint16
        assert series.axes == 'CYX'
        assert series.kind == 'ome'
        assert series.is_multifile
        assert series.is_pyramidal

        # assert other files are closed after TiffFile._series_ome
        for page in tif.series[0].pages:
            assert bool(page.parent.filehandle._fh) == (page.parent == tif)

        assert len(series.levels) == 8
        assert series.levels[0].name == 'Baseline'
        assert series.levels[0].shape == (35, 27328, 54002)
        assert series.levels[1].name == 'Level 1'
        assert series.levels[1].shape == (35, 13664, 27001)
        assert series.levels[2].shape == (35, 6832, 13500)
        assert series.levels[3].shape == (35, 3416, 6750)
        assert series.levels[4].shape == (35, 1708, 3375)
        assert series.levels[5].shape == (35, 854, 1687)
        assert series.levels[6].shape == (35, 427, 843)
        assert series.levels[7].shape == (35, 213, 421)

        # assert data
        data = tif.asarray(level=4)
        assert data.shape == (35, 1708, 3375)
        assert data.dtype == numpy.uint16
        assert data[9, 426, 272] == 99

        # assert other files are still closed after TiffFile.asarray
        for page in tif.series[0].pages:
            assert bool(page.parent.filehandle._fh) == (page.parent == tif)
        assert tif.filehandle._fh

        assert__str__(tif)

        # test aszarr
        assert_aszarr_method(series.levels[4], data)
        del data

        # assert other files are still closed after ZarrStore.close
        for page in tif.series[0].pages:
            assert bool(page.parent.filehandle._fh) == (page.parent == tif)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_read_ome_companion(caplog):
    """Test read multifile OME-TIFF using companion file."""
    filename = _file('OME/companion/multifile-Z2.ome.tiff')
    with TiffFile(filename) as tif:
        assert tif.is_ome
        with caplog.at_level(logging.DEBUG):
            assert tif.series[0].kind == 'generic'
            assert 'OME series is BinaryOnly' in caplog.text

    with open(
        _file('OME/companion/multifile.companion.ome'), encoding='utf-8'
    ) as fh:
        omexml = fh.read()
    with TiffFile(filename, omexml=omexml) as tif:
        assert tif.is_ome
        series = tif.series[0]
        assert series.kind == 'ome'
        image = series.asarray()

    filename = _file('OME/companion/multifile-Z1.ome.tiff')
    image2 = imread(filename)
    assert_array_equal(image, image2)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_read_ome_rgb():
    """Test read OME RGB image."""
    # https://github.com/openmicroscopy/bioformats/pull/1986
    filename = _file('OME/test_rgb.ome.tif')
    with TiffFile(filename) as tif:
        assert tif.is_ome
        assert tif.byteorder == '<'
        assert len(tif.pages) == 1
        assert len(tif.series) == 1
        # assert page properties
        page = tif.pages.first
        assert page.is_contiguous
        assert page.tags['Software'].value[:15] == 'OME Bio-Formats'
        assert page.compression == COMPRESSION.NONE
        assert page.imagewidth == 1280
        assert page.imagelength == 720
        assert page.bitspersample == 8
        assert page.samplesperpixel == 3
        # assert series properties
        series = tif.series[0]
        assert series.shape == (3, 720, 1280)
        assert series.dtype == numpy.uint8
        assert series.axes == 'SYX'
        assert series.kind == 'ome'
        assert series.dataoffset == 17524
        assert not series.is_multifile
        # assert data
        data = tif.asarray()
        assert data.shape == (3, 720, 1280)
        assert data.dtype == numpy.uint8
        assert data[1, 158, 428] == 253
        assert_aszarr_method(tif, data)
        assert_aszarr_method(tif, data, chunkmode='page')
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE or SKIP_CODECS, reason=REASON)
def test_read_ome_samplesperpixel():
    """Test read OME image stack with SamplesPerPixel>1."""
    # Reported by Grzegorz Bokota on 2019.1.30
    filename = _file('OME/test_samplesperpixel.ome.tif')
    with TiffFile(filename) as tif:
        assert tif.is_ome
        assert tif.byteorder == '>'
        assert len(tif.pages) == 6
        assert len(tif.series) == 1
        # assert page properties
        page = tif.pages.first
        assert page.tags['Software'].value[:15] == 'OME Bio-Formats'
        assert page.compression == COMPRESSION.LZW
        assert page.imagewidth == 1024
        assert page.imagelength == 1024
        assert page.bitspersample == 8
        assert page.samplesperpixel == 3
        # assert series properties
        series = tif.series[0]
        assert series.shape == (6, 3, 1024, 1024)
        assert series.dtype == numpy.uint8
        assert series.axes == 'ZSYX'
        assert series.kind == 'ome'
        assert not series.is_multifile
        # assert data
        data = tif.asarray()
        assert data.shape == (6, 3, 1024, 1024)
        assert data.dtype == numpy.uint8
        assert tuple(data[5, :, 191, 449]) == (253, 0, 28)
        assert_aszarr_method(tif, data)
        assert_aszarr_method(tif, data, chunkmode='page')
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_read_ome_float_modulo_attributes():
    """Test read OME with floating point modulo attributes."""
    # reported by Start Berg. File by Lorenz Maier.
    filename = _file('OME/float_modulo_attributes.ome.tiff')
    with TiffFile(filename) as tif:
        assert tif.is_ome
        assert tif.byteorder == '>'
        assert len(tif.pages) == 2
        assert len(tif.series) == 1
        # assert page properties
        page = tif.pages.first
        assert page.is_contiguous
        assert page.tags['Software'].value[:15] == 'OME Bio-Formats'
        assert page.compression == COMPRESSION.NONE
        assert page.imagewidth == 512
        assert page.imagelength == 512
        assert page.bitspersample == 16
        assert page.samplesperpixel == 1
        # assert series properties
        series = tif.series[0]
        assert series.shape == (2, 512, 512)
        assert series.dtype == numpy.uint16
        assert series.axes == 'QYX'
        assert series.kind == 'ome'
        assert not series.is_multifile
        # assert data
        data = tif.asarray()
        assert isinstance(data, numpy.ndarray)
        assert data.shape == (2, 512, 512)
        assert data.dtype == numpy.uint16
        assert data[1, 158, 428] == 51
        assert_aszarr_method(tif, data)
        assert_aszarr_method(tif, data, chunkmode='page')
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_read_ome_cropped(caplog):
    """Test read bad OME by ImageJ cropping."""
    # ImageJ produces invalid ome-xml when cropping
    # http://lists.openmicroscopy.org.uk/pipermail/ome-devel/2013-December
    #  /002631.html
    # Reported by Hadrien Mary on Dec 11, 2013
    filename = _file('ome/cropped.ome.tif')
    with TiffFile(filename) as tif:
        assert tif.is_ome
        assert tif.byteorder == '<'
        assert len(tif.pages) == 100
        assert len(tif.series) == 1
        assert 'invalid TiffData index' in caplog.text
        # assert page properties
        page = tif.pages.first
        assert page.tags['Software'].value[:15] == 'OME Bio-Formats'
        assert page.imagewidth == 324
        assert page.imagelength == 249
        assert page.bitspersample == 16
        # assert series properties
        series = tif.series[0]
        assert series.shape == (5, 10, 2, 249, 324)
        assert series.dtype == numpy.uint16
        assert series.axes == 'TZCYX'
        assert series.kind == 'ome'
        assert not series.is_multifile
        # assert data
        data = tif.asarray()
        assert data.shape == (5, 10, 2, 249, 324)
        assert data.dtype == numpy.uint16
        assert data[4, 9, 1, 175, 123] == 9605
        assert_aszarr_method(tif, data)
        assert_aszarr_method(tif, data, chunkmode='page')
        del data
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE or SKIP_CODECS or SKIP_LARGE, reason=REASON)
def test_read_ome_corrupted_page(caplog):
    """Test read OME with corrupted but not referenced page."""
    # https://forum.image.sc/t/qupath-0-2-0-not-able-to-open-ome-tiff/23821/3
    filename = _file('ome/2019_02_19__7760_s1.ome.tiff')
    with TiffFile(filename) as tif:
        assert tif.is_ome
        assert tif.is_bigtiff
        assert tif.byteorder == '<'
        assert len(tif.pages) == 5
        assert len(tif.series) == 1
        assert 'missing required tags' in caplog.text
        # assert page properties
        page = tif.pages.first
        assert page.imagewidth == 7506
        assert page.imagelength == 7506
        assert page.bitspersample == 16
        # assert series properties
        series = tif.series[0]
        assert series.shape == (4, 7506, 7506)
        assert series.dtype == numpy.uint16
        assert series.axes == 'CYX'
        assert series.kind == 'ome'
        assert not series.is_multifile
        # assert data
        data = tif.asarray()
        assert data.shape == (4, 7506, 7506)
        assert data.dtype == numpy.uint16
        assert tuple(data[:, 2684, 2684]) == (496, 657, 7106, 469)
        assert_aszarr_method(tif, data)
        assert_aszarr_method(tif, data, chunkmode='page')
        del data
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_read_ome_nikon():
    """Test read bad OME by Nikon."""
    # OME-XML references only first image
    # received from E. Gratton
    filename = _file('OME/Nikon-cell011.ome.tif')
    with TiffFile(filename) as tif:
        assert tif.is_ome
        assert tif.byteorder == '<'
        assert len(tif.pages) == 1000
        assert len(tif.series) == 1
        # assert 'index out of range' in caplog.text
        # assert page properties
        page = tif.pages.first
        assert page.photometric != PHOTOMETRIC.RGB
        assert page.imagewidth == 1982
        assert page.imagelength == 1726
        assert page.bitspersample == 16
        assert page.is_contiguous
        assert (
            page.tags['ImageLength'].offset - page.tags['ImageWidth'].offset
            == 20
        )
        assert page.tags['ImageWidth'].offset == 6856262146
        assert page.tags['ImageWidth'].valueoffset == 6856262158
        assert page.tags['ImageLength'].offset == 6856262166
        assert page.tags['ImageLength'].valueoffset == 6856262178
        assert page.tags['StripByteCounts'].offset == 6856262366
        assert page.tags['StripByteCounts'].valueoffset == 6856262534
        # assert series properties
        series = tif.series[0]
        assert len(series._pages) == 1
        assert len(series.pages) == 1
        assert series.dataoffset == 16  # contiguous
        assert series.shape == (1726, 1982)
        assert series.dtype == numpy.uint16
        assert series.axes == 'YX'
        assert series.kind == 'ome'
        assert__str__(tif)

    with TiffFile(filename, is_ome=False) as tif:
        assert not tif.is_ome
        # assert series properties
        series = tif.series[0]
        assert len(series.pages) == 1000
        assert series.dataoffset is None  # not contiguous
        assert series.shape == (1000, 1726, 1982)
        assert series.dtype == numpy.uint16
        assert series.axes == 'IYX'
        assert series.kind == 'uniform'
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_read_ome_shape_mismatch(caplog):
    """Test read OME with page shape mismatch."""
    # TCX (20000, 2, 500) is stored in 2 pages of (20000, 500)
    # probably exported by ZEN Software
    filename = _file('OME/Image 7.ome_h00.tiff')
    with TiffFile(filename) as tif:
        assert tif.is_ome
        assert tif.byteorder == '<'
        assert len(tif.pages) == 2
        assert len(tif.series) == 2
        assert 'cannot handle discontiguous storage' in caplog.text
        # assert page properties
        page = tif.pages.first
        assert page.is_contiguous
        assert page.photometric == PHOTOMETRIC.MINISBLACK
        assert page.imagewidth == 500
        assert page.imagelength == 20000
        assert page.bitspersample == 16
        assert page.samplesperpixel == 1
        page = tif.pages[1]
        assert page.is_contiguous
        assert page.photometric == PHOTOMETRIC.PALETTE
        assert page.imagewidth == 500
        assert page.imagelength == 20000
        assert page.bitspersample == 8
        assert page.samplesperpixel == 1
        # assert series properties
        series = tif.series[0]
        assert series.shape == (20000, 500)
        assert series.dtype == numpy.uint16
        assert series.axes == 'YX'
        assert series.dataoffset == 8
        assert series.kind == 'generic'


@pytest.mark.skipif(
    SKIP_FILE or SKIP_CODECS or not imagecodecs.JPEG2K.available,
    reason=REASON,
)
def test_read_ome_jpeg2000_be():
    """Test read JPEG2000 compressed big-endian OME-TIFF."""
    filename = _file('OME/mitosis.jpeg2000.ome.tif')
    with TiffFile(filename) as tif:
        assert tif.is_ome
        assert tif.byteorder == '>'
        assert len(tif.pages) == 510
        assert len(tif.series) == 1
        # assert page properties
        page = tif.pages.first
        assert not page.is_contiguous
        assert page.tags['Software'].value[:15] == 'OME Bio-Formats'
        assert page.compression == COMPRESSION.APERIO_JP2000_YCBC
        assert page.imagewidth == 171
        assert page.imagelength == 196
        assert page.bitspersample == 16
        assert page.samplesperpixel == 1
        # assert series properties
        series = tif.series[0]
        assert series.shape == (51, 5, 2, 196, 171)
        assert series.dtype == numpy.uint16
        assert series.axes == 'TZCYX'
        assert series.kind == 'ome'
        # assert data
        data = page.asarray()
        assert isinstance(data, numpy.ndarray)
        assert data.shape == (196, 171)
        assert data.dtype == numpy.uint16
        assert data[0, 0] == 1904
        assert_aszarr_method(page, data)
        assert_aszarr_method(page, data, chunkmode='page')
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE or SKIP_CODECS, reason=REASON)
def test_read_ome_samplesperpixel_mismatch(caplog):
    """Test read OME with SamplesPerPixel mismatch: OME=1, TIFF=4."""
    # https://forum.image.sc/t/ilastik-refuses-to-load-image-file/48541/1
    filename = _file('OME/MismatchSamplesPerPixel.ome.tif')
    with TiffFile(filename) as tif:
        assert tif.is_ome
        assert tif.byteorder == '<'
        assert len(tif.pages) == 1
        assert len(tif.series) == 1
        # assert page properties
        page = tif.pages.first
        assert page.photometric == PHOTOMETRIC.RGB
        assert page.compression == COMPRESSION.LZW
        assert page.imagewidth == 2080
        assert page.imagelength == 1552
        assert page.bitspersample == 8
        assert page.samplesperpixel == 4
        # assert series properties
        series = tif.series[0]
        assert 'cannot handle discontiguous storage' in caplog.text
        assert series.shape == (1552, 2080, 4)
        assert series.dtype == numpy.uint8
        assert series.axes == 'YXS'
        assert series.kind == 'generic'  # ome series failed
        assert not series.is_multifile
        # assert data
        data = tif.asarray()
        assert data.shape == (1552, 2080, 4)
        assert_aszarr_method(tif, data)
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE or SKIP_ZARR, reason=REASON)
@pytest.mark.parametrize('chunkmode', [0, 2])
def test_read_ome_multiscale(chunkmode):
    """Test read pyramidal OME file."""
    filename = _file('tifffile/multiscene_pyramidal.ome.tif')
    with TiffFile(filename) as tif:
        assert tif.is_ome
        assert tif.byteorder == '<'
        assert len(tif.pages) == 1025
        assert len(tif.series) == 2
        # assert page properties
        page = tif.pages.first
        assert page.photometric == PHOTOMETRIC.MINISBLACK
        assert page.compression == COMPRESSION.ADOBE_DEFLATE
        assert page.imagewidth == 256
        assert page.imagelength == 256
        assert page.bitspersample == 8
        assert page.samplesperpixel == 1
        # assert series properties
        series = tif.series[0]
        assert series.kind == 'ome'
        assert series.shape == (16, 32, 2, 256, 256)
        assert series.dtype == numpy.uint8
        assert series.axes == 'TZCYX'
        assert series.is_pyramidal
        assert not series.is_multifile
        series = tif.series[1]
        assert series.kind == 'ome'
        assert series.shape == (128, 128, 3)
        assert series.dtype == numpy.uint8
        assert series.axes == 'YXS'
        assert not series.is_pyramidal
        assert not series.is_multifile
        # assert data
        data = tif.asarray()
        assert data.shape == (16, 32, 2, 256, 256)
        assert_aszarr_method(tif, data, chunkmode=chunkmode)
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_read_andor_light_sheet_512p():
    """Test read Andor."""
    # 12113x13453, uint16
    filename = _file('andor/light sheet 512px.tif')
    with TiffFile(filename) as tif:
        assert tif.byteorder == '<'
        assert len(tif.pages) == 100
        assert len(tif.series) == 1
        assert tif.is_andor
        # assert page properties
        page = tif.pages.first
        assert page.is_andor
        assert page.is_contiguous
        assert page.compression == COMPRESSION.NONE
        assert page.imagewidth == 512
        assert page.imagelength == 512
        assert page.bitspersample == 16
        assert page.samplesperpixel == 1
        # assert metadata
        t = page.andor_tags
        assert t['SoftwareVersion'] == '4.23.30014.0'
        assert t['Frames'] == 100.0
        # assert series properties
        series = tif.series[0]
        assert series.shape == (100, 512, 512)
        assert series.dtype == numpy.uint16
        assert series.axes == 'IYX'
        assert series.kind == 'uniform'
        assert isinstance(series.pages[2], TiffFrame)
        # assert data
        data = tif.asarray()
        assert data.shape == (100, 512, 512)
        assert data.dtype == numpy.uint16
        assert round(abs(data[50, 256, 256] - 703), 7) == 0
        assert_aszarr_method(tif, data)
        assert_aszarr_method(tif, data, chunkmode='page')
        assert__str__(tif, 0)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_read_nih_morph():
    """Test read NIH."""
    # 388x252 uint8
    filename = _file('nihimage/morph.tiff')
    with TiffFile(filename) as tif:
        assert tif.is_nih
        assert tif.byteorder == '>'
        assert len(tif.pages) == 1
        assert len(tif.series) == 1
        # assert page properties
        page = tif.pages.first
        assert page.imagewidth == 388
        assert page.imagelength == 252
        assert page.bitspersample == 8
        # assert series properties
        series = tif.series[0]
        assert series.kind == 'nih'
        assert series.shape == (252, 388)
        assert series.dtype == numpy.uint8
        assert series.axes == 'YX'
        # assert NIH tags
        tags = tif.nih_metadata
        assert tags['FileID'] == 'IPICIMAG'
        assert tags['PixelsPerLine'] == 388
        assert tags['nLines'] == 252
        assert tags['ForegroundIndex'] == 255
        # assert data
        data = tif.asarray()
        assert isinstance(data, numpy.ndarray)
        assert data.shape == (252, 388)
        assert data.dtype == numpy.uint8
        assert data[195, 144] == 41
        assert_aszarr_method(tif, data)
        assert_aszarr_method(tif, data, chunkmode='page')
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_read_nih_silver_lake():
    """Test read NIH palette."""
    # 259x187 16 bit palette
    filename = _file('nihimage/silver lake.tiff')
    with TiffFile(filename) as tif:
        assert tif.is_nih
        assert tif.byteorder == '>'
        assert len(tif.pages) == 1
        assert len(tif.series) == 1
        # assert page properties
        page = tif.pages.first
        assert page.is_contiguous
        assert page.photometric == PHOTOMETRIC.PALETTE
        assert page.imagewidth == 259
        assert page.imagelength == 187
        assert page.bitspersample == 8
        # assert series properties
        series = tif.series[0]
        assert series.kind == 'nih'
        assert series.shape == (187, 259)
        assert series.dtype == numpy.uint8
        assert series.axes == 'YX'
        # assert NIH tags
        tags = tif.nih_metadata
        assert tags['FileID'] == 'IPICIMAG'
        assert tags['PixelsPerLine'] == 259
        assert tags['nLines'] == 187
        assert tags['ForegroundIndex'] == 109
        # assert data
        data = page.asrgb()
        assert isinstance(data, numpy.ndarray)
        assert data.shape == (187, 259, 3)
        assert data.dtype == numpy.uint16
        assert tuple(data[86, 102, :]) == (26214, 39321, 39321)
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_read_nih_scala_media():
    """Test read multi-page NIH."""
    # 36x54x84 palette
    filename = _file('nihimage/scala-media.tif')
    with TiffFile(filename) as tif:
        assert tif.is_nih
        assert tif.byteorder == '>'
        assert len(tif.pages) == 36
        assert len(tif.series) == 1
        # assert page properties
        page = tif.pages.first
        assert page.is_contiguous
        assert page.photometric == PHOTOMETRIC.PALETTE
        assert page.imagewidth == 84
        assert page.imagelength == 54
        assert page.bitspersample == 8
        # assert series properties
        series = tif.series[0]
        assert series.kind == 'nih'
        assert series.shape == (36, 54, 84)
        assert series.dtype == numpy.uint8
        assert series.axes == 'IYX'
        # assert NIH tags
        tags = tif.nih_metadata
        assert tags['Version'] == 160
        assert tags['nLines'] == 54
        # assert data
        data = tif.asarray()
        assert isinstance(data, numpy.ndarray)
        assert data.shape == (36, 54, 84)
        assert data.dtype == numpy.uint8
        assert data[35, 35, 65] == 171
        assert_aszarr_method(tif, data)
        assert_aszarr_method(tif, data, chunkmode='page')
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE or SKIP_CODECS, reason=REASON)
def test_read_imagej_rrggbb():
    """Test read planar RGB ImageJ file created by Bio-Formats."""
    filename = _file('tifffile/rrggbb.ij.tif')
    with TiffFile(filename) as tif:
        assert tif.is_imagej
        assert tif.byteorder == '<'
        assert len(tif.pages) == 1
        assert len(tif.series) == 1
        # assert page properties
        page = tif.pages.first
        assert page.photometric == PHOTOMETRIC.RGB
        assert page.compression == COMPRESSION.LZW
        assert page.imagewidth == 31
        assert page.imagelength == 32
        assert page.bitspersample == 16
        # assert series properties
        series = tif.series[0]
        assert series.kind == 'imagej'
        assert series.dtype == numpy.uint16
        assert series.shape == (3, 32, 31)
        assert series.axes == 'CYX'
        assert series.get_shape(squeeze=False) == (1, 1, 3, 32, 31, 1)
        assert series.get_axes(squeeze=False) == 'TZCYXS'
        assert len(series._pages) == 1
        assert len(series.pages) == 1
        # assert ImageJ tags
        ijmeta = tif.imagej_metadata
        assert ijmeta is not None
        assert ijmeta['ImageJ'] == ''
        assert ijmeta['images'] == 3
        assert ijmeta['channels'] == 3
        assert ijmeta['slices'] == 1
        assert ijmeta['frames'] == 1
        assert ijmeta['hyperstack']
        # assert data
        data = tif.asarray()
        assert isinstance(data, numpy.ndarray)
        assert data.shape == (3, 32, 31)
        assert data.dtype == numpy.uint16
        assert tuple(data[:, 15, 15]) == (812, 1755, 648)
        assert_decode_method(page)
        assert_aszarr_method(series, data)
        assert_aszarr_method(series, data, chunkmode='page')
        # don't squeeze
        data = tif.asarray(squeeze=False)
        assert data.shape == (1, 1, 3, 32, 31, 1)
        assert_aszarr_method(series, data, squeeze=False)
        assert__str__(tif, 0)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_read_imagej_focal1():
    """Test read ImageJ 205x434x425 uint8."""
    filename = _file('imagej/focal1.tif')
    with TiffFile(filename) as tif:
        assert tif.is_imagej
        assert tif.byteorder == '>'
        assert len(tif.pages) == 205
        assert len(tif.series) == 1
        # assert page properties
        page = tif.pages.first
        assert page.photometric != PHOTOMETRIC.RGB
        assert page.imagewidth == 425
        assert page.imagelength == 434
        assert page.bitspersample == 8
        assert page.is_contiguous
        # assert series properties
        series = tif.series[0]
        assert series.kind == 'imagej'
        assert series.dataoffset == 768
        assert series.shape == (205, 434, 425)
        assert series.dtype == numpy.uint8
        assert series.axes == 'IYX'
        assert series.get_shape(squeeze=False) == (205, 1, 1, 1, 434, 425, 1)
        assert series.get_axes(squeeze=False) == 'ITZCYXS'
        assert len(series._pages) == 1
        assert len(series.pages) == 205
        # assert ImageJ tags
        ijmeta = tif.imagej_metadata
        assert ijmeta is not None
        assert ijmeta['ImageJ'] == '1.34k'
        assert ijmeta['images'] == 205
        # assert data
        data = tif.asarray()
        assert isinstance(data, numpy.ndarray)
        assert data.shape == (205, 434, 425)
        assert data.dtype == numpy.uint8
        assert data[102, 216, 212] == 120
        assert_aszarr_method(series, data)
        assert_aszarr_method(series, data, chunkmode='page')
        assert__str__(tif, 0)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_read_imagej_hela_cells():
    """Test read ImageJ 512x672 RGB uint16."""
    filename = _file('imagej/hela-cells.tif')
    with TiffFile(filename) as tif:
        assert tif.is_imagej
        assert tif.byteorder == '>'
        assert len(tif.pages) == 1
        assert len(tif.series) == 1
        # assert page properties
        page = tif.pages.first
        assert page.photometric == PHOTOMETRIC.RGB
        assert page.imagewidth == 672
        assert page.imagelength == 512
        assert page.bitspersample == 16
        assert page.is_contiguous
        # assert series properties
        series = tif.series[0]
        assert series.kind == 'imagej'
        assert series.shape == (512, 672, 3)
        assert series.dtype == numpy.uint16
        assert series.axes == 'YXS'
        assert series.get_shape(squeeze=False) == (1, 1, 1, 512, 672, 3)
        assert series.get_axes(squeeze=False) == 'TZCYXS'
        # assert ImageJ tags
        ijmeta = tif.imagej_metadata
        assert ijmeta is not None
        assert ijmeta['ImageJ'] == '1.46i'
        assert ijmeta['channels'] == 3
        # assert data
        data = tif.asarray()
        assert isinstance(data, numpy.ndarray)
        assert data.shape == (512, 672, 3)
        assert data.dtype == numpy.uint16
        assert tuple(data[255, 336]) == (440, 378, 298)
        assert_aszarr_method(series, data)
        assert_aszarr_method(series, data, chunkmode='page')
        # don't squeeze
        data = tif.asarray(squeeze=False)
        assert data.shape == (1, 1, 1, 512, 672, 3)
        assert_aszarr_method(series, data, squeeze=False)
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_read_imagej_flybrain():
    """Test read ImageJ 57x256x256 RGB."""
    filename = _file('imagej/flybrain.tif')
    with TiffFile(filename) as tif:
        assert tif.is_imagej
        assert tif.byteorder == '>'
        assert len(tif.pages) == 57
        assert len(tif.series) == 1  # hyperstack
        # assert page properties
        page = tif.pages.first
        assert page.photometric == PHOTOMETRIC.RGB
        assert page.imagewidth == 256
        assert page.imagelength == 256
        assert page.bitspersample == 8
        # assert series properties
        series = tif.series[0]
        assert series.kind == 'imagej'
        assert series.shape == (57, 256, 256, 3)
        assert series.dtype == numpy.uint8
        assert series.axes == 'ZYXS'
        assert series.get_shape(squeeze=False) == (1, 57, 1, 256, 256, 3)
        assert series.get_axes(squeeze=False) == 'TZCYXS'
        # assert ImageJ tags
        ijmeta = tif.imagej_metadata
        assert ijmeta is not None
        assert ijmeta['ImageJ'] == '1.43d'
        assert ijmeta['slices'] == 57
        # assert data
        data = tif.asarray()
        assert isinstance(data, numpy.ndarray)
        assert data.shape == (57, 256, 256, 3)
        assert data.dtype == numpy.uint8
        assert tuple(data[18, 108, 97]) == (165, 157, 0)
        assert_aszarr_method(series, data)
        assert_aszarr_method(series, data, chunkmode='page')
        # don't squeeze
        data = tif.asarray(squeeze=False)
        assert data.shape == (1, 57, 1, 256, 256, 3)
        assert_aszarr_method(series, data, squeeze=False)
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_read_imagej_confocal_series():
    """Test read ImageJ 25x2x400x400 ZCYX."""
    filename = _file('imagej/confocal-series.tif')
    with TiffFile(filename) as tif:
        assert tif.is_imagej
        assert tif.byteorder == '>'
        assert len(tif.pages) == 50
        assert len(tif.series) == 1  # hyperstack
        # assert page properties
        page = tif.pages.first
        assert page.imagewidth == 400
        assert page.imagelength == 400
        assert page.bitspersample == 8
        assert page.is_contiguous
        # assert series properties
        series = tif.series[0]
        assert series.kind == 'imagej'
        assert series.shape == (25, 2, 400, 400)
        assert series.dtype == numpy.uint8
        assert series.axes == 'ZCYX'
        assert len(series._pages) == 1
        assert len(series.pages) == 50
        # assert ImageJ tags
        ijmeta = tif.imagej_metadata
        assert ijmeta is not None
        assert ijmeta['ImageJ'] == '1.43d'
        assert ijmeta['images'] == len(tif.pages)
        assert ijmeta['channels'] == 2
        assert ijmeta['slices'] == 25
        assert ijmeta['hyperstack']
        # assert data
        data = tif.asarray()
        assert isinstance(data, numpy.ndarray)
        assert data.shape == (25, 2, 400, 400)
        assert data.dtype == numpy.uint8
        assert tuple(data[12, :, 100, 300]) == (6, 66)
        # assert only two pages are loaded
        assert isinstance(tif.pages._loaded[0], TiffPage)
        if tif.pages.cache:
            assert isinstance(tif.pages._loaded[1], TiffFrame)
        else:
            assert tif.pages._offsets[1] == 8000911
        assert tif.pages._offsets[2] == 8001073
        assert tif.pages._offsets[-1] == 8008687
        assert_aszarr_method(series, data)
        assert_aszarr_method(series, data, chunkmode='page')
        # don't squeeze
        data = tif.asarray(squeeze=False)
        assert data.shape == (1, 25, 2, 400, 400, 1)
        assert_aszarr_method(series, data, squeeze=False)
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_read_imagej_graphite():
    """Test read ImageJ 1024x593 float32."""
    filename = _file('imagej/graphite1-1024.tif')
    with TiffFile(filename) as tif:
        assert tif.is_imagej
        assert tif.byteorder == '>'
        assert len(tif.pages) == 1
        assert len(tif.series) == 1
        # assert page properties
        page = tif.pages.first
        assert page.imagewidth == 1024
        assert page.imagelength == 593
        assert page.bitspersample == 32
        assert page.is_contiguous
        # assert series properties
        series = tif.series[0]
        assert series.kind == 'imagej'
        assert len(series._pages) == 1
        assert len(series.pages) == 1
        assert series.shape == (593, 1024)
        assert series.dtype == numpy.float32
        assert series.axes == 'YX'
        # assert ImageJ tags
        ijmeta = tif.imagej_metadata
        assert ijmeta is not None
        assert ijmeta['ImageJ'] == '1.47t'
        assert round(abs(ijmeta['max'] - 1686.10949707), 7) == 0
        assert round(abs(ijmeta['min'] - 852.08605957), 7) == 0
        # assert data
        data = tif.asarray()
        assert isinstance(data, numpy.ndarray)
        assert data.shape == (593, 1024)
        assert data.dtype == numpy.float32
        assert round(abs(data[443, 656] - 2203.040771484375), 7) == 0
        assert_aszarr_method(series, data)
        # don't squeeze
        data = tif.asarray(squeeze=False)
        assert data.shape == (1, 1, 1, 593, 1024, 1)
        assert_aszarr_method(series, data, squeeze=False)
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_read_imagej_bat_cochlea_volume():
    """Test read ImageJ 114 images, no frames, slices, channels specified."""
    filename = _file('imagej/bat-cochlea-volume.tif')
    with TiffFile(filename) as tif:
        assert tif.is_imagej
        assert tif.byteorder == '>'
        assert len(tif.pages) == 114
        assert len(tif.series) == 1
        # assert page properties
        page = tif.pages.first
        assert page.photometric != PHOTOMETRIC.RGB
        assert page.imagewidth == 121
        assert page.imagelength == 154
        assert page.bitspersample == 8
        assert page.is_contiguous
        # assert series properties
        series = tif.series[0]
        assert series.kind == 'imagej'
        assert len(series._pages) == 1
        assert len(series.pages) == 114
        assert series.shape == (114, 154, 121)
        assert series.dtype == numpy.uint8
        assert series.axes == 'IYX'
        # assert ImageJ tags
        ijmeta = tif.imagej_metadata
        assert ijmeta is not None
        assert ijmeta['ImageJ'] == '1.20n'
        assert ijmeta['images'] == 114
        # assert data
        data = tif.asarray()
        assert isinstance(data, numpy.ndarray)
        assert data.shape == (114, 154, 121)
        assert data.dtype == numpy.uint8
        assert data[113, 97, 61] == 255
        assert_aszarr_method(series, data)
        # don't squeeze
        data = tif.asarray(squeeze=False)
        assert data.shape == (114, 1, 1, 1, 154, 121, 1)
        assert_aszarr_method(series, data, squeeze=False)
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_read_imagej_first_instar_brain():
    """Test read ImageJ 56x256x256x3 ZYXS."""
    filename = _file('imagej/first-instar-brain.tif')
    with TiffFile(filename) as tif:
        assert tif.is_imagej
        assert tif.byteorder == '>'
        assert len(tif.pages) == 56
        assert len(tif.series) == 1
        # assert page properties
        page = tif.pages.first
        assert page.photometric == PHOTOMETRIC.RGB
        assert page.imagewidth == 256
        assert page.imagelength == 256
        assert page.bitspersample == 8
        assert page.is_contiguous
        # assert series properties
        series = tif.series[0]
        assert series.kind == 'imagej'
        assert len(series._pages) == 1
        assert len(series.pages) == 56
        assert series.shape == (56, 256, 256, 3)
        assert series.dtype == numpy.uint8
        assert series.axes == 'ZYXS'
        # assert ImageJ tags
        ijmeta = tif.imagej_metadata
        assert ijmeta is not None
        assert ijmeta['ImageJ'] == '1.44j'
        assert ijmeta['images'] == 56
        assert ijmeta['slices'] == 56
        # assert data
        data = tif.asarray()
        assert isinstance(data, numpy.ndarray)
        assert data.shape == (56, 256, 256, 3)
        assert data.dtype == numpy.uint8
        assert tuple(data[55, 151, 112]) == (209, 8, 58)
        assert_aszarr_method(series, data)
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_read_imagej_fluorescentcells():
    """Test read ImageJ three channels."""
    filename = _file('imagej/FluorescentCells.tif')
    with TiffFile(filename) as tif:
        assert tif.is_imagej
        assert tif.byteorder == '>'
        assert len(tif.pages) == 3
        assert len(tif.series) == 1
        # assert page properties
        page = tif.pages.first
        assert page.photometric == PHOTOMETRIC.PALETTE
        assert page.imagewidth == 512
        assert page.imagelength == 512
        assert page.bitspersample == 8
        assert page.is_contiguous
        # assert series properties
        series = tif.series[0]
        assert series.kind == 'imagej'
        assert series.shape == (3, 512, 512)
        assert series.dtype == numpy.uint8
        assert series.axes == 'CYX'
        # assert ImageJ tags
        ijmeta = tif.imagej_metadata
        assert ijmeta is not None
        assert ijmeta['ImageJ'] == '1.40c'
        assert ijmeta['images'] == 3
        assert ijmeta['channels'] == 3
        # assert data
        data = tif.asarray()
        assert isinstance(data, numpy.ndarray)
        assert data.shape == (3, 512, 512)
        assert data.dtype == numpy.uint8
        assert tuple(data[:, 256, 256]) == (57, 120, 13)
        assert_aszarr_method(series, data)
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE or SKIP_LARGE, reason=REASON)
def test_read_imagej_100000_pages():
    """Test read ImageJ with 100000 pages."""
    # 100000x64x64
    # file is big endian, memory mapped
    filename = _file('tifffile/100000_pages.tif')
    with TiffFile(filename) as tif:
        assert tif.is_imagej
        assert tif.byteorder == '>'
        assert len(tif.pages) == 100000
        assert len(tif.series) == 1
        # assert page properties
        page = tif.pages.first
        assert page.imagewidth == 64
        assert page.imagelength == 64
        assert page.bitspersample == 16
        assert page.is_contiguous
        # assert series properties
        series = tif.series[0]
        assert series.kind == 'imagej'
        assert len(series._pages) == 1
        assert len(series.pages) == 100000
        assert series.shape == (100000, 64, 64)
        assert series.dtype == numpy.uint16
        assert series.axes == 'TYX'
        # assert ImageJ tags
        ijmeta = tif.imagej_metadata
        assert ijmeta is not None
        assert ijmeta['ImageJ'] == '1.48g'
        assert round(abs(ijmeta['max'] - 119.0), 7) == 0
        assert round(abs(ijmeta['min'] - 86.0), 7) == 0
        # assert data
        data = tif.asarray(out='memmap')
        assert isinstance(data, numpy.memmap)
        assert data.shape == (100000, 64, 64)
        assert data.dtype == numpy.dtype('>u2')
        assert round(abs(data[7310, 25, 25] - 100), 7) == 0
        if not SKIP_SLOW:
            assert_aszarr_method(series, data)
        assert__str__(tif, 0)
        del data


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_read_imagej_invalid_metadata(caplog):
    """Test read bad ImageJ metadata."""
    # file contains 1 page but metadata claims 3500 images
    # memory map big endian data
    filename = _file('sima/0.tif')
    with TiffFile(filename) as tif:
        assert tif.is_imagej
        assert tif.byteorder == '>'
        assert len(tif.pages) == 1
        assert len(tif.series) == 1
        assert 'ImageJ series metadata invalid or corrupted' in caplog.text
        # assert page properties
        page = tif.pages.first
        assert page.photometric != PHOTOMETRIC.RGB
        assert page.imagewidth == 173
        assert page.imagelength == 173
        assert page.bitspersample == 16
        assert page.is_contiguous
        # assert series properties
        series = tif.series[0]
        assert series.kind == 'generic'  # imagej series failed
        assert series.dataoffset == 8  # 8
        assert series.shape == (173, 173)
        assert series.dtype == numpy.uint16
        assert series.axes == 'YX'
        # assert ImageJ tags
        ijmeta = tif.imagej_metadata
        assert ijmeta is not None
        assert ijmeta['ImageJ'] == '1.49i'
        assert ijmeta['images'] == 3500
        # assert data
        data = tif.asarray(out='memmap')
        assert isinstance(data, numpy.memmap)
        assert data.shape == (173, 173)
        assert data.dtype == numpy.dtype('>u2')
        assert data[94, 34] == 1257
        assert_aszarr_method(series, data)
        assert_aszarr_method(series, data, chunkmode='page')
        assert__str__(tif)
        del data


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_read_imagej_invalid_hyperstack():
    """Test read bad ImageJ hyperstack."""
    # file claims to be a hyperstack but is not stored as such
    # produced by OME writer
    # reported by Taras Golota on 10/27/2016
    filename = _file('imagej/X0.ome.CTZ.perm.tif')
    with TiffFile(filename) as tif:
        assert tif.is_imagej
        assert tif.byteorder == '<'
        assert len(tif.pages) == 48  # not a hyperstack
        assert len(tif.series) == 1
        # assert page properties
        page = tif.pages.first
        assert page.photometric != PHOTOMETRIC.RGB
        assert page.imagewidth == 1392
        assert page.imagelength == 1040
        assert page.bitspersample == 16
        assert page.is_contiguous
        # assert series properties
        series = tif.series[0]
        assert series.kind == 'imagej'
        assert series.dataoffset is None  # not contiguous
        assert series.shape == (2, 4, 6, 1040, 1392)
        assert series.dtype == numpy.uint16
        assert series.axes == 'TZCYX'
        # assert ImageJ tags
        ijmeta = tif.imagej_metadata
        assert ijmeta is not None
        assert ijmeta['hyperstack']
        assert ijmeta['images'] == 48
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_read_scifio():
    """Test read SCIFIO file using ImageJ metadata."""
    # https://github.com/AllenCellModeling/aicsimageio/issues/436
    # read
    filename = _file('scifio/2MF1P2_glia.tif')
    with TiffFile(filename) as tif:
        assert tif.is_imagej
        assert tif.byteorder == '>'
        assert len(tif.pages) == 343  # not a hyperstack
        assert len(tif.series) == 1
        # assert page properties
        page = tif.pages.first
        assert page.photometric == PHOTOMETRIC.MINISBLACK
        assert page.imagewidth == 1024
        assert page.imagelength == 1024
        assert page.bitspersample == 8
        assert page.is_contiguous
        # assert series properties
        series = tif.series[0]
        assert series.kind == 'imagej'
        assert series.dataoffset is None  # not contiguous
        assert series.shape == (343, 1024, 1024)
        assert series.dtype == numpy.uint8
        assert series.axes == 'IYX'
        assert isinstance(series.pages[2], TiffFrame)
        # assert ImageJ tags
        ijmeta = tif.imagej_metadata
        assert ijmeta is not None
        assert ijmeta['SCIFIO'] == '0.42.0'
        assert ijmeta['hyperstack']
        assert ijmeta['images'] == 343
        # assert data
        # data = series.asarray()
        # assert data[192, 740, 420] == 2
        # assert_aszarr_method(series, data)
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_read_fluoview_lsp1_v_laser():
    """Test read FluoView CTYX."""
    # raises 'UnicodeWarning: Unicode equal comparison failed' on Python 2
    filename = _file('fluoview/lsp1-V-laser0.3-1.tif')
    with TiffFile(filename) as tif:
        assert tif.byteorder == '<'
        assert len(tif.pages) == 100
        assert len(tif.series) == 1
        assert tif.is_fluoview
        # assert page properties
        page = tif.pages.first
        assert page.is_fluoview
        assert page.is_contiguous
        assert page.compression == COMPRESSION.NONE
        assert page.imagewidth == 256
        assert page.imagelength == 256
        assert page.bitspersample == 16
        assert page.samplesperpixel == 1
        # assert metadata
        m = fluoview_description_metadata(page.description)
        assert m['Version Info']['FLUOVIEW Version'] == (
            'FV10-ASW ,ValidBitColunt=12'
        )
        assert tuple(m['LUT Ch1'][255]) == (255, 255, 255)
        mm = tif.fluoview_metadata
        assert mm['ImageName'] == 'lsp1-V-laser0.3-1.oib'
        # assert series properties
        series = tif.series[0]
        assert series.shape == (2, 50, 256, 256)
        assert series.dtype == numpy.uint16
        assert series.axes == 'CTYX'
        # assert data
        data = tif.asarray()
        assert data.shape == (2, 50, 256, 256)
        assert data.dtype == numpy.uint16
        assert round(abs(data[1, 36, 128, 128] - 824), 7) == 0
        assert_aszarr_method(series, data)
        assert_aszarr_method(series, data, chunkmode='page')
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE or SKIP_LARGE, reason=REASON)
def test_read_fluoview_120816_bf_f0000():
    """Test read FluoView TZYX."""
    filename = _file('fluoview/120816_bf_f0000.tif')
    with TiffFile(filename) as tif:
        assert tif.byteorder == '<'
        assert len(tif.pages) == 864
        assert len(tif.series) == 1
        assert tif.is_fluoview
        # assert page properties
        page = tif.pages.first
        assert page.is_fluoview
        assert page.is_contiguous
        assert page.compression == COMPRESSION.NONE
        assert page.imagewidth == 1024
        assert page.imagelength == 1024
        assert page.bitspersample == 16
        assert page.samplesperpixel == 1
        # assert metadata
        m = fluoview_description_metadata(page.description)
        assert m['Environment']['User'] == 'admin'
        assert m['Region Info (Fields) Field']['Width'] == 1331.2
        m = tif.fluoview_metadata
        assert m['ImageName'] == '120816_bf'
        # assert series properties
        series = tif.series[0]
        assert series.shape == (144, 6, 1024, 1024)
        assert series.dtype == numpy.uint16
        assert series.axes == 'TZYX'
        # assert data
        data = tif.asarray()
        assert data.shape == (144, 6, 1024, 1024)
        assert data.dtype == numpy.uint16
        assert round(abs(data[1, 2, 128, 128] - 8317), 7) == 0
        if not SKIP_SLOW:
            assert_aszarr_method(series, data)
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE or SKIP_CODECS, reason=REASON)
def test_read_metaseries():
    """Test read MetaSeries 1040x1392 uint16, LZW."""
    # Strips do not contain an EOI code as required by the TIFF spec.
    filename = _file('metaseries/metaseries.tif')
    with TiffFile(filename) as tif:
        assert tif.byteorder == '<'
        assert len(tif.pages) == 1
        assert len(tif.series) == 1
        # assert page properties
        page = tif.pages.first
        assert page.imagewidth == 1392
        assert page.imagelength == 1040
        assert page.bitspersample == 16
        # assert metadata
        assert page.description.startswith('<MetaData>')
        # assert series properties
        series = tif.series[0]
        assert series.shape == (1040, 1392)
        assert series.dtype == numpy.uint16
        assert series.axes == 'YX'
        assert series.kind == 'uniform'
        # assert data
        data = tif.asarray()
        assert data.shape == (1040, 1392)
        assert data.dtype == numpy.uint16
        assert data[256, 256] == 1917
        assert_aszarr_method(series, data)
        assert_aszarr_method(series, data, chunkmode='page')
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_read_metaseries_g4d7r():
    """Test read Metamorph/Metaseries."""
    # 12113x13453, uint16
    import uuid

    filename = _file('metaseries/g4d7r.tif')
    with TiffFile(filename) as tif:
        assert tif.byteorder == '<'
        assert len(tif.pages) == 1
        assert len(tif.series) == 1
        assert tif.is_metaseries
        # assert page properties
        page = tif.pages.first
        assert page.is_metaseries
        assert page.is_contiguous
        assert page.compression == COMPRESSION.NONE
        assert page.imagewidth == 13453
        assert page.imagelength == 12113
        assert page.bitspersample == 16
        assert page.samplesperpixel == 1
        # assert metadata
        m = metaseries_description_metadata(page.description)
        assert m['ApplicationVersion'] == '7.8.6.0'
        assert m['PlaneInfo']['pixel-size-x'] == 13453
        assert m['SetInfo']['number-of-planes'] == 1
        assert m['PlaneInfo']['modification-time-local'] == datetime.datetime(
            2014, 10, 28, 16, 17, 16, 620000
        )
        assert m['PlaneInfo']['plane-guid'] == uuid.UUID(
            '213d9ee7-b38f-4598-9601-6474bf9d0c81'
        )
        # assert series properties
        series = tif.series[0]
        assert series.shape == (12113, 13453)
        assert series.dtype == numpy.uint16
        assert series.axes == 'YX'
        assert series.kind == 'uniform'
        # assert data
        data = tif.asarray(out='memmap')
        assert isinstance(data, numpy.memmap)
        assert data.shape == (12113, 13453)
        assert data.dtype == numpy.dtype('<u2')
        assert round(abs(data[512, 2856] - 4095), 7) == 0
        if not SKIP_LARGE:
            assert_aszarr_method(series, data)
            assert_aszarr_method(series, data, chunkmode='page')
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_read_mdgel_rat():
    """Test read Molecular Dynamics GEL."""
    # Second page does not contain data, only private tags
    filename = _file('mdgel/rat.gel')
    with TiffFile(filename) as tif:
        assert tif.byteorder == '<'
        assert len(tif.pages) == 2
        assert len(tif.series) == 1
        # assert page properties
        page = tif.pages.first
        assert page.is_contiguous
        assert page.compression == COMPRESSION.NONE
        assert page.imagewidth == 1528
        assert page.imagelength == 413
        assert page.bitspersample == 16
        assert page.samplesperpixel == 1
        assert page.tags['Software'].value == (
            'ImageQuant Software Release Version 2.0'
        )
        assert page.tags['PageName'].value == r'C:\DATA\RAT.GEL'

        # assert 2nd page properties
        page = tif.pages[1]
        assert page.is_mdgel
        assert page.imagewidth == 0
        assert page.imagelength == 0
        assert page.bitspersample == 1
        assert page.samplesperpixel == 1
        assert page.tags['MDFileTag'].value == 2
        assert page.tags['MDScalePixel'].value == (1, 21025)
        assert len(page.tags['MDColorTable'].value) == 17
        md = tif.mdgel_metadata
        assert md['SampleInfo'] == 'Rat slices from Dr. Schweitzer'
        assert md['PrepDate'] == '12 July 90'
        assert md['PrepTime'] == '40hr'
        assert md['FileUnits'] == 'Counts'

        # assert series properties
        series = tif.series[0]
        assert series.shape == (413, 1528)
        assert series.dtype == numpy.float32
        assert series.axes == 'YX'
        assert series.kind == 'mdgel'
        # assert data
        data = series.asarray()
        assert isinstance(data, numpy.ndarray)
        assert data.shape == (413, 1528)
        assert data.dtype == numpy.float32
        assert round(abs(data[260, 740] - 399.1728515625), 7) == 0
        assert_aszarr_method(series, data)
        assert_aszarr_method(series, data, chunkmode='page')
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_read_mediacy_imagepro():
    """Test read Media Cybernetics SEQ."""
    # TZYX, uint16, OME multifile TIFF
    filename = _file('mediacy/imagepro.tif')
    with TiffFile(filename) as tif:
        assert tif.byteorder == '<'
        assert len(tif.pages) == 1
        assert len(tif.series) == 1
        # assert page properties
        page = tif.pages.first
        assert page.is_mediacy
        assert page.is_contiguous
        assert page.compression == COMPRESSION.NONE
        assert page.imagewidth == 201
        assert page.imagelength == 201
        assert page.bitspersample == 8
        assert page.samplesperpixel == 1
        assert page.tags['Software'].value == 'Image-Pro Plus'
        assert page.tags['MC_Id'].value[:-1] == b'MC TIFF 4.0'
        # assert series properties
        series = tif.series[0]
        assert series.shape == (201, 201)
        assert series.dtype == numpy.uint8
        assert series.axes == 'YX'
        assert series.kind == 'uniform'
        # assert data
        data = tif.asarray()
        assert data.shape == (201, 201)
        assert data.dtype == numpy.uint8
        assert round(abs(data[120, 34] - 4), 7) == 0
        assert_aszarr_method(series, data)
        assert_aszarr_method(series, data, chunkmode='page')
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_read_pilatus_100k():
    """Test read Pilatus."""
    filename = _file('TvxPilatus/Pilatus100K_scan030_033.tiff')
    with TiffFile(filename) as tif:
        assert tif.byteorder == '<'
        assert len(tif.pages) == 1
        assert tif.is_pilatus
        # assert page properties
        page = tif.pages.first
        assert page.imagewidth == 487
        assert page.imagelength == 195
        assert page.bitspersample == 32
        assert page.samplesperpixel == 1
        # assert metadata
        assert page.tags['Model'].value == (
            'PILATUS 100K, S/N 1-0230, Cornell University'
        )
        attr = pilatus_description_metadata(page.description)
        assert attr['Tau'] == 1.991e-07
        assert attr['Silicon'] == 0.000320
        assert_aszarr_method(page)
        assert_aszarr_method(page, chunkmode='page')
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_read_pilatus_gibuf2():
    """Test read Pilatus."""
    filename = _file('TvxPilatus/GIbuf2_A9_18_001_0009.tiff')
    with TiffFile(filename) as tif:
        assert tif.byteorder == '<'
        assert len(tif.pages) == 1
        assert tif.is_pilatus
        # assert page properties
        page = tif.pages.first
        assert page.imagewidth == 487
        assert page.imagelength == 195
        assert page.bitspersample == 32
        assert page.samplesperpixel == 1
        # assert metadata
        assert page.tags['Model'].value == 'PILATUS 100K-S, S/N 1-0299,'
        attr = pilatus_description_metadata(page.description)
        assert attr['Filter_transmission'] == 1.0
        assert attr['Silicon'] == 0.000320
        assert_aszarr_method(page)
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_read_epics_attrib():
    """Test read EPICS."""
    filename = _file('epics/attrib.tif')
    with TiffFile(filename) as tif:
        assert tif.is_epics
        assert tif.byteorder == '<'
        assert len(tif.pages) == 1
        assert len(tif.series) == 1
        # assert series properties
        series = tif.series[0]
        assert series.shape == (2048, 2048)
        assert series.dtype == numpy.uint16
        assert series.axes == 'YX'
        assert series.kind == 'uniform'
        # assert page properties
        page = tif.pages.first
        assert page.shape == (2048, 2048)
        assert page.imagewidth == 2048
        assert page.imagelength == 2048
        assert page.bitspersample == 16
        assert page.is_contiguous
        # assert EPICS tags
        tags = tif.epics_metadata
        assert tags['timeStamp'] == 802117891.5714135
        assert tags['uniqueID'] == 15
        assert tags['Focus'] == 0.6778
        assert epics_datetime(
            tags['epicsTSSec'], tags['epicsTSNsec']
        ) == datetime.datetime(2015, 6, 2, 11, 31, 56, 103746)
        assert_aszarr_method(page)
        assert_aszarr_method(page, chunkmode='page')
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_read_tvips_tietz_16bit():
    """Test read TVIPS metadata."""
    # file provided by Marco Oster on 10/26/2016
    filename = _file('tvips/test_tietz_16bit.tif')
    with TiffFile(filename) as tif:
        assert tif.is_tvips
        tvips = tif.tvips_metadata
        assert tvips['Magic'] == 0xAAAAAAAA
        assert tvips['ImageFolder'] == 'B:\\4Marco\\Images\\Tiling_EMTOOLS\\'
        assert_aszarr_method(tif)
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_read_gdal_structural_metadata():
    """Test read non-TIFF GDAL structural metadata."""
    filename = _file('CloudOptimizedGeoTIFF/cog_rgb.tif')
    with TiffFile(filename) as tif:
        assert not tif.is_geotiff
        assert tif.byteorder == '<'
        assert len(tif.pages) == 2
        assert len(tif.series) == 1
        series = tif.series[0]
        assert series.shape == (256, 256, 3)
        assert series.dtype == numpy.uint8
        assert series.axes == 'YXS'
        assert series.kind == 'generic'
        assert tif.gdal_structural_metadata == {
            'LAYOUT': 'IFDS_BEFORE_DATA',
            'BLOCK_ORDER': 'ROW_MAJOR',
            'BLOCK_LEADER': 'SIZE_AS_UINT4',
            'BLOCK_TRAILER': 'LAST_4_BYTES_REPEATED',
            'KNOWN_INCOMPATIBLE_EDITION': 'NO',
        }


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_read_geotiff_dimapdocument():
    """Test read GeoTIFF with 43 MB XML tag value."""
    # tag 65000  45070067s @487  "<Dimap_Document...
    filename = _file('geotiff/DimapDocument.tif')
    with TiffFile(filename) as tif:
        assert tif.is_geotiff
        assert tif.byteorder == '>'
        assert len(tif.pages) == 1
        assert len(tif.series) == 1
        # assert series properties
        series = tif.series[0]
        assert series.shape == (1830, 1830)
        assert series.dtype == numpy.uint16
        assert series.axes == 'YX'
        assert series.kind == 'uniform'
        # assert page properties
        page = tif.pages.first
        assert page.shape == (1830, 1830)
        assert page.imagewidth == 1830
        assert page.imagelength == 1830
        assert page.bitspersample == 16
        assert page.is_contiguous
        assert page.tags['65000'].value.startswith(
            '<?xml version="1.0" encoding="ISO-8859-1"?>'
        )
        # assert GeoTIFF tags
        tags = tif.geotiff_metadata
        assert tags['GTCitationGeoKey'] == 'WGS 84 / UTM zone 29N'
        assert tags['ProjectedCSTypeGeoKey'] == 32629
        assert_allclose(
            tags['ModelTransformation'],
            [
                [60.0, 0.0, 0.0, 6.0e5],
                [0.0, -60.0, 0.0, 5900040.0],
                [0.0, 0.0, 0.0, 0.0],
                [0.0, 0.0, 0.0, 1.0],
            ],
        )
        assert_aszarr_method(page)
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_read_geotiff_spaf27_markedcorrect():
    """Test read GeoTIFF."""
    filename = _file('geotiff/spaf27_markedcorrect.tif')
    with TiffFile(filename) as tif:
        assert tif.is_geotiff
        assert tif.byteorder == '<'
        assert len(tif.pages) == 1
        assert len(tif.series) == 1
        # assert series properties
        series = tif.series[0]
        assert series.shape == (20, 20)
        assert series.dtype == numpy.uint8
        assert series.axes == 'YX'
        assert series.kind == 'uniform'
        # assert page properties
        page = tif.pages.first
        assert page.shape == (20, 20)
        assert page.imagewidth == 20
        assert page.imagelength == 20
        assert page.bitspersample == 8
        assert page.is_contiguous
        # assert GeoTIFF tags
        tags = tif.geotiff_metadata
        assert tags['GTCitationGeoKey'] == 'NAD27 / California zone VI'
        assert tags['GeogAngularUnitsGeoKey'] == 9102
        assert tags['ProjFalseOriginLatGeoKey'] == 32.1666666666667
        assert_allclose(tags['ModelPixelScale'], [195.509321, 198.32184, 0])
        assert_aszarr_method(page)
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_read_geotiff_cint16():
    """Test read complex integer images."""
    filename = _file('geotiff/cint16.tif')
    with TiffFile(filename) as tif:
        assert tif.is_geotiff
        assert tif.byteorder == '<'
        assert len(tif.pages) == 1
        assert len(tif.series) == 1
        # assert page properties
        page = tif.pages.first
        assert page.sampleformat == SAMPLEFORMAT.COMPLEXINT
        assert page.bitspersample == 32
        assert page.dtype == numpy.complex64
        assert page.shape == (100, 100)
        assert page.imagewidth == 100
        assert page.imagelength == 100
        assert page.compression == COMPRESSION.ADOBE_DEFLATE
        assert not page.is_contiguous
        data = page.asarray()
        assert data[9, 11] == 0 + 0j
        assert_aszarr_method(page, data)
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
@pytest.mark.parametrize('bits', [16, 32])
def test_read_complexint(bits):
    """Test read complex integer images."""
    filename = _file(f'gdal/cint{bits}.tif')
    with TiffFile(filename) as tif:
        assert tif.is_geotiff
        assert tif.byteorder == '<'
        assert len(tif.pages) == 1
        assert len(tif.series) == 1
        # assert page properties
        page = tif.pages.first
        assert page.sampleformat == SAMPLEFORMAT.COMPLEXINT
        assert page.bitspersample == bits * 2
        assert page.dtype == f'complex{bits * 4}'
        assert page.shape == (20, 20)
        assert page.imagewidth == 20
        assert page.imagelength == 20
        assert not page.is_contiguous
        data = page.asarray()
        assert data[9, 11] == 107 + 0j
        # assert GeoTIFF tags
        tags = tif.geotiff_metadata
        assert tags['GTCitationGeoKey'] == 'NAD27 / UTM zone 11N'
        assert_aszarr_method(page, data)
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE or SKIP_CODECS, reason=REASON)
def test_read_qpi():
    """Test read PerkinElmer-QPI, non Pyramid."""
    filename = _file('PerkinElmer-QPI/18-2470_2471_Scan1.qptiff')
    with TiffFile(filename) as tif:
        assert len(tif.series) == 4
        assert len(tif.pages) == 9
        assert tif.is_qpi
        page = tif.pages.first
        assert page.compression == COMPRESSION.JPEG
        assert page.photometric == PHOTOMETRIC.RGB
        assert page.planarconfig == PLANARCONFIG.CONTIG
        assert page.imagewidth == 34560
        assert page.imagelength == 57600
        assert page.bitspersample == 8
        assert page.samplesperpixel == 3
        assert page.tags['Software'].value == 'PerkinElmer-QPI'

        page = tif.pages[1]
        assert page.compression == COMPRESSION.LZW
        assert page.photometric == PHOTOMETRIC.RGB
        assert page.planarconfig == PLANARCONFIG.CONTIG
        assert page.imagewidth == 270
        assert page.imagelength == 450
        assert page.bitspersample == 8
        assert page.samplesperpixel == 3

        series = tif.series[0]
        assert series.kind == 'qpi'
        assert series.name == 'Baseline'
        assert series.shape == (57600, 34560, 3)
        assert series.dtype == numpy.uint8
        assert series.is_pyramidal
        assert len(series.levels) == 6

        series = tif.series[1]
        assert series.kind == 'qpi'
        assert series.name == 'Thumbnail'
        assert series.shape == (450, 270, 3)
        assert series.dtype == numpy.uint8
        assert not series.is_pyramidal

        series = tif.series[2]
        assert series.kind == 'qpi'
        assert series.name == 'Macro'
        assert series.shape == (4065, 2105, 3)
        assert series.dtype == numpy.uint8
        assert not series.is_pyramidal

        series = tif.series[3]
        assert series.kind == 'qpi'
        assert series.name == 'Label'
        assert series.shape == (453, 526, 3)
        assert series.dtype == numpy.uint8
        assert not series.is_pyramidal

        # assert data
        image = tif.asarray(series=1)
        image = tif.asarray(series=2)
        image = tif.asarray(series=3)
        image = tif.asarray(series=0, level=4)
        assert image.shape == (3600, 2160, 3)
        assert image.dtype == numpy.uint8
        assert tuple(image[1200, 1500]) == (244, 233, 229)
        assert_aszarr_method(tif, image, series=0, level=4)
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE or SKIP_CODECS, reason=REASON)
def test_read_qpi_nopyramid():
    """Test read PerkinElmer-QPI, non Pyramid."""
    filename = _file(
        'PerkinElmer-QPI/LuCa-7color_[13860,52919]_1x1component_data.tiff'
    )
    with TiffFile(filename) as tif:
        assert len(tif.series) == 2
        assert len(tif.pages) == 9
        assert tif.is_qpi
        page = tif.pages.first
        assert page.compression == COMPRESSION.LZW
        assert page.photometric == PHOTOMETRIC.MINISBLACK
        assert page.planarconfig == PLANARCONFIG.CONTIG
        assert page.imagewidth == 1868
        assert page.imagelength == 1400
        assert page.bitspersample == 32
        assert page.samplesperpixel == 1
        assert page.tags['Software'].value == 'PerkinElmer-QPI'
        series = tif.series[0]
        assert series.kind == 'qpi'
        assert series.shape == (8, 1400, 1868)
        assert series.dtype == numpy.float32
        assert not series.is_pyramidal
        series = tif.series[1]
        assert series.kind == 'qpi'
        assert series.shape == (350, 467, 3)
        assert series.dtype == numpy.uint8
        assert not series.is_pyramidal
        # assert data
        image = tif.asarray()
        assert image.shape == (8, 1400, 1868)
        assert image.dtype == numpy.float32
        assert image[7, 1200, 1500] == 2.2132580280303955
        image = tif.asarray(series=1)
        assert image.shape == (350, 467, 3)
        assert image.dtype == numpy.uint8
        assert image[300, 400, 1] == 48
        assert_aszarr_method(tif, image, series=1)
        assert_aszarr_method(tif, image, series=1, chunkmode='page')
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_read_indica():
    """Test read Indica Labs pyramid."""
    # https://forum.image.sc/t/89191/4
    filename = _file('Indica/mouse_link.tif')
    with TiffFile(filename) as tif:
        assert len(tif.series) == 1
        assert len(tif.pages) == 40
        assert tif.is_indica
        assert tif.indica_metadata.endswith('</indica>')
        page = tif.pages.first
        assert page.compression == COMPRESSION.ADOBE_DEFLATE
        assert page.photometric == PHOTOMETRIC.MINISBLACK
        assert page.shape == (26836, 18282)
        assert page.bitspersample == 32
        assert page.samplesperpixel == 1
        assert page.dtype == numpy.float32
        assert page.tags['Software'].value == 'IndicaLabsImageWriter v1.2.1'
        series = tif.series[0]
        assert series.kind == 'generic'  # 'indica'
        assert series.axes == 'IYX'  # 'CYX'
        assert series.shape == (8, 26836, 18282)
        assert series.dtype == numpy.float32
        assert len(series.levels) == 5
        assert series.is_pyramidal
        # assert data
        image = tif.asarray(series=0, level=3)
        assert image.shape == (8, 3355, 2286)
        assert image.dtype == numpy.float32
        assert_allclose(image[7, 3000, 1000], 6.0852714, rtol=1e-5)
        assert_aszarr_method(series, image, level=3)
        assert_aszarr_method(series, image, level=3, chunkmode='page')
        assert__str__(tif)


@pytest.mark.skipif(SKIP_CODECS or SKIP_FILE, reason=REASON)
def test_read_avs():
    """Test read Argos AVS pyramid."""
    # https://github.com/openslide/openslide/issues/614
    filename = _file('ArgosAVS/TestSlide1_ZStack.avs')
    with TiffFile(filename) as tif:
        assert len(tif.series) == 3
        assert len(tif.pages) == 42
        assert tif.is_avs
        assert tif.avs_metadata.startswith('<Argos.Scan.Metadata>')
        page = tif.pages.first
        assert page.compression == COMPRESSION.JPEG
        assert page.photometric == PHOTOMETRIC.YCBCR
        assert page.shape == (57440, 130546, 3)
        assert page.bitspersample == 8
        assert page.samplesperpixel == 3
        assert page.dtype == numpy.uint8
        series = tif.series[0]
        assert series.kind == 'avs'
        assert series.name == 'Baseline'
        assert series.axes == 'ZYXS'
        assert series.shape == (5, 57440, 130546, 3)
        assert series.dtype == numpy.uint8
        assert series.is_pyramidal
        assert len(series.levels) == 8
        series = tif.series[1]
        assert series.kind == 'avs'
        assert series.name == 'Map'
        assert series.axes == 'YXS'
        assert series.shape == (1399, 3180, 3)
        series = tif.series[2]
        assert series.kind == 'avs'
        assert series.name == 'Macro'
        assert series.axes == 'YXS'
        assert series.shape == (508, 1489, 3)
        # assert data
        series = tif.series[0]
        image = tif.asarray(series=0, level=4)
        assert image.shape == (5, 3590, 8159, 3)
        assert image.dtype == numpy.uint8
        assert image[2, 900, 3000, 0] == 218
        assert_aszarr_method(series, image, level=4)
        assert_aszarr_method(series, image, level=4, chunkmode='page')
        assert__str__(tif)


@pytest.mark.skipif(
    SKIP_FILE or SKIP_CODECS or not imagecodecs.JPEG.available,
    reason=REASON,
)
def test_read_philips():
    """Test read Philips DP pyramid."""
    # https://camelyon17.grand-challenge.org/Data/
    filename = _file('PhilipsDP/test_001.tif')
    with TiffFile(filename) as tif:
        assert len(tif.series) == 1
        assert len(tif.pages) == 9
        assert tif.is_philips
        assert tif.philips_metadata.endswith('</DataObject>')

        page = tif.pages.first
        assert page.compression == COMPRESSION.JPEG
        assert page.photometric == PHOTOMETRIC.YCBCR
        assert page.planarconfig == PLANARCONFIG.CONTIG
        assert page.tags['ImageWidth'].value == 86016
        assert page.tags['ImageLength'].value == 89600
        assert page.imagewidth == 86016
        assert page.imagelength == 89600
        assert page.bitspersample == 8
        assert page.samplesperpixel == 3
        assert page.tags['Software'].value == 'Philips DP v1.0'

        series = tif.series[0]
        assert series.kind == 'philips'
        assert series.shape == (89600, 86016, 3)
        assert len(series.levels) == 9
        assert series.is_pyramidal
        assert series.levels[1].shape == (44800, 43008, 3)
        assert series.levels[2].shape == (22400, 21504, 3)
        assert series.levels[3].shape == (11200, 10752, 3)
        assert series.levels[4].shape == (5600, 5376, 3)
        assert series.levels[5].shape == (2800, 2688, 3)
        assert series.levels[6].shape == (1400, 1344, 3)
        assert series.levels[7].shape == (700, 672, 3)
        assert series.levels[8].shape == (350, 336, 3)

        page = series.levels[1].keyframe
        assert page.compression == COMPRESSION.JPEG
        assert page.photometric == PHOTOMETRIC.YCBCR
        assert page.planarconfig == PLANARCONFIG.CONTIG
        assert page.tags['ImageWidth'].value == 43008
        assert page.tags['ImageLength'].value == 45056
        assert page.imagewidth == 43008
        assert page.imagelength == 44800

        for level in range(1, 9):
            tif.asarray(series=0, level=level)

        # assert data
        image = tif.asarray(series=0, level=5)
        assert image.shape == (2800, 2688, 3)
        assert image[300, 400, 1] == 206
        assert_aszarr_method(series, image, level=5)
        assert_aszarr_method(series, image, level=5, chunkmode='page')
        assert__str__(tif)


@pytest.mark.skipif(
    SKIP_FILE or SKIP_CODECS or not imagecodecs.JPEG.available,
    reason=REASON,
)
def test_read_philips_issue249():
    """Test write_fsspec with Philips slide missing row of tiles."""
    # https://github.com/cgohlke/tifffile/issues/249
    filename = _file('PhilipsDP/patient_080_node_2.tif')
    with TiffFile(filename) as tif:
        assert len(tif.series) == 2
        assert len(tif.pages) == 11
        assert tif.is_philips
        assert tif.philips_metadata.endswith('</DataObject>')

        page = tif.pages.first
        assert page.compression == COMPRESSION.JPEG
        assert page.photometric == PHOTOMETRIC.YCBCR
        assert page.planarconfig == PLANARCONFIG.CONTIG
        assert page.tags['ImageWidth'].value == 155136
        assert page.tags['ImageLength'].value == 78336
        assert page.imagewidth == 155136
        assert page.imagelength == 78336
        assert page.bitspersample == 8
        assert page.samplesperpixel == 3
        assert page.tags['Software'].value == 'Philips DP v1.0'

        series = tif.series[1]
        assert series.kind == 'philips'
        assert series.name == 'Macro'
        assert series.shape == (801, 1756, 3)
        assert len(series.levels) == 1
        assert not series.is_pyramidal

        series = tif.series[0]
        assert series.kind == 'philips'
        assert series.name == 'Baseline'
        assert series.shape == (78336, 155136, 3)
        assert len(series.levels) == 10
        assert series.is_pyramidal

        page = series.levels[4].keyframe
        assert page.compression == COMPRESSION.JPEG
        assert page.photometric == PHOTOMETRIC.YCBCR
        assert page.planarconfig == PLANARCONFIG.CONTIG
        assert page.tags['ImageWidth'].value == 9728
        assert page.tags['ImageLength'].value == 5120
        assert page.imagewidth == 9696
        assert page.imagelength == 4896

        for level in range(3, 10):
            tif.asarray(series=0, level=level)

        # assert data
        image = tif.asarray(series=0, level=4)
        assert image.shape == (4896, 9696, 3)
        assert image[300, 400, 1] == 225
        assert_aszarr_method(series, image, level=4)
        assert__str__(tif)

        if SKIP_ZARR:
            return

        # write fsspec
        from imagecodecs.numcodecs import register_codecs

        register_codecs('imagecodecs_jpeg', verbose=False)
        url = os.path.dirname(filename).replace('\\', '/')
        with TempFileName('issue_philips_fsspec', ext='.json') as jsonfile:
            page.aszarr().write_fsspec(jsonfile, url, version=0)
            zobj = zarr.open(refs_as_store(jsonfile), zarr_format=2)
            assert_array_equal(zobj[:], image)


@pytest.mark.skipif(
    SKIP_FILE or SKIP_CODECS or not imagecodecs.JPEG.available,
    reason=REASON,
)
def test_read_philips_issue253():
    """Test read Philips DP pyramid with seemingly extra column of tiles."""
    # https://github.com/cgohlke/tifffile/issues/253
    # https://registry.opendata.aws/camelyon/
    filename = _file('PhilipsDP/sample.tiff')
    with TiffFile(filename) as tif:
        assert len(tif.series) == 3
        assert len(tif.pages) == 12
        assert tif.is_philips
        assert tif.philips_metadata.endswith('</DataObject>')

        page = tif.pages.first
        assert page.compression == COMPRESSION.JPEG
        assert page.photometric == PHOTOMETRIC.RGB
        assert page.planarconfig == PLANARCONFIG.CONTIG
        assert page.tags['ImageWidth'].value == 188416
        assert page.tags['ImageLength'].value == 93696
        assert page.imagewidth == 188416
        assert page.imagelength == 93696
        assert page.bitspersample == 8
        assert page.samplesperpixel == 3
        assert page.tags['Software'].value == 'Philips DP v1.0'

        series = tif.series[1]
        assert series.kind == 'philips'
        assert series.name == 'Macro'
        assert series.shape == (812, 1806, 3)
        assert not series.is_pyramidal

        series = tif.series[2]
        assert series.kind == 'philips'
        assert series.name == 'Label'
        assert series.shape == (812, 671, 3)
        assert not series.is_pyramidal

        series = tif.series[0]
        assert series.kind == 'philips'
        assert series.name == 'Baseline'
        assert series.shape == (93696, 188416, 3)
        assert len(series.levels) == 10
        assert series.is_pyramidal
        assert series.levels[1].shape == (46848, 94208, 3)
        assert series.levels[2].shape == (23424, 47104, 3)
        assert series.levels[3].shape == (11712, 23552, 3)
        assert series.levels[4].shape == (5856, 11776, 3)
        assert series.levels[5].shape == (2928, 5888, 3)
        assert series.levels[6].shape == (1464, 2944, 3)
        assert series.levels[7].shape == (732, 1472, 3)
        assert series.levels[8].shape == (366, 736, 3)
        assert series.levels[9].shape == (183, 368, 3)

        page = series.levels[1].keyframe
        assert page.compression == COMPRESSION.JPEG
        assert page.photometric == PHOTOMETRIC.RGB
        assert page.planarconfig == PLANARCONFIG.CONTIG
        assert page.tags['ImageWidth'].value == 94208
        assert page.tags['ImageLength'].value == 47104
        assert page.imagewidth == 94208
        assert page.imagelength == 46848

        for level in range(1, 10):
            tif.asarray(series=0, level=level)

        # assert data
        image = tif.asarray(series=0, level=5)
        assert image.shape == (2928, 5888, 3)
        assert image[300, 400, 1] == 254
        assert_aszarr_method(series, image, level=5)
        assert_aszarr_method(series, image, level=5, chunkmode='page')
        assert__str__(tif)


@pytest.mark.skipif(
    SKIP_FILE or SKIP_CODECS or not imagecodecs.JPEG.available,
    reason=REASON,
)
def test_read_zif():
    """Test read Zoomable Image Format ZIF."""
    filename = _file('zif/ZoomifyImageExample.zif')
    with TiffFile(filename) as tif:
        # assert tif.is_zif
        assert len(tif.pages) == 5
        assert len(tif.series) == 1
        for page in tif.pages:
            assert page.description == (
                'Created by Objective Pathology Services'
            )
        # first page
        page = tif.pages.first
        assert page.photometric == PHOTOMETRIC.YCBCR
        assert page.compression == COMPRESSION.JPEG
        assert page.shape == (3120, 2080, 3)
        assert tuple(page.asarray()[3110, 2070, :]) == (27, 45, 59)
        # page 4
        page = tif.pages[-1]
        assert page.photometric == PHOTOMETRIC.YCBCR
        assert page.compression == COMPRESSION.JPEG
        assert page.shape == (195, 130, 3)
        assert tuple(page.asarray()[191, 127, :]) == (30, 49, 66)
        # series
        series = tif.series[0]
        assert series.kind == 'generic'
        assert series.is_pyramidal
        assert len(series.levels) == 5
        assert series.shape == (3120, 2080, 3)
        assert tuple(series.asarray()[3110, 2070, :]) == (27, 45, 59)
        assert series.levels[-1].shape == (195, 130, 3)
        assert tuple(series.asarray(level=-1)[191, 127, :]) == (30, 49, 66)
        assert_aszarr_method(series, level=-1)
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE or SKIP_CODECS, reason=REASON)
def test_read_vsi():
    """Test read Olympus VSI."""
    filename = _file('VSI/brightfield.vsi')
    with TiffFile(filename) as tif:
        assert not tif.is_sis
        assert tif.byteorder == '<'
        assert len(tif.pages) == 5
        assert len(tif.series) == 5
        # assert page properties
        page = tif.pages.first
        assert not page.is_contiguous
        assert page.imagewidth == 991
        assert page.imagelength == 375
        assert page.bitspersample == 8
        assert page.samplesperpixel == 3
        assert page.tags['Artist'].value == 'ics'
        # assert data
        data = tif.asarray()
        assert data.shape == (375, 991, 3)
        assert data[200, 256, 1] == 3
        # assert metadata
        sis = tif.sis_metadata
        assert sis['magnification'] == 1.0
        assert_aszarr_method(tif, data)
        assert_aszarr_method(tif, data, chunkmode='page')
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_read_sis():
    """Test read Olympus SIS."""
    filename = _file('sis/4A5IE8EM_F00000409.tif')
    with TiffFile(filename) as tif:
        assert tif.is_sis
        assert tif.byteorder == '<'
        assert len(tif.pages) == 122
        assert len(tif.series) == 1
        # assert page properties
        page = tif.pages.first
        assert page.is_contiguous
        assert page.imagewidth == 353
        assert page.imagelength == 310
        assert page.bitspersample == 16
        assert page.samplesperpixel == 1
        assert page.tags['Software'].value == 'analySIS 5.0'
        # assert data
        data = tif.asarray()
        assert data.shape == (61, 2, 310, 353)
        assert data[30, 1, 256, 256] == 210
        # assert metadata
        sis = tif.sis_metadata
        assert sis is not None
        assert sis['axes'] == 'TC'
        assert sis['shape'] == (61, 2)
        assert sis['Band'][1]['BandName'] == 'Fura380'
        assert sis['Band'][0]['LUT'].shape == (256, 3)
        assert sis['Time']['TimePos'].shape == (61,)
        assert sis['name'] == 'Hela-Zellen'
        assert sis['magnification'] == 60.0
        assert_aszarr_method(tif, data)
        assert_aszarr_method(tif, data, chunkmode='page')
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_read_sis_noini():
    """Test read Olympus SIS without INI tag."""
    filename = _file('sis/110.tif')
    with TiffFile(filename) as tif:
        assert tif.is_sis
        assert tif.byteorder == '<'
        assert len(tif.pages) == 1
        assert len(tif.series) == 1
        # assert page properties
        page = tif.pages.first
        assert page.imagewidth == 2560
        assert page.imagelength == 1920
        assert page.bitspersample == 8
        assert page.samplesperpixel == 3
        # assert metadata
        sis = tif.sis_metadata
        assert 'axes' not in sis
        assert sis['magnification'] == 20.0
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_read_sem_metadata():
    """Test read Zeiss SEM metadata."""
    # file from hyperspy tests
    filename = _file('hyperspy/test_tiff_Zeiss_SEM_1k.tif')
    with TiffFile(filename) as tif:
        assert tif.is_sem
        assert tif.byteorder == '<'
        assert len(tif.pages) == 1
        assert len(tif.series) == 1
        # assert page properties
        page = tif.pages.first
        assert page.is_contiguous
        assert page.photometric == PHOTOMETRIC.PALETTE
        assert page.imagewidth == 1024
        assert page.imagelength == 768
        assert page.bitspersample == 8
        assert page.samplesperpixel == 1
        # assert data and metadata
        data = page.asrgb()
        assert tuple(data[563, 320]) == (38550, 38550, 38550)
        sem = tif.sem_metadata
        assert sem[''][3] == 2.614514e-06
        assert sem['ap_date'] == ('Date', '23 Dec 2015')
        assert sem['ap_time'] == ('Time', '9:40:32')
        assert sem['dp_image_store'] == ('Store resolution', '1024 * 768')
        assert sem['ap_fib_fg_emission_actual'] == (
            'Flood Gun Emission Actual',
            0.0,
            'µA',
        )
        assert_aszarr_method(tif)
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_read_sem_bad_metadata():
    """Test read Zeiss SEM metadata with wrong length."""
    # reported by Klaus Schwarzburg on 8/27/2018
    filename = _file('issues/sem_bad_metadata.tif')
    with TiffFile(filename) as tif:
        assert tif.is_sem
        assert tif.byteorder == '<'
        assert len(tif.pages) == 1
        assert len(tif.series) == 1
        # assert page properties
        page = tif.pages.first
        assert page.is_contiguous
        assert page.photometric == PHOTOMETRIC.PALETTE
        assert page.imagewidth == 1024
        assert page.imagelength == 768
        assert page.bitspersample == 8
        assert page.samplesperpixel == 1
        # assert data and metadata
        data = page.asrgb()
        assert tuple(data[350, 150]) == (17476, 17476, 17476)
        sem = tif.sem_metadata
        assert sem['sv_version'][1] == 'V05.07.00.00 : 08-Jul-14'
        assert_aszarr_method(tif)
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_read_fei_metadata():
    """Test read Helios FEI metadata."""
    # file from hyperspy tests
    filename = _file('hyperspy/test_tiff_FEI_SEM.tif')
    with TiffFile(filename) as tif:
        assert tif.is_fei
        assert tif.byteorder == '<'
        assert len(tif.pages) == 1
        assert len(tif.series) == 1
        # assert page properties
        page = tif.pages.first
        assert page.is_contiguous
        assert page.photometric != PHOTOMETRIC.PALETTE
        assert page.imagewidth == 1536
        assert page.imagelength == 1103
        assert page.bitspersample == 8
        assert page.samplesperpixel == 1
        # assert data and metadata
        data = page.asarray()
        assert data[563, 320] == 220
        fei = tif.fei_metadata
        assert fei['User']['User'] == 'supervisor'
        assert fei['System']['DisplayHeight'] == 0.324
        assert_aszarr_method(tif)
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE or SKIP_LARGE or SKIP_ZARR, reason=REASON)
def test_read_mmstack_multifile(caplog):
    """Test read MicroManager 2.0 multi-file, multi-position dataset."""
    # TODO: what version of MicroManager does not write corrupted files?
    # second ImageDescription tag value is beyond 4 GB
    # MicroManager headers are beyond 4 GB
    # MicroManager display settings are truncated
    filename = _file('MMStack/NDTiff.index/_4_MMStack_Pos0.ome.tif')
    with TiffFile(filename) as tif:
        assert 'coercing invalid ASCII to bytes' in caplog.text
        assert tif.is_micromanager
        assert tif.is_mmstack
        assert tif.is_ome
        assert not tif.is_imagej
        assert not tif.is_ndtiff
        assert tif.byteorder == '<'
        assert len(tif.pages) == 8092
        assert len(tif.series) == 1
        assert 'failed to read display settings' not in caplog.text
        assert 'failed to read comments: invalid header' not in caplog.text
        # assert metadata
        meta = tif.micromanager_metadata
        assert meta is not None
        assert meta['MajorVersion'] == 0
        assert meta['Summary']['MicroManagerVersion'] == '2.0.0'
        assert meta['Summary']['Prefix'] == '_4'
        assert meta['IndexMap'].shape == (8092, 5)
        assert 'Comments' in meta
        # assert series properties
        series = tif.series[0]
        assert len(series) == 17472
        assert series.shape == (91, 2, 48, 2, 512, 512)
        assert series.axes == 'TRZCYX'
        assert series.kind == 'mmstack'
        assert series.is_multifile
        # assert data
        data = tif.asarray()
        assert isinstance(data, numpy.ndarray)
        assert data.shape == (91, 2, 48, 2, 512, 512)
        assert data.dtype == numpy.uint16
        assert data[48, 1, 23, 1, 253, 257] == 1236
        # assert_aszarr_method(series, data)  # takes 2 minutes
        with series.aszarr() as store:
            zdata = zarr.open(store, mode='r')
            assert zdata[48, 1, 23, 1, 253, 257] == 1236
            del zdata
        # test OME; ImageJ can't handle multi-file or positions
        data1 = data[:, 0]
        data2 = imread(filename, is_mmstack=False)
        assert data1.shape == data2.shape
        assert_array_equal(data1[17], data2[17])
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE or SKIP_LARGE, reason=REASON)
def test_read_mmstack_mosaic(caplog):
    """Test read MicroManager 1.4 mosaic dataset."""
    filename = _file(
        'MMStack/mosaic/d220708_HybISS_AS_cycles1to5_NoBridgeProbes_dim3x3__3'
        '_MMStack_2-Pos_000_001.ome.tif'
    )
    with TiffFile(filename) as tif:
        assert 'coercing invalid ASCII to bytes' not in caplog.text
        assert tif.is_micromanager
        assert tif.is_mmstack
        assert tif.is_ome
        assert tif.is_imagej
        assert not tif.is_ndtiff
        assert tif.byteorder == '<'
        assert len(tif.pages) == 55
        assert len(tif.series) == 1
        # assert metadata
        meta = tif.micromanager_metadata
        assert meta is not None
        assert meta['MajorVersion'] == 0
        assert meta['Summary']['MicroManagerVersion'] == '1.4.24 20220315'
        assert meta['Summary']['Prefix'] == (
            'd220708_HybISS_AS_cycles1to5_NoBridgeProbes_dim3x3__3'
        )
        assert meta['IndexMap'].shape == (55, 5)
        assert 'Comments' in meta
        # assert series properties
        series = tif.series[0]
        assert len(series) == 495
        assert series.shape == (9, 11, 5, 1040, 1388)
        assert series.axes == 'RZCYX'
        assert series.kind == 'mmstack'
        assert series.is_multifile
        # assert data
        data = tif.asarray()
        assert isinstance(data, numpy.ndarray)
        assert data.shape == (9, 11, 5, 1040, 1388)
        assert data.dtype == numpy.uint16
        assert data[7, 9, 3, 753, 1257] == 90
        assert_aszarr_method(series, data)
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE or SKIP_LARGE, reason=REASON)
def test_read_mmstack_single():
    """Test read MicroManager single-file multi-region dataset."""
    filename = _file('MMStack/181003_multi_pos_time_course_1_MMStack.ome.tif')
    with TiffFile(filename) as tif:
        assert tif.is_micromanager
        assert tif.is_mmstack
        assert tif.is_ome
        assert tif.is_imagej
        assert not tif.is_ndtiff
        assert tif.byteorder == '<'
        assert len(tif.pages) == 20
        assert len(tif.series) == 1
        # assert metadata
        meta = tif.micromanager_metadata
        assert meta is not None
        assert meta['MajorVersion'] == 0
        assert meta['Summary']['MicroManagerVersion'] == '2.0.0-beta3 20160512'
        assert meta['Summary']['Prefix'] == '181003_multi_pos_time_course_1'
        assert meta['IndexMap'].shape == (20, 5)
        assert meta['Comments']['0_0_4_1'] == ''
        # assert series properties
        series = tif.series[0]
        assert len(series) == 20
        assert series.shape == (10, 2, 256, 256)
        assert series.axes == 'TRYX'
        assert series.kind == 'mmstack'
        assert not series.is_multifile
        # assert data
        data = tif.asarray()
        assert isinstance(data, numpy.ndarray)
        assert data.shape == (10, 2, 256, 256)
        assert data.dtype == numpy.uint16
        assert data[7, 1, 111, 222] == 6991
        assert_aszarr_method(series, data)
        # test OME; ImageJ can't handle positions
        assert_array_equal(data[:, 0], imread(filename, is_mmstack=False))
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE or SKIP_LARGE, reason=REASON)
def test_read_mmstack_missing(caplog):
    """Test read MicroManager missing files and pages in dataset."""
    filename = _file('MMStack/movie_9_MMStack.ome.tif')
    with TiffFile(filename) as tif:
        assert tif.is_micromanager
        assert tif.is_mmstack
        assert tif.is_ome
        assert tif.is_imagej
        assert not tif.is_ndtiff
        assert tif.byteorder == '<'
        assert len(tif.pages) == 126
        assert len(tif.series) == 1
        assert 'MMStack series is missing files' in caplog.text
        assert 'MMStack is missing 1 page' in caplog.text
        # assert metadata
        meta = tif.micromanager_metadata
        assert meta is not None
        assert meta['MajorVersion'] == 0
        assert meta['Summary']['MicroManagerVersion'] == '1.4.16 20140128'
        assert meta['Summary']['Prefix'] == 'movie_9'
        assert meta['IndexMap'].shape == (125, 5)
        assert meta['Comments'] == {'Summary': ''}
        assert meta['DisplaySettings'][0]['Name'] == 'Dual-GFP'
        # assert series properties
        series = tif.series[0]
        assert series[-1] is None  # missing page
        assert len(series) == 126
        assert series.shape == (63, 2, 264, 320)
        assert series.axes == 'TCYX'
        assert series.kind == 'mmstack'
        assert not series.is_multifile
        # assert data
        data = tif.asarray()
        assert isinstance(data, numpy.ndarray)
        assert data.shape == (63, 2, 264, 320)
        assert data.dtype == numpy.uint16
        assert data[59, 1, 151, 186] == 599
        # assert zarr
        if not SKIP_ZARR and zarr is not None:
            with series.aszarr(fillvalue=100) as store:
                assert asyncio.run(store.exists('1.1.0.0'))
                # missing page
                assert not asyncio.run(store.exists('62.1.0.0'))
                z = zarr.open(store, mode='r')
                assert z[62, 1, 0, 0] == 100
                assert_array_equal(data[:62], z[:62])
        # test OME and ImageJ
        assert_array_equal(data, imread(filename, is_mmstack=False))
        assert_array_equal(
            data, imread(filename, is_mmstack=False, is_ome=False)
        )
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE or SKIP_LARGE, reason=REASON)
def test_read_mmstack_bytesio(caplog):
    """Test read MicroManager missing data in BytesIO."""
    filename = _file('MMStack/movie_9_MMStack.ome.tif')
    with open(filename, 'rb') as fh:
        bytesio = BytesIO(fh.read())
    with TiffFile(bytesio) as tif:
        assert tif.is_micromanager
        assert tif.is_mmstack
        assert tif.is_ome
        assert tif.is_imagej
        assert not tif.is_ndtiff
        assert not tif.filehandle.is_file
        assert tif.byteorder == '<'
        assert len(tif.pages) == 126
        assert len(tif.series) == 1
        assert 'MMStack series is missing files' in caplog.text
        assert 'MMStack is missing 1 page' in caplog.text
        # assert metadata
        meta = tif.micromanager_metadata
        assert meta is not None
        assert meta['MajorVersion'] == 0
        assert meta['Summary']['MicroManagerVersion'] == '1.4.16 20140128'
        assert meta['Summary']['Prefix'] == 'movie_9'
        assert meta['IndexMap'].shape == (125, 5)
        assert meta['Comments'] == {'Summary': ''}
        assert meta['DisplaySettings'][0]['Name'] == 'Dual-GFP'
        # assert series properties
        series = tif.series[0]
        assert len(series) == 126
        assert series.shape == (63, 2, 264, 320)
        assert series.axes == 'TCYX'
        assert series.kind == 'mmstack'
        assert not series.is_multifile
        # assert data
        data = tif.asarray()
        assert isinstance(data, numpy.ndarray)
        assert data.shape == (63, 2, 264, 320)
        assert data.dtype == numpy.uint16
        assert data[59, 1, 151, 186] == 599
        assert_aszarr_method(series, data, mode='r')
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE or SKIP_LARGE, reason=REASON)
def test_read_mmstack_missing_sbs(caplog):
    """Test read MicroManager dataset with missing data."""
    # https://github.com/cgohlke/tifffile/issues/187
    filename = _file('MMStack/10X_c1-SBS-1_A1_Tile-102.sbs.tif')
    with TiffFile(filename) as tif:
        assert tif.is_micromanager
        assert tif.is_mmstack
        assert tif.is_ome
        assert tif.is_imagej
        assert not tif.is_ndtiff
        assert tif.byteorder == '<'
        assert len(tif.pages) == 5
        assert len(tif.series) == 1
        assert 'MMStack file name is invalid' in caplog.text
        assert 'MMStack series is missing files' in caplog.text
        # assert metadata
        meta = tif.micromanager_metadata
        assert meta is not None
        assert meta['MajorVersion'] == 0
        assert meta['Summary']['MicroManagerVersion'].startswith('1.4.23 2019')
        assert meta['Summary']['Prefix'] == '10X_c1-SBS-1_1'
        assert meta['IndexMap'].shape == (5, 5)
        assert meta['Comments']['Summary'] == ''
        assert meta['DisplaySettings'][0]['Name'] == 'DAPI_10p'
        # assert series properties
        series = tif.series[0]
        assert len(series) == 5
        assert series.shape == (5, 1024, 1024)
        assert series.axes == 'CYX'
        assert series.kind == 'mmstack'
        assert not series.is_multifile
        # assert data
        data = tif.asarray()
        assert isinstance(data, numpy.ndarray)
        assert data.shape == (5, 1024, 1024)
        assert data.dtype == numpy.uint16
        assert data[3, 151, 186] == 542
        assert_aszarr_method(series, data)
        # test OME
        assert_array_equal(data, imread(filename, is_mmstack=False))
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE or SKIP_LARGE, reason=REASON)
def test_read_mmstack_trzc():
    """Test read MicroManager 6 dimensional dataset."""
    filename = _file(
        'MMStack'
        '/image_stack_tpzc_50tp_2p_5z_3c_512k_1_MMStack_2-Pos000_000.ome.tif'
    )
    with TiffFile(filename) as tif:
        assert tif.is_micromanager
        assert tif.is_mmstack
        assert tif.is_ome
        assert tif.is_imagej
        assert not tif.is_ndtiff
        assert tif.byteorder == '<'
        assert len(tif.pages) == 750
        assert len(tif.series) == 1
        # assert metadata
        meta = tif.micromanager_metadata
        assert meta is not None
        assert meta['MajorVersion'] == 0
        assert meta['Summary']['MicroManagerVersion'].startswith('2.0.0-gamma')
        assert meta['Summary']['Prefix'] == (
            'image_stack_tpzc_50tp_2p_5z_3c_512k_1'
        )
        assert meta['IndexMap'].shape == (750, 5)
        assert meta['Comments']['Summary'] == ''
        assert 'DisplaySettings' not in meta
        # assert series properties
        series = tif.series[0]
        assert len(series) == 1500
        assert series.shape == (50, 2, 5, 3, 256, 256)
        assert series.axes == 'TRZCYX'
        assert series.kind == 'mmstack'
        assert series.is_multifile
        # assert data
        data = tif.asarray()
        assert isinstance(data, numpy.ndarray)
        assert data.shape == (50, 2, 5, 3, 256, 256)
        assert data.dtype == numpy.uint16
        assert data[27, 1, 3, 2, 151, 186] == 16
        assert_aszarr_method(series, data)
        # test OME
        assert_array_equal(
            data[:, 1], imread(filename, is_mmstack=False, series=1)
        )
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_read_ndtiff_magellanstack():
    """Test read NDTiffStorage/MagellanStack."""
    # https://github.com/cgohlke/tifffile/issues/23
    filename = _file(
        'NDTiffStorage/MagellanStack/Full resolution/democam_MagellanStack.tif'
    )
    with TiffFile(filename) as tif:
        assert tif.is_micromanager
        assert len(tif.pages) == 12
        # with pytest.warns(UserWarning):
        assert tif.micromanager_metadata is not None
        assert 'Comments' not in tif.micromanager_metadata
        meta = tif.pages[-1].tags['MicroManagerMetadata'].value
        assert meta['Axes']['repetition'] == 2
        assert meta['Axes']['exposure'] == 3
        # NDTiff v0 and v1 series are not supported
        series = tif.series[0]
        assert series.kind == 'uniform'  # not 'ndtiff'
        assert series.dtype == numpy.uint8
        assert series.shape == (12, 512, 512)
        assert series.axes == 'IYX'
        data = series.asarray()
        assert data[8, 100, 100] == 164
        assert_aszarr_method(tif, data)
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_read_ndtiff_v2():
    """Test read NDTiffStorage v2."""
    filename = _file(
        'NDTiffStorage/v2/ndtiffv2.0_test/Full resolution'
        '/ndtiffv2.0_test_NDTiffStack.tif'
    )
    with TiffFile(filename) as tif:
        assert tif.is_micromanager
        assert tif.is_ndtiff
        meta = tif.pages[-1].tags['MicroManagerMetadata'].value
        assert meta['Axes'] == {'channel': 1, 'time': 4}
        meta = tif.micromanager_metadata
        assert meta is not None
        assert meta['MajorVersion'] == 2
        assert meta['Summary']['PixelType'] == 'GRAY16'
        series = tif.series[0]
        assert series.kind == 'ndtiff'
        assert series.dtype == numpy.uint16
        assert series.shape == (5, 2, 32, 32)
        assert series.axes == 'TCYX'
        data = series.asarray()
        if not SKIP_NDTIFF:
            ndt = ndtiff.Dataset(os.path.join(os.path.dirname(filename), '..'))
            try:
                assert_array_equal(
                    data, ndt.as_array(axes=['time', 'channel'])
                )
            finally:
                ndt.close()

        assert_aszarr_method(tif, data)
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_read_ndtiff_tiled():
    """Test read NDTiffStorage v2 tiled."""
    filename = _file(
        'NDTiffStorage/v2/ndtiffv2.0_stitched_test/Full resolution'
        '/ndtiffv2.0_stitched_test_NDTiffStack.tif'
    )
    with TiffFile(filename) as tif:
        assert tif.is_micromanager
        assert tif.is_ndtiff
        meta = tif.pages[-1].tags['MicroManagerMetadata'].value
        assert meta['Axes'] == {'channel': 0, 'column': 0, 'row': 0}
        meta = tif.micromanager_metadata
        assert meta is not None
        assert meta['MajorVersion'] == 2
        assert meta['Summary']['PixelType'] == 'GRAY16'
        series = tif.series[0]
        assert series.kind == 'ndtiff'
        assert series.dtype == numpy.uint16
        assert series.shape == (2, 2, 32, 32)
        assert series.axes == 'JKYX'
        data = series.asarray()
        if not SKIP_NDTIFF:
            ndt = ndtiff.Dataset(os.path.join(os.path.dirname(filename), '..'))
            try:
                assert_array_equal(data, ndt.as_array(axes=['row', 'column']))
            finally:
                ndt.close()
        assert_aszarr_method(tif, data)
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_read_ndtiff_v3():
    """Test read NDTiffStorage v3."""
    filename = _file(
        'NDTiffStorage/v3/ndtiffv3.0_test/ndtiffv3.0_test_NDTiffStack.tif'
    )
    with TiffFile(filename) as tif:
        assert tif.is_micromanager
        assert tif.is_ndtiff
        meta = tif.pages[-1].tags['MicroManagerMetadata'].value
        assert meta['Axes'] == {'channel': 1, 'time': 4}
        meta = tif.micromanager_metadata
        assert meta is not None
        assert meta['MajorVersion'] == 3
        assert meta['MinorVersion'] == 0
        assert meta['Summary']['PixelType'] == 'GRAY16'
        series = tif.series[0]
        assert series.kind == 'ndtiff'
        assert series.dtype == numpy.uint16
        assert series.shape == (5, 2, 32, 32)
        assert series.axes == 'TCYX'
        data = series.asarray()
        if not SKIP_NDTIFF:
            ndt = ndtiff.Dataset(os.path.dirname(filename))
            try:
                assert_array_equal(
                    data, ndt.as_array(axes=['time', 'channel'])
                )
            finally:
                ndt.close()
        assert_aszarr_method(tif, data)
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_read_ndtiff_tcz():
    """Test read NDTiffStorage v3 with squeezed axes."""
    filename = _file(
        'NDTiffStorage/v3/mm_mda_tcz_15/mm_mda_tcz_15_NDTiffStack.tif'
    )
    with TiffFile(filename) as tif:
        assert tif.is_micromanager
        assert tif.is_ndtiff
        meta = tif.pages[-1].tags['MicroManagerMetadata'].value
        assert 'Axes' not in meta  # missing?
        # expected {'channel': 1, 'z': 6, 'position': 0, 'time': 7}
        meta = tif.micromanager_metadata
        assert meta is not None
        assert meta['MajorVersion'] == 3
        assert meta['MinorVersion'] == 0
        assert meta['Summary']['PixelType'] == 'GRAY16'
        series = tif.series[0]
        assert series.kind == 'ndtiff'
        assert series.dtype == numpy.uint16
        assert series.get_shape(squeeze=False) == (1, 8, 2, 7, 512, 512)
        assert series.get_axes(squeeze=False) == 'RTCZYX'
        data = series.asarray(squeeze=True)
        assert data.shape == (8, 2, 7, 512, 512)
        if not SKIP_NDTIFF:
            ndt = ndtiff.Dataset(os.path.dirname(filename))
            try:
                # axes=['position', 'time', 'channel', 'z']
                assert_array_equal(data, numpy.squeeze(ndt.as_array()))
            finally:
                ndt.close()
        assert_aszarr_method(tif, data)
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_read_ndtiff_bytesio(caplog):
    """Test read NDTiffStorage v3 with BytesIO."""
    filename = _file(
        'NDTiffStorage/v3/mm_mda_tcz_15/mm_mda_tcz_15_NDTiffStack.tif'
    )
    with open(filename, 'rb') as fh:
        bytesio = BytesIO(fh.read())
    with TiffFile(bytesio) as tif:
        assert tif.is_micromanager
        assert not tif.is_ndtiff
        meta = tif.pages[-1].tags['MicroManagerMetadata'].value
        assert 'Axes' not in meta  # missing?
        # expected {'channel': 1, 'z': 6, 'position': 0, 'time': 7}
        meta = tif.micromanager_metadata
        assert meta is not None
        assert meta['MajorVersion'] == 3
        assert meta['MinorVersion'] == 0
        assert meta['Summary']['PixelType'] == 'GRAY16'
        series = tif.series[0]
        assert 'NDTiff.index not found for' not in caplog.text
        assert series.kind == 'uniform'  # not ndtiff
        assert series.dtype == numpy.uint16
        assert series.get_shape(squeeze=False) == (112, 512, 512)
        assert series.get_axes(squeeze=False) == 'IYX'
        data = series.asarray(squeeze=True)
        assert data.shape == (112, 512, 512)
        assert_aszarr_method(tif, data, mode='r')
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_read_ndtiff_multichannel():
    """Test read NDTiffStorage v3 with channel names."""
    filename = _file(
        'NDTiffStorage/v3/ndtiff3.2_multichannel'
        '/NDTiff3.2_multichannel_NDTiffStack.tif'
    )
    with TiffFile(filename) as tif:
        assert tif.is_micromanager
        assert tif.is_ndtiff
        meta = tif.pages[-1].tags['MicroManagerMetadata'].value
        assert meta['Axes'] == {'channel': 'FITC', 'z': 15, 'time': 7}
        meta = tif.micromanager_metadata
        assert meta is not None
        assert meta['MajorVersion'] == 3
        assert meta['MinorVersion'] == 2
        assert meta['Summary']['PixelType'] == 'GRAY16'
        series = tif.series[0]
        assert series.kind == 'ndtiff'
        assert series.dtype == numpy.uint16
        assert series.shape == (8, 2, 16, 64, 64)
        assert series.axes == 'TCZYX'
        data = series.asarray()
        if not SKIP_NDTIFF:
            ndt = ndtiff.Dataset(os.path.dirname(filename))
            try:
                assert_array_equal(
                    data, ndt.as_array(axes=['time', 'channel', 'z'])
                )
            finally:
                ndt.close()
        assert_aszarr_method(tif, data)
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_read_ndtiff_unordered_axes(caplog):
    """Test read NDTiffStorage with unordered axes_dict keys."""
    # https://github.com/cgohlke/tifffile/issues/311
    filename = _file(
        'NDTiffStorage/v3/issue311/NDTiff3.2_multichannel_NDTiffStack.tif'
    )
    with TiffFile(filename) as tif:
        assert tif.is_micromanager
        assert tif.is_ndtiff
        meta = tif.pages[-1].tags['MicroManagerMetadata'].value
        assert meta['Axes'] == {'channel': 'FITC', 'z': 15, 'time': 7}
        meta = tif.micromanager_metadata
        assert meta is not None
        assert meta['MajorVersion'] == 3
        assert meta['MinorVersion'] == 2
        assert meta['Summary']['PixelType'] == 'GRAY16'
        series = tif.series[0]
        with caplog.at_level(logging.WARNING):
            assert 'NDTiff.index axes_dict.keys' in caplog.text
        assert series.kind == 'ndtiff'
        assert series.dtype == numpy.uint16
        assert series.shape == (8, 2, 16, 64, 64)
        assert series.axes == 'TCZYX'
        data = series.asarray()
        if not SKIP_NDTIFF:
            ndt = ndtiff.Dataset(os.path.dirname(filename))
            try:
                assert_array_equal(
                    data, ndt.as_array(axes=['time', 'channel', 'z'])
                )
            finally:
                ndt.close()
        assert_aszarr_method(tif, data)
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE or SKIP_ZARR, reason=REASON)
def test_read_zarr():
    """Test read TIFF with zarr."""
    filename = _file('imagecodecs/gray.u1.tif')
    with TiffFile(filename) as tif:
        image = tif.asarray()
        store = tif.aszarr()
    try:
        data = zarr.open(store, mode='r')
        assert_array_equal(image, data)
        del data
    finally:
        store.close()


@pytest.mark.skipif(SKIP_FILE or SKIP_ZARR, reason=REASON)
@pytest.mark.xfail(reason='LRUStoreCache not available in Zarr 3')
def test_read_zarr_lrucache():
    """Test read TIFF with zarr LRUStoreCache."""
    # fails with zarr 2.15/16
    # https://github.com/zarr-developers/zarr-python/issues/1497
    filename = _file('imagecodecs/gray.u1.tif')
    with TiffFile(filename) as tif:
        image = tif.asarray()
        store = tif.aszarr()
    try:
        cache = zarr.LRUStoreCache(store, max_size=2**10)
        data = zarr.open(cache, mode='r')
        assert_array_equal(image, data)
        del data
    finally:
        store.close()


@pytest.mark.skipif(SKIP_FILE or SKIP_ZARR, reason=REASON)
def test_read_zarr_multifile():
    """Test read multifile OME-TIFF with zarr."""
    filename = _file('OME/multifile/multifile-Z1.ome.tiff')
    with TiffFile(filename) as tif:
        image = tif.asarray()
        store = tif.aszarr()
    try:
        data = zarr.open(store, mode='r')
        assert_array_equal(image, data)
        del data
    finally:
        store.close()


@pytest.mark.skipif(SKIP_FILE or SKIP_ZARR, reason=REASON)
@pytest.mark.parametrize('multiscales', [None, False, True])
def test_read_zarr_multiscales(multiscales):
    """Test Zarr store multiscales parameter."""
    filename = _file('tifffile/multiscene_pyramidal.ome.tif')
    with TiffFile(filename) as tif:
        page = tif.pages[1]
        series = tif.series[0]
        assert series.kind == 'ome'
        image = page.asarray()
        with page.aszarr(multiscales=multiscales) as store:
            z = zarr.open(store, mode='r')
            if multiscales:
                assert isinstance(z, zarr.Group)
                assert_array_equal(z['0'][:], image)
            else:
                assert isinstance(z, zarr.Array)
                assert_array_equal(z[:], image)
            del z
        with series.aszarr(multiscales=multiscales) as store:
            z = zarr.open(store, mode='r')
            if multiscales or multiscales is None:
                assert isinstance(z, zarr.Group)
                assert_array_equal(z['0'][0, 0, 1], image)
                assert len(list(z.groups())) == 0
                assert len(list(z.arrays())) == len(series.levels)
                assert len(list(z.keys())) == len(series.levels)
                assert z.tree()
            else:
                assert isinstance(z, zarr.Array)
                assert_array_equal(z[0, 0, 1], image)
            del z


@pytest.mark.skipif(SKIP_FILE or SKIP_ZARR, reason=REASON)
def test_read_zarr_level():
    """Test Zarr store of level."""
    filename = _file('tifffile/multiscene_pyramidal.ome.tif')
    data = imread(filename, key=1, series=0, level=2)
    store = imread(filename, key=1, series=0, level=2, aszarr=True)
    z = zarr.open(store, mode='r')
    image = z[:]
    del z
    assert_array_equal(image, data)


@pytest.mark.skipif(SKIP_FILE or SKIP_CODECS, reason=REASON)
@pytest.mark.parametrize('superres', [0, 1, 3])
def test_read_eer(caplog, superres):
    """Test read EER superresolution."""
    # https://github.com/fei-company/EerReaderLib/issues/1
    # https://github.com/cgohlke/tifffile/issues/313
    # requires imagecodecs > 2025.8.2

    filename = _file(
        'EER/FoilHole_29355520_Data_29330528_29330530_20200329_234551'
        '_Fractions.mrc.eer'
    )
    size = 4096 * 2 ** min(superres, 2)

    with TiffFile(filename, superres=superres) as tif:
        assert not caplog.text  # no warning
        assert tif.is_bigtiff
        assert tif.is_eer
        assert tif.byteorder == '<'
        assert len(tif.pages) == 567
        assert len(tif.series) == 1

        # assert page properties
        page = tif.pages.first
        assert page.tags['ImageWidth'].value == 4096
        assert page.tags['ImageLength'].value == 4096
        assert not page.is_contiguous
        assert page.photometric == PHOTOMETRIC.MINISBLACK
        assert page.compression == 65001
        assert page.imagewidth == size
        assert page.imagelength == size
        assert page.bitspersample == 1
        assert page.samplesperpixel == 1
        assert page.shape == (size, size)
        assert page.dtype == '?'

        meta = tif.eer_metadata
        assert meta['meanDoseRate'] == 4.138042
        assert meta['meanDoseRate.unit'] == 'e/pixel/s'

        # assert data
        data = page.asarray()
        assert data.shape == (size, size)
        assert data.dtype == '?'
        assert data.sum(dtype=numpy.uint32) == 278343

        # assert series
        series = tif.series[0]
        assert series.shape == (567, size, size)
        assert series.dtype == numpy.dtype('bool')
        assert series.axes == 'IYX'
        assert series.kind == 'eer'
        assert series.name == 'frames'

        if superres == 0:
            assert data[791, 1112]
            assert not data[792, 1112]
            stack = series.asarray()
            assert stack.shape == (567, size, size)
            assert stack.dtype == numpy.dtype('bool')
            assert stack.sum(dtype=numpy.uint32) == 158281907
            del stack

        for frame in series.pages:
            assert isinstance(
                frame, TiffPage if frame.index == 0 else TiffFrame
            )
            assert frame.compression == 65001
            assert frame.shape == (size, size)

        assert_aszarr_method(page, data)
        assert__str__(tif)

        if not (SKIP_ZARR or SKIP_HTTP):
            try:
                from imagecodecs.numcodecs import register_codecs
            except ImportError:
                return
            register_codecs('imagecodecs_eer', verbose=False)
            filename = os.path.split(filename)[-1]
            url = URL + 'test/EER/'
            with TempFileName(
                filename + f'_{superres}', ext='.json'
            ) as jsonfile:
                with page.aszarr() as store:
                    store.write_fsspec(jsonfile, url)
                # if this fails add ".eer" as "image/tiff" to mime types
                assert_fsspec(URL + os.path.split(jsonfile)[-1], data)


@pytest.mark.skipif(SKIP_FILE or SKIP_CODECS, reason=REASON)
def test_read_eer_integrate(caplog):
    """Test read EER with integrated image."""
    filename = _file('EER/with_thumbnail.eer')
    with TiffFile(filename, superres=0) as tif:
        assert not caplog.text  # no warning
        assert tif.is_bigtiff
        assert tif.is_eer
        assert tif.byteorder == '<'
        assert len(tif.pages) == 484
        assert len(tif.series) == 2

        # assert page properties
        page = tif.pages.first
        assert page.is_contiguous
        assert page.photometric == PHOTOMETRIC.MINISBLACK
        assert page.compression == 1
        assert page.imagewidth == 4096
        assert page.imagelength == 4096
        assert page.bitspersample == 16
        assert page.samplesperpixel == 1
        assert page.shape == (4096, 4096)
        assert page.dtype == numpy.uint16

        page = tif.pages[1]
        assert isinstance(page, TiffPage)
        assert not page.is_contiguous
        assert page.photometric == PHOTOMETRIC.MINISBLACK
        assert page.compression == 65001
        assert page.imagewidth == 4096
        assert page.imagelength == 4096
        assert page.bitspersample == 1
        assert page.samplesperpixel == 1
        assert page.shape == (4096, 4096)
        assert page.dtype == '?'

        meta = tif.eer_metadata
        assert meta['numberOfFrames'] == 483
        assert meta['exposureTime'] == 2.0105

        # assert series
        series = tif.series[1]
        assert len(series) == 1
        assert series.axes == 'YX'
        assert series.kind == 'eer'
        assert series.name == 'integrated'
        final = series.asarray()
        assert final.shape == (4096, 4096)
        assert final.dtype == numpy.uint16
        assert final.sum(dtype=numpy.uint32) == 3841768991

        series = tif.series[0]
        assert len(series) == 483
        assert series.shape == (483, 4096, 4096)
        assert series.dtype == numpy.dtype('bool')
        assert series.axes == 'IYX'
        assert series.kind == 'eer'
        assert series.name == 'frames'
        assert series.pages[0].asarray().sum(dtype=numpy.uint32) == 226656


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_read_astrotiff(caplog):
    """Test read AstroTIFF with FITS metadata."""
    # https://astro-tiff.sourceforge.io/
    filename = _file('AstroTIFF/NGC2024_astro-tiff_sample_48bit.tif')
    with TiffFile(filename) as tif:
        assert not caplog.text  # no warning
        assert tif.is_astrotiff
        assert tif.byteorder == '<'
        assert len(tif.pages) == 1
        assert len(tif.series) == 1
        # assert page properties
        page = tif.pages.first
        assert not page.is_contiguous
        assert page.photometric == PHOTOMETRIC.RGB
        assert page.compression == COMPRESSION.ADOBE_DEFLATE
        assert page.imagewidth == 3040
        assert page.imagelength == 2016
        assert page.bitspersample == 16
        assert page.samplesperpixel == 3
        # assert data and metadata
        assert tuple(page.asarray()[545, 1540]) == (10401, 11804, 12058)
        meta = tif.astrotiff_metadata
        assert meta['SIMPLE']
        assert meta['APTDIA'] == 100.0
        assert meta['APTDIA:COMMENT'] == 'Aperture diameter of telescope in mm'
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_read_streak():
    """Test read Hamamatus Streak file."""
    filename = _file('HamamatsuStreak/hamamatsu_streak.tif')
    with TiffFile(filename) as tif:
        assert tif.is_streak
        assert tif.byteorder == '<'
        assert len(tif.pages) == 1
        assert len(tif.series) == 1
        # assert page properties
        page = tif.pages.first
        assert page.is_contiguous
        assert page.photometric == PHOTOMETRIC.MINISBLACK
        assert page.imagewidth == 672
        assert page.imagelength == 508
        assert page.bitspersample == 16
        assert page.samplesperpixel == 1
        # assert data and metadata
        assert page.asarray()[277, 341] == 47
        meta = tif.streak_metadata
        assert meta['Application']['SoftwareVersion'] == '9.5 pf4'
        assert meta['Acquisition']['areSource'] == (0, 0, 672, 508)
        assert meta['Camera']['Prop_InternalLineInterval'] == 9.74436e-06
        assert meta['Camera']['Prop_OutputTriggerPeriod_2'] == 0.000001
        assert meta['Camera']['HWidth'] == 672
        assert meta['DisplayLUT']['EntrySize'] == 4
        assert meta['Spectrograph']['Front Ent. Slitw.'] == 0
        assert meta['Scaling']['ScalingYScalingFile'] == 'Focus mode'
        xscale = meta['Scaling']['ScalingXScaling']
        assert xscale.size == 672
        assert xscale[0] == 231.09092712402344
        assert xscale[-1] == 242.59259033203125
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_read_agilent():
    """Test read Agilent Technologies file."""
    filename = _file('Agilent/SG11410002_253174651388_S001.tif')
    with TiffFile(filename) as tif:
        assert tif.is_agilent
        assert not tif.is_mdgel
        assert len(tif.pages) == 4
        assert len(tif.series) == 2
        # assert page properties
        page = tif.pages.first
        assert page.is_contiguous
        assert page.photometric == PHOTOMETRIC.MINISBLACK
        assert page.imagewidth == 20334
        assert page.imagelength == 7200
        assert page.bitspersample == 16
        assert page.samplesperpixel == 1
        assert page.tags[285].value == 'Red'
        assert page.tags[37702].value == 'Unknown'
        # assert data
        series = tif.series[0]
        assert series.shape == (2, 7200, 20334)
        assert series.asarray()[1, 277, 341] == 24
        series = tif.series[1]
        assert series.shape == (2, 2400, 6778)
        assert series.asarray()[1, 277, 341] == 634
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_read_nuvu():
    """Test read Nuvu camera file."""
    filename = _file('Nuvu/hluc-lumi-5s_0.tif')
    with TiffFile(filename) as tif:
        assert tif.is_nuvu
        assert tif.byteorder == '<'
        assert len(tif.pages) == 1
        assert len(tif.series) == 1
        # assert page properties
        page = tif.pages.first
        assert page.is_contiguous
        assert page.photometric == PHOTOMETRIC.MINISBLACK
        assert page.imagewidth == 512
        assert page.imagelength == 513
        assert page.bitspersample == 16
        assert page.samplesperpixel == 1
        assert page.dataoffsets == (8,)
        assert page.databytecounts == (525312,)
        # assert data and metadata
        assert page.asarray()[277, 341] == 1699
        meta = tif.nuvu_metadata
        assert len(meta) == 56
        assert meta['AMP_TYPE'] == 1
        assert meta['RETD_TOD'] == '2025-08-29 17:03:05.713'
        assert meta['DATE'] == '2025-08-29 17:03:05.723'
        assert meta['TEMP_CCD'] == -99.98
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE or SKIP_ZARR, reason=REASON)
@pytest.mark.parametrize('chunkmode', [0, 2])
def test_read_selection(chunkmode):
    """Test read selection via imread."""
    filename = _file('tifffile/multiscene_pyramidal.ome.tif')
    selection = (8, slice(16, 17), slice(None), slice(51, 99), slice(51, 99))
    # series 0
    assert_array_equal(
        imread(filename)[8, 16:17, :, 51:99, 51:99],
        imread(filename, selection=selection, chunkmode=chunkmode),
    )
    # level 1
    assert_array_equal(
        imread(filename, series=0, level=1)[8, 16:17, :, 51:99, 51:99],
        imread(
            filename,
            series=0,
            level=1,
            selection=selection,
            chunkmode=chunkmode,
        ),
    )
    # page 99
    assert_array_equal(
        imread(filename, key=99)[51:99, 51:99],
        imread(
            filename,
            key=99,
            selection=(slice(51, 99), slice(51, 99)),
            chunkmode=chunkmode,
        ),
    )
    # series 1
    assert_array_equal(
        imread(filename, series=1)[51:99, 51:99],
        imread(
            filename,
            series=1,
            selection=(slice(51, 99), slice(51, 99)),
            chunkmode=chunkmode,
        ),
    )


@pytest.mark.skipif(SKIP_FILE or SKIP_ZARR, reason=REASON)
@pytest.mark.parametrize('out', [None, 'empty', 'memmap', 'name'])
def test_read_selection_out(out):
    """Test read selection via imread into out."""
    # https://github.com/cgohlke/tifffile/pull/222
    filename = _file('tifffile/multiscene_pyramidal.ome.tif')
    selection = (8, slice(16, 17), slice(None), slice(51, 99), slice(51, 99))
    expected = imread(filename)[8, 16:17, :, 51:99, 51:99]
    if out is None:
        # new array
        image = imread(filename, selection=selection, out=None)
    elif out == 'empty':
        # existing array
        image = numpy.empty_like(expected)
        imread(filename, selection=selection, out=image)
    elif out == 'memmap':
        # memmap in temp dir
        image = imread(filename, selection=selection, out='memmap')
        assert isinstance(image, numpy.memmap)
    elif out == 'name':
        # memmap in specified file
        with TempFileName('read_selection_out', ext='.memmap') as fileout:
            image = imread(filename, selection=selection, out=fileout)
            assert isinstance(image, numpy.memmap)

    assert_array_equal(image, expected)
    del image  # close memmap


@pytest.mark.skipif(SKIP_FILE or SKIP_CODECS or SKIP_ZARR, reason=REASON)
def test_read_selection_filesequence():
    """Test read selection from file sequence via imread."""
    filename = _files('TiffSequence/*.tif')
    assert_array_equal(
        imread(filename)[5:8, 51:99, 51:99],
        imread(
            filename, selection=(slice(5, 8), slice(51, 99), slice(51, 99))
        ),
    )


def test_read_xarray_page_properties():
    """Test read TiffPage xarray properties."""
    dtype = numpy.uint8
    resolution = (1.1, 2.2)
    with TempFileName('read_xarray_page_properties') as filename:
        with TiffWriter(filename) as tif:
            # gray
            tif.write(
                shape=(33, 31),
                dtype=dtype,
                resolution=resolution,
                photometric='minisblack',
            )
            # RGB
            tif.write(
                shape=(33, 31, 3),
                dtype=dtype,
                resolution=resolution,
                photometric='rgb',
            )
            # RGBA
            tif.write(
                shape=(33, 31, 4),
                dtype=dtype,
                resolution=resolution,
                photometric='rgb',
            )
            # CMYK
            tif.write(
                shape=(33, 31, 4),
                dtype=dtype,
                resolution=resolution,
                photometric='separated',
            )
            # gray with extrasamples
            tif.write(
                shape=(33, 31, 5),
                dtype=dtype,
                resolution=resolution,
                photometric='minisblack',
                planarconfig='contig',
            )
            # RRGGBB
            tif.write(
                shape=(3, 33, 31),
                dtype=dtype,
                resolution=resolution,
                photometric='rgb',
                planarconfig='separate',
            )
            # depth
            tif.write(
                shape=(7, 33, 31),
                dtype=dtype,
                resolution=resolution,
                photometric='minisblack',
                volumetric=True,
            )

        xcoords = numpy.linspace(
            0, 31 / resolution[0], 31, endpoint=False, dtype=numpy.float32
        )
        ycoords = numpy.linspace(
            0, 33 / resolution[1], 33, endpoint=False, dtype=numpy.float32
        )
        # zcoords = numpy.linspace(
        #     0, 7 / 1, 7, endpoint=False, dtype=numpy.float32
        # )
        with TiffFile(filename) as tif:
            # gray
            page = tif.pages.first
            assert page.name == 'TiffPage 0'
            assert page.shape == (33, 31)
            assert page.ndim == 2
            assert page.axes == 'YX'
            assert page.dims == ('height', 'width')
            assert page.sizes == {'height': 33, 'width': 31}
            assert_array_equal(page.coords['height'], ycoords)
            assert_array_equal(page.coords['width'], xcoords)
            assert page.attr == {}

            # RGB
            page = tif.pages[1]
            assert page.name == 'TiffPage 1'
            assert page.shape == (33, 31, 3)
            assert page.ndim == 3
            assert page.axes == 'YXS'
            assert page.dims == ('height', 'width', 'sample')
            assert page.sizes == {'height': 33, 'width': 31, 'sample': 3}
            assert_array_equal(
                page.coords['sample'], numpy.array(['Red', 'Green', 'Blue'])
            )
            assert_array_equal(page.coords['height'], ycoords)
            assert_array_equal(page.coords['width'], xcoords)

            # RGBA
            page = tif.pages[2]
            assert page.name == 'TiffPage 2'
            assert page.shape == (33, 31, 4)
            assert page.ndim == 3
            assert page.axes == 'YXS'
            assert page.dims == ('height', 'width', 'sample')
            assert page.sizes == {'height': 33, 'width': 31, 'sample': 4}
            assert_array_equal(
                page.coords['sample'],
                numpy.array(['Red', 'Green', 'Blue', 'Unassalpha']),
            )
            assert_array_equal(page.coords['height'], ycoords)
            assert_array_equal(page.coords['width'], xcoords)

            # CMYK
            page = tif.pages[3]
            assert page.name == 'TiffPage 3'
            assert page.shape == (33, 31, 4)
            assert page.ndim == 3
            assert page.axes == 'YXS'
            assert page.dims == ('height', 'width', 'sample')
            assert page.sizes == {'height': 33, 'width': 31, 'sample': 4}
            assert_array_equal(
                page.coords['sample'],
                numpy.array(['Cyan', 'Magenta', 'Yellow', 'Black']),
            )
            assert_array_equal(page.coords['height'], ycoords)
            assert_array_equal(page.coords['width'], xcoords)

            # gray with extrasamples
            page = tif.pages[4]
            assert page.name == 'TiffPage 4'
            assert page.shape == (33, 31, 5)
            assert page.ndim == 3
            assert page.axes == 'YXS'
            assert page.dims == ('height', 'width', 'sample')
            assert page.sizes == {'height': 33, 'width': 31, 'sample': 5}
            assert_array_equal(
                page.coords['sample'],
                numpy.arange(5),
            )
            assert_array_equal(page.coords['height'], ycoords)
            assert_array_equal(page.coords['width'], xcoords)

            # RRGGBB
            page = tif.pages[5]
            assert page.name == 'TiffPage 5'
            assert page.shape == (3, 33, 31)
            assert page.ndim == 3
            assert page.axes == 'SYX'
            assert page.dims == ('sample', 'height', 'width')
            assert page.sizes == {'sample': 3, 'height': 33, 'width': 31}
            assert_array_equal(
                page.coords['sample'], numpy.array(['Red', 'Green', 'Blue'])
            )
            assert_array_equal(page.coords['height'], ycoords)
            assert_array_equal(page.coords['width'], xcoords)

            # depth
            page = tif.pages[6]
            assert page.name == 'TiffPage 6'
            assert page.shape == (7, 33, 31)
            assert page.ndim == 3
            assert page.axes == 'ZYX'
            assert page.dims == ('depth', 'height', 'width')
            assert page.sizes == {'depth': 7, 'height': 33, 'width': 31}
            assert_array_equal(page.coords['depth'], numpy.arange(7))
            assert_array_equal(page.coords['height'], ycoords)
            assert_array_equal(page.coords['width'], xcoords)


###############################################################################

# Test TiffWriter

WRITE_DATA = numpy.arange(
    3 * 219 * 301,
    dtype=numpy.uint16,
).reshape((3, 219, 301))


@pytest.mark.skipif(SKIP_EXTENDED, reason=REASON)
@pytest.mark.parametrize(
    'shape',
    [
        (219, 301),
        (219, 301, 2),
        (219, 301, 3),
        (219, 301, 4),
        (2, 219, 301),
        (3, 219, 301),
        (4, 219, 301),
        (5, 219, 301),
        (4, 3, 219, 301),
        (4, 219, 301, 3),
        (3, 4, 219, 301),
        (3, 4, 219, 301, 1),
    ],
)
@pytest.mark.parametrize(
    'compression', [None]  # , 'zlib', 'lzw', 'lzma', 'zstd', 'packbits']
)
@pytest.mark.parametrize('dtype', list('?bhiqefdBHIQFD'))
@pytest.mark.parametrize('byteorder', ['>', '<'])
@pytest.mark.parametrize('bigtiff', ['plaintiff', 'bigtiff'])
@pytest.mark.parametrize('tile', [None, (64, 64)])
@pytest.mark.parametrize('data', ['random', None])
def test_write(data, byteorder, bigtiff, compression, dtype, shape, tile):
    """Test TiffWriter with various options."""
    if compression is not None and (data is None or SKIP_CODECS):
        pytest.xfail(REASON)
    filename = 'write_{}_{}_{}_{}{}{}{}'.format(
        bigtiff,
        {'<': 'le', '>': 'be'}[byteorder],
        numpy.dtype(dtype).name,
        str(shape).replace(' ', ''),
        '_tiled' if tile is not None else '',
        '_empty' if data is None else '',
        f'_{compression}' if compression is not None else '',
    )
    bigtiff = bigtiff == 'bigtiff'
    if (3 in shape or 4 in shape) and shape[-1] != 1 and dtype != '?':
        photometric = 'rgb'
    else:
        photometric = None

    with TempFileName(filename) as filename:
        if data is None:
            with TiffWriter(
                filename, byteorder=byteorder, bigtiff=bigtiff
            ) as tif:
                if dtype == '?':
                    # cannot write non-contiguous empty file
                    with pytest.raises(ValueError):
                        tif.write(
                            shape=shape,
                            dtype=dtype,
                            tile=tile,
                            photometric=photometric,
                        )
                    return
                tif.write(
                    shape=shape,
                    dtype=dtype,
                    tile=tile,
                    photometric=photometric,
                )
                assert__repr__(tif)
            with TiffFile(filename) as tif:
                assert__str__(tif)
                image = tif.asarray()
        else:
            data = random_data(dtype, shape)
            imwrite(
                filename,
                data,
                byteorder=byteorder,
                bigtiff=bigtiff,
                tile=tile,
                photometric=photometric,
                compression=compression,
            )
            image = imread(filename)
            assert image.flags['C_CONTIGUOUS']
            assert_array_equal(data.squeeze(), image.squeeze())
            if not SKIP_ZARR:
                with imread(filename, aszarr=True) as store:
                    data = zarr.open(store, mode='r')
                    assert_array_equal(data, image)

        assert shape == image.shape
        assert dtype == image.dtype
        if not bigtiff:
            assert_valid_tiff(filename)


def test_write_deprecated_planarconfig():
    """Test deprecated planarconfig."""
    with TempFileName('write_deprecated_planarconfig_contig') as filename:
        with pytest.warns(DeprecationWarning):
            imwrite(filename, shape=(31, 33, 3), dtype=numpy.float32)
        with TiffFile(filename) as tif:
            page = tif.pages.first
            assert page.photometric == PHOTOMETRIC.RGB
            assert page.planarconfig == PLANARCONFIG.CONTIG

    with TempFileName('write_deprecated_planarconfig_separate') as filename:
        with pytest.warns(DeprecationWarning):
            imwrite(filename, shape=(3, 31, 33), dtype=numpy.float32)
        with TiffFile(filename) as tif:
            page = tif.pages.first
            assert page.photometric == PHOTOMETRIC.RGB
            assert page.planarconfig == PLANARCONFIG.SEPARATE


def test_write_ycbcr_subsampling(caplog):
    """Test write YCBCR."""
    with TempFileName('write_ycbcr_subsampling') as filename:
        imwrite(
            filename,
            shape=(31, 33, 3),
            dtype=numpy.uint8,
            photometric=PHOTOMETRIC.YCBCR,
            subsampling=(1, 2),
        )
        assert 'cannot apply subsampling' in caplog.text
        with TiffFile(filename) as tif:
            page = tif.pages.first
            assert page.photometric == PHOTOMETRIC.YCBCR
            assert page.subsampling == (1, 1)
            assert 'ReferenceBlackWhite' in page.tags


@pytest.mark.parametrize('samples', [0, 1, 2])
def test_write_invalid_samples(samples):
    """Test TiffWriter with invalid options."""
    data = numpy.zeros((16, 16, samples) if samples else (16, 16), numpy.uint8)
    filename = f'write_invalid_samples{samples}'
    with TempFileName(filename) as filename:  # noqa: SIM117
        with pytest.raises(ValueError):
            imwrite(filename, data, photometric='rgb')


@pytest.mark.skipif(SKIP_FILE or SKIP_CODECS, reason=REASON)
@pytest.mark.parametrize('tile', [False, True])
@pytest.mark.parametrize(
    'codec',
    [
        'adobe_deflate',
        'lzma',
        'lzw',
        'packbits',
        'zstd',
        'webp',
        'png',
        'jpeg',
        'jpegxl',
        'jpegxr',
        'jpeg2000',
    ],
)
@pytest.mark.parametrize('mode', ['gray', 'rgb', 'planar'])
def test_write_codecs(mode, tile, codec):
    """Test write various compression."""
    if mode in {'gray', 'planar'} and codec == 'webp':
        pytest.xfail("WebP doesn't support grayscale or planar mode")
    level = {'webp': -1, 'jpeg': 99}.get(codec)
    compressionargs = {'level': level} if level else None
    tile = (16, 16) if tile else None
    data = numpy.load(_file('tifffile/rgb.u1.npy'))
    if mode == 'rgb':
        photometric = PHOTOMETRIC.RGB
        planarconfig = PLANARCONFIG.CONTIG
    elif mode == 'planar':
        photometric = PHOTOMETRIC.RGB
        planarconfig = PLANARCONFIG.SEPARATE
        data = numpy.moveaxis(data, -1, 0).copy()
    else:
        planarconfig = None
        photometric = PHOTOMETRIC.MINISBLACK
        data = data[..., :1].copy()
    data = numpy.repeat(data[numpy.newaxis], 3, axis=0)
    data[1] = 255 - data[1]
    shape = data.shape
    with TempFileName(
        'write_codecs_{}_{}{}'.format(mode, codec, '_tile' if tile else '')
    ) as filename:
        imwrite(
            filename,
            data,
            compression=codec,
            compressionargs=compressionargs,
            tile=tile,
            photometric=photometric,
            planarconfig=planarconfig,
            subsampling=(1, 1),
        )
        assert_valid_tiff(filename)
        with TiffFile(filename) as tif:
            assert len(tif.pages) == shape[0]
            page = tif.pages.first
            assert not page.is_contiguous
            assert page.compression == enumarg(COMPRESSION, codec)
            assert page.photometric in {photometric, PHOTOMETRIC.YCBCR}
            if planarconfig is not None:
                assert page.planarconfig == planarconfig
            assert page.imagewidth == 31
            assert page.imagelength == 32
            assert page.samplesperpixel == 1 if mode == 'gray' else 3
            # samplesperpixel = page.samplesperpixel
            image = tif.asarray()
            if codec == 'jpeg':
                assert_allclose(data, image, atol=10)
            else:
                assert_array_equal(data, image)
            assert_decode_method(page)
            assert__str__(tif)
        if (
            imagecodecs.TIFF.available
            and codec not in {'png', 'jpegxr', 'jpeg2000', 'jpegxl'}
            and mode != 'planar'
        ):
            im = imagecodecs.imread(filename, index=None)
            # if codec == 'jpeg':
            #     # tiff_decode returns JPEG compressed TIFF as RGBA
            #     im = numpy.squeeze(im[..., :samplesperpixel])
            assert_array_equal(im, numpy.squeeze(image))


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
@pytest.mark.parametrize('mode', ['gray', 'rgb', 'planar'])
@pytest.mark.parametrize('tile', [False, True])
@pytest.mark.parametrize(
    'dtype', ['u1', 'u2', 'u4', 'i1', 'i2', 'i4', 'f2', 'f4', 'f8']
)
@pytest.mark.parametrize('byteorder', ['>', '<'])
def test_write_predictor(byteorder, dtype, tile, mode):
    """Test predictors."""
    if dtype[0] == 'f' and SKIP_CODECS:
        pytest.xfail('requires imagecodecs')
    tile = (32, 32) if tile else None
    f4 = imread(_file('tifffile/gray.f4.tif'))
    if mode == 'rgb':
        photometric = PHOTOMETRIC.RGB
        planarconfig = PLANARCONFIG.CONTIG
        data = numpy.empty((83, 111, 3), 'f4')
        data[..., 0] = f4
        data[..., 1] = f4[::-1]
        data[..., 2] = f4[::-1, ::-1]
    elif mode == 'planar':
        photometric = PHOTOMETRIC.RGB
        planarconfig = PLANARCONFIG.SEPARATE
        data = numpy.empty((3, 83, 111), 'f4')
        data[0] = f4
        data[1] = f4[::-1]
        data[2] = f4[::-1, ::-1]
    else:
        planarconfig = None
        photometric = PHOTOMETRIC.MINISBLACK
        data = f4

    if dtype[0] in 'if':
        data -= 0.5
    if dtype in 'u1i1':
        data *= 255
    elif dtype in 'i2u2':
        data *= 2**12
    elif dtype in 'i4u4':
        data *= 2**21
    else:
        data *= 3.145
    data = data.astype(byteorder + dtype)

    with TempFileName(
        'write_predictor_{}_{}_{}{}'.format(
            dtype,
            'be' if byteorder == '>' else 'le',
            mode,
            '_tile' if tile else '',
        )
    ) as filename:
        imwrite(
            filename,
            data,
            predictor=True,
            compression=COMPRESSION.ADOBE_DEFLATE,
            tile=tile,
            photometric=photometric,
            planarconfig=planarconfig,
            byteorder=byteorder,
        )
        assert_valid_tiff(filename)

        with TiffFile(filename) as tif:
            assert tif.tiff.byteorder == byteorder
            assert len(tif.pages) == 1
            page = tif.pages.first
            assert not page.is_contiguous
            assert page.compression == COMPRESSION.ADOBE_DEFLATE
            assert page.predictor == (3 if dtype[0] == 'f' else 2)
            assert page.photometric == photometric
            if planarconfig is not None:
                assert page.planarconfig == planarconfig
            assert page.imagewidth == 111
            assert page.imagelength == 83
            assert page.samplesperpixel == 1 if mode == 'gray' else 3
            # samplesperpixel = page.samplesperpixel
            image = tif.asarray()
            assert_array_equal(data, image)
            assert_decode_method(page)
            assert__str__(tif)

        if not SKIP_CODECS and imagecodecs.TIFF.available:
            im = imagecodecs.imread(filename, index=None)
            assert_array_equal(im, numpy.squeeze(image))


@pytest.mark.parametrize('bytecount', [16, 256])
@pytest.mark.parametrize('count', [1, 2, 4])
@pytest.mark.parametrize('compression', [0, 6])
@pytest.mark.parametrize('tiled', [0, 1])
@pytest.mark.parametrize('bigtiff', [0, 1])
def test_write_bytecount(bigtiff, tiled, compression, count, bytecount):
    """Test write bytecount formats."""
    if tiled:
        tag = 'TileByteCounts'
        rowsperstrip = None
        tile = (bytecount, bytecount)
        shape = {
            1: (bytecount, bytecount),
            2: (bytecount * 2, bytecount),
            4: (bytecount * 2, bytecount * 2),
        }[count]
        is_contiguous = count != 4 and compression == 0
    else:
        tag = 'StripByteCounts'
        tile = None
        rowsperstrip = bytecount
        shape = (bytecount * count, bytecount)
        is_contiguous = compression == 0
    data = random_data(numpy.uint8, shape)

    if count == 1:
        dtype = DATATYPE.LONG8 if bigtiff else DATATYPE.LONG
    elif bytecount == 256:
        dtype = DATATYPE.LONG
    else:
        dtype = DATATYPE.SHORT

    with TempFileName(
        f'write_bytecounts_{bigtiff}{tiled}{compression}{count}{bytecount}'
    ) as filename:
        imwrite(
            filename,
            data,
            bigtiff=bigtiff,
            tile=tile,
            compression=COMPRESSION.ADOBE_DEFLATE if compression else None,
            compressionargs={'level': compression} if compression else None,
            rowsperstrip=rowsperstrip,
        )
        if not bigtiff:
            assert_valid_tiff(filename)
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 1
            page = tif.pages.first
            assert page.tags[tag].count == count
            assert page.tags[tag].dtype == dtype
            assert page.is_contiguous == is_contiguous
            assert page.planarconfig == PLANARCONFIG.CONTIG
            assert page.photometric == PHOTOMETRIC.MINISBLACK
            assert page.imagewidth == shape[1]
            assert page.imagelength == shape[0]
            assert page.samplesperpixel == 1
            assert_array_equal(page.asarray(), data)
            assert_aszarr_method(page, data)
            assert__str__(tif)


@pytest.mark.skipif(SKIP_EXTENDED, reason=REASON)
@pytest.mark.parametrize('repeat', [1, 4])
@pytest.mark.parametrize('shape', [(1, 0), (0, 1), (3, 0, 2, 1)])
@pytest.mark.parametrize('data', ['random', 'empty'])
@pytest.mark.parametrize('shaped', [True, False])
def test_write_zeroshape(shaped, data, repeat, shape):
    """Test write arrays with zero shape."""
    dtype = numpy.uint8
    filename = 'write_shape_{}x{}{}{}'.format(
        repeat,
        str(shape).replace(' ', ''),
        '_shaped' if shaped else '',
        '_empty' if data == 'empty' else '',
    )
    metadata = {} if shaped else None

    with TempFileName(filename) as filename:
        if data == 'empty':
            with TiffWriter(filename) as tif:  # noqa: SIM117
                with pytest.warns(UserWarning):  # noqa: PT031
                    for _ in range(repeat):
                        tif.write(
                            shape=shape,
                            dtype=dtype,
                            contiguous=True,
                            metadata=metadata,
                        )
                    tif.write(numpy.zeros((16, 16), 'u2'), metadata=metadata)
            with TiffFile(filename) as tif:
                assert__str__(tif)
                image = zimage = tif.asarray()
                if not SKIP_ZARR:
                    zimage = zarr.open(tif.aszarr(), mode='r')
        else:
            data = random_data(dtype, shape)
            with TiffWriter(filename) as tif:  # noqa: SIM117
                with pytest.warns(UserWarning):  # noqa: PT031
                    for _ in range(repeat):
                        tif.write(data, contiguous=True, metadata=metadata)
                    tif.write(numpy.zeros((16, 16), 'u2'), metadata=metadata)
            with TiffFile(filename) as tif:
                assert__str__(tif)
                image = zimage = tif.asarray()
                if not SKIP_ZARR:
                    zimage = zarr.open(tif.aszarr(), mode='r')

            assert image.flags['C_CONTIGUOUS']
            if shaped:
                if repeat > 1:
                    for i in range(repeat):
                        assert_array_equal(image[i], data)
                        assert_array_equal(zimage[i], data)
                else:
                    assert_array_equal(image, data)
                    assert_array_equal(zimage, data)
            else:
                empty = numpy.empty((0, 0), dtype)
                if repeat > 1:
                    for i in range(repeat):
                        assert_array_equal(image[i], empty)
                        assert_array_equal(zimage[i], empty)
                else:
                    assert_array_equal(image.squeeze(), empty)
                    # assert_array_equal(zimage.squeeze(), empty)

        if repeat > 1:
            assert image.shape[0] == repeat
            assert zimage.shape[0] == repeat
        elif shaped:
            assert shape == image.shape
            assert shape == zimage.shape
        else:
            assert image.shape == (0, 0)
            assert zimage.shape == (0, 0)
        assert dtype == image.dtype
        assert dtype == zimage.dtype


@pytest.mark.parametrize('repeats', [1, 2])
@pytest.mark.parametrize('series', [1, 2])
@pytest.mark.parametrize('subifds', [0, 1, 2])
@pytest.mark.parametrize('compressed', [False, True])
@pytest.mark.parametrize('tiled', [False, True])
@pytest.mark.parametrize('ome', [False, True])
def test_write_subidfs(ome, tiled, compressed, series, repeats, subifds):
    """Test write SubIFDs."""
    # use BigTIFF to prevent Windows explorer from locking the file
    if repeats > 1 and (compressed or tiled or ome):
        pytest.xfail('contiguous not working with compression, tiles, ome')

    data = [
        (RNG.random((5, 64, 64)) * 1023).astype(numpy.uint16),
        (RNG.random((5, 32, 32)) * 1023).astype(numpy.uint16),
        (RNG.random((5, 16, 16)) * 1023).astype(numpy.uint16),
    ]

    kwargs = {
        'tile': (16, 16) if tiled else None,
        'compression': COMPRESSION.ADOBE_DEFLATE if compressed else None,
        'compressionargs': {'level': 6} if compressed else None,
    }

    with TempFileName(
        'write_subidfs_'
        f'{ome}-{tiled}-{compressed}-{subifds}-{series}-{repeats}'
    ) as filename:
        with TiffWriter(filename, ome=ome, bigtiff=True) as tif:
            for _ in range(series):
                for r in range(repeats):
                    kwargs['contiguous'] = r != 0
                    tif.write(data[0], subifds=subifds, **kwargs)
                for i in range(1, subifds + 1):
                    for r in range(repeats):
                        kwargs['contiguous'] = r != 0
                        tif.write(data[i], subfiletype=1, **kwargs)

        with TiffFile(filename) as tif:
            for i, page in enumerate(tif.pages):
                assert not page.is_subifd
                if i % (5 * repeats):
                    assert page.description == ''
                elif ome:
                    if i == 0:
                        assert page.is_ome
                    else:
                        assert page.description == ''
                else:
                    assert page.is_shaped

                assert_array_equal(page.asarray(), data[0][i % 5])
                assert_aszarr_method(page, data[0][i % 5])
                if subifds:
                    assert len(page.pages) == subifds
                    for j, subifd in enumerate(page.pages):
                        assert subifd.is_subifd
                        assert_array_equal(
                            subifd.asarray(), data[j + 1][i % 5]
                        )
                        assert_aszarr_method(subifd, data[j + 1][i % 5])
                else:
                    assert page.pages is None

            for i, page in enumerate(tif.pages[:-1]):
                assert page._nextifd() == tif.pages[i + 1].offset

                if subifds:
                    for j, subifd in enumerate(page.pages[:-1]):
                        assert subifd.is_subifd
                        assert subifd.subfiletype == FILETYPE.REDUCEDIMAGE
                        assert subifd._nextifd() == page.subifds[j + 1]
                    assert page.pages[-1]._nextifd() == 0
                else:
                    assert page.pages is None

            assert len(tif.series) == series

            if repeats > 1:
                for s in range(series):
                    assert tif.series[s].kind == 'ome' if ome else 'shaped'
                    assert_array_equal(tif.series[s].asarray()[0], data[0])
                    for i in range(subifds):
                        assert_array_equal(
                            tif.series[s].levels[i + 1].asarray()[0],
                            data[i + 1],
                        )
            else:
                for s in range(series):
                    assert tif.series[s].kind == 'ome' if ome else 'shaped'
                    assert_array_equal(tif.series[s].asarray(), data[0])
                    for i in range(subifds):
                        assert_array_equal(
                            tif.series[s].levels[i + 1].asarray(), data[i + 1]
                        )


def test_write_lists():
    """Test write lists."""
    array = numpy.arange(1000).reshape((10, 10, 10)).astype(numpy.uint16)
    data = array.tolist()
    with TempFileName('write_lists') as filename:
        with TiffWriter(filename) as tif:
            tif.write(data, dtype=numpy.uint16)
            tif.write(data, compression=COMPRESSION.ADOBE_DEFLATE)
            tif.write([100.0])
            with pytest.warns(UserWarning):
                tif.write([])
        with TiffFile(filename) as tif:
            assert_array_equal(tif.series[0].asarray(), array)
            assert_array_equal(tif.series[1].asarray(), array)
            assert_array_equal(tif.series[2].asarray(), [100.0])
            assert_array_equal(tif.series[3].asarray(), [])
            assert_aszarr_method(tif.series[0], array)
            assert_aszarr_method(tif.series[1], array)
            assert_aszarr_method(tif.series[2], [100.0])
            # assert_aszarr_method(tif.series[3], [])


def test_write_nopages():
    """Test write TIFF with no pages."""
    with TempFileName('write_nopages') as filename:
        with TiffWriter(filename) as tif:
            pass
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 0
            tif.asarray()
        if not SKIP_VALIDATE:
            with pytest.raises(ValueError):
                assert_valid_tiff(filename)


def test_write_append_not_exists():
    """Test append to non existing file."""
    with TempFileName(  # noqa: SIM117
        'write_append_not_exists.bin'
    ) as filename:
        # with self.assertRaises(ValueError):
        with TiffWriter(filename, append=True):
            pass


def test_write_append_nontif():
    """Test fail to append to non-TIFF file."""
    with TempFileName('write_append_nontif.bin') as filename:
        with open(filename, 'wb') as fh:
            fh.write(b'not a TIFF file')
        with pytest.raises(TiffFileError):  # noqa: SIM117
            with TiffWriter(filename, append=True):
                pass


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_write_append_lsm():
    """Test fail to append to LSM file."""
    filename = _file('lsm/take1.lsm')
    with pytest.raises(ValueError):  # noqa: SIM117
        with TiffWriter(filename, append=True):
            pass


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_write_append_create():
    """Test append to non-existing file."""
    with TempFileName('write_append_create') as filename:
        if os.path.exists(filename):
            os.remove(filename)
        with TiffWriter(filename, append=True) as tif:
            tif.write(shape=(31, 33), dtype=numpy.uint8)
        data = imread(filename)
    assert data.shape == (31, 33)


def test_write_append_imwrite():
    """Test append using imwrite."""
    data = random_data(numpy.uint8, (21, 31))
    with TempFileName('write_imwrite_append') as filename:
        imwrite(filename, data, metadata=None)
        for _ in range(3):
            imwrite(filename, data, append=True, metadata=None)
        a = imread(filename)
        assert a.shape == (4, 21, 31)
        assert_array_equal(a[3], data)


def test_write_append():
    """Test append to existing TIFF file."""
    data = random_data(numpy.uint8, (21, 31))
    with TempFileName('write_append') as filename:
        with TiffWriter(filename) as tif:
            pass
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 0
            assert__str__(tif)

        with TiffWriter(filename, append=True) as tif:
            tif.write(data)
        with TiffFile(filename) as tif:
            assert len(tif.series) == 1
            assert len(tif.pages) == 1
            page = tif.pages.first
            assert page.imagewidth == 31
            assert page.imagelength == 21
            assert__str__(tif)

        with TiffWriter(filename, append=True) as tif:
            tif.write(data)
            tif.write(data, contiguous=True)
        with TiffFile(filename) as tif:
            assert len(tif.series) == 2
            assert len(tif.pages) == 3
            page = tif.pages.first
            assert page.imagewidth == 31
            assert page.imagelength == 21
            assert_array_equal(tif.asarray(series=1)[1], data)
            assert__str__(tif)

        assert_valid_tiff(filename)


def test_write_append_bytesio():
    """Test append to existing TIFF file in BytesIO."""
    data = random_data(numpy.uint8, (21, 31))
    offset = 11
    file = BytesIO()
    file.write(b'a' * offset)

    with TiffWriter(file) as tif:
        pass
    file.seek(offset)
    with TiffFile(file) as tif:
        assert len(tif.pages) == 0

    file.seek(offset)
    with TiffWriter(file, append=True) as tif:
        tif.write(data)
    file.seek(offset)
    with TiffFile(file) as tif:
        assert len(tif.series) == 1
        assert len(tif.pages) == 1
        page = tif.pages.first
        assert page.imagewidth == 31
        assert page.imagelength == 21
        assert__str__(tif)

    file.seek(offset)
    with TiffWriter(file, append=True) as tif:
        tif.write(data)
        tif.write(data, contiguous=True)
    file.seek(offset)
    with TiffFile(file) as tif:
        assert len(tif.series) == 2
        assert len(tif.pages) == 3
        page = tif.pages.first
        assert page.imagewidth == 31
        assert page.imagelength == 21
        assert_array_equal(tif.asarray(series=1)[1], data)
        assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE or SKIP_CODECS, reason=REASON)
def test_write_roundtrip_filename():
    """Test write and read using file name."""
    data = imread(_file('tifffile/generic_series.tif'))
    with TempFileName('write_roundtrip_filename') as filename:
        imwrite(filename, data, photometric=PHOTOMETRIC.RGB)
        assert_array_equal(imread(filename), data)


@pytest.mark.skipif(SKIP_FILE or SKIP_CODECS, reason=REASON)
def test_write_roundtrip_openfile():
    """Test write and read using open file."""
    pad = b'0' * 7
    data = imread(_file('tifffile/generic_series.tif'))
    with TempFileName('write_roundtrip_openfile') as filename:
        with open(filename, 'wb') as fh:
            fh.write(pad)
            imwrite(fh, data, photometric=PHOTOMETRIC.RGB)
            fh.write(pad)
        with open(filename, 'rb') as fh:
            fh.seek(len(pad))
            assert_array_equal(imread(fh), data)


@pytest.mark.skipif(SKIP_FILE or SKIP_CODECS, reason=REASON)
def test_write_roundtrip_bytesio():
    """Test write and read using BytesIO."""
    pad = b'0' * 7
    data = imread(_file('tifffile/generic_series.tif'))
    buf = BytesIO()
    buf.write(pad)
    imwrite(buf, data, photometric=PHOTOMETRIC.RGB)
    buf.write(pad)
    buf.seek(len(pad))
    assert_array_equal(imread(buf), data)


def test_write_pages():
    """Test write tags for contiguous data in all pages."""
    data = random_data(numpy.float32, (17, 219, 301))
    with TempFileName('write_pages') as filename:
        imwrite(filename, data, photometric=PHOTOMETRIC.MINISBLACK)
        assert_valid_tiff(filename)
        # assert file
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 17
            for i, page in enumerate(tif.pages):
                assert page.is_contiguous
                assert page.planarconfig == PLANARCONFIG.CONTIG
                assert page.photometric == PHOTOMETRIC.MINISBLACK
                assert page.imagewidth == 301
                assert page.imagelength == 219
                assert page.samplesperpixel == 1
                image = page.asarray()
                assert_array_equal(data[i], image)
            # assert series
            series = tif.series[0]
            assert series.kind == 'shaped'
            assert series.dataoffset is not None
            image = series.asarray()
            assert_array_equal(data, image)
            assert__str__(tif)


def test_write_truncate():
    """Test only one page is written for truncated files."""
    shape = (4, 5, 6, 1)
    with TempFileName('write_truncate') as filename:
        imwrite(filename, random_data(numpy.uint8, shape), truncate=True)
        assert_valid_tiff(filename)
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 1  # not 4
            page = tif.pages.first
            assert page.is_shaped
            assert page.shape == (5, 6)
            assert '"shape": [4, 5, 6, 1]' in page.description
            assert '"truncated": true' in page.description
            series = tif.series[0]
            assert series.is_truncated
            assert series.kind == 'shaped'
            assert series.shape == shape
            assert len(series._pages) == 1
            assert len(series.pages) == 1
            data = tif.asarray()
            assert data.shape == shape
            assert_aszarr_method(tif, data)
            assert_aszarr_method(tif, data, chunkmode='page')
            assert__str__(tif)


def test_write_is_shaped():
    """Test files are written with shape."""
    with TempFileName('write_is_shaped') as filename:
        imwrite(
            filename,
            random_data(numpy.uint8, (4, 5, 6, 3)),
            photometric=PHOTOMETRIC.RGB,
        )
        assert_valid_tiff(filename)
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 4
            page = tif.pages.first
            assert page.is_shaped
            assert page.description == '{"shape": [4, 5, 6, 3]}'
            assert__str__(tif)
    with TempFileName('write_is_shaped_with_description') as filename:
        descr = 'test is_shaped_with_description'
        imwrite(
            filename,
            random_data(numpy.uint8, (5, 6, 3)),
            photometric=PHOTOMETRIC.RGB,
            description=descr,
        )
        assert_valid_tiff(filename)
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 1
            page = tif.pages.first
            assert page.is_shaped
            assert page.description == descr
            assert_aszarr_method(page)
            assert_aszarr_method(page, chunkmode='page')
            assert__str__(tif)


def test_write_bytes_str():
    """Test write bytes in place of 7-bit ascii string."""
    micron = b'micron \xb5'  # can't be encoded as 7-bit ascii
    data = numpy.arange(4, dtype=numpy.uint32).reshape((2, 2))
    with TempFileName('write_bytes_str') as filename:
        imwrite(
            filename,
            data,
            description=micron,
            software=micron,
            extratags=[(50001, 's', 8, micron, True)],
        )
        with TiffFile(filename) as tif:
            page = tif.pages.first
            assert page.description == 'micron \xb5'
            assert page.software == 'micron \xb5'
            assert page.tags[50001].value == 'micron \xb5'


def test_write_extratags():
    """Test write extratags."""
    data = random_data(numpy.uint8, (2, 219, 301))
    description = 'Created by TestTiffWriter\nLorem ipsum dolor...'
    pagename = 'Page name'
    extratags = [
        ('ImageDescription', 's', 0, description, True),
        ('PageName', 's', 0, pagename, False),
        (50001, 'b', 1, b'1', True),
        (50002, 'b', 2, b'12', True),
        (50004, 'b', 4, b'1234', True),
        (50008, 'B', 8, b'12345678', True),
    ]
    with TempFileName('write_extratags') as filename:
        imwrite(filename, data, extratags=extratags)
        assert_valid_tiff(filename)
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 2
            assert tif.pages.first.description1 == description
            assert 'ImageDescription' not in tif.pages[1].tags
            assert tif.pages.first.tags['PageName'].value == pagename
            assert tif.pages[1].tags['PageName'].value == pagename
            assert '50001' not in tif.pages[1].tags
            tags = tif.pages.first.tags
            assert tags['50001'].value == 49
            assert tags['50002'].value == (49, 50)
            assert tags['50004'].value == (49, 50, 51, 52)
            assert_array_equal(tags['50008'].value, b'12345678')
            #                   (49, 50, 51, 52, 53, 54, 55, 56))
            assert__str__(tif)


def test_write_double_tags():
    """Test write single and sequences of doubles."""
    # older versions of tifffile do not use offset to write doubles
    # reported by Eric Prestat on Feb 21, 2016
    data = random_data(numpy.uint8, (8, 8))
    value = math.pi
    extratags = [
        (34563, 'd', 1, value, False),
        (34564, 'd', 1, (value,), False),
        (34565, 'd', 2, (value, value), False),
        (34566, 'd', 2, [value, value], False),
        (34567, 'd', 2, numpy.array((value, value)), False),
    ]
    with TempFileName('write_double_tags') as filename:
        imwrite(filename, data, extratags=extratags)
        assert_valid_tiff(filename)
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 1
            tags = tif.pages.first.tags
            assert tags['34563'].value == value
            assert tags['34564'].value == value
            assert tuple(tags['34565'].value) == (value, value)
            assert tuple(tags['34566'].value) == (value, value)
            assert tuple(tags['34567'].value) == (value, value)
            assert__str__(tif)

    with TempFileName('write_double_tags_bigtiff') as filename:
        imwrite(filename, data, bigtiff=True, extratags=extratags)
        # assert_jhove(filename)
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 1
            tags = tif.pages.first.tags
            assert tags['34563'].value == value
            assert tags['34564'].value == value
            assert tuple(tags['34565'].value) == (value, value)
            assert tuple(tags['34566'].value) == (value, value)
            assert tuple(tags['34567'].value) == (value, value)
            assert__str__(tif)


def test_write_short_tags():
    """Test write single and sequences of words."""
    data = random_data(numpy.uint8, (8, 8))
    value = 65531
    extratags = [
        (34564, 'H', 1, (value,) * 1, False),
        (34565, 'H', 2, (value,) * 2, False),
        (34566, 'H', 3, (value,) * 3, False),
        (34567, 'H', 4, (value,) * 4, False),
    ]
    with TempFileName('write_short_tags') as filename:
        imwrite(filename, data, extratags=extratags)
        assert_valid_tiff(filename)
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 1
            tags = tif.pages.first.tags
            assert tags['34564'].value == value
            assert tuple(tags['34565'].value) == (value,) * 2
            assert tuple(tags['34566'].value) == (value,) * 3
            assert tuple(tags['34567'].value) == (value,) * 4
            assert__str__(tif)


@pytest.mark.parametrize('subfiletype', [0b1, 0b10, 0b100, 0b1000, 0b1111])
def test_write_subfiletype(subfiletype):
    """Test write subfiletype."""
    data = random_data(numpy.uint8, (16, 16))
    if subfiletype & 0b100:
        data = data.astype('bool')
    with TempFileName(f'write_subfiletype_{subfiletype}') as filename:
        imwrite(filename, data, subfiletype=subfiletype)
        assert_valid_tiff(filename)
        with TiffFile(filename) as tif:
            page = tif.pages.first
            assert page.subfiletype == subfiletype
            assert page.is_reduced == bool(subfiletype & 0b1)
            assert page.is_multipage == bool(subfiletype & 0b10)
            assert page.is_mask == bool(subfiletype & 0b100)
            assert page.is_mrc == bool(subfiletype & 0b1000)
            assert_array_equal(data, page.asarray())
            assert__str__(tif)


@pytest.mark.parametrize('dt', [None, True, datetime, '2019:01:30 04:05:37'])
def test_write_datetime_tag(dt):
    """Test write datetime tag."""
    arg = dt
    if dt is datetime:
        arg = datetime.datetime.now().replace(microsecond=0)
    data = random_data(numpy.uint8, (31, 32))
    with TempFileName('write_datetime') as filename:
        imwrite(filename, data, datetime=arg)
        with TiffFile(filename) as tif:
            page = tif.pages.first
            if dt is None:
                assert 'DateTime' not in page.tags
                assert page.datetime is None
            elif dt is True:
                dt = datetime.datetime.now().strftime('%Y:%m:%d %H:')
                assert page.tags['DateTime'].value.startswith(dt)
            elif dt is datetime:
                assert page.tags['DateTime'].value == arg.strftime(
                    '%Y:%m:%d %H:%M:%S'
                )
                assert page.datetime == arg
            else:
                assert page.tags['DateTime'].value == dt
                assert page.datetime == datetime.datetime.strptime(
                    dt, '%Y:%m:%d %H:%M:%S'
                )
            assert__str__(tif)


def test_write_software_tag():
    """Test write Software tag."""
    data = random_data(numpy.uint8, (2, 219, 301))
    software = 'test_tifffile.py'
    with TempFileName('write_software_tag') as filename:
        imwrite(filename, data, software=software)
        assert_valid_tiff(filename)
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 2
            assert tif.pages.first.software == software
            assert 'Software' not in tif.pages[1].tags
            assert__str__(tif)


def test_write_description_tag():
    """Test write two description tags."""
    data = random_data(numpy.uint8, (2, 219, 301))
    description = 'Created by TestTiffWriter\nLorem ipsum dolor...'
    with TempFileName('write_description_tag') as filename:
        imwrite(filename, data, description=description)
        assert_valid_tiff(filename)
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 2
            assert tif.pages.first.description == description
            assert tif.pages.first.description1 == '{"shape": [2, 219, 301]}'
            assert 'ImageDescription' not in tif.pages[1].tags
            assert__str__(tif)


def test_write_description_tag_nometadata():
    """Test no JSON description is written with metadata=None."""
    data = random_data(numpy.uint8, (2, 219, 301))
    description = 'Created by TestTiffWriter\nLorem ipsum dolor...'
    with TempFileName('write_description_tag_nometadata') as filename:
        imwrite(filename, data, description=description, metadata=None)
        assert_valid_tiff(filename)
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 2
            assert tif.pages.first.description == description
            assert 'ImageDescription' not in tif.pages[1].tags
            assert (
                tif.pages.first.tags.get('ImageDescription', index=1) is None
            )
            assert tif.series[0].kind == 'generic'
            assert__str__(tif)


def test_write_description_tag_notshaped():
    """Test no JSON description is written with shaped=False."""
    data = random_data(numpy.uint8, (2, 219, 301))
    description = 'Created by TestTiffWriter\nLorem ipsum dolor...'
    with TempFileName('write_description_tag_notshaped') as filename:
        imwrite(filename, data, description=description, shaped=False)
        assert_valid_tiff(filename)
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 2
            assert tif.pages.first.description == description
            assert 'ImageDescription' not in tif.pages[1].tags
            assert (
                tif.pages.first.tags.get('ImageDescription', index=1) is None
            )
            assert tif.series[0].kind == 'generic'
            assert__str__(tif)


def test_write_description_ome():
    """Test write multiple imagedescription tags to OME."""
    # https://forum.image.sc/t/79471
    with TempFileName('write_description_ome') as filename:
        with pytest.warns(UserWarning):
            imwrite(
                filename,
                shape=(2, 32, 32),
                dtype=numpy.uint8,
                ome=True,
                description='description',  # not written
                extratags=[('ImageDescription', 2, None, 'extratags', False)],
            )
        with TiffFile(filename) as tif:
            assert tif.is_ome
            page = tif.pages.first
            assert page.description.startswith('<?xml')
            assert page.description1 == 'extratags'
            assert tif.pages[1].description == 'extratags'


def test_write_description_imagej():
    """Test write multiple imagedescription tags."""
    with TempFileName('write_description_imagej') as filename:
        with pytest.warns(UserWarning):
            imwrite(
                filename,
                shape=(2, 32, 32),
                dtype=numpy.uint8,
                imagej=True,
                description='description',  # not written
                extratags=[('ImageDescription', 2, None, 'extratags', False)],
            )
        with TiffFile(filename) as tif:
            assert tif.is_imagej
            page = tif.pages.first
            assert page.description.startswith('ImageJ=')
            assert page.description1 == 'extratags'
            assert tif.pages[1].description == 'extratags'


def test_write_description_shaped():
    """Test write multiple imagedescription tags to shaped."""
    with TempFileName('write_description_shaped') as filename:
        imwrite(
            filename,
            shape=(2, 32, 32),
            dtype=numpy.uint8,
            description='description',  # written
            extratags=[('ImageDescription', 2, None, 'extratags', False)],
        )
        with TiffFile(filename) as tif:
            assert tif.is_shaped
            page = tif.pages.first
            assert page.description == 'description'
            assert page.description1.startswith('{')
            assert page.tags.getall(270)[2].value == 'extratags'
            assert tif.pages[1].description == 'extratags'


def test_write_description_overwrite():
    """Test overwrite imagedescription tag."""
    with TempFileName('write_description_overwrite') as filename:
        with TiffWriter(filename) as tif:
            tif.write(
                shape=(2, 32, 32),
                dtype=numpy.uint8,
                description='description',  # overwritten
                extratags=[('ImageDescription', 2, None, 'extratags', False)],
                metadata=None,
            )
            tif.overwrite_description('overwritten description')
        with TiffFile(filename) as tif:
            assert not tif.is_shaped
            page = tif.pages.first
            assert page.description == 'overwritten description'
            assert page.description1 == 'extratags'
            assert tif.pages[1].description == 'extratags'


def test_write_description_extratags():
    """Test write imagedescription tags using extratag."""
    with TempFileName('write_description_extratags') as filename:
        with TiffWriter(filename) as tif:
            tif.write(
                shape=(2, 32, 32),
                dtype=numpy.uint8,
                extratags=[
                    (270, 2, None, 'description 0', False),
                    ('ImageDescription', 2, None, 'description 1', False),
                ],
                metadata=None,
            )
        with TiffFile(filename) as tif:
            assert not tif.is_shaped
            page = tif.pages.first
            assert page.description == 'description 0'
            assert page.description1 == 'description 1'
            page = tif.pages[1]
            assert page.description == 'description 0'
            assert page.description1 == 'description 1'


def test_write_resolution_float():
    """Test write float Resolution tag."""
    data = random_data(numpy.uint8, (2, 219, 301))
    resolution = (92.0, 92.0)
    with TempFileName('write_resolution_float') as filename:
        imwrite(filename, data, resolution=resolution)
        assert_valid_tiff(filename)
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 2
            assert tif.pages.first.tags['XResolution'].value == (92, 1)
            assert tif.pages.first.tags['YResolution'].value == (92, 1)
            assert tif.pages.first.tags['ResolutionUnit'].value == 2
            assert tif.pages[1].tags['XResolution'].value == (92, 1)
            assert tif.pages[1].tags['YResolution'].value == (92, 1)
            assert tif.pages.first.tags['ResolutionUnit'].value == 2
            assert__str__(tif)


def test_write_resolution_rational():
    """Test write rational Resolution tag."""
    data = random_data(numpy.uint8, (1, 219, 301))
    resolution = ((300, 1), (300, 1))
    with TempFileName('write_resolution_rational') as filename:
        imwrite(filename, data, resolution=resolution, resolutionunit=1)
        assert_valid_tiff(filename)
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 1
            assert tif.pages.first.tags['XResolution'].value == (300, 1)
            assert tif.pages.first.tags['YResolution'].value == (300, 1)
            assert tif.pages.first.tags['ResolutionUnit'].value == 1


def test_write_resolution_unit():
    """Test write Resolution tag unit."""
    data = random_data(numpy.uint8, (219, 301))
    resolution = (92.0, (9200, 100), 3)
    with TempFileName('write_resolution_unit') as filename:
        # this silently passes in tifffile 2026
        # with pytest.raises(ValueError):
        #     imwrite(filename, data, resolution=resolution)
        imwrite(
            filename,
            data,
            resolution=resolution[:2],
            resolutionunit=resolution[2],
        )
        assert_valid_tiff(filename)
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 1
            assert tif.pages.first.tags['XResolution'].value == (92, 1)
            assert tif.pages.first.tags['YResolution'].value == (92, 1)
            assert tif.pages.first.tags['ResolutionUnit'].value == 3
            assert__str__(tif)


@pytest.mark.skipif(SKIP_CODECS, reason=REASON)
@pytest.mark.parametrize('bps', [1, 2, 7, 8])
@pytest.mark.parametrize('dtype', [numpy.uint8, numpy.uint16, numpy.uint32])
def test_write_bitspersample(bps, dtype):
    """Test write with packints."""
    dtype = numpy.dtype(dtype)
    bps += (dtype.itemsize // 2) * 8
    data = numpy.arange(256 * 256 * 3, dtype=dtype).reshape((256, 256, 3))
    with TempFileName(f'write_bitspersample_{dtype.char}{bps}') as filename:
        # TODO: enable all cases once imagecodecs.packints_encode works
        if bps == dtype.itemsize * 8:
            imwrite(
                filename, data, bitspersample=bps, photometric=PHOTOMETRIC.RGB
            )
            assert_array_equal(imread(filename), data)
        else:
            with pytest.raises(NotImplementedError):
                imwrite(
                    filename,
                    data,
                    bitspersample=bps,
                    photometric=PHOTOMETRIC.RGB,
                )
                # assert_array_equal(imread(filename), data)


def test_write_bitspersample_fail():
    """Test write with packints fails."""
    data = numpy.arange(32 * 32 * 3, dtype=numpy.uint32).reshape((32, 32, 3))
    with (
        TempFileName('write_bitspersample_fail') as filename,
        TiffWriter(filename) as tif,
    ):
        # not working with compression
        with pytest.raises(ValueError):
            tif.write(
                data.astype(numpy.uint8),
                bitspersample=4,
                compression=COMPRESSION.ADOBE_DEFLATE,
                photometric=PHOTOMETRIC.RGB,
            )
        # dtype.itemsize != bitspersample
        for dtype in (
            numpy.int8,
            numpy.int16,
            numpy.float32,
            numpy.uint64,
        ):
            with pytest.raises(ValueError):
                tif.write(
                    data.astype(dtype),
                    bitspersample=4,
                    photometric=PHOTOMETRIC.RGB,
                )
        # bitspersample out of data range
        for bps in (0, 9, 16, 32):
            with pytest.raises(ValueError):
                tif.write(
                    data.astype(numpy.uint8),
                    bitspersample=bps,
                    photometric=PHOTOMETRIC.RGB,
                )
        for bps in (1, 8, 17, 32):
            with pytest.raises(ValueError):
                tif.write(
                    data.astype(numpy.uint16),
                    bitspersample=bps,
                    photometric=PHOTOMETRIC.RGB,
                )
        for bps in (1, 8, 16, 33, 64):
            with pytest.raises(ValueError):
                tif.write(
                    data.astype(numpy.uint32),
                    bitspersample=bps,
                    photometric=PHOTOMETRIC.RGB,
                )


@pytest.mark.parametrize('kind', ['enum', 'int', 'lower', 'upper'])
def test_write_enum_parameters(kind):
    """Test imwrite using different kind of enum."""
    data = random_data(numpy.uint8, (2, 6, 219, 301))
    with TempFileName(f'write_enum_parameters_{kind}') as filename:
        if kind == 'enum':
            imwrite(
                filename,
                data,
                photometric=PHOTOMETRIC.RGB,
                planarconfig=PLANARCONFIG.SEPARATE,
                extrasamples=(
                    EXTRASAMPLE.ASSOCALPHA,
                    EXTRASAMPLE.UNSPECIFIED,
                    EXTRASAMPLE.UNASSALPHA,
                ),
                compression=COMPRESSION.ADOBE_DEFLATE,
                predictor=PREDICTOR.HORIZONTAL,
            )
        elif kind == 'int':
            imwrite(
                filename,
                data,
                photometric=2,
                planarconfig=2,
                extrasamples=(1, 0, 2),
                compression=8,
                predictor=2,
            )
        elif kind == 'upper':
            imwrite(
                filename,
                data,
                photometric='RGB',
                planarconfig='SEPARATE',
                extrasamples=('ASSOCALPHA', 'UNSPECIFIED', 'UNASSALPHA'),
                compression='ADOBE_DEFLATE',
                predictor='HORIZONTAL',
            )
        elif kind == 'lower':
            imwrite(
                filename,
                data,
                photometric='rgb',
                planarconfig='separate',
                extrasamples=('assocalpha', 'unspecified', 'unassalpha'),
                compression='adobe_deflate',
                predictor='horizontal',
            )
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 2
            page = tif.pages.first
            assert page.imagewidth == 301
            assert page.imagelength == 219
            assert page.samplesperpixel == 6
            assert page.photometric == PHOTOMETRIC.RGB
            assert page.planarconfig == PLANARCONFIG.SEPARATE
            assert page.extrasamples == (
                EXTRASAMPLE.ASSOCALPHA,
                EXTRASAMPLE.UNSPECIFIED,
                EXTRASAMPLE.UNASSALPHA,
            )
            assert page.compression == COMPRESSION.ADOBE_DEFLATE
            assert page.predictor == PREDICTOR.HORIZONTAL
            image = tif.asarray()
            assert_array_equal(data, image)
            assert_aszarr_method(tif, image)
            assert__str__(tif)


@pytest.mark.parametrize(
    'args',
    [
        (0, 0),
        (1, 1),
        (2, COMPRESSION.NONE),
        (3, COMPRESSION.ADOBE_DEFLATE),
        (4, 'zlib'),
        (5, 'zlib', 5, {}),
        (6, 'zlib', 5, {'out': None}),
        (7, 'zlib', None, {'level': 5}),
    ],
)
def test_write_compression_args(args):
    """Test compression parameter."""
    i = args[0]
    compressionargs = args[1:]
    compressed = compressionargs[0] not in {0, 1, COMPRESSION.NONE}
    if len(compressionargs) == 1:
        compressionargs = compressionargs[0]
    rowsperstrip = 100 if compressed else None

    kwargs = {
        'photometric': PHOTOMETRIC.RGB,
        'rowsperstrip': rowsperstrip,
        'compression': compressionargs,
    }

    data = WRITE_DATA
    with TempFileName(f'write_compression_args_{i}') as filename:
        # with pytest.warns(DeprecationWarning if i > 4 else None):
        if i > 4:
            with pytest.raises((TypeError, ValueError)):
                imwrite(filename, data, **kwargs)
            kwargs['compression'] = compressionargs[0]
            if len(compressionargs) > 1:
                kwargs['compressionargs'] = {
                    'level': compressionargs[1],
                    **compressionargs[2],
                }
            imwrite(filename, data, **kwargs)
        else:
            imwrite(filename, data, **kwargs)
        assert_valid_tiff(filename)
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 1
            page = tif.pages.first
            assert page.compression == (
                COMPRESSION.ADOBE_DEFLATE if compressed else COMPRESSION.NONE
            )
            assert page.planarconfig == PLANARCONFIG.SEPARATE
            assert page.photometric == PHOTOMETRIC.RGB
            assert page.imagewidth == 301
            assert page.imagelength == 219
            assert page.samplesperpixel == 3
            assert len(page.dataoffsets) == (9 if compressed else 3)
            image = tif.asarray()
            assert_array_equal(data, image)
            assert__str__(tif)


@pytest.mark.parametrize(
    'args',
    [
        (0, 0),
        (1, 5),
        (2, COMPRESSION.ADOBE_DEFLATE),
        (3, COMPRESSION.ADOBE_DEFLATE, 5),
    ],
)
def test_write_compress_args(args):
    """Test compress parameter no longer works in 2022.4.22."""
    i = args[0]
    compressargs = args[1:]
    if len(compressargs) == 1:
        compressargs = compressargs[0]

    data = WRITE_DATA
    with TempFileName(  # noqa: SIM117
        f'write_compression_args_{i}'
    ) as filename:
        with pytest.raises(TypeError):
            imwrite(
                filename,
                data,
                compress=compressargs,
                photometric=PHOTOMETRIC.RGB,
            )


def test_write_compression_none():
    """Test write compression=0."""
    data = WRITE_DATA
    with TempFileName('write_compression_none') as filename:
        imwrite(filename, data, compression=0, photometric=PHOTOMETRIC.RGB)
        assert_valid_tiff(filename)
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 1
            page = tif.pages.first
            assert page.is_contiguous
            assert page.compression == COMPRESSION.NONE
            assert page.planarconfig == PLANARCONFIG.SEPARATE
            assert page.photometric == PHOTOMETRIC.RGB
            assert page.imagewidth == 301
            assert page.imagelength == 219
            assert page.samplesperpixel == 3
            assert len(page.dataoffsets) == 3
            image = tif.asarray()
            assert_array_equal(data, image)
            assert_aszarr_method(tif, image)
            assert__str__(tif)


# @pytest.mark.parametrize('optimize', [None, False, True])
# @pytest.mark.parametrize('smoothing', [None, 10])
@pytest.mark.skipif(
    SKIP_FILE or SKIP_CODECS or not imagecodecs.JPEG.available, reason=REASON
)
@pytest.mark.parametrize('subsampling', ['444', '422', '420', '411'])
@pytest.mark.parametrize('dtype', [numpy.uint8, numpy.uint16])
def test_write_compression_jpeg(dtype, subsampling):
    """Test write JPEG compression with subsampling."""
    dtype = numpy.dtype(dtype)
    filename = f'write_compression_jpeg_{dtype}_{subsampling}'
    subsampling, atol = {
        '444': [(1, 1), 5],
        '422': [(2, 1), 10],
        '420': [(2, 2), 20],
        '411': [(4, 1), 40],
    }[subsampling]
    data = numpy.load(_file('tifffile/rgb.u1.npy')).astype(dtype)
    data = data[:32, :16].copy()  # make divisible by subsamples
    with TempFileName(filename) as filename:
        imwrite(
            filename,
            data,
            compression=COMPRESSION.JPEG,
            compressionargs={'level': 99},
            subsampling=subsampling,
            photometric=PHOTOMETRIC.RGB,
        )
        assert_valid_tiff(filename)
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 1
            page = tif.pages.first
            assert not page.is_contiguous
            if subsampling[0] > 1:
                assert page.is_subsampled
                assert page.tags['YCbCrSubSampling'].value == subsampling
            assert page.compression == COMPRESSION.JPEG
            assert page.photometric == PHOTOMETRIC.YCBCR
            assert page.imagewidth == data.shape[1]
            assert page.imagelength == data.shape[0]
            assert page.samplesperpixel == 3
            image = tif.asarray()
            assert_allclose(data, image, atol=atol)
            assert_aszarr_method(tif, image)
            assert__str__(tif)


@pytest.mark.parametrize('dtype', [numpy.uint8, bool])
def test_write_compression_deflate(dtype):
    """Test write ZLIB compression."""
    dtype = numpy.dtype(dtype)
    data = WRITE_DATA.astype(dtype)
    with TempFileName(f'write_compression_deflate_{dtype}') as filename:
        imwrite(
            filename,
            data,
            compression=COMPRESSION.DEFLATE,
            compressionargs={'level': 6},
            photometric=PHOTOMETRIC.RGB,
            rowsperstrip=108,
        )
        assert_valid_tiff(filename)
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 1
            page = tif.pages.first
            assert not page.is_contiguous
            assert page.compression == COMPRESSION.DEFLATE
            assert page.planarconfig == PLANARCONFIG.SEPARATE
            assert page.photometric == PHOTOMETRIC.RGB
            assert page.imagewidth == 301
            assert page.imagelength == 219
            assert page.samplesperpixel == 3
            assert page.rowsperstrip == 108
            assert len(page.dataoffsets) == 9
            image = tif.asarray()
            assert_array_equal(data, image)
            assert_aszarr_method(tif, image)
            assert__str__(tif)


def test_write_compression_deflate_level():
    """Test write ZLIB compression with level."""
    data = WRITE_DATA
    with TempFileName('write_compression_deflate_level') as filename:
        imwrite(
            filename,
            data,
            compression=COMPRESSION.ADOBE_DEFLATE,
            compressionargs={'level': 9},
            photometric=PHOTOMETRIC.RGB,
        )
        assert_valid_tiff(filename)
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 1
            page = tif.pages.first
            assert not page.is_contiguous
            assert page.compression == COMPRESSION.ADOBE_DEFLATE
            assert page.planarconfig == PLANARCONFIG.SEPARATE
            assert page.photometric == PHOTOMETRIC.RGB
            assert page.imagewidth == 301
            assert page.imagelength == 219
            assert page.samplesperpixel == 3
            image = tif.asarray()
            assert_array_equal(data, image)
            assert_aszarr_method(tif, image)
            assert__str__(tif)


@pytest.mark.parametrize('dtype', [numpy.uint8, bool])
def test_write_compression_lzma(dtype):
    """Test write LZMA compression."""
    dtype = numpy.dtype(dtype)
    data = WRITE_DATA.astype(dtype)
    with TempFileName(f'write_compression_lzma_{dtype}') as filename:
        imwrite(
            filename,
            data,
            compression=COMPRESSION.LZMA,
            photometric=PHOTOMETRIC.RGB,
            rowsperstrip=108,
        )
        assert_valid_tiff(filename)
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 1
            page = tif.pages.first
            assert not page.is_contiguous
            assert page.compression == COMPRESSION.LZMA
            assert page.planarconfig == PLANARCONFIG.SEPARATE
            assert page.photometric == PHOTOMETRIC.RGB
            assert page.imagewidth == 301
            assert page.imagelength == 219
            assert page.samplesperpixel == 3
            assert page.rowsperstrip == 108
            assert len(page.dataoffsets) == 9
            image = tif.asarray()
            assert_array_equal(data, image)
            assert_aszarr_method(tif, image)
            assert__str__(tif)


@pytest.mark.skipif(
    SKIP_CODECS or not imagecodecs.ZSTD.available, reason=REASON
)
@pytest.mark.parametrize('dtype', [numpy.uint8, bool])
def test_write_compression_zstd(dtype):
    """Test write ZSTD compression."""
    dtype = numpy.dtype(dtype)
    data = WRITE_DATA.astype(dtype)
    with TempFileName(f'write_compression_zstd_{dtype}') as filename:
        imwrite(
            filename,
            data,
            compression=COMPRESSION.ZSTD,
            photometric=PHOTOMETRIC.RGB,
            rowsperstrip=108,
        )
        assert_valid_tiff(filename)
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 1
            page = tif.pages.first
            assert not page.is_contiguous
            assert page.compression == COMPRESSION.ZSTD
            assert page.planarconfig == PLANARCONFIG.SEPARATE
            assert page.photometric == PHOTOMETRIC.RGB
            assert page.imagewidth == 301
            assert page.imagelength == 219
            assert page.samplesperpixel == 3
            assert page.rowsperstrip == 108
            assert len(page.dataoffsets) == 9
            image = tif.asarray()
            assert_array_equal(data, image)
            assert_aszarr_method(tif, image)
            assert__str__(tif)


@pytest.mark.skipif(
    SKIP_CODECS or not imagecodecs.WEBP.available, reason=REASON
)
def test_write_compression_webp():
    """Test write WEBP compression."""
    data = WRITE_DATA.astype(numpy.uint8).reshape((219, 301, 3))
    with TempFileName('write_compression_webp') as filename:
        imwrite(
            filename,
            data,
            compression=COMPRESSION.WEBP,
            compressionargs={'level': -1},
            photometric=PHOTOMETRIC.RGB,
        )
        assert_valid_tiff(filename)
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 1
            page = tif.pages.first
            assert not page.is_contiguous
            assert page.compression == COMPRESSION.WEBP
            assert page.photometric == PHOTOMETRIC.RGB
            assert page.imagewidth == 301
            assert page.imagelength == 219
            assert page.samplesperpixel == 3
            image = tif.asarray()
            assert_array_equal(data, image)
            assert_aszarr_method(tif, image)
            assert__str__(tif)


@pytest.mark.skipif(
    SKIP_CODECS or not imagecodecs.JPEG2K.available, reason=REASON
)
def test_write_compression_jpeg2k():
    """Test write JPEG 2000 compression."""
    data = WRITE_DATA.astype(numpy.uint8).reshape((219, 301, 3))
    with TempFileName('write_compression_jpeg2k') as filename:
        imwrite(
            filename,
            data,
            compression=COMPRESSION.JPEG2000,
            compressionargs={'level': -1},
            photometric=PHOTOMETRIC.RGB,
        )
        assert_valid_tiff(filename)
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 1
            page = tif.pages.first
            assert not page.is_contiguous
            assert page.compression == COMPRESSION.JPEG2000
            assert page.photometric == PHOTOMETRIC.RGB
            assert page.imagewidth == 301
            assert page.imagelength == 219
            assert page.samplesperpixel == 3
            image = tif.asarray()
            assert_array_equal(data, image)
            assert_aszarr_method(tif, image)
            assert__str__(tif)


@pytest.mark.skipif(
    SKIP_CODECS or not imagecodecs.JPEGXL.available, reason=REASON
)
def test_write_compression_jpegxl():
    """Test write JPEG XL compression."""
    data = WRITE_DATA.astype(numpy.uint8).reshape((219, 301, 3))
    with TempFileName('write_compression_jpegxl') as filename:
        imwrite(
            filename,
            data,
            compression=COMPRESSION.JPEGXL,
            compressionargs={'level': 101},  # lossless
            photometric=PHOTOMETRIC.RGB,
        )
        assert_valid_tiff(filename)
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 1
            page = tif.pages.first
            assert not page.is_contiguous
            assert page.compression == COMPRESSION.JPEGXL
            assert page.photometric == PHOTOMETRIC.RGB
            assert page.imagewidth == 301
            assert page.imagelength == 219
            assert page.samplesperpixel == 3
            image = tif.asarray()
            assert_array_equal(data, image)
            assert_aszarr_method(tif, image)
            assert__str__(tif)


@pytest.mark.skipif(SKIP_CODECS, reason=REASON)
@pytest.mark.parametrize('compression', [None, 'deflate', 'zstd'])
def test_write_compression_lerc(compression):
    """Test write LERC compression."""
    if not hasattr(imagecodecs, 'LERC'):
        pytest.skip('LERC codec missing')
    compressionargs = {
        None: None,
        'deflate': {'level': 7},
        'zstd': {'level': 10},
    }[compression]
    lercparameters = {
        None: (4, 0),
        'deflate': (4, 1),
        'zstd': (4, 2),
    }[compression]

    data = WRITE_DATA.astype(numpy.uint16).reshape((219, 301, 3))
    with TempFileName(f'write_compression_lerc_{compression}') as filename:
        imwrite(
            filename,
            data,
            photometric=PHOTOMETRIC.RGB,
            compression=COMPRESSION.LERC,
            compressionargs={
                'compression': compression,
                'compressionargs': compressionargs,
            },
        )
        assert_valid_tiff(filename)
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 1
            page = tif.pages.first
            assert not page.is_contiguous
            assert page.compression == COMPRESSION.LERC
            assert page.photometric == PHOTOMETRIC.RGB
            assert page.imagewidth == 301
            assert page.imagelength == 219
            assert page.samplesperpixel == 3
            assert page.tags['LercParameters'].value == lercparameters

            image = tif.asarray()
            assert_array_equal(data, image)
            assert_aszarr_method(tif, image)
            assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE or SKIP_CODECS, reason=REASON)
def test_write_compression_jetraw():
    """Test write Jetraw compression."""
    try:
        have_jetraw = imagecodecs.JETRAW.available
    except AttributeError:
        # requires imagecodecs > 2022.22.2
        have_jetraw = False
    if not have_jetraw:
        pytest.skip('Jetraw codec not available')

    data = imread(_file('jetraw/16ms-1.tif'))
    assert data.dtype == numpy.uint16
    assert data.shape == (2304, 2304)
    assert data[1490, 1830] == 36701

    # Jetraw requires initialization
    imagecodecs.jetraw_init()

    with TempFileName('write_compression_jetraw') as filename:
        try:
            imwrite(
                filename,
                data,
                compression=COMPRESSION.JETRAW,
                compressionargs={'identifier': '500202_standard_bin1x'},
            )
        except imagecodecs.JetrawError as exc:
            if 'license' in str(exc):
                pytest.skip('Jetraw_encode requires a license')
            else:
                raise

        with TiffFile(filename) as tif:
            assert len(tif.pages) == 1
            page = tif.pages.first
            assert page.compression == COMPRESSION.JETRAW
            assert page.photometric == PHOTOMETRIC.MINISBLACK
            assert page.planarconfig == PLANARCONFIG.CONTIG
            assert page.imagewidth == 2304
            assert page.imagelength == 2304
            assert page.rowsperstrip == 2304
            assert page.bitspersample == 16
            assert page.samplesperpixel == 1
            image = tif.asarray()
            assert (
                numpy.mean(
                    image.astype(numpy.float32) - data.astype(numpy.float32)
                )
                < 0.5
            )
            assert__str__(tif)


@pytest.mark.skipif(SKIP_CODECS, reason=REASON)
@pytest.mark.parametrize('dtype', [numpy.int8, numpy.uint8, bool])
@pytest.mark.parametrize('tile', [None, (16, 16)])
def test_write_compression_packbits(dtype, tile):
    """Test write PackBits compression."""
    dtype = numpy.dtype(dtype)
    uncompressed = numpy.frombuffer(
        b'\xaa\xaa\xaa\x80\x00\x2a\xaa\xaa\xaa\xaa\x80\x00'
        b'\x2a\x22\xaa\xaa\xaa\xaa\xaa\xaa\xaa\xaa\xaa\xaa',
        dtype=dtype,
    )
    shape = 2, 7, uncompressed.size
    data = numpy.empty(shape, dtype=dtype)
    data[..., :] = uncompressed
    with TempFileName(f'write_compression_packits_{dtype}') as filename:
        imwrite(filename, data, compression=COMPRESSION.PACKBITS, tile=tile)
        assert_valid_tiff(filename)
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 2
            page = tif.pages.first
            assert not page.is_contiguous
            assert page.compression == COMPRESSION.PACKBITS
            assert page.planarconfig == PLANARCONFIG.CONTIG
            assert page.imagewidth == uncompressed.size
            assert page.imagelength == 7
            assert page.samplesperpixel == 1
            image = tif.asarray()
            assert_array_equal(data, image)
            assert_aszarr_method(tif, image)
            assert__str__(tif)


def test_write_compression_rowsperstrip():
    """Test write rowsperstrip with compression."""
    data = WRITE_DATA
    with TempFileName('write_compression_rowsperstrip') as filename:
        imwrite(
            filename,
            data,
            compression=COMPRESSION.ADOBE_DEFLATE,
            rowsperstrip=32,
            photometric=PHOTOMETRIC.RGB,
        )
        assert_valid_tiff(filename)
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 1
            page = tif.pages.first
            assert not page.is_contiguous
            assert page.compression == COMPRESSION.ADOBE_DEFLATE
            assert page.planarconfig == PLANARCONFIG.SEPARATE
            assert page.photometric == PHOTOMETRIC.RGB
            assert page.imagewidth == 301
            assert page.imagelength == 219
            assert page.samplesperpixel == 3
            assert page.rowsperstrip == 32
            assert len(page.dataoffsets) == 21
            image = tif.asarray()
            assert_array_equal(data, image)
            assert_aszarr_method(tif, image)
            assert__str__(tif)


def test_write_compression_tiled():
    """Test write compressed tiles."""
    data = WRITE_DATA
    with TempFileName('write_compression_tiled') as filename:
        imwrite(
            filename,
            data,
            compression=COMPRESSION.ADOBE_DEFLATE,
            tile=(32, 32),
            photometric=PHOTOMETRIC.RGB,
        )
        assert_valid_tiff(filename)
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 1
            page = tif.pages.first
            assert not page.is_contiguous
            assert page.is_tiled
            assert page.compression == COMPRESSION.ADOBE_DEFLATE
            assert page.planarconfig == PLANARCONFIG.SEPARATE
            assert page.photometric == PHOTOMETRIC.RGB
            assert page.imagewidth == 301
            assert page.imagelength == 219
            assert page.samplesperpixel == 3
            assert len(page.dataoffsets) == 210
            image = tif.asarray()
            assert_array_equal(data, image)
            assert_aszarr_method(tif, image)
            assert__str__(tif)


def test_write_compression_predictor():
    """Test write horizontal differencing."""
    data = WRITE_DATA
    with TempFileName('write_compression_predictor') as filename:
        imwrite(
            filename,
            data,
            compression=COMPRESSION.ADOBE_DEFLATE,
            predictor=PREDICTOR.HORIZONTAL,
            photometric=PHOTOMETRIC.RGB,
        )
        assert_valid_tiff(filename)
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 1
            page = tif.pages.first
            assert not page.is_contiguous
            assert page.compression == COMPRESSION.ADOBE_DEFLATE
            assert page.planarconfig == PLANARCONFIG.SEPARATE
            assert page.photometric == PHOTOMETRIC.RGB
            assert page.predictor == PREDICTOR.HORIZONTAL
            assert page.imagewidth == 301
            assert page.imagelength == 219
            assert page.samplesperpixel == 3
            image = tif.asarray()
            assert_array_equal(data, image)
            assert_aszarr_method(tif, image)
            assert__str__(tif)


@pytest.mark.skipif(SKIP_CODECS, reason=REASON)
@pytest.mark.parametrize('dtype', [numpy.uint16, numpy.float32])
def test_write_compression_predictor_tiled(dtype):
    """Test write horizontal differencing with tiles."""
    dtype = numpy.dtype(dtype)
    data = WRITE_DATA.astype(dtype)
    with TempFileName(
        f'write_compression_tiled_predictor_{dtype}'
    ) as filename:
        imwrite(
            filename,
            data,
            compression=COMPRESSION.ADOBE_DEFLATE,
            predictor=True,
            tile=(32, 32),
            photometric=PHOTOMETRIC.RGB,
        )
        if dtype.kind != 'f':
            assert_valid_tiff(filename)
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 1
            page = tif.pages.first
            assert not page.is_contiguous
            assert page.is_tiled
            assert page.compression == COMPRESSION.ADOBE_DEFLATE
            assert page.planarconfig == PLANARCONFIG.SEPARATE
            assert page.photometric == PHOTOMETRIC.RGB
            assert page.imagewidth == 301
            assert page.imagelength == 219
            assert page.samplesperpixel == 3
            assert page.predictor == 3 if dtype.kind == 'f' else 2
            image = tif.asarray()
            assert_array_equal(data, image)
            assert_aszarr_method(tif, image)
            assert__str__(tif)


def test_write_rowsperstrip():
    """Test write rowsperstrip without compression."""
    data = WRITE_DATA
    with TempFileName('write_rowsperstrip') as filename:
        imwrite(
            filename,
            data,
            rowsperstrip=32,
            contiguous=False,
            photometric=PHOTOMETRIC.RGB,
            metadata=None,
        )
        assert_valid_tiff(filename)
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 1
            page = tif.pages.first
            assert page.is_contiguous
            assert page.planarconfig == PLANARCONFIG.SEPARATE
            assert page.photometric == PHOTOMETRIC.RGB
            assert page.imagewidth == 301
            assert page.imagelength == 219
            assert page.samplesperpixel == 3
            assert page.rowsperstrip == 32
            assert len(page.dataoffsets) == 21
            stripbytecounts = page.tags['StripByteCounts'].value
            assert stripbytecounts[0] == 19264
            assert stripbytecounts[6] == 16254
            image = tif.asarray()
            assert_array_equal(data, image)
            assert_aszarr_method(tif, image)
            assert__str__(tif)


@pytest.mark.skipif(IS_BE, reason=REASON)
def test_write_bigendian():
    """Test write big endian file."""
    # also test memory mapping non-native byte order
    data = random_data(numpy.float32, (2, 3, 219, 301))
    data = data.view(data.dtype.newbyteorder())
    data = numpy.nan_to_num(data, copy=False)
    with TempFileName('write_bigendian') as filename:
        imwrite(
            filename,
            data,
            planarconfig=PLANARCONFIG.SEPARATE,
            photometric=PHOTOMETRIC.RGB,
        )
        assert_valid_tiff(filename)
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 2
            assert len(tif.series) == 1
            assert tif.byteorder == '>'
            # assert not tif.isnative
            assert tif.series[0].dataoffset is not None
            page = tif.pages.first
            assert page.is_contiguous
            assert page.planarconfig == PLANARCONFIG.SEPARATE
            assert page.photometric == PHOTOMETRIC.RGB
            assert page.imagewidth == 301
            assert page.imagelength == 219
            assert page.samplesperpixel == 3
            # test read data
            image = tif.asarray()
            assert_array_equal(data, image)
            assert_aszarr_method(tif, image)
            image = page.asarray()
            assert_array_equal(data[0], image)
            # test direct memory mapping; returns big endian array
            image = tif.asarray(out='memmap')
            assert isinstance(image, numpy.memmap)
            assert image.dtype == numpy.dtype('>f4')
            assert_array_equal(data, image)
            del image
            image = page.asarray(out='memmap')
            assert isinstance(image, numpy.memmap)
            assert image.dtype == numpy.dtype('>f4')
            assert_array_equal(data[0], image)
            del image
            # test indirect memory mapping; returns native endian array
            image = tif.asarray(out='memmap:')
            assert isinstance(image, numpy.memmap)
            assert image.dtype == numpy.dtype('=f4')
            assert_array_equal(data, image)
            del image
            image = page.asarray(out='memmap:')
            assert isinstance(image, numpy.memmap)
            assert image.dtype == numpy.dtype('=f4')
            assert_array_equal(data[0], image)
            del image
            # test 2nd page
            page = tif.pages[1]
            image = page.asarray(out='memmap')
            assert isinstance(image, numpy.memmap)
            assert image.dtype == numpy.dtype('>f4')
            assert_array_equal(data[1], image)
            del image
            image = page.asarray(out='memmap:')
            assert isinstance(image, numpy.memmap)
            assert image.dtype == numpy.dtype('=f4')
            assert_array_equal(data[1], image)
            del image
            assert__str__(tif)


def test_write_zero_size():
    """Test write zero size array no longer fails."""
    # with pytest.raises(ValueError):
    with TempFileName('write_empty') as filename:  # noqa: SIM117
        with pytest.warns(UserWarning):
            imwrite(filename, numpy.empty(0))


def test_write_pixel():
    """Test write single pixel."""
    data = numpy.zeros(1, dtype=numpy.uint8)
    with TempFileName('write_pixel') as filename:
        imwrite(filename, data)
        assert_valid_tiff(filename)
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 1
            assert tif.series[0].axes == 'Y'
            page = tif.pages.first
            assert page.is_contiguous
            assert page.planarconfig == PLANARCONFIG.CONTIG
            assert page.photometric != PHOTOMETRIC.RGB
            assert page.imagewidth == 1
            assert page.imagelength == 1
            assert page.samplesperpixel == 1
            image = tif.asarray()
            assert_array_equal(data, image)
            assert_aszarr_method(tif, image)
            assert_aszarr_method(tif, image, chunkmode='page')
            assert__str__(tif)


def test_write_small():
    """Test write small image."""
    data = random_data(numpy.uint8, (1, 1))
    with TempFileName('write_small') as filename:
        imwrite(filename, data)
        assert_valid_tiff(filename)
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 1
            page = tif.pages.first
            assert page.is_contiguous
            assert page.planarconfig == PLANARCONFIG.CONTIG
            assert page.photometric != PHOTOMETRIC.RGB
            assert page.imagewidth == 1
            assert page.imagelength == 1
            assert page.samplesperpixel == 1
            image = tif.asarray()
            assert_array_equal(data, image)
            assert_aszarr_method(tif, image)
            assert__str__(tif)


def test_write_2d_as_rgb():
    """Test write RGB color palette as RGB image."""
    # image length should be 1
    data = numpy.arange(3 * 256, dtype=numpy.uint16).reshape((256, 3)) // 3
    with TempFileName('write_2d_as_rgb_contig') as filename:
        imwrite(filename, data, photometric=PHOTOMETRIC.RGB)
        assert_valid_tiff(filename)
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 1
            assert tif.series[0].axes == 'XS'
            page = tif.pages.first
            assert page.is_contiguous
            assert page.planarconfig == PLANARCONFIG.CONTIG
            assert page.photometric == PHOTOMETRIC.RGB
            assert page.imagewidth == 256
            assert page.imagelength == 1
            assert page.samplesperpixel == 3
            image = tif.asarray()
            assert_array_equal(data, image)
            assert_aszarr_method(tif, image)
            assert_aszarr_method(tif, image, chunkmode='page')
            assert__str__(tif)


def test_write_auto_photometric_planar():
    """Test detect photometric in planar mode."""
    data = random_data(numpy.uint8, (4, 31, 33))
    with TempFileName('write_auto_photometric_planar1') as filename:
        with pytest.warns(DeprecationWarning):
            imwrite(filename, data)
        assert_valid_tiff(filename)
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 1
            page = tif.pages.first
            assert page.shape == (4, 31, 33)
            assert page.axes == 'SYX'
            assert page.planarconfig == PLANARCONFIG.SEPARATE
            assert page.photometric == PHOTOMETRIC.RGB
            assert len(page.extrasamples) == 1

    with TempFileName('write_auto_photometric_planar2') as filename:
        with pytest.warns(DeprecationWarning):
            imwrite(filename, data, planarconfig='separate')
        assert_valid_tiff(filename)
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 1
            page = tif.pages.first
            assert page.shape == (4, 31, 33)
            assert page.axes == 'SYX'
            assert page.planarconfig == PLANARCONFIG.SEPARATE
            assert page.photometric == PHOTOMETRIC.RGB
            assert len(page.extrasamples) == 1

    data = random_data(numpy.uint8, (4, 7, 31, 33))
    with TempFileName('write_auto_photometric_volumetric_planar1') as filename:
        with pytest.warns(DeprecationWarning):
            imwrite(filename, data, volumetric=True)
        assert_valid_tiff(filename)
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 1
            page = tif.pages.first
            assert page.shape == (4, 7, 31, 33)
            assert page.axes == 'SZYX'
            assert page.planarconfig == PLANARCONFIG.SEPARATE
            assert page.photometric == PHOTOMETRIC.RGB
            assert len(page.extrasamples) == 1
            assert page.is_volumetric

    with TempFileName('write_auto_photometric_volumetric_planar2') as filename:
        with pytest.warns(DeprecationWarning):
            imwrite(filename, data, planarconfig='separate', volumetric=True)
        assert_valid_tiff(filename)
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 1
            page = tif.pages.first
            assert page.shape == (4, 7, 31, 33)
            assert page.axes == 'SZYX'
            assert page.planarconfig == PLANARCONFIG.SEPARATE
            assert page.photometric == PHOTOMETRIC.RGB
            assert len(page.extrasamples) == 1
            assert page.is_volumetric


def test_write_auto_photometric_contig():
    """Test detect photometric in contig mode."""
    data = random_data(numpy.uint8, (31, 33, 4))
    with TempFileName('write_auto_photometric_contig1') as filename:
        imwrite(filename, data)
        assert_valid_tiff(filename)
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 1
            page = tif.pages.first
            assert page.shape == (31, 33, 4)
            assert page.axes == 'YXS'
            assert page.planarconfig == PLANARCONFIG.CONTIG
            assert page.photometric == PHOTOMETRIC.RGB
            assert len(page.extrasamples) == 1

    with TempFileName('write_auto_photometric_contig2') as filename:
        imwrite(filename, data, planarconfig='contig')
        assert_valid_tiff(filename)
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 1
            page = tif.pages.first
            assert page.shape == (31, 33, 4)
            assert page.axes == 'YXS'
            assert page.planarconfig == PLANARCONFIG.CONTIG
            assert page.photometric == PHOTOMETRIC.RGB
            assert len(page.extrasamples) == 1

    data = random_data(numpy.uint8, (7, 31, 33, 4))
    with TempFileName('write_auto_photometric_volumetric_contig1') as filename:
        imwrite(filename, data, volumetric=True)
        assert_valid_tiff(filename)
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 1
            page = tif.pages.first
            assert page.shape == (7, 31, 33, 4)
            assert page.axes == 'ZYXS'
            assert page.planarconfig == PLANARCONFIG.CONTIG
            assert page.photometric == PHOTOMETRIC.RGB
            assert len(page.extrasamples) == 1
            assert page.is_volumetric

    with TempFileName('write_auto_photometric_volumetric_contig2') as filename:
        imwrite(filename, data, planarconfig='contig', volumetric=True)
        assert_valid_tiff(filename)
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 1
            page = tif.pages.first
            assert page.shape == (7, 31, 33, 4)
            assert page.axes == 'ZYXS'
            assert page.planarconfig == PLANARCONFIG.CONTIG
            assert page.photometric == PHOTOMETRIC.RGB
            assert len(page.extrasamples) == 1
            assert page.is_volumetric


def test_write_invalid_contig_rgb():
    """Test write planar RGB with 2 samplesperpixel."""
    data = random_data(numpy.uint8, (219, 301, 2))
    with TempFileName('write_invalid_contig_rgb') as filename:  # noqa: SIM117
        with pytest.raises(ValueError):
            imwrite(filename, data, photometric=PHOTOMETRIC.RGB)
    # default to pages
    with TempFileName('write_invalid_contig_rgb_pages') as filename:
        imwrite(filename, data)
        assert_valid_tiff(filename)
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 219
            assert tif.series[0].axes == 'QYX'
            page = tif.pages.first
            assert page.is_contiguous
            assert page.planarconfig == PLANARCONFIG.CONTIG
            assert page.photometric != PHOTOMETRIC.RGB
            assert page.imagewidth == 2
            assert page.imagelength == 301
            assert page.samplesperpixel == 1
            image = tif.asarray()
            assert_array_equal(data, image)
            assert_aszarr_method(tif, image)
            assert__str__(tif)
    # better save as contig samples
    with TempFileName('write_invalid_contig_rgb_samples') as filename:
        imwrite(filename, data, planarconfig=PLANARCONFIG.CONTIG)
        assert_valid_tiff(filename)
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 1
            assert tif.series[0].axes == 'YXS'
            page = tif.pages.first
            assert page.is_contiguous
            assert page.planarconfig == PLANARCONFIG.CONTIG
            assert page.photometric != PHOTOMETRIC.RGB
            assert page.imagewidth == 301
            assert page.imagelength == 219
            assert page.samplesperpixel == 2
            image = tif.asarray()
            assert_array_equal(data, image)
            assert_aszarr_method(tif, image)
            assert__str__(tif)


def test_write_invalid_planar_rgb():
    """Test write planar RGB with 2 samplesperpixel."""
    data = random_data(numpy.uint8, (2, 219, 301))
    with pytest.raises(ValueError):  # noqa: SIM117
        with TempFileName('write_invalid_planar_rgb') as filename:
            imwrite(
                filename,
                data,
                photometric=PHOTOMETRIC.RGB,
                planarconfig=PLANARCONFIG.SEPARATE,
            )
    # default to pages
    with TempFileName('write_invalid_planar_rgb_pages') as filename:
        imwrite(filename, data)
        assert_valid_tiff(filename)
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 2
            assert tif.series[0].axes == 'QYX'
            page = tif.pages.first
            assert page.is_contiguous
            assert page.planarconfig == PLANARCONFIG.CONTIG
            assert page.photometric != PHOTOMETRIC.RGB
            assert page.imagewidth == 301
            assert page.imagelength == 219
            assert page.samplesperpixel == 1
            image = tif.asarray()
            assert_array_equal(data, image)
            assert_aszarr_method(tif, image)
            assert__str__(tif)
    # or save as planar samples
    with TempFileName('write_invalid_planar_rgb_samples') as filename:
        imwrite(filename, data, planarconfig=PLANARCONFIG.SEPARATE)
        assert_valid_tiff(filename)
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 1
            assert tif.series[0].axes == 'SYX'
            page = tif.pages.first
            assert page.is_contiguous
            assert page.planarconfig == PLANARCONFIG.SEPARATE
            assert page.photometric != PHOTOMETRIC.RGB
            assert page.imagewidth == 301
            assert page.imagelength == 219
            assert page.samplesperpixel == 2
            image = tif.asarray()
            assert_array_equal(data, image)
            assert_aszarr_method(tif, image)
            assert__str__(tif)


def test_write_extrasamples_gray():
    """Test write grayscale with extrasamples contig."""
    data = random_data(numpy.uint8, (301, 219, 2))
    with TempFileName('write_extrasamples_gray') as filename:
        imwrite(filename, data, extrasamples=[EXTRASAMPLE.UNASSALPHA])
        assert_valid_tiff(filename)
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 1
            page = tif.pages.first
            assert page.is_contiguous
            assert page.photometric == PHOTOMETRIC.MINISBLACK
            assert page.planarconfig == PLANARCONFIG.CONTIG
            assert page.imagewidth == 219
            assert page.imagelength == 301
            assert page.samplesperpixel == 2
            assert page.extrasamples[0] == 2
            image = tif.asarray()
            assert_array_equal(data, image)
            assert_aszarr_method(tif, image)
            assert__str__(tif)


def test_write_extrasamples_gray_planar():
    """Test write planar grayscale with extrasamples."""
    data = random_data(numpy.uint8, (2, 301, 219))
    with TempFileName('write_extrasamples_gray_planar') as filename:
        imwrite(
            filename,
            data,
            planarconfig=PLANARCONFIG.SEPARATE,
            extrasamples=[EXTRASAMPLE.UNASSALPHA],
        )
        assert_valid_tiff(filename)
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 1
            page = tif.pages.first
            assert page.is_contiguous
            assert page.photometric == PHOTOMETRIC.MINISBLACK
            assert page.planarconfig == PLANARCONFIG.SEPARATE
            assert page.imagewidth == 219
            assert page.imagelength == 301
            assert page.samplesperpixel == 2
            assert page.extrasamples[0] == 2
            image = tif.asarray()
            assert_array_equal(data, image)
            assert_aszarr_method(tif, image)
            assert__str__(tif)


def test_write_extrasamples_gray_mix():
    """Test write grayscale with multiple extrasamples."""
    data = random_data(numpy.uint8, (301, 219, 4))
    with TempFileName('write_extrasamples_gray_mix') as filename:
        imwrite(
            filename,
            data,
            photometric=PHOTOMETRIC.MINISBLACK,
            extrasamples=[
                EXTRASAMPLE.ASSOCALPHA,
                EXTRASAMPLE.UNASSALPHA,
                EXTRASAMPLE.UNSPECIFIED,
            ],
        )
        assert_valid_tiff(filename)
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 1
            page = tif.pages.first
            assert page.is_contiguous
            assert page.photometric == PHOTOMETRIC.MINISBLACK
            assert page.imagewidth == 219
            assert page.imagelength == 301
            assert page.samplesperpixel == 4
            assert page.extrasamples == (1, 2, 0)
            image = tif.asarray()
            assert_array_equal(data, image)
            assert_aszarr_method(tif, image)
            assert__str__(tif)


def test_write_extrasamples_unspecified():
    """Test write RGB with unspecified extrasamples by default."""
    data = random_data(numpy.uint8, (301, 219, 5))
    with TempFileName('write_extrasamples_unspecified') as filename:
        imwrite(filename, data, photometric=PHOTOMETRIC.RGB)
        assert_valid_tiff(filename)
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 1
            page = tif.pages.first
            assert page.is_contiguous
            assert page.photometric == PHOTOMETRIC.RGB
            assert page.imagewidth == 219
            assert page.imagelength == 301
            assert page.samplesperpixel == 5
            assert page.extrasamples == (0, 0)
            image = tif.asarray()
            assert_array_equal(data, image)
            assert_aszarr_method(tif, image)
            assert__str__(tif)


def test_write_extrasamples_assocalpha():
    """Test write RGB with assocalpha extrasample."""
    data = random_data(numpy.uint8, (219, 301, 4))
    with TempFileName('write_extrasamples_assocalpha') as filename:
        imwrite(
            filename,
            data,
            photometric=PHOTOMETRIC.RGB,
            extrasamples=EXTRASAMPLE.ASSOCALPHA,
        )
        assert_valid_tiff(filename)
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 1
            page = tif.pages.first
            assert page.is_contiguous
            assert page.planarconfig == PLANARCONFIG.CONTIG
            assert page.photometric == PHOTOMETRIC.RGB
            assert page.imagewidth == 301
            assert page.imagelength == 219
            assert page.samplesperpixel == 4
            assert page.extrasamples[0] == 1
            image = tif.asarray()
            assert_array_equal(data, image)
            assert_aszarr_method(tif, image)
            assert__str__(tif)


def test_write_extrasamples_mix():
    """Test write RGB with mixture of extrasamples."""
    data = random_data(numpy.uint8, (219, 301, 6))
    with TempFileName('write_extrasamples_mix') as filename:
        imwrite(
            filename,
            data,
            photometric=PHOTOMETRIC.RGB,
            extrasamples=[
                EXTRASAMPLE.ASSOCALPHA,
                EXTRASAMPLE.UNASSALPHA,
                EXTRASAMPLE.UNSPECIFIED,
            ],
        )
        assert_valid_tiff(filename)
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 1
            page = tif.pages.first
            assert page.is_contiguous
            assert page.planarconfig == PLANARCONFIG.CONTIG
            assert page.photometric == PHOTOMETRIC.RGB
            assert page.imagewidth == 301
            assert page.imagelength == 219
            assert page.samplesperpixel == 6
            assert page.extrasamples == (1, 2, 0)
            image = tif.asarray()
            assert_array_equal(data, image)
            assert_aszarr_method(tif, image)
            assert__str__(tif)


def test_write_extrasamples_contig():
    """Test write contig grayscale with large number of extrasamples."""
    data = random_data(numpy.uint8, (3, 219, 301))
    with TempFileName('write_extrasamples_contig') as filename:
        imwrite(filename, data, planarconfig=PLANARCONFIG.CONTIG)
        assert_valid_tiff(filename)
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 1
            page = tif.pages.first
            assert page.is_contiguous
            assert page.planarconfig == PLANARCONFIG.CONTIG
            assert page.photometric != PHOTOMETRIC.RGB
            assert page.imagewidth == 219
            assert page.imagelength == 3
            assert page.samplesperpixel == 301
            assert len(page.extrasamples) == 301 - 1
            image = tif.asarray()
            assert_array_equal(data, image)
            assert_aszarr_method(tif, image)
            assert__str__(tif)
    # better save as RGB planar
    with TempFileName('write_extrasamples_contig_planar') as filename:
        imwrite(
            filename,
            data,
            photometric=PHOTOMETRIC.RGB,
            planarconfig=PLANARCONFIG.SEPARATE,
        )
        assert_valid_tiff(filename)
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 1
            page = tif.pages.first
            assert page.is_contiguous
            assert page.planarconfig == PLANARCONFIG.SEPARATE
            assert page.photometric == PHOTOMETRIC.RGB
            assert page.imagewidth == 301
            assert page.imagelength == 219
            assert page.samplesperpixel == 3
            image = tif.asarray()
            assert_array_equal(data, image)
            assert_aszarr_method(tif, image)
            assert__str__(tif)


def test_write_extrasamples_contig_rgb2():
    """Test write contig RGB with large number of extrasamples."""
    data = random_data(numpy.uint8, (3, 219, 301))
    with TempFileName('write_extrasamples_contig_rgb2') as filename:
        imwrite(
            filename,
            data,
            photometric=PHOTOMETRIC.RGB,
            planarconfig=PLANARCONFIG.CONTIG,
        )
        assert_valid_tiff(filename)
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 1
            page = tif.pages.first
            assert page.is_contiguous
            assert page.planarconfig == PLANARCONFIG.CONTIG
            assert page.photometric == PHOTOMETRIC.RGB
            assert page.imagewidth == 219
            assert page.imagelength == 3
            assert page.samplesperpixel == 301
            assert len(page.extrasamples) == 301 - 3
            image = tif.asarray()
            assert_array_equal(data, image)
            assert_aszarr_method(tif, image)
            assert__str__(tif)
    # better save as planar
    with TempFileName('write_extrasamples_contig_rgb2_planar') as filename:
        imwrite(
            filename,
            data,
            photometric=PHOTOMETRIC.RGB,
            planarconfig=PLANARCONFIG.SEPARATE,
        )
        assert_valid_tiff(filename)
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 1
            page = tif.pages.first
            assert page.is_contiguous
            assert page.planarconfig == PLANARCONFIG.SEPARATE
            assert page.photometric == PHOTOMETRIC.RGB
            assert page.imagewidth == 301
            assert page.imagelength == 219
            assert page.samplesperpixel == 3
            image = tif.asarray()
            assert_array_equal(data, image)
            assert_aszarr_method(tif, image)
            assert__str__(tif)


def test_write_extrasamples_planar():
    """Test write planar large number of extrasamples."""
    data = random_data(numpy.uint8, (219, 301, 3))
    with TempFileName('write_extrasamples_planar') as filename:
        imwrite(filename, data, planarconfig=PLANARCONFIG.SEPARATE)
        assert_valid_tiff(filename)
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 1
            page = tif.pages.first
            assert page.is_contiguous
            assert page.planarconfig == PLANARCONFIG.SEPARATE
            assert page.photometric != PHOTOMETRIC.RGB
            assert page.imagewidth == 3
            assert page.imagelength == 301
            assert page.samplesperpixel == 219
            assert len(page.extrasamples) == 219 - 1
            image = tif.asarray()
            assert_array_equal(data, image)
            assert_aszarr_method(tif, image)
            assert__str__(tif)


def test_write_extrasamples_planar_rgb2():
    """Test write planar RGB with large number of extrasamples."""
    data = random_data(numpy.uint8, (219, 301, 3))
    with TempFileName('write_extrasamples_planar_rgb2') as filename:
        imwrite(
            filename,
            data,
            photometric=PHOTOMETRIC.RGB,
            planarconfig=PLANARCONFIG.SEPARATE,
        )
        assert_valid_tiff(filename)
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 1
            page = tif.pages.first
            assert page.is_contiguous
            assert page.planarconfig == PLANARCONFIG.SEPARATE
            assert page.photometric == PHOTOMETRIC.RGB
            assert page.imagewidth == 3
            assert page.imagelength == 301
            assert page.samplesperpixel == 219
            assert len(page.extrasamples) == 219 - 3
            image = tif.asarray()
            assert_array_equal(data, image)
            assert_aszarr_method(tif, image)
            assert__str__(tif)


def test_write_minisblack_planar():
    """Test write planar minisblack."""
    data = random_data(numpy.uint8, (3, 219, 301))
    with TempFileName('write_minisblack_planar') as filename:
        imwrite(filename, data, photometric=PHOTOMETRIC.MINISBLACK)
        assert_valid_tiff(filename)
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 3
            page = tif.pages.first
            assert page.is_contiguous
            assert page.planarconfig == PLANARCONFIG.CONTIG
            assert page.photometric != PHOTOMETRIC.RGB
            assert page.imagewidth == 301
            assert page.imagelength == 219
            assert page.samplesperpixel == 1
            image = tif.asarray()
            assert_array_equal(data, image)
            assert_aszarr_method(tif, image)
            assert__str__(tif)


def test_write_minisblack_contig():
    """Test write contig minisblack."""
    data = random_data(numpy.uint8, (219, 301, 3))
    with TempFileName('write_minisblack_contig') as filename:
        imwrite(filename, data, photometric=PHOTOMETRIC.MINISBLACK)
        assert_valid_tiff(filename)
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 219
            page = tif.pages.first
            assert page.is_contiguous
            assert page.planarconfig == PLANARCONFIG.CONTIG
            assert page.photometric != PHOTOMETRIC.RGB
            assert page.imagewidth == 3
            assert page.imagelength == 301
            assert page.samplesperpixel == 1
            image = tif.asarray()
            assert_array_equal(data, image)
            assert_aszarr_method(tif, image)
            assert__str__(tif)


def test_write_scalar():
    """Test write 2D grayscale."""
    data = random_data(numpy.uint8, (219, 301))
    with TempFileName('write_scalar') as filename:
        imwrite(filename, data)
        assert_valid_tiff(filename)
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 1
            page = tif.pages.first
            assert page.is_contiguous
            assert page.planarconfig == PLANARCONFIG.CONTIG
            assert page.photometric != PHOTOMETRIC.RGB
            assert page.imagewidth == 301
            assert page.imagelength == 219
            assert page.samplesperpixel == 1
            image = tif.asarray()
            assert_array_equal(data, image)
            assert_aszarr_method(tif, image)
            assert__str__(tif)


def test_write_scalar_3d():
    """Test write 3D grayscale."""
    data = random_data(numpy.uint8, (63, 219, 301))
    with TempFileName('write_scalar_3d') as filename:
        imwrite(filename, data)
        assert_valid_tiff(filename)
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 63
            page = tif.pages[62]
            assert page.is_contiguous
            assert page.planarconfig == PLANARCONFIG.CONTIG
            assert page.photometric != PHOTOMETRIC.RGB
            assert page.imagewidth == 301
            assert page.imagelength == 219
            assert page.samplesperpixel == 1
            image = tif.asarray()
            assert isinstance(image, numpy.ndarray)
            assert_array_equal(data, image)
            assert_aszarr_method(tif, image)
            assert__str__(tif)


def test_write_scalar_4d():
    """Test write 4D grayscale."""
    data = random_data(numpy.uint8, (3, 2, 219, 301))
    with TempFileName('write_scalar_4d') as filename:
        imwrite(filename, data)
        assert_valid_tiff(filename)
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 6
            page = tif.pages[5]
            assert page.is_contiguous
            assert page.planarconfig == PLANARCONFIG.CONTIG
            assert page.photometric != PHOTOMETRIC.RGB
            assert page.imagewidth == 301
            assert page.imagelength == 219
            assert page.samplesperpixel == 1
            image = tif.asarray()
            assert_array_equal(data, image)
            assert_aszarr_method(tif, image)
            assert__str__(tif)


def test_write_contig_extrasample():
    """Test write grayscale with contig extrasamples."""
    data = random_data(numpy.uint8, (219, 301, 2))
    with TempFileName('write_contig_extrasample') as filename:
        imwrite(filename, data, planarconfig=PLANARCONFIG.CONTIG)
        assert_valid_tiff(filename)
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 1
            page = tif.pages.first
            assert page.is_contiguous
            assert page.planarconfig == PLANARCONFIG.CONTIG
            assert page.photometric != PHOTOMETRIC.RGB
            assert page.imagewidth == 301
            assert page.imagelength == 219
            assert page.samplesperpixel == 2
            image = tif.asarray()
            assert_array_equal(data, image)
            assert_aszarr_method(tif, image)
            assert__str__(tif)


def test_write_planar_extrasample():
    """Test write grayscale with planar extrasamples."""
    data = random_data(numpy.uint8, (2, 219, 301))
    with TempFileName('write_planar_extrasample') as filename:
        imwrite(filename, data, planarconfig=PLANARCONFIG.SEPARATE)
        assert_valid_tiff(filename)
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 1
            page = tif.pages.first
            assert page.is_contiguous
            assert page.planarconfig == PLANARCONFIG.SEPARATE
            assert page.photometric != PHOTOMETRIC.RGB
            assert page.imagewidth == 301
            assert page.imagelength == 219
            assert page.samplesperpixel == 2
            image = tif.asarray()
            assert_array_equal(data, image)
            assert_aszarr_method(tif, image)
            assert__str__(tif)


def test_write_auto_rgb_contig():
    """Test write auto contig RGB."""
    data = random_data(numpy.uint8, (219, 301, 3))
    with TempFileName('write_auto_rgb_contig') as filename:
        imwrite(filename, data)  # photometric=RGB
        assert_valid_tiff(filename)
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 1
            page = tif.pages.first
            assert page.is_contiguous
            assert page.planarconfig == PLANARCONFIG.CONTIG
            assert page.photometric == PHOTOMETRIC.RGB
            assert page.imagewidth == 301
            assert page.imagelength == 219
            assert page.samplesperpixel == 3
            image = tif.asarray()
            assert_array_equal(data, image)
            assert_aszarr_method(tif, image)
            assert__str__(tif)


def test_write_auto_rgb_planar():
    """Test write auto planar RGB."""
    data = random_data(numpy.uint8, (3, 219, 301))
    with TempFileName('write_auto_rgb_planar') as filename:
        with pytest.warns(DeprecationWarning):
            imwrite(filename, data)  # photometric=RGB, planarconfig=SEPARATE
        assert_valid_tiff(filename)
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 1
            page = tif.pages.first
            assert page.is_contiguous
            assert page.planarconfig == PLANARCONFIG.SEPARATE
            assert page.photometric == PHOTOMETRIC.RGB
            assert page.imagewidth == 301
            assert page.imagelength == 219
            assert page.samplesperpixel == 3
            image = tif.asarray()
            assert_array_equal(data, image)
            assert_aszarr_method(tif, image)
            assert__str__(tif)


def test_write_auto_rgba_contig():
    """Test write auto contig RGBA."""
    data = random_data(numpy.uint8, (219, 301, 4))
    with TempFileName('write_auto_rgba_contig') as filename:
        imwrite(filename, data)  # photometric=RGB
        assert_valid_tiff(filename)
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 1
            page = tif.pages.first
            assert page.is_contiguous
            assert page.planarconfig == PLANARCONFIG.CONTIG
            assert page.photometric == PHOTOMETRIC.RGB
            assert page.imagewidth == 301
            assert page.imagelength == 219
            assert page.samplesperpixel == 4
            assert page.extrasamples[0] == EXTRASAMPLE.UNASSALPHA
            image = tif.asarray()
            assert_array_equal(data, image)
            assert_aszarr_method(tif, image)
            assert__str__(tif)


def test_write_auto_rgba_planar():
    """Test write auto planar RGBA."""
    data = random_data(numpy.uint8, (4, 219, 301))
    with TempFileName('write_auto_rgba_planar') as filename:
        with pytest.warns(DeprecationWarning):
            imwrite(filename, data)  # photometric=RGB, planarconfig=SEPARATE
        assert_valid_tiff(filename)
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 1
            page = tif.pages.first
            assert page.is_contiguous
            assert page.planarconfig == PLANARCONFIG.SEPARATE
            assert page.photometric == PHOTOMETRIC.RGB
            assert page.imagewidth == 301
            assert page.imagelength == 219
            assert page.samplesperpixel == 4
            assert page.extrasamples[0] == EXTRASAMPLE.UNASSALPHA
            image = tif.asarray()
            assert_array_equal(data, image)
            assert_aszarr_method(tif, image)
            assert__str__(tif)


def test_write_extrasamples_contig_rgb():
    """Test write contig RGB with extrasamples."""
    data = random_data(numpy.uint8, (219, 301, 8))
    with TempFileName('write_extrasamples_contig') as filename:
        imwrite(filename, data, photometric=PHOTOMETRIC.RGB)
        assert_valid_tiff(filename)
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 1
            page = tif.pages.first
            assert page.is_contiguous
            assert page.planarconfig == PLANARCONFIG.CONTIG
            assert page.photometric == PHOTOMETRIC.RGB
            assert page.imagewidth == 301
            assert page.imagelength == 219
            assert page.samplesperpixel == 8
            assert len(page.extrasamples) == 5
            assert page.extrasamples[0] == EXTRASAMPLE.UNSPECIFIED
            image = tif.asarray()
            assert_array_equal(data, image)
            assert_aszarr_method(tif, image)
            assert__str__(tif)


def test_write_extrasamples_planar_rgb():
    """Test write planar RGB with extrasamples."""
    data = random_data(numpy.uint8, (8, 219, 301))
    with TempFileName('write_extrasamples_planar') as filename:
        imwrite(filename, data, photometric=PHOTOMETRIC.RGB)
        assert_valid_tiff(filename)
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 1
            page = tif.pages.first
            assert page.is_contiguous
            assert page.planarconfig == PLANARCONFIG.SEPARATE
            assert page.photometric == PHOTOMETRIC.RGB
            assert page.imagewidth == 301
            assert page.imagelength == 219
            assert page.samplesperpixel == 8
            assert len(page.extrasamples) == 5
            assert page.extrasamples[0] == EXTRASAMPLE.UNSPECIFIED
            image = tif.asarray()
            assert_array_equal(data, image)
            assert_aszarr_method(tif, image)
            assert__str__(tif)


@pytest.mark.skipif(SKIP_CODECS, reason=REASON)
def test_write_iccprofile():
    """Test write RGB with ICC profile."""
    data = random_data(numpy.uint8, (219, 301, 3))
    iccprofile = imagecodecs.cms_profile('srgb')

    with TempFileName('write_iccprofile') as filename:
        imwrite(
            filename, data, photometric=PHOTOMETRIC.RGB, iccprofile=iccprofile
        )
        assert_valid_tiff(filename)
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 1
            page = tif.pages.first
            assert page.is_contiguous
            assert page.planarconfig == PLANARCONFIG.CONTIG
            assert page.photometric == PHOTOMETRIC.RGB
            assert page.imagewidth == 301
            assert page.imagelength == 219
            assert page.samplesperpixel == 3
            assert page.tags[34675].dtype == DATATYPE.UNDEFINED
            assert page.iccprofile == iccprofile
            imagecodecs.cms_profile_validate(page.iccprofile)
            tif.asarray()
            assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE or SKIP_CODECS, reason=REASON)
def test_write_cfa():
    """Test write uncompressed CFA image."""
    # TODO: write a valid TIFF/EP file
    data = imread(_file('DNG/cinemadng/M14-1451_000085_cDNG_uncompressed.dng'))
    extratags = [
        (271, 's', 4, 'Make', False),
        (272, 's', 5, 'Model', False),
        (33421, 'H', 2, (2, 2), False),  # CFARepeatPatternDim
        (33422, 'B', 4, b'\x00\x01\x01\x02', False),  # CFAPattern
        # (37398, 'B', 4, b'\x01\x00\x00\x00', False),  # TIFF/EPStandardID
        # (37399, 'H', 1, 0)  # SensingMethod Undefined
        # (50706, 'B', 4, b'\x01\x04\x00\x00', False),  # DNGVersion
    ]
    with TempFileName('write_cfa') as filename:
        imwrite(
            filename,
            data,
            photometric=PHOTOMETRIC.CFA,
            software='Tifffile',
            datetime=True,
            extratags=extratags,
        )
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 1
            page = tif.pages.first
            assert page.compression == 1
            assert page.photometric == PHOTOMETRIC.CFA
            assert page.imagewidth == 960
            assert page.imagelength == 540
            assert page.bitspersample == 16
            assert page.tags['CFARepeatPatternDim'].value == (2, 2)
            assert page.tags['CFAPattern'].value == b'\x00\x01\x01\x02'
            assert_array_equal(page.asarray(), data)
            assert_aszarr_method(page, data)


def test_write_tiled_compressed():
    """Test write compressed tiles."""
    data = random_data(numpy.uint8, (3, 219, 301))
    with TempFileName('write_tiled_compressed') as filename:
        imwrite(
            filename,
            data,
            photometric=PHOTOMETRIC.RGB,
            planarconfig=PLANARCONFIG.SEPARATE,
            compression=COMPRESSION.ADOBE_DEFLATE,
            compressionargs={'level': -1},
            tile=(96, 64),
        )
        assert_valid_tiff(filename)
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 1
            page = tif.pages.first
            assert page.is_tiled
            assert not page.is_contiguous
            assert page.planarconfig == PLANARCONFIG.SEPARATE
            assert page.photometric == PHOTOMETRIC.RGB
            assert page.imagewidth == 301
            assert page.imagelength == 219
            assert page.tilewidth == 64
            assert page.tilelength == 96
            assert page.samplesperpixel == 3
            image = tif.asarray()
            assert_array_equal(data, image)
            assert_aszarr_method(tif, image)
            assert__str__(tif)


def test_write_tiled():
    """Test write tiled."""
    data = random_data(numpy.uint16, (219, 301))
    with TempFileName('write_tiled') as filename:
        imwrite(filename, data, tile=(96, 64))
        assert_valid_tiff(filename)
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 1
            page = tif.pages.first
            assert page.is_tiled
            assert not page.is_contiguous
            assert page.planarconfig == PLANARCONFIG.CONTIG
            assert page.photometric != PHOTOMETRIC.RGB
            assert page.imagewidth == 301
            assert page.imagelength == 219
            assert page.tilewidth == 64
            assert page.tilelength == 96
            assert page.samplesperpixel == 1
            image = tif.asarray()
            assert_array_equal(data, image)
            assert_aszarr_method(tif, image)
            assert__str__(tif)


def test_write_tiled_planar():
    """Test write planar tiles."""
    data = random_data(numpy.uint8, (4, 219, 301))
    with TempFileName('write_tiled_planar') as filename:
        imwrite(
            filename,
            data,
            tile=(96, 64),
            photometric=PHOTOMETRIC.RGB,
            planarconfig=PLANARCONFIG.SEPARATE,
        )
        assert_valid_tiff(filename)
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 1
            page = tif.pages.first
            assert page.is_tiled
            assert not page.is_contiguous
            assert not page.is_volumetric
            assert page.planarconfig == PLANARCONFIG.SEPARATE
            assert page.photometric == PHOTOMETRIC.RGB
            assert page.imagewidth == 301
            assert page.imagelength == 219
            assert page.tilewidth == 64
            assert page.tilelength == 96
            assert page.samplesperpixel == 4
            image = tif.asarray()
            assert_array_equal(data, image)
            assert_aszarr_method(tif, image)
            assert__str__(tif)


def test_write_tiled_contig():
    """Test write contig tiles."""
    data = random_data(numpy.uint8, (219, 301, 3))
    with TempFileName('write_tiled_contig') as filename:
        imwrite(filename, data, tile=(96, 64), photometric=PHOTOMETRIC.RGB)
        assert_valid_tiff(filename)
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 1
            page = tif.pages.first
            assert page.is_tiled
            assert not page.is_contiguous
            assert page.planarconfig == PLANARCONFIG.CONTIG
            assert page.photometric == PHOTOMETRIC.RGB
            assert page.imagewidth == 301
            assert page.imagelength == 219
            assert page.tilewidth == 64
            assert page.tilelength == 96
            assert page.samplesperpixel == 3
            image = tif.asarray()
            assert_array_equal(data, image)
            assert_aszarr_method(tif, image)
            assert__str__(tif)


def test_write_tiled_pages():
    """Test write multiple tiled pages."""
    data = random_data(numpy.uint8, (5, 219, 301, 3))
    with TempFileName('write_tiled_pages') as filename:
        imwrite(filename, data, tile=(96, 64), photometric=PHOTOMETRIC.RGB)
        assert_valid_tiff(filename)
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 5
            page = tif.pages.first
            assert page.is_tiled
            assert not page.is_contiguous
            assert page.planarconfig == PLANARCONFIG.CONTIG
            assert page.photometric == PHOTOMETRIC.RGB
            assert not page.is_volumetric
            assert page.imagewidth == 301
            assert page.imagelength == 219
            assert page.tilewidth == 64
            assert page.tilelength == 96
            assert page.samplesperpixel == 3
            image = tif.asarray()
            assert_array_equal(data, image)
            assert_aszarr_method(tif, image)
            assert__str__(tif)


@pytest.mark.parametrize('compression', [1, 8])
def test_write_iter_tiles(compression):
    """Test write tiles from iterator."""
    data = random_data(numpy.uint16, (12, 16, 16))

    def tiles():
        for i in range(data.shape[0]):
            yield data[i]

    with TempFileName(f'write_iter_tiles_{compression}') as filename:
        with pytest.raises((StopIteration, RuntimeError)):
            # missing tiles
            imwrite(
                filename,
                tiles(),
                shape=(43, 81),
                tile=(16, 16),
                dtype=numpy.uint16,
                compression=compression,
            )

        with pytest.raises(ValueError):
            # missing parameters
            imwrite(filename, tiles(), compression=compression)

        with pytest.raises(ValueError):
            # missing parameters
            imwrite(filename, tiles(), shape=(43, 81), compression=compression)

        with pytest.raises(ValueError):
            # dtype mismatch
            imwrite(
                filename,
                tiles(),
                shape=(43, 61),
                tile=(16, 16),
                dtype=numpy.uint32,
                compression=compression,
            )

        with pytest.raises(ValueError):
            # shape mismatch
            imwrite(
                filename,
                tiles(),
                shape=(43, 61),
                tile=(8, 8),
                dtype=numpy.uint16,
                compression=compression,
            )

        imwrite(
            filename,
            tiles(),
            shape=(43, 61),
            tile=(16, 16),
            dtype=numpy.uint16,
            compression=compression,
        )

        with TiffFile(filename) as tif:
            page = tif.pages.first
            assert page.shape == (43, 61)
            assert page.tilelength == 16
            assert page.tilewidth == 16
            assert page.compression == compression
            image = page.asarray()
            assert_array_equal(image[:16, :16], data[0])
            for i, segment in enumerate(page.segments()):
                assert_array_equal(numpy.squeeze(segment[0]), data[i])


@pytest.mark.parametrize('compression', [1, 8])
def test_write_iter_tiles_separate(compression):
    """Test write separate tiles from iterator."""
    data = random_data(numpy.uint16, (24, 16, 16))

    def tiles():
        for i in range(data.shape[0]):
            yield data[i]

    with TempFileName(f'write_iter_tiles_separate_{compression}') as filename:
        imwrite(
            filename,
            tiles(),
            shape=(2, 43, 61),
            tile=(16, 16),
            dtype=numpy.uint16,
            planarconfig=PLANARCONFIG.SEPARATE,
            compression=compression,
        )

        with TiffFile(filename) as tif:
            assert len(tif.pages) == 1
            page = tif.pages.first
            assert page.shape == (2, 43, 61)
            assert page.tilelength == 16
            assert page.tilewidth == 16
            assert page.planarconfig == 2
            image = page.asarray()
            assert_array_equal(image[0, :16, :16], data[0])
            for i, segment in enumerate(page.segments()):
                assert_array_equal(numpy.squeeze(segment[0]), data[i])


@pytest.mark.parametrize('compression', [1, 8])
def test_write_iter_tiles_none(compression):
    """Test write tiles from iterator with missing tiles.

    Missing tiles are not with tileoffset=0 and tilebytecount=0.

    """
    data = random_data(numpy.uint16, (12, 16, 16))

    def tiles():
        for i in range(data.shape[0]):
            if i % 3 == 1:
                data[i] = 0
                yield None
            else:
                yield data[i]

    with TempFileName(f'write_iter_tiles_none_{compression}') as filename:
        imwrite(
            filename,
            tiles(),
            shape=(43, 61),
            tile=(16, 16),
            dtype=numpy.uint16,
            compression=compression,
        )
        with TiffFile(filename) as tif:
            page = tif.pages.first
            assert page.shape == (43, 61)
            assert page.tilelength == 16
            assert page.tilewidth == 16
            assert page.databytecounts[1] == 0
            assert page.dataoffsets[1] == 0
            image = page.asarray()
            assert_array_equal(image[:16, :16], data[0])
            for i, segment in enumerate(page.segments()):
                if i % 3 == 1:
                    assert segment[0] is None
                else:
                    assert_array_equal(numpy.squeeze(segment[0]), data[i])


@pytest.mark.parametrize('compression', [1, 8])
def test_write_iter_tiles_bytes(compression):
    """Test write tiles from iterator of bytes."""
    data = random_data(numpy.uint16, (5, 3, 15, 17))

    with TempFileName(f'write_iter_tiles_bytes_{compression}') as filename:
        imwrite(
            filename + 'f',
            data,
            tile=(16, 16),
            compression=compression,
            planarconfig='separate',
            photometric='rgb',
        )

        def tiles():
            with TiffFile(filename + 'f') as tif:
                fh = tif.filehandle
                for page in tif.pages:
                    for offset, bytecount in zip(
                        page.dataoffsets, page.databytecounts, strict=True
                    ):
                        fh.seek(offset)
                        strip = fh.read(bytecount)
                        yield strip

        imwrite(
            filename,
            tiles(),
            shape=data.shape,
            dtype=data.dtype,
            tile=(16, 16),
            compression=compression,
            planarconfig='separate',
            photometric='rgb',
        )
        assert_array_equal(imread(filename), data)


@pytest.mark.parametrize('compression', [1, 8])
@pytest.mark.parametrize('rowsperstrip', [5, 16])
def test_write_iter_strips_bytes(compression, rowsperstrip):
    """Test write strips from iterator of bytes."""
    data = random_data(numpy.uint16, (5, 3, 16, 16))

    with TempFileName(
        f'write_iter_strips_bytes_{compression}{rowsperstrip}'
    ) as filename:
        imwrite(
            filename + 'f',
            data,
            rowsperstrip=rowsperstrip,
            compression=compression,
            planarconfig='separate',
            photometric='rgb',
        )

        def strips():
            with TiffFile(filename + 'f') as tif:
                fh = tif.filehandle
                for page in tif.pages:
                    for offset, bytecount in zip(
                        page.dataoffsets, page.databytecounts, strict=True
                    ):
                        fh.seek(offset)
                        strip = fh.read(bytecount)
                        yield strip

        imwrite(
            filename,
            strips(),
            shape=data.shape,
            dtype=data.dtype,
            rowsperstrip=rowsperstrip,
            compression=compression,
            planarconfig='separate',
            photometric='rgb',
        )
        assert_array_equal(imread(filename), data)


@pytest.mark.parametrize('compression', [1, 8])
def test_write_iter_tiles_tuple(compression):
    """Test write tiles from iterator of tuple of bytes and bytecount."""
    # https://github.com/cgohlke/tifffile/issues/301
    data = random_data(numpy.uint16, (5, 3, 15, 17))
    align = 4096

    with TempFileName(f'write_iter_tiles_tuple_{compression}') as filename:
        imwrite(
            filename + 'f',
            data,
            tile=(16, 16),
            compression=compression,
            planarconfig='separate',
            photometric='rgb',
        )

        def tiles():
            with TiffFile(filename + 'f') as tif:
                fh = tif.filehandle
                for page in tif.pages:
                    for offset, bytecount in zip(
                        page.dataoffsets, page.databytecounts, strict=True
                    ):
                        fh.seek(offset)
                        strip = fh.read(bytecount)
                        strip += b'\x00' * (align - (len(strip) % align))
                        assert len(strip) % align == 0
                        yield strip, bytecount

        imwrite(
            filename,
            tiles(),
            shape=data.shape,
            dtype=data.dtype,
            tile=(16, 16),
            compression=compression,
            planarconfig='separate',
            photometric='rgb',
            align=align,
        )
        assert_array_equal(imread(filename), data)
        with TiffFile(filename) as tif:
            for page in tif.pages:
                assert page.is_tiled
                assert not page.is_contiguous
                for _offset in page.dataoffsets:
                    assert align % 4096 == 0


@pytest.mark.parametrize('compression', [1, 8])
@pytest.mark.parametrize('rowsperstrip', [5, 16])
def test_write_iter_pages_none(compression, rowsperstrip):
    """Test write pages from iterator with missing pages.

    Missing pages are written as zeros.

    """
    data = random_data(numpy.uint16, (12, 16, 16))

    def pages():
        for i in range(data.shape[0]):
            if i % 3 == 1:
                data[i] = 0
                yield None
            else:
                yield data[i]

    with TempFileName(
        f'write_iter_pages_none_{compression}{rowsperstrip}'
    ) as filename:
        imwrite(
            filename,
            pages(),
            shape=(12, 16, 16),
            dtype=numpy.uint16,
            rowsperstrip=rowsperstrip,
            compression=compression,
        )
        with TiffFile(filename) as tif:
            for i, page in enumerate(tif.pages):
                assert page.shape == (16, 16)
                assert page.rowsperstrip == rowsperstrip
                assert_array_equal(page.asarray(), data[i])
                for j, segment in enumerate(page.segments()):
                    assert_array_equal(
                        numpy.squeeze(segment[0]),
                        numpy.squeeze(
                            data[i, j * rowsperstrip : (j + 1) * rowsperstrip]
                        ),
                    )


def test_write_pyramids():
    """Test write two pyramids to shaped file."""
    data = random_data(numpy.uint8, (31, 64, 96, 3))
    with TempFileName('write_pyramids') as filename:
        with TiffWriter(filename) as tif:
            # use pages
            tif.write(data, tile=(16, 16), photometric=PHOTOMETRIC.RGB)
            # interrupt pyramid, for example thumbnail
            tif.write(data[0, :, :, 0])
            # pyramid levels
            tif.write(
                data[:, ::2, ::2],
                tile=(16, 16),
                subfiletype=FILETYPE.REDUCEDIMAGE,
                photometric=PHOTOMETRIC.RGB,
            )
            tif.write(
                data[:, ::4, ::4],
                tile=(16, 16),
                subfiletype=FILETYPE.REDUCEDIMAGE,
                photometric=PHOTOMETRIC.RGB,
            )
            # second pyramid using volumetric with downsampling factor 3
            tif.write(data, tile=(16, 16, 16), photometric=PHOTOMETRIC.RGB)
            tif.write(
                data[::3, ::3, ::3],
                tile=(16, 16, 16),
                subfiletype=FILETYPE.REDUCEDIMAGE,
                photometric=PHOTOMETRIC.RGB,
            )

        assert_valid_tiff(filename)

        with TiffFile(filename) as tif:
            assert len(tif.pages) == 3 * 31 + 2 + 1
            assert len(tif.series) == 3

            series = tif.series[0]
            assert series.kind == 'shaped'
            assert series.is_pyramidal
            assert len(series.levels) == 3
            assert len(series.levels[0].pages) == 31
            assert len(series.levels[1].pages) == 31
            assert len(series.levels[2].pages) == 31
            assert series.levels[0].shape == (31, 64, 96, 3)
            assert series.levels[1].shape == (31, 32, 48, 3)
            assert series.levels[2].shape == (31, 16, 24, 3)

            series = tif.series[1]
            assert series.kind == 'shaped'
            assert not series.is_pyramidal
            assert series.shape == (64, 96)

            series = tif.series[2]
            assert series.kind == 'shaped'
            assert series.is_pyramidal
            assert len(series.levels) == 2
            assert len(series.levels[0].pages) == 1
            assert len(series.levels[1].pages) == 1
            assert series.levels[0].keyframe.is_volumetric
            assert series.levels[1].keyframe.is_volumetric
            assert series.levels[0].shape == (31, 64, 96, 3)
            assert series.levels[1].shape == (11, 22, 32, 3)

            assert_array_equal(tif.asarray(), data)
            assert_array_equal(tif.asarray(series=0, level=0), data)
            assert_aszarr_method(tif, data, series=0, level=0)

            assert_array_equal(
                data[:, ::2, ::2], tif.asarray(series=0, level=1)
            )
            assert_aszarr_method(tif, data[:, ::2, ::2], series=0, level=1)

            assert_array_equal(
                data[:, ::4, ::4], tif.asarray(series=0, level=2)
            )
            assert_aszarr_method(tif, data[:, ::4, ::4], series=0, level=2)

            assert_array_equal(data[0, :, :, 0], tif.asarray(series=1))
            assert_aszarr_method(tif, data[0, :, :, 0], series=1)

            assert_array_equal(data, tif.asarray(series=2, level=0))
            assert_aszarr_method(tif, data, series=2, level=0)

            assert_array_equal(
                data[::3, ::3, ::3], tif.asarray(series=2, level=1)
            )
            assert_aszarr_method(tif, data[::3, ::3, ::3], series=2, level=1)

            assert__str__(tif)


def test_write_volumetric_tiled():
    """Test write tiled volume."""
    data = random_data(numpy.uint8, (253, 64, 96))
    with TempFileName('write_volumetric_tiled') as filename:
        imwrite(filename, data, tile=(64, 64, 64))
        assert_valid_tiff(filename)
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 1
            page = tif.pages.first
            assert page.is_volumetric
            assert page.is_tiled
            assert not page.is_contiguous
            assert page.planarconfig == PLANARCONFIG.CONTIG
            assert page.photometric != PHOTOMETRIC.RGB
            assert page.imagewidth == 96
            assert page.imagelength == 64
            assert page.imagedepth == 253
            assert page.tilewidth == 64
            assert page.tilelength == 64
            assert page.tiledepth == 64
            assert page.tile == (64, 64, 64)
            assert page.samplesperpixel == 1
            image = tif.asarray()
            assert_array_equal(data, image)
            assert_aszarr_method(tif, image)
            assert__str__(tif)


@pytest.mark.skipif(SKIP_CODECS, reason=REASON)
def test_write_volumetric_tiled_png():
    """Test write tiled volume using an image compressor."""
    data = random_data(numpy.uint8, (16, 64, 96, 3))
    with TempFileName('write_volumetric_tiled_png') as filename:
        imwrite(
            filename,
            data,
            tile=(1, 64, 64),
            photometric=PHOTOMETRIC.RGB,
            compression=COMPRESSION.PNG,
        )
        assert_valid_tiff(filename)
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 1
            page = tif.pages.first
            assert page.is_volumetric
            assert page.is_tiled
            assert page.compression == COMPRESSION.PNG
            assert not page.is_contiguous
            assert page.planarconfig == PLANARCONFIG.CONTIG
            assert page.photometric == PHOTOMETRIC.RGB
            assert page.imagewidth == 96
            assert page.imagelength == 64
            assert page.imagedepth == 16
            assert page.tilewidth == 64
            assert page.tilelength == 64
            assert page.tiledepth == 1
            assert page.samplesperpixel == 3
            image = tif.asarray()
            assert_array_equal(data, image)
            assert_aszarr_method(tif, image)
            assert__str__(tif)


def test_write_volumetric_tiled_planar_rgb():
    """Test write 5D array as RGB volumes."""
    shape = (2, 3, 256, 64, 96)
    data = numpy.empty(shape, dtype=numpy.uint8)
    data[:] = numpy.arange(256, dtype=numpy.uint8).reshape((1, 1, -1, 1, 1))
    with TempFileName('write_volumetric_tiled_planar_rgb') as filename:
        imwrite(
            filename,
            data,
            tile=(256, 64, 96),
            photometric=PHOTOMETRIC.RGB,
            planarconfig=PLANARCONFIG.SEPARATE,
        )
        assert_valid_tiff(filename)
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 2
            page = tif.pages.first
            assert page.is_volumetric
            assert page.is_tiled
            assert page.is_contiguous
            assert page.planarconfig == PLANARCONFIG.SEPARATE
            assert page.photometric == PHOTOMETRIC.RGB
            assert page.imagewidth == 96
            assert page.imagelength == 64
            assert page.imagedepth == 256
            assert page.tilewidth == 96
            assert page.tilelength == 64
            assert page.tiledepth == 256
            assert page.samplesperpixel == 3
            series = tif.series[0]
            assert series.kind == 'shaped'
            assert len(series._pages) == 1
            assert len(series.pages) == 2
            assert series.dataoffset is not None
            assert series.shape == shape
            image = tif.asarray()
            assert_array_equal(data, image)
            assert_aszarr_method(tif, image)
            assert__str__(tif)


def test_write_volumetric_tiled_planar_extrasamples():
    """Test write 5D array as grayscale volumes with extrasamples."""
    shape = (2, 3, 256, 64, 96)
    data = numpy.empty(shape, dtype=numpy.uint8)
    data[:] = numpy.arange(256, dtype=numpy.uint8).reshape((1, 1, -1, 1, 1))
    with TempFileName(
        'write_volumetric_tiled_planar_extrasamples'
    ) as filename:
        imwrite(
            filename,
            data,
            tile=(32, 32, 32),
            photometric=PHOTOMETRIC.MINISBLACK,
            planarconfig=PLANARCONFIG.SEPARATE,
        )
        assert_valid_tiff(filename)
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 2
            page = tif.pages.first
            assert page.is_volumetric
            assert page.is_tiled
            assert not page.is_contiguous
            assert page.planarconfig == PLANARCONFIG.SEPARATE
            assert page.photometric == PHOTOMETRIC.MINISBLACK
            assert page.imagewidth == 96
            assert page.imagelength == 64
            assert page.imagedepth == 256
            assert page.tilewidth == 32
            assert page.tilelength == 32
            assert page.tiledepth == 32
            assert page.samplesperpixel == 3
            assert len(page.extrasamples) == 2
            series = tif.series[0]
            assert series.kind == 'shaped'
            assert len(series._pages) == 2
            assert len(series.pages) == 2
            assert series.dataoffset is None
            assert series.shape == shape
            image = tif.asarray()
            assert_array_equal(data, image)
            assert_aszarr_method(tif, image)
            assert__str__(tif)


def test_write_volumetric_tiled_contig_extrasamples():
    """Test write 5D array as grayscale volumes with extrasamples."""
    shape = (2, 256, 64, 96, 3)
    data = numpy.empty(shape, dtype=numpy.uint8)
    data[:] = numpy.arange(256, dtype=numpy.uint8).reshape((1, -1, 1, 1, 1))
    with TempFileName(
        'write_volumetric_tiled_contig_extrasamples'
    ) as filename:
        imwrite(
            filename,
            data,
            tile=(32, 32, 32),
            photometric=PHOTOMETRIC.MINISBLACK,
            planarconfig=PLANARCONFIG.CONTIG,
        )
        assert_valid_tiff(filename)
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 2
            page = tif.pages.first
            assert page.is_volumetric
            assert page.is_tiled
            assert not page.is_contiguous
            assert page.planarconfig == PLANARCONFIG.CONTIG
            assert page.photometric == PHOTOMETRIC.MINISBLACK
            assert page.imagewidth == 96
            assert page.imagelength == 64
            assert page.imagedepth == 256
            assert page.tilewidth == 32
            assert page.tilelength == 32
            assert page.tiledepth == 32
            assert page.samplesperpixel == 3
            assert len(page.extrasamples) == 2
            series = tif.series[0]
            assert series.kind == 'shaped'
            assert len(series._pages) == 2
            assert len(series.pages) == 2
            assert series.dataoffset is None
            assert series.shape == shape
            image = tif.asarray()
            assert_array_equal(data, image)
            assert_aszarr_method(tif, image)
            assert__str__(tif)


def test_write_volumetric_tiled_contig_rgb():
    """Test write 6D array as contig RGB volumes."""
    shape = (2, 3, 256, 64, 96, 3)
    data = numpy.empty(shape, dtype=numpy.uint8)
    data[:] = numpy.arange(256, dtype=numpy.uint8).reshape((1, 1, -1, 1, 1, 1))
    with TempFileName('write_volumetric_tiled_contig_rgb') as filename:
        imwrite(
            filename, data, tile=(256, 64, 96), photometric=PHOTOMETRIC.RGB
        )
        assert_valid_tiff(filename)
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 6
            page = tif.pages.first
            assert page.is_volumetric
            assert page.is_tiled
            assert page.is_contiguous
            assert page.planarconfig == PLANARCONFIG.CONTIG
            assert page.photometric == PHOTOMETRIC.RGB
            assert page.imagewidth == 96
            assert page.imagelength == 64
            assert page.imagedepth == 256
            assert page.tilewidth == 96
            assert page.tilelength == 64
            assert page.tiledepth == 256
            assert page.samplesperpixel == 3
            # self.assertEqual(page.tags['TileOffsets'].value, (352,))
            assert page.tags['TileByteCounts'].value == (4718592,)
            series = tif.series[0]
            assert series.kind == 'shaped'
            assert len(series._pages) == 1
            assert len(series.pages) == 6
            assert series.dataoffset is not None
            assert series.shape == shape
            image = tif.asarray()
            assert_array_equal(data, image)
            # assert iterating over series.pages
            data = data.reshape((6, 256, 64, 96, 3))
            for i, page in enumerate(series.pages):
                image = page.asarray()
                assert_array_equal(data[i], image)
                assert_aszarr_method(page, image)
            assert__str__(tif)


@pytest.mark.skipif(SKIP_LARGE, reason=REASON)
def test_write_volumetric_tiled_contig_rgb_empty():
    """Test write empty 6D array as contig RGB volumes."""
    shape = (2, 3, 256, 64, 96, 3)
    with TempFileName('write_volumetric_tiled_contig_rgb_empty') as filename:
        with TiffWriter(filename) as tif:
            tif.write(
                shape=shape,
                dtype=numpy.uint8,
                tile=(256, 64, 96),
                photometric=PHOTOMETRIC.RGB,
            )
        assert_valid_tiff(filename)
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 6
            page = tif.pages.first
            assert page.is_volumetric
            assert page.is_tiled
            assert page.is_contiguous
            assert page.planarconfig == PLANARCONFIG.CONTIG
            assert page.photometric == PHOTOMETRIC.RGB
            assert page.imagewidth == 96
            assert page.imagelength == 64
            assert page.imagedepth == 256
            assert page.tilewidth == 96
            assert page.tilelength == 64
            assert page.tiledepth == 256
            assert page.samplesperpixel == 3
            # self.assertEqual(page.tags['TileOffsets'].value, (352,))
            assert page.tags['TileByteCounts'].value == (4718592,)
            series = tif.series[0]
            assert series.kind == 'shaped'
            assert len(series._pages) == 1
            assert len(series.pages) == 6
            assert series.dataoffset is not None
            assert series.shape == shape
            image = tif.asarray()
            assert_array_equal(image.shape, shape)
            assert_aszarr_method(tif, image)
            assert__str__(tif)


def test_write_volumetric_striped():
    """Test write striped volume."""
    data = random_data(numpy.uint8, (15, 63, 95))
    with TempFileName('write_volumetric_striped') as filename:
        imwrite(filename, data, volumetric=True)
        assert_valid_tiff(filename)
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 1
            page = tif.pages.first
            assert page.is_volumetric
            assert not page.is_tiled
            assert page.is_contiguous
            assert page.planarconfig == PLANARCONFIG.CONTIG
            assert page.photometric != PHOTOMETRIC.RGB
            assert page.imagewidth == 95
            assert page.imagelength == 63
            assert page.imagedepth == 15
            assert len(page.dataoffsets) == 15
            assert len(page.databytecounts) == 15
            assert page.samplesperpixel == 1
            image = tif.asarray()
            assert_array_equal(data, image)
            assert_aszarr_method(tif, image)
            assert__str__(tif)


@pytest.mark.skipif(SKIP_CODECS, reason=REASON)
def test_write_volumetric_striped_png():
    """Test write tiled volume using an image compressor."""
    data = random_data(numpy.uint8, (15, 63, 95, 3))
    with TempFileName('write_volumetric_striped_png') as filename:
        imwrite(
            filename,
            data,
            photometric=PHOTOMETRIC.RGB,
            volumetric=True,
            rowsperstrip=32,
            compression=COMPRESSION.PNG,
        )
        assert_valid_tiff(filename)
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 1
            page = tif.pages.first
            assert page.is_volumetric
            assert not page.is_tiled
            assert page.compression == COMPRESSION.PNG
            assert not page.is_contiguous
            assert page.planarconfig == PLANARCONFIG.CONTIG
            assert page.photometric == PHOTOMETRIC.RGB
            assert page.imagewidth == 95
            assert page.imagelength == 63
            assert page.imagedepth == 15
            assert page.samplesperpixel == 3
            assert len(page.dataoffsets) == 30
            assert len(page.databytecounts) == 30
            image = tif.asarray()
            assert_array_equal(data, image)
            assert_aszarr_method(tif, image)
            assert_aszarr_method(tif, image, chunkmode='page')
            assert__str__(tif)


def test_write_volumetric_striped_planar_rgb():
    """Test write 5D array as grayscale volumes."""
    shape = (2, 3, 15, 63, 96)
    data = numpy.empty(shape, dtype=numpy.uint8)
    data[:] = numpy.arange(15, dtype=numpy.uint8).reshape((1, 1, -1, 1, 1))
    with TempFileName('write_volumetric_striped_planar_rgb') as filename:
        imwrite(filename, data, volumetric=True, photometric=PHOTOMETRIC.RGB)
        assert_valid_tiff(filename)
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 2
            page = tif.pages.first
            assert page.is_volumetric
            assert not page.is_tiled
            assert page.is_contiguous
            assert page.planarconfig == PLANARCONFIG.SEPARATE
            assert page.photometric == PHOTOMETRIC.RGB
            assert page.imagewidth == 96
            assert page.imagelength == 63
            assert page.imagedepth == 15
            assert page.samplesperpixel == 3
            assert len(page.dataoffsets) == 15 * 3
            assert len(page.databytecounts) == 15 * 3
            series = tif.series[0]
            assert series.kind == 'shaped'
            assert len(series._pages) == 1
            assert len(series.pages) == 2
            assert series.dataoffset is not None
            assert series.shape == shape
            image = tif.asarray()
            assert_array_equal(data, image)
            assert_aszarr_method(tif, image)
            assert__str__(tif)


def test_write_volumetric_striped_contig_rgb():
    """Test write 6D array as contig RGB volumes."""
    shape = (2, 3, 15, 63, 95, 3)
    data = numpy.empty(shape, dtype=numpy.uint8)
    data[:] = numpy.arange(15, dtype=numpy.uint8).reshape((1, 1, -1, 1, 1, 1))
    with TempFileName('write_volumetric_striped_contig_rgb') as filename:
        imwrite(filename, data, volumetric=True, photometric=PHOTOMETRIC.RGB)
        assert_valid_tiff(filename)
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 6
            page = tif.pages.first
            assert page.is_volumetric
            assert not page.is_tiled
            assert page.is_contiguous
            assert page.planarconfig == PLANARCONFIG.CONTIG
            assert page.photometric == PHOTOMETRIC.RGB
            assert page.imagewidth == 95
            assert page.imagelength == 63
            assert page.imagedepth == 15
            assert page.samplesperpixel == 3
            assert len(page.dataoffsets) == 15
            assert len(page.databytecounts) == 15
            series = tif.series[0]
            assert series.kind == 'shaped'
            assert len(series._pages) == 1
            assert len(series.pages) == 6
            assert series.dataoffset is not None
            assert series.shape == shape
            image = tif.asarray()
            assert_array_equal(data, image)
            # assert iterating over series.pages
            data = data.reshape((6, 15, 63, 95, 3))
            for i, page in enumerate(series.pages):
                image = page.asarray()
                assert_array_equal(data[i], image)
                assert_aszarr_method(page, image)
            assert__str__(tif)


@pytest.mark.skipif(SKIP_LARGE, reason=REASON)
def test_write_volumetric_striped_contig_rgb_empty():
    """Test write empty 6D array as contig RGB volumes."""
    shape = (2, 3, 15, 63, 95, 3)
    with TempFileName('write_volumetric_striped_contig_rgb_empty') as filename:
        with TiffWriter(filename) as tif:
            tif.write(
                shape=shape,
                dtype=numpy.uint8,
                volumetric=True,
                photometric=PHOTOMETRIC.RGB,
            )
        assert_valid_tiff(filename)
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 6
            page = tif.pages.first
            assert page.is_volumetric
            assert not page.is_tiled
            assert page.is_contiguous
            assert page.planarconfig == PLANARCONFIG.CONTIG
            assert page.photometric == PHOTOMETRIC.RGB
            assert page.imagewidth == 95
            assert page.imagelength == 63
            assert page.imagedepth == 15
            assert page.samplesperpixel == 3
            assert len(page.dataoffsets) == 15
            assert len(page.databytecounts) == 15
            series = tif.series[0]
            assert series.kind == 'shaped'
            assert len(series._pages) == 1
            assert len(series.pages) == 6
            assert series.dataoffset is not None
            assert series.shape == shape
            image = tif.asarray()
            assert_array_equal(image.shape, shape)
            assert_aszarr_method(tif, image)
            assert__str__(tif)


def test_write_contiguous():
    """Test contiguous mode."""
    data = random_data(numpy.uint8, (5, 4, 219, 301, 3))
    with TempFileName('write_contiguous') as filename:
        with TiffWriter(filename, bigtiff=True) as tif:
            for i in range(data.shape[0]):
                tif.write(
                    data[i], contiguous=True, photometric=PHOTOMETRIC.RGB
                )
        # assert_jhove(filename)
        with TiffFile(filename) as tif:
            assert tif.is_bigtiff
            assert len(tif.pages) == 20
            # check metadata is updated in-place
            assert tif.pages.first.tags[270].valueoffset < tif.pages[1].offset
            for page in tif.pages:
                assert page.is_contiguous
                assert page.planarconfig == PLANARCONFIG.CONTIG
                assert page.photometric == PHOTOMETRIC.RGB
                assert page.imagewidth == 301
                assert page.imagelength == 219
                assert page.samplesperpixel == 3
            image = tif.asarray()
            assert_array_equal(data, image)
            assert_aszarr_method(tif, image)
            assert__str__(tif)


@pytest.mark.skipif(SKIP_LARGE, reason=REASON)
def test_write_3gb():
    """Test write 3 GB non-BigTIFF file."""
    # https://github.com/blink1073/tifffile/issues/47
    data = numpy.empty((4096 - 32, 1024, 1024), dtype=numpy.uint8)
    with TempFileName('write_3gb') as filename:
        imwrite(filename, data)
        del data
        assert_valid_tiff(filename)
        # assert file
        with TiffFile(filename) as tif:
            assert not tif.is_bigtiff


@pytest.mark.skipif(SKIP_LARGE, reason=REASON)
def test_write_6gb():
    """Test write 6 GB non-BigTIFF file."""
    # https://stackoverflow.com/questions/74930263
    data = numpy.empty((2**16, 2**15, 3), dtype=numpy.uint8)
    with TempFileName('write_6gb') as filename:
        imwrite(
            filename,
            data,
            bigtiff=False,
            photometric='rgb',
            rowsperstrip=2**15,
        )
        del data
        assert_valid_tiff(filename)
        # assert file
        with TiffFile(filename) as tif:
            assert not tif.is_bigtiff
            assert tif.pages.first.dataoffsets[1] > 2**16
            assert tif.pages.first.databytecounts[1] == 3221225472
            # image = tif.asarray()
        # assert_array_equal(data, image)


@pytest.mark.skipif(SKIP_LARGE, reason=REASON)
def test_write_5gb_fails():
    """Test data too large for non-BigTIFF file."""
    # TiffWriter should fail without bigtiff parameter
    data = numpy.empty((640, 1024, 1024), dtype=numpy.float64)
    data[:] = numpy.arange(640, dtype=numpy.float64).reshape((-1, 1, 1))
    with TempFileName('write_5gb_fails') as filename:  # noqa: SIM117
        with pytest.raises(ValueError):
            with TiffWriter(filename) as tif:
                tif.write(data)
    # TODO: test 'unclosed file' not in capsys


@pytest.mark.skipif(SKIP_LARGE, reason=REASON)
def test_write_5gb_bigtiff():
    """Test write 5GB BigTiff file."""
    data = numpy.empty((640, 1024, 1024), dtype=numpy.float64)
    data[:] = numpy.arange(640, dtype=numpy.float64).reshape((-1, 1, 1))
    with TempFileName('write_5gb_bigtiff') as filename:
        # imwrite should use bigtiff for large data
        imwrite(filename, data)
        # assert_jhove(filename)
        # assert file
        with TiffFile(filename) as tif:
            assert tif.is_bigtiff
            assert len(tif.pages) == 640
            page = tif.pages.first
            assert page.is_contiguous
            assert page.planarconfig == PLANARCONFIG.CONTIG
            assert page.photometric != PHOTOMETRIC.RGB
            assert page.imagewidth == 1024
            assert page.imagelength == 1024
            assert page.samplesperpixel == 1
            image = tif.asarray(out='memmap')
            assert_array_equal(data, image)
            del image
            del data
            assert__str__(tif)


@pytest.mark.parametrize('compression', [0, 6])
@pytest.mark.parametrize('dtype', [numpy.uint8, numpy.uint16])
def test_write_palette(dtype, compression):
    """Test write palette images."""
    dtype = numpy.dtype(dtype)
    data = random_data(dtype, (3, 219, 301))
    cmap = random_data(numpy.uint16, (3, 2 ** (data.itemsize * 8)))
    with TempFileName(f'write_palette_{compression}{dtype}') as filename:
        imwrite(
            filename,
            data,
            colormap=cmap,
            compression=COMPRESSION.ADOBE_DEFLATE if compression else None,
            compressionargs={'level': compression} if compression else None,
        )
        assert_valid_tiff(filename)
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 3
            page = tif.pages.first
            assert page.is_contiguous != bool(compression)
            assert page.planarconfig == PLANARCONFIG.CONTIG
            assert page.photometric == PHOTOMETRIC.PALETTE
            assert page.imagewidth == 301
            assert page.imagelength == 219
            assert page.samplesperpixel == 1
            for i, page in enumerate(tif.pages):
                assert_array_equal(apply_colormap(data[i], cmap), page.asrgb())
            assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_write_palette_django():
    """Test write palette read from existing file."""
    filename = _file('django.tiff')
    with TiffFile(filename) as tif:
        page = tif.pages.first
        assert page.photometric == PHOTOMETRIC.PALETTE
        assert page.imagewidth == 320
        assert page.imagelength == 480
        data = page.asarray()  # .squeeze()  # UserWarning ...
        cmap = page.colormap
        assert__str__(tif)
    with TempFileName('write_palette_django') as filename:
        imwrite(
            filename,
            data,
            colormap=cmap,
            compression=COMPRESSION.ADOBE_DEFLATE,
        )
        assert_valid_tiff(filename)
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 1
            page = tif.pages.first
            assert not page.is_contiguous
            assert page.planarconfig == PLANARCONFIG.CONTIG
            assert page.photometric == PHOTOMETRIC.PALETTE
            assert page.imagewidth == 320
            assert page.imagelength == 480
            assert page.samplesperpixel == 1
            image = page.asrgb(uint8=False)
            assert_array_equal(apply_colormap(data, cmap), image)
            assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_write_multiple_series():
    """Test write multiple data into one file using various options."""
    data1 = imread(_file('ome/multi-channel-4D-series.ome.tif'))
    image1 = imread(_file('django.tiff'))
    image2 = imread(_file('horse-16bit-col-littleendian.tif'))
    with TempFileName('write_multiple_series') as filename:
        with TiffWriter(filename, bigtiff=False) as tif:
            # series 0
            tif.write(
                image1,
                compression=COMPRESSION.ADOBE_DEFLATE,
                compressionargs={'level': 5},
                description='Django',
            )
            # series 1
            tif.write(image2, photometric=PHOTOMETRIC.RGB)
            # series 2
            tif.write(data1[0], metadata={'axes': 'TCZYX'})
            for i in range(1, data1.shape[0]):
                tif.write(data1[i], contiguous=True)
            # series 3
            tif.write(data1[0], contiguous=False)
            # series 4
            tif.write(data1[0, 0, 0], tile=(64, 64))
            # series 5
            tif.write(
                image1,
                compression=COMPRESSION.ADOBE_DEFLATE,
                description='DEFLATE',
            )
        assert_valid_tiff(filename)
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 124
            assert len(tif.series) == 6
            series = tif.series[0]
            assert not series.dataoffset
            assert series.axes == 'YX'
            assert series.kind == 'shaped'
            assert_array_equal(image1, series.asarray())
            assert_aszarr_method(series, image1)
            series = tif.series[1]
            assert series.dataoffset
            assert series.axes == 'YXS'
            assert series.kind == 'shaped'
            assert_array_equal(image2, series.asarray())
            assert_aszarr_method(series, image2)
            series = tif.series[2]
            assert series.dataoffset
            assert series.keyframe.is_contiguous
            assert len(series) == 105
            assert len(series.pages) == 105
            assert isinstance(series[104], TiffPage)
            assert len(series[range(105)]) == 105
            assert len(series[slice(0, None, 2)]) == 53
            with pytest.raises(TypeError):
                assert series['1']
            assert series.axes == 'TCZYX'
            assert series.kind == 'shaped'
            result = series.asarray(out='memmap')
            assert_array_equal(data1, result)
            assert_aszarr_method(series, data1)
            assert tif.filehandle.path == result.filename
            del result
            series = tif.series[3]
            assert series.dataoffset
            assert series.axes == 'QQYX'
            assert series.kind == 'shaped'
            assert_array_equal(data1[0], series.asarray())
            assert_aszarr_method(series, data1[0])
            series = tif.series[4]
            assert not series.dataoffset
            assert series.axes == 'YX'
            assert series.kind == 'shaped'
            assert_array_equal(data1[0, 0, 0], series.asarray())
            assert_aszarr_method(series, data1[0, 0, 0])
            series = tif.series[5]
            assert not series.dataoffset
            assert series.axes == 'YX'
            assert series.kind == 'shaped'
            assert_array_equal(image1, series.asarray())
            assert_aszarr_method(series, image1)
            assert__str__(tif)

            # test TiffFile.asarray key and series parameters
            assert_array_equal(image1, tif.asarray(key=0))
            assert_array_equal(image1, tif.asarray(key=-1))

            assert_array_equal(image2, tif.asarray(key=[1]))
            assert_array_equal(image2, tif.asarray(key=0, series=1))
            assert_array_equal(
                image2, tif.asarray(key=0, series=tif.series[1])
            )

            assert_array_equal(
                data1, tif.asarray(key=range(2, 107)).reshape(data1.shape)
            )

            assert_array_equal(
                data1,
                tif.asarray(key=range(105), series=2).reshape(data1.shape),
            )

            assert_array_equal(
                data1,
                tif.asarray(key=slice(None), series=2).reshape(data1.shape),
            )

            assert_array_equal(
                data1[0],
                tif.asarray(key=slice(107, 122)).reshape(data1[0].shape),
            )

            assert_array_equal(
                data1[0].reshape((-1, 167, 439))[::2],
                tif.asarray(key=slice(107, 122, 2)).reshape((-1, 167, 439)),
            )

            with pytest.raises(RuntimeError):
                tif.asarray(key=[0, 1])

            with pytest.raises(RuntimeError):
                tif.asarray(key=[-3, -2])

        assert_array_equal(image1, imread(filename, key=0))
        assert_array_equal(image1, imread(filename, key=-1))
        assert_array_equal(image2, imread(filename, key=[1]))
        assert_array_equal(
            data1, imread(filename, key=range(2, 107)).reshape(data1.shape)
        )
        assert_array_equal(
            data1,
            imread(filename, key=range(105), series=2).reshape(data1.shape),
        )
        assert_array_equal(
            data1[0],
            imread(filename, key=slice(107, 122)).reshape(data1[0].shape),
        )


@pytest.mark.skipif(
    SKIP_CODECS or not imagecodecs.PNG.available, reason=REASON
)
def test_write_multithreaded():
    """Test write large tiled multithreaded."""
    data = (
        numpy.arange(4001 * 6003 * 3)
        .astype(numpy.uint8)
        .reshape((4001, 6003, 3))
    )
    with TempFileName('write_multithreaded') as filename:
        imwrite(
            filename, data, tile=(512, 512), compression='PNG', maxworkers=6
        )
        # assert_valid_tiff(filename)
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 1
            page = tif.pages.first
            assert not page.is_contiguous
            assert page.compression == COMPRESSION.PNG
            assert page.planarconfig == PLANARCONFIG.CONTIG
            assert page.imagewidth == 6003
            assert page.imagelength == 4001
            assert page.samplesperpixel == 3
            image = tif.asarray(maxworkers=6)
            assert_array_equal(data, image)
            assert_aszarr_method(tif, image)
            assert__str__(tif)


@pytest.mark.skipif(SKIP_ZARR, reason=REASON)
def test_write_zarr():
    """Test write to TIFF via Zarr interface."""
    with TempFileName('write_zarr', ext='.ome.tif') as filename:
        with TiffWriter(filename, bigtiff=True) as tif:
            tif.write(
                shape=(7, 5, 252, 244),
                dtype=numpy.uint16,
                tile=(64, 64),
                subifds=2,
            )
            tif.write(
                shape=(7, 5, 126, 122), dtype=numpy.uint16, tile=(64, 64)
            )
            tif.write(shape=(7, 5, 63, 61), dtype=numpy.uint16, tile=(32, 32))
            tif.write(
                shape=(3, 252, 244),
                dtype=numpy.uint8,
                photometric='RGB',
                planarconfig='SEPARATE',
                rowsperstrip=63,
            )
            tif.write(
                shape=(252, 244, 3),
                dtype=numpy.uint8,
                photometric='RGB',
                rowsperstrip=64,
            )
            tif.write(
                numpy.zeros((252, 244, 3), numpy.uint8),
                photometric='RGB',
                rowsperstrip=252,
                compression='zlib',
            )

        with TiffFile(filename, mode='r+') as tif:
            with tif.series[0].aszarr() as store:
                z = zarr.open(store, mode='r+')
                z['0'][2, 2:3, 100:111, 100:200] = 100
                z['1'][3, 3:4, 100:111, 100:] = 101
                z['2'][4, 4:5, 33:40, 41:] = 102
            assert tif.asarray(series=0)[2, 2, 100, 199] == 100
            assert tif.asarray(series=0, level=1)[3, 3, 100, 121] == 101
            assert tif.asarray(series=0, level=2)[4, 4, 33, 41] == 102

        with TiffFile(filename, mode='r+') as tif:
            with tif.series[1].aszarr() as store:
                z = zarr.open(store, mode='r+')
                z[1, 100:111, 100:200] = 104
            assert tif.series[1].asarray()[1, 100, 199] == 104

        with TiffFile(filename, mode='r+') as tif:
            with tif.series[2].aszarr() as store:
                z = zarr.open(store, mode='r+')
                z[200:, 20:, 1] = 105
            assert tif.series[2].asarray()[251, 243, 1] == 105

        # compressed series cannot be written to
        with (  # noqa: SIM117
            TiffFile(filename, mode='r+') as tif,
            tif.series[3].aszarr() as store,
        ):
            with pytest.raises(ValueError):
                z = zarr.open(store, mode='r+')


@pytest.mark.skipif(
    SKIP_HTTP or SKIP_ZARR or SKIP_CODECS or not imagecodecs.JPEG.available,
    reason=REASON,
)
@pytest.mark.parametrize('byteorder', ['<', '>'])
@pytest.mark.parametrize('version', [0, 1])
def test_write_fsspec(version, byteorder):
    """Test write fsspec for multi-series OME-TIFF."""
    from imagecodecs.numcodecs import register_codecs

    register_codecs('imagecodecs_jpeg', verbose=False)
    register_codecs('imagecodecs_delta', verbose=False)
    register_codecs('imagecodecs_floatpred', verbose=False)

    data0 = random_data(numpy.uint8, (3, 252, 244))
    data1 = random_data(numpy.uint8, (219, 301, 3))
    data2 = random_data(numpy.uint16, (3, 219, 301))
    data3 = random_data(numpy.float32, (210, 301))

    bo = {'>': 'be', '<': 'le'}[byteorder]

    with TempFileName(
        f'write_fsspec_v{version}_{bo}', ext='.ome.tif'
    ) as filepath:
        filename = os.path.split(filepath)[-1]
        with TiffWriter(filepath, ome=True, byteorder=byteorder) as tif:
            # series 0
            options = {
                'tile': (64, 64),
                'photometric': PHOTOMETRIC.MINISBLACK,
                'compression': COMPRESSION.DEFLATE,
                'predictor': PREDICTOR.HORIZONTAL,
            }
            tif.write(data0, subifds=2, **options)
            tif.write(data0[:, ::2, ::2], subfiletype=1, **options)
            tif.write(data0[:, ::4, ::4], subfiletype=1, **options)
            # series 1
            tif.write(
                data1, photometric=PHOTOMETRIC.RGB, rowsperstrip=data1.shape[0]
            )
            # series 2
            tif.write(
                data2,
                rowsperstrip=data1.shape[1],
                photometric=PHOTOMETRIC.RGB,
                planarconfig=PLANARCONFIG.SEPARATE,
                compression=COMPRESSION.DEFLATE,
                predictor=PREDICTOR.HORIZONTAL,
            )
            # series 3
            tif.write(data1, photometric=PHOTOMETRIC.RGB, rowsperstrip=5)
            # series 4
            tif.write(
                data1,
                photometric=PHOTOMETRIC.RGB,
                tile=(32, 32),
                compression=COMPRESSION.JPEG,
            )
            # series 5
            tif.write(
                data3,
                rowsperstrip=105,
                photometric=PHOTOMETRIC.MINISBLACK,
                compression=COMPRESSION.DEFLATE,
                predictor=PREDICTOR.FLOATINGPOINT,
            )

        with TiffFile(filepath) as tif:
            assert tif.is_ome
            assert len(tif.series) == 6

            # TODO: clean up temp JSON files
            with tif.series[0].aszarr() as store:
                assert store.is_multiscales
                store.write_fsspec(
                    filepath + f'.v{version}.s0.json', URL, version=version
                )
            assert_array_equal(tif.series[0].asarray(), data0)
            assert_fsspec(URL + filename + f'.v{version}.s0.json', data0)

            with tif.series[1].aszarr() as store:
                assert not store.is_multiscales
                store.write_fsspec(
                    filepath + f'.v{version}.s1.json', URL, version=version
                )
            assert_array_equal(tif.series[1].asarray(), data1)
            assert_fsspec(URL + filename + f'.v{version}.s1.json', data1)

            with tif.series[2].aszarr() as store:
                store.write_fsspec(
                    filepath + f'.v{version}.s2.json', URL, version=version
                )
            assert_array_equal(tif.series[2].asarray(), data2)
            assert_fsspec(URL + filename + f'.v{version}.s2.json', data2)

            with tif.series[3].aszarr(chunkmode=2) as store:
                store.write_fsspec(
                    filepath + f'.v{version}.s3.json', URL, version=version
                )
            assert_array_equal(tif.series[3].asarray(), data1)
            assert_fsspec(URL + filename + f'.v{version}.s3.json', data1)

            with tif.series[3].aszarr() as store:  # noqa: SIM117
                with pytest.raises(ValueError):
                    # imagelength % rowsperstrip != 0
                    store.write_fsspec(
                        filepath + f'.v{version}.s3fail.json',
                        URL,
                        version=version,
                    )

            with tif.series[4].aszarr() as store:
                store.write_fsspec(
                    filepath + f'.v{version}.s4.json', URL, version=version
                )
            assert_fsspec(
                URL + filename + f'.v{version}.s4.json',
                tif.series[4].asarray(),
            )

            with tif.series[5].aszarr() as store:
                store.write_fsspec(
                    filepath + f'.v{version}.s5.json', URL, version=version
                )
            assert_array_equal(tif.series[5].asarray(), data3)
            assert_fsspec(URL + filename + f'.v{version}.s5.json', data3)


@pytest.mark.skipif(SKIP_FILE or SKIP_ZARR, reason=REASON)
@pytest.mark.parametrize('version', [0, 1])
@pytest.mark.parametrize('chunkmode', [0, 2])
def test_write_fsspec_multifile(version, chunkmode):
    """Test write fsspec for multi-file OME series."""
    filename = _file('OME/multifile/multifile-Z1.ome.tiff')
    url = os.path.dirname(filename).replace('\\', '/')
    with TempFileName(
        f'write_fsspec_multifile_{version}{chunkmode}', ext='.json'
    ) as jsonfile:
        # write to file handle
        with (
            open(jsonfile, 'w', encoding='utf-8') as fh,
            TiffFile(filename) as tif,
        ):
            data = tif.series[0].asarray()
            with tif.series[0].aszarr(chunkmode=chunkmode) as store:
                store.write_fsspec(
                    fh, url=url, version=version, templatename='f'
                )
        zobj = zarr.open(refs_as_store(jsonfile), zarr_format=2)
        assert_array_equal(zobj[:], data)


@pytest.mark.skipif(
    SKIP_FILE or SKIP_LARGE or SKIP_CODECS or SKIP_ZARR,
    reason=REASON,
)
@pytest.mark.parametrize('version', [1])  # 0,
def test_write_fsspec_sequence(version):
    """Test write fsspec for multi-file sequence."""
    # https://bbbc.broadinstitute.org/BBBC006
    categories = {'p': {chr(i + 97): i for i in range(25)}}
    ptrn = r'(?:_(z)_(\d+)).*_(?P<p>[a-z])(?P<a>\d+)(?:_(s)(\d))(?:_(w)(\d))'
    filenames = _files('BBBC/BBBC006_v1_images_z_00/*.tif')
    filenames += _files('BBBC/BBBC006_v1_images_z_01/*.tif')
    tifs = TiffSequence(
        filenames,
        imread=imagecodecs.imread,
        pattern=ptrn,
        axesorder=(1, 2, 0, 3, 4),
        categories=categories,
    )
    assert len(tifs) == 3072
    assert tifs.shape == (16, 24, 2, 2, 2)
    assert tifs.axes == 'PAZSW'
    data = tifs.asarray()
    with TempFileName(
        'write_fsspec_sequence', ext=f'.v{version}.json'
    ) as jsonfile:
        with tifs.aszarr(
            imreadargs={'codec': imagecodecs.tiff_decode}
        ) as store:
            store.write_fsspec(
                jsonfile,
                'file:///' + store._commonpath.replace('\\', '/'),
                version=version,
            )

        from imagecodecs.numcodecs import register_codecs

        register_codecs(verbose=False)

        zobj = zarr.open(refs_as_store(jsonfile), zarr_format=2)
        assert_array_equal(zobj[:], data)


@pytest.mark.skipif(SKIP_FILE or SKIP_ZARR, reason=REASON)
def test_write_tiff2fsspec():
    """Test tiff2fsspec function."""
    filename = _file('tifffile/multiscene_pyramidal.ome.tif')
    url = os.path.dirname(filename).replace('\\', '/')
    data = imread(filename, series=0, level=1, maxworkers=1)
    with TempFileName('write_tiff2fsspec', ext='.json') as jsonfile:
        tiff2fsspec(
            filename,
            url,
            out=jsonfile,
            series=0,
            level=1,
            version=0,
        )
        zobj = zarr.open(refs_as_store(jsonfile), zarr_format=2)
        assert_array_equal(zobj[:], data)

        with pytest.raises(ValueError):
            tiff2fsspec(
                filename,
                url,
                out=jsonfile,
                series=0,
                level=1,
                version=0,
                chunkmode=CHUNKMODE.PAGE,
            )


@pytest.mark.skipif(SKIP_ZARR, reason=REASON)
def test_write_numcodecs():
    """Test write Zarr with numcodecs.Tiff."""
    from tifffile import numcodecs

    data = numpy.arange(256 * 256 * 3, dtype=numpy.uint16).reshape(
        (256, 256, 3)
    )
    numcodecs.register_codec()
    compressor = numcodecs.Tiff(
        bigtiff=True,
        photometric=PHOTOMETRIC.MINISBLACK,
        planarconfig=PLANARCONFIG.CONTIG,
        compression=COMPRESSION.ADOBE_DEFLATE,
        compressionargs={'level': 5},
        key=0,
    )
    with TempFileName('write_numcodecs', ext='.zarr') as filename:
        z = zarr.open(
            filename,
            mode='w',
            shape=(256, 256, 3),
            chunks=(100, 100, 3),
            dtype=numpy.uint16,
            compressor=compressor,
            zarr_format=2,
        )
        z[:] = data
        assert_array_equal(z[:], data)


###############################################################################

# Test write ImageJ


@pytest.mark.skipif(SKIP_EXTENDED, reason=REASON)
@pytest.mark.parametrize(
    'shape',
    [
        (219, 301, 1),
        (219, 301, 2),
        (219, 301, 3),
        (219, 301, 4),
        (219, 301, 5),
        (1, 219, 301),
        (2, 219, 301),
        (3, 219, 301),
        (4, 219, 301),
        (5, 219, 301),
        (4, 3, 219, 301),
        (4, 219, 301, 3),
        (3, 4, 219, 301),
        (1, 3, 1, 219, 301),
        (3, 1, 1, 219, 301),
        (1, 3, 4, 219, 301),
        (3, 1, 4, 219, 301),
        (3, 4, 1, 219, 301),
        (3, 4, 1, 219, 301, 3),
        (2, 3, 4, 219, 301),
        (4, 3, 2, 219, 301, 3),
    ],
)
@pytest.mark.parametrize(
    'dtype', [numpy.uint8, numpy.uint16, numpy.int16, numpy.float32]
)
@pytest.mark.parametrize('byteorder', ['>', '<'])
def test_write_imagej(byteorder, dtype, shape):
    """Test write ImageJ format."""
    # TODO: test compression and bigtiff ?
    dtype = numpy.dtype(dtype)
    if dtype != numpy.uint8 and shape[-1] in {3, 4}:
        pytest.xfail('ImageJ only supports uint8 RGB')
    data = random_data(dtype, shape)
    filename = 'write_imagej_{}_{}_{}'.format(
        {'<': 'le', '>': 'be'}[byteorder], dtype, str(shape).replace(' ', '')
    )
    with TempFileName(filename) as filename:
        imwrite(filename, data, byteorder=byteorder, imagej=True)
        image = imread(filename)
        assert_array_equal(data.squeeze(), image.squeeze())
        # TODO: assert_aszarr_method
        assert_valid_tiff(filename)


def test_write_imagej_voxel_size():
    """Test write ImageJ with xyz voxel size 2.6755x2.6755x3.9474 µm^3."""
    data = numpy.zeros((4, 1, 256, 256), dtype=numpy.float32)
    with TempFileName('write_imagej_voxel_size') as filename:
        imwrite(
            filename,
            data,
            imagej=True,
            resolution=(0.373759, 0.373759),
            metadata={'spacing': 3.947368, 'unit': 'um'},
        )
        with TiffFile(filename) as tif:
            assert tif.is_imagej
            ijmeta = tif.imagej_metadata
            assert ijmeta is not None
            assert 'unit' in ijmeta
            assert ijmeta['unit'] == 'um'
            series = tif.series[0]
            assert series.kind == 'imagej'
            assert series.axes == 'ZYX'
            assert series.shape == (4, 256, 256)
            assert series.get_axes(squeeze=False) == 'TZCYXS'
            assert series.get_shape(squeeze=False) == (1, 4, 1, 256, 256, 1)
            assert__str__(tif)
        assert_valid_tiff(filename)


def test_write_imagej_metadata():
    """Test write additional ImageJ metadata."""
    data = numpy.empty((4, 256, 256), dtype=numpy.uint16)
    data[:] = numpy.arange(256 * 256, dtype=numpy.uint16).reshape(
        (1, 256, 256)
    )
    with TempFileName('write_imagej_metadata') as filename:
        imwrite(filename, data, imagej=True, metadata={'unit': 'um'})
        with TiffFile(filename) as tif:
            assert tif.is_imagej
            assert tif.imagej_metadata is not None
            assert 'unit' in tif.imagej_metadata
            assert tif.imagej_metadata['unit'] == 'um'
            assert__str__(tif)
        assert_valid_tiff(filename)


@pytest.mark.parametrize('dtype', ['u1', 'u2', 'f4'])
@pytest.mark.parametrize('photometric', [None, 'minisblack'])
def test_write_imagej_colormap(dtype, photometric):
    """Test write ImageJ with colormap."""
    # https://forum.image.sc/t/101788
    data = numpy.empty((4, 256, 256), dtype=numpy.uint8)
    data[:] = numpy.arange(256 * 256, dtype=numpy.uint8).reshape((1, 256, 256))
    colormap = numpy.arange(3 * 256, dtype=numpy.uint16).reshape((3, 256))
    photometric_out = (
        PHOTOMETRIC.PALETTE if dtype == 'u1' else PHOTOMETRIC.MINISBLACK
    )

    with TempFileName(
        f'write_imagej_colormap_{dtype}_{photometric}'
    ) as filename:
        imwrite(
            filename,
            data.astype(dtype),
            imagej=True,
            colormap=colormap,
            photometric=photometric,
        )
        with TiffFile(filename) as tif:
            assert tif.is_imagej
            page = tif.pages.first
            assert page.photometric == photometric_out
            assert_array_equal(page.colormap, colormap)
            assert__str__(tif)
        assert_valid_tiff(filename)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_write_imagej_ijmetadata_tag():
    """Test write and read IJMetadata tag."""
    filename = _file('imagej/IJMetadata.tif')
    with TiffFile(filename) as tif:
        assert tif.is_imagej
        assert tif.byteorder == '>'
        assert len(tif.pages) == 3
        assert len(tif.series) == 1
        data = tif.asarray()
        ijmeta = tif.pages.first.tags['IJMetadata'].value

    assert ijmeta['Info'][:21] == 'FluorescentCells.tif\n'
    assert ijmeta['ROI'][:5] == b'Iout\x00'
    assert ijmeta['Overlays'][1][:5] == b'Iout\x00'
    assert ijmeta['Ranges'] == (0.0, 255.0, 0.0, 255.0, 0.0, 255.0)
    assert ijmeta['Labels'] == ['Red', 'Green', 'Blue']
    assert ijmeta['LUTs'][2][2, 255] == 255
    assert_valid_tiff(filename)

    with TempFileName('write_imagej_ijmetadata') as filename:
        with pytest.raises(TypeError):
            imwrite(
                filename,
                data,
                byteorder='>',
                imagej=True,
                metadata={'mode': 'composite'},
                ijmetadata=ijmeta,
            )

        imwrite(
            filename,
            data,
            byteorder='>',
            imagej=True,
            metadata={**ijmeta, 'mode': 'composite'},
        )
        with TiffFile(filename) as tif:
            assert tif.is_imagej
            assert tif.byteorder == '>'
            assert len(tif.pages) == 3
            assert len(tif.series) == 1
            imagej_metadata = tif.imagej_metadata
            data2 = tif.asarray()
            ijmeta2 = tif.pages.first.tags['IJMetadata'].value

            assert__str__(tif)

    assert_array_equal(data, data2)
    assert imagej_metadata is not None
    assert imagej_metadata['mode'] == 'composite'
    assert imagej_metadata['Info'] == ijmeta['Info']
    assert ijmeta2['Info'] == ijmeta['Info']
    assert ijmeta2['ROI'] == ijmeta['ROI']
    assert ijmeta2['Overlays'] == ijmeta['Overlays']
    assert ijmeta2['Ranges'] == ijmeta['Ranges']
    assert ijmeta2['Labels'] == ijmeta['Labels']
    assert_array_equal(ijmeta2['LUTs'][2], ijmeta['LUTs'][2])
    assert_valid_tiff(filename)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_write_imagej_roundtrip():
    """Test ImageJ metadata survive read/write roundtrip."""
    filename = _file('imagej/IJMetadata.tif')
    with TiffFile(filename) as tif:
        assert tif.is_imagej
        assert tif.byteorder == '>'
        assert len(tif.pages) == 3
        assert len(tif.series) == 1
        data = tif.asarray()
        ijmeta = tif.imagej_metadata

    assert ijmeta is not None
    assert ijmeta['Info'][:21] == 'FluorescentCells.tif\n'
    assert ijmeta['ROI'][:5] == b'Iout\x00'
    assert ijmeta['Overlays'][1][:5] == b'Iout\x00'
    assert ijmeta['Ranges'] == (0.0, 255.0, 0.0, 255.0, 0.0, 255.0)
    assert ijmeta['Labels'] == ['Red', 'Green', 'Blue']
    assert ijmeta['LUTs'][2][2, 255] == 255
    assert ijmeta['mode'] == 'composite'
    assert not ijmeta['loop']
    assert ijmeta['ImageJ'] == '1.52b'
    assert type(ijmeta['LUTs']) is list
    assert_valid_tiff(filename)

    with TempFileName('write_imagej_ijmetadata_roundtrip') as filename:
        imwrite(filename, data, byteorder='>', imagej=True, metadata=ijmeta)

        with TiffFile(filename) as tif:
            assert tif.is_imagej
            assert tif.byteorder == '>'
            assert len(tif.pages) == 3
            assert len(tif.series) == 1
            ijmeta2 = tif.imagej_metadata
            data2 = tif.asarray()
            assert__str__(tif)

    assert_array_equal(data, data2)
    assert ijmeta2 is not None
    assert ijmeta2['ImageJ'] == ijmeta['ImageJ']
    assert ijmeta2['mode'] == ijmeta['mode']
    assert ijmeta2['Info'] == ijmeta['Info']
    assert ijmeta2['ROI'] == ijmeta['ROI']
    assert ijmeta2['Overlays'] == ijmeta['Overlays']
    assert ijmeta2['Ranges'] == ijmeta['Ranges']
    assert ijmeta2['Labels'] == ijmeta['Labels']
    assert_array_equal(ijmeta2['LUTs'][2], ijmeta['LUTs'][2])
    assert_valid_tiff(filename)


@pytest.mark.parametrize('mmap', [False, True])
@pytest.mark.parametrize('truncate', [False, True])
def test_write_imagej_hyperstack(truncate, mmap):
    """Test write ImageJ hyperstack."""
    shape = (5, 6, 7, 49, 61, 3)
    data = numpy.empty(shape, dtype=numpy.uint8)
    data[:] = numpy.arange(210, dtype=numpy.uint8).reshape((5, 6, 7, 1, 1, 1))

    _truncate = ['', '_trunc'][truncate]
    _memmap = ['', '_memmap'][mmap]
    with TempFileName(
        f'write_imagej_hyperstack{_truncate}{_memmap}'
    ) as filename:
        if mmap:
            image = memmap(
                filename,
                shape=data.shape,
                dtype=data.dtype,
                imagej=True,
                truncate=truncate,
            )
            image[:] = data
            del image
        else:
            imwrite(filename, data, truncate=truncate, imagej=True)
        # assert file
        with TiffFile(filename) as tif:
            assert not tif.is_bigtiff
            assert not tif.is_shaped
            assert len(tif.pages) == 1 if truncate else 210
            page = tif.pages.first
            assert page.is_contiguous
            assert page.planarconfig == PLANARCONFIG.CONTIG
            assert page.photometric == PHOTOMETRIC.RGB
            assert page.imagewidth == 61
            assert page.imagelength == 49
            assert page.samplesperpixel == 3
            # assert series properties
            series = tif.series[0]
            assert series.is_truncated == truncate
            assert series.kind == 'imagej'
            assert series.shape == shape
            assert len(series._pages) == 1
            assert len(series.pages) == 1 if truncate else 210
            assert series.dtype == numpy.uint8
            assert series.axes == 'TZCYXS'
            assert series.get_axes(squeeze=False) == 'TZCYXS'
            assert series.get_shape(squeeze=False) == shape
            # assert data
            image = tif.asarray(out='memmap')
            assert_array_equal(data.squeeze(), image.squeeze())
            del image
            # assert iterating over series.pages
            data = data.reshape((210, 49, 61, 3))
            for i, page in enumerate(series.pages):
                image = page.asarray()
                assert_array_equal(data[i], image)
            del image
            assert__str__(tif)
        assert_valid_tiff(filename)


def test_write_imagej_append():
    """Test write ImageJ file consecutively."""
    data = numpy.empty((256, 1, 256, 256), dtype=numpy.uint8)
    data[:] = numpy.arange(256, dtype=numpy.uint8).reshape((-1, 1, 1, 1))

    with TempFileName('write_imagej_append') as filename:
        with TiffWriter(filename, imagej=True) as tif:
            for image in data:
                tif.write(image, contiguous=True)

        assert_valid_tiff(filename)

        # assert file
        with TiffFile(filename) as tif:
            assert not tif.is_bigtiff
            assert not tif.is_shaped
            assert len(tif.pages) == 256
            page = tif.pages.first
            assert page.is_contiguous
            assert page.planarconfig == PLANARCONFIG.CONTIG
            assert page.photometric != PHOTOMETRIC.RGB
            assert page.imagewidth == 256
            assert page.imagelength == 256
            assert page.samplesperpixel == 1
            # assert series properties
            series = tif.series[0]
            assert series.kind == 'imagej'
            assert series.shape == (256, 256, 256)
            assert series.dtype == numpy.uint8
            assert series.axes == 'ZYX'
            assert series.get_axes(squeeze=False) == 'TZCYXS'
            assert series.get_shape(squeeze=False) == (1, 256, 1, 256, 256, 1)
            # assert data
            image = tif.asarray(out='memmap')
            assert_array_equal(data.squeeze(), image)
            del image
            assert__str__(tif)


def test_write_imagej_bigtiff():
    """Test write ImageJ BigTIFF with warning."""
    with TempFileName('write_imagej_bigtiff') as filename:  # noqa: SIM117
        with pytest.warns(UserWarning):
            imwrite(
                filename,
                shape=(31, 33),
                dtype=numpy.float32,
                bigtiff=True,
                imagej=True,
                metadata=None,
            )


@pytest.mark.skipif(SKIP_LARGE, reason=REASON)
def test_write_imagej_raw():
    """Test write ImageJ 5 GB raw file."""
    data = numpy.empty((1280, 1, 1024, 1024), dtype=numpy.float32)
    data[:] = numpy.arange(1280, dtype=numpy.float32).reshape((-1, 1, 1, 1))

    with TempFileName('write_imagej_big') as filename:
        with pytest.warns(UserWarning):
            # UserWarning: truncating ImageJ file
            imwrite(filename, data, imagej=True)
        assert_valid_tiff(filename)
        # assert file
        with TiffFile(filename) as tif:
            assert not tif.is_bigtiff
            assert not tif.is_shaped
            assert len(tif.pages) == 1
            page = tif.pages.first
            assert page.is_contiguous
            assert page.planarconfig == PLANARCONFIG.CONTIG
            assert page.photometric != PHOTOMETRIC.RGB
            assert page.imagewidth == 1024
            assert page.imagelength == 1024
            assert page.samplesperpixel == 1
            # assert series properties
            series = tif.series[0]
            assert series.kind == 'imagej'
            assert len(series._pages) == 1
            assert len(series.pages) == 1
            assert series.shape == (1280, 1024, 1024)
            assert series.dtype == numpy.float32
            assert series.axes == 'ZYX'
            assert series.get_axes(squeeze=False) == 'TZCYXS'
            assert series.get_shape(squeeze=False) == (
                1,
                1280,
                1,
                1024,
                1024,
                1,
            )
            # assert data
            image = tif.asarray(out='memmap')
            assert_array_equal(data.squeeze(), image.squeeze())
            del image

            if not SKIP_ZARR:
                store = imread(filename, aszarr=True)
                try:
                    z = zarr.open(store, mode='r')
                    chunk = z[333:356, 99:513, 31:127]
                finally:
                    store.close()
                assert_array_equal(chunk, data[333:356, 0, 99:513, 31:127])

                store = imread(filename, aszarr=True, chunkmode=2)
                try:
                    z = zarr.open(store, mode='r')
                    chunk = z[333:356, 99:513, 31:127]
                finally:
                    store.close()
                assert_array_equal(chunk, data[333:356, 0, 99:513, 31:127])

            assert__str__(tif)


@pytest.mark.skipif(SKIP_EXTENDED, reason=REASON)
@pytest.mark.parametrize(
    ('shape', 'axes'),
    [
        ((219, 301, 1), None),
        ((219, 301, 2), None),
        ((219, 301, 3), None),
        ((219, 301, 4), None),
        ((219, 301, 5), None),
        ((1, 219, 301), None),
        ((2, 219, 301), None),
        ((3, 219, 301), None),
        ((4, 219, 301), None),
        ((5, 219, 301), None),
        ((4, 3, 219, 301), None),
        ((4, 219, 301, 3), None),
        ((3, 4, 219, 301), None),
        ((1, 3, 1, 219, 301), None),
        ((3, 1, 1, 219, 301), None),
        ((1, 3, 4, 219, 301), None),
        ((3, 1, 4, 219, 301), None),
        ((3, 4, 1, 219, 301), None),
        ((3, 4, 1, 219, 301, 3), None),
        ((2, 3, 4, 219, 301), None),
        ((4, 3, 2, 219, 301, 3), None),
        ((3, 33, 31), 'CYX'),
        ((33, 31, 3), 'YXC'),
        ((5, 1, 33, 31), 'CSYX'),
        ((5, 1, 33, 31), 'ZCYX'),
        ((2, 3, 4, 219, 301, 3), 'TCZYXS'),
        ((10, 5, 63, 65), 'EPYX'),
        ((2, 3, 4, 5, 6, 7, 33, 31, 3), 'TQCPZRYXS'),
    ],
)
def test_write_ome(shape, axes):
    """Test write OME-TIFF format."""
    photometric = None
    planarconfig = None
    if shape[-1] in {3, 4}:
        photometric = PHOTOMETRIC.RGB
        planarconfig = PLANARCONFIG.CONTIG
    elif shape[-3] in {3, 4}:
        photometric = PHOTOMETRIC.RGB
        planarconfig = PLANARCONFIG.SEPARATE

    metadata = {'axes': axes} if axes is not None else None
    data = random_data(numpy.uint8, shape)
    filename = 'write_ome_{}.ome'.format(str(shape).replace(' ', ''))
    with TempFileName(filename) as filename:
        imwrite(
            filename,
            data,
            metadata=metadata,
            photometric=photometric,
            planarconfig=planarconfig,
        )
        with TiffFile(filename) as tif:
            assert tif.is_ome
            assert not tif.is_shaped
            assert tif.series[0].kind == 'ome'
            image = tif.asarray()
            omexml = tif.ome_metadata
            if axes:
                if axes == 'CYX':
                    axes = 'SYX'
                elif axes == 'YXC':
                    axes = 'YXS'
                assert tif.series[0].axes == squeeze_axes(shape, axes)[1]
            assert_array_equal(data.squeeze(), image.squeeze())
            assert_aszarr_method(tif, image)
            assert_valid_omexml(omexml)
        assert_valid_tiff(filename)


def test_write_ome_enable():
    """Test OME-TIFF enabling."""
    data = numpy.zeros((32, 32), dtype=numpy.uint8)
    with TempFileName('write_ome_enable.ome') as filename:
        imwrite(filename, data)
        with TiffFile(filename) as tif:
            assert tif.is_ome
        imwrite(filename, data, description='not OME')
        with TiffFile(filename) as tif:
            assert not tif.is_ome
        with pytest.warns(UserWarning):
            imwrite(filename, data, description='not OME', ome=True)
        with TiffFile(filename) as tif:
            assert tif.is_ome
        imwrite(filename, data, imagej=True)
        with TiffFile(filename) as tif:
            assert not tif.is_ome
            assert tif.is_imagej
        imwrite(filename, data, imagej=True, ome=True)
        with TiffFile(filename) as tif:
            assert tif.is_ome
            assert not tif.is_imagej

    with TempFileName('write_ome_auto.tif') as filename:
        imwrite(filename, data)
        with TiffFile(filename) as tif:
            assert not tif.is_ome
        imwrite(filename, data, ome=True)
        with TiffFile(filename) as tif:
            assert tif.is_ome


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
@pytest.mark.parametrize(
    'method', ['manual', 'copy', 'iter', 'compression', 'xml', 'dask']
)
def test_write_ome_methods(method):
    """Test re-write OME-TIFF."""
    # 4D (7 time points, 5 focal planes)
    if method == 'dask' and (SKIP_DASK or SKIP_ZARR):
        pytest.skip('skip dask')

    filename = _file('OME/bioformats-artificial/4D-series.ome.tiff')
    with TiffFile(filename) as tif:
        series = tif.series[0]
        data = series.asarray()
        dtype = data.dtype
        shape = data.shape
        axes = series.axes
        omexml = tif.ome_metadata
        if method == 'dask':
            store = series.aszarr()
            darr = dask.array.from_zarr(store)

    def pages():
        yield from data.reshape((-1, *data.shape[-2:]))

    with TempFileName(f'write_ome_{method}.ome') as filename:
        if method == 'xml':
            # use original XML metadata
            metadata = xml2dict(omexml)
            metadata['axes'] = axes
            imwrite(
                filename,
                data,
                byteorder='>',
                photometric=PHOTOMETRIC.MINISBLACK,
                metadata=metadata,
            )

        elif method == 'manual':
            # manually write omexml to first page and data to individual pages
            # process OME-XML
            omexml = omexml.replace(
                '4D-series.ome.tiff', os.path.split(filename)[-1]
            )
            # omexml = omexml.replace('BigEndian="true"', 'BigEndian="false"')
            data = data.view(data.dtype.newbyteorder('>'))
            # save image planes in the order referenced in the OME-XML
            # make sure storage options (compression, byteorder, photometric)
            #   match OME-XML
            # write OME-XML to first page only
            with TiffWriter(filename, byteorder='>') as tif:
                for i, image in enumerate(pages()):
                    description = omexml if i == 0 else None
                    tif.write(
                        image,
                        description=description,
                        photometric=PHOTOMETRIC.MINISBLACK,
                        metadata=None,
                        contiguous=False,
                    )

        elif method == 'iter':
            # use iterator over individual pages
            imwrite(
                filename,
                pages(),
                shape=shape,
                dtype=dtype,
                byteorder='>',
                photometric=PHOTOMETRIC.MINISBLACK,
                metadata={'axes': axes},
            )

        elif method == 'compression':
            # use iterator with compression
            imwrite(
                filename,
                pages(),
                shape=shape,
                dtype=dtype,
                byteorder='>',
                photometric=PHOTOMETRIC.MINISBLACK,
                compression=COMPRESSION.ADOBE_DEFLATE,
                metadata={'axes': axes},
            )

        elif method == 'copy':
            # use one numpy array
            imwrite(
                filename,
                data,
                byteorder='>',
                photometric=PHOTOMETRIC.MINISBLACK,
                metadata={'axes': axes},
            )

        elif method == 'dask':
            # dask array
            imwrite(
                filename,
                darr,
                byteorder='>',
                photometric=PHOTOMETRIC.MINISBLACK,
                compression=COMPRESSION.ADOBE_DEFLATE,
                metadata={'axes': axes},
            )
            del darr
            store.close()

        with TiffFile(filename) as tif:
            assert tif.is_ome
            assert tif.byteorder == '>'
            assert len(tif.pages) == 35
            assert len(tif.series) == 1
            # assert page properties
            page = tif.pages.first
            if method not in {'compression', 'dask'}:
                assert page.is_contiguous
                assert page.compression == COMPRESSION.NONE
            assert page.imagewidth == 439
            assert page.imagelength == 167
            assert page.bitspersample == 8
            assert page.samplesperpixel == 1
            # assert series properties
            series = tif.series[0]
            assert series.kind == 'ome'
            assert series.shape == (7, 5, 167, 439)
            assert series.dtype == numpy.int8
            assert series.axes == 'TZYX'
            # assert data
            assert_array_equal(data, tif.asarray())
            assert_valid_omexml(tif.ome_metadata)
            assert__str__(tif)

        assert_valid_tiff(filename)


@pytest.mark.parametrize('contiguous', [True, False])
def test_write_ome_manual(contiguous):
    """Test write OME-TIFF manually."""
    data = RNG.integers(0, 255, (19, 31, 21), numpy.uint8)

    with TempFileName(f'write_ome__manual{int(contiguous)}.ome') as filename:
        with TiffWriter(filename) as tif:
            # successively write image data to TIFF pages
            # disable tifffile from writing any metadata
            # add empty ImageDescription tag to first page
            for i, frame in enumerate(data):
                tif.write(
                    frame,
                    contiguous=contiguous,
                    metadata=None,
                    description=None if i else b'',
                )
            # update ImageDescription tag with custom OME-XML
            xml = OmeXml()
            xml.addimage(
                numpy.uint8, (16, 31, 21), (16, 1, 1, 31, 21, 1), axes='ZYX'
            )
            xml.addimage(
                numpy.uint8, (3, 31, 21), (3, 1, 1, 31, 21, 1), axes='CYX'
            )
            tif.overwrite_description(xml.tostring())

        with TiffFile(filename) as tif:
            assert tif.is_ome
            assert len(tif.pages) == 19
            assert len(tif.series) == 2
            # assert series properties
            series = tif.series[0]
            assert series.kind == 'ome'
            assert series.axes == 'ZYX'
            assert bool(series.dataoffset) == contiguous
            assert_array_equal(data[:16], series.asarray())
            series = tif.series[1]
            assert series.kind == 'ome'
            assert series.axes == 'CYX'
            assert bool(series.dataoffset) == contiguous
            assert_array_equal(data[16:], series.asarray())
            assert_valid_omexml(tif.ome_metadata)
            assert__str__(tif)

        assert_valid_tiff(filename)


@pytest.mark.skipif(
    SKIP_FILE or SKIP_CODECS or not imagecodecs.JPEG.available,
    reason=REASON,
)
def test_rewrite_ome():
    """Test rewrite multi-series, pyramidal OME-TIFF."""
    # https://github.com/cgohlke/tifffile/issues/156
    # - data loss in case of JPEG recompression; copy tiles verbatim
    # - OME metadata not copied; use ometypes library after writing
    # - tifffile does not support multi-file OME-TIFF writing
    filename = _file('tifffile/multiscene_pyramidal.ome.tif')
    with TiffFile(filename) as tif:
        assert tif.is_ome
        with TempFileName('rewrite_ome', ext='.ome.tif') as filename_copy:
            with TiffWriter(
                filename_copy,
                bigtiff=tif.is_bigtiff,
                byteorder=tif.byteorder,
                ome=tif.is_ome,
            ) as copy:
                for series in tif.series:
                    subifds = len(series.levels) - 1
                    metadata = {'axes': series.axes}
                    for level in series.levels:
                        keyframe = level.keyframe
                        copy.write(
                            level.asarray(),
                            planarconfig=keyframe.planarconfig,
                            photometric=keyframe.photometric,
                            extrasamples=keyframe.extrasamples,
                            tile=keyframe.tile,
                            rowsperstrip=keyframe.rowsperstrip,
                            compression=keyframe.compression,
                            predictor=keyframe.predictor,
                            subsampling=keyframe.subsampling,
                            datetime=keyframe.datetime,
                            resolution=keyframe.resolution,
                            resolutionunit=keyframe.resolutionunit,
                            subfiletype=keyframe.subfiletype,
                            colormap=keyframe.colormap,
                            iccprofile=keyframe.iccprofile,
                            subifds=subifds,
                            metadata=metadata,
                        )
                        subifds = None
                        metadata = None
            # verify
            with TiffFile(filename_copy) as copy:
                assert copy.byteorder == tif.byteorder
                assert copy.is_bigtiff == tif.is_bigtiff
                assert copy.is_imagej == tif.is_imagej
                assert copy.is_ome == tif.is_ome
                assert len(tif.series) == len(copy.series)
                for series, series_copy in zip(
                    tif.series, copy.series, strict=True
                ):
                    assert len(series.levels) == len(series_copy.levels)
                    metadata = {'axes': series.axes}
                    for level, level_copy in zip(
                        series.levels, series_copy.levels, strict=True
                    ):
                        assert len(level.pages) == len(level_copy.pages)
                        assert level.shape == level_copy.shape
                        assert level.dtype == level_copy.dtype
                        assert level.keyframe.hash == level_copy.keyframe.hash


@pytest.mark.skipif(
    SKIP_FILE or SKIP_CODECS or SKIP_LARGE or not imagecodecs.JPEG.available,
    reason=REASON,
)
def test_write_ome_copy():
    """Test write pyramidal OME-TIFF by copying compressed tiles from SVS."""

    def tiles(page):
        # return iterator over compressed tiles in page
        assert page.is_tiled
        fh = page.parent.filehandle
        for offset, bytecount in zip(
            page.dataoffsets, page.databytecounts, strict=True
        ):
            fh.seek(offset)
            yield fh.read(bytecount)

    with TiffFile(_file('AperioSVS/CMU-1.svs')) as svs:
        assert svs.is_svs
        levels = svs.series[0].levels

        with TempFileName('write_ome_copy', ext='.ome.tif') as filename:
            with TiffWriter(filename, ome=True, bigtiff=True) as tif:
                level = levels[0]
                assert len(level.pages) == 1
                page = level.pages[0]
                if page.compression == 7:
                    # override default that RGB will be compressed as YCBCR
                    compressionargs = {'outcolorspace': page.photometric}
                else:
                    compressionargs = {}
                extratags = (
                    # copy some extra tags
                    page.tags.get('ImageDepth').astuple(),  # this is ignored
                    page.tags.get('InterColorProfile').astuple(),
                )
                tif.write(
                    tiles(page),
                    shape=page.shape,
                    dtype=page.dtype,
                    tile=page.tile,
                    datetime=page.datetime,
                    photometric=page.photometric,
                    planarconfig=page.planarconfig,
                    compression=page.compression,
                    compressionargs=compressionargs,
                    jpegtables=page.jpegtables,
                    # iccprofile=page.iccprofile,  # in extratags
                    subsampling=page.subsampling,
                    subifds=len(levels) - 1,
                    extratags=extratags,
                )
                for level in levels[1:]:
                    assert len(level.pages) == 1
                    page = level.pages[0]
                    if page.compression == 7:
                        compressionargs = {'outcolorspace': page.photometric}
                    else:
                        compressionargs = {}
                    tif.write(
                        tiles(page),
                        shape=page.shape,
                        dtype=page.dtype,
                        tile=page.tile,
                        datetime=page.datetime,
                        photometric=page.photometric,
                        planarconfig=page.planarconfig,
                        compression=page.compression,
                        compressionargs=compressionargs,
                        jpegtables=page.jpegtables,
                        iccprofile=page.iccprofile,
                        subsampling=page.subsampling,
                        subfiletype=1,
                    )

            with TiffFile(filename) as tif:
                assert tif.is_ome
                assert len(tif.pages) == 1
                assert len(tif.pages.first.pages) == 2
                assert 'InterColorProfile' in tif.pages.first.tags
                assert 'ImageDepth' not in tif.pages.first.tags
                assert tif.series[0].kind == 'ome'
                levels_ = tif.series[0].levels
                assert len(levels_) == len(levels)
                for level, level_ in zip(levels[1:], levels_[1:], strict=True):
                    assert level.shape == level_.shape
                    assert level.dtype == level_.dtype
                    assert_array_equal(level.asarray(), level_.asarray())
                    assert level_.keyframe.subfiletype == FILETYPE.REDUCEDIMAGE


@pytest.mark.skipif(
    SKIP_FILE or SKIP_CODECS or not imagecodecs.JPEG.available,
    reason=REASON,
)
def test_write_geotiff_copy():
    """Test write a copy of striped, compressed GeoTIFF."""

    def strips(page):
        # return iterator over compressed strips in page
        assert not page.is_tiled
        fh = page.parent.filehandle
        for offset, bytecount in zip(
            page.dataoffsets, page.databytecounts, strict=True
        ):
            fh.seek(offset)
            yield fh.read(bytecount)

    with TiffFile(_file('GeoTIFF/ML_30m.tif')) as geotiff:
        assert geotiff.is_geotiff
        assert len(geotiff.pages) == 1

        with TempFileName('write_geotiff_copy') as filename:
            with TiffWriter(
                filename,
                byteorder=geotiff.byteorder,
                bigtiff=geotiff.is_bigtiff,
            ) as tif:
                page = geotiff.pages[0]
                tags = page.tags
                extratags = (
                    tags.get('ModelPixelScaleTag').astuple(),
                    tags.get('ModelTiepointTag').astuple(),
                    tags.get('GeoKeyDirectoryTag').astuple(),
                    tags.get('GeoAsciiParamsTag').astuple(),
                    tags.get('GDAL_NODATA').astuple(),
                )
                tif.write(
                    strips(page),
                    shape=page.shape,
                    dtype=page.dtype,
                    rowsperstrip=page.rowsperstrip,
                    photometric=page.photometric,
                    planarconfig=page.planarconfig,
                    compression=page.compression,
                    predictor=page.predictor,
                    jpegtables=page.jpegtables,
                    iccprofile=page.iccprofile,
                    subsampling=page.subsampling,
                    extratags=extratags,
                )

            with TiffFile(filename) as tif:
                assert tif.is_geotiff
                assert len(tif.pages) == 1
                page = tif.pages.first
                assert page.nodata == -32767
                assert page.tags['ModelPixelScaleTag'].value == (
                    30.0,
                    30.0,
                    0.0,
                )
                assert page.tags['ModelTiepointTag'].value == (
                    0.0,
                    0.0,
                    0.0,
                    1769487.0,
                    5439473.0,
                    0.0,
                )
                assert tif.geotiff_metadata['GeogAngularUnitsGeoKey'] == 9102
                assert_array_equal(tif.asarray(), geotiff.asarray())


@pytest.mark.parametrize('ext', ['', 'b'])
def test_write_open_mode(ext):
    """Test TiffWriter with file open modes."""
    data = RNG.integers(0, 255, (5, 31, 21), numpy.uint8)
    with TempFileName('write_open_mode' + ext) as filename:
        # write
        imwrite(filename, data, mode='w' + ext)
        assert_array_equal(imread(filename), data)
        # exclusive creation
        with pytest.raises(FileExistsError):
            imwrite(filename, data, mode='x' + ext)
        os.remove(filename)
        imwrite(filename, data, mode='x' + ext)
        assert_array_equal(imread(filename), data)
        # append
        imwrite(filename, data, mode='r+' + ext)
        with TiffFile(filename) as tif:
            assert len(tif.pages) == 10
            assert len(tif.series) == 2
            assert_array_equal(tif.series[0].asarray(), data)
            assert_array_equal(tif.series[1].asarray(), data)

        # write to file handle
        with FileHandle(filename, mode='w' + ext) as fh:
            imwrite(fh, data, mode='ignored')
        assert_array_equal(imread(filename), data)
        # exclusive creation with file handle
        with pytest.raises(FileExistsError):  # noqa: SIM117
            with FileHandle(filename, mode='x' + ext) as fh:
                pass
        os.remove(filename)
        with FileHandle(filename, mode='x' + ext) as fh:
            imwrite(fh, data, mode='ignored' + ext)
            assert not fh.closed
            fh.seek(0)
            with pytest.raises(OSError):  # io.UnsupportedOperation
                fh.read(8)
        assert_array_equal(imread(filename), data)
        # append to file handle
        with FileHandle(filename, mode='r+' + ext) as fh:
            imwrite(fh, data, mode='ignored')
            assert not fh.closed
            fh.seek(0)
            with TiffFile(fh) as tif:
                assert len(tif.pages) == 10
                assert len(tif.series) == 2
                assert_array_equal(tif.series[0].asarray(), data)
                assert_array_equal(tif.series[1].asarray(), data)

        with pytest.raises(ValueError):
            imwrite(filename, data, mode=ext)

        with pytest.raises(ValueError):
            imwrite(filename, data, mode='w' + ext, append=True)


@pytest.mark.skipif(SKIP_ZARR or SKIP_DASK or SKIP_CODECS, reason=REASON)
@pytest.mark.parametrize('atype', ['numpy', 'dask', 'zarr'])
@pytest.mark.parametrize('compression', [None, 'zlib'])
@pytest.mark.parametrize('tiled', [None, False, True])
@pytest.mark.parametrize('byteorder', ['<', '>'])
def test_write_array_types(atype, byteorder, tiled, compression):
    """Test write array types."""
    kwargs = {}
    filename = f'write_{atype}_'
    if tiled is None:
        pass
    elif tiled:
        kwargs['tile'] = (64, 64)
        filename += 'tiled_'
    else:
        kwargs['rowsperstrip'] = 64
        filename += 'striped_'
    if compression is not None:
        kwargs['compression'] = compression
        filename += compression
    filename += 'be' if byteorder == '>' else 'le'

    data = RNG.integers(0, 1024, (3, 361, 254, 3), numpy.uint16)
    if atype == 'numpy':
        arr = data.copy()
    elif atype == 'dask':
        arr = dask.array.from_array(data, chunks=(1, 32, 64, 3))
    elif atype == 'zarr':
        arr = zarr.zeros(data.shape, chunks=(1, 32, 64, 3), dtype=data.dtype)
        arr[:] = data
    with TempFileName(filename) as filename:
        imwrite(
            filename,
            arr[1:, 33 : 33 + 261, 31 : 31 + 200],
            byteorder=byteorder,
            **kwargs,
        )
        assert_array_equal(
            imread(filename), data[1:, 33 : 33 + 261, 31 : 31 + 200]
        )
        assert_array_equal(
            imagecodecs.imread(filename, index=None),
            data[1:, 33 : 33 + 261, 31 : 31 + 200],
        )


###############################################################################

# Test embedded TIFF files

EMBED_NAME = _file('tifffile/test_FileHandle.bin')
EMBED_OFFSET = 7077
EMBED_SIZE = 5744
EMBED_OFFSET1 = 13820
EMBED_SIZE1 = 7936382


def assert_embed_tif(tif):
    """Assert embedded TIFF file."""
    # 4 series in 6 pages
    assert tif.byteorder == '<'
    assert len(tif.pages) == 6
    assert len(tif.series) == 4
    # assert series 0 properties
    series = tif.series[0]
    assert series.shape == (3, 20, 20)
    assert series.dtype == numpy.uint8
    assert series.axes == 'IYX'
    assert series.kind == 'generic'
    page = series.pages[0]
    assert page.compression == COMPRESSION.LZW
    assert page.imagewidth == 20
    assert page.imagelength == 20
    assert page.bitspersample == 8
    assert page.samplesperpixel == 1
    data = tif.asarray(series=0)
    assert isinstance(data, numpy.ndarray)
    assert data.shape == (3, 20, 20)
    assert data.dtype == numpy.uint8
    assert tuple(data[:, 9, 9]) == (19, 90, 206)
    # assert series 1 properties
    series = tif.series[1]
    assert series.shape == (10, 10, 3)
    assert series.dtype == numpy.float32
    assert series.axes == 'YXS'
    assert series.kind == 'generic'
    page = series.pages[0]
    assert page.photometric == PHOTOMETRIC.RGB
    assert page.compression == COMPRESSION.LZW
    assert page.imagewidth == 10
    assert page.imagelength == 10
    assert page.bitspersample == 32
    assert page.samplesperpixel == 3
    data = tif.asarray(series=1)
    assert isinstance(data, numpy.ndarray)
    assert data.shape == (10, 10, 3)
    assert data.dtype == numpy.float32
    assert round(abs(data[9, 9, 1] - 214.5733642578125), 7) == 0
    # assert series 2 properties
    series = tif.series[2]
    assert series.shape == (20, 20, 3)
    assert series.dtype == numpy.uint8
    assert series.axes == 'YXS'
    assert series.kind == 'generic'
    page = series.pages[0]
    assert page.photometric == PHOTOMETRIC.RGB
    assert page.compression == COMPRESSION.LZW
    assert page.imagewidth == 20
    assert page.imagelength == 20
    assert page.bitspersample == 8
    assert page.samplesperpixel == 3
    data = tif.asarray(series=2)
    assert isinstance(data, numpy.ndarray)
    assert data.shape == (20, 20, 3)
    assert data.dtype == numpy.uint8
    assert tuple(data[9, 9, :]) == (19, 90, 206)
    # assert series 3 properties
    series = tif.series[3]
    assert series.shape == (10, 10)
    assert series.dtype == numpy.float32
    assert series.axes == 'YX'
    assert series.kind == 'generic'
    page = series.pages[0]
    assert page.compression == COMPRESSION.LZW
    assert page.imagewidth == 10
    assert page.imagelength == 10
    assert page.bitspersample == 32
    assert page.samplesperpixel == 1
    data = tif.asarray(series=3)
    assert isinstance(data, numpy.ndarray)
    assert data.shape == (10, 10)
    assert data.dtype == numpy.float32
    assert round(abs(data[9, 9] - 223.1648712158203), 7) == 0
    assert__str__(tif)


def assert_embed_micromanager(tif):
    """Assert embedded MicroManager TIFF file."""
    assert tif.is_ome
    assert tif.is_imagej
    assert tif.is_micromanager
    assert tif.byteorder == '<'
    assert len(tif.pages) == 15
    assert len(tif.series) == 1
    # assert non-tiff micromanager_metadata
    assert tif.micromanager_metadata is not None
    tags = tif.micromanager_metadata['Summary']
    assert tags['MicroManagerVersion'] == '1.4.x dev'
    assert tags['UserName'] == 'trurl'
    # assert page properties
    page = tif.pages.first
    assert page.is_contiguous
    assert page.compression == COMPRESSION.NONE
    assert page.imagewidth == 512
    assert page.imagelength == 512
    assert page.bitspersample == 16
    assert page.samplesperpixel == 1
    # two description tags
    assert page.description.startswith('<?xml')
    assert page.description1.startswith('ImageJ')
    # assert some metadata
    ijmeta = tif.imagej_metadata
    assert ijmeta is not None
    assert ijmeta['frames'] == 5
    assert ijmeta['slices'] == 3
    assert ijmeta['Ranges'] == (706.0, 5846.0)
    tags = tif.pages.first.tags[51123].value
    assert tags['FileName'] == 'Untitled_1_MMStack.ome.tif'
    assert tags['PixelType'] == 'GRAY16'
    # assert series properties
    series = tif.series[0]
    assert series.shape == (5, 3, 512, 512)
    assert series.dtype == numpy.uint16
    assert series.axes == 'TZYX'
    assert series.kind == 'mmstack'
    # assert data
    data = tif.asarray()
    assert data.shape == (5, 3, 512, 512)
    assert data.dtype == numpy.uint16
    assert data[4, 2, 511, 511] == 1602
    # assert memmap
    data = tif.asarray(out='memmap')
    assert isinstance(data, numpy.memmap)
    assert data.shape == (5, 3, 512, 512)
    assert data.dtype == numpy.dtype('<u2')
    assert data[4, 2, 511, 511] == 1602
    del data
    assert__str__(tif)


@pytest.mark.skipif(SKIP_FILE or SKIP_CODECS, reason=REASON)
def test_embed_tif_filename():
    """Test embedded TIFF from filename."""
    with TiffFile(EMBED_NAME, offset=EMBED_OFFSET, size=EMBED_SIZE) as tif:
        assert_embed_tif(tif)


@pytest.mark.skipif(SKIP_FILE or SKIP_CODECS, reason=REASON)
def test_embed_tif_openfile():
    """Test embedded TIFF from file handle."""
    with (
        open(EMBED_NAME, 'rb') as fh,
        TiffFile(fh, offset=EMBED_OFFSET, size=EMBED_SIZE) as tif,
    ):
        assert_embed_tif(tif)


@pytest.mark.skipif(SKIP_FILE or SKIP_CODECS, reason=REASON)
def test_embed_tif_openfile_seek():
    """Test embedded TIFF from sought file handle."""
    with open(EMBED_NAME, 'rb') as fh:
        fh.seek(EMBED_OFFSET)
        with TiffFile(fh, size=EMBED_SIZE) as tif:
            assert_embed_tif(tif)


@pytest.mark.skipif(SKIP_FILE or SKIP_CODECS, reason=REASON)
def test_embed_tif_filehandle():
    """Test embedded TIFF from FileHandle."""
    with (
        FileHandle(EMBED_NAME, offset=EMBED_OFFSET, size=EMBED_SIZE) as fh,
        TiffFile(fh) as tif,
    ):
        assert_embed_tif(tif)


@pytest.mark.skipif(SKIP_FILE or SKIP_CODECS, reason=REASON)
def test_embed_tif_bytesio():
    """Test embedded TIFF from BytesIO."""
    with open(EMBED_NAME, 'rb') as fh:
        bytesio = BytesIO(fh.read())
    with TiffFile(bytesio, offset=EMBED_OFFSET, size=EMBED_SIZE) as tif:
        assert_embed_tif(tif)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_embed_mm_filename():
    """Test embedded MicroManager TIFF from file name."""
    with TiffFile(EMBED_NAME, offset=EMBED_OFFSET1, size=EMBED_SIZE1) as tif:
        assert_embed_micromanager(tif)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_embed_mm_openfile():
    """Test embedded MicroManager TIFF from file handle."""
    with (
        open(EMBED_NAME, 'rb') as fh,
        TiffFile(fh, offset=EMBED_OFFSET1, size=EMBED_SIZE1) as tif,
    ):
        assert_embed_micromanager(tif)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_embed_mm_openfile_seek():
    """Test embedded MicroManager TIFF from sought file handle."""
    with open(EMBED_NAME, 'rb') as fh:
        fh.seek(EMBED_OFFSET1)
        with TiffFile(fh, size=EMBED_SIZE1) as tif:
            assert_embed_micromanager(tif)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_embed_mm_filehandle():
    """Test embedded MicroManager TIFF from FileHandle."""
    with (
        FileHandle(EMBED_NAME, offset=EMBED_OFFSET1, size=EMBED_SIZE1) as fh,
        TiffFile(fh) as tif,
    ):
        assert_embed_micromanager(tif)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_embed_mm_bytesio():
    """Test embedded MicroManager TIFF from BytesIO."""
    with open(EMBED_NAME, 'rb') as fh:
        bytesio = BytesIO(fh.read())
    with TiffFile(bytesio, offset=EMBED_OFFSET1, size=EMBED_SIZE1) as tif:
        assert_embed_micromanager(tif)


###############################################################################

# Test sequence of image files


def test_sequence_stream_list():
    """Test TiffSequence with list of ByteIO streams raises TypeError."""
    data = RNG.random((7, 9))
    files = [BytesIO(), BytesIO()]
    for buffer in files:
        imwrite(buffer, data)
    with pytest.raises(TypeError):
        imread(files)


@pytest.mark.skipif(SKIP_FILE or SKIP_CODECS, reason=REASON)
@pytest.mark.parametrize('case', [0, 1, 2, 3])
def test_sequence_glob_pattern(case):
    """Test TiffSequence with glob pattern without axes pattern."""
    filename = _files('TiffSequence/*.tif')
    tifs = TiffSequence(filename, pattern=None)
    assert len(tifs) == 10
    assert tifs.shape == (10,)
    assert tifs.axes == 'I'
    data = tifs.asarray()
    if case == 0:
        data = tifs.asarray()
    elif case == 1:
        data = tifs.asarray(
            chunkshape=(480, 640), chunkdtype=numpy.uint8, pattern=None, key=0
        )
    elif case == 2:
        data = tifs.asarray(
            imreadargs={'key': 0},
            chunkshape=(480, 640),
            chunkdtype=numpy.uint8,
            pattern=None,
            out_inplace=True,
            out='memmap',
        )
    elif case == 3:
        data = numpy.empty((10, 480, 640), dtype=numpy.uint8)
        tifs.asarray(
            chunkshape=(480, 640),
            chunkdtype=numpy.uint8,
            pattern=None,
            out_inplace=True,
            out=data,
            key=0,
        )
    assert isinstance(data, numpy.ndarray)
    assert data.flags['C_CONTIGUOUS']
    assert data.shape == (10, 480, 640)
    assert data.dtype == numpy.uint8
    assert data[9, 256, 256] == 135
    if not SKIP_ZARR:
        with tifs.aszarr() as store:
            assert isinstance(store, ZarrStore)
            assert isinstance(store, ZarrFileSequenceStore)
            assert_array_equal(data, zarr.open(store, mode='r'))
    assert__repr__(tifs)


@pytest.mark.skipif(SKIP_FILE or SKIP_CODECS, reason=REASON)
def test_sequence_name_list():
    """Test TiffSequence with list of file names without pattern."""
    filename = [
        _file('TiffSequence/AT3_1m4_01.tif'),
        _file('TiffSequence/AT3_1m4_10.tif'),
    ]
    tifs = TiffSequence(filename, pattern=None)
    assert len(tifs) == 2
    assert tifs.shape == (2,)
    assert tifs.axes == 'I'
    data = tifs.asarray()
    assert isinstance(data, numpy.ndarray)
    assert data.flags['C_CONTIGUOUS']
    assert data.shape == (2, 480, 640)
    assert data.dtype == numpy.uint8
    assert data[1, 256, 256] == 135
    if not SKIP_ZARR:
        with tifs.aszarr() as store:
            assert_array_equal(data, zarr.open(store, mode='r'))
    assert__repr__(tifs)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_sequence_oif_series():
    """Test TiffSequence with files from Olympus OIF, memory mapped."""
    filename = _files(
        'oif/MB231cell1_paxgfp_PDMSgasket_PMMAflat_30nm_378sli.oif.files/*.tif'
    )
    tifs = TiffSequence(filename, pattern='axes')
    assert len(tifs) == 756
    assert tifs.shape == (2, 378)
    assert tifs.axes == 'CZ'
    # memory map
    data = tifs.asarray(out='memmap', ioworkers=None)
    assert isinstance(data, numpy.memmap)
    assert data.flags['C_CONTIGUOUS']
    assert data.shape == (2, 378, 256, 256)
    assert data.dtype == numpy.dtype('<u2')
    assert data[1, 377, 70, 146] == 29
    if not SKIP_ZARR:
        with tifs.aszarr() as store:
            assert_array_equal(data, zarr.open(store, mode='r'))
    del data
    assert__repr__(tifs)


@pytest.mark.skipif(SKIP_FILE or SKIP_CODECS, reason=REASON)
def test_sequence_leica_series():
    """Test TiffSequence with Leica TIFF series, memory mapped."""
    filename = _files('leicaseries/Series019_*.tif')
    tifs = TiffSequence(filename, pattern='axes')
    assert len(tifs) == 46
    assert tifs.shape == (46,)
    assert tifs.axes == 'Y'
    # memory map
    data = tifs.asarray(out='memmap')
    assert isinstance(data, numpy.memmap)
    assert data.flags['C_CONTIGUOUS']
    assert data.shape == (46, 512, 512, 3)
    assert data.dtype == numpy.uint8
    assert tuple(data[45, 256, 256]) == (93, 56, 56)
    if not SKIP_ZARR:
        with tifs.aszarr() as store:
            assert_array_equal(data, zarr.open(store, mode='r'))
    del data


@pytest.mark.skipif(SKIP_FILE or SKIP_CODECS, reason=REASON)
def test_sequence_zip_container():
    """Test TiffSequence with glob pattern without axes pattern."""
    filename = _file('TiffSequence.zip')
    with TiffSequence('*.tif', container=filename, pattern=None) as tifs:
        assert len(tifs) == 10
        assert tifs.shape == (10,)
        assert tifs.axes == 'I'
        data = tifs.asarray(chunkshape=(480, 640), chunkdtype=numpy.uint8)
        assert isinstance(data, numpy.ndarray)
        assert data.flags['C_CONTIGUOUS']
        assert data.shape == (10, 480, 640)
        assert data.dtype == numpy.uint8
        assert data[9, 256, 256] == 135
    assert_array_equal(data, imread(container=filename))


@pytest.mark.skipif(SKIP_FILE or SKIP_LARGE or SKIP_CODECS, reason=REASON)
def test_sequence_wells_axesorder():
    """Test FileSequence with well plates and axes reorder."""
    # https://bbbc.broadinstitute.org/BBBC006
    categories = {'p': {chr(i + 97): i for i in range(25)}}
    ptrn = r'(?:_(z)_(\d+)).*_(?P<p>[a-z])(?P<a>\d+)(?:_(s)(\d))(?:_(w)(\d))'
    filenames = _files('BBBC/BBBC006_v1_images_z_00/*.tif')
    filenames += _files('BBBC/BBBC006_v1_images_z_01/*.tif')
    tifs = TiffSequence(
        filenames,
        pattern=ptrn,
        categories=categories,
        axesorder=(1, 2, 0, 3, 4),
    )
    assert len(tifs) == 3072
    assert tifs.shape == (16, 24, 2, 2, 2)
    assert tifs.axes == 'PAZSW'
    data = tifs.asarray(ioworkers=16, maxworkers=1)
    assert isinstance(data, numpy.ndarray)
    assert data.flags['C_CONTIGUOUS']
    assert data.shape == (16, 24, 2, 2, 2, 520, 696)
    assert data.dtype == numpy.uint16
    assert data[8, 12, 1, 0, 1, 256, 519] == 1579
    if not SKIP_ZARR:
        with tifs.aszarr(ioworkers=16, maxworkers=1) as store:
            assert_array_equal(data, zarr.open(store, mode='r'))


@pytest.mark.skipif(SKIP_FILE or SKIP_LARGE, reason=REASON)
@pytest.mark.parametrize('tiled', [False, True])
def test_sequence_tiled(tiled):
    """Test FileSequence with tiled OME-TIFFs."""
    # Dataset from https://github.com/tlambert03/tifffolder/issues/2
    ptrn = re.compile(
        r'\[(?P<U>\d+) x (?P<V>\d+)\].*(C)(\d+).*(Z)(\d+)', re.IGNORECASE
    )
    filenames = _file('TiffSequenceTiled/*.tif')
    tifs = TiffSequence(filenames, pattern=ptrn)
    assert len(tifs) == 60
    assert tifs.shape == (2, 3, 2, 5)
    assert tifs.axes == 'UVCZ'
    tiled = {0: 0, 1: 1} if tiled else None
    data = tifs.asarray(
        axestiled=tiled, is_ome=False, ioworkers=16, maxworkers=1
    )
    assert isinstance(data, numpy.ndarray)
    assert data.flags['C_CONTIGUOUS']
    assert data.dtype == numpy.uint16
    if tiled:
        assert data.shape == (2, 5, 2 * 2560, 3 * 2160)
        assert data[1, 3, 2560 + 1024, 2 * 2160 + 1024] == 596
    else:
        assert data.shape == (2, 3, 2, 5, 2560, 2160)
        assert data[1, 2, 1, 3, 1024, 1024] == 596
    if not SKIP_ZARR:
        with tifs.aszarr(
            axestiled=tiled, is_ome=False, ioworkers=16, maxworkers=1
        ) as store:
            if tiled:
                assert_array_equal(
                    data[1, 3, 2048:3072],
                    zarr.open(store, mode='r')[1, 3, 2048:3072],
                )
            else:
                assert_array_equal(
                    data[1, 2, 1, 3:5],
                    zarr.open(store, mode='r')[1, 2, 1, 3:5],
                )


@pytest.mark.skipif(SKIP_FILE or SKIP_CODECS, reason=REASON)
def test_sequence_imread():
    """Test TiffSequence with imagecodecs.imread."""
    filename = _files('PNG/*.png')
    pngs = TiffSequence(filename, imread=imagecodecs.imread)
    assert len(pngs) == 4
    assert pngs.shape == (4,)
    assert pngs.axes == 'I'
    data = pngs.asarray(codec=imagecodecs.png_decode)
    assert data.flags['C_CONTIGUOUS']
    assert data.shape == (4, 200, 200)
    assert data.dtype == numpy.uint16
    if not SKIP_ZARR:
        with pngs.aszarr(codec=imagecodecs.png_decode) as store:
            assert_array_equal(data, zarr.open(store, mode='r'))
    del data


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_sequence_imread_list():
    """Test imread with list of files."""
    files = _files('Nuvu/Sequence/*.tif')
    pattern = os.path.join(DATA_DIR, 'Nuvu/Sequence/*.tif')

    # list not sorted by default
    assert imread(files)[-1, 256, 256] == 1682
    assert imread(files, sort=False)[-1, 256, 256] == 1682
    # list sorted
    assert imread(files, sort=True)[-1, 256, 256] == 1684
    assert imread(files, sort=natural_sorted)[-1, 256, 256] == 1684
    # glob sorted by default
    assert imread(pattern)[-1, 256, 256] == 1684
    assert imread(pattern, sort=True)[-1, 256, 256] == 1684
    assert imread(pattern, sort=natural_sorted)[-1, 256, 256] == 1684
    # glob not sorted
    assert imread(pattern, sort=False)[-1, 256, 256] == 1682


@pytest.mark.skipif(SKIP_FILE or SKIP_CODECS, reason=REASON)
def test_sequence_imread_glob():
    """Test imread function with glob pattern."""
    filename = _files('TiffSequence/*.tif')
    data = imread(
        filename,
        imreadargs={'key': 0},
        chunkshape=(480, 640),
        chunkdtype=numpy.uint8,
    )
    assert data.shape == (10, 480, 640)
    if not SKIP_ZARR:
        store = imread(
            filename,
            aszarr=True,
            imreadargs={'key': 0},
            chunkshape=(480, 640),
            chunkdtype=numpy.uint8,
        )
        try:
            assert_array_equal(data, zarr.open(store, mode='r'))
        finally:
            store.close()


@pytest.mark.skipif(SKIP_FILE or SKIP_CODECS, reason=REASON)
def test_sequence_imread_one():
    """Test imread function with one item in sequence."""
    filename = _file('TiffSequence/AT3_1m4_01.tif')
    assert_array_equal(imread([filename]), imread(filename))


###############################################################################

# Test packages depending on tifffile


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_dependent_roifile():
    """Test roifile.ImagejRoi class."""
    roifile = pytest.importorskip('roifile')

    for roi in roifile.ImagejRoi.fromfile(_file('imagej/IJMetadata.tif')):
        assert roi == roifile.ImagejRoi.frombytes(roi.tobytes())
        roi.coordinates()
        str(roi)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_dependent_lfdfiles():
    """Test lfdfiles conversion to TIFF."""
    lfdfiles = pytest.importorskip('lfdfiles')

    filename = _file('SimFCS/simfcs.Z64')
    with TempFileName('depend_simfcsz_z64', ext='.tif') as outfile:
        with lfdfiles.SimfcsZ64(filename) as z64:
            data = z64.asarray()
            z64.totiff(outfile)
        with TiffFile(outfile) as tif:
            assert len(tif.pages) == 256
            assert len(tif.series) == 1
            assert tif.series[0].shape == (256, 256, 256)
            assert tif.series[0].dtype == numpy.float32
            assert_array_equal(data, tif.asarray())

    filename = _file('SimFCS/gpint')
    with lfdfiles.LfdFileSequence(
        filename + '/v*001.int',
        pattern=r'v(?P<Channel>\d)(?P<Image>\d*).int',
        imread=lfdfiles.SimfcsInt,
    ) as ims:
        assert ims.axes == 'CI'
        assert ims.asarray().shape == (2, 1, 256, 256)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_dependent_cmapfile():
    """Test cmapfile.lsm2cmap."""
    cmapfile = pytest.importorskip('cmapfile')

    filename = _file('LSM/3d_zfish_onephoton_zoom.lsm')
    data = imread(filename)
    with TempFileName('depend_cmapfile', ext='.cmap') as cfile:
        cmapfile.lsm2cmap(filename, cfile, step=(1.0, 1.0, 20.0))
        filename = os.path.join(
            os.path.split(cfile)[0],
            cfile.replace('.cmap', '.ch0000.cmap'),
        )
        with cmapfile.CmapFile(filename, mode='r') as cmap:
            assert_array_equal(
                cmap['map00000']['data00000'], data.squeeze()[:, 0, :, :]
            )


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_dependent_czifile():
    """Test czifile.CziFile."""
    # TODO: test LZW compressed czi file
    czifile = pytest.importorskip('czifile')

    filename = _file('czi/pollen.czi')
    # with pytest.warns(DeprecationWarning):
    with czifile.CziFile(filename) as czi:
        assert czi.shape == (1, 1, 104, 365, 364, 1)
        assert czi.axes == 'TCZYX0'
        # verify data
        data = czi.asarray()
        assert data.flags['C_CONTIGUOUS']
        assert data.shape == (1, 1, 104, 365, 364, 1)
        assert data.dtype == numpy.uint8
        assert data[0, 0, 52, 182, 182, 0] == 10


@pytest.mark.skipif(SKIP_FILE or SKIP_LARGE, reason=REASON)
def test_dependent_czi2tif():
    """Test czifile.czi2tif."""
    czifile = pytest.importorskip('czifile.czifile')

    filename = _file('CZI/pollen.czi')
    # with pytest.warns(DeprecationWarning):
    with czifile.CziFile(filename) as czi:
        metadata = czi.metadata()
        data = czi.asarray().squeeze()
    with TempFileName('depend_czi2tif') as tif:
        czifile.czi2tif(filename, tif, bigtiff=False)
        with TiffFile(tif) as t:
            im = t.asarray()
            assert t.pages[0].description == metadata

        assert_array_equal(im, data)
        del im
        del data
        assert_valid_tiff(tif)


@pytest.mark.skipif(SKIP_FILE or SKIP_LARGE, reason=REASON)
def test_dependent_czi2tif_airy():
    """Test czifile.czi2tif with AiryScan."""
    czifile = pytest.importorskip('czifile.czifile')

    filename = _file('CZI/AiryscanSRChannel.czi')
    with TempFileName('depend_czi2tif_airy') as tif:
        # with pytest.warns(DeprecationWarning):
        czifile.czi2tif(
            filename, tif, verbose=True, truncate=True, bigtiff=False
        )
        im = memmap(tif)
        assert im.shape == (32, 6, 1680, 1680)
        assert tuple(im[17, :, 1500, 1000]) == (95, 109, 3597, 0, 0, 0)
        del im
        assert_valid_tiff(tif)


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_dependent_oiffile():
    """Test oiffile.OifFile."""
    oiffile = pytest.importorskip('oiffile')

    filename = _file(
        'oib/MB231cell1_paxgfp_PDMSgasket_PMMAflat_30nm_378sli.oib'
    )
    with oiffile.OifFile(filename) as oib:
        assert oib.is_oib
        tifs = oib.series[0]
        assert len(tifs) == 756
        assert tifs.shape == (2, 378)
        assert tifs.axes == 'CZ'
        # verify data
        data = tifs.asarray(out='memmap')
        assert data.flags['C_CONTIGUOUS']
        assert data.shape == (2, 378, 256, 256)
        assert data.dtype == numpy.dtype('<u2')
        assert data[1, 377, 70, 146] == 29
        del data


@pytest.mark.skipif(SKIP_FILE or SKIP_LARGE, reason=REASON)
@pytest.mark.xfail(reason='not compatible with Zarr 3')
def test_dependent_tiffslide():
    """Test tiffslide package."""
    # https://github.com/bayer-science-for-a-better-life/tiffslide
    tiffslide = pytest.importorskip('tiffslide', exc_type=ImportError)

    filename = _file('AperioSVS/CMU-1.svs')
    with tiffslide.TiffSlide(filename) as slide:
        region = slide.read_region((300, 400), 0, (512, 512), as_array=True)
        assert tuple(region[150, 200]) == (243, 246, 246)
        slide.get_thumbnail((200, 200))


@pytest.mark.skipif(SKIP_FILE or SKIP_LARGE or not IS_WIN, reason=REASON)
def test_dependent_opentile():
    """Test opentile package."""
    # https://github.com/imi-bigpicture/opentile
    try:
        from hashlib import md5

        import turbojpeg
        from opentile.formats import NdpiTiler
        from opentile.geometry import Point
    except ImportError:
        pytest.skip('opentile missing')

    filename = _file('HamamatsuNDPI/CMU-1.ndpi')

    turbo_path = os.path.join(
        os.path.dirname(turbojpeg.__file__), 'turbojpeg.dll'
    )

    with NdpiTiler(
        filename,
        512,
        turbo_path=turbo_path,
    ) as tiler:
        # from example
        tile = tiler.get_tile(0, 0, 0, (0, 0))
        assert md5(tile).hexdigest() in (
            '30c69cab610e5b3db4beac63806d6513',
            'e813041cd57d1d078cf1564c0ed9ad7f',
        )
        # read from file handle
        level = tiler.get_level(0)
        offset = level._page.dataoffsets[50]
        length = level._page.databytecounts[50]
        fh = level._page.parent.filehandle
        fh.seek(offset)
        data = fh.read(length)
        assert length == 700
        assert offset == 33451
        assert md5(data).hexdigest() == '2a903c6e05bd10f10d856eecceb591f0'
        # read level
        data = level._read_frame(50)
        assert md5(data).hexdigest() == '2a903c6e05bd10f10d856eecceb591f0'
        # read frame
        index = level._get_stripe_position_to_index(Point(50, 0))
        stripe = level._read_frame(index)
        assert md5(stripe).hexdigest() == '2a903c6e05bd10f10d856eecceb591f0'
        # get frame
        image = level._read_extended_frame(Point(10, 10), level.frame_size)
        assert md5(image).hexdigest() == 'aeffd12997ca6c232d0ef35aaa35f6b7'
        # get tile
        tile = level.get_tile((0, 0))
        assert md5(tile).hexdigest() in (
            '30c69cab610e5b3db4beac63806d6513',
            'e813041cd57d1d078cf1564c0ed9ad7f',
        )
        tile = level.get_tile((20, 20))
        assert md5(tile).hexdigest() in (
            'fec8116d05485df513f4f41e13eaa994',
            'e755ee12d9fd65fc601e70951ce90696',
        )


@pytest.mark.skipif(SKIP_FILE or SKIP_DASK, reason=REASON)
def test_dependent_aicsimageio():
    """Test aicsimageio package."""
    # https://github.com/AllenCellModeling/aicsimageio
    aicsimageio = pytest.importorskip('aicsimageio')

    filename = _file('tifffile/multiscene_pyramidal.ome.tif')
    img = aicsimageio.AICSImage(filename)
    assert img.shape == (16, 2, 32, 256, 256)
    assert img.dims.order == 'TCZYX'
    assert img.dims.X == 256
    assert img.data.shape == (16, 2, 32, 256, 256)
    assert img.xarray_data.dims == ('T', 'C', 'Z', 'Y', 'X')
    t1 = img.get_image_data('ZCYX', T=1)
    assert t1.shape == (32, 2, 256, 256)
    assert img.current_scene == 'Image:0'
    assert img.scenes == ('Image:0', 'Image:1')
    img.set_scene(1)
    assert img.current_scene == 'Image:1'
    assert img.shape == (1, 1, 1, 128, 128, 3)
    img.set_scene('Image:0')
    lazy_t1 = img.get_image_dask_data('ZCYX', T=1)
    assert_array_equal(lazy_t1.compute(), t1)


@pytest.mark.xfail(reason=REASON)
@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_dependent_imageio():
    """Test imageio package."""
    # https://github.com/imageio/imageio/blob/master/tests/test_tifffile.py
    # TODO: use imageio.v3
    try:
        import imageio.v2 as iio
    except ImportError:
        pytest.skip('imageio missing')

    data = numpy.ones((10, 10, 3), numpy.uint8) * 2
    filename2 = _file('imageio/multipage_rgb.tif')
    filename3 = _file('imageio/test_tiff2.tiff')

    with TempFileName('depend_imageio') as filename1:
        # one image
        iio.imwrite(filename1, data)
        im = iio.imread(filename1)
        ims = iio.mimread(filename1)
        assert im.shape == data.shape
        assert_array_equal(im, data)
        assert len(ims) == 1

        # multiple images
        iio.mimwrite(filename1, [data, data, data])
        im = iio.imread(filename1)
        ims = iio.mimread(filename1)
        assert im.shape == data.shape
        assert_array_equal(im, data)
        assert len(ims) == 3
        for i in range(3):
            assert ims[i].shape == data.shape
            assert_array_equal(ims[i], data)

        # volumetric data
        iio.volwrite(filename1, numpy.tile(data, (3, 1, 1, 1)))
        vol = iio.volread(filename1)
        vols = iio.mvolread(filename1)
        assert vol.shape == (3, *data.shape)
        assert len(vols) == 1
        assert vol.shape == vols[0].shape
        for i in range(3):
            assert_array_equal(vol[i], data)

        # remote channel-first volume rgb (2, 3, 10, 10)

        img = iio.mimread(filename2)
        assert len(img) == 2
        assert img[0].shape == (3, 10, 10)

        # mixed
        W = iio.save(filename1)
        W.set_meta_data({'planarconfig': 'SEPARATE'})
        assert W.format.name == 'TIFF'
        W.append_data(data)
        W.append_data(data)
        W.close()

        R = iio.read(filename1)
        assert R.format.name == 'TIFF'
        ims = list(R)  # == [im for im in R]
        assert_array_equal(ims[0], data)
        # fail
        with pytest.raises(IndexError):
            R.get_data(-1)

        with pytest.raises(IndexError):
            R.get_data(3)

        # ensure imread + imwrite works round trip
        im1 = iio.imread(filename1)
        iio.imwrite(filename3, im1)
        im3 = iio.imread(filename3)
        assert im1.ndim == 3
        assert im1.shape == im3.shape
        assert_array_equal(im1, im3)

        # ensure imread + imwrite works round trip - volume like
        im1 = numpy.stack(iio.mimread(filename1))
        iio.volwrite(filename3, im1)
        im3 = iio.volread(filename3)
        assert im1.ndim == 4
        assert im1.shape == im3.shape
        assert_array_equal(im1, im3)

        # read metadata
        md = iio.get_reader(filename2).get_meta_data()
        assert not md['is_imagej']
        assert md['description'] == 'shape=(2,3,10,10)'
        assert md['description1'] == ''
        assert md['datetime'] == datetime.datetime(2015, 5, 9, 9, 8, 29)
        assert md['software'] == 'tifffile.py'

        # write metadata
        dt = datetime.datetime(2018, 8, 6, 15, 35, 5)
        with iio.get_writer(filename1, software='testsoftware') as w:
            w.append_data(
                numpy.zeros((10, 10)),
                meta={'description': 'test desc', 'datetime': dt},
            )
            w.append_data(
                numpy.zeros((10, 10)), meta={'description': 'another desc'}
            )
        with iio.get_reader(filename1) as r:
            for md in r.get_meta_data(), r.get_meta_data(0):
                assert 'datetime' in md
                assert md['datetime'] == dt
                assert 'software' in md
                assert md['software'] == 'testsoftware'
                assert 'description' in md
                assert md['description'] == 'test desc'

            md = r.get_meta_data(1)
            assert 'description' in md
            assert md['description'] == 'another desc'


@pytest.mark.skipif(SKIP_FILE, reason=REASON)
def test_dependent_pims():
    """Test PIMS package."""
    # https://github.com/soft-matter/pims
    pims = pytest.importorskip('pims')

    filename = _file('pims/tiff_stack.tif')
    ims = pims.TiffStack_tifffile(filename)
    assert_array_equal(ims.get_frame(2), imread(filename, key=2))
    repr(ims)
    ims.close()


@pytest.mark.skipif(SKIP_FILE or SKIP_ZARR, reason=REASON)
def test_dependent_kerchunk():
    """Test kerchunk package."""
    # https://github.com/fsspec/kerchunk/blob/main/tests/test_tiff.py
    kerchunk_tiff = pytest.importorskip('kerchunk.tiff')

    filename = _file('kerchunk/lcmap_tiny_cog_2019.tif')
    filename = pathlib.Path(filename).as_uri()
    store = refs_as_store(kerchunk_tiff.tiff_to_zarr(filename))
    z = zarr.open(store, zarr_format=2)
    assert list(z) == ['0', '1', '2']
    assert z.attrs['multiscales'] == [
        {
            'datasets': [{'path': '0'}, {'path': '1'}, {'path': '2'}],
            'metadata': {},
            'name': '',
            'version': '0.1',
        }
    ]
    assert z['0'].shape == (2048, 2048)
    assert z['0'][:].max() == 8
    assert len(list(z.groups())) == 0
    assert len(list(z.arrays())) == 3
    assert z.tree()

    filename = _file('kerchunk/example.tif')
    filename = pathlib.Path(filename).as_uri()
    out = kerchunk_tiff.tiff_to_zarr(filename)
    store = refs_as_store(out)
    z = zarr.open(store, zarr_format=2)


@pytest.mark.skipif(not hasattr(sys, '_is_gil_enabled'), reason=REASON)
def test_gil_enabled():
    """Test that GIL is disabled on thread-free Python."""
    assert sys._is_gil_enabled() != sysconfig.get_config_var('Py_GIL_DISABLED')


###############################################################################

if __name__ == '__main__':
    import warnings

    # warnings.simplefilter('always')
    warnings.filterwarnings('ignore', category=ImportWarning)
    argv = sys.argv
    argv.append('--cov-report=html')
    argv.append('--cov=tifffile')
    argv.append('--verbose')
    sys.exit(pytest.main(argv))

# mypy: allow-untyped-defs
# mypy: check-untyped-defs=False
