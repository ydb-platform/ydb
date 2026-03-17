"""Classes capable of reading and writing datasets

Instances of these classes are called dataset objects.
"""

import logging
import warnings

from rasterio._base import get_dataset_driver, driver_can_create, driver_can_create_copy
from rasterio._io import (
    DatasetReaderBase,
    DatasetWriterBase,
    BufferedDatasetWriterBase,
    MemoryFileBase,
)
from rasterio.windows import WindowMethodsMixin
from rasterio.env import ensure_env
from rasterio.errors import RasterioDeprecationWarning
from rasterio.transform import TransformMethodsMixin
from rasterio._path import _UnparsedPath

try:
    from rasterio._filepath import FilePathBase
except ImportError:
    FilePathBase = object


log = logging.getLogger(__name__)


class DatasetReader(DatasetReaderBase, WindowMethodsMixin, TransformMethodsMixin):
    """An unbuffered data and metadata reader"""

    def __repr__(self):
        return "<{} DatasetReader name='{}' mode='{}'>".format(
            self.closed and 'closed' or 'open', self.name, self.mode)


class DatasetWriter(DatasetWriterBase, WindowMethodsMixin, TransformMethodsMixin):
    """An unbuffered data and metadata writer. Its methods write data
    directly to disk.
    """

    def __repr__(self):
        return "<{} DatasetWriter name='{}' mode='{}'>".format(
            self.closed and 'closed' or 'open', self.name, self.mode)


class BufferedDatasetWriter(
    BufferedDatasetWriterBase, WindowMethodsMixin, TransformMethodsMixin
):
    """Maintains data and metadata in a buffer, writing to disk or
    network only when `close()` is called.

    This allows incremental updates to datasets using formats that don't
    otherwise support updates, such as JPEG.
    """

    def __repr__(self):
        return "<{} BufferedDatasetWriter name='{}' mode='{}'>".format(
            self.closed and 'closed' or 'open', self.name, self.mode)


class MemoryFile(MemoryFileBase):
    """A BytesIO-like object, backed by an in-memory file.

    This allows formatted files to be read and written without I/O.

    A MemoryFile created with initial bytes becomes immutable. A
    MemoryFile created without initial bytes may be written to using
    either file-like or dataset interfaces.

    Examples
    --------

    A GeoTIFF can be loaded in memory and accessed using the GeoTIFF
    format driver

    >>> with open('tests/data/RGB.byte.tif', 'rb') as f, MemoryFile(f) as memfile:
    ...     with memfile.open() as src:
    ...         pprint.pprint(src.profile)
    ...
    {'count': 3,
     'crs': CRS({'init': 'epsg:32618'}),
     'driver': 'GTiff',
     'dtype': 'uint8',
     'height': 718,
     'interleave': 'pixel',
     'nodata': 0.0,
     'tiled': False,
     'transform': Affine(300.0379266750948, 0.0, 101985.0,
           0.0, -300.041782729805, 2826915.0),
     'width': 791}

    """

    def __init__(self, file_or_bytes=None, dirname=None, filename=None, ext=".tif"):
        """Create a new file in memory

        Parameters
        ----------
        file_or_bytes : file-like object or bytes, optional
            File or bytes holding initial data.
        filename : str, optional
            An optional filename. A unique one will otherwise be generated.
        ext : str, optional
            An optional extension.

        Returns
        -------
        MemoryFile
        """
        super().__init__(
            file_or_bytes=file_or_bytes, dirname=dirname, filename=filename, ext=ext
        )

    @ensure_env
    def open(self, driver=None, width=None, height=None, count=None, crs=None,
             transform=None, dtype=None, nodata=None, sharing=False, thread_safe=False, **kwargs):
        """Open the file and return a Rasterio dataset object.

        If data has already been written, the file is opened in 'r'
        mode. Otherwise, the file is opened in 'w' mode.

        Parameters
        ----------
        Note well that there is no `path` parameter: a `MemoryFile`
        contains a single dataset and there is no need to specify a
        path.

        Other parameters are optional and have the same semantics as the
        parameters of `rasterio.open()`.
        """
        mempath = _UnparsedPath(self.name)

        if self.closed:
            raise ValueError("I/O operation on closed file.")
        if len(self) > 0:
            log.debug(f"VSI path: {mempath.path}")
            rd = DatasetReader(mempath, driver=driver, sharing=sharing, thread_safe=thread_safe, **kwargs)
        else:
            writer = get_writer_for_driver(driver)
            rd = writer(
                mempath,
                "w+",
                driver=driver,
                width=width,
                height=height,
                count=count,
                crs=crs,
                transform=transform,
                dtype=dtype,
                nodata=nodata,
                sharing=sharing,
                **kwargs
            )

        # Push the new dataset's context exit onto the MemoryFile's ExitStack.
        # This ensures that when this MemoryFile is closed, any derived dataset
        # is also closed automatically.
        self._env.push(rd)
        return rd

    def __enter__(self):
        return self

    def __exit__(self, *args):
        if self._env:
            self._env.close()
        self.close()


class _FilePath(FilePathBase):
    """A BytesIO-like object, backed by a Python file object.

    .. deprecated:: 1.4.0
       FilePath is supplanted by open's new opener keyword argument,
       and will be removed in 2.0.0.

    """
    def __init__(self, filelike_obj, dirname=None, filename=None):
        """Create a new wrapper around the provided file-like object.

        Parameters
        ----------
        filelike_obj : file-like object
            Open file-like object. Currently only reading is supported.
        filename : str, optional
            An optional filename. A unique one will otherwise be generated.

        Returns
        -------
        PythonVSIFile
        """
        warnings.warn(
            "FilePath is supplanted by open's new opener keyword argument, and will be removed in 2.0.0.",
            RasterioDeprecationWarning,
        )
        super().__init__(
            filelike_obj, dirname=dirname, filename=filename
        )

    @ensure_env
    def open(self, driver=None, sharing=False, thread_safe=False, **kwargs):
        """Open the file and return a Rasterio dataset object.

        The provided file-like object is assumed to be readable.
        Writing is currently not supported.

        Parameters are optional and have the same semantics as the
        parameters of `rasterio.open()`.

        Returns
        -------
        DatasetReader

        Raises
        ------
        IOError
            If the memory file is closed.

        """
        mempath = _UnparsedPath(self.name)

        if self.closed:
            raise OSError("I/O operation on closed file.")

        # Assume we were given a non-empty file-like object
        log.debug(f"VSI path: {mempath.path}")

        return DatasetReader(mempath, driver=driver, sharing=sharing, thread_safe=thread_safe, **kwargs)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


if FilePathBase is not object:
    # only make this object available if the cython extension was compiled
    FilePath = _FilePath


class ZipMemoryFile(MemoryFile):
    """A read-only BytesIO-like object backed by an in-memory zip file.

    This allows a zip file containing formatted files to be read
    without I/O.
    """

    def __init__(self, file_or_bytes=None):
        super().__init__(file_or_bytes, ext="zip")

    @ensure_env
    def open(self, path, driver=None, sharing=False, thread_safe=False, **kwargs):
        """Open a dataset within the zipped stream.

        Parameters
        ----------
        path : str
            Path to a dataset in the zip file, relative to the root of the
            archive.

        Other parameters are optional and have the same semantics as the
        parameters of `rasterio.open()`.

        Returns
        -------
        A Rasterio dataset object
        """
        zippath = _UnparsedPath("/vsizip{}/{}".format(self.name, path.lstrip("/")))

        if self.closed:
            raise ValueError("I/O operation on closed file.")
        return DatasetReader(zippath, driver=driver, sharing=sharing, thread_safe=thread_safe, **kwargs)


def get_writer_for_driver(driver):
    """Return the writer class appropriate for the specified driver."""
    if not driver:
        raise ValueError("'driver' is required to read/write dataset.")
    cls = None
    if driver_can_create(driver):
        cls = DatasetWriter
    elif driver_can_create_copy(driver):  # pragma: no branch
        cls = BufferedDatasetWriter
    return cls


def get_writer_for_path(path, driver=None):
    """Return the writer class appropriate for the existing dataset."""
    if not driver:
        driver = get_dataset_driver(path)
    return get_writer_for_driver(driver)
