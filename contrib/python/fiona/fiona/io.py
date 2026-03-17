"""Classes capable of reading and writing collections
"""

import logging

from fiona.ogrext import MemoryFileBase, _listdir, _listlayers
from fiona.collection import Collection
from fiona.meta import supports_vsi
from fiona.errors import DriverError

log = logging.getLogger(__name__)


class MemoryFile(MemoryFileBase):
    """A BytesIO-like object, backed by an in-memory file.

    This allows formatted files to be read and written without I/O.

    A MemoryFile created with initial bytes becomes immutable. A
    MemoryFile created without initial bytes may be written to using
    either file-like or dataset interfaces.

    Parameters
    ----------
    file_or_bytes : an open Python file, bytes, or None
        If not None, the MemoryFile becomes immutable and read-only.
        If None, it is write-only.
    filename : str
        An optional filename. The default is a UUID-based name.
    ext : str
        An optional file extension. Some format drivers require a
        specific value.

    """
    def __init__(self, file_or_bytes=None, filename=None, ext=""):
        if ext and not ext.startswith("."):
            ext = "." + ext
        super().__init__(
            file_or_bytes=file_or_bytes, filename=filename, ext=ext)

    def open(
        self,
        mode=None,
        driver=None,
        schema=None,
        crs=None,
        encoding=None,
        layer=None,
        vfs=None,
        enabled_drivers=None,
        crs_wkt=None,
        allow_unsupported_drivers=False,
        **kwargs
    ):
        """Open the file and return a Fiona collection object.

        If data has already been written, the file is opened in 'r'
        mode. Otherwise, the file is opened in 'w' mode.

        Parameters
        ----------
        Note well that there is no `path` parameter: a `MemoryFile`
        contains a single dataset and there is no need to specify a
        path.

        Other parameters are optional and have the same semantics as the
        parameters of `fiona.open()`.
        """
        if self.closed:
            raise OSError("I/O operation on closed file.")

        if (
            not allow_unsupported_drivers
            and driver is not None
            and not supports_vsi(driver)
        ):
            raise DriverError(f"Driver {driver} does not support virtual files.")

        if mode in ('r', 'a') and not self.exists():
            raise OSError("MemoryFile does not exist.")
        if layer is None and mode == 'w' and self.exists():
            raise OSError("MemoryFile already exists.")

        if not self.exists() or mode == 'w':
            if driver is not None:
                self._ensure_extension(driver)
            mode = 'w'
        elif mode is None:
            mode = 'r'

        return Collection(
            self.name,
            mode,
            crs=crs,
            driver=driver,
            schema=schema,
            encoding=encoding,
            layer=layer,
            enabled_drivers=enabled_drivers,
            allow_unsupported_drivers=allow_unsupported_drivers,
            crs_wkt=crs_wkt,
            **kwargs
        )

    def listdir(self, path=None):
        """List files in a directory.

        Parameters
        ----------
        path : URI (str or pathlib.Path)
            A dataset resource identifier.

        Returns
        -------
        list
            A list of filename strings.

        """
        if self.closed:
            raise OSError("I/O operation on closed file.")
        if path:
            vsi_path = f"{self.name}/{path.lstrip('/')}"
        else:
            vsi_path = f"{self.name}"
        return _listdir(vsi_path)

    def listlayers(self, path=None):
        """List layer names in their index order

        Parameters
        ----------
        path : URI (str or pathlib.Path)
            A dataset resource identifier.

        Returns
        -------
        list
            A list of layer name strings.

        """
        if self.closed:
            raise OSError("I/O operation on closed file.")
        if path:
            vsi_path = f"{self.name}/{path.lstrip('/')}"
        else:
            vsi_path = f"{self.name}"
        return _listlayers(vsi_path)

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        self.close()


class ZipMemoryFile(MemoryFile):
    """A read-only BytesIO-like object backed by an in-memory zip file.

    This allows a zip file containing formatted files to be read
    without I/O.

    Parameters
    ----------
    file_or_bytes : an open Python file, bytes, or None
        If not None, the MemoryFile becomes immutable and read-only. If
        None, it is write-only.
    filename : str
        An optional filename. The default is a UUID-based name.
    ext : str
        An optional file extension. Some format drivers require a
        specific value. The default is ".zip".
    """

    def __init__(self, file_or_bytes=None, filename=None, ext=".zip"):
        super().__init__(file_or_bytes, filename=filename, ext=ext)
        self.name = f"/vsizip{self.name}"

    def open(
        self,
        path=None,
        driver=None,
        encoding=None,
        layer=None,
        enabled_drivers=None,
        allow_unsupported_drivers=False,
        **kwargs
    ):
        """Open a dataset within the zipped stream.

        Parameters
        ----------
        path : str
            Path to a dataset in the zip file, relative to the root of the
            archive.

        Returns
        -------
        A Fiona collection object

        """
        if path:
            vsi_path = f"/vsizip{self.name}/{path.lstrip('/')}"
        else:
            vsi_path = f"/vsizip{self.name}"

        if self.closed:
            raise OSError("I/O operation on closed file.")
        if path:
            vsi_path = f"{self.name}/{path.lstrip('/')}"
        else:
            vsi_path = f"{self.name}"

        return Collection(
            vsi_path,
            "r",
            driver=driver,
            encoding=encoding,
            layer=layer,
            enabled_drivers=enabled_drivers,
            allow_unsupported_drivers=allow_unsupported_drivers,
            **kwargs
        )
