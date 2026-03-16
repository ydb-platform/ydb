from __future__ import annotations

import base64
import gzip
import http.client as httplib
import logging
import mimetypes
import sys
import tempfile
import threading
import urllib.parse as urlparse
from abc import abstractmethod
from io import BytesIO
from pathlib import Path
from tempfile import _TemporaryFileWrapper
from typing import TYPE_CHECKING, Any, Callable, ClassVar
from urllib import request
from urllib.parse import unquote as urllib_unquote

from xhtml2pdf.config.httpconfig import httpConfig

if TYPE_CHECKING:
    from http.client import HTTPResponse
    from urllib.parse import SplitResult

log = logging.getLogger(__name__)

GAE: bool = "google.appengine" in sys.modules
STRATEGIES: tuple[type, Any] = (
    (BytesIO, BytesIO) if GAE else (BytesIO, tempfile.NamedTemporaryFile)
)


class TmpFiles(threading.local):
    files: ClassVar[list[_TemporaryFileWrapper[bytes]]] = []

    def append(self, file) -> None:
        self.files.append(file)

    def cleanFiles(self) -> None:
        for file in self.files:
            file.close()
        self.files.clear()


files_tmp: TmpFiles = TmpFiles()  # permanent safe file, to prevent file close


class pisaTempFile:
    """
    A temporary file implementation that uses memory unless
    either capacity is breached or fileno is requested, at which
    point a real temporary file will be created and the relevant
    details returned
    If capacity is -1 the second strategy will never be used.
    Inspired by:
    http://code.activestate.com/recipes/496744/.
    """

    STRATEGIES = STRATEGIES

    CAPACITY: int = 10 * 1024

    def __init__(self, buffer: str = "", capacity: int = CAPACITY) -> None:
        """
        Creates a TempFile object containing the specified buffer.
        If capacity is specified, we use a real temporary file once the
        file gets larger than that size.  Otherwise, the data is stored
        in memory.
        """
        self.name: str | None = None
        self.capacity: int = capacity
        self.strategy: int = int(len(buffer) > self.capacity)
        try:
            self._delegate = self.STRATEGIES[self.strategy]()
        except IndexError:
            # Fallback for Google AppEngine etc.
            self._delegate = self.STRATEGIES[0]()
        self.write(buffer)
        # we must set the file's position for preparing to read
        self.seek(0)

    def makeTempFile(self) -> None:
        """
        Switch to next strategy. If an error occurred,
        stay with the first strategy.
        """
        if self.strategy == 0:
            try:
                new_delegate = self.STRATEGIES[1]()
                new_delegate.write(self.getvalue())
                self._delegate = new_delegate
                self.strategy = 1
                log.warning("Created temporary file %s", self.name)
            except Exception:
                self.capacity = -1

    def getFileName(self) -> str | None:
        """Get a named temporary file."""
        self.makeTempFile()
        return self.name

    def fileno(self) -> int:
        """
        Forces this buffer to use a temporary file as the underlying.
        object and returns the fileno associated with it.
        """
        self.makeTempFile()
        return self._delegate.fileno()

    def getvalue(self) -> bytes:
        """
        Get value of file. Work around for second strategy.
        Always returns bytes.
        """
        if self.strategy == 0:
            return self._delegate.getvalue()
        self._delegate.flush()
        self._delegate.seek(0)
        value = self._delegate.read()
        if not isinstance(value, bytes):
            value = value.encode("utf-8")
        return value

    def write(self, value: bytes | str):
        """If capacity != -1 and length of file > capacity it is time to switch."""
        if self.capacity > 0 and self.strategy == 0:
            len_value = len(value)
            if len_value >= self.capacity:
                needs_new_strategy = True
            else:
                self.seek(0, 2)  # find end of file
                needs_new_strategy = (self.tell() + len_value) >= self.capacity
            if needs_new_strategy:
                self.makeTempFile()

        if not isinstance(value, bytes):
            value = value.encode("utf-8")

        self._delegate.write(value)

    def __getattr__(self, name: str) -> Any:
        try:
            return getattr(self._delegate, name)
        except AttributeError as e:
            msg = f"object '{type(self).__name__}' has no attribute '{name}'"
            raise AttributeError(msg) from e


class BaseFile:
    def __init__(self, path: str, basepath: str | None) -> None:
        self.path: str = path
        self.basepath: str | None = basepath
        self.mimetype: str | None = None
        self.suffix: str | None = None
        self.uri: str | Path | None = None

    @abstractmethod
    def extract_data(self) -> bytes | None:
        raise NotImplementedError

    def get_data(self) -> bytes | None:
        try:
            return self.extract_data()
        except Exception as e:
            log.error(  # noqa: TRY400
                "%s: %s while extracting data from %s: %r",
                type(e).__name__,
                e,
                type(self).__name__,
                self.uri,
            )
        return None

    def get_uri(self) -> str | Path | None:
        return self.uri

    def get_mimetype(self) -> str | None:
        return self.mimetype

    def get_named_tmp_file(self) -> _TemporaryFileWrapper[bytes]:
        data: bytes | None = self.get_data()
        tmp_file = tempfile.NamedTemporaryFile(suffix=self.suffix)
        # print(tmp_file.name, len(data))
        if data:
            tmp_file.write(data)
            tmp_file.flush()
            files_tmp.append(tmp_file)
        if self.path is None:
            self.path = tmp_file.name
        return tmp_file

    def get_BytesIO(self) -> BytesIO | None:
        data: bytes | None = self.get_data()
        if data:
            return BytesIO(data)
        return None


class B64InlineURI(BaseFile):
    mime_params: list

    def extract_data(self) -> bytes | None:
        # RFC 2397 form: data:[<mediatype>][;base64],<data>
        parts = self.path.split("base64,")
        if (
            not self.path.startswith("data:")
            or "base64," not in self.path
            or len(parts) != 2
        ):
            msg = "Base64-encoded data URI is malformed"
            raise RuntimeError(msg)
        data = parts[1]
        # Strip 'data:' prefix and split mime type with optional params
        mime = parts[0][len("data:") :].split(";")
        # mime_params are preserved for future use
        self.mimetype, self.mime_params = mime[0], mime[1:]

        b64: bytes = urllib_unquote(data).encode("utf-8")
        return base64.b64decode(b64)


class LocalProtocolURI(BaseFile):
    def extract_data(self) -> bytes | None:
        if self.basepath and self.path.startswith("/"):
            self.uri = urlparse.urljoin(self.basepath, self.path[1:])
            urlResponse = request.urlopen(self.uri)
            self.mimetype = urlResponse.info().get("Content-Type", "").split(";")[0]
            return urlResponse.read()
        return None


class NetworkFileUri(BaseFile):
    def __init__(self, path: str, basepath: str | None) -> None:
        super().__init__(path, basepath)
        self.attempts: int = 3
        self.actual_attempts: int = 0

    def get_data(self) -> bytes | None:
        data = None
        # try several attempts if network problems happens
        while self.attempts > self.actual_attempts and data is None:
            self.actual_attempts += 1
            try:
                data = self.extract_data()
            except Exception as e:
                log.error(  # noqa: TRY400
                    "%s: %s while extracting data from %s: %r on attempt %d",
                    type(e).__name__,
                    e,
                    type(self).__name__,
                    self.uri,
                    self.actual_attempts,
                )
        return data

    def get_httplib(self, uri) -> tuple[bytes | None, bool]:
        log.debug("Sending request for %r with httplib", uri)
        data: bytes | None = None
        is_gzip: bool = False
        url_splitted: SplitResult = urlparse.urlsplit(uri)
        server: str = url_splitted[1]
        path: str = url_splitted[2]
        path += f"?{url_splitted[3]}" if url_splitted[3] else ""
        conn: httplib.HTTPConnection | httplib.HTTPSConnection | None = None
        if uri.startswith("https://"):
            conn = httplib.HTTPSConnection(server, **httpConfig)
        else:
            conn = httplib.HTTPConnection(server)
        conn.request("GET", path)
        r1: HTTPResponse = conn.getresponse()
        if (r1.status, r1.reason) == (200, "OK"):
            self.mimetype = r1.getheader("Content-Type", "").split(";")[0]
            data = r1.read()
            if r1.getheader("content-encoding") == "gzip":
                is_gzip = True
        else:
            log.debug("Received non-200 status: %d %s", r1.status, r1.reason)
        return data, is_gzip

    def extract_data(self) -> bytes | None:
        # FIXME: When self.path don't start with http
        if self.basepath and not self.path.startswith("http"):
            uri = urlparse.urljoin(self.basepath, self.path)
        else:
            uri = self.path
        self.uri = uri
        data, is_gzip = self.get_httplib(uri)
        if is_gzip and data:
            data = gzip.GzipFile(mode="rb", fileobj=BytesIO(data)).read()
        log.debug("Uri parsed: %r", uri)
        return data


class LocalFileURI(BaseFile):
    @staticmethod
    def guess_mimetype(name) -> str | None:
        """Guess the mime type."""
        mimetype = mimetypes.guess_type(str(name))[0]
        if mimetype is not None:
            mimetype = mimetype.split(";")[0]
        return mimetype

    def extract_data(self) -> bytes | None:
        data = None
        log.debug("Unrecognized scheme, assuming local file path")
        path = Path(self.path)
        uri = None
        uri = Path(self.basepath) / path if self.basepath is not None else Path() / path
        if path.exists() and not uri.exists():
            uri = path
        if uri.is_file():
            self.uri = uri
            self.suffix = uri.suffix
            self.mimetype = self.guess_mimetype(uri)
            if self.mimetype and self.mimetype.startswith("text"):
                with open(uri, encoding="utf-8") as file_handler:
                    data = file_handler.read().encode("utf-8")
            else:
                with open(uri, "rb") as file_handler:
                    data = file_handler.read()
        return data


class BytesFileUri(BaseFile):
    def extract_data(self) -> bytes | None:
        self.uri = self.path
        return self.path.encode("utf-8")


class LocalTmpFile(BaseFile):
    def __init__(self, path, basepath) -> None:
        self.path: str = path
        self.basepath: str | None = None
        self.mimetype: str | None = basepath
        self.suffix: str | None = None
        self.uri: str | Path | None = None

    def get_named_tmp_file(self):
        tmp_file = super().get_named_tmp_file()
        if self.path is None:
            self.path = tmp_file.name
        return tmp_file

    def extract_data(self) -> bytes | None:
        if self.path is None:
            return None
        self.uri = self.path
        with open(self.path, "rb") as arch:
            return arch.read()


class FileNetworkManager:
    @staticmethod
    def get_manager(uri, basepath=None):
        if uri is None:
            return LocalTmpFile(uri, basepath)
        if isinstance(uri, bytes):
            instance = BytesFileUri(uri, basepath)
        elif uri.startswith("data:"):
            instance = B64InlineURI(uri, basepath)
        else:
            if basepath and not urlparse.urlparse(uri).scheme:
                urlParts = urlparse.urlparse(basepath)
            else:
                urlParts = urlparse.urlparse(uri)

            log.debug("URLParts: %r, %r", urlParts, urlParts.scheme)
            if urlParts.scheme == "file":
                instance = LocalProtocolURI(uri, basepath)
            elif urlParts.scheme in {"http", "https"}:
                instance = NetworkFileUri(uri, basepath)
            else:
                instance = LocalFileURI(uri, basepath)
        return instance


class pisaFileObject:
    def __init__(
        self,
        uri: str | Path | None,
        basepath: str | None = None,
        callback: Callable | None = None,
    ) -> None:
        self.uri: str | Path | None = uri
        self.basepath: str | None = basepath
        if callback and (new := callback(uri, basepath)):
            self.uri = new
            self.basepath = None

        log.debug("FileObject %r, Basepath: %r", self.uri, self.basepath)

        self.instance: BaseFile = FileNetworkManager.get_manager(
            self.uri, basepath=self.basepath
        )

    def getFileContent(self) -> bytes | None:
        return self.instance.get_data()

    def getNamedFile(self) -> str | None:
        f = self.instance.get_named_tmp_file()
        return f.name if f else None

    def getData(self) -> bytes | None:
        return self.instance.get_data()

    def getFile(self) -> BytesIO | _TemporaryFileWrapper | None:
        if GAE:
            return self.instance.get_BytesIO()
        return self.instance.get_named_tmp_file()

    def getMimeType(self) -> str | None:
        return self.instance.get_mimetype()

    def notFound(self) -> bool:
        return self.getData() is None

    def getAbsPath(self):
        return self.instance.get_uri()

    def getBytesIO(self):
        return self.instance.get_BytesIO()


def getFile(*a, **kw) -> pisaFileObject:
    return pisaFileObject(*a, **kw)


def cleanFiles() -> None:
    files_tmp.cleanFiles()
