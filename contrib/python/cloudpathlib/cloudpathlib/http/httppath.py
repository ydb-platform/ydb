import datetime
import http
import os
from pathlib import Path, PurePosixPath
from tempfile import TemporaryDirectory
from typing import Any, Tuple, TYPE_CHECKING, Union, Optional
import urllib

from ..cloudpath import CloudPath, NoStatError, register_path_class


if TYPE_CHECKING:
    from .httpclient import HttpClient, HttpsClient


@register_path_class("http")
class HttpPath(CloudPath):
    cloud_prefix = "http://"
    client: "HttpClient"

    def __init__(
        self,
        cloud_path: Union[str, "HttpPath"],
        *parts: str,
        client: Optional["HttpClient"] = None,
    ) -> None:
        super().__init__(cloud_path, *parts, client=client)

        self._path = (
            PurePosixPath(self._url.path)
            if self._url.path.startswith("/")
            else PurePosixPath(f"/{self._url.path}")
        )

    @property
    def _local(self) -> Path:
        """Cached local version of the file."""
        # remove params, query, fragment to get local path
        return self.client._local_cache_dir / self._url.path.lstrip("/")

    def _dispatch_to_path(self, func: str, *args, **kwargs) -> Any:
        sup = super()._dispatch_to_path(func, *args, **kwargs)

        # some dispatch methods like "__truediv__" strip trailing slashes;
        # for http paths, we need to keep them to indicate directories
        if func == "__truediv__" and str(args[0]).endswith("/"):
            return self._new_cloudpath(str(sup) + "/")

        else:
            return sup

    @property
    def parsed_url(self) -> urllib.parse.ParseResult:
        return self._url

    @property
    def drive(self) -> str:
        # For HTTP paths, no drive; use .anchor for scheme + netloc
        return self._url.netloc

    @property
    def anchor(self) -> str:
        return f"{self._url.scheme}://{self._url.netloc}/"

    @property
    def _no_prefix_no_drive(self) -> str:
        # netloc appears in anchor and drive for httppath; so don't double count
        return self._str[len(self.anchor) - 1 :]

    def is_dir(self, follow_symlinks: bool = True) -> bool:
        if not self.exists():
            return False

        # Use client default to identify directories
        return self.client.dir_matcher(str(self))

    def is_file(self, follow_symlinks: bool = True) -> bool:
        if not self.exists():
            return False

        return not self.client.dir_matcher(str(self))

    def mkdir(
        self, parents: bool = False, exist_ok: bool = False, mode: Optional[Any] = None
    ) -> None:
        pass  # no-op for HTTP Paths

    def touch(self, exist_ok: bool = True, mode: Optional[Any] = None) -> None:
        if self.exists():
            if not exist_ok:
                raise FileExistsError(f"File already exists: {self}")

            raise NotImplementedError(
                "Touch not implemented for existing HTTP files since we can't update the modified time; "
                "use `put()` or write to the file instead."
            )
        else:
            empty_file = Path(TemporaryDirectory().name) / "empty_file.txt"
            empty_file.parent.mkdir(parents=True, exist_ok=True)
            empty_file.write_text("")
            self.client._upload_file(empty_file, self)

    def stat(self, follow_symlinks: bool = True) -> os.stat_result:
        try:
            meta = self.client._get_metadata(self)
        except:  # noqa E722
            raise NoStatError(f"Could not get metadata for {self}")

        return os.stat_result(
            (  # type: ignore
                None,  # mode
                None,  # ino
                self.cloud_prefix,  # dev,
                None,  # nlink,
                None,  # uid,
                None,  # gid,
                meta.get("size", 0),  # size,
                None,  # atime,
                meta.get(
                    "last_modified", datetime.datetime.fromtimestamp(0)
                ).timestamp(),  # mtime,
                None,  # ctime,
            )
        )

    def as_url(self, presign: bool = False, expire_seconds: int = 60 * 60) -> str:
        if presign:
            raise NotImplementedError("Presigning not supported for HTTP paths")

        return (
            self._url.geturl()
        )  # recreate from what was initialized so we have the same query params, etc.

    @property
    def name(self) -> str:
        return self._path.name

    @property
    def parents(self) -> Tuple["HttpPath", ...]:
        return super().parents + (self._new_cloudpath(""),)

    def get(self, **kwargs) -> Tuple[http.client.HTTPResponse, bytes]:
        """Issue a get request with `urllib.request.Request`"""
        return self.client.request(self, "GET", **kwargs)

    def put(self, **kwargs) -> Tuple[http.client.HTTPResponse, bytes]:
        """Issue a put request with `urllib.request.Request`"""
        return self.client.request(self, "PUT", **kwargs)

    def post(self, **kwargs) -> Tuple[http.client.HTTPResponse, bytes]:
        """Issue a post request with `urllib.request.Request`"""
        return self.client.request(self, "POST", **kwargs)

    def delete(self, **kwargs) -> Tuple[http.client.HTTPResponse, bytes]:
        """Issue a delete request with `urllib.request.Request`"""
        return self.client.request(self, "DELETE", **kwargs)

    def head(self, **kwargs) -> Tuple[http.client.HTTPResponse, bytes]:
        """Issue a head request with `urllib.request.Request`"""
        return self.client.request(self, "HEAD", **kwargs)


@register_path_class("https")
class HttpsPath(HttpPath):
    cloud_prefix: str = "https://"
    client: "HttpsClient"
