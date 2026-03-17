import atexit
from hashlib import md5
import mimetypes
import os
from pathlib import Path, PurePosixPath
import shutil
import sys
from tempfile import TemporaryDirectory
from time import sleep
from typing import Callable, ClassVar, Dict, Iterable, List, Optional, Tuple, Union

from ..client import Client
from ..enums import FileCacheMode
from .localpath import LocalPath


class LocalClient(Client):
    """Abstract client for accessing objects the local filesystem. Subclasses are as a monkeypatch
    substitutes for normal Client subclasses when writing tests."""

    # Class-level variable to tracks the default storage directory for this client class
    # that is used if a client is instantiated without a directory being explicitly provided
    _default_storage_temp_dir: ClassVar[Optional[TemporaryDirectory]] = None

    # Instance-level variable that tracks the local storage directory for this client
    _local_storage_dir: Optional[Union[str, os.PathLike]]

    def __init__(
        self,
        *args,
        local_storage_dir: Optional[Union[str, os.PathLike]] = None,
        file_cache_mode: Optional[Union[str, FileCacheMode]] = None,
        local_cache_dir: Optional[Union[str, os.PathLike]] = None,
        content_type_method: Optional[Callable] = mimetypes.guess_type,
        **kwargs,
    ):
        self._local_storage_dir = local_storage_dir

        super().__init__(
            local_cache_dir=local_cache_dir,
            content_type_method=content_type_method,
            file_cache_mode=file_cache_mode,
        )

    @classmethod
    def get_default_storage_dir(cls) -> Path:
        """Return the default storage directory for this client class. This is used if a client
        is instantiated without a storage directory being explicitly provided. In this usage,
        "storage" refers to the local storage that simulates the cloud.
        """
        if cls._default_storage_temp_dir is None:
            cls._default_storage_temp_dir = TemporaryDirectory()
            _temp_dirs_to_clean.append(cls._default_storage_temp_dir)
        return Path(cls._default_storage_temp_dir.name)

    @classmethod
    def reset_default_storage_dir(cls) -> Path:
        """Reset the default storage directly. This tears down and recreates the directory used by
        default for this client class when instantiating a client without explicitly providing
        a storage directory. In this usage, "storage" refers to the local storage that simulates
        the cloud.
        """
        cls._default_storage_temp_dir = None
        return cls.get_default_storage_dir()

    @property
    def local_storage_dir(self) -> Path:
        """The local directory where files are stored for this client. This storage directory is
        the one that simulates the cloud. If no storage directory was provided on instantiating the
        client, the default storage directory for this client class is used.
        """
        if self._local_storage_dir is None:
            # No explicit local storage was provided on instantiating the client.
            # Use the default storage directory for this class.
            return self.get_default_storage_dir()
        return Path(self._local_storage_dir)

    def _cloud_path_to_local(self, cloud_path: "LocalPath") -> Path:
        return self.local_storage_dir / cloud_path._no_prefix

    def _local_to_cloud_path(self, local_path: Union[str, os.PathLike]) -> "LocalPath":
        local_path = Path(local_path)
        cloud_prefix = self._cloud_meta.path_class.cloud_prefix
        return self.CloudPath(
            f"{cloud_prefix}{PurePosixPath(local_path.relative_to(self.local_storage_dir))}"
        )

    def _download_file(self, cloud_path: "LocalPath", local_path: Union[str, os.PathLike]) -> Path:
        local_path = Path(local_path)
        local_path.parent.mkdir(exist_ok=True, parents=True)

        try:
            shutil.copyfile(self._cloud_path_to_local(cloud_path), local_path)
        except FileNotFoundError:
            # erroneous FileNotFoundError appears in tests sometimes; patiently insist on the parent directory existing
            sleep(1.0)
            local_path.parent.mkdir(exist_ok=True, parents=True)
            sleep(1.0)

            shutil.copyfile(self._cloud_path_to_local(cloud_path), local_path)

        return local_path

    def _exists(self, cloud_path: "LocalPath") -> bool:
        return self._cloud_path_to_local(cloud_path).exists()

    def _is_dir(self, cloud_path: "LocalPath", follow_symlinks=True) -> bool:
        kwargs = dict(follow_symlinks=follow_symlinks)
        if sys.version_info < (3, 13):
            kwargs.pop("follow_symlinks")

        return self._cloud_path_to_local(cloud_path).is_dir(**kwargs)

    def _is_file(self, cloud_path: "LocalPath", follow_symlinks=True) -> bool:
        kwargs = dict(follow_symlinks=follow_symlinks)
        if sys.version_info < (3, 13):
            kwargs.pop("follow_symlinks")

        return self._cloud_path_to_local(cloud_path).is_file(**kwargs)

    def _is_file_or_dir(self, cloud_path: "LocalPath") -> Optional[str]:
        if self._is_dir(cloud_path):
            return "dir"
        elif self._is_file(cloud_path):
            return "file"
        else:
            raise FileNotFoundError(f"Path could not be identified as file or dir: {cloud_path}")

    def _list_dir(
        self, cloud_path: "LocalPath", recursive=False
    ) -> Iterable[Tuple["LocalPath", bool]]:
        pattern = "**/*" if recursive else "*"
        for obj in self._cloud_path_to_local(cloud_path).glob(pattern):
            yield (self._local_to_cloud_path(obj), obj.is_dir())

    def _md5(self, cloud_path: "LocalPath") -> str:
        return md5(self._cloud_path_to_local(cloud_path).read_bytes()).hexdigest()

    def _move_file(
        self, src: "LocalPath", dst: "LocalPath", remove_src: bool = True
    ) -> "LocalPath":
        self._cloud_path_to_local(dst).parent.mkdir(exist_ok=True, parents=True)

        if remove_src:
            self._cloud_path_to_local(src).replace(self._cloud_path_to_local(dst))
        else:
            shutil.copy(self._cloud_path_to_local(src), self._cloud_path_to_local(dst))
        return dst

    def _remove(self, cloud_path: "LocalPath", missing_ok: bool = True) -> None:
        local_storage_path = self._cloud_path_to_local(cloud_path)
        if not missing_ok and not local_storage_path.exists():
            raise FileNotFoundError(f"File does not exist: {cloud_path}")

        if local_storage_path.is_file():
            local_storage_path.unlink()
        elif local_storage_path.is_dir():
            shutil.rmtree(local_storage_path)

    def _stat(self, cloud_path: "LocalPath") -> os.stat_result:
        stat_result = self._cloud_path_to_local(cloud_path).stat()

        return os.stat_result(
            (  # type: ignore
                None,  # type: ignore # mode
                None,  # ino
                cloud_path.cloud_prefix,  # dev,
                None,  # nlink,
                None,  # uid,
                None,  # gid,
                stat_result.st_size,  # size,
                None,  # atime,
                stat_result.st_mtime,  # mtime,
                None,  # ctime,
            )
        )

    def _touch(self, cloud_path: "LocalPath", exist_ok: bool = True) -> None:
        local_storage_path = self._cloud_path_to_local(cloud_path)
        if local_storage_path.exists() and not exist_ok:
            raise FileExistsError(f"File exists: {cloud_path}")
        local_storage_path.parent.mkdir(exist_ok=True, parents=True)
        local_storage_path.touch()

    def _upload_file(
        self, local_path: Union[str, os.PathLike], cloud_path: "LocalPath"
    ) -> "LocalPath":
        dst = self._cloud_path_to_local(cloud_path)
        dst.parent.mkdir(exist_ok=True, parents=True)
        shutil.copy(local_path, dst)
        return cloud_path

    def _get_metadata(self, cloud_path: "LocalPath") -> Dict:
        # content_type is the only metadata we test currently
        if self.content_type_method is None:
            content_type_method = lambda x: (None, None)
        else:
            content_type_method = self.content_type_method

        return {
            "content_type": content_type_method(str(self._cloud_path_to_local(cloud_path)))[0],
        }

    def _get_public_url(self, cloud_path: "LocalPath") -> str:
        return cloud_path.as_uri()

    def _generate_presigned_url(
        self, cloud_path: "LocalPath", expire_seconds: int = 60 * 60
    ) -> str:
        raise NotImplementedError("Cannot generate a presigned URL for a local path.")


_temp_dirs_to_clean: List[TemporaryDirectory] = []


@atexit.register
def clean_temp_dirs():
    for temp_dir in _temp_dirs_to_clean:
        temp_dir.cleanup()
