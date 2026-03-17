import contextlib
import warnings
from typing import Any, ClassVar, Dict, Iterator, Optional

from libcloud.storage.base import Container
from libcloud.storage.types import ObjectDoesNotExistError
from sqlalchemy_file.helpers import LOCAL_STORAGE_DRIVER_NAME, get_metadata_file_obj
from sqlalchemy_file.stored_file import StoredFile


class StorageManager:
    """Takes care of managing the whole Storage environment for the application.

    Use [add_storage][sqlalchemy_file.storage.StorageManager.add_storage] method
    to add new `libcloud.storage.base.Container`and associate a name which
    will be use later to retrieve this container.

    The first container will be used as default, to simplify code when you have
    only one container.

    Use associated name as `upload_storage` for [FileField][sqlalchemy_file.types.FileField]
    to store his files inside the corresponding container.

    """

    _default_storage_name: ClassVar[Optional[str]] = None
    _storages: ClassVar[Dict[str, Container]] = {}

    @classmethod
    def set_default(cls, name: str) -> None:
        """Replaces the current application default storage."""
        if name not in cls._storages:
            raise RuntimeError(f"{name} storage has not been added")
        cls._default_storage_name = name

    @classmethod
    def get_default(cls) -> str:
        """Gets the current application default storage."""
        if cls._default_storage_name is None:
            raise RuntimeError("No default storage has been added")
        return cls._default_storage_name

    @classmethod
    def add_storage(cls, name: str, container: Container) -> None:
        """Add new storage."""
        assert isinstance(container, Container), "Invalid container"
        if name in cls._storages:
            raise RuntimeError(f"Storage {name} has already been added")
        if cls._default_storage_name is None:
            cls._default_storage_name = name
        cls._storages[name] = container

    @classmethod
    def get(cls, name: Optional[str] = None) -> Container:
        """Gets the container instance associate to the name,
        return default if name isn't provided.
        """
        if name is None and cls._default_storage_name is None:
            raise RuntimeError("No default storage have been added")
        if name is None:
            name = cls._default_storage_name
        if name in cls._storages:
            return cls._storages[name]
        raise RuntimeError(f"{name} storage has not been added")

    @classmethod
    def save_file(
        cls,
        name: str,
        content: Optional[Iterator[bytes]] = None,
        upload_storage: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        extra: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        content_path: Optional[str] = None,
    ) -> StoredFile:
        if content is None and content_path is None:
            raise ValueError("Either content or content_path must be specified")
        if metadata is not None:
            warnings.warn(
                'metadata attribute is deprecated. Use extra={"meta_data": ...} instead',
                DeprecationWarning,
                stacklevel=1,
            )
            extra = {
                "meta_data": metadata,
                "content_type": metadata.get(
                    "content_type", "application/octet-stream"
                ),
            }
        """Save file into provided `upload_storage`"""
        container = cls.get(upload_storage)
        if (
            container.driver.name == LOCAL_STORAGE_DRIVER_NAME
            and extra is not None
            and extra.get("meta_data", None) is not None
        ):
            """
            Libcloud local storage driver doesn't support metadata, so the metadata
            is saved in the same container with the combination of the original name
            and `.metadata.json` as name
            """
            container.upload_object_via_stream(
                iterator=get_metadata_file_obj(extra["meta_data"]),
                object_name=f"{name}.metadata.json",
            )
        if content_path is not None:
            return StoredFile(
                container.upload_object(
                    file_path=content_path,
                    object_name=name,
                    extra=extra,
                    headers=headers,
                )
            )
        assert content is not None
        return StoredFile(
            container.upload_object_via_stream(
                iterator=content, object_name=name, extra=extra, headers=headers
            )
        )

    @classmethod
    def get_file(cls, path: str) -> StoredFile:
        """Retrieve the file with `provided` path,
        path is expected to be `storage_name/file_id`.
        """
        upload_storage, file_id = path.split("/")
        return StoredFile(StorageManager.get(upload_storage).get_object(file_id))

    @classmethod
    def delete_file(cls, path: str) -> bool:
        """Delete the file with `provided` path.

        The path is expected to be `storage_name/file_id`.
        """
        upload_storage, file_id = path.split("/")
        obj = StorageManager.get(upload_storage).get_object(file_id)
        if obj.driver.name == LOCAL_STORAGE_DRIVER_NAME:
            """Try deleting associated metadata file"""
            with contextlib.suppress(ObjectDoesNotExistError):
                obj.container.get_object(f"{obj.name}.metadata.json").delete()

        return obj.delete()

    @classmethod
    def _clear(cls) -> None:
        """This is only for testing pourposes, resets the StorageManager."""
        cls._default_storage_name = None
        cls._storages = {}
