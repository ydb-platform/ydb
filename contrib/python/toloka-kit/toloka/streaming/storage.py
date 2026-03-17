__all__ = [
    'BaseStorage',
    'JSONLocalStorage',
    'S3Storage',
]

import collections
import json
import os
import shutil
from contextlib import contextmanager
from io import BytesIO
from typing import Any, ContextManager, Dict, Optional, Sequence, TypeVar
from typing_extensions import Protocol

import attr

from .locker import BaseLocker, FileLocker
from ..util.stored import get_base64_digest, get_stored_meta, pickle_dumps_base64, pickle_loads_base64

Pickleable = TypeVar('Pickleable')


class BaseStorage:
    def _get_base_path(self, key: str) -> str:
        """Prepare key representing all current pipeline-related stored data."""
        return f'{self.__class__.__name__}_{get_base64_digest(key)}'

    def lock(self, key: str) -> ContextManager[Any]:
        pass

    def save(self, base_key: str, data: Dict[str, Pickleable]) -> None:
        raise NotImplementedError(f'Not implemented method save in {self.__class__.__name__}')

    def load(self, base_key: str, keys: Sequence[str]) -> Optional[Dict[str, Pickleable]]:
        raise NotImplementedError(f'Not implemented method load in {self.__class__.__name__}')

    def cleanup(self, base_key: str, keys: Sequence[str], lock: Any) -> None:
        pass


@attr.s
class BaseExternalLockerStorage(BaseStorage):
    locker: Optional[BaseLocker] = attr.ib(default=None, kw_only=True)

    @contextmanager
    def lock(self, key: str) -> ContextManager[Any]:
        if self.locker:
            with self.locker(key) as lock:
                yield lock
                return
        yield None

    def cleanup(self, base_key: str, keys: Sequence[str], lock: Any) -> None:
        if self.locker:
            self.locker.cleanup(lock)


@attr.s
class JSONLocalStorage(BaseExternalLockerStorage):
    """Simplest local storage to dump state of a pipeline and restore in case of restart.

    Attributes:
        dirname: Directory to store pipeline's state files. By default, "/tmp".
        locker: Optional locker object. By default, FileLocker with the same dirname is used.

    Example:
        Allow pipeline to dump it's state to the local storage.

        >>> pipeline = Pipeline(storage=JSONLocalStorage())
        >>> ...
        >>> await pipeline.run()  # Will load from storage at the start and save after each iteration.
        ...

        Set locker explicitly.

        >>> storage = JSONLocalStorage('/store-data-here', locker=FileLocker('/store-locks-here'))
        ...
    """

    class DefaultNearbyFileLocker(BaseLocker):
        pass

    dirname: str = attr.ib(default='/tmp')
    locker: Optional[BaseLocker] = attr.ib(factory=DefaultNearbyFileLocker, kw_only=True)

    def __attrs_post_init__(self) -> None:
        if isinstance(self.locker, self.DefaultNearbyFileLocker):
            self.locker = FileLocker(self.dirname)

    def _get_base_path(self, base_key: str) -> str:
        """Return path to the directory containing all observers related to the current pipeline."""
        return os.path.join(self.dirname, super()._get_base_path(base_key))

    def _join_minor_path(self, base_path: str, key: str) -> str:
        """Return path to the file representing exactly observer with the given key."""
        return os.path.join(base_path, get_base64_digest(key))

    def save(self, base_key: str, data: Dict[str, Pickleable]) -> None:
        base_path = self._get_base_path(base_key)
        try:
            os.mkdir(base_path)
        except FileExistsError:
            if not os.path.isdir(base_path):
                raise

        for key, value in data.items():
            with open(self._join_minor_path(base_path, key), 'w') as file:
                data = {'base_key': base_key,
                        'key': key,
                        'value': pickle_dumps_base64(value).decode(),
                        'meta': get_stored_meta()}
                json.dump(data, file, indent=4)

    def load(self, base_key: str, keys: Sequence[str]) -> Optional[Dict[str, Pickleable]]:
        base_path = self._get_base_path(base_key)
        if not os.path.exists(base_path):
            return None

        res: Dict[str, Pickleable] = {}
        for key in keys:
            try:
                with open(self._join_minor_path(base_path, key), 'r') as file:
                    content = file.read()
                    if content:
                        res[key] = pickle_loads_base64(json.loads(content)['value'])
            except FileNotFoundError:
                pass

        return res

    def cleanup(self, base_key: str, keys: Sequence[str], lock: Any) -> None:
        try:
            shutil.rmtree(self._get_base_path(base_key))
        except FileNotFoundError:
            pass
        if self.locker:
            self.locker.cleanup(lock)


class ObjectSummaryCollection(Protocol):
    def filter(self, Prefix, **kwargs) -> 'ObjectSummaryCollection':
        ...

    def delete(self, **kwargs):
        ...


class BucketType(Protocol):
    def upload_fileobj(self, Fileobj, Key, ExtraArgs=None, **kwargs):
        ...

    def download_fileobj(self, Fileobj, ExtraArgs=None, **kwargs):
        ...

    objects: ObjectSummaryCollection


@attr.s
class S3Storage(BaseExternalLockerStorage):
    """Storage that save to AWS S3 using given boto3 client.

    {% note warning %}

    Requires toloka-kit[s3] extras. Install it with the following command:

    ```shell
    pip install toloka-kit[s3]
    ```

    {% endnote %}

    Attributes:
        bucket: Boto3 bucket object.
        locker: Optional locker object. By default, no locker is used.

    Examples:
        Create new instance.

        >>> !pip install toloka-kit[s3]
        >>> import boto3
        >>> import os
        >>> session = boto3.Session(
        >>>     aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        >>>     aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
        >>> )
        >>> resource = session.resource('s3', region_name=os.getenv('AWS_DEFAULT_REGION', 'us-east-2'))
        >>> bucket = resource.Bucket('my-bucket')
        >>> storage = S3Storage(bucket)
        ...

        Use with pipelines.

        >>> storage = S3Storage(bucket=bucket, locker=ZooKeeperLocker(kazoo_client, '/lock-dir'))
        >>> pipeline = Pipeline(storage=storage)
        >>> ...
        >>> await pipeline.run()  # Will load from storage at the start and save after each iteration.
        ...
    """
    bucket: BucketType = attr.ib()

    @classmethod
    def _is_not_found_error(cls, exc: Exception) -> bool:
        response = getattr(exc, 'response', None)
        if isinstance(response, collections.abc.Mapping):
            error_info = response.get('Error')
            if error_info and 'Code' in error_info and 'Message' in error_info:
                return error_info['Code'] == '404' and error_info['Message'] == 'Not Found'
        return False

    def _join_minor_path(self, base_path: str, key: str) -> str:
        """Each observer is being saved with the key prefixed by the base path."""
        return f'{base_path}_{get_base64_digest(key)}'

    def save(self, base_key: str, data: Dict[str, Pickleable]) -> None:
        base_path = self._get_base_path(base_key)
        for key, value in data.items():
            stream = BytesIO(pickle_dumps_base64(value))
            path = self._join_minor_path(base_path, key)
            metadata = {'base_key': base_key,  # Metadata values should be strings.
                        'key': key,
                        'meta': json.dumps(get_stored_meta(), ensure_ascii=True, indent=None, separators=(',', ':'))}
            self.bucket.upload_fileobj(stream, path, ExtraArgs={'Metadata': metadata})

    def load(self, base_key: str, keys: Sequence[str]) -> Dict[str, Pickleable]:
        base_path = self._get_base_path(base_key)

        res: Dict[str, Pickleable] = {}
        for key in keys:
            path = self._join_minor_path(base_path, key)
            try:
                with BytesIO() as file:
                    self.bucket.download_fileobj(path, file)
                    file.seek(0)
                    content = file.read()
                    if content:
                        res[key] = pickle_loads_base64(content)
            except Exception as exc:
                if self._is_not_found_error(exc):
                    return None
                raise

        return res

    def cleanup(self, base_key: str, keys: Sequence[str], lock: Any) -> None:
        self.bucket.objects.filter(Prefix=self._get_base_path(base_key)).delete()
        if self.locker:
            self.locker.cleanup(lock)
