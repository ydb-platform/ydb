import time
from pathlib import Path
from typing import Any

import pytest

from pathy import (
    BucketClient,
    BucketClientFS,
    Pathy,
    get_client,
    get_fs_client,
    register_client,
    set_client_params,
    use_fs,
    use_fs_cache,
)

from . import gcs_testable
from .conftest import ENV_ID, TEST_ADAPTERS


@pytest.mark.skipif(not gcs_testable, reason="requires gcs")
def test_clients_get_client_works_with_optional_builtin_schems() -> None:
    from pathy.gcs import BucketClientGCS

    assert isinstance(get_client("gs"), BucketClientGCS)


def test_clients_get_client_works_with_builtin_schems() -> None:
    assert isinstance(get_client("file"), BucketClientFS)
    assert isinstance(get_client(""), BucketClientFS)


def test_clients_get_client_respects_use_fs_override() -> None:
    use_fs(True)
    assert isinstance(get_client("gs"), BucketClientFS)
    use_fs(False)


def test_clients_get_client_errors_with_unknown_scheme() -> None:
    with pytest.raises(ValueError):
        get_client("foo")


def test_clients_use_fs(with_fs: Path) -> None:
    assert get_fs_client() is None
    # Use the default path
    use_fs()
    client = get_fs_client()
    assert isinstance(client, BucketClientFS)
    assert client.root.exists()

    # Use with False disables the client
    use_fs(False)
    client = get_fs_client()
    assert client is None

    # Can use a given path
    use_fs(str(with_fs))
    client = get_fs_client()
    assert isinstance(client, BucketClientFS)
    assert client.root == with_fs

    # Can use a pathlib.Path
    use_fs(with_fs / "sub_folder")
    client = get_fs_client()
    assert isinstance(client, BucketClientFS)
    assert client.root == with_fs / "sub_folder"

    use_fs(False)


@pytest.mark.parametrize("adapter", TEST_ADAPTERS)
def test_api_use_fs_cache(with_adapter: str, with_fs: str, bucket: str) -> None:
    path = Pathy(f"{with_adapter}://{bucket}/{ENV_ID}/directory/foo.txt")
    path.write_text("---")
    assert isinstance(path, Pathy)
    with pytest.raises(ValueError):
        Pathy.to_local(path)

    use_fs_cache(Path(with_fs) / "sub_folder")
    source_file: Path = Pathy.to_local(path)
    foo_timestamp = Path(f"{source_file}.time")
    assert foo_timestamp.exists()
    orig_cache_time = foo_timestamp.read_text()

    # fetch from the local cache
    cached_file: Path = Pathy.to_local(f"{path}")
    assert cached_file == source_file
    cached_cache_time = foo_timestamp.read_text()
    assert orig_cache_time == cached_cache_time, "cached blob timestamps should match"

    # Update the blob
    time.sleep(1.0)
    path.write_text('{ "cool" : true }')

    # Fetch the updated blob
    Pathy.to_local(path)
    updated_cache_time = foo_timestamp.read_text()
    assert updated_cache_time != orig_cache_time, "cached timestamp did not change"


class BucketClientTest(BucketClient):
    def __init__(self, required_arg: bool) -> None:
        self.recreate(required_arg=required_arg)

    def recreate(self, **kwargs: Any) -> None:
        self.required_arg = kwargs["required_arg"]


def test_clients_set_client_params() -> None:
    register_client("test", BucketClientTest)
    with pytest.raises(TypeError):
        get_client("test")

    set_client_params("test", required_arg=True)
    client: BucketClientTest = get_client("test")
    assert isinstance(client, BucketClientTest)


def test_clients_set_client_params_recreates_client() -> None:
    register_client("test", BucketClientTest)
    set_client_params("test", required_arg=False)
    client: BucketClientTest = get_client("test")
    assert client.required_arg is False
    set_client_params("test", required_arg=True)
    assert client.required_arg is True
    assert isinstance(client, BucketClientTest)
