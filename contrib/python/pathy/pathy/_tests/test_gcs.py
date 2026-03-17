from unittest.mock import patch

import pytest

from pathy import Pathy, get_client, set_client_params, use_fs

from . import gcs_installed, gcs_testable

GCS_ADAPTER = ["gcs"]


def raise_default_creds_error() -> None:
    from google.auth.exceptions import DefaultCredentialsError

    raise DefaultCredentialsError()


@pytest.mark.parametrize("adapter", GCS_ADAPTER)
@patch("google.auth.default", raise_default_creds_error)
@pytest.mark.skipif(not gcs_testable, reason="requires gcs")
def test_gcs_default_credentials_error_is_preserved(
    with_adapter: str, bucket: str
) -> None:
    from google.auth.exceptions import DefaultCredentialsError

    # Remove the default credentials (this will recreate the client and error)
    with pytest.raises(DefaultCredentialsError):
        set_client_params("gs")


@pytest.mark.skipif(gcs_installed, reason="requires gcs deps to NOT be installed")
def test_gcs_import_error_missing_deps() -> None:
    use_fs(False)
    with pytest.raises(ImportError):
        get_client("gs")


@pytest.mark.parametrize("adapter", GCS_ADAPTER)
@pytest.mark.skipif(not gcs_testable, reason="requires gcs")
def test_gcs_scandir_invalid_bucket_name(with_adapter: str) -> None:
    from pathy.gcs import ScanDirGCS

    root = Pathy("gs://invalid_h3gE_ds5daEf_Sdf15487t2n4/bar")
    client = root.client(root)
    scandir = ScanDirGCS(client=client, path=root)
    assert len(list(scandir)) == 0


@pytest.mark.parametrize("adapter", GCS_ADAPTER)
@pytest.mark.skipif(not gcs_testable, reason="requires gcs")
def test_gcs_bucket_client_list_blobs(with_adapter: str, bucket: str) -> None:
    """Test corner-case in GCS client that isn't easily reachable from Pathy"""
    from pathy.gcs import BucketClientGCS

    client: BucketClientGCS = get_client("gs")
    root = Pathy("gs://invalid_h3gE_ds5daEf_Sdf15487t2n4")
    assert len(list(client.list_blobs(root))) == 0
