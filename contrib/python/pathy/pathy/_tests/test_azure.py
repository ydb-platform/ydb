from dataclasses import dataclass

import mock
import pytest

from pathy import Pathy, get_client

from . import azure_testable
from .conftest import ENV_ID

AZURE_ADAPTER = ["azure"]


@pytest.mark.parametrize("adapter", AZURE_ADAPTER)
@pytest.mark.skipif(not azure_testable, reason="requires azure")
def test_azure_recreate_expected_args(with_adapter: str) -> None:
    from pathy.azure import BucketClientAzure

    # Must specify either service, or connection_string
    with pytest.raises(ValueError):
        BucketClientAzure()


@pytest.mark.parametrize("adapter", AZURE_ADAPTER)
@pytest.mark.skipif(not azure_testable, reason="requires azure")
def test_azure_bucket_get_blob_failure_cases(with_adapter: str, bucket: str) -> None:
    root = Pathy(f"{with_adapter}://{bucket}")
    client = root.client(root)
    azure_bucket = client.get_bucket(root)
    # Returns none if blob contains invalid character
    assert azure_bucket.get_blob("invlid_bkt_nme_18$%^@57582397?.___asd") is None


@pytest.mark.parametrize("adapter", AZURE_ADAPTER)
@pytest.mark.skipif(not azure_testable, reason="requires azure")
def test_azure_bucket_client_list_blobs(with_adapter: str, bucket: str) -> None:
    """Test corner-case in Azure client that isn't easily reachable from Pathy"""
    from pathy.azure import BucketClientAzure

    client: BucketClientAzure = get_client("azure")

    # Invalid bucket
    root = Pathy("azure://invalid_h3gE_ds5daEf_Sdf15487t2n4")
    blobs = list(client.list_blobs(root))
    assert blobs == []

    # Invalid search prefix
    root = Pathy(f"azure://{bucket}")
    blobs = list(client.list_blobs(root, prefix="#@/:|*?%@^@$^@$@#$@#$"))
    assert blobs == []


@dataclass
class MockAzureBlobClientCopyProperties:
    status: str
    id: str


@dataclass
class MockAzureBlobClientProperties:
    copy: MockAzureBlobClientCopyProperties


@dataclass
class MockAzureBlobClient:
    aborted = False

    def start_copy_from_url(self, url: str) -> None:
        pass

    def get_blob_properties(self) -> MockAzureBlobClientProperties:
        return MockAzureBlobClientProperties(
            MockAzureBlobClientCopyProperties(status="failed", id="test")
        )

    def abort_copy(self, copy_id: str) -> None:
        self.aborted = True


@dataclass
class MockAzureBlobServiceClient:
    mock_client: MockAzureBlobClient

    def get_blob_client(self, container: str, blob_name: str) -> MockAzureBlobClient:
        return self.mock_client


@pytest.mark.parametrize("adapter", AZURE_ADAPTER)
@pytest.mark.skipif(not azure_testable, reason="requires azure")
def test_azure_bucket_copy_blob_aborts_copy_on_failure(
    with_adapter: str, bucket: str
) -> None:
    """Verify abort_copy is called when copy status != 'success'"""
    root: Pathy = Pathy(f"{with_adapter}://{bucket}/{ENV_ID}/azure_abort_copy")

    # Create a source blob to copy
    cool_blob = root / "file.txt"
    cool_blob.write_text("cool")
    client = root.client(root)

    # Get the bucket client / source blob
    azure_bucket = client.get_bucket(root)
    azure_blob = azure_bucket.get_blob(cool_blob.prefix[:-1])
    assert azure_blob is not None

    # Mock out blob client to catch `abort_copy` call
    mock_blob_client = MockAzureBlobClient()
    mock_bucket = MockAzureBlobServiceClient(mock_blob_client)
    assert mock_blob_client.aborted is False
    with mock.patch.object(azure_bucket, "client", new=mock_bucket):
        assert azure_bucket.copy_blob(azure_blob, azure_bucket, "file2.txt") is None
    # abort_copy was called
    assert mock_blob_client.aborted is True
