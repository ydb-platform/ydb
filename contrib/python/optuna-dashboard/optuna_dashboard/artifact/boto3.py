from __future__ import annotations

import io
import shutil
from typing import TYPE_CHECKING
import warnings

import boto3
from botocore.exceptions import ClientError
from optuna_dashboard.artifact.exceptions import ArtifactNotFound


if TYPE_CHECKING:
    from typing import BinaryIO

    from mypy_boto3_s3 import S3Client


class Boto3Backend:
    """An artifact backend for S3.

    .. warning::

       This class is deprecated. Please use `optuna.artifacts.Boto3ArtifactStore
       <https://optuna.readthedocs.io/en/latest/reference/generated/optuna.artifacts.
       Boto3ArtifactStore.html>`_ instead.

    Example:
       .. code-block:: python

          import optuna
          from optuna_dashboard.artifact import upload_artifact
          from optuna_dashboard.artifact.boto3 import Boto3Backend

          artifact_backend = Boto3Backend("my-bucket")

          def objective(trial: optuna.Trial) -> float:
              ... = trial.suggest_float("x", -10, 10)
              file_path = generate_example_png(...)
              upload_artifact(artifact_backend, trial, file_path)
              return ...
    """

    def __init__(
        self, bucket_name: str, client: S3Client | None = None, *, avoid_buf_copy: bool = False
    ) -> None:
        self.bucket = bucket_name
        self.client = client or boto3.client("s3")
        # This flag is added to avoid that upload_fileobj() method of Boto3 client
        # may close the source file object.
        # See https://github.com/boto/boto3/issues/929
        self._avoid_buf_copy = avoid_buf_copy
        warnings.warn(
            "Boto3Backend is deprecated. Please use Boto3ArtifactStore instead.\n"
            "See https://optuna-dashboard.readthedocs.io/en/latest/errors.html for details",
            DeprecationWarning,
        )

    def open(self, artifact_id: str) -> BinaryIO:
        try:
            obj = self.client.get_object(Bucket=self.bucket, Key=artifact_id)
        except ClientError as e:
            if _is_not_found_error(e):
                raise ArtifactNotFound("not found") from e
            raise
        body = obj.get("Body")
        assert body is not None
        return body  # type: ignore

    def write(self, artifact_id: str, content_body: BinaryIO) -> None:
        fsrc: BinaryIO = content_body
        if not self._avoid_buf_copy:
            buf = io.BytesIO()
            shutil.copyfileobj(content_body, buf)
            buf.seek(0)
            fsrc = buf
        self.client.upload_fileobj(fsrc, self.bucket, artifact_id)

    def remove(self, artifact_id: str) -> None:
        try:
            self.client.delete_object(Bucket=self.bucket, Key=artifact_id)
        except ClientError as e:
            if _is_not_found_error(e):
                raise ArtifactNotFound("not found") from e
            raise


def _is_not_found_error(e: ClientError) -> bool:
    error_code = e.response.get("Error", {}).get("Code")
    http_status_code = e.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    return error_code == "NoSuchKey" or http_status_code == 404


if TYPE_CHECKING:
    # A mypy-runtime assertion to ensure that Boto3Backend
    # implements all abstract methods in ArtifactBackendProtocol.
    from .protocol import ArtifactBackend

    _: ArtifactBackend = Boto3Backend("")
