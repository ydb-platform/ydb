from __future__ import annotations

import logging
from typing import TYPE_CHECKING


_logger = logging.getLogger(__name__)


if TYPE_CHECKING:
    from typing import BinaryIO

    from optuna_dashboard.artifact.protocol import ArtifactBackend


class AppendPrefix:
    """An artifact backend middleware that appends a prefix string to artifact ids.

    Example:
       .. code-block:: python

          import optuna
          from optuna_dashboard.artifact import upload_artifact
          from optuna_dashboard.artifact.boto3 import Boto3Backend
          from optuna_dashboard.artifact.prefix import AppendPrefix

          artifact_backend = AppendPrefix(
             Boto3Backend("my-bucket"),
             prefix="my-folder/"
          )

          def objective(trial: optuna.Trial) -> float:
              ... = trial.suggest_float("x", -10, 10)
              file_path = generate_example_png(...)
              upload_artifact(artifact_backend, trial, file_path)
              return ...
    """

    def __init__(
        self,
        backend: ArtifactBackend,
        prefix: str,
    ) -> None:
        self._backend = backend
        self._prefix = prefix

    def _with_prefix(self, artifact_id: str) -> str:
        return self._prefix + artifact_id

    def open(self, artifact_id: str) -> BinaryIO:
        return self._backend.open(self._with_prefix(artifact_id))

    def write(self, artifact_id: str, content_body: BinaryIO) -> None:
        return self._backend.write(self._with_prefix(artifact_id), content_body)

    def remove(self, artifact_id: str) -> None:
        return self._backend.remove(self._with_prefix(artifact_id))


if TYPE_CHECKING:
    # A mypy-runtime assertion to ensure that SCSBackend
    # implements all abstract methods in ArtifactBackendProtocol.
    from optuna_dashboard.artifact.file_system import FileSystemBackend

    _: ArtifactBackend = AppendPrefix(FileSystemBackend("."), "prefix-")
