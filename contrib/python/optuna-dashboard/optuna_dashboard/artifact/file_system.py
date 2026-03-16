from __future__ import annotations

import os
import shutil
from typing import TYPE_CHECKING
import warnings

from optuna_dashboard.artifact.exceptions import ArtifactNotFound


if TYPE_CHECKING:
    from typing import BinaryIO


class FileSystemBackend:
    """An artifact backend for file systems.

    .. warning::

       This class is deprecated. Please use `optuna.artifacts.FileSystemArtifactStore
       <https://optuna.readthedocs.io/en/latest/reference/generated/optuna.artifacts.
       FileSystemArtifactStore.html>`_ instead.

    Example:
       .. code-block:: python

          import optuna
          from optuna_dashboard.artifact import upload_artifact
          from optuna_dashboard.artifact.file_system import FileSystemBackend

          artifact_backend = FileSystemBackend("./artifacts")

          def objective(trial: optuna.Trial) -> float:
              ... = trial.suggest_float("x", -10, 10)
              file_path = generate_example_png(...)
              upload_artifact(artifact_backend, trial, file_path)
              return ...
    """

    def __init__(self, base_path: str) -> None:
        self._base_path = base_path
        warnings.warn(
            "FileSystemBackend is deprecated. Please use FileSystemArtifactStore instead.\n"
            "See https://optuna-dashboard.readthedocs.io/en/latest/errors.html for details",
            DeprecationWarning,
        )

    def open(self, artifact_id: str) -> BinaryIO:
        filepath = os.path.join(self._base_path, artifact_id)
        try:
            f = open(filepath, "rb")
        except FileNotFoundError as e:
            raise ArtifactNotFound("not found") from e
        return f

    def write(self, artifact_id: str, content_body: BinaryIO) -> None:
        filepath = os.path.join(self._base_path, artifact_id)
        with open(filepath, "wb") as f:
            shutil.copyfileobj(content_body, f)

    def remove(self, artifact_id: str) -> None:
        filepath = os.path.join(self._base_path, artifact_id)
        try:
            os.remove(filepath)
        except FileNotFoundError as e:
            raise ArtifactNotFound("not found") from e


if TYPE_CHECKING:
    # A mypy-runtime assertion to ensure that LocalArtifactBackend
    # implements all abstract methods in ArtifactBackendProtocol.
    from .protocol import ArtifactBackend

    _: ArtifactBackend = FileSystemBackend("")
