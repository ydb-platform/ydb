from __future__ import annotations

from typing import TYPE_CHECKING

from optuna_dashboard.artifact.exceptions import ArtifactNotFound as DashboardArtifactNotFound


if TYPE_CHECKING:
    from typing import BinaryIO
    from typing import TypeGuard

    from optuna.artifacts._protocol import ArtifactStore

    from .protocol import ArtifactBackend


def is_artifact_backend(store: ArtifactBackend | ArtifactStore) -> TypeGuard[ArtifactBackend]:
    return getattr(store, "open_reader", None) is None


def to_artifact_store(store: ArtifactBackend | ArtifactStore) -> ArtifactStore:
    if is_artifact_backend(store):
        return ArtifactBackendToStore(store)
    # mypy cannot infer the type of `store` here.
    return store  # type: ignore


class ArtifactBackendToStore:
    """Converts a Dashboard's ArtifactBackend to Optuna's ArtifactStore."""

    def __init__(self, artifact_backend: ArtifactBackend) -> None:
        self._backend = artifact_backend

    def open_reader(self, artifact_id: str) -> BinaryIO:
        from optuna.artifacts.exceptions import ArtifactNotFound

        try:
            return self._backend.open(artifact_id)
        except DashboardArtifactNotFound as e:
            raise ArtifactNotFound from e

    def write(self, artifact_id: str, content_body: BinaryIO) -> None:
        self._backend.write(artifact_id, content_body)

    def remove(self, artifact_id: str) -> None:
        from optuna.artifacts.exceptions import ArtifactNotFound

        try:
            self._backend.remove(artifact_id)
        except DashboardArtifactNotFound as e:
            raise ArtifactNotFound from e
