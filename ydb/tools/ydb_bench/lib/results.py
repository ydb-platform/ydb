"""Durable, versioned result manifests for benchmark runs.

Schema versions before 4 are deliberately rejected by ``load_manifest``.  They
did not carry immutable plan steps, so interpreting them as resumable results
would be unsafe.
"""

from copy import deepcopy
from pathlib import Path

from ydb.tools.ydb_bench.lib.common import BenchmarkError, atomic_write_json

SCHEMA_VERSION = 4
PENDING = "pending"
RUNNING = "running"
TERMINAL_STATES = frozenset(("passed", "failed", "unsupported", "cancelled"))
_TRANSITIONS = {
    PENDING: frozenset((RUNNING, "unsupported", "cancelled")),
    RUNNING: frozenset(("passed", "failed", "cancelled")),
}


def _non_finite_json_as_null(_value):
    """Migrate manifests written before strict finite-number serialization."""

    return None


def transition(record, state, **fields):
    """Return a new record after one valid lifecycle transition."""
    old = record["state"]
    if state not in _TRANSITIONS.get(old, ()):
        raise BenchmarkError("invalid result state transition {} -> {}".format(old, state))
    updated = dict(record)
    updated.update(fields)
    updated["state"] = state
    return updated


def load_manifest(path):
    import json

    try:
        with Path(path).open(encoding="utf-8") as stream:
            value = json.load(stream, parse_constant=_non_finite_json_as_null)
    except (OSError, ValueError) as error:
        raise BenchmarkError("cannot read result manifest {}: {}".format(path, error)) from error
    if not isinstance(value, dict):
        raise BenchmarkError("result manifest must be a JSON object")
    if value.get("schema_version") != SCHEMA_VERSION:
        raise BenchmarkError("unsupported result manifest schema version {}".format(value.get("schema_version")))
    return value


class ResultStore:
    """Own a manifest and ensure each published version is atomically replaced."""

    def __init__(self, path, manifest):
        self.path = Path(path)
        self.manifest = deepcopy(manifest)
        self.manifest["schema_version"] = SCHEMA_VERSION

    def write(self):
        # atomic_write_json fsyncs the temporary file before replacement.
        atomic_write_json(self.path, self.manifest)

    def transition_step(self, step_id, state, **fields):
        for index, record in enumerate(self.manifest["steps"]):
            if record["id"] != step_id:
                continue
            self.manifest["steps"][index] = transition(record, state, **fields)
            self.write()
            return self.manifest["steps"][index]
        raise BenchmarkError("unknown run step {}".format(step_id))

    def update_step(self, step_id, **fields):
        """Atomically add progress fields without changing a running step's state."""
        for record in self.manifest["steps"]:
            if record["id"] != step_id:
                continue
            if record["state"] != RUNNING:
                raise BenchmarkError("cannot update step {} in state {}".format(step_id, record["state"]))
            record.update(fields)
            self.write()
            return record
        raise BenchmarkError("unknown run step {}".format(step_id))

    def add_artifacts(self, step_id, artifacts):
        """Publish only paths that already exist below the manifest directory."""
        for record in self.manifest["steps"]:
            if record["id"] == step_id:
                break
        else:
            raise BenchmarkError("unknown run step {}".format(step_id))
        if record["state"] != RUNNING:
            raise BenchmarkError("cannot publish artifacts for step {} in state {}".format(step_id, record["state"]))
        root = self.path.parent.resolve()
        checked = []
        for artifact in artifacts:
            candidate = (root / artifact).resolve()
            if root not in candidate.parents or not candidate.is_file():
                raise BenchmarkError("artifact is not durably available: {}".format(artifact))
            checked.append(str(Path(artifact)))
        record["artifacts"] = checked
        self.write()


def write_manifest(path, manifest):
    """Persist a manifest through the sole result-persistence boundary."""
    atomic_write_json(path, manifest)
