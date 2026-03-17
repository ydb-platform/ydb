from __future__ import annotations

import csv
from dataclasses import dataclass
import functools
import importlib
import io
from itertools import chain
import logging
import mimetypes
import os
import re
import typing
import warnings

from bottle import Bottle
from bottle import redirect
from bottle import request
from bottle import response
from bottle import run
from bottle import static_file
import optuna
from optuna.exceptions import DuplicatedStudyError
from optuna.storages import BaseStorage
from optuna.study import StudyDirection
from optuna.trial import TrialState

from . import _note as note
from ._bottle_util import BottleViewReturn
from ._bottle_util import json_api_view
from ._custom_plot_data import get_plotly_graph_objects
from ._importance import get_param_importance_from_trials_cache
from ._inmemory_cache import get_cached_extra_study_property
from ._inmemory_cache import InMemoryCache
from ._pareto_front import get_pareto_front_trials
from ._preference_setting import _register_preference_feedback_component
from ._preferential_history import NewHistory
from ._preferential_history import PreferenceHistoryNotFound
from ._preferential_history import remove_history
from ._preferential_history import report_history
from ._preferential_history import restore_history
from ._rdb_migration import register_rdb_migration_route
from ._serializer import serialize_frozen_study
from ._serializer import serialize_study_detail
from ._storage import create_new_study
from ._storage import get_studies
from ._storage import get_study
from ._storage import get_trials
from ._storage_url import get_storage
from .artifact._backend import delete_all_artifacts
from .artifact._backend import register_artifact_route
from .artifact._backend_to_store import to_artifact_store
from .llm._api_views import register_llm_route
from .preferential._study import _SYSTEM_ATTR_PREFERENTIAL_STUDY
from .preferential._study import get_best_trials as get_best_preferential_trials
from .preferential._system_attrs import get_skipped_trial_ids
from .preferential._system_attrs import report_skip


if typing.TYPE_CHECKING:
    from typing import Any
    from typing import Literal

    from _typeshed.wsgi import WSGIApplication
    from optuna.artifacts._protocol import ArtifactStore
    from optuna_dashboard.artifact.protocol import ArtifactBackend
    from optuna_dashboard.llm.provider import LLMProvider


logger = logging.getLogger(__name__)

# Static files
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STATIC_DIR = os.path.join(BASE_DIR, "public")
IMG_DIR = os.path.join(BASE_DIR, "img")
cached_path_exists = functools.lru_cache(maxsize=10)(os.path.exists)


@dataclass
class JupyterLabExtensionContext:
    base_url: str


def create_app(
    storage: BaseStorage,
    artifact_store: ArtifactStore | None = None,
    llm_provider: LLMProvider | None = None,
    debug: bool = False,
    jupyterlab_extension_context: JupyterLabExtensionContext | None = None,
    allow_unsafe: bool = False,
) -> Bottle:
    app = Bottle()
    app._inmemory_cache = InMemoryCache()

    @app.hook("before_request")
    def remove_trailing_slashes_hook() -> None:
        request.environ["PATH_INFO"] = request.environ["PATH_INFO"].rstrip("/")

    @app.get("/")
    def index() -> BottleViewReturn:
        return redirect("/dashboard", 302)  # Status Found

    # Accept any following paths for client-side routing
    @app.get("/dashboard<:re:(/.*)?>")
    def dashboard() -> BottleViewReturn:
        if allow_unsafe:
            headers = {}
        else:
            # CSP header
            if llm_provider is not None:
                script_src_str = "script-src 'self' 'unsafe-inline' 'unsafe-eval'"
            else:
                # Parallel coordinate, which uses WebGL, requires unsafe-eval.
                script_src_str = "script-src 'self' 'unsafe-eval'"

            csp_string = ";".join(
                [
                    "default-src 'self'",
                    "img-src 'self' data: blob:",
                    "frame-src 'self'",
                    "object-src 'none'",
                    "connect-src 'self'",
                    "style-src 'self' data: 'unsafe-inline'",
                    script_src_str,
                ]
            )
            headers = {"Content-Security-Policy": csp_string}

        return static_file("index.html", BASE_DIR, mimetype="text/html", headers=headers)

    @app.get("/api/meta")
    @json_api_view
    def api_meta() -> dict[str, Any]:
        meta: dict[str, Any] = {
            "artifact_is_available": artifact_store is not None,
            "llm_is_available": llm_provider is not None,
            "plotlypy_is_available": importlib.util.find_spec("plotly") is not None,
            "allow_unsafe": allow_unsafe,
        }
        if jupyterlab_extension_context is not None:
            meta["jupyterlab_extension_context"] = {
                "base_url": jupyterlab_extension_context.base_url
            }
        return meta

    @app.get("/api/studies")
    @json_api_view
    def list_studies() -> dict[str, Any]:
        studies = get_studies(storage)
        serialized = [serialize_frozen_study(s) for s in studies]
        # TODO(umezawa): Rename `study_summaries` to `studies`.
        return {
            "study_summaries": serialized,
        }

    @app.post("/api/studies")
    @json_api_view
    def create_study() -> dict[str, Any]:
        study_name = request.json.get("study_name", None)
        request_directions = [d.lower() for d in request.json.get("directions", [])]
        if (
            study_name is None
            or len(request_directions) == 0
            or not all([d in ("minimize", "maximize") for d in request_directions])
        ):
            response.status = 400  # Bad request
            return {"reason": "You need to set study_name and direction"}

        directions = [
            StudyDirection.MAXIMIZE if d == "maximize" else StudyDirection.MINIMIZE
            for d in request_directions
        ]
        try:
            study_id = create_new_study(storage, study_name, directions)
        except DuplicatedStudyError:
            response.status = 400  # Bad request
            return {"reason": f"'{study_name}' already exists"}

        study = get_study(storage, study_id)
        if study is None:
            response.status = 500  # Internal server error
            return {"reason": "Failed to create study"}
        response.status = 201  # Created
        return {"study_summary": serialize_frozen_study(study)}

    @app.post("/api/studies/<study_id:int>/rename")
    @json_api_view
    def rename_study(study_id: int) -> dict[str, Any]:
        dst_study_name = request.json.get("study_name", None)
        if dst_study_name is None:
            response.status = 400  # Bad request
            return {"reason": "You need to set study_name and direction"}

        src_study_name = storage.get_study_name_from_id(study_id)
        try:
            src_study = optuna.load_study(storage=storage, study_name=src_study_name)
        except KeyError:
            response.status = 404  # Not found
            return {"reason": f"study_id={study_id} is not found"}

        try:
            dst_study = optuna.create_study(
                storage=storage, study_name=dst_study_name, directions=src_study.directions
            )
            dst_study.add_trials(src_study.get_trials(deepcopy=False))
            for key, value in src_study.user_attrs.items():
                dst_study.set_user_attr(key, value)
            note.copy_notes(storage, src_study, dst_study)
        except DuplicatedStudyError:
            response.status = 400  # Bad request
            return {"reason": f"study_name={dst_study_name} is duplicaated"}
        except Exception as e:
            logger.exception("Unexpected error:")
            response.status = 500
            storage.delete_study(dst_study._study_id)
            return {"reason": str(e)}
        new_study = get_study(storage, dst_study._study_id)
        if new_study is None:
            response.status = 500
            return {"reason": "Failed to load the new study"}

        storage.delete_study(src_study._study_id)
        response.status = 201
        return serialize_frozen_study(new_study)

    @app.delete("/api/studies/<study_id:int>")
    @json_api_view
    def delete_study(study_id: int) -> dict[str, Any]:
        data = request.json or {}
        remove_associated_artifacts = data.get("remove_associated_artifacts", True)

        if artifact_store is not None and remove_associated_artifacts:
            delete_all_artifacts(artifact_store, storage, study_id)

        try:
            storage.delete_study(study_id)
        except KeyError:
            response.status = 404  # Not found
            return {"reason": f"study_id={study_id} is not found"}
        response.status = 204  # No content
        return {}

    @app.get("/api/studies/<study_id:int>")
    @json_api_view
    def get_study_detail(study_id: int) -> dict[str, Any]:
        try:
            after = int(request.params["after"])
            assert after >= 0
        except AssertionError:
            response.status = 400  # Bad parameter
            return {"reason": "`after` should be larger or equal 0."}
        except KeyError:
            after = 0
        study = get_study(storage, study_id)
        if study is None:
            response.status = 404  # Not found
            return {"reason": f"study_id={study_id} is not found"}
        trials = get_trials(app._inmemory_cache, storage, study_id)

        system_attrs = getattr(study, "system_attrs", {})
        is_preferential = system_attrs.get(_SYSTEM_ATTR_PREFERENTIAL_STUDY, False)
        # TODO(c-bata): Cache best_trials
        if is_preferential:
            best_trials = get_best_preferential_trials(study_id, storage)
        elif len(study.directions) == 1:
            if len([t for t in trials if t.state == TrialState.COMPLETE]) == 0:
                best_trials = []
            else:
                best_trials = [storage.get_best_trial(study_id)]
        else:
            best_trials = get_pareto_front_trials(trials=trials, directions=study.directions)
        (
            # TODO: intersection_search_space and union_search_space look more clear since now we
            # have union_user_attrs.
            intersection,
            union,
            union_user_attrs,
            has_intermediate_values,
        ) = get_cached_extra_study_property(app._inmemory_cache, study_id, trials)

        plotly_graph_objects = get_plotly_graph_objects(system_attrs)
        skipped_trial_ids = get_skipped_trial_ids(system_attrs)
        skipped_trial_numbers = [t.number for t in trials if t._trial_id in skipped_trial_ids]
        return serialize_study_detail(
            study,
            best_trials,
            trials[after:],
            intersection,
            union,
            union_user_attrs,
            has_intermediate_values,
            plotly_graph_objects,
            skipped_trial_numbers,
        )

    @app.get("/api/studies/<study_id:int>/param_importances")
    @json_api_view
    def get_param_importances(study_id: int) -> dict[str, Any]:
        try:
            n_directions = len(storage.get_study_directions(study_id))
        except KeyError:
            response.status = 404  # Study is not found
            return {"reason": f"study_id={study_id} is not found"}

        trials = get_trials(app._inmemory_cache, storage, study_id)
        try:
            importances = [
                get_param_importance_from_trials_cache(
                    app._inmemory_cache,
                    storage,
                    study_id,
                    objective_id,
                    trials,
                )
                for objective_id in range(n_directions)
            ]
            return {"param_importances": importances}
        except ValueError as e:
            response.status = 400  # Bad request
            return {"reason": str(e)}

    @app.get("/api/studies/<study_id:int>/plot/<plot_type>")
    @json_api_view
    def get_plot(study_id: int, plot_type: str) -> dict[str, Any]:
        study = optuna.load_study(
            study_name=storage.get_study_name_from_id(study_id), storage=storage
        )
        if plot_type == "contour":
            fig = optuna.visualization.plot_contour(study)
        elif plot_type == "slice":
            fig = optuna.visualization.plot_slice(study)
            # Note: Optuna's implementation forces a minimum width.
            # We override it to prevent the figure from going beyond the screen width.
            # https://github.com/optuna/optuna/blob/2abd0ae81eaf3683ce1dd580429904c8a705300d/optuna/visualization/_slice.py#L237-L239
            fig.update_layout(width=None)
        elif plot_type == "parallel_coordinate":
            fig = optuna.visualization.plot_parallel_coordinate(study)
        elif plot_type == "rank":
            fig = optuna.visualization.plot_rank(study)
        elif plot_type == "edf":
            fig = optuna.visualization.plot_edf(study)
        elif plot_type == "timeline":
            fig = optuna.visualization.plot_timeline(study)
        elif plot_type == "param_importances":
            fig = optuna.visualization.plot_param_importances(study)
        elif plot_type == "pareto_front":
            fig = optuna.visualization.plot_pareto_front(study)
        else:
            response.status = 404  # Not found
            return {"reason": f"plot_type={plot_type} is not supported."}
        return fig.to_json()

    @app.get("/api/compare-studies/plot/<plot_type>")
    @json_api_view
    def get_compare_studies_plot(plot_type: str) -> dict[str, Any]:
        study_ids = map(int, request.query.getall("study_ids[]"))
        studies = [
            optuna.load_study(study_name=storage.get_study_name_from_id(study_id), storage=storage)
            for study_id in study_ids
        ]
        if plot_type == "edf":
            fig = optuna.visualization.plot_edf(studies)
        else:
            response.status = 404  # Not found
            return {"reason": f"plot_type={plot_type} is not supported."}
        return fig.to_json()

    @app.put("/api/studies/<study_id:int>/note")
    @json_api_view
    def save_study_note(study_id: int) -> dict[str, Any]:
        req_note_ver = request.json.get("version", None)
        req_note_body = request.json.get("body", None)
        if req_note_ver is None or req_note_body is None:
            response.status = 400  # Bad request
            return {"reason": "Invalid request."}

        system_attrs = storage.get_study_system_attrs(study_id)
        if not note.version_is_incremented(system_attrs, None, req_note_ver):
            response.status = 409  # Conflict
            return {
                "reason": "The text you are editing has changed. "
                "Please copy your edits and refresh the page.",
                "note": note.get_note_from_system_attrs(system_attrs, None),
            }

        note.save_note_with_version(storage, study_id, None, req_note_ver, req_note_body)
        response.status = 204  # No content
        return {}

    @app.post("/api/studies/<study_id:int>/preference")
    @json_api_view
    def post_preference(study_id: int) -> dict[str, Any]:
        try:
            mode = request.json.get("mode", "")
            candidates = [int(d) for d in request.json.get("candidates", [])]
            clicked = int(request.json.get("clicked", -1))
        except ValueError:
            response.status = 400
            return {
                "reason": (
                    "`candidates` should be an array of integers and "
                    "`clicked` should be an integer."
                )
            }

        if clicked == -1:
            response.status = 400
            return {"reason": "`clicked` should be specified."}
        if mode != "ChooseWorst":
            response.status = 400
            return {"reason": "`mode` should be 'ChooseWorst'."}

        report_history(
            study_id,
            storage,
            NewHistory(
                mode=mode,
                candidates=candidates,
                clicked=clicked,
            ),
        )

        response.status = 204
        return {}

    @app.put("/api/studies/<study_id:int>/preference_feedback_component")
    @json_api_view
    def put_preference_feedback_component(study_id: int) -> dict[str, Any]:
        try:
            component_type = request.json.get("output_type", "")
            artifact_key = request.json.get("artifact_key", None)
        except ValueError:
            response.status = 400
            return {"reason": "invalid request."}
        if component_type not in ["note", "artifact"]:
            response.status = 400
            return {"reason": "component_type must be either 'note' or 'artifact'."}

        _register_preference_feedback_component(
            study_id=study_id,
            storage=storage,
            component_type=component_type,
            artifact_key=artifact_key,
        )
        response.status = 204
        return {}

    @app.delete("/api/studies/<study_id:int>/preference/<history_id>")
    @json_api_view
    def remove_preference(study_id: int, history_id: str) -> dict[str, Any]:
        try:
            remove_history(study_id, storage, history_id)
        except PreferenceHistoryNotFound:
            response.status = 404
            return {"reason": f"history_id={history_id} is not found"}

        response.status = 204
        return {}

    @app.post("/api/studies/<study_id:int>/preference/<history_id>")
    @json_api_view
    def restore_preference(study_id: int, history_id: str) -> dict[str, Any]:
        try:
            restore_history(study_id, storage, history_id)
        except PreferenceHistoryNotFound:
            response.status = 404
            return {"reason": f"history_id={history_id} is not found"}

        response.status = 204
        return {}

    @app.post("/api/trials/<trial_id:int>/tell")
    @json_api_view
    def tell_trial(trial_id: int) -> dict[str, Any]:
        if "state" not in request.json:
            response.status = 400  # Bad request
            return {"reason": "state must be specified."}

        try:
            state = TrialState[request.json["state"].upper()]
        except Exception:  # To catch KeyError and Exception by non str case.
            response.status = 400  # Bad request
            return {"reason": "state must be either 'Complete' or 'Fail'."}

        if state not in [TrialState.COMPLETE, TrialState.FAIL]:
            response.status = 400  # Bad request
            return {"reason": "state must be either 'Complete' or 'Fail'."}

        values = None
        if state == TrialState.COMPLETE:
            vs = request.json.get("values")
            if vs is None:
                response.status = 400  # Bad request
                return {"reason": "values attribute is required when state is 'Complete'."}
            try:
                values = [float(v) for v in vs]
            except (ValueError, TypeError):
                response.status = 400  # Bad request
                return {"reason": "values attribute must be an array of numbers"}

        storage.set_trial_state_values(trial_id, state, values)

        response.status = 204
        return {}

    @app.post("/api/trials/<trial_id:int>/user-attrs")
    @json_api_view
    def save_trial_user_attrs(trial_id: int) -> dict[str, Any]:
        user_attrs = request.json.get("user_attrs", {})
        if not user_attrs:
            response.status = 400  # Bad request
            return {"reason": "user_attrs must be specified."}

        for key, val in user_attrs.items():
            storage.set_trial_user_attr(trial_id, key, val)

        response.status = 204
        return {}

    @app.post("/api/studies/<study_id:int>/<trial_id:int>/skip")
    @json_api_view
    def skip_trial(study_id: int, trial_id: int) -> dict[str, Any]:
        try:
            system_attrs = storage.get_study_system_attrs(study_id)
        except KeyError:
            response.status = 404  # Not found
            return {"reason": f"study_id={study_id} is not found"}
        is_preferential = system_attrs.get(_SYSTEM_ATTR_PREFERENTIAL_STUDY, False)
        if not is_preferential:
            response.status = 400  # Bad request
            return {"reason": "The study is not preferential."}

        report_skip(study_id, trial_id, storage)
        response.status = 204  # No content
        return {}

    @app.put("/api/studies/<study_id:int>/<trial_id:int>/note")
    @json_api_view
    def save_trial_note(study_id: int, trial_id: int) -> dict[str, Any]:
        req_note_ver = request.json.get("version", None)
        req_note_body = request.json.get("body", None)
        if req_note_ver is None or req_note_body is None:
            response.status = 400  # Bad request
            return {"reason": "Invalid request."}

        # Store note content in study system attrs since it's always updatable.
        system_attrs = storage.get_study_system_attrs(study_id=study_id)
        if not note.version_is_incremented(system_attrs, trial_id, req_note_ver):
            response.status = 409  # Conflict
            return {
                "reason": "The text you are editing has changed. "
                "Please copy your edits and refresh the page.",
                "note": note.get_note_from_system_attrs(system_attrs, trial_id),
            }

        note.save_note_with_version(storage, study_id, trial_id, req_note_ver, req_note_body)
        response.status = 204  # No content
        return {}

    @app.get("/csv/<study_id:int>")
    def download_csv(study_id: int) -> BottleViewReturn:
        trial_ids_str = request.query.get("trial_ids", "")
        trial_ids: list[int] | None = None
        if trial_ids_str:
            try:
                trial_ids = [int(tid.strip()) for tid in trial_ids_str.split(",")]
            except ValueError:
                response.status = 400  # Bad Request
                return {"reason": "Invalid trial_ids format. Expected comma-separated integers"}

        # Create a CSV file
        try:
            study_name = storage.get_study_name_from_id(study_id)
            study = optuna.load_study(storage=storage, study_name=study_name)
        except KeyError:
            response.status = 404  # Not found
            return {"reason": f"study_id={study_id} is not found"}

        if trial_ids is not None:
            trials = [t for t in study.trials if t.number in trial_ids]
            if not trials:
                response.status = 404
                return {"reason": "all specified trial_ids is not found"}
        else:
            trials = study.trials

        param_names = sorted(set(chain.from_iterable([t.params.keys() for t in trials])))
        user_attr_names = sorted(set(chain.from_iterable([t.user_attrs.keys() for t in trials])))
        param_names_header = [f"Param {x}" for x in param_names]
        user_attr_names_header = [f"UserAttribute {x}" for x in user_attr_names]
        n_objs = len(study.directions)
        if hasattr(study, "metric_names") and study.metric_names is not None:
            value_header = study.metric_names
        else:  # optuna < v3.4.0
            value_header = ["Value"] if n_objs == 1 else [f"Objective {x}" for x in range(n_objs)]
        column_names = (
            ["Number", "State"] + value_header + param_names_header + user_attr_names_header
        )

        buf = io.StringIO("")
        writer = csv.writer(buf)
        writer.writerow(column_names)
        for frozen_trial in trials:
            row = [frozen_trial.number, frozen_trial.state.name]
            row.extend(frozen_trial.values if frozen_trial.values is not None else [None] * n_objs)
            row.extend([frozen_trial.params.get(name, None) for name in param_names])
            row.extend([frozen_trial.user_attrs.get(name, None) for name in user_attr_names])
            writer.writerow(row)

        # Set response headers
        output_name = "-".join(re.sub(r'[\\/:*?"<>|]+', "", study_name).split(" "))
        response.headers["Content-Type"] = "text/csv; chatset=cp932"
        response.headers["Content-Disposition"] = f"attachment; filename={output_name}.csv"

        # Response body
        buf.seek(0)
        return buf.read()

    @app.get("/favicon.ico")
    def favicon() -> BottleViewReturn:
        use_gzip = "gzip" in request.headers["Accept-Encoding"]
        filename = "favicon.ico.gz" if use_gzip else "favicon.ico"
        return static_file(filename, IMG_DIR)

    @app.get("/static/<filename:path>")
    def send_static(filename: str) -> BottleViewReturn:
        mimetype: str | Literal[True] = True
        headers: dict[str, str] | None = None
        if not debug and "gzip" in request.headers["Accept-Encoding"]:
            gz_filename = filename.strip("/\\") + ".gz"
            if cached_path_exists(os.path.join(STATIC_DIR, gz_filename)):
                filename = gz_filename
                headers = {"Content-Encoding": "gzip"}

            mimetype_, _ = mimetypes.guess_type(filename)
            if mimetype_ is not None:
                mimetype = mimetype_
        return static_file(filename, root=STATIC_DIR, mimetype=mimetype, headers=headers)

    register_rdb_migration_route(app, storage)
    register_artifact_route(app, storage, artifact_store)
    register_llm_route(app, llm_provider)
    return app


def run_server(
    storage: str | BaseStorage,
    host: str = "localhost",
    port: int = 8080,
    artifact_store: ArtifactStore | ArtifactBackend | None = None,
    *,
    artifact_backend: ArtifactBackend | None = None,
    llm_provider: LLMProvider | None = None,
) -> None:
    """Start running optuna-dashboard and blocks until the server terminates.

    This function uses wsgiref module which is not intended for the production
    use. If you want to run optuna-dashboard more secure and/or more fast,
    please use WSGI server like Gunicorn or uWSGI via :func:`wsgi` function.

    Args:
        storage: Optuna storage.
        port: The port number to listen on.
        host: The hostname or IP address to bind to.
        artifact_store: Optuna's Artifact store (optional).
        llm_provider: LLM providers defined under the ``optuna_dashboard.llm`` package (optional).
    """
    # TODO(c-bata): Remove artifact_backend keyword argument in the future release.
    store: ArtifactStore | None = None
    if artifact_store is not None:
        store = to_artifact_store(artifact_store)
    elif artifact_backend is not None:
        warnings.warn(
            "The `artifact_backend` argument is deprecated. Please use `artifact_store` instead.",
            DeprecationWarning,
        )
        store = to_artifact_store(artifact_backend)

    app = create_app(
        get_storage(storage),
        artifact_store=store,
        llm_provider=llm_provider,
    )
    run(app, host=host, port=port)


def wsgi(
    storage: str | BaseStorage,
    artifact_store: ArtifactBackend | ArtifactStore | None = None,
    *,
    artifact_backend: ArtifactBackend | None = None,
    llm_provider: LLMProvider | None = None,
    jupyterlab_extension_context: JupyterLabExtensionContext | None = None,
) -> WSGIApplication:
    """This function exposes WSGI interface for people who want to run on the
    production-class WSGI servers like Gunicorn or uWSGI.

    Args:
        storage: Optuna storage.
        artifact_store: Optuna's Artifact store (optional).
        llm_provider: LLM providers defined under the ``optuna_dashboard.llm`` package (optional).

    Returns:
        WSGI application object.
    """
    # TODO(c-bata): Remove artifact_backend keyword argument in the future release.
    store: ArtifactStore | None = None
    if artifact_store is not None:
        store = to_artifact_store(artifact_store)
    elif artifact_backend is not None:
        warnings.warn(
            "The `artifact_backend` argument is deprecated. Please use `artifact_store` instead.",
            DeprecationWarning,
        )
        store = to_artifact_store(artifact_backend)

    return create_app(
        get_storage(storage),
        artifact_store=store,
        llm_provider=llm_provider,
        jupyterlab_extension_context=jupyterlab_extension_context,
    )
