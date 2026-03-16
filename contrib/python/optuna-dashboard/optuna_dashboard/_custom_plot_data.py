from __future__ import annotations

import math
from typing import TYPE_CHECKING
import uuid

from optuna import Study


if TYPE_CHECKING:
    from typing import Any

    from optuna.storages import BaseStorage
    import plotly.graph_objs as go


SYSTEM_ATTR_PLOT_DATA = "dashboard:plot_data:"
SYSTEM_ATTR_MAX_LENGTH = 2045


def save_plotly_graph_object(
    study: Study, figure: go.Figure, *, graph_object_id: str | None = None
) -> str:
    """Save the user-defined plotly's graph object to the study.

    Example:

       .. code-block:: python

          import optuna
          from optuna_dashboard import save_plotly_graph_object

          def objective(trial):
              x = trial.suggest_float("x", -100, 100)
              y = trial.suggest_categorical("y", [-1, 0, 1])
              return x**2 + y

          study = optuna.create_study()
          study.optimize(objective, n_trials=100)

          figure = optuna.visualization.plot_optimization_history(study)
          save_plotly_graph_object(study, figure)

    Args:
        study:
            Target study object.
        figure:
            A :class:`plotly.graph_objects.Figure` object to save.
        graph_object_id:
            Unique identifier of the graph object. If specified, the graph object is overwritten.
            This must be a valid HTML id attribute value.

    Returns:
        The graph object ID.
    """
    if graph_object_id is not None and not is_valid_graph_object_id(graph_object_id):
        raise ValueError("graph_object_id must be a valid HTML id attribute value.")

    storage = study._storage
    study_id = study._study_id

    graph_object_id = graph_object_id or str(uuid.uuid4())
    key = SYSTEM_ATTR_PLOT_DATA + graph_object_id + ":"
    plot_data_json_str = figure.to_json()
    save_graph_object_json(storage, study_id, key, plot_data_json_str)
    return graph_object_id


def save_graph_object_json(
    storage: BaseStorage, study_id: int, key_prefix: str, plot_data_json_str: str
) -> None:
    plot_data_system_attrs = split_plot_data(plot_data_json_str, key_prefix)
    for k, v in plot_data_system_attrs.items():
        storage.set_study_system_attr(study_id, k, v)

    # Clear previous graph object attributes
    study_system_attrs = storage.get_study_system_attrs(study_id)
    all_plot_data_system_attrs = [k for k in study_system_attrs if k.startswith(key_prefix)]
    if len(all_plot_data_system_attrs) > len(plot_data_system_attrs):
        for i in range(len(plot_data_system_attrs), len(all_plot_data_system_attrs)):
            storage.set_study_system_attr(study_id, f"{key_prefix}{i}", "")


def list_graph_object_ids(system_attrs: dict[str, Any]) -> list[str]:
    titles = set()
    for key in system_attrs:
        if not key.startswith(SYSTEM_ATTR_PLOT_DATA):
            continue

        s = key.split(":", maxsplit=2)  # e.g. ["dashboard", "plot_data", "Optimization History:1"]
        if len(s) != 3:
            continue
        # Please note that title may contain ":".
        title = s[2].rsplit(":", maxsplit=1)[0]
        titles.add(title)
    return list(titles)


def get_plotly_graph_objects(system_attrs: dict[str, Any]) -> dict[str, str]:
    graph_objects = {}
    for title in list_graph_object_ids(system_attrs):
        key_prefix = SYSTEM_ATTR_PLOT_DATA + title + ":"
        plot_data_attrs = {k: v for k, v in system_attrs.items() if k.startswith(key_prefix)}
        graph_objects[title] = concat_plot_data(plot_data_attrs, key_prefix)
    return graph_objects


def split_plot_data(plot_data_str: str, key_prefix: str) -> dict[str, str]:
    plot_data_len = len(plot_data_str)
    attrs = {}
    for i in range(math.ceil(plot_data_len / SYSTEM_ATTR_MAX_LENGTH)):
        start = i * SYSTEM_ATTR_MAX_LENGTH
        end = min((i + 1) * SYSTEM_ATTR_MAX_LENGTH, plot_data_len)
        attrs[f"{key_prefix}{i}"] = plot_data_str[start:end]
    return attrs


def concat_plot_data(plot_data_attrs: dict[str, str], key_prefix: str) -> str:
    return "".join(plot_data_attrs[f"{key_prefix}{i}"] for i in range(len(plot_data_attrs)))


def is_valid_graph_object_id(graph_object_id: str) -> bool:
    if len(graph_object_id) == 0:
        return False

    # Can only contain letters [A-Za-z], numbers [0-9], hyphens ("-"), underscores ("_"),
    # colons, and periods.
    if not all(
        "a" <= c <= "z" or "A" <= c <= "Z" or "0" <= c <= "9" or c in ("-", "_", ":", ".")
        for c in graph_object_id[1:]
    ):
        return False
    # Unlike HTML id attribute, graph object id can begin with a letter [A-Za-z]
    return True
