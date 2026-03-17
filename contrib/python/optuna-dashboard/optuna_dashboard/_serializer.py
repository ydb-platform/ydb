from __future__ import annotations

from datetime import datetime
import json
import numbers
from typing import Any
from typing import TYPE_CHECKING
from typing import Union

import numpy as np
from optuna.distributions import BaseDistribution
from optuna.distributions import CategoricalDistribution
from optuna.distributions import FloatDistribution
from optuna.distributions import IntDistribution
from optuna.study._frozen import FrozenStudy
from optuna.trial import FrozenTrial

from . import _note as note
from ._form_widget import get_form_widgets_json
from ._named_objectives import get_objective_names
from ._preference_setting import _SYSTEM_ATTR_FEEDBACK_COMPONENT
from ._preferential_history import _SYSTEM_ATTR_PREFIX_HISTORY
from .artifact._backend import list_study_artifacts
from .artifact._backend import list_trial_artifacts
from .preferential._study import _SYSTEM_ATTR_PREFERENTIAL_STUDY
from .preferential._system_attrs import get_preferences
from .preferential._system_attrs import is_preference_removed


if TYPE_CHECKING:
    from typing import Literal
    from typing import TypedDict

    from ._preferential_history import History
    from ._preferential_history import SerializedHistory

    Attribute = TypedDict(
        "Attribute",
        {
            "key": str,
            "value": str,
        },
    )
    AttributeSpec = TypedDict(
        "AttributeSpec",
        {
            "key": str,
            "sortable": bool,
        },
    )
    IntermediateValue = TypedDict(
        "IntermediateValue",
        {
            "step": int,
            "value": Union[float, Literal["inf", "-inf", "nan"]],
        },
    )

    FloatDistributionJSON = TypedDict(
        "FloatDistributionJSON",
        {
            "type": Literal["FloatDistribution"],
            "low": float,
            "high": float,
            "step": Union[float, None],
            "log": bool,
        },
    )
    IntDistributionJSON = TypedDict(
        "IntDistributionJSON",
        {
            "type": Literal["IntDistribution"],
            "low": int,
            "high": int,
            "step": int,
            "log": bool,
        },
    )
    CategoricalDistributionChoiceJSON = TypedDict(
        "CategoricalDistributionChoiceJSON",
        {
            "pytype": str,
            "value": str,
        },
    )
    CategoricalDistributionJSON = TypedDict(
        "CategoricalDistributionJSON",
        {
            "type": Literal["CategoricalDistribution"],
            "choices": list[CategoricalDistributionChoiceJSON],
        },
    )
    DistributionJSON = Union[
        FloatDistributionJSON, IntDistributionJSON, CategoricalDistributionJSON
    ]


MAX_ATTR_LENGTH = 1024
CONSTRAINTS_KEY = "constraints"


def serialize_attrs(attrs: dict[str, Any]) -> list[Attribute]:
    serialized: list[Attribute] = []
    for k, v in attrs.items():
        value: str
        if isinstance(v, bytes):
            value = "<binary object>"
        elif isinstance(v, str):
            value = v
        elif isinstance(v, numbers.Real):
            value = str(v)
        else:
            value = json.dumps(v)
            value = value[:MAX_ATTR_LENGTH] if len(value) > MAX_ATTR_LENGTH else value
        serialized.append({"key": k, "value": value})
    return serialized


def serialize_frozen_study(study: FrozenStudy) -> dict[str, Any]:
    serialized = {
        "study_id": study._study_id,
        "study_name": study.study_name,
        "directions": [d.name.lower() for d in study.directions],
        "user_attrs": serialize_attrs(study.user_attrs),
        "is_preferential": study.system_attrs.get(_SYSTEM_ATTR_PREFERENTIAL_STUDY, False),
    }

    return serialized


def serialize_study_detail(
    study: FrozenStudy,
    best_trials: list[FrozenTrial],
    trials: list[FrozenTrial],
    intersection: list[tuple[str, BaseDistribution]],
    union: list[tuple[str, BaseDistribution]],
    union_user_attrs: list[tuple[str, bool]],
    has_intermediate_values: bool,
    plotly_graph_objects: dict[str, str],
    skipped_trial_numbers: list[int],
) -> dict[str, Any]:
    serialized: dict[str, Any] = {
        "name": study.study_name,
        "directions": [d.name.lower() for d in study.directions],
        "user_attrs": serialize_attrs(study.user_attrs),
    }
    system_attrs = study.system_attrs
    serialized["artifacts"] = list_study_artifacts(system_attrs)

    serialized["trials"] = [
        serialize_frozen_trial(study._study_id, trial, system_attrs) for trial in trials
    ]
    serialized["best_trials"] = [
        serialize_frozen_trial(study._study_id, trial, system_attrs) for trial in best_trials
    ]
    serialized["intersection_search_space"] = serialize_search_space(intersection)
    serialized["union_search_space"] = serialize_search_space(union)
    serialized["union_user_attrs"] = [{"key": a[0], "sortable": a[1]} for a in union_user_attrs]
    serialized["has_intermediate_values"] = has_intermediate_values
    serialized["note"] = note.get_note_from_system_attrs(system_attrs, None)
    serialized["is_preferential"] = system_attrs.get(_SYSTEM_ATTR_PREFERENTIAL_STUDY, False)
    objective_names = get_objective_names(system_attrs)
    if objective_names:
        serialized["objective_names"] = objective_names
    form_widgets = get_form_widgets_json(system_attrs)
    if form_widgets:
        serialized["form_widgets"] = form_widgets
    serialized["feedback_component_type"] = system_attrs.get(
        _SYSTEM_ATTR_FEEDBACK_COMPONENT,
        {
            "output_type": "note",
        },
    )
    if serialized["is_preferential"]:
        serialized["preference_history"] = serialize_preference_history(system_attrs)
        serialized["preferences"] = get_preferences(system_attrs)
        serialized["skipped_trial_numbers"] = skipped_trial_numbers
    serialized["plotly_graph_objects"] = [
        {"id": id_, "graph_object": graph_object}
        for id_, graph_object in plotly_graph_objects.items()
    ]
    return serialized


def serialize_preference_history(
    system_attrs: dict[str, Any],
) -> list[SerializedHistory]:
    histories: list[SerializedHistory] = []
    for k, v in system_attrs.items():
        if not k.startswith(_SYSTEM_ATTR_PREFIX_HISTORY):
            continue
        choice: dict[str, Any] = json.loads(v)
        if choice["mode"] == "ChooseWorst":
            history: History = {
                "mode": "ChooseWorst",
                "id": choice["id"],
                "timestamp": choice["timestamp"],
                "candidates": choice["candidates"],
                "clicked": choice["clicked"],
                "preferences": choice["preferences"],
            }
            histories.append(
                {
                    "history": history,
                    "is_removed": is_preference_removed(system_attrs, choice["id"]),
                }
            )

    histories.sort(key=lambda c: datetime.fromisoformat(c["history"]["timestamp"]))
    return histories


def serialize_frozen_trial(
    study_id: int, trial: FrozenTrial, study_system_attrs: dict[str, Any]
) -> dict[str, Any]:
    params = []
    for param_name, param_external_value in trial.params.items():
        distribution = trial.distributions.get(param_name)
        if distribution is None:
            continue
        params.append(
            {
                "name": param_name,
                "param_internal_value": distribution.to_internal_repr(param_external_value),
                "param_external_value": str(param_external_value),
                "param_external_pytyp": str(type(param_external_value)),
                "distribution": serialize_distribution(distribution),
            }
        )
    trial_system_attrs: dict[str, Any] = getattr(trial, "_system_attrs", {})
    fixed_params = trial_system_attrs.get("fixed_params", {})
    serialized = {
        "trial_id": trial._trial_id,
        "study_id": study_id,
        "number": trial.number,
        "state": trial.state.name.capitalize(),
        "params": params,
        "fixed_params": [
            {"name": param_name, "param_external_value": str(fixed_params.get(param_name, None))}
            for param_name in fixed_params
        ],
        "user_attrs": serialize_attrs(trial.user_attrs),
        "note": note.get_note_from_system_attrs(study_system_attrs, trial._trial_id),
        "artifacts": list_trial_artifacts(study_system_attrs, trial_system_attrs, trial),
        "constraints": trial_system_attrs.get(CONSTRAINTS_KEY, []),
    }

    serialized_intermediate_values: list[IntermediateValue] = []
    for step, value in trial.intermediate_values.items():
        serialized_value: Union[float, Literal["nan", "inf", "-inf"]]
        if np.isnan(value):
            serialized_value = "nan"
        elif np.isposinf(value):
            serialized_value = "inf"
        elif np.isneginf(value):
            serialized_value = "-inf"
        else:
            assert np.isfinite(value)
            serialized_value = value
        serialized_intermediate_values.append({"step": step, "value": serialized_value})
    serialized["intermediate_values"] = sorted(
        serialized_intermediate_values, key=lambda v: v["step"]
    )

    if trial.values is not None:
        serialized_values: list[Union[float, Literal["inf", "-inf"]]] = []
        for v in trial.values:
            assert not np.isnan(v), "Should not detect nan value"
            if np.isposinf(v):
                serialized_values.append("inf")
            elif np.isneginf(v):
                serialized_values.append("-inf")
            else:
                serialized_values.append(v)
        serialized["values"] = serialized_values

    if trial.datetime_start is not None:
        serialized["datetime_start"] = trial.datetime_start.isoformat()

    if trial.datetime_complete is not None:
        serialized["datetime_complete"] = trial.datetime_complete.isoformat()

    return serialized


def serialize_distribution(distribution: BaseDistribution) -> DistributionJSON:
    if isinstance(distribution, FloatDistribution):
        float_distribution: FloatDistributionJSON = {
            "type": "FloatDistribution",
            "low": getattr(distribution, "low"),
            "high": getattr(distribution, "high"),
            "step": getattr(distribution, "step"),
            "log": getattr(distribution, "log"),
        }
        return float_distribution
    if isinstance(distribution, IntDistribution):
        int_distribution: IntDistributionJSON = {
            "type": "IntDistribution",
            "low": getattr(distribution, "low"),
            "high": getattr(distribution, "high"),
            "step": getattr(distribution, "step"),
            "log": getattr(distribution, "log"),
        }
        return int_distribution
    if isinstance(distribution, CategoricalDistribution):
        categorical: CategoricalDistributionJSON = {
            "type": "CategoricalDistribution",
            "choices": [
                {"pytype": str(type(choice)), "value": str(choice)}
                for choice in distribution.choices
            ],
        }
        return categorical
    raise ValueError(f"Unexpected distribution {str(distribution)}")


def serialize_search_space(
    search_space: list[tuple[str, BaseDistribution]],
) -> list[dict[str, Any]]:
    serialized = []
    for param_name, distribution in search_space:
        serialized.append(
            {
                "name": param_name,
                "distribution": serialize_distribution(distribution),
            }
        )
    return serialized
