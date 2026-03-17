from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import json
from typing import TYPE_CHECKING

from optuna.storages import BaseStorage

from .preferential._system_attrs import _SYSTEM_ATTR_PREFIX_PREFERENCE
from .preferential._system_attrs import report_preferences


_SYSTEM_ATTR_PREFIX_HISTORY = "preference:history"

if TYPE_CHECKING:
    from typing import Literal
    from typing import TypedDict

    FeedbackMode = Literal["ChooseWorst"]
    ChooseWorstHistory = TypedDict(
        "ChooseWorstHistory",
        {
            "mode": FeedbackMode,
            "id": str,
            "timestamp": str,
            "candidates": list[int],
            "clicked": int,
            "preferences": list[tuple[int, int]],
        },
    )
    History = ChooseWorstHistory
    SerializedHistory = TypedDict(
        "SerializedHistory",
        {
            "history": History,
            "is_removed": bool,
        },
    )


class PreferenceHistoryNotFound(Exception):
    pass


@dataclass
class NewHistory:
    mode: FeedbackMode
    candidates: list[int]
    clicked: int


def report_history(
    study_id: int,
    storage: BaseStorage,
    input_data: NewHistory,
) -> str:
    preferences = []
    # TODO(moririn): Use TypeGuard after adding other history types.
    if input_data.mode == "ChooseWorst":
        preferences = [
            (better, input_data.clicked)
            for better in input_data.candidates
            if better != input_data.clicked
        ]
    else:
        assert False, f"Unknown data: {input_data}"

    preference_id = report_preferences(
        study_id=study_id,
        storage=storage,
        preferences=preferences,
    )

    if input_data.mode == "ChooseWorst":
        history: ChooseWorstHistory = {
            "mode": "ChooseWorst",
            "id": preference_id,
            "timestamp": datetime.now().isoformat(),
            "candidates": input_data.candidates,
            "clicked": input_data.clicked,
            "preferences": preferences,
        }

    key = _SYSTEM_ATTR_PREFIX_HISTORY + preference_id
    storage.set_study_system_attr(
        study_id=study_id,
        key=key,
        value=json.dumps(history),
    )
    return preference_id


def remove_history(study_id: int, storage: BaseStorage, history_id: str) -> None:
    system_attrs = storage.get_study_system_attrs(study_id)
    history_key = _SYSTEM_ATTR_PREFIX_HISTORY + history_id
    if history_key not in system_attrs:
        raise PreferenceHistoryNotFound
    storage.set_study_system_attr(study_id, _SYSTEM_ATTR_PREFIX_PREFERENCE + history_id, [])


def restore_history(study_id: int, storage: BaseStorage, history_id: str) -> None:
    system_attrs = storage.get_study_system_attrs(study_id)
    history_key = _SYSTEM_ATTR_PREFIX_HISTORY + history_id
    if history_key not in system_attrs:
        raise PreferenceHistoryNotFound
    history: History = json.loads(system_attrs.get(history_key, ""))
    storage.set_study_system_attr(
        study_id, _SYSTEM_ATTR_PREFIX_PREFERENCE + history_id, history["preferences"]
    )
