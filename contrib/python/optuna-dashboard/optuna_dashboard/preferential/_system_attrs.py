from __future__ import annotations

from typing import Any
import uuid

from optuna.storages import BaseStorage
from optuna.trial import TrialState


_SYSTEM_ATTR_PREFIX_PREFERENCE = "preference:values"
_SYSTEM_ATTR_PREFIX_SKIP_TRIAL = "preference:skip_trial:"
_SYSTEM_ATTR_N_GENERATE = "preference:n_generate"


def report_preferences(
    study_id: int,
    storage: BaseStorage,
    preferences: list[tuple[int, int]],
) -> str:
    preference_id = str(uuid.uuid4())
    key = _SYSTEM_ATTR_PREFIX_PREFERENCE + preference_id
    storage.set_study_system_attr(
        study_id=study_id,
        key=key,
        value=preferences,
    )
    trials = storage.get_all_trials(study_id, deepcopy=False)
    directions = storage.get_study_directions(study_id)
    values = [0 for _ in directions]
    updated_trials = {num for tpl in preferences for num in tpl}
    for number in updated_trials:
        trial_id = trials[number]._trial_id
        if trials[number].state != TrialState.COMPLETE:
            storage.set_trial_state_values(trial_id, TrialState.COMPLETE, values)
    return preference_id


def get_preferences(study_system_attrs: dict[str, Any]) -> list[tuple[int, int]]:
    preferences: list[tuple[int, int]] = []
    for k, v in study_system_attrs.items():
        if not k.startswith(_SYSTEM_ATTR_PREFIX_PREFERENCE):
            continue
        preferences.extend(v)  # type: ignore
    return preferences


def is_preference_removed(study_system_attrs: dict[str, Any], preference_id: str) -> bool:
    key = _SYSTEM_ATTR_PREFIX_PREFERENCE + preference_id
    preference = study_system_attrs.get(key, [])
    return len(preference) == 0


def report_skip(
    study_id: int,
    trial_id: int,
    storage: BaseStorage,
) -> None:
    storage.set_study_system_attr(
        study_id=study_id,
        key=_SYSTEM_ATTR_PREFIX_SKIP_TRIAL + str(trial_id),
        value=True,
    )


def is_skipped_trial(trial_id: int, study_system_attrs: dict[str, Any]) -> bool:
    key = _SYSTEM_ATTR_PREFIX_SKIP_TRIAL + str(trial_id)
    return key in study_system_attrs


def get_skipped_trial_ids(study_system_attrs: dict[str, Any]) -> list[int]:
    skipped_trial_ids: list[int] = []
    for k in study_system_attrs:
        if not k.startswith(_SYSTEM_ATTR_PREFIX_SKIP_TRIAL):
            continue
        try:
            trial_id = int(k[len(_SYSTEM_ATTR_PREFIX_SKIP_TRIAL) :])  # noqa: E203
            skipped_trial_ids.append(trial_id)
        except ValueError:
            continue
    return skipped_trial_ids


def get_n_generate(study_system_attrs: dict[str, Any]) -> int:
    return study_system_attrs[_SYSTEM_ATTR_N_GENERATE]


def set_n_generate(study_id: int, storage: BaseStorage, n_generate: int) -> None:
    storage.set_study_system_attr(
        study_id=study_id,
        key=_SYSTEM_ATTR_N_GENERATE,
        value=n_generate,
    )
