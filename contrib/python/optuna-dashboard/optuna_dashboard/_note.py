from __future__ import annotations

import math
from typing import TYPE_CHECKING

import optuna
from optuna.storages import BaseStorage


if TYPE_CHECKING:
    from typing import Any
    from typing import Optional
    from typing import TypedDict

    NoteType = TypedDict(
        "NoteType",
        {
            "version": int,
            "body": str,
        },
    )

SYSTEM_ATTR_MAX_LENGTH = 2045


def save_note(study_or_trial: optuna.Study | optuna.Trial, body: str) -> None:
    """Save the note (Markdown format) to the Study or Trial.

    Example:

       .. code-block:: python

          import optuna
          from optuna_dashboard import save_note


          def objective(trial: optuna.Trial) -> float:
              x1 = trial.suggest_float("x1", 0, 10)

              save_note(trial, textwrap.dedent(f'''\
              ## Trial {trial.number}

              You can *freely* take a **note** that is associated with the Trial.
              '''))
              return (x1 - 2) ** 2


          study = optuna.create_study()
          save_note(study, textwrap.dedent(f'''\
          ## {study.study_name}

          You can *freely* take a **note** that is associated with the study.
          '''))
          study.optimize(objective, n_trials=10)
    """
    trial_id: Optional[int] = None
    if isinstance(study_or_trial, optuna.Study):
        storage = study_or_trial._storage
        study_id = study_or_trial._study_id
    else:
        storage = study_or_trial.storage
        study_id = study_or_trial.study._study_id
        trial_id = study_or_trial._trial_id

    system_attrs = storage.get_study_system_attrs(study_id)
    next_ver = system_attrs.get(note_ver_key(trial_id), 0) + 1
    save_note_with_version(storage, study_id, trial_id, next_ver, body)


def get_note(study_or_trial: optuna.Study | optuna.Trial) -> str:
    """Get the note (Markdown format) from the Study or Trial.

    Example:

       .. code-block:: python

          import optuna
          from optuna_dashboard import save_note, get_note

          study = optuna.create_study()
          save_note(study, "**Hello** World")

          text = get_note(study)
          print(text)  # '**Hello** World'
    """
    trial_id: Optional[int] = None
    if isinstance(study_or_trial, optuna.Study):
        storage = study_or_trial._storage
        study_id = study_or_trial._study_id
    else:
        storage = study_or_trial.storage
        study_id = study_or_trial.study._study_id
        trial_id = study_or_trial._trial_id
    system_attrs = storage.get_study_system_attrs(study_id)
    note = get_note_from_system_attrs(system_attrs, trial_id)
    return note["body"]


def note_ver_key(trial_id: Optional[int]) -> str:
    prefix = "dashboard:note_ver"
    if trial_id is None:
        return prefix
    return f"dashboard:{trial_id}:note_ver"


def note_str_key_prefix(trial_id: Optional[int]) -> str:
    prefix = "dashboard:note_str:"
    if trial_id is None:
        return prefix
    return f"dashboard:{trial_id}:note_str:"


def copy_notes(storage: BaseStorage, src_study: optuna.Study, dst_study: optuna.Study) -> None:
    system_attrs = storage.get_study_system_attrs(study_id=src_study._study_id)

    # Copy individual trial notes
    for src_trial, dst_trial in zip(src_study.get_trials(), dst_study.get_trials()):
        note = get_note_from_system_attrs(system_attrs, src_trial._trial_id)["body"]
        save_note_with_version(storage, dst_study._study_id, dst_trial._trial_id, 0, note)

    # Copy study note
    note = get_note_from_system_attrs(system_attrs, None)["body"]
    save_note_with_version(storage, dst_study._study_id, None, 0, note)


def get_note_from_system_attrs(system_attrs: dict[str, Any], trial_id: Optional[int]) -> NoteType:
    if note_ver_key(trial_id) not in system_attrs:
        return {
            "version": 0,
            "body": "",
        }
    note_ver = int(system_attrs[note_ver_key(trial_id)])
    note_attrs: dict[str, str] = {
        key: value
        for key, value in system_attrs.items()
        if key.startswith(note_str_key_prefix(trial_id))
    }
    return {"version": note_ver, "body": concat_body(note_attrs, trial_id)}


def version_is_incremented(
    system_attrs: dict[str, Any], trial_id: Optional[int], req_note_ver: int
) -> bool:
    db_note_ver = system_attrs.get(note_ver_key(trial_id), 0)
    return req_note_ver == db_note_ver + 1


def save_note_with_version(
    storage: BaseStorage, study_id: int, trial_id: Optional[int], ver: int, body: str
) -> None:
    storage.set_study_system_attr(study_id, note_ver_key(trial_id), ver)

    attrs = split_body(body, trial_id)
    for k, v in attrs.items():
        storage.set_study_system_attr(study_id, k, v)

    # Clear previous messages
    all_note_attrs: dict[str, str] = {
        key: value
        for key, value in storage.get_study_system_attrs(study_id).items()
        if key.startswith(note_str_key_prefix(trial_id))
    }
    if len(all_note_attrs) > len(attrs):
        for i in range(len(attrs), len(all_note_attrs)):
            storage.set_study_system_attr(study_id, f"{note_str_key_prefix(trial_id)}{i}", "")


def split_body(note_str: str, trial_id: Optional[int]) -> dict[str, str]:
    note_len = len(note_str)
    attrs = {}
    for i in range(math.ceil(note_len / SYSTEM_ATTR_MAX_LENGTH)):
        start = i * SYSTEM_ATTR_MAX_LENGTH
        end = min((i + 1) * SYSTEM_ATTR_MAX_LENGTH, note_len)
        attrs[f"{note_str_key_prefix(trial_id)}{i}"] = note_str[start:end]
    return attrs


def concat_body(note_attrs: dict[str, str], trial_id: Optional[int]) -> str:
    return "".join(
        note_attrs[f"{note_str_key_prefix(trial_id)}{i}"] for i in range(len(note_attrs))
    )
