from __future__ import annotations

from typing import Any
from typing import TYPE_CHECKING

from optuna.storages import BaseStorage

from .preferential._study import PreferentialStudy


if TYPE_CHECKING:
    from typing import Literal

    OUTPUT_COMPONENT_TYPE = Literal["note", "artifact"]

_SYSTEM_ATTR_FEEDBACK_COMPONENT = "preference:component"


def _register_preference_feedback_component(
    study_id: int,
    storage: BaseStorage,
    component_type: OUTPUT_COMPONENT_TYPE,
    artifact_key: str | None = None,
) -> None:
    value: dict[str, Any] = {"output_type": component_type}
    if artifact_key is not None:
        value["artifact_key"] = artifact_key
    storage.set_study_system_attr(
        study_id=study_id,
        key=_SYSTEM_ATTR_FEEDBACK_COMPONENT,
        value=value,
    )


def register_preference_feedback_component(
    study: PreferentialStudy,
    component_type: OUTPUT_COMPONENT_TYPE,
    artifact_key: str | None = None,
) -> None:
    """Register a preference feedback component to the study.

    With this feature, you can change the component, displayed on the
    human feedback pages. By default, the Markdown note (``component_type="note"``)
    is displayed.  If you specify ``component_type="artifact"``, the viewer for the
    specified artifact file will be displayed.

    Args:
        study:
            The study to register the preference feedback component.
        component_type:
            The component type, displayed on the human feedback pages
            (default: ``"note"``).
        user_attr_artifact_key:
            This option is required when the ``component_type`` is ``"artifact"``.
            The user attribute, which is specified this field, must contain the
            ``artifact``id you want to display on the human feedback page.
    """
    if component_type == "artifact":
        assert artifact_key is not None, (
            "artifact_key must be specified when component_type is Artifact"
        )

    _register_preference_feedback_component(
        study_id=study._study._study_id,
        storage=study._study._storage,
        component_type=component_type,
        artifact_key=artifact_key,
    )
