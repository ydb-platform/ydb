from __future__ import annotations

from dataclasses import dataclass
import json
from typing import TYPE_CHECKING
from typing import Union
import warnings

import optuna


if TYPE_CHECKING:
    from typing import Any
    from typing import Literal
    from typing import Optional
    from typing import Sequence
    from typing import TypedDict

    ChoiceWidgetJSON = TypedDict(
        "ChoiceWidgetJSON",
        {
            "type": Literal["choice"],
            "description": Optional[str],
            "choices": list[str],
            "values": list[float],
            "user_attr_key": Optional[str],
        },
    )
    SliderWidgetLabel = TypedDict(
        "SliderWidgetLabel",
        {"value": float, "label": str},
    )
    SliderWidgetJSON = TypedDict(
        "SliderWidgetJSON",
        {
            "type": Literal["slider"],
            "description": Optional[str],
            "min": float,
            "max": float,
            "step": Optional[float],
            "labels": Optional[list[SliderWidgetLabel]],
            "user_attr_key": Optional[str],
        },
    )
    TextInputWidgetJSON = TypedDict(
        "TextInputWidgetJSON",
        {
            "type": Literal["text"],
            "description": Optional[str],
            "user_attr_key": Optional[str],
            "optional": bool,
        },
    )
    UserAttrRefJSON = TypedDict("UserAttrRefJSON", {"type": Literal["user_attr"], "key": str})
    FormWidgetJSON = TypedDict(
        "FormWidgetJSON",
        {
            "output_type": Literal["objective", "user_attr"],
            "widgets": Sequence[
                Union[ChoiceWidgetJSON, SliderWidgetJSON, TextInputWidgetJSON, UserAttrRefJSON]
            ],
        },
    )


@dataclass
class ChoiceWidget:
    """A widget representing a choice with associated values.

    Args:
        choices: A list of strings representing the available choices.
        values: A list of float values associated with each choice.
        description: A description of the widget. Defaults to None.
        user_attr_key: The key used by `register_user_attr_form_widgets`.
            Form output is saved as `trial.user_attrs[user_attr_key]`. Defaults to None.

    Example:
        .. code-block:: python

            from optuna_dashboard import ChoiceWidget


            choice_widget = ChoiceWidget(
                choices=["A", "B", "C"], values=[1.0, 2.0, 3.0], description="Choose one"
            )
    """

    choices: list[str]
    values: list[float]
    description: Optional[str] = None
    user_attr_key: Optional[str] = None

    def to_dict(self) -> ChoiceWidgetJSON:
        """Convert the ChoiceWidget object to a dictionary.

        Returns:
            ChoiceWidgetJSON: A dictionary representing the ChoiceWidget object.
        """
        return {
            "type": "choice",
            "description": self.description,
            "choices": self.choices,
            "values": self.values,
            "user_attr_key": self.user_attr_key,
        }

    @classmethod
    def _from_dict(cls, d: dict[str, Any]) -> ChoiceWidget:
        assert d.get("type") == "choice"
        return cls(
            description=d.get("description"),
            choices=d["choices"],
            values=d["values"],
            user_attr_key=d.get("user_attr_key"),
        )


@dataclass
class SliderWidget:
    """A widget representing a slider for selecting a value within a range.

    Args:
        min: The minimum value of the slider.
        max: The maximum value of the slider.
        step: The step size for the slider. Defaults to None.
        labels: A list of tuples containing value and label for the slider. Defaults to None.
        description: A description for the slider. Defaults to None.
        user_attr_key: The key used by `register_user_attr_form_widgets`.
            Form output is saved as `trial.user_attrs[user_attr_key]`. Defaults to None.

    Example:
        .. code-block:: python

            from optuna_dashboard import SliderWidget


            slide_widget = SliderWidget(min=0, max=10, step=1, description="Example slider")
    """

    min: float
    max: float
    step: Optional[float] = None
    labels: Optional[list[tuple[float, str]]] = None
    description: Optional[str] = None
    user_attr_key: Optional[str] = None

    def to_dict(self) -> SliderWidgetJSON:
        """Convert the SliderWidget instance to a dictionary.

        Returns:
            SliderWidgetJSON: A dictionary representation of the SliderWidget instance.
        """
        labels: Optional[list[SliderWidgetLabel]] = None
        if self.labels is not None:
            labels = [{"value": value, "label": label} for value, label in self.labels]
        return {
            "type": "slider",
            "description": self.description,
            "min": self.min,
            "max": self.max,
            "step": self.step,
            "labels": labels,
            "user_attr_key": self.user_attr_key,
        }

    @classmethod
    def _from_dict(cls, d: dict[str, Any]) -> SliderWidget:
        assert d.get("type") == "slider"
        labels = d.get("labels")
        if labels is not None:
            labels = [(label["value"], label["label"]) for label in labels]
        return cls(
            description=d.get("description"),
            min=d["min"],
            max=d["max"],
            step=d.get("step"),
            labels=labels,
            user_attr_key=d.get("user_attr_key"),
        )


@dataclass
class TextInputWidget:
    """
    A text input widget class that defines a text input field.

    Args:
        description: A description of the text input field.
        user_attr_key: The key used by `register_user_attr_form_widgets`.
            Form output is saved as `trial.user_attrs[user_attr_key]`. Defaults to None.
        optional: If True, an empty string is acceptable.

    Example:
        .. code-block:: python

            from optuna_dashboard import TextInputWidget


            text_input = TextInputWidget(description="Text Input Example")
    """

    description: Optional[str] = None
    user_attr_key: Optional[str] = None
    optional: bool = False

    def to_dict(self) -> TextInputWidgetJSON:
        """
        Converts the TextInputWidget instance to a dictionary representation.

        Returns:
            TextInputWidgetJSON: The dictionary representation of the TextInputWidget instance.
        """
        return {
            "type": "text",
            "description": self.description,
            "user_attr_key": self.user_attr_key,
            "optional": self.optional,
        }

    @classmethod
    def _from_dict(cls, d: dict[str, Any]) -> TextInputWidget:
        assert d.get("type") == "text"
        return cls(
            description=d.get("description"),
            user_attr_key=d.get("user_attr_key"),
            optional=d.get("optional", False),
        )


@dataclass
class ObjectiveUserAttrRef:
    """
    A class representing a reference to a value of `trial.user_attrs`.
    When combined with `register_objective_form_widgets`, users can tell values that are
    registered to `trial.user_attrs` during the human-in-the-loop optimization.

    Args:
        key: The key of `trial.user_attrs` being referenced.

    Example:
        .. code-block:: python

            from optuna_dashboard import ObjectiveUserAttrRef


            user_attr_ref = ObjectiveUserAttrRef(key="key")
    """

    key: str

    def to_dict(self) -> UserAttrRefJSON:
        """
        Converts the ObjectiveUserAttrRef instance to a dictionary representation.

        Returns:
            UserAttrRefJSON: The dictionary representation of the ObjectiveUserAttrRef instance.
        """
        return {
            "type": "user_attr",
            "key": self.key,
        }

    @classmethod
    def _from_dict(cls, d: dict[str, Any]) -> ObjectiveUserAttrRef:
        assert d.get("type") == "user_attr"
        return cls(
            key=d["key"],
        )


ObjectiveFormWidget = Union[ChoiceWidget, SliderWidget, TextInputWidget, ObjectiveUserAttrRef]
# For backward compatibility.
ObjectiveChoiceWidget = ChoiceWidget
ObjectiveSliderWidget = SliderWidget
ObjectiveTextInputWidget = TextInputWidget
FORM_WIDGETS_KEY = "dashboard:form_widgets:v2"


def dict_to_form_widget(d: dict[str, Any]) -> ObjectiveFormWidget:
    """Restore form widget objects from the dictionary.

    Args:
        d: A dictionary object.

    Returns:
        object: an instance of the restored form widget class.
    """
    widget_type = d.get("type", None)
    if widget_type == "choice":
        return ChoiceWidget._from_dict(d)
    elif widget_type == "slider":
        return SliderWidget._from_dict(d)
    elif widget_type == "text":
        return TextInputWidget._from_dict(d)
    elif widget_type == "user_attr":
        return ObjectiveUserAttrRef._from_dict(d)
    raise ValueError("Unexpected widget type")


def register_objective_form_widgets(
    study: optuna.Study, widgets: list[ObjectiveFormWidget]
) -> None:
    """
    Register a list of form widgets to an Optuna study.

    Submitted values to the forms are told as each trial's objective values.

    Args:
        study: The Optuna study object to register the form widgets for.
        widgets: A list of ObjectiveFormWidget objects to be registered in the study.

    Raises:
        ValueError: If the length of study directions is not equal to the length of widgets.
        Warning: If any widget has `user_attr_key` specified, but it will not be used.

    Examples:
        .. code-block:: python

            import optuna
            from optuna_dashboard import ChoiceWidget, SliderWidget
            from optuna_dashboard import register_objective_form_widgets


            study = optuna.create_study()
            register_objective_form_widgets(
                study,
                widgets=[
                    ObjectiveChoiceWidget(
                        choices=["Good 👍", "Bad 👎"],
                        values=[-1, 1],
                        description="Please input your score!",
                    ),
                    ObjectiveSliderWidget(
                        min=1,
                        max=10,
                        step=1,
                        description="Higher is better.",
                    ),
                ],
            )
    """
    if len(study.directions) != len(widgets):
        raise ValueError("The length of actions must be the same with the number of objectives.")
    if any(
        not isinstance(w, ObjectiveUserAttrRef) and w.user_attr_key is not None for w in widgets
    ):
        warnings.warn("`user_attr_key` specified, but it will not be used.")
    if any(isinstance(w, TextInputWidget) and w.optional for w in widgets):
        raise ValueError("TextInputWidget.optional must be False.")
    form_widgets: FormWidgetJSON = {
        "output_type": "objective",
        "widgets": [w.to_dict() for w in widgets],
    }
    study._storage.set_study_system_attr(
        study._study_id,
        FORM_WIDGETS_KEY,
        # TODO(c-bata): Remove type: ignore comment
        form_widgets,  # type: ignore
    )


def register_user_attr_form_widgets(
    study: optuna.Study, widgets: list[ObjectiveFormWidget]
) -> None:
    """
    Register a list of form widgets to an Optuna study.

    Submitted values to the forms are registered as each trial's user_attrs.

    Args:
        study: The Optuna study object to register the form widgets for.
        widgets: A list of ObjectiveFormWidget objects to be registered in the study.

    Raises:
        ValueError: If an ObjectiveUserAttrRef is specified or if `user_attr_key` is not specified.
        ValueError: If `user_attr_key` is not unique for each widget.

    Examples:
        .. code-block:: python

            import optuna
            from optuna_dashboard import ChoiceWidget, SliderWidget
            from optuna_dashboard import register_user_attr_form_widgets


            study = optuna.create_study()
            register_user_attr_form_widgets(
                study,
                widgets=[
                    ChoiceWidget(
                        choices=["Good 👍", "Bad 👎"],
                        values=[-1, 1],
                        description="Please input your score!",
                        user_attr_key="hitl/choice",
                    ),
                    SliderWidget(
                        min=1,
                        max=10,
                        step=1,
                        description="Higher is better.",
                        user_attr_key="hitl/slider",
                    ),
                ],
            )
    """
    user_attr_keys = set()
    widget_dicts: list[Union[ChoiceWidgetJSON, SliderWidgetJSON, TextInputWidgetJSON]] = []
    for w in widgets:
        if isinstance(w, ObjectiveUserAttrRef):
            raise ValueError("ObjectiveUserAttrRef can't be specified.")
        if w.user_attr_key is None:
            raise ValueError("`user_attr_key` is not specified.")
        user_attr_keys.add(w.user_attr_key)
        widget_dicts.append(w.to_dict())

    if len(widget_dicts) != len(user_attr_keys):
        raise ValueError("`user_attr_key` must be unique for each widget.")

    form_widgets: FormWidgetJSON = {
        "output_type": "user_attr",
        "widgets": widget_dicts,
    }
    study._storage.set_study_system_attr(
        study._study_id,
        FORM_WIDGETS_KEY,
        # TODO(c-bata): Remove type: ignore comment
        form_widgets,  # type: ignore
    )


def get_form_widgets_json(study_system_attr: dict[str, Any]) -> Optional[FormWidgetJSON]:
    if FORM_WIDGETS_KEY in study_system_attr:
        return study_system_attr[FORM_WIDGETS_KEY]

    # For optuna-dashboard v0.9.0 and v0.9.0b6 users
    if "dashboard:objective_form_widgets:v1" in study_system_attr:
        return {
            "output_type": "objective",
            "widgets": study_system_attr["dashboard:objective_form_widgets:v1"],
        }

    # For optuna-dashboard v0.9.0b5 users
    if "dashboard:objective_form_widgets" in study_system_attr:
        return {
            "output_type": "objective",
            "widgets": json.loads(study_system_attr["dashboard:objective_form_widgets"]),
        }
    return None
