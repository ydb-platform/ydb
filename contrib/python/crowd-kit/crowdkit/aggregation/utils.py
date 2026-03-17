"""
Helper routines for aggregation.
"""

__all__ = [
    "evaluate_in",
    "evaluate_equal",
    "evaluate",
    "factorize",
    "get_most_probable_labels",
    "normalize_rows",
    "manage_data",
    "get_accuracy",
    "add_skills_to_data",
    "named_series_attrib",
    "clone_aggregator",
]

from typing import Any, Callable, Optional, Tuple, Union

import attr
import numpy as np
import numpy.typing as npt
import pandas as pd

from . import base


def clone_aggregator(
    aggregator: "base.BaseClassificationAggregator",
) -> "base.BaseClassificationAggregator":
    """Construct a new unfitted aggregator with the same parameters.
    Args:
        aggregator (BaseClassificationAggregator): aggregator instance to be cloned

    Returns:
        BaseClassificationAggregator: cloned aggregator's instance. Its params are same to input,
            except for the results of previous fit (private attributes).
    """
    assert isinstance(
        aggregator, base.BaseClassificationAggregator
    ), "Can't clone object that is not inherit BaseClassificationAggregator"
    aggregator_class = aggregator.__class__
    new_object_params = dict()
    for attr_name in aggregator.__dict__:
        # if attribute is not private
        if not (attr_name.startswith("_") or attr_name.endswith("_")):
            new_object_params[attr_name] = getattr(aggregator, attr_name)
    new_object = aggregator_class(**new_object_params)
    return new_object


def _argmax_random_ties(array: npt.NDArray[Any]) -> int:
    """Returns the index of the maximum element. If there are several such elements, it returns a random one."""
    return int(np.random.choice(np.flatnonzero(array == array.max())))


def evaluate_in(row: "pd.Series[Any]") -> int:
    return int(row["label_pred"] in row["label_true"])


def evaluate_equal(row: "pd.Series[Any]") -> int:
    return int(row["label_pred"] == row["label_true"])


def evaluate(
    df_true: pd.DataFrame,
    df_pred: pd.DataFrame,
    evaluate_func: Callable[["pd.Series[Any]"], int] = evaluate_in,
) -> Union[str, float]:
    df = df_true.merge(df_pred, on="task", suffixes=("_true", "_pred"))

    assert len(df_true) == len(
        df
    ), f"Dataset length mismatch, expected {len(df_true)}, got {len(df)}"

    df["evaluation"] = df.apply(evaluate_func, axis=1)
    return float(df["evaluation"].mean())


def factorize(data: npt.NDArray[Any]) -> Tuple[npt.NDArray[Any], npt.NDArray[Any]]:
    unique_values, coded = np.unique(data, return_inverse=True)
    return unique_values, coded.reshape(data.shape)


def get_most_probable_labels(proba: pd.DataFrame) -> "pd.Series[Any]":
    """Returns most probable labels

    Args:
        proba (DataFrame): Tasks' label probability distributions.
            A pandas.DataFrame indexed by `task` such that `result.loc[task, label]`
            is the probability of `task`'s true label to be equal to `label`. Each
            probability is between 0 and 1, all task's probabilities should sum up to 1
    """
    # patch for pandas<=1.1.5
    if not proba.size:
        return pd.Series([], dtype="O")
    return proba.idxmax(axis="columns")


def normalize_rows(scores: pd.DataFrame) -> pd.DataFrame:
    """Scales values so that every raw sums to 1

    Args:
        scores (DataFrame): Tasks' label scores.
            A pandas.DataFrame indexed by `task` such that `result.loc[task, label]`
            is the score of `label` for `task`.

    Returns:
        DataFrame: Tasks' label probability distributions.
            A pandas.DataFrame indexed by `task` such that `result.loc[task, label]`
            is the probability of `task`'s true label to be equal to `label`. Each
            probability is between 0 and 1, all task's probabilities should sum up to 1
    """
    return scores.div(scores.sum(axis=1), axis=0)


def manage_data(
    data: pd.DataFrame,
    weights: Optional["pd.Series[Any]"] = None,
    skills: Optional["pd.Series[Any]"] = None,
) -> pd.DataFrame:
    """
    Args:
        data (DataFrame): Workers' labeling results.
            A pandas.DataFrame containing `task`, `worker` and `label` columns.
        skills (Series): workers' skills.
            A pandas.Series index by workers and holding corresponding worker's skill
    """
    data = data[["task", "worker", "label"]]

    if weights is None:
        data["weight"] = 1
    else:
        data = data.join(weights.rename("weight"), on="task")

    if skills is None:
        data["skill"] = 1
    else:
        data = data.join(skills.rename("skill"), on="task")

    return data


def get_accuracy(
    data: pd.DataFrame, true_labels: "pd.Series[Any]", by: Optional[str] = None
) -> "pd.Series[Any]":
    """
    Args:
        data (DataFrame): Workers' labeling results.
            A pandas.DataFrame containing `task`, `worker` and `label` columns.
        true_labels (Series): Tasks' ground truth labels.
            A pandas.Series indexed by `task` such that `labels.loc[task]`
            is the tasks's ground truth label.

    Returns:
        Series: workers' skills.
            A pandas.Series index by workers and holding corresponding worker's skill
    """
    if "weight" in data.columns:
        data = data[["task", "worker", "label", "weight"]]
    else:
        data = data[["task", "worker", "label"]]

    if data.empty:
        data["true_label"] = []
    else:
        data = data.join(pd.Series(true_labels, name="true_label"), on="task")

    data = data[data.true_label.notna()]

    if "weight" not in data.columns:
        data["weight"] = 1
    data.eval("score = weight * (label == true_label)", inplace=True)

    data = data.sort_values("score").drop_duplicates(
        ["task", "worker", "label"], keep="last"
    )

    if by is not None:
        group = data.groupby(by)
        return group.score.sum() / group.weight.sum()
    else:
        return data.score.sum() / data.weight.sum()  # type: ignore


def named_series_attrib(name: str) -> "pd.Series[Any]":
    """Attrs attribute with converter and setter which preserves specified attribute name"""

    def converter(series: "pd.Series[Any]") -> "pd.Series[Any]":
        series.name = name
        return series

    return attr.ib(init=False, converter=converter, on_setattr=attr.setters.convert)


def add_skills_to_data(
    data: pd.DataFrame,
    skills: "pd.Series[Any]",
    on_missing_skill: str,
    default_skill: Optional[float],
) -> pd.DataFrame:
    """
    Args:
        skills (Series): workers' skills.
            A pandas.Series index by workers and holding corresponding worker's skill
        on_missing_skill (str): How to handle assignments done by workers with unknown skill.
            Possible values:
                    * "error" — raise an exception if there is at least one assignment done by user with unknown skill;
                    * "ignore" — drop assignments with unknown skill values during prediction. Raise an exception if there is no
                    assignments with known skill for any task;
                    * value — default value will be used if skill is missing.
    """
    data = data.join(skills.rename("skill"), on="worker")

    if on_missing_skill != "value" and default_skill is not None:
        raise ValueError('default_skill is used but on_missing_skill is not "value"')

    if on_missing_skill == "error":
        missing_skills_count = data["skill"].isna().sum()
        if missing_skills_count > 0:
            raise ValueError(
                f"Skill value is missing in {missing_skills_count} assignments. Specify skills for every"
                f"used worker or use different 'on_unknown_skill' value."
            )
    elif on_missing_skill == "ignore":
        data.set_index("task", inplace=True)
        index_before_drop = data.index
        data.dropna(inplace=True)
        dropped_tasks_count = len(index_before_drop.difference(data.index))
        if dropped_tasks_count > 0:
            raise ValueError(
                f"{dropped_tasks_count} tasks has no workers with known skills. Provide at least one worker with known"
                f"skill for every task or use different 'on_unknown_skill' value."
            )
        data.reset_index(inplace=True)
    elif on_missing_skill == "value":
        if default_skill is None:
            raise ValueError(
                'Default skill value must be specified when using on_missing_skill="value"'
            )
        data.loc[data["skill"].isna(), "skill"] = default_skill
    else:
        raise ValueError(
            f'Unknown option {on_missing_skill!r} of "on_missing_skill" argument.'
        )
    return data
