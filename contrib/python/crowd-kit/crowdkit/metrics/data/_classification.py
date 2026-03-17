__all__ = [
    "consistency",
    "uncertainty",
    "alpha_krippendorff",
]

from typing import Any, Callable, Hashable, List, Optional, Union, cast

import numpy as np
import pandas as pd
from nltk.metrics.agreement import AnnotationTask
from nltk.metrics.distance import binary_distance
from scipy.stats import entropy

from crowdkit.aggregation import MajorityVote
from crowdkit.aggregation.base import BaseClassificationAggregator


def _check_answers(answers: pd.DataFrame) -> None:
    if not isinstance(answers, pd.DataFrame):
        raise TypeError("Working only with pandas DataFrame")
    assert "task" in answers, 'There is no "task" column in answers'
    assert "worker" in answers, 'There is no "worker" column in answers'
    assert "label" in answers, 'There is no "label" column in answers'


def _label_probability(row: "pd.Series[Any]", label: Any, n_labels: int) -> float:
    """Numerator in the Bayes formula"""
    if row["label"] == label:
        return float(row["skill"])
    else:
        return (1.0 - float(row["skill"])) / (n_labels - 1)


def _task_consistency(row: "pd.Series[Any]") -> float:
    """Posterior probability for a single task"""
    if row["denominator"] != 0:
        return float(row[row["aggregated_label"]]) / float(row["denominator"])
    else:
        return 0.0


def consistency(
    answers: pd.DataFrame,
    workers_skills: Optional["pd.Series[Any]"] = None,
    aggregator: BaseClassificationAggregator = MajorityVote(),
    by_task: bool = False,
) -> Union[float, "pd.Series[Any]"]:
    """
    Consistency metric: posterior probability of aggregated label given workers skills
    calculated using the specified aggregator.

    Args:
        answers (pandas.DataFrame): A data frame containing `task`, `worker` and `label` columns.
        workers_skills (Optional[pandas.Series]): workers skills e.g. golden set skills. If not provided,
            uses aggregator's `workers_skills` attribute.
        aggregator (aggregation.base.BaseClassificationAggregator): aggregation method, default: MajorityVote
        by_task (bool): if set, returns consistencies for every task in provided data frame.

    Returns:
        Union[float, pd.Series]
    """
    _check_answers(answers)
    aggregated = aggregator.fit_predict(answers)
    if workers_skills is None:
        if hasattr(aggregator, "skills_"):
            workers_skills = aggregator.skills_
        else:
            raise AssertionError(
                "This aggregator is not supported. Please, provide workers skills."
            )

    answers = answers.copy(deep=False)
    answers.set_index("task", inplace=True)
    answers = answers.reset_index().set_index("worker")
    answers["skill"] = workers_skills
    answers.reset_index(inplace=True)

    labels = pd.unique(answers.label)
    for label in labels:
        answers[label] = answers.apply(
            lambda row: _label_probability(row, label, len(labels)), axis=1
        )

    labels_proba = answers.groupby("task").prod(numeric_only=True)
    labels_proba["aggregated_label"] = aggregated
    labels_proba["denominator"] = labels_proba[list(labels)].sum(axis=1)

    consistencies = labels_proba.apply(_task_consistency, axis=1)

    if by_task:
        return consistencies
    else:
        return consistencies.mean()


def _task_uncertainty(row: "pd.Series[Any]", labels: List[str]) -> float:
    if row["denominator"] == 0:
        row[labels] = 1 / len(labels)
    else:
        row[labels] /= row["denominator"]
    softmax = row[labels]
    log_softmax = np.log(row[list(labels)])
    return float(-np.sum(softmax * log_softmax))


def uncertainty(
    answers: pd.DataFrame,
    workers_skills: Optional["pd.Series[Any]"] = None,
    aggregator: Optional[BaseClassificationAggregator] = None,
    compute_by: str = "task",
    aggregate: bool = True,
) -> Union[float, "pd.Series[Any]"]:
    r"""Label uncertainty metric: entropy of labels probability distribution.
    Computed as Shannon's Entropy with label probabilities computed either for tasks or workers:
    $H(L) = -\sum_{label_i \in L} p(label_i) \cdot \log(p(label_i))$.

    Args:
        answers: A data frame containing `task`, `worker` and `label` columns.
        workers_skills: workers skills e.g. golden set skills. If not provided,
            but aggregator provided, uses aggregator's `workers_skills` attribute.
            Otherwise assumes equal skills for workers.
        aggregator: aggregation method to obtain
            worker skills if not provided.
        compute_by: what to compute uncertainty for. If 'task', compute uncertainty of answers per task.
            If 'worker', compute uncertainty for each worker.
        aggregate: If true, return the mean uncertainty, otherwise return uncertainties for each task or worker.

    Returns:
        Union[float, pd.Series]

    Examples:
        Mean task uncertainty minimal, as all answers to task are same.

        >>> uncertainty(pd.DataFrame.from_records([
        >>>     {'task': 'X', 'worker': 'A', 'label': 'Yes'},
        >>>     {'task': 'X', 'worker': 'B', 'label': 'Yes'},
        >>> ]))
        0.0

        Mean task uncertainty maximal, as all answers to task are different.

        >>> uncertainty(pd.DataFrame.from_records([
        >>>     {'task': 'X', 'worker': 'A', 'label': 'Yes'},
        >>>     {'task': 'X', 'worker': 'B', 'label': 'No'},
        >>>     {'task': 'X', 'worker': 'C', 'label': 'Maybe'},
        >>> ]))
        1.0986122886681096

        Uncertainty by task without averaging.

        >>> uncertainty(pd.DataFrame.from_records([
        >>>     {'task': 'X', 'worker': 'A', 'label': 'Yes'},
        >>>     {'task': 'X', 'worker': 'B', 'label': 'No'},
        >>>     {'task': 'Y', 'worker': 'A', 'label': 'Yes'},
        >>>     {'task': 'Y', 'worker': 'B', 'label': 'Yes'},
        >>> ]),
        >>> workers_skills=pd.Series([1, 1], index=['A', 'B']),
        >>> compute_by="task", aggregate=False)
        task
        X    0.693147
        Y    0.000000
        dtype: float64

        Uncertainty by worker

        >>> uncertainty(pd.DataFrame.from_records([
        >>>     {'task': 'X', 'worker': 'A', 'label': 'Yes'},
        >>>     {'task': 'X', 'worker': 'B', 'label': 'No'},
        >>>     {'task': 'Y', 'worker': 'A', 'label': 'Yes'},
        >>>     {'task': 'Y', 'worker': 'B', 'label': 'Yes'},
        >>> ]),
        >>> workers_skills=pd.Series([1, 1], index=['A', 'B']),
        >>> compute_by="worker", aggregate=False)
        worker
        A    0.000000
        B    0.693147
        dtype: float64

    Args:
        answers (DataFrame): Workers' labeling results.
            A pandas.DataFrame containing `task`, `worker` and `label` columns.
        workers_skills (typing.Optional[pandas.core.series.Series]): workers' skills.
            A pandas.Series index by workers and holding corresponding worker's skill
    """
    _check_answers(answers)

    if workers_skills is None and aggregator is not None:
        aggregator.fit(answers)
        if hasattr(aggregator, "skills_"):
            workers_skills = aggregator.skills_
        else:
            raise AssertionError(
                "This aggregator is not supported. Please, provide workers skills."
            )

    answers = answers.copy(deep=False)
    answers = answers.set_index("worker")
    answers["skill"] = workers_skills if workers_skills is not None else 1
    if answers["skill"].isnull().any():
        missing_workers = set(answers[answers.skill.isnull()].index.tolist())
        raise AssertionError(
            f"Did not provide skills for workers: {missing_workers}."
            f"Please provide workers skills."
        )
    answers.reset_index(inplace=True)
    labels = pd.unique(answers.label)
    for label in labels:
        answers[label] = answers.apply(
            lambda row: _label_probability(row, label, len(labels)), axis=1
        )

    labels_proba = answers.groupby(compute_by).sum(numeric_only=True)
    uncertainties = labels_proba.apply(
        lambda row: entropy(row[labels] / (sum(row[labels]) + 1e-6)), axis=1
    )

    if aggregate:
        return cast(float, uncertainties.mean())

    return cast("pd.Series[Any]", uncertainties)


def alpha_krippendorff(
    answers: pd.DataFrame,
    distance: Callable[[Hashable, Hashable], float] = binary_distance,
) -> float:
    """Inter-annotator agreement coefficient (Krippendorff 1980).

    Amount that annotators agreed on label assignments beyond what is expected by chance.
    The value of alpha should be interpreted as follows.
        alpha >= 0.8 indicates a reliable annotation,
        alpha >= 0.667 allows making tentative conclusions only,
        while the lower values suggest the unreliable annotation.

    Args:
        answers: A data frame containing `task`, `worker` and `label` columns.
        distance: Distance metric, that takes two arguments,
            and returns a value between 0.0 and 1.0
            By default: binary_distance (0.0 for equal labels 1.0 otherwise).

    Returns:
        Float value.

    Examples:
        Consistent answers.

        >>> alpha_krippendorff(pd.DataFrame.from_records([
        >>>     {'task': 'X', 'worker': 'A', 'label': 'Yes'},
        >>>     {'task': 'X', 'worker': 'B', 'label': 'Yes'},
        >>>     {'task': 'Y', 'worker': 'A', 'label': 'No'},
        >>>     {'task': 'Y', 'worker': 'B', 'label': 'No'},
        >>> ]))
        1.0

        Partially inconsistent answers.

        >>> alpha_krippendorff(pd.DataFrame.from_records([
        >>>     {'task': 'X', 'worker': 'A', 'label': 'Yes'},
        >>>     {'task': 'X', 'worker': 'B', 'label': 'Yes'},
        >>>     {'task': 'Y', 'worker': 'A', 'label': 'No'},
        >>>     {'task': 'Y', 'worker': 'B', 'label': 'No'},
        >>>     {'task': 'Z', 'worker': 'A', 'label': 'Yes'},
        >>>     {'task': 'Z', 'worker': 'B', 'label': 'No'},
        >>> ]))
        0.4444444444444444
    """
    _check_answers(answers)
    data = answers[["worker", "task", "label"]].values.tolist()
    return float(AnnotationTask(data, distance).alpha())
