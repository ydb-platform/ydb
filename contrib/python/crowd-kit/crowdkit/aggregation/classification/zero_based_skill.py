__all__ = ["ZeroBasedSkill"]

from typing import Any, Optional

import attr
import pandas as pd
from sklearn.utils.validation import check_is_fitted

from ..base import BaseClassificationAggregator
from ..utils import get_accuracy, named_series_attrib
from .majority_vote import MajorityVote


@attr.attrs(auto_attribs=True)
class ZeroBasedSkill(BaseClassificationAggregator):
    r"""The **Zero-Based Skill** (ZBS) aggregation model performs weighted majority voting on tasks. After processing a pool of tasks,
    it re-estimates the workers' skills with a gradient descend step to optimize
    the mean squared error of the current skills and the fraction of responses that
    are equal to the aggregated labels.

    This process is repeated until the labels change or exceed the number of iterations.

    Note:
        It is necessary that all workers in the dataset that is sent to `predict` exist in responses to
        the dataset that was sent to `fit`.

    Examples:
        >>> from crowdkit.aggregation import ZeroBasedSkill
        >>> from crowdkit.datasets import load_dataset
        >>> df, gt = load_dataset('relevance-2')
        >>> result = ZeroBasedSkill().fit_predict(df)
    """

    n_iter: int = 100
    """The maximum number of iterations."""

    lr_init: float = 1.0
    """The initial learning rate."""

    lr_steps_to_reduce: int = 20
    """The number of steps required to reduce the learning rate."""

    lr_reduce_factor: float = 0.5
    """The factor by which the learning rate will be multiplied every `lr_steps_to_reduce` step."""

    eps: float = 1e-5
    """The convergence threshold."""

    skills_: Optional["pd.Series[Any]"] = named_series_attrib(name="skill")
    """The workers' skills.
    The `pandas.Series` data is indexed by `worker` and has the corresponding worker skill."""

    probas_: Optional[pd.DataFrame] = attr.ib(init=False)
    """The probability distributions of task labels.
    The `pandas.DataFrame` data is indexed by `task` so that `result.loc[task, label]` is the probability that
    the `task` true label is equal to `label`. Each probability is in the range from 0 to 1,
    all task probabilities must sum up to 1."""

    def _init_skills(self, data: pd.DataFrame) -> "pd.Series[Any]":
        skill_value = 1 / data.label.unique().size + self.eps
        skill_index = pd.Index(data.worker.unique(), name="worker")
        return pd.Series(skill_value, index=skill_index)

    def _apply(self, data: pd.DataFrame) -> "ZeroBasedSkill":
        check_is_fitted(self, attributes="skills_")
        mv = MajorityVote().fit(data, self.skills_)
        self.labels_ = mv.labels_
        self.probas_ = mv.probas_
        return self

    def fit(self, data: pd.DataFrame) -> "ZeroBasedSkill":
        """Fits the model to the training data.

        Args:
            data (DataFrame): The training dataset of workers' labeling results
                which is represented as the `pandas.DataFrame` data containing `task`, `worker`, and `label` columns.

        Returns:
            ZeroBasedSkill: self.
        """

        # Initialization
        data = data[["task", "worker", "label"]]
        skills = self._init_skills(data)
        mv = MajorityVote()

        # Updating skills and re-fitting majority vote n_iter times
        learning_rate = self.lr_init
        for iteration in range(1, self.n_iter + 1):
            if iteration % self.lr_steps_to_reduce == 0:
                learning_rate *= self.lr_reduce_factor
            mv.fit(data, skills=skills)
            assert mv.labels_ is not None, "no labels_"
            skills = skills + learning_rate * (
                get_accuracy(data, mv.labels_, by="worker") - skills
            )

        # Saving results
        self.skills_ = skills

        return self

    def predict(self, data: pd.DataFrame) -> "pd.Series[Any]":
        """Predicts the true labels of tasks when the model is fitted.

        Args:
            data (DataFrame): The training dataset of workers' labeling results
                which is represented as the `pandas.DataFrame` data containing `task`, `worker`, and `label` columns.

        Returns:
            Series: The task labels. The `pandas.Series` data is indexed by `task`
                so that `labels.loc[task]` is the most likely true label of tasks.
        """

        self._apply(data)
        assert self.labels_ is not None, "no labels_"
        return self.labels_

    def predict_proba(self, data: pd.DataFrame) -> pd.DataFrame:
        """Returns probability distributions of labels for each task when the model is fitted.

        Args:
            data (DataFrame): The training dataset of workers' labeling results
                which is represented as the `pandas.DataFrame` data containing `task`, `worker`, and `label` columns.

        Returns:
            DataFrame: The probability distributions of task labels.
                The `pandas.DataFrame` data is indexed by `task` so that `result.loc[task, label]` is the probability that the `task` true label is equal to `label`.
                Each probability is in the range from 0 to 1, all task probabilities must sum up to 1.
        """

        self._apply(data)
        assert self.probas_ is not None, "no probas_"
        return self.probas_

    def fit_predict(self, data: pd.DataFrame) -> "pd.Series[Any]":
        """Fits the model to the training data and returns the aggregated results.

        Args:
            data (DataFrame): The training dataset of workers' labeling results
                which is represented as the `pandas.DataFrame` data containing `task`, `worker`, and `label` columns.
        Returns:
            Series: The task labels. The `pandas.Series` data is indexed by `task`
                so that `labels.loc[task]` is the most likely true label of tasks.
        """

        return self.fit(data).predict(data)

    def fit_predict_proba(self, data: pd.DataFrame) -> pd.DataFrame:
        """Fits the model to the training data and returns the aggregated results.
        Args:
            data (DataFrame): The training dataset of workers' labeling results
                which is represented as the `pandas.DataFrame` data containing `task`, `worker`, and `label` columns.
        Returns:
            Series: The task labels. The `pandas.Series` data is indexed by `task`
                so that `labels.loc[task]` is the most likely true label of tasks.
        """

        return self.fit(data).predict_proba(data)
