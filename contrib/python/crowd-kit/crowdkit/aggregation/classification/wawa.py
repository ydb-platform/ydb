__all__ = ["Wawa"]

from typing import Any, Optional

import attr
import pandas as pd
from sklearn.utils.validation import check_is_fitted

from ..base import BaseClassificationAggregator
from ..utils import get_accuracy, named_series_attrib
from .majority_vote import MajorityVote


@attr.s
class Wawa(BaseClassificationAggregator):
    r"""The **Worker Agreement with Aggregate** (Wawa) algorithm consists of three steps:
    1. calculates the majority vote label;
    2. estimates workers' skills as a fraction of responses that are equal to the majority vote;
    3. calculates the weigthed majority vote based on skills from the previous step.

    Examples:
        >>> from crowdkit.aggregation import Wawa
        >>> from crowdkit.datasets import load_dataset
        >>> df, gt = load_dataset('relevance-2')
        >>> result = Wawa().fit_predict(df)
    """

    skills_: Optional["pd.Series[Any]"] = named_series_attrib(name="skill")
    """The workers' skills.
    The `pandas.Series` data is indexed by `worker` and has the corresponding worker skill."""

    probas_: Optional[pd.DataFrame] = attr.ib(init=False)
    """The probability distributions of task labels.
    The `pandas.DataFrame` data is indexed by `task` so that `result.loc[task, label]` is the probability that
    the `task` true label is equal to `label`. Each probability is in the range from 0 to 1,
    all task probabilities must sum up to 1."""

    def _apply(self, data: pd.DataFrame) -> "Wawa":
        check_is_fitted(self, attributes="skills_")
        mv = MajorityVote().fit(data, skills=self.skills_)
        self.probas_ = mv.probas_
        self.labels_ = mv.labels_
        return self

    def fit(self, data: pd.DataFrame) -> "Wawa":
        """Fits the model to the training data.

        Args:
            data (DataFrame): The training dataset of workers' labeling results
                which is represented as the `pandas.DataFrame` data containing `task`, `worker`, and `label` columns.

        Returns:
            Wawa: self.
        """

        # TODO: support weights?
        data = data[["task", "worker", "label"]]
        mv = MajorityVote().fit(data)
        assert mv.labels_ is not None, "no labels_"
        self.skills_ = get_accuracy(data, true_labels=mv.labels_, by="worker")
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
            DataFrame: Probability distributions of task labels.
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
        """Fits the model to the training data and returns probability distributions of labels for each task.

        Args:
            data (DataFrame): The training dataset of workers' labeling results
                which is represented as the `pandas.DataFrame` data containing `task`, `worker`, and `label` columns.

        Returns:
            DataFrame: Probability distributions of task labels.
                The `pandas.DataFrame` data is indexed by `task` so that `result.loc[task, label]` is the probability that the `task` true label is equal to `label`.
                Each probability is in the range from 0 to 1, all task probabilities must sum up to 1.
        """

        return self.fit(data).predict_proba(data)
