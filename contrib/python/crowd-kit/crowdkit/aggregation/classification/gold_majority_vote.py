__all__ = ["GoldMajorityVote"]

from typing import Any, Optional

import attr
import pandas as pd
from sklearn.utils.validation import check_is_fitted

from ..base import BaseClassificationAggregator
from ..utils import get_accuracy, named_series_attrib
from .majority_vote import MajorityVote


@attr.s
class GoldMajorityVote(BaseClassificationAggregator):
    r"""The **Gold Majority Vote** model is used when a golden dataset (ground truth) exists for some tasks.
    It calculates the probability of a correct label for each worker based on the golden set.
    After that, the sum of the probabilities of each label is calculated for each task.
    The correct label is the one with the greatest sum of the probabilities.

    For example, you have 10 000 tasks completed by 3 000 different workers. And you have 100 tasks where you already
    know the ground truth labels. First, you can call `fit` to calculate the percentage of correct labels for each worker.
    And then call `predict` to calculate labels for your 10 000 tasks.

    The following rules must be observed:
    1. All workers must complete at least one task from the golden dataset.
    2. All workers from the dataset that is submitted to `predict` must be included in the response dataset that is submitted to `fit`.

    Examples:
        >>> import pandas as pd
        >>> from crowdkit.aggregation import GoldMajorityVote
        >>> df = pd.DataFrame(
        >>>     [
        >>>         ['t1', 'p1', 0],
        >>>         ['t1', 'p2', 0],
        >>>         ['t1', 'p3', 1],
        >>>         ['t2', 'p1', 1],
        >>>         ['t2', 'p2', 0],
        >>>         ['t2', 'p3', 1],
        >>>     ],
        >>>     columns=['task', 'worker', 'label']
        >>> )
        >>> true_labels = pd.Series({'t1': 0})
        >>> gold_mv = GoldMajorityVote()
        >>> result = gold_mv.fit_predict(df, true_labels)

    Attributes:
        labels_ (typing.Optional[pandas.core.series.Series]): The task labels. The `pandas.Series` data is indexed by `task`
            so that `labels.loc[task]` is the most likely true label of tasks.

        skills_ (typing.Optional[pandas.core.series.Series]): The workers' skills. The `pandas.Series` data is indexed by `worker`
            and has the corresponding worker skill.

        probas_ (typing.Optional[pandas.core.frame.DataFrame]): The probability distributions of task labels.
            The `pandas.DataFrame` data is indexed by `task` so that `result.loc[task, label]`
            is the probability that the `task` true label is equal to `label`. Each
            probability is in the range from 0 to 1, all task probabilities must sum up to 1.
    """

    # Available after fit
    skills_: Optional["pd.Series[Any]"] = named_series_attrib(name="skill")

    # Available after predict or predict_proba
    # labels_
    probas_: Optional[pd.DataFrame] = attr.ib(init=False)

    def _apply(self, data: pd.DataFrame) -> "GoldMajorityVote":
        check_is_fitted(self, attributes="skills_")
        mv = MajorityVote().fit(data, self.skills_)
        self.labels_ = mv.labels_
        self.probas_ = mv.probas_
        return self

    def fit(self, data: pd.DataFrame, true_labels: "pd.Series[Any]") -> "GoldMajorityVote":  # type: ignore
        """Fits the model to the training data.

        Args:
            data (DataFrame): The training dataset of workers' labeling results
                which is represented as the `pandas.DataFrame` data containing `task`, `worker`, and `label` columns.

            true_labels (Series): The ground truth labels of tasks. The `pandas.Series` data is indexed by `task`
                so that `labels.loc[task]` is the task ground truth label.

        Returns:
            GoldMajorityVote: self.
        """

        data = data[["task", "worker", "label"]]
        self.skills_ = get_accuracy(data, true_labels=true_labels, by="worker")
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

    def fit_predict(self, data: pd.DataFrame, true_labels: "pd.Series[Any]") -> "pd.Series[Any]":  # type: ignore
        """Fits the model to the training data and returns the aggregated results.

        Args:
            data (DataFrame): The training dataset of workers' labeling results
                which is represented as the `pandas.DataFrame` data containing `task`, `worker`, and `label` columns.

            true_labels (Series): The ground truth labels of tasks. The `pandas.Series` data is indexed by `task`
                so that `labels.loc[task]` is the task ground truth label.

        Returns:
            Series: The task labels. The `pandas.Series` data is indexed by `task`
                so that `labels.loc[task]` is the most likely true label of tasks.
        """

        return self.fit(data, true_labels).predict(data)

    def fit_predict_proba(
        self, data: pd.DataFrame, true_labels: "pd.Series[Any]"
    ) -> pd.DataFrame:
        """Fits the model to the training data and returns probability distributions of labels for each task.

        Args:
            data (DataFrame): The training dataset of workers' labeling results
                which is represented as the `pandas.DataFrame` data containing `task`, `worker`, and `label` columns.

            true_labels (Series): The ground truth labels of tasks. The `pandas.Series` data is indexed by `task`
                so that `labels.loc[task]` is the task ground truth label.

        Returns:
            DataFrame: Probability distributions of task labels.
                The `pandas.DataFrame` data is indexed by `task` so that `result.loc[task, label]` is the probability that the `task` true label is equal to `label`.
                Each probability is in the range from 0 to 1, all task probabilities must sum up to 1.
        """

        return self.fit(data, true_labels).predict_proba(data)
