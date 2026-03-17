__all__ = [
    "RASA",
]

from functools import partial
from typing import Any, List, Optional

import attr
import numpy as np
import numpy.typing as npt
import pandas as pd
import scipy.stats as sps
from scipy.spatial import distance

from ..base import BaseEmbeddingsAggregator
from .closest_to_average import ClosestToAverage

_EPS = 1e-5


@attr.s
class RASA(BaseEmbeddingsAggregator):
    r"""The **Reliability Aware Sequence Aggregation** (RASA) algorithm consists of three steps.

    **Step 1**. Encode the worker answers into embeddings.

    **Step 2**. Estimate the *global* workers' reliabilities $\beta$ by iteratively performing two steps:
    1. For each task, estimate the aggregated embedding: $\hat{e}_i = \frac{\sum_k
    \beta_k e_i^k}{\sum_k \beta_k}$
    2. For each worker, estimate the global reliability: $\beta_k = \frac{\chi^2_{(\alpha/2,
    |\mathcal{V}_k|)}}{\sum_i\left(\|e_i^k - \hat{e}_i\|^2\right)}$, where $\mathcal{V}_k$
    is a set of tasks completed by the worker $k$.

    **Step 3**. Estimate the aggregated result. It is the output which embedding is
    the closest one to $\hat{e}_i$.

    Jiyi Li, Fumiyo Fukumoto. A Dataset of Crowdsourced Word Sequences: Collections and Answer Aggregation for Ground Truth Creation.
    In *Proceedings of the First Workshop on Aggregating and Analysing Crowdsourced Annotations for NLP*,
    Hong Kong, China (November 3, 2019), 24–28.
    <https://doi.org/10.18653/v1/D19-5904>

    Examples:
        >>> import numpy as np
        >>> import pandas as pd
        >>> from crowdkit.aggregation import RASA
        >>> df = pd.DataFrame(
        >>>     [
        >>>         ['t1', 'p1', 'a', np.array([1.0, 0.0])],
        >>>         ['t1', 'p2', 'a', np.array([1.0, 0.0])],
        >>>         ['t1', 'p3', 'b', np.array([0.0, 1.0])]
        >>>     ],
        >>>     columns=['task', 'worker', 'output', 'embedding']
        >>> )
        >>> result = RASA().fit_predict(df)
    """

    n_iter: int = attr.ib(default=100)
    """The maximum number of iterations."""

    tol: float = attr.ib(default=1e-9)
    """The tolerance stopping criterion for iterative methods with a variable number of steps. The algorithm converges when the loss change is less than the `tol` parameter."""

    alpha: float = attr.ib(default=0.05)
    """The significance level of the chi-squared distribution quantiles in the $\beta$ parameter formula."""

    loss_history_: List[float] = attr.ib(init=False)
    """A list of loss values during training."""

    @staticmethod
    def _aggregate_embeddings(
        data: pd.DataFrame,
        skills: "pd.Series[Any]",
        true_embeddings: Optional["pd.Series[Any]"] = None,
    ) -> "pd.Series[Any]":
        """Calculates the weighted average of embeddings for each task."""
        data = data.join(skills.rename("skill"), on="worker")
        data["weighted_embedding"] = data.skill * data.embedding
        group = data.groupby("task")
        aggregated_embeddings = (
            group.weighted_embedding.apply(np.sum) / group.skill.sum()
        )
        if true_embeddings is None:
            true_embeddings = pd.Series([], dtype=np.float64)
        aggregated_embeddings.update(true_embeddings)
        return aggregated_embeddings

    @staticmethod
    def _update_skills(
        data: pd.DataFrame,
        aggregated_embeddings: "pd.Series[Any]",
        prior_skills: "pd.Series[Any]",
    ) -> "pd.Series[Any]":
        """Estimates the global reliabilities by aggregated embeddings."""
        data = data.join(
            aggregated_embeddings.rename("aggregated_embedding"), on="task"
        )
        data["distance"] = ((data.embedding - data.aggregated_embedding) ** 2).apply(
            np.sum
        )
        total_distances = data.groupby("worker").distance.apply(np.sum)
        total_distances.clip(lower=_EPS, inplace=True)
        return prior_skills / total_distances

    @staticmethod
    def _cosine_distance(
        embedding: npt.NDArray[Any], avg_embedding: npt.NDArray[Any]
    ) -> float:
        if not embedding.any() or not avg_embedding.any():
            return float("inf")
        return float(distance.cosine(embedding, avg_embedding))

    def _apply(
        self, data: pd.DataFrame, true_embeddings: Optional["pd.Series[Any]"] = None
    ) -> "RASA":
        cta = ClosestToAverage(distance=self._cosine_distance)
        cta.fit(
            data,
            aggregated_embeddings=self.aggregated_embeddings_,
            true_embeddings=true_embeddings,
        )
        self.scores_ = cta.scores_
        self.embeddings_and_outputs_ = cta.embeddings_and_outputs_
        return self

    def fit(
        self, data: pd.DataFrame, true_embeddings: Optional["pd.Series[Any]"] = None
    ) -> "RASA":
        """Fits the model to the training data.

        Args:
            data (DataFrame): The workers' outputs with their embeddings.
                The `pandas.DataFrame` data contains the `task`, `worker`, `output`, and `embedding` columns.
            true_embeddings (Series): The embeddings of the true task responses.
                The `pandas.Series` data is indexed by `task` and contains the corresponding embeddings.
                The multiple true embeddings are not supported for a single task.

        Returns:
            RASA: self.
        """

        data = data[["task", "worker", "embedding"]]

        if true_embeddings is not None and not true_embeddings.index.is_unique:
            raise ValueError(
                "Incorrect data in true_embeddings: multiple true embeddings for a single task are not supported."
            )

        # What we call skills here is called reliabilities in the paper
        prior_skills = data.worker.value_counts().apply(
            partial(sps.chi2.isf, self.alpha / 2)
        )
        skills = pd.Series(1.0, index=data.worker.unique())
        aggregated_embeddings = None
        last_aggregated = None

        for _ in range(self.n_iter):
            aggregated_embeddings = self._aggregate_embeddings(
                data, skills, true_embeddings
            )
            skills = self._update_skills(data, aggregated_embeddings, prior_skills)

            if last_aggregated is not None:
                delta = aggregated_embeddings - last_aggregated
                loss = (delta * delta).sum().sum() / (
                    aggregated_embeddings * aggregated_embeddings
                ).sum().sum()
                if loss < self.tol:
                    break
            last_aggregated = aggregated_embeddings

        self.prior_skills_ = prior_skills
        self.skills_ = skills
        self.aggregated_embeddings_ = aggregated_embeddings
        return self

    def fit_predict_scores(
        self, data: pd.DataFrame, true_embeddings: Optional["pd.Series[Any]"] = None
    ) -> pd.DataFrame:
        """Fits the model to the training data and returns the estimated scores.

        Args:
            data (DataFrame): The workers' outputs with their embeddings.
                The `pandas.DataFrame` data contains the `task`, `worker`, `output`, and `embedding` columns.
            true_embeddings (Series): The embeddings of the true task responses.
                The `pandas.Series` data is indexed by `task` and contains the corresponding embeddings.
                The multiple true embeddings are not supported for a single task.

        Returns:
            DataFrame: The task label scores.
                The `pandas.DataFrame` data is indexed by `task` so that `result.loc[task, label]`
                is a score of `label` for `task`.
        """

        return self.fit(data, true_embeddings)._apply(data, true_embeddings).scores_

    def fit_predict(
        self, data: pd.DataFrame, true_embeddings: Optional["pd.Series[Any]"] = None
    ) -> pd.DataFrame:
        """Fits the model to the training data and returns the aggregated outputs.

        Args:
            data (DataFrame): The workers' outputs with their embeddings.
                The `pandas.DataFrame` data contains the `task`, `worker`, `output`, and `embedding` columns.
            true_embeddings (Series): The embeddings of the true task responses.
                The `pandas.Series` data is indexed by `task` and contains the corresponding embeddings.
                The multiple true embeddings are not supported for a single task.

        Returns:
            DataFrame: The task embeddings and outputs.
                The `pandas.DataFrame` data is indexed by `task` and has the `embedding` and `output` columns.
        """

        return (
            self.fit(data, true_embeddings)
            ._apply(data, true_embeddings)
            .embeddings_and_outputs_
        )
