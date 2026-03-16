__all__ = ["KOS"]

from typing import Any, cast

import attr
import numpy as np
import pandas as pd

from ..base import BaseClassificationAggregator

_MAX = np.float_power(10, 300)


@attr.s
class KOS(BaseClassificationAggregator):
    r"""The **KOS** (Karger, Oh, and Shah 2011) aggregation model is an iterative algorithm that calculates the log-likelihood of the task being positive while modeling
    the worker reliability.

    Let $A_{ij}$ be a matrix of the responses of a worker $j$ on a task $i$.
    If the worker $j$ does not respond to the task $i$, then $A_{ij} = 0$. Otherwise, $|A_{ij}| = 1$.
    The algorithm operates on real-valued task messages $x_{i \rightarrow j}$  and
    worker messages $y_{j \rightarrow i}$. A task message $x_{i \rightarrow j}$ represents
    the log-likelihood of task $i$ being a positive task, and a worker message $y_{j \rightarrow i}$ represents
    how reliable worker $j$ is.

    At $k$-th iteration, the values are updated as follows:
    $x_{i \rightarrow j}^{(k)} = \sum_{j^{'} \in \partial i \backslash j} A_{ij^{'}} y_{j^{'} \rightarrow i}^{(k-1)}$
    and
    $y_{j \rightarrow i}^{(k)} = \sum_{i^{'} \in \partial j \backslash i} A_{i^{'}j} x_{i^{'} \rightarrow j}^{(k-1)}$.

    David R. Karger, Sewoong Oh, and Devavrat Shah. Budget-Optimal Task Allocation for Reliable Crowdsourcing Systems.
    *Operations Research 62.1 (2014)*, 1-38.

    <https://arxiv.org/abs/1110.3564>

    Examples:
        >>> from crowdkit.aggregation import KOS
        >>> from crowdkit.datasets import load_dataset
        >>> df, gt = load_dataset('relevance-2')
        >>> ds = KOS(10)
        >>> result = ds.fit_predict(df)
    """

    n_iter: int = attr.ib(default=100)
    """The maximum number of iterations."""

    random_state: int = attr.ib(default=0)
    """The state of the random number generator."""

    def fit(self, data: pd.DataFrame) -> "KOS":
        """Fits the model to the training data.

        Args:
            data (DataFrame): The training dataset of workers' labeling results
                which is represented as the `pandas.DataFrame` data containing `task`, `worker`, and `label` columns.

        Returns:
            KOS: self.
        """

        np.random.seed(self.random_state)

        # Early exit
        if not data.size:
            self.labels_ = pd.Series([], dtype="O")
            return self

        # Initialization
        kos_data = data.copy()
        labels = kos_data.label.unique()
        if len(labels) != 2:
            raise ValueError(
                "KOS aggregation method is for binary classification only."
            )
        mapping = {labels[0]: 1, labels[1]: -1}
        kos_data.label = kos_data.label.apply(lambda x: mapping[x])
        kos_data["reliabilities"] = np.random.normal(loc=1, scale=1, size=len(kos_data))

        # Updating reliabilities
        for _ in range(self.n_iter):
            # Update inferred labels for (task, worker)
            kos_data["multiplied"] = kos_data.label * kos_data.reliabilities
            kos_data["summed"] = list(
                kos_data.groupby("task")["multiplied"].sum()[kos_data.task]
            )
            # Early exit to prevent NaN
            if (np.abs(kos_data["summed"]) > _MAX).any():
                break
            kos_data["inferred"] = (kos_data["summed"] - kos_data["multiplied"]).astype(
                float
            )

            # Update reliabilities for (task, worker)
            kos_data["multiplied"] = kos_data.label * kos_data.inferred
            kos_data["summed"] = list(
                kos_data.groupby("worker")["multiplied"].sum()[kos_data.worker]
            )
            # Early exit to prevent NaN
            if (np.abs(kos_data["summed"]) > _MAX).any():
                break
            kos_data["reliabilities"] = (kos_data.summed - kos_data.multiplied).astype(
                "float"
            )

        kos_data["inferred"] = kos_data.label * kos_data.reliabilities
        inferred_labels = np.sign(kos_data.groupby("task")["inferred"].sum())
        back_mapping = {v: k for k, v in mapping.items()}
        self.labels_ = cast(
            "pd.Series[Any]", inferred_labels.apply(lambda x: back_mapping[x])
        )
        return self

    def fit_predict(self, data: pd.DataFrame) -> "pd.Series[Any]":
        """Fits the model to the training data and returns the aggregated results.

        Args:
            data (DataFrame): The training dataset of workers' labeling results
                which is represented as the `pandas.DataFrame` data containing `task`, `worker`, and `label` columns.

        Returns:
            Series: The task labels. The `pandas.Series` data is indexed by `task`
                so that `labels.loc[task]` is the most likely true label of tasks.
        """

        self.fit(data)
        assert self.labels_ is not None, "no labels_"
        return self.labels_
