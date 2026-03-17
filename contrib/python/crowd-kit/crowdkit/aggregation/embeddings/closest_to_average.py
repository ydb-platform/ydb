__all__ = ["ClosestToAverage"]

from typing import Any, Callable, Optional

import attr
import numpy as np
import numpy.typing as npt
import pandas as pd

from ..base import BaseEmbeddingsAggregator


@attr.s
class ClosestToAverage(BaseEmbeddingsAggregator):
    """The **Closest to Average** aggregation model chooses the output with the embedding that's closest to the average embedding.

    This method takes a `DataFrame` containing four columns: `task`, `worker`, `output`, and `embedding`.
    Here the `embedding` is a vector containing a representation of the `output` which might be any
    type of data such as text, images, NumPy arrays, etc. As a result, the method returns the output which
    embedding is the closest one to the average embedding of the task responses.
    """

    distance: Callable[[npt.NDArray[Any], npt.NDArray[Any]], float] = attr.ib()
    """A callable that takes two NumPy arrays (the task embedding and the aggregated embedding)
    and returns a single `float` number: the distance between these two vectors."""

    embeddings_and_outputs_: pd.DataFrame = attr.ib(init=False)
    """The task embeddings and outputs. The `pandas.DataFrame` data is indexed by `task` and has the `embedding` and `output` columns."""

    scores_: pd.DataFrame = attr.ib(init=False)
    """The task label scores. The `pandas.DataFrame` data is indexed by `task` so that `result.loc[task, label]` is a score of `label` for `task`."""

    def fit(
        self,
        data: pd.DataFrame,
        aggregated_embeddings: Optional["pd.Series[Any]"] = None,
        true_embeddings: Optional["pd.Series[Any]"] = None,
    ) -> "ClosestToAverage":
        """Fits the model to the training data.

        Args:
            data (DataFrame): The workers' outputs with their embeddings.
                The `pandas.DataFrame` data contains the `task`, `worker`, `output`, and `embedding` columns.
            aggregated_embeddings (Series): The task aggregated embeddings.
                The `pandas.Series` data is indexed by `task` and contains the corresponding aggregated embeddings.
            true_embeddings (Series): The embeddings of the true task responses.
                The `pandas.Series` data is indexed by `task` and contains the corresponding embeddings.
                The multiple true embeddings are not supported for a single task.

        Returns:
            ClosestToAverage: self.
        """

        if true_embeddings is not None and not true_embeddings.index.is_unique:
            raise ValueError(
                "Incorrect data in true_embeddings: multiple true embeddings for a single task are not supported."
            )

        data = data[["task", "worker", "output", "embedding"]]
        if aggregated_embeddings is None:
            group = data.groupby("task")
            # we don't use .mean() because it does not work with np.array in older pandas versions
            avg_embeddings = group.embedding.apply(np.sum) / group.worker.count()
            avg_embeddings.update(true_embeddings)  # type: ignore
        else:
            avg_embeddings = aggregated_embeddings

        # Calculating distances (scores)
        data = data.join(avg_embeddings.rename("avg_embedding"), on="task")
        # TODO: native Python functions are slow
        data["score"] = data.apply(
            lambda row: self.distance(row.embedding, row.avg_embedding), axis=1
        )

        # Selecting best scores and outputs
        scores = data[["task", "output", "score", "embedding"]]
        # TODO: process cases when we actually have an answer in true_embeddings
        # TODO: to do that we must make true_embeddings a DataFrame with `output` column
        embeddings_and_outputs = scores[["task", "output", "embedding"]].loc[
            scores.groupby("task")["score"].idxmin()
        ]

        #
        self.scores_ = scores.set_index("task")
        self.embeddings_and_outputs_ = embeddings_and_outputs.set_index("task")

        return self

    def fit_predict_scores(
        self,
        data: pd.DataFrame,
        aggregated_embeddings: Optional["pd.Series[Any]"] = None,
    ) -> pd.DataFrame:
        """Fits the model to the training data and returns the estimated scores.

        Args:
            data (DataFrame): The workers' outputs with their embeddings.
                The `pandas.DataFrame` data contains the `task`, `worker`, `output`, and `embedding` columns.
            aggregated_embeddings (Series): The task aggregated embeddings.
                The `pandas.Series` data is indexed by `task` and contains the corresponding aggregated embeddings.

        Returns:
            DataFrame: The task label scores.
                The `pandas.DataFrame` data is indexed by `task` so that `result.loc[task, label]`
                is a score of `label` for `task`.
        """

        return self.fit(data, aggregated_embeddings).scores_

    def fit_predict(
        self,
        data: pd.DataFrame,
        aggregated_embeddings: Optional["pd.Series[Any]"] = None,
    ) -> pd.DataFrame:
        """
        Fits the model to the training data and returns the aggregated outputs.

        Args:
            data (DataFrame): The workers' outputs with their embeddings.
                The `pandas.DataFrame` data contains the `task`, `worker`, `output`, and `embedding` columns.
            aggregated_embeddings (Series): The task aggregated embeddings.
                The `pandas.Series` data is indexed by `task` and contains the corresponding aggregated embeddings.

        Returns:
            DataFrame: The task embeddings and outputs.
                The `pandas.DataFrame` data is indexed by `task` and has the `embedding` and `output` columns.
        """

        return self.fit(data, aggregated_embeddings).embeddings_and_outputs_
