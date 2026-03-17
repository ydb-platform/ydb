__all__ = [
    "BaseClassificationAggregator",
    "BaseImageSegmentationAggregator",
    "BaseEmbeddingsAggregator",
    "BaseTextsAggregator",
    "BasePairwiseAggregator",
]

from typing import Any, Optional

import attr
import pandas as pd
from sklearn.base import BaseEstimator

from ..utils import named_series_attrib


@attr.s
class BaseClassificationAggregator(BaseEstimator):  # type: ignore[misc]
    """This is a base class for all classification aggregators

    Attributes:
        labels_ (typing.Optional[pandas.core.series.Series]): Tasks' labels.
            A pandas.Series indexed by `task` such that `labels.loc[task]`
            is the task's most likely true label.
    """

    labels_: Optional["pd.Series[Any]"] = named_series_attrib(name="agg_label")

    def fit(self, data: pd.DataFrame) -> "BaseClassificationAggregator":
        """Args:
            data (DataFrame): Workers' labeling results.
                A pandas.DataFrame containing `task`, `worker` and `label` columns.

        Returns:
            BaseClassificationAggregator: self.
        """
        raise NotImplementedError()

    def fit_predict(self, data: pd.DataFrame) -> "pd.Series[Any]":
        """Args:
            data (DataFrame): Workers' labeling results.
                A pandas.DataFrame containing `task`, `worker` and `label` columns.
        Returns:
            Series: Tasks' labels.
                A pandas.Series indexed by `task` such that `labels.loc[task]`
                is the tasks's most likely true label.
        """
        raise NotImplementedError()


@attr.s
class BaseImageSegmentationAggregator:
    """This is a base class for all image segmentation aggregators

    Attributes:
        segmentations_ (Series): Tasks' segmentations.
            A pandas.Series indexed by `task` such that `labels.loc[task]`
            is the tasks's aggregated segmentation.
    """

    segmentations_: "pd.Series[Any]" = named_series_attrib(name="agg_segmentation")

    def fit(self, data: pd.DataFrame) -> "BaseImageSegmentationAggregator":
        """Args:
            data (DataFrame): Workers' segmentations.
                A pandas.DataFrame containing `worker`, `task` and `segmentation` columns'.

        Returns:
            BaseImageSegmentationAggregator: self.
        """
        raise NotImplementedError()

    def fit_predict(self, data: pd.DataFrame) -> "pd.Series[Any]":
        """Args:
            data (DataFrame): Workers' segmentations.
                A pandas.DataFrame containing `worker`, `task` and `segmentation` columns'.

        Returns:
            Series: Tasks' segmentations.
                A pandas.Series indexed by `task` such that `labels.loc[task]`
                is the tasks's aggregated segmentation.
        """
        raise NotImplementedError()


@attr.s
class BaseEmbeddingsAggregator:
    """This is a base class for all embeddings aggregators
    Attributes:
        embeddings_and_outputs_ (DataFrame): Tasks' embeddings and outputs.
            A pandas.DataFrame indexed by `task` with `embedding` and `output` columns.
    """

    embeddings_and_outputs_: pd.DataFrame = attr.ib(init=False)

    def fit(self, data: pd.DataFrame) -> "BaseEmbeddingsAggregator":
        """Args:
            data (DataFrame): Workers' outputs with their embeddings.
                A pandas.DataFrame containing `task`, `worker`, `output` and `embedding` columns.
        Returns:
            BaseEmbeddingsAggregator: self.
        """
        raise NotImplementedError()

    def fit_predict(self, data: pd.DataFrame) -> pd.DataFrame:
        """Args:
            data (DataFrame): Workers' outputs with their embeddings.
                A pandas.DataFrame containing `task`, `worker`, `output` and `embedding` columns.
        Returns:
            DataFrame: Tasks' embeddings and outputs.
                A pandas.DataFrame indexed by `task` with `embedding` and `output` columns.
        """
        raise NotImplementedError()


@attr.s
class BaseTextsAggregator:
    """This is a base class for all texts aggregators
    Attributes:
        texts_ (Series): Tasks' texts.
            A pandas.Series indexed by `task` such that `result.loc[task, text]`
            is the task's text.
    """

    texts_: "pd.Series[Any]" = named_series_attrib(name="agg_text")

    def fit(self, data: pd.DataFrame) -> "BaseTextsAggregator":
        """Args:
            data (DataFrame): Workers' text outputs.
                A pandas.DataFrame containing `task`, `worker` and `text` columns.
        Returns:
            BaseTextsAggregator: self.
        """
        raise NotImplementedError()

    def fit_predict(self, data: pd.DataFrame) -> "pd.Series[Any]":
        """Args:
            data (DataFrame): Workers' text outputs.
                A pandas.DataFrame containing `task`, `worker` and `text` columns.
        Returns:
            Series: Tasks' texts.
                A pandas.Series indexed by `task` such that `result.loc[task, text]`
                is the task's text.
        """
        raise NotImplementedError()


@attr.s
class BasePairwiseAggregator:
    """This is a base class for all pairwise comparison aggregators
    Attributes:
        scores_ (Series): 'Labels' scores.
            A pandas.Series index by labels and holding corresponding label's scores
    """

    scores_: "pd.Series[Any]" = named_series_attrib(name="agg_score")

    def fit(self, data: pd.DataFrame) -> "BasePairwiseAggregator":
        """Args:
            data (DataFrame): Workers' pairwise comparison results.
                A pandas.DataFrame containing `worker`, `left`, `right`, and `label` columns'.
                For each row `label` must be equal to either `left` column or `right` column.

        Returns:
            BasePairwiseAggregator: self.
        """
        raise NotImplementedError()

    def fit_predict(self, data: pd.DataFrame) -> "pd.Series[Any]":
        """Args:
            data (DataFrame): Workers' pairwise comparison results.
                A pandas.DataFrame containing `worker`, `left`, `right`, and `label` columns'.
                For each row `label` must be equal to either `left` column or `right` column.

        Returns:
            Series: 'Labels' scores.
                A pandas.Series index by labels and holding corresponding label's scores
        """
        raise NotImplementedError()
