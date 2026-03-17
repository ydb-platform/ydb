__all__ = ["SegmentationRASA"]

from typing import Any, List, cast

import attr
import numpy as np
import numpy.typing as npt
import pandas as pd

from ..base import BaseImageSegmentationAggregator

_EPS = 1e-5


@attr.s
class SegmentationRASA(BaseImageSegmentationAggregator):
    r"""The **Segmentation RASA** (Reliability Aware Sequence Aggregation) algorithm chooses a pixel if the sum of the weighted votes of each worker is more than 0.5.

    The Segmentation RASA algorithm consists of three steps:
    1. Performs the weighted Majority Vote algorithm.
    2. Calculates weights for each worker from the current Majority Vote estimation.
    3. Performs the Segmentation RASA algorithm for a single image.

    The algorithm works iteratively. At each step, the workers are reweighted in proportion to their distances
    from the current answer estimation. The distance is calculated as $1 - IOU$, where `IOU` (Intersection over Union) is an extent of overlap of two boxes.
    This algorithm is a modification of the RASA method for texts.

    J. Li, F. Fukumoto. A Dataset of Crowdsourced Word Sequences: Collections and Answer Aggregation for Ground Truth Creation.
    *Proceedings of the First Workshop on Aggregating and Analysing Crowdsourced Annotations for NLP*, (2019), 24-28.

    <https://doi.org/10.18653/v1/D19-5904>

    Examples:
        >>> import numpy as np
        >>> import pandas as pd
        >>> from crowdkit.aggregation import SegmentationRASA
        >>> df = pd.DataFrame(
        >>>     [
        >>>         ['t1', 'p1', np.array([[1, 0], [1, 1]])],
        >>>         ['t1', 'p2', np.array([[0, 1], [1, 1]])],
        >>>         ['t1', 'p3', np.array([[0, 1], [1, 1]])]
        >>>     ],
        >>>     columns=['task', 'worker', 'segmentation']
        >>> )
        >>> result = SegmentationRASA().fit_predict(df)
    """

    n_iter: int = attr.ib(default=10)
    """The maximum number of iterations."""

    tol: float = attr.ib(default=1e-5)
    """The tolerance stopping criterion for iterative methods with a variable number of steps.
    The algorithm converges when the loss change is less than the `tol` parameter."""

    weights_: npt.NDArray[Any] = attr.ib(init=False)
    """A list of workers' weights."""

    mv_: npt.NDArray[Any] = attr.ib(init=False)
    """The weighted task segmentations calculated with the Majority Vote algorithm."""

    loss_history_: List[float] = attr.ib(init=False)
    """A list of loss values during training."""

    @staticmethod
    def _segmentation_weighted(
        segmentations: "pd.Series[Any]", weights: npt.NDArray[Any]
    ) -> npt.NDArray[Any]:
        """
        Performs the weighted Majority Vote algorithm.

        From the weights of all workers and their segmentation, performs the
        weighted Majority Vote for the inclusion of each pixel in the answer.
        """
        weighted_segmentations = (weights * segmentations.T).T
        return cast(npt.NDArray[Any], weighted_segmentations.sum(axis=0))

    @staticmethod
    def _calculate_weights(
        segmentations: "pd.Series[Any]", mv: npt.NDArray[Any]
    ) -> npt.NDArray[Any]:
        """
        Calculates weights for each worker from the current Majority Vote estimation.
        """
        intersection = (segmentations & mv).astype(float)
        union = (segmentations | mv).astype(float)
        distances = 1 - intersection.sum(axis=(1, 2)) / union.sum(axis=(1, 2))  # type: ignore
        # add a small bias for more
        # numerical stability and correctness of transform.
        weights = np.log(1 / (distances + _EPS) + 1)
        return cast(npt.NDArray[Any], weights / np.sum(weights))

    def _aggregate_one(self, segmentations: "pd.Series[Any]") -> npt.NDArray[Any]:
        """
        Performs Segmentation RASA algorithm for a single image.
        """
        size = len(segmentations)
        segmentations_np = np.stack(segmentations.values)  # type: ignore
        weights = np.full(size, 1 / size)
        mv = self._segmentation_weighted(segmentations_np, weights)

        last_aggregated = None

        self.loss_history_ = []

        for _ in range(self.n_iter):
            weighted = self._segmentation_weighted(segmentations_np, weights)
            mv = weighted >= 0.5
            weights = self._calculate_weights(segmentations_np, mv)  # type: ignore[assignment,unused-ignore]

            if last_aggregated is not None:
                delta = weighted - last_aggregated
                loss = (delta * delta).sum().sum() / (weighted * weighted).sum().sum()
                self.loss_history_.append(loss)

                if loss < self.tol:
                    break

            last_aggregated = weighted

        return mv

    def fit(self, data: pd.DataFrame) -> "SegmentationRASA":
        """Fits the model to the training data.

        Args:
            data (DataFrame): The training dataset of workers' segmentations
                which is represented as the `pandas.DataFrame` data containing `task`, `worker`, and `segmentation` columns.

        Returns:
            SegmentationRASA: self.
        """

        data = data[["task", "worker", "segmentation"]]

        # The latest pandas version installable under Python3.7 is pandas 1.1.5.
        # This version fails to accept a method with an error but works fine with lambdas
        # >>> TypeError: unhashable type: 'SegmentationRASA'duito an inner logic that tries
        aggregate_one = lambda arg: self._aggregate_one(arg)

        self.segmentations_ = data.groupby("task").segmentation.apply(aggregate_one)

        return self

    def fit_predict(self, data: pd.DataFrame) -> "pd.Series[Any]":
        """Fits the model to the training data and returns the aggregated segmentations.

        Args:
            data (DataFrame): The training dataset of workers' segmentations
                which is represented as the `pandas.DataFrame` data containing `task`, `worker`, and `segmentation` columns.

        Returns:
            Series: Task segmentations. The `pandas.Series` data is indexed by `task`
                so that `segmentations.loc[task]` is the task aggregated segmentation.
        """

        return self.fit(data).segmentations_
