__all__ = ["SegmentationEM"]

from typing import Any, List, Union, cast

import attr
import numpy as np
import numpy.typing as npt
import pandas as pd

from ..base import BaseImageSegmentationAggregator


@attr.s
class SegmentationEM(BaseImageSegmentationAggregator):
    r"""The **Segmentation EM-algorithm** performs a categorical
    aggregation task for each pixel: should it be included in the resulting aggregate or not.
    This task is solved by the single-coin Dawid-Skene algorithm.
    Each worker has a latent parameter `skill` that shows the probability of this worker to answer correctly.

    Skills and true pixel labels are optimized by the Expectation-Maximization algorithm:
    1. **E-step**. Estimates the posterior probabilities using the specified workers' segmentations, the prior probabilities for each pixel,
    and the workers' error probability vector.
    2. **M-step**. Estimates the probability of a worker answering correctly using the specified workers' segmentations and the posterior probabilities for each pixel.


    D. Jung-Lin Lee, A. Das Sarma and A. Parameswaran. Aggregating Crowdsourced Image Segmentations.
    *CEUR Workshop Proceedings. Vol. 2173*, (2018), 1-44.

    <https://ceur-ws.org/Vol-2173/paper10.pdf>

    Examples:
        >>> import numpy as np
        >>> import pandas as pd
        >>> from crowdkit.aggregation import SegmentationEM
        >>> df = pd.DataFrame(
        >>>     [
        >>>         ['t1', 'p1', np.array([[1, 0], [1, 1]])],
        >>>         ['t1', 'p2', np.array([[0, 1], [1, 1]])],
        >>>         ['t1', 'p3', np.array([[0, 1], [1, 1]])]
        >>>     ],
        >>>     columns=['task', 'worker', 'segmentation']
        >>> )
        >>> result = SegmentationEM().fit_predict(df)
    """

    n_iter: int = attr.ib(default=10)
    """The maximum number of EM iterations."""

    tol: float = attr.ib(default=1e-5)
    """The tolerance stopping criterion for iterative methods with a variable number of steps.
    The algorithm converges when the loss change is less than the `tol` parameter."""

    eps: float = 1e-15
    """The convergence threshold."""

    segmentation_region_size_: int = attr.ib(init=False)
    """Segmentation region size."""

    segmentations_sizes_: npt.NDArray[Any] = attr.ib(init=False)
    """Sizes of image segmentations."""

    priors_: Union[float, npt.NDArray[Any]] = attr.ib(init=False)
    """The prior probabilities for each pixel to be included in the resulting aggregate.
    Each probability is in the range from 0 to 1, all probabilities must sum up to 1."""

    posteriors_: npt.NDArray[Any] = attr.ib(init=False)
    """The posterior probabilities for each pixel to be included in the resulting aggregate.
    Each probability is in the range from 0 to 1, all probabilities must sum up to 1."""

    errors_: npt.NDArray[Any] = attr.ib(init=False)
    """The workers' error probability vector."""

    loss_history_: List[float] = attr.ib(init=False)
    """A list of loss values during training."""

    @staticmethod
    def _e_step(
        segmentations: npt.NDArray[Any],
        errors: npt.NDArray[Any],
        priors: Union[float, npt.NDArray[Any]],
    ) -> npt.NDArray[Any]:
        """
        Performs E-step of the algorithm.
        Estimates the posterior probabilities using the specified workers' segmentations, the prior probabilities for each pixel,
        and the workers' error probability vector.
        """

        weighted_seg = (
            np.multiply(errors, segmentations.T.astype(float)).T
            + np.multiply((1 - errors), (1 - segmentations).T.astype(float)).T
        )

        with np.errstate(divide="ignore"):
            pos_log_prob = np.log(priors) + np.log(weighted_seg).sum(axis=0)
            neg_log_prob = np.log(1 - priors) + np.log(1 - weighted_seg).sum(axis=0)

            with np.errstate(invalid="ignore"):
                # division by the denominator in the Bayes formula
                posteriors: npt.NDArray[Any] = np.nan_to_num(
                    np.exp(pos_log_prob)
                    / (np.exp(pos_log_prob) + np.exp(neg_log_prob)),
                    nan=0,
                )

        return posteriors

    @staticmethod
    def _m_step(
        segmentations: npt.NDArray[Any],
        posteriors: npt.NDArray[Any],
        segmentation_region_size: int,
        segmentations_sizes: npt.NDArray[Any],
    ) -> npt.NDArray[Any]:
        """
        Performs M-step of the algorithm.
        Estimates the probability of a worker answering correctly using the specified workers' segmentations and the posterior probabilities for each pixel.
        """

        mean_errors_expectation: npt.NDArray[Any] = (
            segmentations_sizes
            + posteriors.sum()
            - 2 * (segmentations * posteriors).sum(axis=(1, 2))
        ) / segmentation_region_size

        # return probability of worker marking pixel correctly
        return 1 - mean_errors_expectation

    def _evidence_lower_bound(
        self,
        segmentations: npt.NDArray[Any],
        priors: Union[float, npt.NDArray[Any]],
        posteriors: npt.NDArray[Any],
        errors: npt.NDArray[Any],
    ) -> float:
        weighted_seg = (
            np.multiply(errors, segmentations.T.astype(float)).T
            + np.multiply((1 - errors), (1 - segmentations).T.astype(float)).T
        )

        # we handle log(0) * 0 == 0 case with nan_to_num so warnings are irrelevant here
        with np.errstate(divide="ignore", invalid="ignore"):
            log_likelihood_expectation: float = (
                np.nan_to_num(
                    (np.log(weighted_seg) + np.log(priors)[None, ...]) * posteriors,
                    nan=0,
                ).sum()
                + np.nan_to_num(
                    (np.log(1 - weighted_seg) + np.log(1 - priors)[None, ...])
                    * (1 - posteriors),
                    nan=0,
                ).sum()
            )

            return log_likelihood_expectation - float(
                np.nan_to_num(np.log(posteriors) * posteriors, nan=0).sum()
            )

    def _aggregate_one(self, segmentations: "pd.Series[Any]") -> npt.NDArray[np.bool_]:
        """
        Performs the Expectation-Maximization algorithm for a single image.
        """
        priors = sum(segmentations) / len(segmentations)
        segmentations_np: npt.NDArray[Any] = np.stack(segmentations.values)  # type: ignore
        segmentation_region_size = segmentations_np.any(axis=0).sum()

        if segmentation_region_size == 0:
            return np.zeros_like(segmentations_np[0])

        segmentations_sizes = segmentations_np.sum(axis=(1, 2))
        # initialize with errors assuming that ground truth segmentation is majority vote
        errors = self._m_step(
            segmentations_np,
            np.round(priors),
            segmentation_region_size,
            segmentations_sizes,
        )
        loss = -np.inf
        self.loss_history_ = []
        for _ in range(self.n_iter):
            posteriors = self._e_step(segmentations_np, errors, priors)
            posteriors[posteriors < self.eps] = 0
            errors = self._m_step(
                segmentations_np,
                posteriors,
                segmentation_region_size,
                segmentations_sizes,
            )
            new_loss = self._evidence_lower_bound(
                segmentations_np, priors, posteriors, errors
            ) / (len(segmentations_np) * segmentations_np[0].size)
            priors = posteriors
            self.loss_history_.append(new_loss)
            if new_loss - loss < self.tol:
                break
            loss = new_loss

        return cast(npt.NDArray[np.bool_], priors > 0.5)

    def fit(self, data: pd.DataFrame) -> "SegmentationEM":
        """Fits the model to the training data with the EM algorithm.

        Args:
            data (DataFrame): The training dataset of workers' segmentations
                which is represented as the `pandas.DataFrame` data containing `task`, `worker`, and `segmentation` columns.

        Returns:
            SegmentationEM: self.
        """

        data = data[["task", "worker", "segmentation"]]

        self.segmentations_ = data.groupby("task").segmentation.apply(
            lambda segmentations: self._aggregate_one(
                segmentations
            )  # using lambda for python 3.7 compatibility
        )
        return self

    def fit_predict(self, data: pd.DataFrame) -> "pd.Series[Any]":
        """Fits the model to the training data and returns the aggregated segmentations.

        Args:
            data (DataFrame): The training dataset of workers' segmentations
                which is represented as the `pandas.DataFrame` data containing `task`, `worker`, and `segmentation` columns.

        Returns:
            Series: Task segmentations. The `pandas.Series` data is indexed by `task` so that `segmentations.loc[task]` is the task aggregated segmentation.
        """

        return self.fit(data).segmentations_
