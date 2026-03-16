__all__ = ["NoisyBradleyTerry"]

from typing import Any

import attr
import numpy as np
import numpy.typing as npt
import pandas as pd
from scipy.optimize import minimize
from scipy.special import expit

from ..base import BasePairwiseAggregator
from ..utils import factorize, named_series_attrib


@attr.s
class NoisyBradleyTerry(BasePairwiseAggregator):
    r"""Bradley-Terry model for pairwise comparisons with additional parameters.

    This model is a modification of the BradleyTerry model with parameters
    for workers' skills (reliability) and biases.

    Examples:
        The following example shows how to aggregate results of comparisons **grouped by some column**.
        In the example the two questions `q1` and `q2` are used to group the labeled data.
        Temporary data structure is created and the model is applied to it.
        The results are split into two arrays, and each array contains scores for one of the initial groups.

        >>> import pandas as pd
        >>> from crowdkit.aggregation import NoisyBradleyTerry
        >>> data = pd.DataFrame(
        >>>     [
        >>>         ['q1', 'w1', 'a', 'b', 'a'],
        >>>         ['q1', 'w2', 'a', 'b', 'b'],
        >>>         ['q1', 'w3', 'a', 'b', 'a'],
        >>>         ['q2', 'w1', 'a', 'b', 'b'],
        >>>         ['q2', 'w2', 'a', 'b', 'a'],
        >>>         ['q2', 'w3', 'a', 'b', 'b'],
        >>>     ],
        >>>     columns=['question', 'worker', 'left', 'right', 'label']
        >>> )
        >>> # Append question to other columns. After that the data looks like:
        >>> #   question worker     left    right    label
        >>> # 0       q1     w1  (q1, a)  (q1, b)  (q1, a)
        >>> for col in 'left', 'right', 'label':
        >>>     data[col] = list(zip(data['question'], data[col]))
        >>> result = NoisyBradleyTerry(n_iter=10).fit_predict(data)
        >>> # Separate results
        >>> result.index = pd.MultiIndex.from_tuples(result.index, names=['question', 'label'])
        >>> print(result['q1'])      # Scores for all items in the q1 question
        >>> print(result['q2']['b']) # Score for the item b in the q2 question
    """

    n_iter: int = attr.ib(default=100)
    """A number of optimization iterations."""

    tol: float = attr.ib(default=1e-5)
    """The tolerance stopping criterion for iterative methods with a variable number of steps.
    The algorithm converges when the loss change is less than the `tol` parameter."""

    regularization_ratio: float = attr.ib(default=1e-5)
    """The regularization ratio."""

    random_state: int = attr.ib(default=0)
    """The state of the random number generator."""

    skills_: "pd.Series[Any]" = named_series_attrib(name="skill")
    """A pandas.Series index by workers and holding corresponding worker's skill"""

    biases_: "pd.Series[Any]" = named_series_attrib(name="bias")
    """Predicted biases for each worker. Indicates the probability of a worker to choose the left item.
    A series of worker biases indexed by workers."""

    def fit(self, data: pd.DataFrame) -> "NoisyBradleyTerry":
        """Args:
            data (DataFrame): Workers' pairwise comparison results.
                A pandas.DataFrame containing `worker`, `left`, `right`, and `label` columns'.
                For each row `label` must be equal to either `left` column or `right` column.

        Returns:
            NoisyBradleyTerry: self.
        """

        unique_labels, np_data = factorize(data[["left", "right", "label"]].values)
        unique_workers, np_workers = factorize(data.worker.values)  # type: ignore
        np.random.seed(self.random_state)
        x_0 = np.random.rand(1 + unique_labels.size + 2 * unique_workers.size)
        np_data += 1

        x = minimize(
            self._compute_log_likelihood,
            x_0,
            jac=self._compute_gradient,
            args=(
                np_data,
                np_workers,
                unique_labels.size,
                unique_workers.size,
                self.regularization_ratio,
            ),
            method="L-BFGS-B",
            options={"maxiter": self.n_iter, "ftol": np.float32(self.tol)},
        )

        biases_begin = unique_labels.size + 1
        workers_begin = biases_begin + unique_workers.size

        self.scores_ = pd.Series(
            expit(x.x[1:biases_begin]),
            index=pd.Index(unique_labels, name="label"),
            name="score",
        )
        self.biases_ = pd.Series(
            expit(x.x[biases_begin:workers_begin]), index=unique_workers
        )
        self.skills_ = pd.Series(expit(x.x[workers_begin:]), index=unique_workers)

        return self

    def fit_predict(self, data: pd.DataFrame) -> "pd.Series[Any]":
        """Args:
            data (DataFrame): Workers' pairwise comparison results.
                A pandas.DataFrame containing `worker`, `left`, `right`, and `label` columns'.
                For each row `label` must be equal to either `left` column or `right` column.

        Returns:
            Series: 'Labels' scores.
                A pandas.Series index by labels and holding corresponding label's scores
        """
        return self.fit(data).scores_

    @staticmethod
    def _compute_log_likelihood(
        x: npt.NDArray[Any],
        np_data: npt.NDArray[Any],
        np_workers: npt.NDArray[Any],
        labels: int,
        workers: int,
        regularization_ratio: float,
    ) -> float:
        s_i = x[np_data[:, 0]]
        s_j = x[np_data[:, 1]]
        y = np.zeros_like(np_data[:, 2])
        y[np_data[:, 0] == np_data[:, 2]] = 1
        y[np_data[:, 0] != np_data[:, 2]] = -1
        q = x[1 + np_workers + labels]
        gamma = x[1 + np_workers + labels + workers]

        total = np.sum(
            np.log(
                expit(gamma) * expit(y * (s_i - s_j))
                + (1 - expit(gamma)) * expit(y * q)
            )
        )
        reg = np.sum(np.log(expit(x[0] - x[1 : labels + 1]))) + np.sum(
            np.log(expit(x[1 : labels + 1] - x[0]))
        )

        return float(-total + regularization_ratio * reg)

    @staticmethod
    def _compute_gradient(
        x: npt.NDArray[Any],
        np_data: npt.NDArray[Any],
        np_workers: npt.NDArray[Any],
        labels: int,
        workers: int,
        regularization_ratio: float,
    ) -> npt.NDArray[Any]:
        gradient = np.zeros_like(x)

        for worker_idx, (left_idx, right_idx, label) in zip(np_workers, np_data):
            s_i = x[left_idx]
            s_j = x[right_idx]
            y = 1 if label == left_idx else -1
            q = x[1 + labels + worker_idx]
            gamma = x[1 + labels + workers + worker_idx]

            # We'll use autograd in the future
            gradient[left_idx] += (y * np.exp(y * (-(s_i - s_j)))) / (
                (np.exp(-gamma) + 1)
                * (np.exp(y * (-(s_i - s_j))) + 1) ** 2
                * (
                    1 / ((np.exp(-gamma) + 1) * (np.exp(y * (-(s_i - s_j))) + 1))
                    + (1 - 1 / (np.exp(-gamma) + 1)) / (np.exp(-q * y) + 1)
                )
            )  # noqa
            gradient[right_idx] += -(
                y * (np.exp(q * y) + 1) * np.exp(y * (s_i - s_j) + gamma)
            ) / (
                (np.exp(y * (s_i - s_j)) + 1)
                * (
                    np.exp(y * (s_i - s_j) + gamma + q * y)
                    + np.exp(y * (s_i - s_j) + gamma)
                    + np.exp(y * (s_i - s_j) + q * y)
                    + np.exp(q * y)
                )
            )  # noqa
            gradient[labels + worker_idx] = (
                y * np.exp(q * y) * (np.exp(s_i * y) + np.exp(s_j * y))
            ) / (
                (np.exp(q * y) + 1)
                * (
                    np.exp(y * (s_i + q) + gamma)
                    + np.exp(s_i * y + gamma)
                    + np.exp(y * (s_i + q))
                    + np.exp(y * (s_j + q))
                )
            )  # noqa #dq
            gradient[labels + workers + worker_idx] = (
                np.exp(gamma) * (np.exp(s_i * y) - np.exp(y * (s_j + q)))
            ) / (
                (np.exp(gamma) + 1)
                * (
                    np.exp(y * (s_i + q) + gamma)
                    + np.exp(s_i * y + gamma)
                    + np.exp(y * (s_i + q))
                    + np.exp(y * (s_j + q))
                )
            )  # noqa #dgamma

        gradient[1 : labels + 1] -= regularization_ratio * np.tanh(
            (x[1 : labels + 1] - x[0]) / 2.0
        )
        gradient[0] += regularization_ratio * np.sum(
            np.tanh((x[1 : labels + 1] - x[0]) / 2.0)
        )
        return -gradient
