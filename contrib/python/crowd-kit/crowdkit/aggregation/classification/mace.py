__all__ = ["MACE"]

from typing import Any, Iterator, List, Optional, Tuple, Union, cast

import attr
import numpy as np
import pandas as pd
import scipy.stats as sps
from numpy.typing import NDArray
from scipy.special import digamma
from tqdm.auto import tqdm, trange

from ..base import BaseClassificationAggregator


def normalize(x: NDArray[np.float64], smoothing: float) -> NDArray[np.float64]:
    """Normalizes the rows of the matrix using the smoothing parameter.

    Args:
        x (np.ndarray): The array to normalize.
        smoothing (float): The smoothing parameter.

    Returns:
        np.ndarray: Normalized array
    """
    norm = (x + smoothing).sum(axis=1)
    return cast(
        NDArray[np.float64],
        np.divide(
            x + smoothing,
            norm[:, np.newaxis],
            out=np.zeros_like(x),
            where=~np.isclose(norm[:, np.newaxis], np.zeros_like(norm[:, np.newaxis])),
        ),
    )


def variational_normalize(
    x: NDArray[np.float64], hparams: NDArray[np.float64]
) -> NDArray[np.float64]:
    """Normalizes the rows of the matrix using the MACE priors.

    Args:
        x (np.ndarray): The array to normalize.
        hparams (np.ndarray): The prior parameters.

    Returns:
        np.ndarray: Normalized array
    """
    norm = (x + hparams).sum(axis=1)
    norm = np.exp(digamma(norm))
    return cast(
        NDArray[np.float64],
        np.divide(
            np.exp(digamma(x + hparams)),
            norm[:, np.newaxis],
            out=np.zeros_like(x),
            where=~np.isclose(norm[:, np.newaxis], np.zeros_like(norm[:, np.newaxis])),
        ),
    )


def decode_distribution(gold_label_marginals: pd.DataFrame) -> pd.DataFrame:
    """Decodes the distribution from marginals.

    Args:
        gold_label_marginals (pd.DataFrame): Gold label marginals.

    Returns:
        pd.DataFrame: Decoded distribution
    """

    return gold_label_marginals.div(gold_label_marginals.sum(axis=1), axis=0)


@attr.s
class MACE(BaseClassificationAggregator):
    r"""The **Multi-Annotator Competence Estimation** (MACE) model is a probabilistic model that associates each worker with a label probability distribution.
    A worker can be spamming on each task. If the worker is not spamming, they label a task correctly. If the worker is spamming, they answer according
    to their probability distribution.

    We assume that the correct label $T_i$ comes from a discrete uniform distribution. When a worker
    annotates a task, they are spamming with probability
    $\operatorname{Bernoulli}(1 - \theta_j)$. $S_{ij}$ specifies whether or not worker $j$ is spamming on instance $i$.
    Thus, if the worker is not spamming on the task, i.e. $S_{ij} = 0$, their response is the true label, i.e. $A_{ij} = T_i$.
    Otherwise, their response $A_{ij}$ is drawn from a multinomial distribution with parameter vector $\xi_j$.

    ![MACE latent label model](https://tlk.s3.yandex.net/crowd-kit/docs/mace_llm.png =500x630)

    The model can be enhanced by adding the Beta prior on $\theta_j$ and the Diriclet
    prior on $\xi_j$.

    The marginal data likelihood is maximized with the Expectation-Maximization algorithm:
    1. **E-step**. Performs `n_restarts` random restarts, and keeps the model with the best marginal data likelihood.
    2. **M-step**. Smooths parameters by adding a fixed value `smoothing` to the fractional counts before normalizing.
    3. **Variational M-step**. Employs Variational-Bayes (VB) training with symmetric Beta priors on $\theta_j$ and symmetric Dirichlet priors on the strategy parameters $\xi_j$.

    D. Hovy, T. Berg-Kirkpatrick, A. Vaswani and E. Hovy. Learning Whom to Trust with MACE.
    In *Proceedings of NAACL-HLT*, Atlanta, GA, USA (2013), 1120–1130.

    <https://aclanthology.org/N13-1132.pdf>

    Examples:
        >>> from crowdkit.aggregation import MACE
        >>> from crowdkit.datasets import load_dataset
        >>> df, gt = load_dataset('relevance-2')
        >>> mace = MACE()
        >>> result = mace.fit_predict(df)
    """

    n_restarts: int = attr.ib(default=10)
    """The number of optimization runs of the algorithms.
    The final parameters are those that gave the best log likelihood.
    If one run takes too long, this parameter can be set to 1."""

    n_iter: int = attr.ib(default=50)
    """The maximum number of EM iterations for each optimization run."""

    method: str = attr.ib(default="vb")
    """The method which is used for the M-step. Either 'vb' or 'em'.
    'vb' means optimization with Variational Bayes using priors.
    'em' means standard Expectation-Maximization algorithm."""

    smoothing: float = attr.ib(default=0.1)
    """The smoothing parameter for the normalization."""

    default_noise: float = attr.ib(default=0.5)
    """The default noise parameter for the initialization."""

    alpha: float = attr.ib(default=0.5)
    r"""The prior parameter for the Beta distribution on $\theta_j$."""

    beta: float = attr.ib(default=0.5)
    r"""The prior parameter for the Beta distribution on $\theta_j$."""

    random_state: int = attr.ib(default=0)
    """The state of the random number generator."""

    verbose: int = attr.ib(default=0)
    """The state of progress bar: 0 — no progress bar, 1 — only for restarts, 2 — for both restarts and optimization."""

    spamming_: NDArray[np.float64] = attr.ib(init=False)
    """The posterior distribution of workers' spamming states."""

    thetas_: NDArray[np.float64] = attr.ib(init=False)
    """The posterior distribution of workers' spamming labels."""

    theta_priors_: Optional[NDArray[np.float64]] = attr.ib(init=False)
    r"""The prior parameters for the Beta distribution on $\theta_j$."""

    strategy_priors_: Optional[NDArray[np.float64]] = attr.ib(init=False)
    r"""The prior parameters for the Diriclet distribution on $\xi_j$."""

    smoothing_: float = attr.ib(init=False)
    """The smoothing parameter."""

    probas_: Optional[pd.DataFrame] = attr.ib(init=False)
    """The probability distributions of task labels.
    The `pandas.DataFrame` data is indexed by `task` so that `result.loc[task, label]` is the probability that
    the `task` true label is equal to `label`. Each probability is in the range from 0 to 1,
    all task probabilities must sum up to 1."""

    def fit(self, data: pd.DataFrame) -> "MACE":
        """Fits the model to the training data.

        Args:
            data (DataFrame): The training dataset of workers' labeling results
                which is represented as the `pandas.DataFrame` data containing `task`, `worker`, and `label` columns.

        Returns:
            MACE: The fitted MACE model.
        """

        workers, worker_names = pd.factorize(data["worker"])
        labels, label_names = pd.factorize(data["label"])
        tasks, task_names = pd.factorize(data["task"])

        n_workers = len(worker_names)
        n_labels = len(label_names)

        self.smoothing_ = 0.01 / n_labels

        annotation = data.copy(deep=True)

        best_log_marginal_likelihood = -np.inf

        def restarts_progress() -> Iterator[int]:
            if self.verbose > 0:
                yield from trange(self.n_restarts, desc="Restarts")
            else:
                yield from range(self.n_restarts)

        for _ in restarts_progress():
            self._initialize(n_workers, n_labels)
            (
                log_marginal_likelihood,
                gold_label_marginals,
                strategy_expected_counts,
                knowing_expected_counts,
            ) = self._e_step(
                annotation,
                task_names,
                worker_names,
                label_names,
                tasks,
                workers,
                labels,
            )

            def iteration_progress() -> Tuple[Iterator[int], Optional["tqdm[int]"]]:
                if self.verbose > 1:
                    trange_ = trange(self.n_iter, desc="Iterations")
                    return iter(trange_), trange_
                else:
                    return iter(range(self.n_iter)), None

            iterator, pbar = iteration_progress()

            for _ in iterator:
                if self.method == "vb":
                    self._variational_m_step(
                        knowing_expected_counts, strategy_expected_counts
                    )
                else:
                    self._m_step(knowing_expected_counts, strategy_expected_counts)
                (
                    log_marginal_likelihood,
                    gold_label_marginals,
                    strategy_expected_counts,
                    knowing_expected_counts,
                ) = self._e_step(
                    annotation,
                    task_names,
                    worker_names,
                    label_names,
                    tasks,
                    workers,
                    labels,
                )
                if self.verbose > 1:
                    assert isinstance(pbar, tqdm)
                    pbar.set_postfix(
                        {"log_marginal_likelihood": round(log_marginal_likelihood, 5)}
                    )
            if log_marginal_likelihood > best_log_marginal_likelihood:
                best_log_marginal_likelihood = log_marginal_likelihood
                best_thetas = self.thetas_.copy()
                best_spamming = self.spamming_.copy()

        self.thetas_ = best_thetas
        self.spamming_ = best_spamming
        _, gold_label_marginals, _, _ = self._e_step(
            annotation, task_names, worker_names, label_names, tasks, workers, labels
        )

        self.probas_ = decode_distribution(gold_label_marginals)
        self.labels_ = self.probas_.idxmax(axis="columns")
        self.labels_.index.name = "task"

        return self

    def fit_predict(self, data: pd.DataFrame) -> "pd.Series[Any]":
        """
        Fits the model to the training data and returns the aggregated results.

        Args:
            data (DataFrame): The training dataset of workers' labeling results
                which is represented as the `pandas.DataFrame` data containing `task`, `worker`, and `label` columns.

        Returns:
            Series: Task labels. The `pandas.Series` data is indexed by `task`
                so that `labels.loc[task]` is the most likely true label of tasks.
        """
        self.fit(data)
        assert self.labels_ is not None, "no labels_"
        return self.labels_

    def fit_predict_proba(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Fits the model to the training data and returns probability distributions of labels for each task.

        Args:
            data (DataFrame): The training dataset of workers' labeling results
                which is represented as the `pandas.DataFrame` data containing `task`, `worker`, and `label` columns.

        Returns:
            DataFrame: Probability distributions of task labels.
                The `pandas.DataFrame` data is indexed by `task` so that `result.loc[task, label]` is the probability that the `task` true label is equal to `label`.
                Each probability is in the range from 0 to 1, all task probabilities must sum up to 1.
        """
        self.fit(data)
        assert self.probas_ is not None, "no probas_"
        return self.probas_

    def _initialize(self, n_workers: int, n_labels: int) -> None:
        """Initializes the MACE parameters.

        Args:
            n_workers (int): The number of workers.
            n_labels (int): The number of labels.

        Returns:
            None
        """

        self.spamming_ = sps.uniform(1, 1 + self.default_noise).rvs(
            size=(n_workers, 2),
            random_state=self.random_state,
        )
        self.thetas_ = sps.uniform(1, 1 + self.default_noise).rvs(
            size=(n_workers, n_labels), random_state=self.random_state
        )

        self.spamming_ = self.spamming_ / self.spamming_.sum(axis=1, keepdims=True)
        self.thetas_ = self.thetas_ / self.thetas_.sum(axis=1, keepdims=True)

        if self.method == "vb":
            self.theta_priors_ = np.empty((n_workers, 2))
            self.theta_priors_[:, 0] = self.alpha
            self.theta_priors_[:, 1] = self.beta

            self.strategy_priors_ = np.multiply(
                10.0, np.ones((n_workers, n_labels)), dtype=np.float64
            )

    def _e_step(
        self,
        annotation: pd.DataFrame,
        task_names: Union[List[Any], "pd.Index[Any]"],
        worker_names: Union[List[Any], "pd.Index[Any]"],
        label_names: Union[List[Any], "pd.Index[Any]"],
        tasks: NDArray[np.int64],
        workers: NDArray[np.int64],
        labels: NDArray[np.int64],
    ) -> Tuple[float, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """Performs E-step of the MACE algorithm.

        Args:
            annotation (DataFrame): The workers' labeling results. The `pandas.DataFrame` data contains `task`, `worker`, and `label` columns.
            task_names (List[Any]): The task names.
            worker_names (List[Any]): The workers' names.
            label_names (List[Any]): The label names.
            tasks (np.ndarray): The task IDs in the annotation.
            workers (np.ndarray): The workers' IDs in the annotation.
            labels (np.ndarray): The label IDs in the annotation.

        Returns:
            Tuple[float, pd.DataFrame, pd.DataFrame, pd.DataFrame]: The log marginal likelihood, gold label marginals,
                strategy expected counts, and knowing expected counts.
        """
        gold_label_marginals = pd.DataFrame(
            np.zeros((len(task_names), len(label_names))),
            index=task_names,
            columns=label_names,
        )

        knowing_expected_counts = pd.DataFrame(
            np.zeros((len(worker_names), 2)),
            index=worker_names,
            columns=["knowing_expected_count_0", "knowing_expected_count_1"],
        )

        for label_idx, label in enumerate(label_names):
            annotation["gold_marginal"] = self.spamming_[workers, 0] * self.thetas_[
                workers, labels
            ] + self.spamming_[workers, 1] * (label_idx == labels)
            gold_label_marginals[label] = annotation.groupby("task").prod(
                numeric_only=True
            )["gold_marginal"] / len(label_names)

        instance_marginals = gold_label_marginals.sum(axis=1)
        log_marginal_likelihood = np.log(instance_marginals + 1e-8).sum()

        annotation["strategy_marginal"] = 0.0
        for label in range(len(label_names)):
            annotation["strategy_marginal"] += gold_label_marginals.values[
                tasks, label
            ] / (
                self.spamming_[workers, 0] * self.thetas_[workers, labels]
                + self.spamming_[workers, 1] * (labels == label)
            )

        annotation["strategy_marginal"] = (
            annotation["strategy_marginal"]
            * self.spamming_[workers, 0]
            * self.thetas_[workers, labels]
        )

        annotation.set_index("task", inplace=True)
        annotation["instance_marginal"] = instance_marginals
        annotation.reset_index(inplace=True)

        annotation["strategy_marginal"] = (
            annotation["strategy_marginal"] / annotation["instance_marginal"]
        )

        strategy_expected_counts = (
            annotation.groupby(["worker", "label"])
            .sum(numeric_only=True)["strategy_marginal"]
            .unstack()
            .fillna(0.0)
        )

        knowing_expected_counts["knowing_expected_count_0"] = annotation.groupby(
            "worker"
        ).sum(numeric_only=True)["strategy_marginal"]

        annotation["knowing_expected_counts"] = (
            gold_label_marginals.values[tasks, labels].ravel()
            * self.spamming_[workers, 1]
            / (
                self.spamming_[workers, 0] * self.thetas_[workers, labels]
                + self.spamming_[workers, 1]
            )
        ) / instance_marginals.values[tasks]
        knowing_expected_counts["knowing_expected_count_1"] = annotation.groupby(
            "worker"
        ).sum(numeric_only=True)["knowing_expected_counts"]

        return (
            log_marginal_likelihood,
            gold_label_marginals,
            strategy_expected_counts,
            knowing_expected_counts,
        )

    def _m_step(
        self,
        knowing_expected_counts: pd.DataFrame,
        strategy_expected_counts: pd.DataFrame,
    ) -> None:
        """
        Performs M-step of the MACE algorithm.

        Args:
            knowing_expected_counts (DataFrame): The knowing expected counts.
            strategy_expected_counts (DataFrame): The strategy expected counts.

        Returns:
            None
        """
        self.spamming_ = normalize(knowing_expected_counts.values, self.smoothing_)
        self.thetas_ = normalize(strategy_expected_counts.values, self.smoothing_)

    def _variational_m_step(
        self,
        knowing_expected_counts: pd.DataFrame,
        strategy_expected_counts: pd.DataFrame,
    ) -> None:
        """
        Performs variational M-step of the MACE algorithm.

        Args:
            knowing_expected_counts (DataFrame): The knowing expected counts.
            strategy_expected_counts (DataFrame): The strategy expected counts.

        Returns:
            None
        """
        assert self.theta_priors_ is not None
        self.spamming_ = variational_normalize(
            knowing_expected_counts.values, self.theta_priors_
        )
        assert self.strategy_priors_ is not None
        self.thetas_ = variational_normalize(
            strategy_expected_counts.values, self.strategy_priors_
        )
