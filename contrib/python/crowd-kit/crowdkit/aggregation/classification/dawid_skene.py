__all__ = ["DawidSkene", "OneCoinDawidSkene"]

from typing import Any, List, Literal, Optional, Tuple, cast

import attr
import numpy as np
import pandas as pd

from ..base import BaseClassificationAggregator
from ..utils import get_most_probable_labels, named_series_attrib
from .majority_vote import MajorityVote

_EPS = np.float_power(10, -10)


@attr.s
class DawidSkene(BaseClassificationAggregator):
    r"""The **Dawid-Skene** aggregation model is a probabilistic model that parametrizes the expertise level of workers with confusion matrices.

    Let $e^w$ be a worker confusion (error) matrix of size $K \times K$ in case of the $K$ class classification,
    $p$ be a vector of prior class probabilities, $z_j$ be a true task label, and $y^w_j$ be a worker
    response to the task $j$. The relationship between these parameters is represented by the following latent
    label model.

    ![Dawid-Skene latent label model](https://tlk.s3.yandex.net/crowd-kit/docs/ds_llm.png)

    Here the prior true label probability is $\operatorname{Pr}(z_j = c) = p[c]$, and the probability distribution
    of the worker responses with the true label $c$ is represented by the corresponding column of the error matrix:
    $\operatorname{Pr}(y_j^w = k | z_j = c) = e^w[k, c]$.

    Parameters $p$, $e^w$, and latent variables $z$ are optimized with the Expectation-Maximization algorithm:
    1. **E-step**. Estimates the true task label probabilities using the specified workers' responses,
        the prior label probabilities, and the workers' error probability matrix.
    2. **M-step**. Estimates the workers' error probability matrix using the specified workers' responses and the true task label probabilities.

    A. Philip Dawid and Allan M. Skene. Maximum Likelihood Estimation of Observer Error-Rates Using the EM Algorithm.
    *Journal of the Royal Statistical Society. Series C (Applied Statistics), Vol. 28*, 1 (1979), 20–28.

    <https://doi.org/10.2307/2346806>

    Examples:
        >>> from crowdkit.aggregation import DawidSkene
        >>> from crowdkit.datasets import load_dataset
        >>> df, gt = load_dataset('relevance-2')
        >>> ds = DawidSkene(100)
        >>> result = ds.fit_predict(df)

    We can use the golden labels to correct the probability distributions of task labels
    by the true labels during the iterative process:

    Examples:
        >>> from crowdkit.aggregation import DawidSkene
        >>> from crowdkit.datasets import load_dataset
        >>> df, gt = load_dataset('relevance-2')
        >>> true_labels = gt[:1000]  # use the first 100 true labels
        >>> ds = DawidSkene(100)
        >>> result = ds.fit_predict(df, true_labels)

    We can also provide the workers' initial error matrices, which come from historical performance data.
    There two strategies to initialize the workers' error matrices: `assign` and `addition`.
    Here we create simple error matrices with two workers:

    ```
                  0  1
    worker label
    w851   0      9  1
           1      1  9
    w6991  0      9  1
           1      1  9
    ```

    Note:
        1. Make sure the error matrix is indexed by `worker` and `label`
        with columns for every `label_id` appeared in `data`.
        You can use the `pandas.MultiIndex` to create such an index, see the example below.

        2. When using `addition` strategy, the error matrix should contain the history **count**(not probability) that `worker` produces `observed_label`,
        given that the task true label is `true_label`.

    When we use the `addition` strategy, partial error matrices are acceptable,
    which will be added to the workers' priori error matrices estimated from the given data.

    Examples:
        >>> import pandas as pd
        >>> from crowdkit.aggregation import DawidSkene
        >>> from crowdkit.datasets import load_dataset
        >>> df, gt = load_dataset('relevance-2')
        >>> error_matrix_index = pd.MultiIndex.from_arrays([['w851', 'w851', 'w6991', 'w6991'], [0, 1, 0, 1]], names=['worker', 'label'])
        >>> initial_error = pd.DataFrame(
        ...     data=[[9, 1], [1, 9], [9, 1], [1, 9]],
        ...     index=error_matrix_index,
        ...     columns=[0, 1],
        ... )
        >>> ds = DawidSkene(100, initial_error_strategy='addition')
        >>> result = ds.fit_predict(df, initial_error=initial_error)

    We can also use the `assign` strategy to initialize the workers' error matrices.
    But in this case, the `initial_error` **must** contain all the workers' error matrices in the data.
    """

    n_iter: int = attr.ib(default=100)
    """The maximum number of EM iterations."""

    tol: float = attr.ib(default=1e-5)
    """The tolerance stopping criterion for iterative methods with a variable number of steps.
    The algorithm converges when the loss change is less than the `tol` parameter."""

    initial_error_strategy: Optional[Literal["assign", "addition"]] = attr.ib(
        default=None
    )
    """The strategy for initializing the workers' error matrices.
    The `assign` strategy assigns the initial error matrix to the workers' error matrices;
    the `addition` strategy adds the initial error matrix with the workers' priori error matrices estimated
    from the given data. If `None`, the initial error matrix is not used.

    Note:
        - `addition` strategy
            - The initial error matrix can be partial, not all workers' error matrices need to be provided.
            - The initial error matrix values should be the history **count** that
              `worker` produces `observed_label`, given that the task true label is `true_label`.
              For example(count values error matrix):

                                  0  1
                    worker label
                    w851   0      9  1
                           1      1  9
                    w6991  0      9  1
                           1      1  9

        - `assign` strategy
            - The initial error matrix must contain all the workers' error matrices in the data.
            - The initial error matrix values could be the probability or count that
              `worker` produces `observed_label`, given that the task true label is `true_label`.
              When given probability error matrix, the values should sum up to 1 for each worker at each `true_label` column.
              For example(probability values error matrix):

                                  0    1
                    worker label
                    w851   0      0.9  0.1
                           1      0.1  0.9
                    w6991  0      0.9  0.1
                           1      0.1  0.9
                    ...
    """

    probas_: Optional[pd.DataFrame] = attr.ib(init=False)
    """The probability distributions of task labels.
    The `pandas.Series` data is indexed by `task` so that `labels.loc[task]` is the most likely true label of tasks."""

    priors_: Optional["pd.Series[Any]"] = named_series_attrib(name="prior")
    """The prior label distribution.
    The `pandas.DataFrame` data is indexed by `task` so that `result.loc[task, label]` is the probability that
    the `task` true label is equal to `label`. Each probability is in the range from 0 to 1, all task probabilities
    must sum up to 1."""

    errors_: Optional[pd.DataFrame] = attr.ib(init=False)
    """The workers' error matrices. The `pandas.DataFrame` data is indexed by `worker` and `label` with a column
    for every `label_id` found in `data` so that `result.loc[worker, observed_label, true_label]` is the probability
    that `worker` produces `observed_label`, given that the task true label is `true_label`."""

    loss_history_: List[float] = attr.ib(init=False)
    """ A list of loss values during training."""

    @staticmethod
    def _m_step(
        data: pd.DataFrame,
        probas: pd.DataFrame,
        initial_error: Optional[pd.DataFrame] = None,
        initial_error_strategy: Optional[Literal["assign", "addition"]] = None,
    ) -> pd.DataFrame:
        """Performs M-step of the Dawid-Skene algorithm.

        Estimates the workers' error probability matrix using the specified workers' responses and the true task label probabilities.
        """
        joined = data.join(probas, on="task")
        joined.drop(columns=["task"], inplace=True)
        errors = joined.groupby(["worker", "label"], sort=False).sum()
        # Apply the initial error matrix
        errors = initial_error_apply(errors, initial_error, initial_error_strategy)
        # Normalize the error matrix
        errors.clip(lower=_EPS, inplace=True)
        errors /= errors.groupby("worker", sort=False).sum()

        return errors

    @staticmethod
    def _e_step(
        data: pd.DataFrame, priors: "pd.Series[Any]", errors: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Performs E-step of the Dawid-Skene algorithm.

        Estimates the true task label probabilities using the specified workers' responses,
        the prior label probabilities, and the workers' error probability matrix.
        """
        # We have to multiply lots of probabilities and such products are known to converge
        # to zero exponentially fast. To avoid floating-point precision problems we work with
        # logs of original values
        joined = data.join(np.log2(errors), on=["worker", "label"])  # type: ignore
        joined.drop(columns=["worker", "label"], inplace=True)

        priors.clip(lower=_EPS, inplace=True)
        log_likelihoods = np.log2(priors) + joined.groupby("task", sort=False).sum()
        log_likelihoods.rename_axis("label", axis=1, inplace=True)

        # Exponentiating log_likelihoods 'as is' may still get us beyond our precision.
        # So we shift every row of log_likelihoods by a constant (which is equivalent to
        # multiplying likelihoods rows by a constant) so that max log_likelihood in each
        # row is equal to 0. This trick ensures proper scaling after exponentiating and
        # does not affect the result of E-step
        scaled_likelihoods = np.exp2(
            log_likelihoods.sub(log_likelihoods.max(axis=1), axis=0)
        )
        scaled_likelihoods = scaled_likelihoods.div(
            scaled_likelihoods.sum(axis=1), axis=0
        )
        # Convert columns types to label type
        scaled_likelihoods.columns = pd.Index(
            scaled_likelihoods.columns, name="label", dtype=data.label.dtype
        )
        return cast(pd.DataFrame, scaled_likelihoods)

    def _evidence_lower_bound(
        self,
        data: pd.DataFrame,
        probas: pd.DataFrame,
        priors: "pd.Series[Any]",
        errors: pd.DataFrame,
    ) -> float:
        # calculate joint probability log-likelihood expectation over probas
        joined = data.join(np.log(errors), on=["worker", "label"])  # type: ignore

        # escape boolean index/column names to prevent confusion between indexing by boolean array and iterable of names
        joined = joined.rename(columns={True: "True", False: "False"}, copy=False)
        priors = priors.rename(index={True: "True", False: "False"}, copy=False)
        priors.clip(lower=_EPS, inplace=True)

        joined.loc[:, priors.index] = joined.loc[:, priors.index].add(np.log(priors))

        joined.set_index(["task", "worker"], inplace=True)
        joint_expectation = (
            (probas.rename(columns={True: "True", False: "False"}) * joined).sum().sum()
        )
        probas.clip(lower=_EPS, inplace=True)
        entropy = -(np.log(probas) * probas).sum().sum()
        return float(joint_expectation + entropy)

    def fit(
        self,
        data: pd.DataFrame,
        true_labels: Optional["pd.Series[Any]"] = None,
        initial_error: Optional[pd.DataFrame] = None,
    ) -> "DawidSkene":
        """Fits the model to the training data with the EM algorithm.

        Args:
            data (DataFrame): The training dataset of workers' labeling results
                which is represented as the `pandas.DataFrame` data containing `task`, `worker`, and `label` columns.
            true_labels (Series): The ground truth labels of tasks.
                The `pandas.Series` data is indexed by `task`  so that `labels.loc[task]` is the task ground truth label.
                When provided, the model will correct the probability distributions of task labels by the true labels
                during the iterative process.
            initial_error (DataFrame): The workers' initial error matrices, comes from historical performance data.
                The `pandas.DataFrame` data is indexed by `worker` and `label` with a column
                for every `label_id` found in `data` so that `result.loc[worker, observed_label, true_label]` is the
                history **count** that `worker` produces `observed_label`, given that the task true label is `true_label`.
                When the `initial_error_strategy` is `assign`, the values in the error matrix can be the probability too.
                Check the examples in the class docstring for more details.

        Returns:
            DawidSkene: self.
        """

        data = data[["task", "worker", "label"]]

        # Early exit
        if not data.size:
            self.probas_ = pd.DataFrame()
            self.priors_ = pd.Series(dtype=float)
            self.errors_ = pd.DataFrame()
            self.labels_ = pd.Series(dtype=float)
            return self

        # Initialization
        probas = MajorityVote().fit_predict_proba(data)
        # correct the probas by true_labels
        if true_labels is not None:
            probas = self._correct_probas_with_golden(probas, true_labels)
        priors = probas.mean()
        errors = self._m_step(data, probas, initial_error, self.initial_error_strategy)
        loss = -np.inf
        self.loss_history_ = []

        # Updating proba and errors n_iter times
        for _ in range(self.n_iter):
            probas = self._e_step(data, priors, errors)
            # correct the probas by true_labels
            if true_labels is not None:
                probas = self._correct_probas_with_golden(probas, true_labels)
            priors = probas.mean()
            errors = self._m_step(data, probas)
            new_loss = self._evidence_lower_bound(data, probas, priors, errors) / len(
                data
            )
            self.loss_history_.append(new_loss)

            if new_loss - loss < self.tol:
                break
            loss = new_loss

        probas.columns = pd.Index(
            probas.columns, name="label", dtype=probas.columns.dtype
        )
        # Saving results
        self.probas_ = probas
        self.priors_ = priors
        self.errors_ = errors
        self.labels_ = get_most_probable_labels(probas)

        return self

    def fit_predict_proba(
        self,
        data: pd.DataFrame,
        true_labels: Optional["pd.Series[Any]"] = None,
        initial_error: Optional[pd.DataFrame] = None,
    ) -> pd.DataFrame:
        """Fits the model to the training data and returns probability distributions of labels for each task.

        Args:
            data (DataFrame): The training dataset of workers' labeling results
                which is represented as the `pandas.DataFrame` data containing `task`, `worker`, and `label` columns.
            true_labels (Series): The ground truth labels of tasks.
                The `pandas.Series` data is indexed by `task`  so that `labels.loc[task]` is the task ground truth label.
                When provided, the model will correct the probability distributions of task labels by the true labels
                during the iterative process.
            initial_error (DataFrame): The workers' initial error matrices, comes from historical performance data.
                The `pandas.DataFrame` data is indexed by `worker` and `label` with a column
                for every `label_id` found in `data` so that `result.loc[worker, observed_label, true_label]` is the
                history **count** that `worker` produces `observed_label`, given that the task true label is `true_label`.
                When the `initial_error_strategy` is `assign`, the values in the error matrix can be the probability too.
                Check the examples in the class docstring for more details.

        Returns:
            DataFrame: Probability distributions of task labels.
                The `pandas.DataFrame` data is indexed by `task` so that `result.loc[task, label]` is the probability that the `task` true label is equal to `label`.
                Each probability is in the range from 0 to 1, all task probabilities must sum up to 1.
        """

        self.fit(data, true_labels, initial_error)
        assert self.probas_ is not None, "no probas_"
        return self.probas_

    def fit_predict(
        self,
        data: pd.DataFrame,
        true_labels: Optional["pd.Series[Any]"] = None,
        initial_error: Optional[pd.DataFrame] = None,
    ) -> "pd.Series[Any]":
        """Fits the model to the training data and returns the aggregated results.

        Args:
            data (DataFrame): The training dataset of workers' labeling results
                which is represented as the `pandas.DataFrame` data containing `task`, `worker`, and `label` columns.
            true_labels (Series): The ground truth labels of tasks.
                The `pandas.Series` data is indexed by `task`  so that `labels.loc[task]` is the task ground truth label.
                When provided, the model will correct the probability distributions of task labels by the true labels
                during the iterative process.
            initial_error (DataFrame): The workers' initial error matrices, comes from historical performance data.
                The `pandas.DataFrame` data is indexed by `worker` and `label` with a column
                for every `label_id` found in `data` so that `result.loc[worker, observed_label, true_label]` is the
                history **count** that `worker` produces `observed_label`, given that the task true label is `true_label`.
                When the `initial_error_strategy` is `assign`, the values in the error matrix can be the probability too.
                Check the examples in the class docstring for more details.

        Returns:
            Series: Task labels. The `pandas.Series` data is indexed by `task` so that `labels.loc[task]` is the most likely true label of tasks.
        """

        self.fit(data, true_labels, initial_error)
        assert self.labels_ is not None, "no labels_"
        return self.labels_

    @staticmethod
    def _correct_probas_with_golden(
        probas: pd.DataFrame, true_labels: "pd.Series[Any]"
    ) -> pd.DataFrame:
        """
        Correct the probas by `true_labels`
        """
        corrected_probas = probas

        # Iterate over the unique labels present in true_labels
        for label in true_labels.unique():
            # Find the indices in both probas and true_labels where the true label is the current label
            indices = true_labels[true_labels == label].index.intersection(probas.index)
            # Set the corresponding probabilities to 1 for the correct label and 0 for others
            corrected_probas.loc[indices] = (
                0  # Set all columns to 0 for the given indices
            )
            corrected_probas.loc[indices, label] = (
                1  # Set the correct label column to 1
            )

        return corrected_probas


def initial_error_apply(
    errors: pd.DataFrame,
    initial_error: Optional[pd.DataFrame],
    initial_error_strategy: Optional[Literal["assign", "addition"]],
) -> pd.DataFrame:
    if initial_error_strategy is None or initial_error is None:
        return errors
    # check the index names of initial_error
    if initial_error.index.names != errors.index.names:
        raise ValueError(
            f"The index of initial_error must be: {errors.index.names},"
            f"but got: {initial_error.index.names}"
        )
    if initial_error_strategy == "assign":
        # check the completeness of initial_error: all the workers in data should be in initial_error
        mask = errors.index.isin(initial_error.index)
        if not mask.all():
            not_found_workers = errors.index[~mask].get_level_values("worker").unique()
            raise ValueError(
                f"All the workers in data should be in initial_error: "
                f"Can not find {len(not_found_workers)} workers' error matrix in initial_error"
            )
        # if the values in initial_error are probability, check the sum of each worker's error matrix
        if (initial_error <= 1.0).all().all() and not np.allclose(
            initial_error.groupby("worker", sort=False).sum(), 1.0
        ):
            raise ValueError(
                "The sum of each worker's error matrix in initial_error should be 1.0"
            )
        errors = initial_error
    elif initial_error_strategy == "addition":
        errors = errors.add(initial_error, axis="index", fill_value=0.0)
    else:
        raise ValueError(
            f"Invalid initial_error_strategy: {initial_error_strategy},"
            f"should be 'assign' or 'addition'"
        )
    return errors


@attr.s
class OneCoinDawidSkene(DawidSkene):
    r"""The **one-coin Dawid-Skene** aggregation model works exactly the same as the original Dawid-Skene model
    based on the EM algorithm, except for calculating the workers' errors
    at the M-step of the algorithm.

    For the one-coin model, a worker confusion (error) matrix is parameterized by a single parameter $s_w$:

    $e^w_{j,z_j}  = \begin{cases}
        s_{w} & y^w_j = z_j \\
        \frac{1 - s_{w}}{K - 1} & y^w_j \neq z_j
    \end{cases}$,

    where $e^w$ is a worker confusion (error) matrix of size $K \times K$ in case of the $K$ class classification,
    $z_j$ be a true task label, $y^w_j$ is a worker response to the task $j$, and $s_w$ is a worker skill (accuracy).

    In other words, the worker $w$ uses a single coin flip to decide their assignment. No matter what the true label is,
    the worker has the $s_w$ probability to assign the correct label, and
    has the $1 − s_w$ probability to randomly assign an incorrect label. For the one-coin model, it
    suffices to estimate $s_w$ for every worker $w$ and estimate $y^w_j$ for every task $j$. Because of its
    simplicity, the one-coin model is easier to estimate and enjoys better convergence properties.

    Parameters $p$, $e^w$, and latent variables $z$ are optimized with the Expectation-Maximization algorithm:
    1. **E-step**. Estimates the true task label probabilities using the specified workers' responses,
    the prior label probabilities, and the workers' error probability matrix.
    2. **M-step**. Calculates a worker skill as their accuracy according to the label probability.
    Then estimates the workers' error probability matrix by assigning user skills to error matrix row by row.

    Y. Zhang, X. Chen, D. Zhou, and M. I. Jordan. Spectral methods meet EM: A provably optimal algorithm for crowdsourcing.
    *Journal of Machine Learning Research. Vol. 17*, (2016), 1-44.

    <https://doi.org/10.48550/arXiv.1406.3824>

    Examples:
        >>> from crowdkit.aggregation import OneCoinDawidSkene
        >>> from crowdkit.datasets import load_dataset
        >>> df, gt = load_dataset('relevance-2')
        >>> hds = OneCoinDawidSkene(100)
        >>> result = hds.fit_predict(df)
    """

    @staticmethod
    def _assign_skills(
        row: "pd.Series[Any]", skills: "pd.Series[Any]"
    ) -> "pd.Series[Any]":
        """
        Assigns user skills to error matrix row by row.
        """
        num_categories = len(row)
        for column_name, _ in row.items():
            if column_name == cast(Tuple[Any, Any], row.name)[1]:
                row[column_name] = skills.loc[cast(Tuple[Any, Any], row.name)[0]]
            else:
                row[column_name] = (
                    1 - skills.loc[cast(Tuple[Any, Any], row.name)[0]]
                ) / (num_categories - 1)
        return row

    @staticmethod
    def _process_skills_to_errors(
        data: pd.DataFrame, probas: pd.DataFrame, skills: "pd.Series[Any]"
    ) -> pd.DataFrame:
        errors = DawidSkene._m_step(data, probas)

        errors = errors.apply(OneCoinDawidSkene._assign_skills, args=(skills,), axis=1)
        errors.clip(lower=_EPS, upper=1 - _EPS, inplace=True)

        return errors

    @staticmethod
    def _m_step(data: pd.DataFrame, probas: pd.DataFrame) -> "pd.Series[Any]":  # type: ignore
        """Performs M-step of Homogeneous Dawid-Skene algorithm.

        Calculates a worker skill as their accuracy according to the label probability.
        """
        skilled_data = data.copy()
        idx_cols, cols = pd.factorize(data["label"])
        idx_rows, rows = pd.factorize(data["task"])
        skilled_data["skill"] = (
            probas.reindex(rows, axis=0)
            .reindex(cols, axis=1)
            .to_numpy()[idx_rows, idx_cols]
        )
        skills = skilled_data.groupby(["worker"], sort=False)["skill"].mean()
        return skills

    def fit(self, data: pd.DataFrame) -> "OneCoinDawidSkene":  # type: ignore[override]
        """Fits the model to the training data with the EM algorithm.

        Args:
            data (DataFrame): The training dataset of workers' labeling results
                which is represented as the `pandas.DataFrame` data containing `task`, `worker`, and `label` columns.
        Returns:
            DawidSkene: self.
        """

        data = data[["task", "worker", "label"]]

        # Early exit
        if not data.size:
            self.probas_ = pd.DataFrame()
            self.priors_ = pd.Series(dtype=float)
            self.errors_ = pd.DataFrame()
            self.labels_ = pd.Series(dtype=float)
            return self

        # Initialization
        probas = MajorityVote().fit_predict_proba(data)
        priors = probas.mean()
        skills = self._m_step(data, probas)
        errors = self._process_skills_to_errors(data, probas, skills)
        loss = -np.inf
        self.loss_history_ = []

        # Updating proba and errors n_iter times
        for _ in range(self.n_iter):
            probas = self._e_step(data, priors, errors)
            priors = probas.mean()
            skills = self._m_step(data, probas)
            errors = self._process_skills_to_errors(data, probas, skills)
            new_loss = self._evidence_lower_bound(data, probas, priors, errors) / len(
                data
            )
            self.loss_history_.append(new_loss)

            if new_loss - loss < self.tol:
                break
            loss = new_loss

        # Saving results
        self.probas_ = probas
        self.priors_ = priors
        self.skills_ = skills
        self.errors_ = errors
        self.labels_ = get_most_probable_labels(probas)

        return self

    def fit_predict_proba(self, data: pd.DataFrame) -> pd.DataFrame:  # type: ignore[override]
        """Fits the model to the training data and returns probability distributions of labels for each task.
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

    def fit_predict(self, data: pd.DataFrame) -> "pd.Series[Any]":  # type: ignore[override]
        """Fits the model to the training data and returns the aggregated results.
        Args:
            data (DataFrame): The training dataset of workers' labeling results
                which is represented as the `pandas.DataFrame` data containing `task`, `worker`, and `label` columns.
        Returns:
            Series: Task labels. The `pandas.Series` data is indexed by `task` so that `labels.loc[task]` is the most likely true label of tasks.
        """

        self.fit(data)
        assert self.labels_ is not None, "no labels_"
        return self.labels_
