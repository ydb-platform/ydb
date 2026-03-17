__all__ = ["TextRASA"]

from typing import Any, Callable, List

import numpy.typing as npt
import pandas as pd

from ..base import BaseTextsAggregator
from ..embeddings.rasa import RASA


class TextRASA(BaseTextsAggregator):
    """RASA on text embeddings.

    Given a sentence encoder, encodes texts provided by workers and runs the RASA algorithm for embedding
    aggregation.

    Args:
        encoder: A callable that takes a text and returns a NumPy array containing the corresponding embedding.
        n_iter: A number of RASA iterations.
        alpha: Confidence level of chi-squared distribution quantiles in beta parameter formula.

    Examples:
        We suggest to use sentence encoders provided by [Sentence Transformers](https://www.sbert.net).
        >>> from crowdkit.datasets import load_dataset
        >>> from crowdkit.aggregation import TextRASA
        >>> from sentence_transformers import SentenceTransformer
        >>> encoder = SentenceTransformer('all-mpnet-base-v2')
        >>> hrrasa = TextRASA(encoder=encoder.encode)
        >>> df, gt = load_dataset('crowdspeech-test-clean')
        >>> df['text'] = df['text'].str.lower()
        >>> result = hrrasa.fit_predict(df)
    """

    # texts_

    @property
    def loss_history_(self) -> List[float]:
        return self._rasa.loss_history_

    def __init__(
        self,
        encoder: Callable[[str], npt.NDArray[Any]],
        n_iter: int = 100,
        tol: float = 1e-5,
        alpha: float = 0.05,
    ):
        super().__init__()
        self.encoder = encoder
        self._rasa = RASA(n_iter, tol, alpha)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._rasa, name)

    def fit(  # type: ignore
        self, data: pd.DataFrame, true_objects: "pd.Series[Any]"
    ) -> "TextRASA":
        """Fit the model.
        Args:
            data (DataFrame): Workers' outputs.
                A pandas.DataFrame containing `task`, `worker` and `output` columns.
            true_objects (Series): Tasks' ground truth labels.
                A pandas.Series indexed by `task` such that `labels.loc[task]`
                is the tasks's ground truth label.

        Returns:
            TextRASA: self.
        """

        self._rasa.fit(self._encode_data(data), self._encode_true_objects(true_objects))
        return self

    def fit_predict_scores(
        self, data: pd.DataFrame, true_objects: "pd.Series[Any]"
    ) -> pd.DataFrame:
        """Fit the model and return scores.

        Args:
            data (DataFrame): Workers' responses.
                A pandas.DataFrame containing `task`, `worker` and `output` columns.
            true_objects (Series): Tasks' ground truth texts.
                A pandas.Series indexed by `task` such that `labels.loc[task]`
                is the tasks's ground truth text.

        Returns:
            DataFrame: Tasks' label scores.
                A pandas.DataFrame indexed by `task` such that `result.loc[task, label]`
                is the score of `label` for `task`.
        """

        return self._rasa.fit_predict_scores(
            self._encode_data(data), self._encode_true_objects(true_objects)
        )

    def fit_predict(  # type: ignore
        self, data: pd.DataFrame, true_objects: "pd.Series[Any]"
    ) -> "pd.Series[Any]":
        """Fit the model and return aggregated texts.

        Args:
            data (DataFrame): Workers' responses.
                A pandas.DataFrame containing `task`, `worker` and `output` columns.
            true_objects (Series): Tasks' ground truth texts.
                A pandas.Series indexed by `task` such that `labels.loc[task]`
                is the tasks's ground truth text.

        Returns:
            Series: Tasks' texts.
                A pandas.Series indexed by `task` such that `result.loc[task, text]`
                is the task's text.
        """

        rasa_results = self._rasa.fit_predict(
            self._encode_data(data), self._encode_true_objects(true_objects)
        )
        self.texts_ = (
            rasa_results.reset_index()[["task", "output"]]
            .rename(columns={"output": "text"})
            .set_index("task")["text"]
        )
        return self.texts_

    def _encode_data(self, data: pd.DataFrame) -> pd.DataFrame:
        data = data[["task", "worker", "text"]].rename(columns={"text": "output"})
        data["embedding"] = data.output.apply(self.encoder)  # type: ignore
        return data

    def _encode_true_objects(self, true_objects: "pd.Series[Any]") -> "pd.Series[Any]":
        return true_objects and true_objects.apply(self.encoder)
