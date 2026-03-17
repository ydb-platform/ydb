__all__ = [
    "entropy_threshold",
]

import warnings
from typing import Any, Optional, cast

import numpy as np
import pandas as pd

from ..metrics.data import uncertainty


def entropy_threshold(
    answers: pd.DataFrame,
    workers_skills: Optional["pd.Series[Any]"] = None,
    percentile: int = 10,
    min_answers: int = 2,
) -> pd.DataFrame:
    """Entropy thresholding postprocessing: filters out all answers by workers,
    whos' entropy (uncertanity) of answers is below specified percentile.

    This heuristic detects answers of workers that answer the same way too often, e.g. when "speed-running" by only
    clicking one button.

    Args:
        answers (pandas.DataFrame): A data frame containing `task`, `worker` and `label` columns.
        workers_skills (Optional[pandas.Series]): workers skills e.g. golden set skills.
        percentile (int): threshold entropy percentile from 0 to 100. Default: 10.
        min_answers (int): worker can be filtered out if he left at least that many answers.

    Returns:
        pd.DataFrame

    Examples:
        Fraudent worker always answers the same and gets filtered out.

        >>> answers = pd.DataFrame.from_records(
        >>>     [
        >>>         {'task': '1', 'worker': 'A', 'label': frozenset(['dog'])},
        >>>         {'task': '1', 'worker': 'B', 'label': frozenset(['cat'])},
        >>>         {'task': '2', 'worker': 'A', 'label': frozenset(['cat'])},
        >>>         {'task': '2', 'worker': 'B', 'label': frozenset(['cat'])},
        >>>         {'task': '3', 'worker': 'A', 'label': frozenset(['dog'])},
        >>>         {'task': '3', 'worker': 'B', 'label': frozenset(['cat'])},
        >>>     ]
        >>> )
        >>> entropy_threshold(answers)
          task worker  label
        0    1         A  (dog)
        2    2         A  (cat)
        4    3         A  (dog)

    Args:
        answers (DataFrame): Workers' labeling results.
            A pandas.DataFrame containing `task`, `worker` and `label` columns.
        workers_skills (typing.Optional[pandas.core.series.Series]): workers' skills.
            A pandas.Series index by workers and holding corresponding worker's skill
    """

    answers_per_worker = answers.groupby("worker")["label"].count()
    answers_per_worker = answers_per_worker[answers_per_worker >= min_answers]

    answers_for_filtration = answers[answers.worker.isin(answers_per_worker.index)]

    uncertainties = cast(
        "pd.Series[Any]",
        uncertainty(
            answers_for_filtration,
            workers_skills,
            compute_by="worker",
            aggregate=False,
        ),
    )

    cutoff = np.percentile(uncertainties, percentile)

    removed_workers = uncertainties[uncertainties <= cutoff].index

    filtered_answers = answers.copy(deep=False)
    filtered_answers = filtered_answers[
        ~filtered_answers["worker"].isin(removed_workers)
    ]

    if filtered_answers.shape[0] <= answers.shape[0] // 2:
        warnings.warn(
            "Removed >= 1/2 of answers with entropy_threshold. This might lead to poor annotation quality. "
            "Try decreasing percentile or min_answers."
        )

    return filtered_answers
