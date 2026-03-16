__all__ = [
    "accuracy_on_aggregates",
]

from typing import Any, Optional, Union

import pandas as pd

from crowdkit.aggregation import MajorityVote
from crowdkit.aggregation.base import BaseClassificationAggregator
from crowdkit.aggregation.utils import get_accuracy


def accuracy_on_aggregates(
    answers: pd.DataFrame,
    aggregator: Optional[BaseClassificationAggregator] = MajorityVote(),
    aggregates: Optional["pd.Series[Any]"] = None,
    by: Optional[str] = None,
) -> Union[float, "pd.Series[Any]"]:
    """
    Accuracy on aggregates: a fraction of worker's answers that match the aggregated one.

    Args:
        answers: a data frame containing `task`, `worker` and `label` columns.
        aggregator: aggregation algorithm. default: MajorityVote
        aggregates: aggregated answers for provided tasks.
        by: if set, returns accuracies for every worker in provided data frame. Otherwise,
            returns an average accuracy of all workers.

        Returns:
            Union[float, pd.Series]
    """
    if aggregates is None and aggregator is None:
        raise AssertionError("One of aggregator or aggregates should be not None")

    if aggregates is None:
        aggregates = aggregator.fit_predict(answers)  # type: ignore

    return get_accuracy(answers, aggregates, by=by)
