__all__ = ["SegmentationMajorityVote"]

from typing import Any, Optional

import attr
import numpy as np
import pandas as pd

from ..base import BaseImageSegmentationAggregator
from ..utils import add_skills_to_data, named_series_attrib


@attr.s
class SegmentationMajorityVote(BaseImageSegmentationAggregator):
    r"""The **Segmentation Majority Vote** algorithm chooses a pixel if and only if the pixel has "yes" votes
    from at least half of all workers.

    This method implements a straightforward approach to the image segmentation aggregation:
    it assumes that if a pixel is not inside the worker's segmentation, this vote is considered to be equal to 0.
    Otherwise, it is equal to 1. Then the `SegmentationEM` algorithm aggregates these categorical values
    for each pixel by the Majority Vote algorithm.

    The method also supports the weighted majority voting if the `skills` parameter is provided for the `fit` method.

    D. Jung-Lin Lee, A. Das Sarma and A. Parameswaran. Aggregating Crowdsourced Image Segmentations.
    *CEUR Workshop Proceedings. Vol. 2173*, (2018), 1-44.

    <https://ceur-ws.org/Vol-2173/paper10.pdf>

    Examples:
        >>> import numpy as np
        >>> import pandas as pd
        >>> from crowdkit.aggregation import SegmentationMajorityVote
        >>> df = pd.DataFrame(
        >>>     [
        >>>         ['t1', 'p1', np.array([[1, 0], [1, 1]])],
        >>>         ['t1', 'p2', np.array([[0, 1], [1, 1]])],
        >>>         ['t1', 'p3', np.array([[0, 1], [1, 1]])]
        >>>     ],
        >>>     columns=['task', 'worker', 'segmentation']
        >>> )
        >>> result = SegmentationMajorityVote().fit_predict(df)
    """

    default_skill: Optional[float] = attr.ib(default=None)
    """Default worker weight value."""

    on_missing_skill: str = attr.ib(default="error")
    """A value which specifies how to handle assignments performed by workers with an unknown skill.

    Possible values:
    * `error`: raises an exception if there is at least one assignment performed by a worker with an unknown skill;
    * `ignore`: drops assignments performed by workers with an unknown skill during prediction,
    raises an exception if there are no assignments with a known skill for any task;
    * `value`: the default value will be used if a skill is missing."""

    skills_: Optional["pd.Series[Any]"] = named_series_attrib(name="skill")
    """The workers' skills. The `pandas.Series` data is indexed by `worker` and has the corresponding worker skill."""

    def fit(
        self, data: pd.DataFrame, skills: Optional["pd.Series[Any]"] = None
    ) -> "SegmentationMajorityVote":
        """
        Fits the model to the training data.

        Args:
            data (DataFrame): The training dataset of workers' segmentations
                which is represented as the `pandas.DataFrame` data containing `task`, `worker`, and `segmentation` columns.

            skills (Series): The workers' skills. The `pandas.Series` data is indexed by `worker`
                and has the corresponding worker skill.

        Returns:
            SegmentationMajorityVote: self.
        """

        data = data[["task", "worker", "segmentation"]]

        if skills is None:
            data["skill"] = 1
        else:
            data = add_skills_to_data(
                data, skills, self.on_missing_skill, self.default_skill
            )

        data["pixel_scores"] = data.segmentation * data.skill
        group = data.groupby("task")

        self.segmentations_ = (
            2 * group.pixel_scores.apply(np.sum) - group.skill.apply(np.sum)
        ).apply(lambda x: x >= 0)
        return self

    def fit_predict(
        self, data: pd.DataFrame, skills: Optional["pd.Series[Any]"] = None
    ) -> "pd.Series[Any]":
        """
        Fits the model to the training data and returns the aggregated segmentations.

        Args:
            data (DataFrame): The training dataset of workers' segmentations
                which is represented as the `pandas.DataFrame` data containing `task`, `worker`, and `segmentation` columns.

            skills (Series): The workers' skills. The `pandas.Series` data is indexed by `worker`
                and has the corresponding worker skill.

        Returns:
            Series: Task segmentations. The `pandas.Series` data is indexed by `task`
                so that `segmentations.loc[task]` is the task aggregated segmentation.
        """

        return self.fit(data, skills).segmentations_
