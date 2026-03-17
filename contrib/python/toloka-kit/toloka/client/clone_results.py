__all__=['CloneResults']
from typing import NamedTuple, List

from .pool import Pool
from .project import Project
from .training import Training


class CloneResults(NamedTuple):
    """The result of a project deep cloning.

    `CloneResults` is returned by the [clone_project](toloka.client.TolokaClient.clone_project.md) method.

    Attributes:
        project: The cloned project.
        pools: A list of cloned pools.
        trainings: A list of cloned trainings.

    Example:
        >>> result = toloka_client.clone_project(project_id='92694')
        >>> print('Project ID:', result.project.id)
        >>> print('Pools:', len(result.pools))
        >>> print('Trainings:', len(result.trainings))
        ...
    """

    project: Project
    pools: List[Pool]
    trainings: List[Training]
