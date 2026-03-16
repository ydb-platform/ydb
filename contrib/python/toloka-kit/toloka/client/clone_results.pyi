__all__ = [
    'CloneResults',
]
import toloka.client.pool
import toloka.client.project
import toloka.client.training
import typing


class CloneResults(tuple):
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

    @staticmethod
    def __new__(
        _cls,
        project: toloka.client.project.Project,
        pools: typing.List[toloka.client.pool.Pool],
        trainings: typing.List[toloka.client.training.Training]
    ):
        """Create new instance of CloneResults(project, pools, trainings)
        """
        ...

    project: toloka.client.project.Project
    pools: typing.List[toloka.client.pool.Pool]
    trainings: typing.List[toloka.client.training.Training]
