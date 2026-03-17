__all__ = [
    'DurationUnit',
    'UserRestriction',
    'AllProjectsUserRestriction',
    'PoolUserRestriction',
    'ProjectUserRestriction',
    'SystemUserRestriction'
]
import datetime
from enum import unique, Enum

from .primitives.base import BaseTolokaObject
from ..util._codegen import attribute
from ..util._docstrings import inherit_docstrings
from ..util._extendable_enum import ExtendableStrEnum


@unique
class DurationUnit(Enum):
    MINUTES = 'MINUTES'
    HOURS = 'HOURS'
    DAYS = 'DAYS'
    PERMANENT = 'PERMANENT'


class UserRestriction(BaseTolokaObject, spec_enum='Scope', spec_field='scope'):
    """A base class for access restrictions.

    Toloker's access to projects or pools can be restricted.
    You can set the duration of the ban or apply an unlimited restriction.

    Use the [set_user_restriction](toloka.client.TolokaClient.set_user_restriction.md) method to apply a restriction
    and the [delete_user_restriction](toloka.client.TolokaClient.delete_user_restriction.md) method to remove it.

    Attributes:
        id: The ID of the restriction.
        user_id: The ID of the Toloker.
        private_comment: A comment visible to the requester only.
        will_expire: The UTC date and time when the access will be restored by Toloka.
            If the parameter isn't set, then the restriction is active until you remove it calling the `delete_user_restriction` method.
        created: The UTC date and time when the restriction was applied. Read-only field.

    Example:
        Restricting access to a project and removing the restriction.

        >>> new_restriction = toloka_client.set_user_restriction(
        >>>     toloka.client.user_restriction.ProjectUserRestriction(
        >>>         user_id='1ad097faba0eff85a04fe30bc04d53db',
        >>>         private_comment='Low response quality',
        >>>         project_id='5'
        >>>     )
        >>> )
        >>> toloka_client.delete_user_restriction(new_restriction.id)
        ...
    """

    @unique
    class Scope(ExtendableStrEnum):
        """A restriction scope.

        Attributes:
            ALL_PROJECTS: Access to all requester's projects is blocked.
            PROJECT: A single project is blocked.
            POOL: A pool is blocked.
            SYSTEM: A system-wide [ban](https://toloka.ai/docs/guide/ban/?form-source=api-ban#ban-platform).
        """

        SYSTEM = 'SYSTEM'
        ALL_PROJECTS = 'ALL_PROJECTS'
        PROJECT = 'PROJECT'
        POOL = 'POOL'

    SYSTEM = Scope.SYSTEM
    ALL_PROJECTS = Scope.ALL_PROJECTS
    PROJECT = Scope.PROJECT
    POOL = Scope.POOL

    user_id: str
    private_comment: str
    will_expire: datetime.datetime

    # Readonly
    id: str = attribute(readonly=True)
    created: datetime.datetime = attribute(readonly=True)


@inherit_docstrings
class AllProjectsUserRestriction(UserRestriction, spec_value=UserRestriction.ALL_PROJECTS):
    """All projects restriction.

    A Toloker doesn't have access to any of requester's projects.
    """

    pass


@inherit_docstrings
class PoolUserRestriction(UserRestriction, spec_value=UserRestriction.POOL):
    """A pool restriction.

    A Toloker doesn't have access to the pool.

    Attributes:
        pool_id: The ID of the pool that is blocked.
    """

    pool_id: str


@inherit_docstrings
class ProjectUserRestriction(UserRestriction, spec_value=UserRestriction.PROJECT):
    """A project restriction.

    A Toloker doesn't have access to the project.

    Attributes:
        project_id: The ID of the project that is blocked.
    """

    project_id: str


@inherit_docstrings
class SystemUserRestriction(UserRestriction, spec_value=UserRestriction.SYSTEM):
    """A system-wide restriction.
    """

    pass
