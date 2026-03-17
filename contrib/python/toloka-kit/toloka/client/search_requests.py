__all__ = [
    'SortItemSelf',
    'SortItemsSelf',
    'SortOrder',
    'BaseSortItem',
    'BaseSortItems',
    'SearchRequestMetaclass',
    'BaseSearchRequest',
    'ProjectSearchRequest',
    'ProjectSortItems',
    'PoolSearchRequest',
    'PoolSortItems',
    'TrainingSearchRequest',
    'TrainingSortItems',
    'SkillSearchRequest',
    'SkillSortItems',
    'AssignmentSearchRequest',
    'AssignmentSortItems',
    'AggregatedSolutionSearchRequest',
    'AggregatedSolutionSortItems',
    'TaskSearchRequest',
    'TaskSortItems',
    'TaskSuiteSearchRequest',
    'TaskSuiteSortItems',
    'AttachmentSearchRequest',
    'AttachmentSortItems',
    'UserSkillSearchRequest',
    'UserSkillSortItems',
    'UserRestrictionSearchRequest',
    'UserRestrictionSortItems',
    'UserBonusSearchRequest',
    'UserBonusSortItems',
    'MessageThreadSearchRequest',
    'MessageThreadSortItems',
    'WebhookSubscriptionSearchRequest',
    'WebhookSubscriptionSortItems',
    'OperationSearchRequest',
    'OperationSortItems',
    'AppProjectSearchRequest',
    'AppProjectSortItems',
    'AppSearchRequest',
    'AppSortItems',
    'AppItemSearchRequest',
    'AppItemSortItems',
    'AppBatchSearchRequest',
    'AppBatchSortItems',
]
import datetime
from enum import Enum, unique, auto
from typing import Optional, TypeVar, Type, Union, List, get_type_hints, cast

import attr

from ._converter import converter, structure, unstructure
from .app import AppItem, AppProject, AppBatch
from .assignment import Assignment
from .attachment import Attachment
from .message_thread import Folder
from .operations import OperationType, Operation
from .pool import Pool
from .primitives.base import BaseTolokaObject, BaseTolokaObjectMetaclass
from .project import Project
from .training import Training
from .user_restriction import UserRestriction
from .webhook_subscription import WebhookSubscription

SortItemSelf = TypeVar('SortItemSelf', bound='BaseSortItem')
SortItemsSelf = TypeVar('SortItemsSelf', bound='BaseSortItems')


@unique
class SortOrder(Enum):
    ASCENDING = auto()
    DESCENDING = auto()


class BaseSortItem(BaseTolokaObject):

    def unstructure(self):
        if self.order == SortOrder.DESCENDING:
            return f'-{self.field.value}'
        return f'{self.field.value}'

    @classmethod
    def structure(cls: Type[SortItemSelf], value: Union[SortItemSelf, str]) -> SortItemSelf:
        if isinstance(value, cls):
            return value

        value = cast(str, value)
        if value.startswith('-'):
            return cls(structure(value[1:], cls.SortField), SortOrder.DESCENDING)  # type: ignore
        return cls(structure(value, cls.SortField), SortOrder.ASCENDING)  # type: ignore

    @staticmethod
    def _create_sort_field_enum(qualname: str, sort_fields: List[str]):
        namespace = {field.upper(): field for field in sort_fields}
        namespace['__qualname__'] = qualname
        return unique(Enum(qualname.split('.')[-1], namespace))  # type: ignore

    @classmethod
    def for_fields(cls, qualname: str, sort_fields: List[str], module_name: str = __name__):
        sort_field_enum = cls._create_sort_field_enum(f'{qualname}.SortField', sort_fields)
        sort_field_enum.__module__ = module_name
        namespace = {
            'SortField': sort_field_enum,
            'order': SortOrder.ASCENDING,
            '__qualname__': qualname,
            '__annotations__': {
                'field': sort_field_enum,
                'order': SortOrder,
            },
        }

        subclass = BaseTolokaObjectMetaclass(qualname.split('.')[-1], (cls,), namespace, kw_only=False)
        subclass.__module__ = module_name
        return subclass


class BaseSortItems(BaseTolokaObject):

    def unstructure(self):
        return ','.join(unstructure(item) for item in self.items)

    @classmethod
    def structure(cls, items):
        if isinstance(items, cls):
            return items
        return cls(items=items)

    @classmethod
    def for_fields(cls, qualname: str, sort_fields: List[str], docstring: Optional[str] = None, module_name: str = __name__):
        sort_item_class: Type = BaseSortItem.for_fields(f'{qualname}.SortItem', sort_fields, module_name=module_name)

        def items_converter(items):
            if isinstance(items, sort_items_class):
                return items
            if isinstance(items, str):
                items = items.split(',')
            return [sort_item_class.structure(item) for item in items]

        namespace = {
            'SortItem': sort_item_class,
            '__annotations__': {'items': List[sort_item_class]},  # type: ignore
            'items': attr.attrib(converter=items_converter),
            '__qualname__': qualname,
        }
        sort_items_class = BaseTolokaObjectMetaclass(qualname.split('.')[-1], (BaseSortItems,), namespace, kw_only=False)
        sort_items_class.__module__ = module_name
        sort_items_class.__doc__ = docstring
        return sort_items_class


class SearchRequestMetaclass(BaseTolokaObjectMetaclass):

    def __new__(mcs, name, bases, namespace, kw_only=False, **kwargs):
        compare_fields_class = namespace['CompareFields']
        annotations = namespace.setdefault('__annotations__', {})

        # For every comparable field creating a corresponding attribute
        for field_name, field_type in get_type_hints(compare_fields_class).items():
            for suffix in 'lt', 'lte', 'gt', 'gte':
                condition_field = f'{field_name}_{suffix}'
                namespace[condition_field] = None
                annotations[condition_field] = Optional[field_type]

        # Building class
        subclass = super().__new__(mcs, name, bases, namespace, kw_only=kw_only, **kwargs)
        return subclass


class BaseSearchRequest(BaseTolokaObject, metaclass=SearchRequestMetaclass):
    """A base class for all search request classes.
    """

    class CompareFields:
        pass


class ProjectSearchRequest(BaseSearchRequest):
    """Parameters for searching projects.

    Attributes:
        status: Project status.
                Refer to the [ProjectStatus](toloka.client.project.Project.ProjectStatus.md) page for more information on the available `status` values.
        id_lt: Projects with IDs less than the specified value.
        id_lte: Projects with IDs less than or equal to the specified value.
        id_gt: Projects with IDs greater than the specified value.
        id_gte: Projects with IDs greater than or equal to the specified value.
        created_lt: Projects created before the specified date.
        created_lte: Projects created before or on the specified date.
        created_gt: Projects created after the specified date.
        created_gte: Projects created after or on the specified date.
    """

    class CompareFields:
        id: str
        created: datetime.datetime

    status: Project.ProjectStatus


ProjectSortItems = BaseSortItems.for_fields(
    'ProjectSortItems', ['id', 'created', 'public_name', 'private_comment'],
    # docstring
    """Keys for sorting projects in search results.

    Attributes:
        items: A list of sorting keys. Supported values:
            * `'id'` — A project ID.
            * `'created'` — A project creation date.
            * `'public_name'` — A project name.
            * `'private_comment'` — A project private comment.

    Example:
        The example shows how to find active projects sorted by names in descending order. Projects with equal names are sorted by IDs in ascending order.

        >>> sort = toloka.client.search_requests.ProjectSortItems(['-public_name', 'id'])
        >>> result = toloka_client.find_projects(status='ACTIVE', sort=sort, limit=50)
        ...
    """
)


class PoolSearchRequest(BaseSearchRequest):
    """Parameters for searching pools.

    Attributes:
        status: Pool status. Refer to the [Pool.Status](toloka.client.pool.Pool.Status.md) page for more information on the available `status` values.
        project_id: Pools belonging to the project with the specified ID.
        id_lt: Pools with IDs less than the specified value.
        id_lte: Pools with IDs less than or equal to the specified value.
        id_gt: Pools with IDs greater than the specified value.
        id_gte: Pools with IDs greater than or equal to the specified value.
        created_lt: Pools created before the specified date.
        created_lte: Pools created before or on the specified date.
        created_gt: Pools created after the specified date.
        created_gte: Pools created after or on the specified date.
        last_started_lt: Pools that were opened last time before the specified date.
        last_started_lte: Pools that were opened last time before or on the specified date.
        last_started_gt: Pools that were opened last time after the specified date.
        last_started_gte: Pools that were opened last time after or on the specified date.
    """

    class CompareFields:
        id: str
        created: datetime.datetime
        last_started: datetime.datetime

    status: Pool.Status
    project_id: str


PoolSortItems = BaseSortItems.for_fields(
    'PoolSortItems', ['id', 'created', 'last_started'],
    # docstring
    """Keys for sorting pools in search results.

    Attributes:
        items: A list of sorting keys. Supported values:
            * `'id'` — A pool ID.
            * `'created'` — A pool creation date.
            * `'last_started'` — The last opening date of a pool.

    Example:
        The example shows how to find opened pools sorted by the last opening date in descending order. Pools with equal opening dates are sorted by IDs in ascending order.

        >>> sort = toloka.client.search_requests.PoolSortItems(['-last_started', 'id'])
        >>> result = toloka_client.find_pools(status='OPEN', sort=sort, limit=50)
        ...
    """

)


class TrainingSearchRequest(BaseSearchRequest):
    """Parameters for searching training pools.

    Attributes:
        status: Training pool status.
                Refer to the [Training.Status](toloka.client.training.Training.Status.md) page for more information on the available `status` values.
        project_id: Training pools belonging to the project with the specified ID.
        id_lt: Training pools with IDs less than the specified value.
        id_lte: Training pools with IDs less than or equal to the specified value.
        id_gt: Training pools with IDs greater than the specified value.
        id_gte: Training pools with IDs greater than or equal to the specified value.
        created_lt: Training pools created before the specified date.
        created_lte: Training pools created before or on the specified date.
        created_gt: Training pools created after the specified date.
        created_gte: Training pools created after or on the specified date.
        last_started_lt: Training pools that were opened last time before the specified date.
        last_started_lte: Training pools that were opened last time before or on the specified date.
        last_started_gt: Training pools that were opened last time after the specified date.
        last_started_gte: Training pools that were opened last time after or on the specified date.
    """

    class CompareFields:
        id: str
        created: datetime.datetime
        last_started: datetime.datetime

    status: Training.Status
    project_id: str


TrainingSortItems = BaseSortItems.for_fields(
    'TrainingSortItems', ['id', 'created', 'last_started'],
    # docstring
    """Keys for sorting training pools in search results.

    Attributes:
        items: A list of sorting keys. Supported values:
            * `'id'` — Training pool ID.
            * `'created'` — Training pool creation date.
            * `'last_started'` — The last opening date of a training pool.

    Example:
        The example shows how to find opened training pools sorted by the last opening date in descending order. Pools with equal opening dates are sorted by IDs in ascending order.

        >>> sort = toloka.client.search_requests.TrainingSortItems(['-last_started', 'id'])
        >>> result = toloka_client.find_trainings(status='OPEN', sort=sort, limit=50)
        ...
    """
)


class SkillSearchRequest(BaseSearchRequest):
    """Parameters for searching skill.

    Attributes:
        name: The name of the skill.
        id_lt: Skills with IDs less than the specified value.
        id_lte: Skills with IDs less than or equal to the specified value.
        id_gt: Skills with IDs greater than the specified value.
        id_gte: Skills with IDs greater than or equal to the specified value.
        created_lt: Skills created before the specified date.
        created_lte: Skills created before or on the specified date.
        created_gt: Skills created after the specified date.
        created_gte: Skills created on or after the specified date.
    """

    class CompareFields:
        id: str
        created: datetime.datetime

    name: str


SkillSortItems = BaseSortItems.for_fields(
    'SkillSortItems', ['id', 'created'],
    # docstring
    """Keys for sorting skills in search results.

    Attributes:
        items: A list of sorting keys. Supported values:
            * `'id'` — The ID of a skill.
            * `'created'` — A skill creation date.

    Example:
        The example shows how to find skills sorted by creation date in descending order. Skills with equal creation dates are sorted by IDs in ascending order.

        >>> sort = toloka.client.search_requests.SkillSortItems(['-created', 'id'])
        >>> result = toloka_client.find_skills(name='Image annotation', sort=sort, limit=10)
        ...
    """
)


class AssignmentSearchRequest(BaseSearchRequest):
    """Parameters for searching task suites assignments.

    Attributes:
        status: Assignment status or a list of statuses.
                Refer to the [Assignment.Status](toloka.client.assignment.Assignment.Status.md) page
                for more information on the available `status` values.
        task_id: Assignments containing the task with the specified ID.
        task_suite_id: Assignments for a task suite with the specified ID.
        pool_id: Assignments in a pool with the specified ID.
        user_id: Assignments from a Toloker with the specified ID.
        id_lt: Assignments with IDs less than the specified value.
        id_lte: Assignments with IDs less than or equal to the specified value.
        id_gt: Assignments with IDs greater than the specified value.
        id_gte: Assignments with IDs greater than or equal to the specified value.
        created_lt: Task suites assigned before the specified date.
        created_lte: Task suites assigned before or on the specified date.
        created_gt: Task suites assigned after the specified date.
        created_gte: Task suites assigned after or on the specified date.
        submitted_lt: Assignments completed before the specified date.
        submitted_lte: Assignments completed before or on the specified date.
        submitted_gt: Assignments completed after the specified date.
        submitted_gte: Assignments completed after or on the specified date.
        accepted_lt: Assignments accepted before the specified date.
        accepted_lte: Assignments accepted before or on the specified date.
        accepted_gt: Assignments accepted after the specified date.
        accepted_gte: Assignments accepted after or on the specified date.
        rejected_lt: Assignments rejected before the specified date.
        rejected_lte: Assignments rejected before or on the specified date.
        rejected_gt: Assignments rejected after the specified date.
        rejected_gte: Assignments rejected after or on the specified date.
        skipped_lt: Assigned task suites skipped before the specified date.
        skipped_lte: Assigned task suites skipped before or on the specified date.
        skipped_gt: Assigned task suites skipped after the specified date.
        skipped_gte: Assigned task suites skipped after or on the specified date.
        expired_lt: Assigned task suites expired before the specified date.
        expired_lte: Assigned task suites expired before or on the specified date.
        expired_gt: Assigned task suites expired after the specified date.
        expired_gte: Assigned task suites expired after or on the specified date.
    """

    class CompareFields:
        id: str
        created: datetime.datetime
        submitted: datetime.datetime
        accepted: datetime.datetime
        rejected: datetime.datetime
        skipped: datetime.datetime
        expired: datetime.datetime

    def _list_converter(value: Union[str, Assignment.Status, List[Union[str, Assignment.Status]]]):
        if value is None:
            return value
        if isinstance(value, str):
            value = value.split(',')
            value = [item.strip() for item in value]
        if not isinstance(value, list):
            value = [value]
        return [Assignment.Status(item) for item in value]

    def _list_setter(self, attribute, value):
        return AssignmentSearchRequest._list_converter(value)

    def unstructure(self) -> Optional[dict]:
        data = super().unstructure()

        if self.status is not None:
            data['status'] = ','.join(converter.unstructure(item) for item in self.status)

        return data

    status: List[Assignment.Status] = attr.attrib(converter=_list_converter, on_setattr=_list_setter)
    task_id: str
    task_suite_id: str
    pool_id: str
    user_id: str


AssignmentSortItems = BaseSortItems.for_fields(
    'AssignmentSortItems', ['id', 'created', 'submitted', 'accepted', 'rejected', 'skipped', 'expired'],
    # docstring
    """Keys for sorting assignments in search results.

    Attributes:
        items: A list of sorting keys. Supported values:
            * `'id'` — An assignment ID.
            * `'created'` — The assigning date of a task suite.
            * `'submitted'` — The completion date of a task suite.
            * `'accepted'` — The acceptance date of a task suite.
            * `'rejected'` — The rejection date a task suite.
            * `'skipped'` — The date when a task suite was skipped.
            * `'expired'` — The expiration date of a task suite.

    Example:
        The example shows how to find assignments sorted by the completion date in descending order. Assignments with equal completion dates are sorted by IDs in descending order.

        >>> sort = toloka.client.search_requests.AssignmentSortItems(['-submitted', '-id'])
        >>> result = toloka_client.find_assignments(pool_id='1080020', status='SUBMITTED', sort=sort, limit=10)
        ...
    """
)


class AggregatedSolutionSearchRequest(BaseSearchRequest):
    """Parameters for searching aggregated responses.

    Attributes:
        task_id_lt: Responses for tasks with IDs less than the specified value.
        task_id_lte: Responses for tasks with IDs less than or equal to the specified value.
        task_id_gt: Responses for tasks with IDs greater than the specified value.
        task_id_gte: Responses for tasks with IDs greater than or equal to the specified value.
    """

    class CompareFields:
        task_id: str


AggregatedSolutionSortItems = BaseSortItems.for_fields(
    'AggregatedSolutionSortItems', ['task_id'],
    # docstring
    """Keys for sorting aggregated responses in search results.

    Attributes:
        items: A list of sorting keys. Supported values:
            * `'task_id'` — The ID of a task.
    """
)


class TaskSearchRequest(BaseSearchRequest):
    """Parameters for searching tasks.

    Attributes:
        pool_id: The ID of the pool to get tasks from.
        overlap: Tasks with an overlap equal to the specified value.
        id_lt: Tasks with IDs less than the specified value.
        id_lte: Tasks with IDs less than or equal to the specified value.
        id_gt: Tasks with IDs greater than the specified value.
        id_gte: Tasks with IDs greater than or equal to the specified value.
        created_lt: Tasks created before the specified date.
        created_lte: Tasks created before or on the specified date.
        created_gt: Tasks created after the specified date.
        created_gte: Tasks created after or on the specified date.
        overlap_lt: Tasks with an overlap less than the specified value.
        overlap_lte: Tasks with an overlap less than or equal to the specified value.
        overlap_gt: Tasks with an overlap greater than the specified value.
        overlap_gte: Tasks with an overlap greater than or equal to the specified value.
    """

    class CompareFields:
        id: str
        created: datetime.datetime
        overlap: int

    pool_id: str
    overlap: int


TaskSortItems = BaseSortItems.for_fields(
    'TaskSortItems', ['id', 'created'],
    # docstring
    """Keys for sorting tasks in search results.

    Attributes:
        items: A list of sorting keys. Supported values:
            * `'id'` — The ID of a task.
            * `'created'` — The creation date of a task.

    Example:
        The example shows how to find tasks sorted by creation dates in descending order. Tasks with equal creation dates are sorted by IDs in descending order.

        >>> sort = toloka.client.search_requests.TaskSortItems(['-created', '-id'])
        >>> result = toloka_client.find_tasks(pool_id='1086170', sort=sort, limit=10)
        ...
    """
)


class TaskSuiteSearchRequest(BaseSearchRequest):
    """Parameters for searching task suites.

    Attributes:
        task_id: Task suite containing a task with the specified ID.
        pool_id: Task suites from a pool with the specified ID.
        overlap: Task suites with an overlap equal to the specified value.
        id_lt: Task suites with IDs less than the specified value.
        id_lte: Task suites with IDs less than or equal to the specified value.
        id_gt: Task suites with IDs greater than the specified value.
        id_gte: Task suites with IDs greater than or equal to the specified value.
        created_lt: Task suites created before the specified date.
        created_lte: Task suites created before or on the specified date.
        created_gt: Task suites created after the specified date.
        created_gte: Task suites created after or on the specified date.
        overlap_lt: Task suites with an overlap less than the specified value.
        overlap_lte: Task suites with an overlap less than or equal to the specified value.
        overlap_gt: Task suites with an overlap greater than the specified value.
        overlap_gte: Task suites with an overlap greater than or equal to the specified value.
    """

    class CompareFields:
        id: str
        created: datetime.datetime
        overlap: int

    task_id: str
    pool_id: str
    overlap: int


TaskSuiteSortItems = BaseSortItems.for_fields(
    'TaskSuiteSortItems', ['id', 'created'],
    # docstring
    """Keys for sorting task suites in search results.

    Attributes:
        items: A list of sorting keys. Supported values:
            * `'id'` — The ID of a task suite.
            * `'created'` — The creation date of a task suite.

    Example:
        The example shows how to find task suites sorted by the creation date in descending order. Task suites with equal creation dates are sorted by IDs in descending order.

        >>> sort = toloka.client.search_requests.TaskSuiteSortItems(['-created', '-id'])
        >>> result = toloka_client.find_task_suites(pool_id='1086170', sort=sort, limit=10)
        ...
    """
)


class AttachmentSearchRequest(BaseSearchRequest):
    """Parameters for searching attachments.

    Attributes:
        name: An attachment file name.
        type: An attachment type.
              Refer to the [Attachment.Type](toloka.client.attachment.Attachment.Type.md) page for more information on the available `type` values.
        user_id: The ID of a Toloker who uploaded attachments.
        assignment_id: The ID of an assignment with attachments. Either `assignment_id` of `pool_id` is required in a search request.
        pool_id: The ID of a pool with attachments. Either `assignment_id` of `pool_id` is required in a search request.
        id_lt: Attachments with IDs less than the specified value.
        id_lte: Attachments with IDs less than or equal to the specified value.
        id_gt: Attachments with IDs greater than the specified value.
        id_gte: Attachments with IDs greater than or equal to the specified value.
        created_lt: Attachments uploaded by Tolokers before the specified date.
        created_lte: Attachments uploaded by Tolokers before or on the specified date.
        created_gt: Attachments uploaded by Tolokers after the specified date.
        created_gte: Attachments uploaded by Tolokers after or on the specified date.
    """

    class CompareFields:
        id: str
        created: datetime.datetime

    name: str
    type: Attachment.Type
    user_id: str
    assignment_id: str
    pool_id: str

    owner_id: str
    owner_company_id: str


AttachmentSortItems = BaseSortItems.for_fields(
    'AttachmentSortItems', ['id', 'created'],
    # docstring
    """Keys for sorting attachments in search results.

    Attributes:
        items: A list of sorting keys. Supported values:
            * `'id'` — The ID of an attachment.
            * `'created'` — The date of uploading an attachment.

    Example:
        The example shows how to find attachments sorted by uploading date in descending order. Attachments with equal uploading dates are sorted by IDs in descending order.

        >>> sort = toloka.client.search_requests.AttachmentSortItems(['-created', '-id'])
        >>> result = toloka_client.find_attachments(pool_id='1086170', sort=sort, limit=10)
        ...
    """
)


class UserSkillSearchRequest(BaseSearchRequest):
    """Parameters for searching Toloker skill.

    Attributes:
        user_id: The ID of a Toloker.
        skill_id: The ID of a skill.
        id_lt: Skills with IDs less than the specified value.
        id_lte: Skills with IDs less than or equal to the specified value.
        id_gt: Skills with IDs greater than the specified value.
        id_gte: Skills with IDs greater than or equal to the specified value.
        created_lt: Skills created before the specified date.
        created_lte: Skills created before or on the specified date.
        created_gt: Skills created after the specified date.
        created_gte: Skills created on or after the specified date.
        modified_lt: Skills that changed before the specified date.
        modified_lte: Skills that changed before the specified date.
        modified_gt: Skills changed after the specified date.
        modified_gte: Skills created on or after the specified date.
    """

    class CompareFields:
        id: str
        created: datetime.datetime
        modified: datetime.datetime

    user_id: str
    skill_id: str


UserSkillSortItems = BaseSortItems.for_fields(
    'UserSkillSortItems', ['id', 'created', 'modified'],
    # docstring
    """Keys for sorting skills in search results.

    Attributes:
        items: A list of sorting keys. Supported values:
            * `'id'` — The ID of a skill.
            * `'created'` — The date when a skill was created.
            * `'modified'` — The date when a skill was modified.

    Example:
        The example shows how to find Tolokers' skills sorted by creation date in descending order. Skills with equal creation dates are sorted by IDs in ascending order.

        >>> sort = toloka.client.search_requests.UserSkillSortItems(['-created', 'id'])
        >>> result = toloka_client.find_user_skills(skill_id='11294', sort=sort, limit=10)
        ...
    """
)


class UserRestrictionSearchRequest(BaseSearchRequest):
    """Parameters for searching Toloker restrictions.

    Attributes:
        scope: The scope of a restriction.
               Refer to the [UserRestriction.Scope](toloka.client.user_restriction.UserRestriction.Scope.md) page
               for more information on the available `scope` values.
        user_id: The Toloker's ID.
        project_id: The ID of a project with restricted access.
        pool_id: The ID of a pool with restricted access.
        id_lt: Restrictions with IDs less than the specified value.
        id_lte: Restrictions with IDs less than or equal to the specified value.
        id_gt: Restrictions with IDs greater than the specified value.
        id_gte: Restrictions with IDs greater than or equal to the specified value.
        created_lt: Restrictions created before the specified date.
        created_lte: Restrictions created before or on the specified date.
        created_gt: Restrictions created after the specified date.
        created_gte: Restrictions created after or on the specified date.
    """

    class CompareFields:
        id: str
        created: datetime.datetime

    scope: UserRestriction.Scope
    user_id: str
    project_id: str
    pool_id: str


UserRestrictionSortItems = BaseSortItems.for_fields(
    'UserRestrictionSortItems', ['id', 'created'],
    # docstring
    """Keys for sorting Toloker restrictions in search results.

    Attributes:
        items: A list of sorting keys. Supported values:
            * `'id'` — The ID of a restriction.
            * `'created'` — The restriction creation date.

    Example:
        The example shows how to find Toloker restrictions sorted by creation date in descending order. Restrictions with equal creation dates are sorted by IDs in ascending order.

        >>> sort = toloka.client.search_requests.UserRestrictionSortItems(['-created', 'id'])
        >>> result = toloka_client.find_user_restrictions(pool_id='1086170', sort=sort, limit=10)
        ...
    """
)


class UserBonusSearchRequest(BaseSearchRequest):
    """Parameters for searching Tolokers' bonuses.

    Attributes:
        user_id: The ID of a Toloker.
        assignment_id: The ID of an assignment a bonus was granted for.
        private_comment: Bonuses with specified comment.
        id_lt: Bonuses with IDs less than the specified value.
        id_lte: Bonuses with IDs less than or equal to the specified value.
        id_gt: Bonuses with IDs greater than the specified value.
        id_gte: Bonuses with IDs greater than or equal to the specified value.
        created_lt: Bonuses given before the specified date.
        created_lte: Bonuses given before or on the specified date.
        created_gt: Bonuses given after the specified date.
        created_gte: Bonuses given after or on the specified date.
    """

    class CompareFields:
        id: str
        created: datetime.datetime

    user_id: str
    assignment_id: str
    private_comment: str


UserBonusSortItems = BaseSortItems.for_fields(
    'UserBonusSortItems', ['id', 'created'],
    # docstring
    """Keys for sorting bonuses in search results.

    Attributes:
        items: A list of sorting keys. Supported values:
            * `'id'` — The ID of a bonus.
            * `'created'` — The date of granting a bonus.

    Example:
        The example shows how to find bonuses sorted by granting date in descending order. Bonuses with equal granting dates are sorted by IDs in ascending order.

        >>> sort = toloka.client.search_requests.UserBonusSortItems(['-created', 'id'])
        >>> result = toloka_client.find_user_bonuses(user_id='fac97860c7929add8048ed2ef63b66fd', sort=sort, limit=10)
        ...
    """
)


class MessageThreadSearchRequest(BaseSearchRequest):
    """Parameters for searching message threads.

    Attributes:
        folder: A folder where to search threads or a list of folders.
                Refer to the [Folder](toloka.client.message_thread.Folder.md) page for more information on the available `folder` values.
        folder_ne: A folder to skip or a list of folders. Supported values are the same as for `folder`.
        id_lt: Threads with IDs less than the specified value.
        id_lte: Threads with IDs less than or equal to the specified value.
        id_gt: Threads with IDs greater than the specified value.
        id_gte: Threads with IDs greater than or equal to the specified value.
        created_lt: Threads created before the specified date.
        created_lte: Threads created before or on the specified date.
        created_gt: Threads created after the specified date.
        created_gte: Threads created after or on the specified date.
    """

    class CompareFields:
        id: str
        created: datetime.datetime

    def _list_converter(value: Union[str, Folder, List[Union[str, Folder]]]) -> List[Folder]:
        if value is None:
            return value
        if isinstance(value, str):
            value = value.split(',')
            value = [item.strip() for item in value]
        if not isinstance(value, list):
            value = [value]
        return [Folder(item) for item in value]

    def _list_setter(self, attribute, value):
        return MessageThreadSearchRequest._list_converter(value)

    def unstructure(self) -> Optional[dict]:
        data = super().unstructure()
        if self.folder is not None:
            data['folder'] = ','.join(converter.unstructure(item) for item in self.folder)
        if self.folder_ne is not None:
            data['folder_ne'] = ','.join(converter.unstructure(item) for item in self.folder_ne)
        return data

    folder: List[Folder] = attr.attrib(converter=_list_converter, on_setattr=_list_setter)
    folder_ne: List[Folder] = attr.attrib(converter=_list_converter, on_setattr=_list_setter)


MessageThreadSortItems = BaseSortItems.for_fields(
    'MessageThreadSortItems', ['id', 'created'],
    # docstring
    """Keys for sorting message threads in search results.

    Attributes:
        items: A list of sorting keys. Supported values:
            * `'id'` — Message thread ID.
            * `'created'` — Message thread creation date.
    """
)


class WebhookSubscriptionSearchRequest(BaseSearchRequest):
    """Parameters for searching event subscriptions.

    Attributes:
        event_type: Event type.
                    Refer to the [EventType](toloka.client.webhook_subscription.WebhookSubscription.EventType.md) page
                    for more information on the available `event_type` values.
        pool_id: The ID of a subscribed pool.
        id_lt: Subscriptions with IDs less than the specified value.
        id_lte: Subscriptions with IDs less than or equal to the specified value.
        id_gt: Subscriptions with IDs greater than the specified value.
        id_gte: Subscriptions with IDs greater than or equal to the specified value.
        created_lt: Subscriptions created before the specified date.
        created_lte: Subscriptions created before or on the specified date.
        created_gt: Subscriptions created after the specified date.
        created_gte: Subscriptions created after or on the specified date.
    """

    class CompareFields:
        id: str
        created: datetime.datetime

    event_type: WebhookSubscription.EventType
    pool_id: str


WebhookSubscriptionSortItems = BaseSortItems.for_fields(
    'WebhookSubscriptionSortItems', ['id', 'created'],
    # docstring
    """Keys for sorting event subscriptions in search results.

    Attributes:
        items: A list of sorting keys. Supported values:
            * `'id'` — A subscription ID.
            * `'created'` — The creation date of a subscription.

    Example:
        The example shows how to find event subscriptions sorted by creation date in descending order. Subscriptions with equal creation dates are sorted by IDs in ascending order.

        >>> sort = toloka.client.search_requests.WebhookSubscriptionSortItems(['-created', 'id'])
        >>> result = toloka_client.find_webhook_subscriptions(event_type='ASSIGNMENT_CREATED', pool_id='1086170', sort=sort, limit=10)
        ...
    """
)


class OperationSearchRequest(BaseSearchRequest):
    """Parameters for searching operations.

        Attributes:
            type: Operation type.
                  Refer to the [OperationType](toloka.client.operations.OperationType.md) page for more information on the available `type` values.
            status: The status of the operation.
                  Refer to the [Operation.Status](toloka.client.operations.Operation.Status.md) page for more information on the available `status` values.
            id_lt: Operations with IDs less than the specified value.
            id_lte: Operations with IDs less than or equal to the specified value.
            id_gt: Operations with IDs greater than the specified value.
            id_gte: Operations with IDs greater than or equal to the specified value.
            submitted_lt: Operations submitted before the specified date.
            submitted_lte: Operations submitted before or on the specified date.
            submitted_gt: Operations submitted after the specified date.
            submitted_gte: Operations submitted after or on the specified date.
            finished_lt: Operations finished before the specified date.
            finished_lte: Operations finished before or on the specified date.
            finished_gt: Operations finished after the specified date.
            finished_gte: Operations finished after or on the specified date.
        """

    class CompareFields:
        id: str
        submitted: datetime.datetime
        finished: datetime.datetime

    type: OperationType
    status: Operation.Status


OperationSortItems = BaseSortItems.for_fields(
    'OperationSortItems', ['id', 'submitted', 'finished'],
    # docstring
    """Keys for sorting operations in search results.

    Attributes:
        items: A list of sorting keys. Supported values:
            * `'id'` — Operation ID.
            * `'submitted'` — The date and time when the request was sent.
            * `'finished'` — The date and time when the operation was finished.

    Example:
        The example shows how to find operations sorted by finish date in descending order. Operations with equal finish dates are sorted by IDs in ascending order.

        >>> sort = toloka.client.search_requests.OperationSortItems(['-finished', 'id'])
        >>> result = toloka_client.find_operations(type='POOL.OPEN', status='SUCCESS', sort=sort, limit=10)
        ...
    """
)


class AppProjectSearchRequest(BaseSearchRequest):
    """Parameters for searching App projects.

    Attributes:
        app_id: Projects created using the solution with the specified ID.
        parent_app_project_id: Projects cloned from the project with the specified ID. Projects can be cloned in the web version of Toloka.
        status: App project status.
                Refer to the [AppProject.Status](toloka.client.app.AppProject.Status.md) page for more information on the available `status` values.
        after_id: The ID of a project used for cursor pagination.
        scope: Values:
            * `'MY'` — Projects created by you.
            * `'COMPANY'` — Projects created by requesters from your company.
            * `'REQUESTER_LIST'` — Projects created by requesters in the `requester_ids` list.
        requester_ids: A list with requester IDs separated by a comma. Use the list with parameter `scope = REQUESTER_LIST`.
        id_gt: Projects with IDs greater than the specified value.
        id_gte: Projects with IDs greater than or equal to the specified value.
        id_lt: Projects with IDs less than the specified value.
        id_lte: Projects with IDs less than or equal to the specified value.
        name_gt: Projects with a name lexicographically greater than the specified value.
        name_gte: Projects with a name lexicographically greater than or equal to the specified value.
        name_lt: Projects with a name lexicographically less than the specified value.
        name_lte: Projects with a name lexicographically less than or equal to the specified value.
        created_gt: Projects created after the specified date.
        created_gte: Projects created after or on the specified date.
        created_lt: Projects created before the specified date.
        created_lte: Projects created before or on the specified date.
    """

    @unique
    class Scope(Enum):
        """
        * `MY` — Projects created by you.
            * `COMPANY` — Projects created by requesters from your company.
            * `REQUESTER_LIST` — Projects created by requesters in the `requester_ids` list.
        """

        MY = 'MY'
        COMPANY = 'COMPANY'
        REQUESTER_LIST = 'REQUESTER_LIST'

    class CompareFields:
        id: str
        name: str
        created: datetime.datetime

    def _list_converter(value: Union[str, List[str]]):
        if isinstance(value, str):
            value = value.split(',')
            value = [item.strip() for item in value]
        return value

    def _list_setter(self, attribute, value):
        return AppProjectSearchRequest._list_converter(value)

    app_id: str
    parent_app_project_id: str
    status: AppProject.Status
    after_id: str
    scope: Scope
    requester_ids: List[str] = attr.attrib(converter=_list_converter, on_setattr=_list_setter)


AppProjectSortItems = BaseSortItems.for_fields(
    'AppProjectSortItems', ['id', 'name', 'created'],
    # docstring
    """Keys for sorting App projects in search results.

    Attributes:
        items: A list of sorting keys. Supported values:
            * `'id'` — An App project ID.
            * `'name'` — An App project name.
            * `'created'` — A project creation date.
    """
)


class AppSearchRequest(BaseSearchRequest):
    """Parameters for searching App solutions.

    Attributes:
        after_id: The ID of a solution used for cursor pagination.
        lang: ISO 639 language code.
        id_gt: Solutions with IDs greater than the specified value.
        id_gte: Solutions with IDs greater than or equal to the specified value.
        id_lt: Solutions with IDs less than the specified value.
        id_lte: Solutions with IDs less than or equal to the specified value.
        name_gt: Solutions with names lexicographically greater than the specified value.
        name_gte: Solutions with names lexicographically greater than or equal to the specified value.
        name_lt: Solutions with names lexicographically less than the specified value.
        name_lte: Solutions with names lexicographically less than or equal to the specified value.
    """

    class CompareFields:
        id: str

    after_id: str
    lang: str


AppSortItems = BaseSortItems.for_fields(
    'AppSortItems', ['id'],
    # docstring
    """Keys for sorting App solutions in search results.

    Attributes:
        items: A list of sorting keys. Supported values:
            * `'id'` — An App solution ID.
    """
)


class AppItemSearchRequest(BaseSearchRequest):
    """Parameters for searching App task items.

    Attributes:
        after_id: The ID of the item used for cursor pagination.
        batch_id: The ID of the batch to look in.
        status: App task item status.
                Refer to the [AppItem.Status](toloka.client.app.AppItem.Status.md) page for more information on the available `status` values.
        id_gt: Items with IDs greater than the specified value.
        id_gte: Items with IDs greater than or equal to the specified value.
        id_lt: Items with IDs less than the specified value.
        id_lte: Items with IDs less than or equal to the specified value.
        created_gt: Items created after the specified date.
        created_gte: Items created after or on the specified date.
        created_lt: Items created before the specified date.
        created_lte: Items created before or on the specified date.
        finished_gt: Items labeled after the specified date.
        finished_gte: Items labeled after or on the specified date.
        finished_lt: Items labeled before the specified date.
        finished_lte: Items labeled before or on the specified date.
    """

    class CompareFields:
        id: str
        created: datetime.datetime
        finished: datetime.datetime

    after_id: str
    batch_id: str
    status: AppItem.Status


AppItemSortItems = BaseSortItems.for_fields(
    'AppItemSortItems', ['id', 'created', 'finished', 'status'],
    # docstring
    """Keys for sorting App items in search results.

    Attributes:
        items: A list of sorting keys. Supported values:
            * `'id'` — A task item ID.
            * `'created'` — The date and time when the item was created.
            * `'finished'` — The date and time when the item processing was completed.
            * `'status'` — The item status.
    """
)


class AppBatchSearchRequest(BaseSearchRequest):
    """Parameters for searching batches in an App project.

    Attributes:
        after_id: The ID of the batch used for cursor pagination.
        status: Refer to the [AppBatch.Status](toloka.client.app.AppBatch.Status.md) page for more information on the available `status` values.
        id_gt: Batches with IDs greater than the specified value.
        id_gte: Batches with IDs greater than or equal to the specified value.
        id_lt: Batches with IDs less than the specified value.
        id_lte: Batches with IDs less than or equal to the specified value.
        name_gt: Batches with names lexicographically greater than the specified value.
        name_gte: Batches with names lexicographically greater than or equal to the specified value.
        name_lt: Batches with names lexicographically less than the specified value.
        name_lte: Batches with names lexicographically less than or equal to the specified value.
        created_gt: Batches created after the specified date.
        created_gte: Batches created after or on the specified date.
        created_lt: Batches created before the specified date.
        created_lte: Batches created before or on the specified date.
    """

    class CompareFields:
        id: str
        name: str
        created: datetime.datetime

    after_id: str
    status: AppBatch.Status


AppBatchSortItems = BaseSortItems.for_fields(
    'AppBatchSortItems', ['id', 'name', 'created', 'status'],
    # docstring
    """Keys for sorting App batches in search results.

    Attributes:
        items: A list of sorting keys. Supported values:
            * `'id'` — A batch ID.
            * `'name'` — A batch name.
            * `'created'` — A batch creation date.
            * `'status'` — The item status.
    """
)
