__all__ = [
    'AggregatedSolutionSearchResult',
    'AssignmentSearchResult',
    'AttachmentSearchResult',
    'MessageThreadSearchResult',
    'ProjectSearchResult',
    'PoolSearchResult',
    'SkillSearchResult',
    'TaskSearchResult',
    'TaskSuiteSearchResult',
    'TrainingSearchResult',
    'UserBonusSearchResult',
    'UserRestrictionSearchResult',
    'UserSkillSearchResult',
    'WebhookSubscriptionSearchResult',
    'OperationSearchResult',
    'AppProjectSearchResult',
    'AppSearchResult',
    'AppItemSearchResult',
    'AppBatchSearchResult'
]
from typing import Type, List, Optional
from .aggregation import AggregatedSolution
from .app import App, AppItem, AppProject, AppBatch
from .assignment import Assignment
from .attachment import Attachment
from .message_thread import MessageThread
from .operations import Operation
from .pool import Pool
from .primitives.base import BaseTolokaObject, BaseTolokaObjectMetaclass
from .project import Project
from .skill import Skill
from .task import Task
from .task_suite import TaskSuite
from .training import Training
from .user_bonus import UserBonus
from .user_restriction import UserRestriction
from .user_skill import UserSkill
from .webhook_subscription import WebhookSubscription


def _create_search_result_class_for(type_: Type, docstring: Optional[str] = None, items_field: str = 'items'):
    cls = BaseTolokaObjectMetaclass(
        f'{type_.__name__}SearchResult',
        (BaseTolokaObject,),
        {'__annotations__': {items_field: List[type_], 'has_more': bool}},
    )
    cls.__module__ = __name__
    cls.__doc__ = docstring
    return cls


def _create_search_result_docstring(name: str, name_in_header: str = None, items_field: str = 'items'):
    name_in_header = name_in_header or name
    return f"""The result of searching {name_in_header}.

    Attributes:
        {items_field}: A list with found {name}.
        has_more: A flag showing whether there are more matching {name}.
            * `True` — There are more matching {name}, not included in `{items_field}` due to the limit set in the search request.
            * `False` — `{items_field}` contains all matching {name}."""


AggregatedSolutionSearchResult = _create_search_result_class_for(
    AggregatedSolution,
    _create_search_result_docstring('aggregated responses')
)
AssignmentSearchResult = _create_search_result_class_for(
    Assignment,
    _create_search_result_docstring('assignments')
)
AttachmentSearchResult = _create_search_result_class_for(
    Attachment,
    _create_search_result_docstring('attachments')
)
MessageThreadSearchResult = _create_search_result_class_for(
    MessageThread,
    _create_search_result_docstring('message threads')
)
ProjectSearchResult = _create_search_result_class_for(
    Project,
    _create_search_result_docstring('projects')
)
PoolSearchResult = _create_search_result_class_for(
    Pool,
    _create_search_result_docstring('pools')
)
SkillSearchResult = _create_search_result_class_for(
    Skill,
    _create_search_result_docstring('skills')
)
TaskSearchResult = _create_search_result_class_for(
    Task,
    _create_search_result_docstring('tasks')
)
TaskSuiteSearchResult = _create_search_result_class_for(
    TaskSuite,
    _create_search_result_docstring('task suites')
)
TrainingSearchResult = _create_search_result_class_for(
    Training,
    _create_search_result_docstring('training pools')
)
UserBonusSearchResult = _create_search_result_class_for(
    UserBonus,
    _create_search_result_docstring("bonuses", "Tolokers' bonuses")
)
UserRestrictionSearchResult = _create_search_result_class_for(
    UserRestriction,
    _create_search_result_docstring('Toloker restrictions')
)
UserSkillSearchResult = _create_search_result_class_for(
    UserSkill,
    _create_search_result_docstring("skills", "Tolokers' skills")
)
WebhookSubscriptionSearchResult = _create_search_result_class_for(
    WebhookSubscription,
    _create_search_result_docstring('subscriptions', 'webhook subscriptions')
)
OperationSearchResult = _create_search_result_class_for(
    Operation,
    _create_search_result_docstring('operations')
)
OperationSearchResult = _create_search_result_class_for(
    Operation,
    """The list of found operations and whether there is something else on the original request

    It's better to use TolokaClient.get_operations(),
    which already implements the correct handling of the search result.

    Attributes:
        items: List of found operations
        has_more: Whether the list is complete:
            * `True` — Not all elements are included in the output due to restrictions in the limit parameter.
            * `False` — The output lists all the items.
    """
)
AppProjectSearchResult = _create_search_result_class_for(
    AppProject,
    items_field='content',
    docstring=_create_search_result_docstring('App projects', items_field='content')
)
AppSearchResult = _create_search_result_class_for(
    App,
    items_field='content',
    docstring=_create_search_result_docstring('App solutions', items_field='content')
)
AppItemSearchResult = _create_search_result_class_for(
    AppItem,
    items_field='content',
    docstring=_create_search_result_docstring('task items', 'App task items', items_field='content')
)
AppBatchSearchResult = _create_search_result_class_for(
    AppBatch,
    items_field='content',
    docstring=_create_search_result_docstring('batches', 'batches in an App project', items_field='content')
)
