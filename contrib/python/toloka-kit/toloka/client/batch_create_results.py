__all__ = [
    'FieldValidationError',
    'TaskBatchCreateResult',
    'TaskSuiteBatchCreateResult',
    'UserBonusBatchCreateResult',
    'WebhookSubscriptionBatchCreateResult'
]
from typing import Any, Dict, List, Optional, Type

from .primitives.base import BaseTolokaObject, BaseTolokaObjectMetaclass
from .task import Task
from .task_suite import TaskSuite
from .user_bonus import UserBonus
from .webhook_subscription import WebhookSubscription


class FieldValidationError(BaseTolokaObject):
    """An error that contains information about an invalid field.

    Attributes:
        code: The error code.
        message: The error message.
        params: A list with additional parameters describing the error.
    """

    code: str
    message: str
    params: List[Any]


def _create_batch_create_result_class_for(type_: Type, docstring: Optional[str] = None):
    cls = BaseTolokaObjectMetaclass(
        f'{type_.__name__}BatchCreateResult',
        (BaseTolokaObject,),
        {
            'validation_errors': None,
            '__annotations__': {
                'items': Dict[str, type_],
                'validation_errors': Optional[Dict[str, Dict[str, FieldValidationError]]],
            }
        },
    )
    cls.__module__ = __name__
    cls.__doc__ = docstring
    return cls


TaskBatchCreateResult = _create_batch_create_result_class_for(
    Task,
    """The result of a task creation.

    `TaskBatchCreateResult` is returned by the [create_tasks](toloka.client.TolokaClient.create_tasks.md) method.

    Attributes:
        items: A dictionary with created tasks. The indexes of a `create_tasks` input list are used as keys in the dictionary.
        validation_errors: A dictionary with validation errors in input tasks. It is filled if the request parameter `skip_invalid_items` is `True`.

    Example:
        >>> tasks = [ toloka.client.task.Task(input_values = {'image': 'https://example.com/image0.png'}, pool_id='1240045') ]
        >>> result = toloka_client.create_tasks(tasks, allow_defaults=True, skip_invalid_items=True)
        >>> print(result.items['0'].created)
        ...
    """
)
TaskSuiteBatchCreateResult = _create_batch_create_result_class_for(
    TaskSuite,
    """The result of a task suite creation.

    `TaskSuiteBatchCreateResult` is returned by the [create_task_suites](toloka.client.TolokaClient.create_task_suites.md) method.

    Attributes:
        items: A dictionary with created task suites. The indexes of a `create_task_suites` input list are used as keys in the dictionary.
        validation_errors: A dictionary with validation errors in input task suites. It is filled if the request parameter `skip_invalid_items` is `True`.

    Example:
        >>> task_suites = [
        >>>     toloka.client.TaskSuite(
        >>>         pool_id=1240045,
        >>>         overlap=1,
        >>>         tasks=[ toloka.client.Task(input_values={'question': 'Choose a random number'}) ]
        >>>     )
        >>> ]
        >>> result = toloka_client.create_task_suites(task_suites=task_suites, skip_invalid_items=True)
        >>> for k, error in result.validation_errors.items():
        >>>     print(k, error.message)
        ...
    """
)
UserBonusBatchCreateResult = _create_batch_create_result_class_for(
    UserBonus,
    """The result of issuing bonuses for Tolokers.

    `UserBonusBatchCreateResult` is returned by the [create_user_bonuses](toloka.client.TolokaClient.create_user_bonuses.md) method.

    Attributes:
        items: A dictionary with created bonuses. The indexes of a `create_user_bonuses` input list are used as keys in the dictionary.
        validation_errors: A dictionary with validation errors. It is filled if the request parameter `skip_invalid_items` is `True`.

    Example:
        >>> import decimal
        >>> bonuses=[
        >>>     toloka.client.user_bonus.UserBonus(
        >>>         user_id='fac97860c7929add8048ed2ef63b66fd',
        >>>         amount=decimal.Decimal('0.50'),
        >>>         public_title={'EN': 'Perfect job!'},
        >>>         public_message={'EN': 'You are the best!'},
        >>>         assignment_id='00001092da--61ef030400c684132d0da0de'
        >>>     ),
        >>> ]
        >>> result = toloka_client.create_user_bonuses(user_bonuses=bonuses, skip_invalid_items=True)
        >>> print(result.items['0'].created)
        ...
    """
)
WebhookSubscriptionBatchCreateResult = _create_batch_create_result_class_for(
    WebhookSubscription,
    """The result of creating webhook subscriptions.

    `WebhookSubscriptionBatchCreateResult` is returned by the [upsert_webhook_subscriptions](toloka.client.TolokaClient.upsert_webhook_subscriptions.md) method.

    Attributes:
        items: A dictionary with created subscriptions. The indexes of a `upsert_webhook_subscriptions` input list are used as keys in the dictionary.
        validation_errors: A dictionary with validation errors.

    Example:
        >>> result = toloka_client.upsert_webhook_subscriptions([
        >>>     {
        >>>         'webhook_url': 'https://awesome-requester.com/toloka-webhook',
        >>>         'event_type': toloka.client.webhook_subscription.WebhookSubscription.EventType.ASSIGNMENT_CREATED,
        >>>         'pool_id': '1240045'
        >>>     }
        >>> ])
        >>> print(result.items['0'].created)
        ...
    """
)
