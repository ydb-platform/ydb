__all__ = [
    'AssignmentEvent',
    'BaseEvent',
    'MessageThreadEvent',
    'TaskEvent',
    'UserBonusEvent',
    'UserRestrictionEvent',
    'UserSkillEvent',
]

from enum import Enum, unique
from datetime import datetime
from ..client import Assignment, MessageThread, Task, UserBonus, UserSkill, UserRestriction
from ..client.primitives.base import BaseTolokaObject
from ..util._codegen import attribute


class BaseEventTypeEnum(Enum):
    @property
    def time_key(self):
        return self.value.lower()


class BaseEvent(BaseTolokaObject):
    event_time: datetime


class AssignmentEvent(BaseEvent):
    """Assignment-related event.

    Attributes:
        event_time: Event datetime.
        event_type: One of the folllowing event types:
            * CREATED
            * SUBMITTED
            * ACCEPTED
            * REJECTED
            * SKIPPED
            * EXPIRED
        assignment: Assignment object itself.
    """

    @unique
    class Type(BaseEventTypeEnum):
        CREATED = 'CREATED'
        SUBMITTED = 'SUBMITTED'
        ACCEPTED = 'ACCEPTED'
        REJECTED = 'REJECTED'
        SKIPPED = 'SKIPPED'
        EXPIRED = 'EXPIRED'

    event_type: Type = attribute(autocast=True)
    assignment: Assignment


class TaskEvent(BaseEvent):
    """Task-related event.

    Attributes:
        event_time: Event datetime.
        task: Task object itself.
    """
    task: Task


class UserBonusEvent(BaseEvent):
    """UserBonus-related event.

    Attributes:
        event_time: Event datetime.
        user_bonus: UserBonus object itself.
    """
    user_bonus: UserBonus


class UserSkillEvent(BaseEvent):
    """UserSkill-related event.

    Attributes:
        event_time: Event datetime.
        event_type: One of the folllowing event types:
            * CREATED
            * MODIFIED
        user_skill: UserSkill object itself.
    """

    @unique
    class Type(BaseEventTypeEnum):
        CREATED = 'CREATED'
        MODIFIED = 'MODIFIED'

    event_type: Type = attribute(autocast=True)
    user_skill: UserSkill


class UserRestrictionEvent(BaseEvent):
    """UserSkill-related event.

    Attributes:
        event_time: Event datetime.
        user_skill: UserSkill object itself.
    """

    user_restriction: UserRestriction


class MessageThreadEvent(BaseEvent):
    """MessageThread-related event.

    Args:
        event_time: Event datetime.
        user_skill: UserSkill object itself.
    """

    message_thread: MessageThread
