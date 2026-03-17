__all__ = [
    'UserBonus',
    'UserBonusCreateRequestParameters',
    'UserBonusesCreateRequestParameters',
]

from attr.validators import optional, instance_of
import datetime
from decimal import Decimal
from typing import Dict

from .primitives.base import BaseTolokaObject
from .primitives.parameter import IdempotentOperationParameters
from ..util._codegen import attribute
from ..util._docstrings import inherit_docstrings


class UserBonus(BaseTolokaObject):
    """A bonus payment to a Toloker.

    Learn more about [Bonuses](https://toloka.ai/docs/guide/bonus/).

    Attributes:
        id: The ID of the bonus.
        user_id: The ID of the Toloker.
        amount: The amount of the bonus in US dollars.
        assignment_id: The ID of the assignment the bonus is issued for.
        private_comment: A comment visible to the requester only.
        without_message:
            * `False` — A message is sent to the Toloker when the bonus is issued.
            * `True` — There is no message sent to the Toloker.

            Default value: `False`.
        public_title: A message title. The title can be provided in several languages.
        public_message: A message text. It can be provided in several languages.
        created: The UTC date and time when the bonus was issued. Read-only field.

    Example:
        An example of issuing a bonus. A message to a Toloker is prepared in two languages.

        >>> from decimal import Decimal
        >>> new_bonus = toloka_client.create_user_bonus(
        >>>     toloka.client.UserBonus(
        >>>         user_id='a1b0b42923c429daa2c764d7ccfc364d',
        >>>         amount=Decimal('0.50'),
        >>>         public_title={
        >>>             'EN': 'Good Job!',
        >>>             'RU': 'Молодец!',
        >>>         },
        >>>         public_message={
        >>>             'EN': 'Ten tasks were completed',
        >>>             'RU': 'Выполнено 10 заданий',
        >>>         },
        >>>         assignment_id='000015fccc--63bfc4c358d7a46c32a7b233'
        >>>     )
        >>> )
        ...
    """

    user_id: str
    amount: Decimal = attribute(validator=optional(instance_of(Decimal)))

    private_comment: str
    public_title: Dict[str, str]
    public_message: Dict[str, str]
    without_message: bool
    assignment_id: str

    # Readonly
    id: str = attribute(readonly=True)
    created: datetime.datetime = attribute(readonly=True)


@inherit_docstrings
class UserBonusCreateRequestParameters(IdempotentOperationParameters):
    """Parameters for issuing a bonus payment to a Toloker.

    The [create_user_bonus](toloka.client.TolokaClient.create_user_bonus.md) method uses these parameters.
    """
    ...


@inherit_docstrings
class UserBonusesCreateRequestParameters(UserBonusCreateRequestParameters):
    """Parameters for issuing bonus payments to Tolokers.

    The [create_user_bonuses](toloka.client.TolokaClient.create_user_bonuses.md) and [create_user_bonuses_async](toloka.client.TolokaClient.create_user_bonuses_async.md) methods use these parameters.

    Attributes:
        skip_invalid_items: Bonus validation option:
            * `True` — All valid bonuses are issued. If a bonus doesn't pass validation, then it isn't issued to a Toloker. All such bonuses are listed in the response.
            * `False` — If any bonus doesn't pass validation, then the operation is cancelled and no bonuses are issued to Tolokers.

            Default value: `False`.
    """
    skip_invalid_items: bool
