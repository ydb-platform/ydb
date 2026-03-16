__all__ = ['Requester']
from attr.validators import optional, instance_of
from decimal import Decimal
from typing import Dict

from .primitives.base import BaseTolokaObject
from ..util._codegen import attribute


class Requester(BaseTolokaObject):
    """Information about a requester.

    Attributes:
        id: The requester's ID.
        balance: Account balance in dollars.
        public_name: The requester's name in Toloka.
        company: Information about a requester's company.
    """

    class Company(BaseTolokaObject):
        """Information about a requester's company.

            Attributes:
                id: The ID of the company.
                superintendent_id: The ID of the client who owns the company account.
        """
        id: str
        superintendent_id: str

    id: str
    balance: Decimal = attribute(validator=optional(instance_of(Decimal)))
    public_name: Dict[str, str]
    company: Company
