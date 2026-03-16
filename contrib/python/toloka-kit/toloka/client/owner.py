__all__ = ['Owner']
from .primitives.base import BaseTolokaObject


class Owner(BaseTolokaObject):
    """Information about a requester who owns some object.

    Attributes:
        id: The ID of the requester.
        myself: A match of the owner OAuth token with the token that is used in the request:
                * `True` — The tokens are the same.
                * `False` — The tokens are different.
        company_id: The ID of the requester's company.
    """

    id: str
    myself: bool
    company_id: str
