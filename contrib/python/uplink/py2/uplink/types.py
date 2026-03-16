# Local imports
from uplink import converters

__all__ = ["List", "Dict"]

List = converters.TypingConverter.List
"""
A proxy for :py:class:`typing.List` that is safe to use in type
hints with Python 3.4 and below.

.. code-block:: python

    @get("/users")
    def get_users(self) -> types.List[str]:
        \"""Fetches all users\"""
"""

Dict = converters.TypingConverter.Dict
"""
A proxy for :py:class:`typing.Dict` that is safe to use in type
hints with Python 3.4 and below.

.. code-block:: python

    @returns.from_json
    @get("/users")
    def get_users(self) -> types.Dict[str, str]:
        \"""Fetches all users\"""
"""
