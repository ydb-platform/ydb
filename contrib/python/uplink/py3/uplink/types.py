# Local imports
from uplink import converters

__all__ = ["Dict", "List"]

List = converters.TypingConverter.List
"""
A proxy for [`typing.List`](typing.List) that is safe to use in type
hints with Python 3.4 and below.

```python
@get("/users")
def get_users(self) -> types.List[str]:
    \"""Fetches all users\"""
```
"""

Dict = converters.TypingConverter.Dict
"""
A proxy for [`typing.Dict`](typing.Dict) that is safe to use in type
hints with Python 3.4 and below.

```python
@returns.from_json
@get("/users")
def get_users(self) -> types.Dict[str, str]:
    \"""Fetches all users\"""
```
"""
