"""
This module is run with mypy to check types can be used correctly externally.
"""

import sys

sys.path.append('.')

from dirty_equals import HasName, HasRepr, IsStr

assert 123 == HasName('int')
assert 123 == HasRepr('123')
assert 123 == HasName(IsStr(regex='i..'))
assert 123 == HasRepr(IsStr(regex=r'\d{3}'))

# type ignore is required (if it wasn't, there would be an error)
assert 123 != HasName(123)  # type: ignore[arg-type]
