from __future__ import annotations

from typing import TypeAlias

# the base class UnicodeError doesn't have attributes like start / end
AnyUnicodeError: TypeAlias = UnicodeEncodeError | UnicodeDecodeError
