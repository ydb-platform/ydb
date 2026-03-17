from typing import TYPE_CHECKING

from pyasn1.type.base import SimpleAsn1Type

if TYPE_CHECKING:

    class MibNode:
        def getSyntax(self) -> SimpleAsn1Type:  # type: ignore
            pass
