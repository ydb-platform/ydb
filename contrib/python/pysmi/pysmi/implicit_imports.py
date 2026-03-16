"""Per-MIB implicit imports workarounds.

Populate this mapping only when necessary. Keep entries minimal and
document why each entry exists (vendor, MIB, date).
"""

from typing import Dict, List, Tuple

IMPLICIT_IMPORTS: Dict[str, List[Tuple[str, str]]] = {
    # 'JUNIPER-SMI': [('SNMPv2-SMI', 'Opaque')],  ## Example entry. Its latest revision missed importing Opaque for Integer64 textual convention.
}
