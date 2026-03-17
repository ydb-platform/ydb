# vim: set ai et ts=4 sts=4 sw=4:
from pyasn1.type import univ, namedtype

class PBEParameter(univ.Sequence):
    componentType = namedtype.NamedTypes(
        namedtype.NamedType('salt', univ.OctetString()),
        namedtype.NamedType('iterationCount', univ.Integer())
    )
