"""
Eases dealing with tags.
"""

from exifread.tags.fields import FIELD_DEFINITIONS, FieldType


class IfdTag:
    """
    Represents an IFD tag.
    """

    def __init__(
        self,
        printable: str,
        tag: int,
        field_type: FieldType,
        values,
        field_offset: int,
        field_length: int,
        prefer_printable: bool = True,
    ) -> None:
        # printable version of data
        self.printable = printable
        # tag ID number
        self.tag = tag
        # field type as index into FIELD_TYPES
        self.field_type = field_type
        # offset of start of field in bytes from beginning of IFD
        self.field_offset = field_offset
        # length of data field in bytes
        self.field_length = field_length
        # either string, bytes or list of data items
        # TODO: sort out this type mess!
        self.values = values
        # indication if printable version should be used upon serialization
        self.prefer_printable = prefer_printable

    def __str__(self) -> str:
        return self.printable

    def __repr__(self) -> str:
        try:
            tag = "(0x%04X) %s=%s @ %d" % (
                self.tag,
                FIELD_DEFINITIONS[self.field_type][1],
                self.printable,
                self.field_offset,
            )
        except TypeError:
            tag = "(%s) %s=%s @ %s" % (
                str(self.tag),
                FIELD_DEFINITIONS[self.field_type][1],
                self.printable,
                str(self.field_offset),
            )
        return tag
