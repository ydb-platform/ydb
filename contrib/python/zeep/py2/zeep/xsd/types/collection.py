from zeep.utils import get_base_class
from zeep.xsd.types.simple import AnySimpleType

__all__ = ["ListType", "UnionType"]


class ListType(AnySimpleType):
    """Space separated list of simpleType values"""

    def __init__(self, item_type):
        self.item_type = item_type
        super(ListType, self).__init__()

    def __call__(self, value):
        return value

    def render(self, parent, value, xsd_type=None, render_path=None):
        parent.text = self.xmlvalue(value)

    def resolve(self):
        self.item_type = self.item_type.resolve()
        self.base_class = self.item_type.__class__
        return self

    def xmlvalue(self, value):
        item_type = self.item_type
        return " ".join(item_type.xmlvalue(v) for v in value)

    def pythonvalue(self, value):
        if not value:
            return []
        item_type = self.item_type
        return [item_type.pythonvalue(v) for v in value.split()]

    def signature(self, schema=None, standalone=True):
        return self.item_type.signature(schema) + "[]"


class UnionType(AnySimpleType):
    """Simple type existing out of multiple other types"""

    def __init__(self, item_types):
        self.item_types = item_types
        self.item_class = None
        assert item_types
        super(UnionType, self).__init__(None)

    def resolve(self):
        self.item_types = [item.resolve() for item in self.item_types]
        base_class = get_base_class(self.item_types)
        if issubclass(base_class, AnySimpleType) and base_class != AnySimpleType:
            self.item_class = base_class
        return self

    def signature(self, schema=None, standalone=True):
        return ""

    def parse_xmlelement(
        self, xmlelement, schema=None, allow_none=True, context=None, schema_type=None
    ):
        if self.item_class:
            return self.item_class().parse_xmlelement(
                xmlelement, schema, allow_none, context
            )
        return xmlelement.text

    def pythonvalue(self, value):
        if self.item_class:
            return self.item_class().pythonvalue(value)
        return value

    def xmlvalue(self, value):
        if self.item_class:
            return self.item_class().xmlvalue(value)
        return value
