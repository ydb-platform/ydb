from zeep.xsd.utils import create_prefixed_name

__all__ = ["Type"]


class Type(object):
    def __init__(self, qname=None, is_global=False):
        self.qname = qname
        self.name = qname.localname if qname else None
        self._resolved = False
        self.is_global = is_global

    def get_prefixed_name(self, schema):
        return create_prefixed_name(self.qname, schema)

    def accept(self, value):
        raise NotImplementedError

    @property
    def accepted_types(self):
        return tuple()

    def validate(self, value, required=False):
        return

    def parse_kwargs(self, kwargs, name, available_kwargs):
        value = None
        name = name or self.name

        if name in available_kwargs:
            value = kwargs[name]
            available_kwargs.remove(name)
            return {name: value}
        return {}

    def parse_xmlelement(
        self, xmlelement, schema=None, allow_none=True, context=None, schema_type=None
    ):
        raise NotImplementedError(
            "%s.parse_xmlelement() is not implemented" % self.__class__.__name__
        )

    def parsexml(self, xml, schema=None):
        raise NotImplementedError

    def render(self, parent, value, xsd_type=None, render_path=None):
        raise NotImplementedError(
            "%s.render() is not implemented" % self.__class__.__name__
        )

    def resolve(self):
        raise NotImplementedError(
            "%s.resolve() is not implemented" % self.__class__.__name__
        )

    def extend(self, child):
        raise NotImplementedError(
            "%s.extend() is not implemented" % self.__class__.__name__
        )

    def restrict(self, child):
        raise NotImplementedError(
            "%s.restrict() is not implemented" % self.__class__.__name__
        )

    @property
    def attributes(self):
        return []

    @classmethod
    def signature(cls, schema=None, standalone=True):
        return ""
