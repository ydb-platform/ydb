import logging

import six
from lxml import etree

from zeep.exceptions import ValidationError
from zeep.xsd.const import Nil, xsd_ns, xsi_ns
from zeep.xsd.types.any import AnyType

logger = logging.getLogger(__name__)

__all__ = ["AnySimpleType"]


@six.python_2_unicode_compatible
class AnySimpleType(AnyType):
    _default_qname = xsd_ns("anySimpleType")

    def __init__(self, qname=None, is_global=False):
        super(AnySimpleType, self).__init__(
            qname or etree.QName(self._default_qname), is_global
        )

    def __call__(self, *args, **kwargs):
        """Return the xmlvalue for the given value.

        Expects only one argument 'value'.  The args, kwargs handling is done
        here manually so that we can return readable error messages instead of
        only '__call__ takes x arguments'

        """
        num_args = len(args) + len(kwargs)
        if num_args != 1:
            raise TypeError(
                (
                    "%s() takes exactly 1 argument (%d given). "
                    + "Simple types expect only a single value argument"
                )
                % (self.__class__.__name__, num_args)
            )

        if kwargs and "value" not in kwargs:
            raise TypeError(
                (
                    "%s() got an unexpected keyword argument %r. "
                    + "Simple types expect only a single value argument"
                )
                % (self.__class__.__name__, next(six.iterkeys(kwargs)))
            )

        value = args[0] if args else kwargs["value"]
        return self.xmlvalue(value)

    def __eq__(self, other):
        return (
            other is not None
            and self.__class__ == other.__class__
            and self.__dict__ == other.__dict__
        )

    def __str__(self):
        return "%s(value)" % (self.__class__.__name__)

    def parse_xmlelement(
        self, xmlelement, schema=None, allow_none=True, context=None, schema_type=None
    ):
        if xmlelement.text is None:
            return
        try:
            return self.pythonvalue(xmlelement.text)
        except (TypeError, ValueError):
            logger.exception("Error during xml -> python translation")
            return None

    def pythonvalue(self, xmlvalue):
        raise NotImplementedError(
            "%s.pytonvalue() not implemented" % self.__class__.__name__
        )

    def render(self, parent, value, xsd_type=None, render_path=None):
        if value is Nil:
            parent.set(xsi_ns("nil"), "true")
            return
        parent.text = self.xmlvalue(value)

    def signature(self, schema=None, standalone=True):
        return self.get_prefixed_name(schema)

    def validate(self, value, required=False):
        if required and value is None:
            raise ValidationError("Value is required")
