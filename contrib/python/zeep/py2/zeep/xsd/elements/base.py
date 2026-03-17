class Base(object):
    @property
    def accepts_multiple(self):
        return self.max_occurs != 1

    @property
    def default_value(self):
        return None

    @property
    def is_optional(self):
        return self.min_occurs == 0

    def parse_args(self, args, index=0):
        result = {}
        if not args:
            return result, args, index

        value = args[index]
        index += 1
        return {self.attr_name: value}, args, index

    def parse_kwargs(self, kwargs, name, available_kwargs):
        raise NotImplementedError()

    def parse_xmlelements(self, xmlelements, schema, name=None, context=None):
        """Consume matching xmlelements and call parse() on each of them

        :param xmlelements: Dequeue of XML element objects
        :type xmlelements: collections.deque of lxml.etree._Element
        :param schema: The parent XML schema
        :type schema: zeep.xsd.Schema
        :param name: The name of the parent element
        :type name: str
        :param context: Optional parsing context (for inline schemas)
        :type context: zeep.xsd.context.XmlParserContext
        :return: dict or None

        """
        raise NotImplementedError()

    def signature(self, schema=None, standalone=False):
        return ""
