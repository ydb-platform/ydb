class AttributeDefinition:
    """A definition of an attribute that can be associated with attributes
    in nodes/messages/signals.

    """

    def __init__(self,
                 name,
                 default_value=None,
                 kind=None,
                 type_name=None,
                 minimum=None,
                 maximum=None,
                 choices=None):
        self._name = name
        self._default_value = default_value
        self._kind = kind
        self._type_name = type_name
        self._minimum = minimum
        self._maximum = maximum
        self._choices = choices

    @property
    def name(self):
        """The attribute name as a string.

        """

        return self._name

    @property
    def default_value(self):
        """The default value that this attribute has, or ``None`` if
        unavailable.

        """

        return self._default_value

    @default_value.setter
    def default_value(self, value):
        self._default_value = value

    @property
    def kind(self):
        """The attribute kind (BU_, BO_, SG_), or ``None`` if unavailable.

        """

        return self._kind

    @property
    def type_name(self):
        """The attribute type (INT, HEX, FLOAT, STRING, ENUM), or ``None`` if
        unavailable.

        """

        return self._type_name

    @property
    def minimum(self):
        """The minimum value of the attribute, or ``None`` if unavailable.

        """

        return self._minimum

    @minimum.setter
    def minimum(self, value):
        self._minimum = value

    @property
    def maximum(self):
        """The maximum value of the attribute, or ``None`` if unavailable.

        """

        return self._maximum

    @maximum.setter
    def maximum(self, value):
        self._maximum = value

    @property
    def choices(self):
        """A dictionary mapping attribute values to enumerated choices, or
        ``None`` if unavailable.

        """

        return self._choices

    @choices.setter
    def choices(self, value):
        self._choices = value

    def __repr__(self):
        return f"attribute_definition('{self._name}', {self._default_value})"
