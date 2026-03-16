class Attribute:
    """An attribute that can be associated with nodes/messages/signals.

    """

    def __init__(self,
                 value,
                 definition):
        self._value = value
        self._definition = definition

    @property
    def name(self):
        """The attribute name as a string.

        """

        return self._definition.name

    @property
    def value(self):
        """The value that this attribute has.

        """

        return self._value

    @value.setter
    def value(self, value):
        self._value = value

    @property
    def definition(self):
        """The attribute definition.

        """

        return self._definition

    def __repr__(self):
        return f"attribute('{self.name}', {self.value})"
