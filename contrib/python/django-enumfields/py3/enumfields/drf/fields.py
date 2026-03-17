from django.utils.encoding import force_str

from rest_framework.fields import ChoiceField


class EnumField(ChoiceField):
    def __init__(self, enum, lenient=False, ints_as_names=False, **kwargs):
        """
        :param enum: The enumeration class.
        :param lenient: Whether to allow lenient parsing (case-insensitive, by value or name)
        :type lenient: bool
        :param ints_as_names: Whether to serialize integer-valued enums by their name, not the integer value
        :type ints_as_names: bool
        """
        self.enum = enum
        self.lenient = lenient
        self.ints_as_names = ints_as_names
        kwargs['choices'] = tuple((e.value, getattr(e, 'label', e.name)) for e in self.enum)
        super().__init__(**kwargs)

    def to_representation(self, instance):
        if instance in ('', None):
            return instance
        try:
            if not isinstance(instance, self.enum):
                instance = self.enum(instance)  # Try to cast it
            if self.ints_as_names and isinstance(instance.value, int):
                # If the enum value is an int, assume the name is more representative
                return instance.name.lower()
            return instance.value
        except ValueError:
            raise ValueError('Invalid value [{!r}] of enum {}'.format(instance, self.enum.__name__))

    def to_internal_value(self, data):
        if isinstance(data, self.enum):
            return data
        try:
            # Convert the value using the same mechanism DRF uses
            converted_value = self.choice_strings_to_values[str(data)]
            return self.enum(converted_value)
        except (ValueError, KeyError):
            pass

        if self.lenient:
            # Normal logic:
            for choice in self.enum:
                if choice.name == data or choice.value == data:
                    return choice

            # Case-insensitive logic:
            l_data = force_str(data).lower()
            for choice in self.enum:
                if choice.name.lower() == l_data or force_str(choice.value).lower() == l_data:
                    return choice

        # Fallback (will likely just raise):
        return super().to_internal_value(data)
