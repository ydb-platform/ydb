from .request_options import RequestOptions


class Filter:
    def __init__(self, field, operator, value):
        self.field = field
        self.operator = operator
        self._value = None
        self.value = value

    def __str__(self):
        value_string = str(self._value)
        if isinstance(self._value, list):
            # this should turn the string representation of the list
            # from ['<string1>', '<string2>', ...]
            # to [<string1>,<string2>]
            # so effectively, remove any spaces between "," and "'" and then remove all "'"
            value_string = value_string.replace(", '", ",'").replace("'", "")
        return f"{self.field}:{self.operator}:{value_string}"

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, filter_value):
        if isinstance(filter_value, list) and self.operator != RequestOptions.Operator.In:
            error = "Filter values can only be a list if the operator is 'in'."
            raise ValueError(error)
        else:
            self._value = filter_value
