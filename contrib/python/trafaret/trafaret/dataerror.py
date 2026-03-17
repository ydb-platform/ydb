from .lib import _empty, STR_TYPES


class DataError(ValueError):
    """
    Error with data preserve
    error can be a message or None if error raised in childs
    data can be anything
    """
    __slots__ = ['error', 'name', 'value', 'trafaret', 'code']

    error_code = 'unknown'

    def __init__(self, error=None, name=None, value=_empty, trafaret=None, code=None):
        """
        :attribute error: can be a string or a dict[string, dataerror]
        :attribute name:
        :attribute value: validated value that leads to this error
        :attribute trafaret: trafaret raised error
        :attribute code: code for error, like `value_is_too_big`
        """
        if not isinstance(error, STR_TYPES + (dict, )):
            raise RuntimeError('Only str or dict is supported, got %r' % error)
        self.error = error
        self.name = name
        self.value = value
        self.trafaret = trafaret
        self.code = code or self.__class__.error_code
        # if self.code == 'unknown':
        #     raise RuntimeError()

    def __str__(self, value=False):
        if value and self.value != _empty:
            return '%s, got %r' % (str(self.error), self.value)
        else:
            return str(self.error)

    def __repr__(self):
        return 'DataError(%r)' % str(self)

    def to_struct(self, value=False):
        if isinstance(self.error, dict):
            return {
                'code': self.code,
                'nested': dict(
                    (k, v.to_struct(value=value) if isinstance(v, DataError) else v)
                    for k, v in self.error.items()
                ),
            }
        return {
            'code': self.code,
            'message': self.__str__(value=value),
        }

    def as_dict(self, value=False):
        """Use `to_struct` if need consistency"""
        if not isinstance(self.error, dict):
            return self.__str__(value=value)

        return dict(
            (k, v.as_dict(value=value) if isinstance(v, DataError) else v)
            for k, v in self.error.items()
        )
