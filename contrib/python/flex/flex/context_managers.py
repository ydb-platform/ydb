from flex.exceptions import (
    ErrorDict,
)


class ErrorCollection(ErrorDict):
    def __init__(self, *args, **kwargs):
        import warnings
        warnings.warn("user `flex.exceptions.ErrorDict` instead", DeprecationWarning)
        super(ErrorCollection, self).__init__(*args, **kwargs)
