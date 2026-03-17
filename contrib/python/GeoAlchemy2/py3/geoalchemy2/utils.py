"""Some utils for the GeoAlchemy2 package."""


def authorized_values_in_docstring(**kwargs):
    """Decorator to replace keywords in docstrings by the actual value of a variable.

    .. Note::
        The keyword must be enclose by <> in the docstring, like <MyKeyword>.
    """

    def inner(func):
        if func.__doc__ is not None:
            for k, v in kwargs.items():
                func.__doc__ = func.__doc__.replace(f"<{k}>", str(v))
        return func

    return inner
