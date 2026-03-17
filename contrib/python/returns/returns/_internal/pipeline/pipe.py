from returns._internal.pipeline.flow import flow


def pipe(*functions):
    """
    Allows to compose a value and up to 7 functions that use this value.

    We use a custom ``mypy`` plugin to make sure types are correct.
    Otherwise, it is currently impossible to properly type this function.

    Each next function uses the previous result as an input parameter.
    Here's how it should be used:

    .. code:: python

       >>> from returns.pipeline import pipe

       >>> # => executes: str(float(int('1')))
       >>> assert pipe(int, float, str)('1') == '1.0'

    This function is closely related
    to :func:`pipe <returns._internal.pipeline.flow.flow>`:

    .. code:: python

      >>> from returns.pipeline import flow
      >>> assert pipe(int, float, str)('1') == flow('1', int, float, str)

    See also:
        - https://stackoverflow.com/a/41585450/4842742
        - https://github.com/gcanti/fp-ts/blob/master/src/pipeable.ts

    """
    return lambda instance: flow(instance, *functions)
