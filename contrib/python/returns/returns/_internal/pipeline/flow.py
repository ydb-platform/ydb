from functools import reduce
from typing import TypeVar

_InstanceType = TypeVar('_InstanceType')
_PipelineStepType = TypeVar('_PipelineStepType')
_ReturnType = TypeVar('_ReturnType')


def flow(
    instance: _InstanceType,
    *functions: _PipelineStepType,
) -> _ReturnType:  # type: ignore[type-var]
    """
    Allows to compose a value and up to multiple functions that use this value.

    All starts with the value itself.
    Each next function uses the previous result as an input parameter.

    We use a custom ``mypy`` plugin to make sure types are correct.
    Otherwise, it is currently impossible to properly type this function.

    Currently, ``flow`` has a hard limit of 21 steps.
    Because, it is not possible to type it otherwise.
    We need a hard limit.
    See: https://github.com/dry-python/returns/issues/461

    Here's how it should be used:

    .. code:: python

       >>> from returns.pipeline import flow

       >>> # => executes: str(float(int('1')))
       >>> assert flow('1', int, float, str) == '1.0'

    This function is closely related
    to :func:`pipe <returns._internal.pipeline.pipe.pipe>`:

    .. code:: python

      >>> from returns.pipeline import pipe
      >>> assert flow('1', int, float, str) == pipe(int, float, str)('1')

    See also:
        - https://stackoverflow.com/a/41585450/4842742
        - https://github.com/gcanti/fp-ts/blob/master/src/pipeable.ts

    Requires our :ref:`mypy plugin <mypy-plugins>`.
    """
    return reduce(  # type: ignore
        lambda composed, function: function(composed),  # type: ignore
        functions,
        instance,
    )
