
"""
Param: A declarative framework for managing parameters and reactive programming in Python.

Param is a lightweight library for defining and managing user-modifiable parameters,
designed to simplify Python programs and enhance their readability, maintainability,
and robustness. In addition Param provides the `rx` framework for reactive programming.

Param is well-suited for use in scientific computing, data analysis tools,
graphical user interfaces (GUIs), and any Python application where well-defined,
validated parameters are needed.

References
----------
For detailed documentation, see https://param.holoviz.org/.

Examples
--------
Here is an example of using `param.Parameterized` to define a class with validated parameters:

>>> import param
>>> class MyClass(param.Parameterized):
...     my_number = param.Number(default=1, bounds=(0, 10))
...     my_list = param.List(default=[1, 2, 3], item_type=int)

>>> obj = MyClass()
>>> obj.my_number = 5  # Valid
>>> obj.my_number = 15  # Raises ValueError: must be in range (0, 10)

Here is an example of using `param.rx` to define a reactive expression:

>>> import param
>>> rx_value = param.rx([1,2,3])
>>> rx_value.rx.len()
3

Lets update the reactive value and check its length:

>>> rx_value.rx.value = [1,2,3,4]
>>> rx_value.rx.len()
4
"""
import os

from . import version
from .depends import depends
from .parameterized import (
    Parameterized, Parameter, Skip, String, ParameterizedFunction,
    ParamOverrides, Undefined, get_logger, ParameterizedABC,
)
from .parameterized import (output, script_repr,
                            discard_events, edit_constant)
from .parameterized import shared_parameters
from .parameterized import logging_level
from .parameterized import DEBUG, VERBOSE, INFO, WARNING, ERROR, CRITICAL
from .parameters import (
    guess_param_types,
    param_union,
    parameterized_class,
    guess_bounds,
    get_soft_bounds,
    resolve_path,
    Time,
    Infinity,
    Dynamic,
    Bytes,
    Number,
    Integer,
    Magnitude,
    Boolean,
    Tuple,
    NumericTuple,
    XYCoordinates,
    Callable,
    Action,
    Composite,
    SelectorBase,
    ListProxy,
    Selector,
    ObjectSelector,
    ClassSelector,
    List,
    HookList,
    Dict,
    Array,
    DataFrame,
    Series,
    Path,
    Filename,
    Foldername,
    FileSelector,
    ListSelector,
    MultiFileSelector,
    Date,
    CalendarDate,
    Color,
    Range,
    DateRange,
    CalendarDateRange,
    Event,
)
from .reactive import bind, rx
from ._utils import (
    descendents,
    concrete_descendents,
    exceptions_summarized,
    _is_number,
)


# Define '__version__'
try:
    # For performance reasons on imports, avoid importing setuptools_scm
    # if not in a .git folder
    if os.path.exists(os.path.join(os.path.dirname(__file__), "..", ".git")):
        # If setuptools_scm is installed (e.g. in a development environment with
        # an editable install), then use it to determine the version dynamically.
        from setuptools_scm import get_version

        # This will fail with LookupError if the package is not installed in
        # editable mode or if Git is not installed.
        __version__ = get_version(root="..", relative_to=__file__)
    else:
        raise FileNotFoundError
except (ImportError, LookupError, FileNotFoundError):
    # As a fallback, use the version that is hard-coded in the file.
    try:
        # __version__ was added in _version in setuptools-scm 7.0.0, we rely on
        # the hopefully stable version variable.
        from ._version import version as __version__
    except (ModuleNotFoundError, ImportError):
        # Either _version doesn't exist (ModuleNotFoundError) or version isn't
        # in _version (ImportError). ModuleNotFoundError is a subclass of
        # ImportError, let's be explicit anyway.

        # Try something else:
        from importlib.metadata import version as mversion, PackageNotFoundError

        try:
            __version__ = mversion("param")
        except PackageNotFoundError:
            # The user is probably trying to run this without having installed
            # the package.
            __version__ = "0.0.0+unknown"

#: Top-level object to allow messaging not tied to a particular
#: Parameterized object, as in 'param.main.warning("Invalid option")'.
main=Parameterized(name="main")


# A global random seed (integer or rational) available for controlling
# the behaviour of Parameterized objects with random state.
random_seed = 42

__all__ = (
    'Action',
    'Array',
    'Boolean',
    'Bytes',
    'CRITICAL',
    'CalendarDate',
    'CalendarDateRange',
    'Callable',
    'ClassSelector',
    'Color',
    'Composite',
    'DEBUG',
    'DataFrame',
    'Date',
    'DateRange',
    'Dict',
    'Dynamic',
    'ERROR',
    'Event',
    'FileSelector',
    'Filename',
    'Foldername',
    'HookList',
    'INFO',
    'Infinity',
    'Integer',
    'List',
    'ListProxy',
    'ListSelector',
    'Magnitude',
    'MultiFileSelector',
    'Number',
    'NumericTuple',
    'ObjectSelector',
    'ParamOverrides',
    'Parameter',
    'Parameterized',
    'ParameterizedABC',
    'ParameterizedFunction',
    'Path',
    'Range',
    'Selector',
    'SelectorBase',
    'Series',
    'Skip',
    'String',
    'Time',
    'Tuple',
    'Undefined',
    'VERBOSE',
    'WARNING',
    'XYCoordinates',
    '__version__',
    '_is_number',
    'bind',
    'concrete_descendents',
    'depends',
    'descendents',
    'discard_events',
    'edit_constant',
    'exceptions_summarized',
    'get_logger',
    'get_soft_bounds',
    'guess_bounds',
    'guess_param_types',
    'logging_level',
    'main',
    'output',
    'param_union',
    'parameterized_class',
    'random_seed',
    'resolve_path',
    'rx',
    'script_repr',
    'serializer',
    'shared_parameters',
    'version',
)
