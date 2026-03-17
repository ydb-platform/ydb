Typing Inspect
==============

[![Build Status](https://travis-ci.org/ilevkivskyi/typing_inspect.svg)](https://travis-ci.org/ilevkivskyi/typing_inspect)

The ``typing_inspect`` module defines experimental API for runtime
inspection of types defined in the Python standard ``typing`` module.
Works with ``typing`` version ``3.7.4`` and later. Example usage:

```python
from typing import Generic, TypeVar, Iterable, Mapping, Union
from typing_inspect import is_generic_type

T = TypeVar('T')

class MyCollection(Generic[T]):
    content: T

assert is_generic_type(Mapping)
assert is_generic_type(Iterable[int])
assert is_generic_type(MyCollection[T])

assert not is_generic_type(int)
assert not is_generic_type(Union[int, T])
```

**Note**: The API is still experimental, if you have ideas/suggestions please
open an issue on [tracker](https://github.com/ilevkivskyi/typing_inspect/issues).
Currently ``typing_inspect`` only supports latest version of ``typing``. This
limitation may be lifted if such requests will appear frequently.

Currently provided functions (see functions docstrings for examples of usage):
* ``is_generic_type(tp)``:
  Test if ``tp`` is a generic type. This includes ``Generic`` itself,
  but excludes special typing constructs such as ``Union``, ``Tuple``,
  ``Callable``, ``ClassVar``.
* ``is_callable_type(tp)``:
  Test ``tp`` is a generic callable type, including subclasses
  excluding non-generic types and callables.
* ``is_tuple_type(tp)``:
  Test if ``tp`` is a generic tuple type, including subclasses excluding
  non-generic classes.
* ``is_union_type(tp)``:
  Test if ``tp`` is a union type.
* ``is_optional_type(tp)``:
  Test if ``tp`` is an optional type (either ``type(None)`` or a direct union to it such as in ``Optional[int]``). Nesting and ``TypeVar``s are not unfolded/inspected in this process.
* ``is_literal_type(tp)``:
  Test if ``tp`` is a literal type.
* ``is_final_type(tp)``:
  Test if ``tp`` is a final type.
* ``is_typevar(tp)``:
  Test if ``tp`` represents a type variable.
* ``is_new_type(tp)``:
  Test if ``tp`` represents a distinct type.
* ``is_classvar(tp)``:
  Test if ``tp`` represents a class variable.
* ``get_origin(tp)``:
  Get the unsubscripted version of ``tp``. Supports generic types, ``Union``,
  ``Callable``, and ``Tuple``. Returns ``None`` for unsupported types.
* ``get_last_origin(tp)``:
  Get the last base of (multiply) subscripted type ``tp``. Supports generic
  types, ``Union``, ``Callable``, and ``Tuple``. Returns ``None`` for
  unsupported types.
* ``get_parameters(tp)``:
  Return type parameters of a parameterizable type ``tp`` as a tuple
  in lexicographic order. Parameterizable types are generic types,
  unions, tuple types and callable types.
* ``get_args(tp, evaluate=False)``:
  Get type arguments of ``tp`` with all substitutions performed. For unions,
  basic simplifications used by ``Union`` constructor are performed.
  If ``evaluate`` is ``False`` (default), report result as nested tuple,
  this matches the internal representation of types. If ``evaluate`` is
  ``True``, then all type parameters are applied (this could be time and
  memory expensive).
* ``get_last_args(tp)``:
  Get last arguments of (multiply) subscripted type ``tp``.
  Parameters for ``Callable`` are flattened.
* ``get_generic_type(obj)``:
  Get the generic type of ``obj`` if possible, or its runtime class otherwise.
* ``get_generic_bases(tp)``:
  Get generic base types of ``tp`` or empty tuple if not possible.
* ``typed_dict_keys(td)``:
  Get ``TypedDict`` keys and their types, or None if ``td`` is not a typed dict.
