import sys
from typing_inspect import (
    is_generic_type, is_callable_type, is_new_type, is_tuple_type, is_union_type,
    is_optional_type, is_final_type, is_literal_type, is_typevar, is_classvar,
    is_forward_ref, get_origin, get_parameters, get_last_args, get_args, get_bound,
    get_constraints, get_generic_type, get_generic_bases, get_last_origin,
    typed_dict_keys, get_forward_arg, WITH_FINAL, WITH_LITERAL, LEGACY_TYPING)
from unittest import TestCase, main, skipIf, skipUnless
from typing import (
    Union, Callable, Optional, TypeVar, Sequence, AnyStr, Mapping,
    MutableMapping, Iterable, Generic, List, Any, Dict, Tuple, NamedTuple,
)

from mypy_extensions import TypedDict as METypedDict
from typing_extensions import TypedDict as TETypedDict
from typing_extensions import Final
from typing_extensions import Literal

# Does this raise an exception ?
#      from typing import NewType
if sys.version_info < (3, 5, 2):
    WITH_NEWTYPE = False
else:
    from typing import NewType
    WITH_NEWTYPE = True


# Does this raise an exception ?
#      from typing import ClassVar
if sys.version_info < (3, 5, 3):
    WITH_CLASSVAR = False
    CLASSVAR_GENERIC = []
    CLASSVAR_TYPEVAR = []
else:
    from typing import ClassVar
    WITH_CLASSVAR = True
    CLASSVAR_GENERIC = [ClassVar[List[int]], ClassVar]
    CLASSVAR_TYPEVAR = [ClassVar[int]]


# Does this raise an exception ?
#     class Foo(Callable[[int], int]):
#         pass
if sys.version_info < (3, 5, 3):
    SUBCLASSABLE_CALLABLES = False
else:
    SUBCLASSABLE_CALLABLES = True


# Does this raise an exception ?
#     class MyClass(Tuple[str, int]):
#         pass
if sys.version_info < (3, 5, 3):
    SUBCLASSABLE_TUPLES = False
else:
    SUBCLASSABLE_TUPLES = True


# Does this raise an exception ?
#     T = TypeVar('T')
#     Union[T, str][int]
if sys.version_info < (3, 5, 3):
    EXISTING_UNIONS_SUBSCRIPTABLE = False
else:
    EXISTING_UNIONS_SUBSCRIPTABLE = True


# Does this raise an exception ?
#     Union[callable, Callable[..., int]]
if sys.version_info[:3] == (3, 5, 3) or sys.version_info[:3] < (3, 5, 2):
    UNION_SUPPORTS_BUILTIN_CALLABLE = False
else:
    UNION_SUPPORTS_BUILTIN_CALLABLE = True


# Does this raise an exception ?
#   Tuple[T][int]
#   List[Tuple[T]][int]
if sys.version_info[:3] == (3, 5, 3) or sys.version_info[:3] < (3, 5, 2):
    GENERIC_TUPLE_PARAMETRIZABLE = False
else:
    GENERIC_TUPLE_PARAMETRIZABLE = True


# Does this raise an exception ?
#    Dict[T, T][int]
#    Dict[int, Tuple[T, T]][int]
if sys.version_info[:3] == (3, 5, 3) or sys.version_info[:3] < (3, 5, 2):
    GENERIC_WITH_MULTIPLE_IDENTICAL_PARAMETERS_CAN_BE_PARAMETRIZED_ONCE = False
else:
    GENERIC_WITH_MULTIPLE_IDENTICAL_PARAMETERS_CAN_BE_PARAMETRIZED_ONCE = True


# Does this raise an exception ?
#    Callable[[T], int][int]
#    Callable[[], T][int]
if sys.version_info[:3] < (3, 5, 4):
    CALLABLE_CAN_BE_PARAMETRIZED = False
else:
    CALLABLE_CAN_BE_PARAMETRIZED = True


NEW_TYPING = sys.version_info[:3] >= (3, 7, 0)  # PEP 560

PY36_TESTS = """
class TDM(METypedDict):
    x: int
    y: int
class TDE(TETypedDict):
    x: int
    y: int
class Other(dict):
    x: int
    y: int
"""

PY36 = sys.version_info[:3] >= (3, 6, 0)
PY39 = sys.version_info[:3] >= (3, 9, 0)
if PY36:
    exec(PY36_TESTS)


class IsUtilityTestCase(TestCase):
    def sample_test(self, fun, samples, nonsamples):
        msg = "Error asserting that %s(%s) is %s"
        for s in samples:
            self.assertTrue(fun(s), msg=msg % (fun.__name__, str(s), 'True'))
        for s in nonsamples:
            self.assertFalse(fun(s), msg=msg % (fun.__name__, str(s), 'False'))

    def test_generic(self):
        T = TypeVar('T')
        samples = [Generic, Generic[T], Iterable[int], Mapping,
                   MutableMapping[T, List[int]], Sequence[Union[str, bytes]]]
        if PY39:
            samples.extend([list[int], dict[str, list[int]]])
        nonsamples = [int, Union[int, str], Union[int, T], Callable[..., T],
                      Optional, bytes, list] + CLASSVAR_GENERIC
        self.sample_test(is_generic_type, samples, nonsamples)

    def test_callable(self):
        samples = [Callable, Callable[..., int],
                   Callable[[int, int], Iterable[str]]]
        nonsamples = [int, type, 42, [], List[int]]
        if UNION_SUPPORTS_BUILTIN_CALLABLE:
            nonsamples.append(Union[callable, Callable[..., int]])
        self.sample_test(is_callable_type, samples, nonsamples)
        if SUBCLASSABLE_CALLABLES:
            class MyClass(Callable[[int], int]):
                pass
            self.assertTrue(is_callable_type(MyClass))

    def test_tuple(self):
        samples = [Tuple, Tuple[str, int], Tuple[Iterable, ...]]
        if PY39:
            samples.append(tuple[int, str])
        nonsamples = [int, tuple, 42, List[int], NamedTuple('N', [('x', int)])]
        self.sample_test(is_tuple_type, samples, nonsamples)
        if SUBCLASSABLE_TUPLES:
            class MyClass(Tuple[str, int]):
                pass
            self.assertTrue(is_tuple_type(MyClass))

    def test_union(self):
        T = TypeVar('T')
        S = TypeVar('S')
        samples = [Union, Union[T, int], Union[int, Union[T, S]]]
        nonsamples = [int, Union[int, int], [], Iterable[Any]]
        self.sample_test(is_union_type, samples, nonsamples)

    def test_optional_type(self):
        T = TypeVar('T')
        samples = [type(None),                # none type
                   Optional[int],             # direct union to none type 1
                   Optional[T],               # direct union to none type 2
                   Union[int, type(None)],    # direct union to none type 4
                   ]
        if EXISTING_UNIONS_SUBSCRIPTABLE:
            samples += [Optional[T][int],          # direct union to none type 3
                        Union[str, T][type(None)]  # direct union to none type 5
                        ]

        # nested unions are supported
        samples += [Union[str, Optional[int]]]         # nested Union 1
        if EXISTING_UNIONS_SUBSCRIPTABLE:
            samples += [Union[T, str][Optional[int]]]   # nested Union 2

        nonsamples = [int, Union[int, int], [], Iterable[Any], T]
        if EXISTING_UNIONS_SUBSCRIPTABLE:
            nonsamples += [Union[T, str][int]]

        # unfortunately current definition sets these ones as non samples too
        S1 = TypeVar('S1', bound=Optional[int])
        S2 = TypeVar('S2', type(None), str)
        S3 = TypeVar('S3', Optional[int], str)
        S4 = TypeVar('S4', bound=Union[str, Optional[int]])
        nonsamples += [S1, S2, S3,                     # typevar bound or constrained to optional
                       Union[S1, int], S4              # combinations of the above
                       ]
        self.sample_test(is_optional_type, samples, nonsamples)

    @skipIf(not WITH_FINAL, "Final is not available")
    def test_final_type(self):
        samples = [
            Final,
            Final[int],
        ]
        nonsamples = [
            "v",
            1,
            (1, 2, 3),
            int,
            str,
            Union["u", "v"],
        ]
        self.sample_test(is_final_type, samples, nonsamples)

    @skipIf(not WITH_LITERAL, "Literal is not available")
    def test_literal_type(self):
        samples = [
            Literal,
            Literal["v"],
            Literal[1, 2, 3],
        ]
        nonsamples = [
            "v",
            (1, 2, 3),
            int,
            str,
            Union["u", "v"],
        ]
        self.sample_test(is_literal_type, samples, nonsamples)

    def test_typevar(self):
        T = TypeVar('T')
        S_co = TypeVar('S_co', covariant=True)
        samples = [T, S_co]
        nonsamples = [int, Union[T, int], Union[T, S_co], type] + CLASSVAR_TYPEVAR
        self.sample_test(is_typevar, samples, nonsamples)

    @skipIf(not WITH_CLASSVAR, "ClassVar is not present")
    def test_classvar(self):
        T = TypeVar('T')
        samples = [ClassVar, ClassVar[int], ClassVar[List[T]]]
        nonsamples = [int, 42, Iterable, List[int], type, T]
        self.sample_test(is_classvar, samples, nonsamples)

    @skipIf(not WITH_NEWTYPE, "NewType is not present")
    def test_new_type(self):
        T = TypeVar('T')
        samples = [
            NewType('A', int),
            NewType('B', complex),
            NewType('C', List[int]),
            NewType('D', Union['p', 'y', 't', 'h', 'o', 'n']),
            NewType('E', List[Dict[str, float]]),
            NewType('F', NewType('F_', int)),
        ]
        nonsamples = [
            int,
            42,
            Iterable,
            List[int],
            Union["u", "v"],
            type,
            T,
        ]
        self.sample_test(is_new_type, samples, nonsamples)

    def test_is_forward_ref(self):
        samples = []
        nonsamples = []
        for tp in (
            Union["FowardReference", Dict[str, List[int]]],
            Union["FR", List["FR"]],
            Optional["Fref"],
            Union["fRef", int],
            Union["fR", AnyStr],
        ):
            fr, not_fr = get_args(tp)
            samples.append(fr)
            nonsamples.append(not_fr)
        self.sample_test(is_forward_ref, samples, nonsamples)


class GetUtilityTestCase(TestCase):

    @skipIf(NEW_TYPING, "Not supported in Python 3.7")
    def test_last_origin(self):
        T = TypeVar('T')
        self.assertEqual(get_last_origin(int), None)
        if WITH_CLASSVAR:
            self.assertEqual(get_last_origin(ClassVar[int]), None)
        self.assertEqual(get_last_origin(Generic[T]), Generic)
        if EXISTING_UNIONS_SUBSCRIPTABLE:
            self.assertEqual(get_last_origin(Union[T, int][str]), Union[T, int])
        if GENERIC_TUPLE_PARAMETRIZABLE:
            tp = List[Tuple[T, T]][int]
            self.assertEqual(get_last_origin(tp), List[Tuple[T, T]])
        self.assertEqual(get_last_origin(List), List)

    def test_origin(self):
        T = TypeVar('T')
        self.assertEqual(get_origin(int), None)
        if WITH_CLASSVAR:
            self.assertEqual(get_origin(ClassVar[int]), None)
        self.assertEqual(get_origin(Generic), Generic)
        self.assertEqual(get_origin(Generic[T]), Generic)
        if PY39:
            self.assertEqual(get_origin(list[int]), list)
        if GENERIC_TUPLE_PARAMETRIZABLE:
            tp = List[Tuple[T, T]][int]
            self.assertEqual(get_origin(tp), list if NEW_TYPING else List)

    def test_parameters(self):
        T = TypeVar('T')
        S_co = TypeVar('S_co', covariant=True)
        U = TypeVar('U')
        self.assertEqual(get_parameters(int), ())
        self.assertEqual(get_parameters(Generic), ())
        self.assertEqual(get_parameters(Union), ())
        if not LEGACY_TYPING:
            self.assertEqual(get_parameters(List[int]), ())
        else:
            # in 3.5.3 a list has no __args__ and instead they are used in __parameters__
            # in 3.5.1 the behaviour is normal again.
            pass
        self.assertEqual(get_parameters(Generic[T]), (T,))
        self.assertEqual(get_parameters(Tuple[List[T], List[S_co]]), (T, S_co))
        if EXISTING_UNIONS_SUBSCRIPTABLE:
            self.assertEqual(get_parameters(Union[S_co, Tuple[T, T]][int, U]), (U,))
        self.assertEqual(get_parameters(Mapping[T, Tuple[S_co, T]]), (T, S_co))
        if PY39:
            self.assertEqual(get_parameters(dict[int, T]), (T,))

    @skipIf(NEW_TYPING, "Not supported in Python 3.7")
    def test_last_args(self):
        T = TypeVar('T')
        S = TypeVar('S')
        self.assertEqual(get_last_args(int), ())
        self.assertEqual(get_last_args(Union), ())
        if WITH_CLASSVAR:
            self.assertEqual(get_last_args(ClassVar[int]), (int,))
        self.assertEqual(get_last_args(Union[T, int]), (T, int))
        self.assertEqual(get_last_args(Union[str, int]), (str, int))
        self.assertEqual(get_last_args(Tuple[T, int]), (T, int))
        self.assertEqual(get_last_args(Tuple[str, int]), (str, int))
        self.assertEqual(get_last_args(Generic[T]), (T, ))
        if GENERIC_TUPLE_PARAMETRIZABLE:
            tp = Iterable[Tuple[T, S]][int, T]
            self.assertEqual(get_last_args(tp), (int, T))
        if LEGACY_TYPING:
            self.assertEqual(get_last_args(Callable[[T, S], int]), (T, S))
            self.assertEqual(get_last_args(Callable[[], int]), ())
        else:
            self.assertEqual(get_last_args(Callable[[T, S], int]), (T, S, int))
            self.assertEqual(get_last_args(Callable[[], int]), (int,))

    @skipIf(NEW_TYPING, "Not supported in Python 3.7")
    def test_args(self):
        if EXISTING_UNIONS_SUBSCRIPTABLE:
            T = TypeVar('T')
            self.assertEqual(get_args(Union[int, Tuple[T, int]][str]),
                             (int, (Tuple, str, int)))
            self.assertEqual(get_args(Union[int, Union[T, int], str][int]),
                             (int, str))
        self.assertEqual(get_args(int), ())

    def test_args_evaluated(self):
        T = TypeVar('T')
        if EXISTING_UNIONS_SUBSCRIPTABLE:
            self.assertEqual(get_args(Union[int, Tuple[T, int]][str], evaluate=True),
                             (int, Tuple[str, int]))
        if GENERIC_WITH_MULTIPLE_IDENTICAL_PARAMETERS_CAN_BE_PARAMETRIZED_ONCE:
            tp = Dict[int, Tuple[T, T]][Optional[int]]
            self.assertEqual(get_args(tp, evaluate=True),
                             (int, Tuple[Optional[int], Optional[int]]))
        if CALLABLE_CAN_BE_PARAMETRIZED:
            tp = Callable[[], T][int]
            self.assertEqual(get_args(tp, evaluate=True), ([], int,))

        self.assertEqual(get_args(Union[int, Callable[[Tuple[T, ...]], str]], evaluate=True),
                         (int, Callable[[Tuple[T, ...]], str]))

        # ClassVar special-casing
        if WITH_CLASSVAR:
            self.assertEqual(get_args(ClassVar, evaluate=True), ())
            self.assertEqual(get_args(ClassVar[int], evaluate=True), (int,))

        # Final special-casing
        if WITH_FINAL:
            self.assertEqual(get_args(Final, evaluate=True), ())
            self.assertEqual(get_args(Final[int], evaluate=True), (int,))

        # Literal special-casing
        if WITH_LITERAL:
            self.assertEqual(get_args(Literal, evaluate=True), ())
            self.assertEqual(get_args(Literal["value"], evaluate=True), ("value",))
            self.assertEqual(get_args(Literal[1, 2, 3], evaluate=True), (1, 2, 3))

        if PY39:
            self.assertEqual(get_args(list[int]), (int,))
            self.assertEqual(get_args(tuple[int, str]), (int, str))
            self.assertEqual(get_args(list[list[int]]), (list[int],))
            # This would return (~T,) before Python 3.9.
            self.assertEqual(get_args(List), ())

    def test_bound(self):
        T = TypeVar('T')
        TB = TypeVar('TB', bound=int)
        self.assertEqual(get_bound(T), None)
        self.assertEqual(get_bound(TB), int)

    def test_constraints(self):
        T = TypeVar('T')
        TC = TypeVar('TC', int, str)
        self.assertEqual(get_constraints(T), ())
        self.assertEqual(get_constraints(TC), (int, str))

    def test_generic_type(self):
        T = TypeVar('T')
        class Node(Generic[T]): pass
        self.assertIs(get_generic_type(Node()), Node)
        if not LEGACY_TYPING:
            self.assertIs(get_generic_type(Node[int]()), Node[int])
            self.assertIs(get_generic_type(Node[T]()), Node[T],)
        else:
            # Node[int]() was creating an object of NEW type Node[~T]
            # and Node[T]() was creating an object of NEW type Node[~T]
            pass
        self.assertIs(get_generic_type(1), int)

    def test_generic_bases(self):
        class MyClass(List[int], Mapping[str, List[int]]): pass
        self.assertEqual(get_generic_bases(MyClass),
                         (List[int], Mapping[str, List[int]]))
        self.assertEqual(get_generic_bases(int), ())

    @skipUnless(PY36, "Python 3.6 required")
    def test_typed_dict_mypy_extension(self):
        TDOld = METypedDict("TDOld", {'x': int, 'y': int})
        self.assertEqual(typed_dict_keys(TDM), {'x': int, 'y': int})
        self.assertEqual(typed_dict_keys(TDOld), {'x': int, 'y': int})
        self.assertIs(typed_dict_keys(dict), None)
        self.assertIs(typed_dict_keys(Other), None)
        self.assertIsNot(typed_dict_keys(TDM), TDM.__annotations__)

    @skipUnless(PY36, "Python 3.6 required")
    def test_typed_dict_typing_extension(self):
        TDOld = TETypedDict("TDOld", {'x': int, 'y': int})
        self.assertEqual(typed_dict_keys(TDE), {'x': int, 'y': int})
        self.assertEqual(typed_dict_keys(TDOld), {'x': int, 'y': int})
        self.assertIs(typed_dict_keys(dict), None)
        self.assertIs(typed_dict_keys(Other), None)
        self.assertIsNot(typed_dict_keys(TDE), TDE.__annotations__)

    @skipIf(
        (3, 5, 2) > sys.version_info[:3] >= (3, 5, 0),
        "get_args doesn't work in Python 3.5.0 and 3.5.1 for type"
        " List and ForwardRef arg"
    )
    def test_get_forward_arg(self):
        tp = List["FRef"]
        fr = get_args(tp)[0]
        self.assertEqual(get_forward_arg(fr), "FRef")
        self.assertEqual(get_forward_arg(tp), None)


if __name__ == '__main__':
    main()
