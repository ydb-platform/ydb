"""schema is a library for validating Python data structures, such as those
obtained from config-files, forms, external services or command-line
parsing, converted from JSON/YAML (or something else) to Python data-types."""

import inspect
import re
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Generic,
    Iterable,
    List,
    NoReturn,
    Sequence,
    Set,
    Sized,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
)

# Use TYPE_CHECKING to determine the correct type hint but avoid runtime import errors
if TYPE_CHECKING:
    # Only for type checking purposes, we import the standard ExitStack
    from contextlib import ExitStack
else:
    try:
        from contextlib import ExitStack  # Python 3.3 and later
    except ImportError:
        from contextlib2 import ExitStack  # Python 2.x/3.0-3.2 fallback


__version__ = "0.7.8"
__all__ = [
    "Schema",
    "And",
    "Or",
    "Regex",
    "Optional",
    "Use",
    "Forbidden",
    "Const",
    "Literal",
    "SchemaError",
    "SchemaWrongKeyError",
    "SchemaMissingKeyError",
    "SchemaForbiddenKeyError",
    "SchemaUnexpectedTypeError",
    "SchemaOnlyOneAllowedError",
]


class SchemaError(Exception):
    """Error during Schema validation."""

    def __init__(
        self,
        autos: Union[Sequence[Union[str, None]], None],
        errors: Union[List, str, None] = None,
    ):
        self.autos = autos if isinstance(autos, List) else [autos]
        self.errors = errors if isinstance(errors, List) else [errors]
        Exception.__init__(self, self.code)

    @property
    def code(self) -> str:
        """Remove duplicates in autos and errors list and combine them into a single message."""

        def uniq(seq: Iterable[Union[str, None]]) -> List[str]:
            """Utility function to remove duplicates while preserving the order."""
            seen: Set[str] = set()
            unique_list: List[str] = []
            for x in seq:
                if x is not None and x not in seen:
                    seen.add(x)
                    unique_list.append(x)
            return unique_list

        data_set = uniq(self.autos)
        error_list = uniq(self.errors)

        return "\n".join(error_list if error_list else data_set)


class SchemaWrongKeyError(SchemaError):
    """Error Should be raised when an unexpected key is detected within the
    data set being."""

    pass


class SchemaMissingKeyError(SchemaError):
    """Error should be raised when a mandatory key is not found within the
    data set being validated"""

    pass


class SchemaOnlyOneAllowedError(SchemaError):
    """Error should be raised when an only_one Or key has multiple matching candidates"""

    pass


class SchemaForbiddenKeyError(SchemaError):
    """Error should be raised when a forbidden key is found within the
    data set being validated, and its value matches the value that was specified"""

    pass


class SchemaUnexpectedTypeError(SchemaError):
    """Error should be raised when a type mismatch is detected within the
    data set being validated."""

    pass


# Type variable to represent a Schema-like type
TSchema = TypeVar("TSchema", bound="Schema")


class And(Generic[TSchema]):
    """
    Utility function to combine validation directives in AND Boolean fashion.
    """

    def __init__(
        self,
        *args: Union[TSchema, Callable[..., Any]],
        error: Union[str, None] = None,
        ignore_extra_keys: bool = False,
        schema: Union[Type[TSchema], None] = None,
    ) -> None:
        self._args: Tuple[Union[TSchema, Callable[..., Any]], ...] = args
        self._error: Union[str, None] = error
        self._ignore_extra_keys: bool = ignore_extra_keys
        self._schema_class: Type[TSchema] = schema if schema is not None else Schema

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({', '.join(repr(a) for a in self._args)})"

    @property
    def args(self) -> Tuple[Union[TSchema, Callable[..., Any]], ...]:
        """The provided parameters"""
        return self._args

    def validate(self, data: Any, **kwargs: Any) -> Any:
        """
        Validate data using defined sub schema/expressions ensuring all
        values are valid.
        :param data: Data to be validated with sub defined schemas.
        :return: Returns validated data.
        """
        # Annotate sub_schema with the type returned by _build_schema
        for sub_schema in self._build_schemas():  # type: TSchema
            data = sub_schema.validate(data, **kwargs)
        return data

    def _build_schemas(self) -> List[TSchema]:
        return [self._build_schema(s) for s in self._args]

    def _build_schema(self, arg: Any) -> TSchema:
        # Assume self._schema_class(arg, ...) returns an instance of TSchema
        return self._schema_class(
            arg, error=self._error, ignore_extra_keys=self._ignore_extra_keys
        )


class Or(And[TSchema]):
    """Utility function to combine validation directives in a OR Boolean
    fashion.

    If one wants to make an xor, one can provide only_one=True optional argument
    to the constructor of this object. When a validation was performed for an
    xor-ish Or instance and one wants to use it another time, one needs to call
    reset() to put the match_count back to 0."""

    def __init__(
        self,
        *args: Union[TSchema, Callable[..., Any]],
        only_one: bool = False,
        **kwargs: Any,
    ) -> None:
        self.only_one: bool = only_one
        self.match_count: int = 0
        super().__init__(*args, **kwargs)

    def reset(self) -> None:
        failed: bool = self.match_count > 1 and self.only_one
        self.match_count = 0
        if failed:
            raise SchemaOnlyOneAllowedError(
                ["There are multiple keys present from the %r condition" % self]
            )

    def validate(self, data: Any, **kwargs: Any) -> Any:
        """
        Validate data using sub defined schema/expressions ensuring at least
        one value is valid.
        :param data: data to be validated by provided schema.
        :return: return validated data if not validation
        """
        autos: List[str] = []
        errors: List[Union[str, None]] = []
        for sub_schema in self._build_schemas():
            try:
                validation: Any = sub_schema.validate(data, **kwargs)
                self.match_count += 1
                if self.match_count > 1 and self.only_one:
                    break
                return validation
            except SchemaError as _x:
                autos += _x.autos
                errors += _x.errors
        raise SchemaError(
            ["%r did not validate %r" % (self, data)] + autos,
            [self._error.format(data) if self._error else None] + errors,
        )


class Regex:
    """
    Enables schema.py to validate string using regular expressions.
    """

    # Map all flags bits to a more readable description
    NAMES = [
        "re.ASCII",
        "re.DEBUG",
        "re.VERBOSE",
        "re.UNICODE",
        "re.DOTALL",
        "re.MULTILINE",
        "re.LOCALE",
        "re.IGNORECASE",
        "re.TEMPLATE",
    ]

    def __init__(
        self, pattern_str: str, flags: int = 0, error: Union[str, None] = None
    ) -> None:
        self._pattern_str: str = pattern_str
        flags_list = [
            Regex.NAMES[i] for i, f in enumerate(f"{flags:09b}") if f != "0"
        ]  # Name for each bit

        self._flags_names: str = ", flags=" + "|".join(flags_list) if flags_list else ""
        self._pattern: re.Pattern = re.compile(pattern_str, flags=flags)
        self._error: Union[str, None] = error

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self._pattern_str!r}{self._flags_names})"

    @property
    def pattern_str(self) -> str:
        """The pattern string for the represented regular expression"""
        return self._pattern_str

    def validate(self, data: str, **kwargs: Any) -> str:
        """
        Validates data using the defined regex.
        :param data: Data to be validated.
        :return: Returns validated data.
        """
        e = self._error

        try:
            if self._pattern.search(data):
                return data
            else:
                error_message = (
                    e.format(data)
                    if e
                    else f"{data!r} does not match {self._pattern_str!r}"
                )
                raise SchemaError(error_message)
        except TypeError:
            error_message = (
                e.format(data) if e else f"{data!r} is not string nor buffer"
            )
            raise SchemaError(error_message)


class Use:
    """
    For more general use cases, you can use the Use class to transform
    the data while it is being validated.
    """

    def __init__(
        self, callable_: Callable[[Any], Any], error: Union[str, None] = None
    ) -> None:
        if not callable(callable_):
            raise TypeError(f"Expected a callable, not {callable_!r}")
        self._callable: Callable[[Any], Any] = callable_
        self._error: Union[str, None] = error

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self._callable!r})"

    def validate(self, data: Any, **kwargs: Any) -> Any:
        try:
            return self._callable(data)
        except SchemaError as x:
            raise SchemaError(
                [None] + x.autos,
                [self._error.format(data) if self._error else None] + x.errors,
            )
        except BaseException as x:
            f = _callable_str(self._callable)
            raise SchemaError(
                "%s(%r) raised %r" % (f, data, x),
                self._error.format(data) if self._error else None,
            )


COMPARABLE, CALLABLE, VALIDATOR, TYPE, DICT, ITERABLE = range(6)


def _priority(s: Any) -> int:
    """Return priority for a given object."""
    if type(s) in (list, tuple, set, frozenset):
        return ITERABLE
    if isinstance(s, dict):
        return DICT
    if issubclass(type(s), type):
        return TYPE
    if isinstance(s, Literal):
        return COMPARABLE
    if hasattr(s, "validate"):
        return VALIDATOR
    if callable(s):
        return CALLABLE
    else:
        return COMPARABLE


def _invoke_with_optional_kwargs(f: Callable[..., Any], **kwargs: Any) -> Any:
    s = inspect.signature(f)
    if len(s.parameters) == 0:
        return f()
    return f(**kwargs)


class Schema(object):
    """
    Entry point of the library, use this class to instantiate validation
    schema for the data that will be validated.
    """

    def __init__(
        self,
        schema: Any,
        error: Union[str, None] = None,
        ignore_extra_keys: bool = False,
        name: Union[str, None] = None,
        description: Union[str, None] = None,
        as_reference: bool = False,
    ) -> None:
        self._schema: Any = schema
        self._error: Union[str, None] = error
        self._ignore_extra_keys: bool = ignore_extra_keys
        self._name: Union[str, None] = name
        self._description: Union[str, None] = description
        self.as_reference: bool = as_reference

        if as_reference and name is None:
            raise ValueError("Schema used as reference should have a name")

    def __repr__(self):
        return "%s(%r)" % (self.__class__.__name__, self._schema)

    @property
    def schema(self) -> Any:
        return self._schema

    @property
    def description(self) -> Union[str, None]:
        return self._description

    @property
    def name(self) -> Union[str, None]:
        return self._name

    @property
    def ignore_extra_keys(self) -> bool:
        return self._ignore_extra_keys

    @staticmethod
    def _dict_key_priority(s) -> float:
        """Return priority for a given key object."""
        if isinstance(s, Hook):
            return _priority(s._schema) - 0.5
        if isinstance(s, Optional):
            return _priority(s._schema) + 0.5
        return _priority(s)

    @staticmethod
    def _is_optional_type(s: Any) -> bool:
        """Return True if the given key is optional (does not have to be found)"""
        return any(isinstance(s, optional_type) for optional_type in [Optional, Hook])

    def is_valid(self, data: Any, **kwargs: Dict[str, Any]) -> bool:
        """Return whether the given data has passed all the validations
        that were specified in the given schema.
        """
        try:
            self.validate(data, **kwargs)
        except SchemaError:
            return False
        else:
            return True

    def _prepend_schema_name(self, message: str) -> str:
        """
        If a custom schema name has been defined, prepends it to the error
        message that gets raised when a schema error occurs.
        """
        if self._name:
            message = "{0!r} {1!s}".format(self._name, message)
        return message

    def validate(self, data: Any, **kwargs: Dict[str, Any]) -> Any:
        Schema = self.__class__
        s: Any = self._schema
        e: Union[str, None] = self._error
        i: bool = self._ignore_extra_keys

        if isinstance(s, Literal):
            s = s.schema

        flavor = _priority(s)
        if flavor == ITERABLE:
            data = Schema(type(s), error=e).validate(data, **kwargs)
            o: Or = Or(*s, error=e, schema=Schema, ignore_extra_keys=i)
            return type(data)(o.validate(d, **kwargs) for d in data)
        if flavor == DICT:
            exitstack = ExitStack()
            data = Schema(dict, error=e).validate(data, **kwargs)
            new: Dict = type(data)()  # new - is a dict of the validated values
            coverage: Set = set()  # matched schema keys
            # for each key and value find a schema entry matching them, if any
            sorted_skeys = sorted(s, key=self._dict_key_priority)
            for skey in sorted_skeys:
                if hasattr(skey, "reset"):
                    exitstack.callback(skey.reset)

            with exitstack:
                # Evaluate dictionaries last
                data_items = sorted(
                    data.items(), key=lambda value: isinstance(value[1], dict)
                )
                for key, value in data_items:
                    for skey in sorted_skeys:
                        svalue = s[skey]
                        try:
                            nkey = Schema(skey, error=e).validate(key, **kwargs)
                        except SchemaError:
                            pass
                        else:
                            if isinstance(skey, Hook):
                                # As the content of the value makes little sense for
                                # keys with a hook, we reverse its meaning:
                                # we will only call the handler if the value does match
                                # In the case of the forbidden key hook,
                                # we will raise the SchemaErrorForbiddenKey exception
                                # on match, allowing for excluding a key only if its
                                # value has a certain type, and allowing Forbidden to
                                # work well in combination with Optional.
                                try:
                                    nvalue = Schema(svalue, error=e).validate(
                                        value, **kwargs
                                    )
                                except SchemaError:
                                    continue
                                skey.handler(nkey, data, e)
                            else:
                                try:
                                    nvalue = Schema(
                                        svalue, error=e, ignore_extra_keys=i
                                    ).validate(value, **kwargs)
                                except SchemaError as x:
                                    k = "Key '%s' error:" % nkey
                                    message = self._prepend_schema_name(k)
                                    raise SchemaError(
                                        [message] + x.autos,
                                        [e.format(data) if e else None] + x.errors,
                                    )
                                else:
                                    new[nkey] = nvalue
                                    coverage.add(skey)
                                    break
            required = set(k for k in s if not self._is_optional_type(k))
            if not required.issubset(coverage):
                missing_keys = required - coverage
                s_missing_keys = ", ".join(
                    repr(k) for k in sorted(missing_keys, key=repr)
                )
                message = "Missing key%s: %s" % (
                    _plural_s(missing_keys),
                    s_missing_keys,
                )
                message = self._prepend_schema_name(message)
                raise SchemaMissingKeyError(message, e.format(data) if e else None)
            if not self._ignore_extra_keys and (len(new) != len(data)):
                wrong_keys = set(data.keys()) - set(new.keys())
                s_wrong_keys = ", ".join(repr(k) for k in sorted(wrong_keys, key=repr))
                message = "Wrong key%s %s in %r" % (
                    _plural_s(wrong_keys),
                    s_wrong_keys,
                    data,
                )
                message = self._prepend_schema_name(message)
                raise SchemaWrongKeyError(message, e.format(data) if e else None)

            # Apply default-having optionals that haven't been used:
            defaults = (
                set(k for k in s if isinstance(k, Optional) and hasattr(k, "default"))
                - coverage
            )
            for default in defaults:
                new[default.key] = (
                    _invoke_with_optional_kwargs(default.default, **kwargs)
                    if callable(default.default)
                    else default.default
                )

            return new
        if flavor == TYPE:
            if isinstance(data, s) and not (isinstance(data, bool) and s == int):
                return data
            else:
                message = "%r should be instance of %r" % (data, s.__name__)
                message = self._prepend_schema_name(message)
                raise SchemaUnexpectedTypeError(message, e.format(data) if e else None)
        if flavor == VALIDATOR:
            try:
                return s.validate(data, **kwargs)
            except SchemaError as x:
                raise SchemaError(
                    [None] + x.autos, [e.format(data) if e else None] + x.errors
                )
            except BaseException as x:
                message = "%r.validate(%r) raised %r" % (s, data, x)
                message = self._prepend_schema_name(message)
                raise SchemaError(message, e.format(data) if e else None)
        if flavor == CALLABLE:
            f = _callable_str(s)
            try:
                if s(data):
                    return data
            except SchemaError as x:
                raise SchemaError(
                    [None] + x.autos, [e.format(data) if e else None] + x.errors
                )
            except BaseException as x:
                message = "%s(%r) raised %r" % (f, data, x)
                message = self._prepend_schema_name(message)
                raise SchemaError(message, e.format(data) if e else None)
            message = "%s(%r) should evaluate to True" % (f, data)
            message = self._prepend_schema_name(message)
            raise SchemaError(message, e.format(data) if e else None)
        if s == data:
            return data
        else:
            message = "%r does not match %r" % (s, data)
            message = self._prepend_schema_name(message)
            raise SchemaError(message, e.format(data) if e else None)

    def json_schema(
        self, schema_id: str, use_refs: bool = False, **kwargs: Any
    ) -> Dict[str, Any]:
        """Generate a draft-07 JSON schema dict representing the Schema.
        This method must be called with a schema_id.

        :param schema_id: The value of the $id on the main schema
        :param use_refs: Enable reusing object references in the resulting JSON schema.
                         Schemas with references are harder to read by humans, but are a lot smaller when there
                         is a lot of reuse
        """

        seen: Dict[int, Dict[str, Any]] = {}
        definitions_by_name: Dict[str, Dict[str, Any]] = {}

        def _json_schema(
            schema: "Schema",
            is_main_schema: bool = True,
            title: Union[str, None] = None,
            description: Union[str, None] = None,
            allow_reference: bool = True,
        ) -> Dict[str, Any]:
            def _create_or_use_ref(return_dict: Dict[str, Any]) -> Dict[str, Any]:
                """If not already seen, return the provided part of the schema unchanged.
                If already seen, give an id to the already seen dict and return a reference to the previous part
                of the schema instead.
                """
                if not use_refs or is_main_schema:
                    return return_schema

                hashed = hash(repr(sorted(return_dict.items())))
                if hashed not in seen:
                    seen[hashed] = return_dict
                    return return_dict
                else:
                    id_str = "#" + str(hashed)
                    seen[hashed]["$id"] = id_str
                    return {"$ref": id_str}

            def _get_type_name(python_type: Type) -> str:
                """Return the JSON schema name for a Python type"""
                if python_type == str:
                    return "string"
                elif python_type == int:
                    return "integer"
                elif python_type == float:
                    return "number"
                elif python_type == bool:
                    return "boolean"
                elif python_type == list:
                    return "array"
                elif python_type == dict:
                    return "object"
                return "string"

            def _to_json_type(value: Any) -> Any:
                """Attempt to convert a constant value (for "const" and "default") to a JSON serializable value"""
                if value is None or type(value) in (str, int, float, bool, list, dict):
                    return value

                if type(value) in (tuple, set, frozenset):
                    return list(value)

                if isinstance(value, Literal):
                    return value.schema

                return str(value)

            def _to_schema(s: Any, ignore_extra_keys: bool) -> Schema:
                if not isinstance(s, Schema):
                    return Schema(s, ignore_extra_keys=ignore_extra_keys)

                return s

            s: Any = schema.schema
            i: bool = schema.ignore_extra_keys
            flavor = _priority(s)

            return_schema: Dict[str, Any] = {}

            return_description: Union[str, None] = description or schema.description
            if return_description:
                return_schema["description"] = return_description
            if title:
                return_schema["title"] = title

            # Check if we have to create a common definition and use as reference
            if allow_reference and schema.as_reference:
                # Generate sub schema if not already done
                if schema.name not in definitions_by_name:
                    definitions_by_name[
                        cast(str, schema.name)
                    ] = {}  # Avoid infinite loop
                    definitions_by_name[cast(str, schema.name)] = _json_schema(
                        schema, is_main_schema=False, allow_reference=False
                    )

                return_schema["$ref"] = "#/definitions/" + cast(str, schema.name)
            else:
                if schema.name and not title:
                    return_schema["title"] = schema.name

                if flavor == TYPE:
                    # Handle type
                    return_schema["type"] = _get_type_name(s)
                elif flavor == ITERABLE:
                    # Handle arrays or dict schema

                    return_schema["type"] = "array"
                    if len(s) == 1:
                        return_schema["items"] = _json_schema(
                            _to_schema(s[0], i), is_main_schema=False
                        )
                    elif len(s) > 1:
                        return_schema["items"] = _json_schema(
                            Schema(Or(*s)), is_main_schema=False
                        )
                elif isinstance(s, Or):
                    # Handle Or values

                    # Check if we can use an enum
                    if all(
                        priority == COMPARABLE
                        for priority in [_priority(value) for value in s.args]
                    ):
                        or_values = [
                            str(s) if isinstance(s, Literal) else s for s in s.args
                        ]
                        # All values are simple, can use enum or const
                        if len(or_values) == 1:
                            or_value = or_values[0]
                            if or_value is None:
                                return_schema["type"] = "null"
                            else:
                                return_schema["const"] = _to_json_type(or_value)
                            return return_schema
                        return_schema["enum"] = or_values
                    else:
                        # No enum, let's go with recursive calls
                        any_of_values = []
                        for or_key in s.args:
                            new_value = _json_schema(
                                _to_schema(or_key, i), is_main_schema=False
                            )
                            if new_value != {} and new_value not in any_of_values:
                                any_of_values.append(new_value)
                        if len(any_of_values) == 1:
                            # Only one representable condition remains, do not put under anyOf
                            return_schema.update(any_of_values[0])
                        else:
                            return_schema["anyOf"] = any_of_values
                elif isinstance(s, And):
                    # Handle And values
                    all_of_values = []
                    for and_key in s.args:
                        new_value = _json_schema(
                            _to_schema(and_key, i), is_main_schema=False
                        )
                        if new_value != {} and new_value not in all_of_values:
                            all_of_values.append(new_value)
                    if len(all_of_values) == 1:
                        # Only one representable condition remains, do not put under allOf
                        return_schema.update(all_of_values[0])
                    else:
                        return_schema["allOf"] = all_of_values
                elif flavor == COMPARABLE:
                    if s is None:
                        return_schema["type"] = "null"
                    else:
                        return_schema["const"] = _to_json_type(s)
                elif flavor == VALIDATOR and type(s) == Regex:
                    return_schema["type"] = "string"
                    # JSON schema uses ECMAScript regex syntax
                    # Translating one to another is not easy, but this should work for simple cases
                    return_schema["pattern"] = re.sub(
                        r"\(\?P<[a-z\d_]+>", "(", s.pattern_str
                    ).replace("/", r"\/")
                else:
                    if flavor != DICT:
                        # If not handled, do not check
                        return return_schema

                    # Schema is a dict

                    required_keys = []
                    expanded_schema = {}
                    additional_properties = i
                    for key in s:
                        if isinstance(key, Hook):
                            continue

                        def _key_allows_additional_properties(key: Any) -> bool:
                            """Check if a key is broad enough to allow additional properties"""
                            if isinstance(key, Optional):
                                return _key_allows_additional_properties(key.schema)

                            return key == str or key == object

                        def _get_key_title(key: Any) -> Union[str, None]:
                            """Get the title associated to a key (as specified in a Literal object). Return None if not a Literal"""
                            if isinstance(key, Optional):
                                return _get_key_title(key.schema)

                            if isinstance(key, Literal):
                                return key.title

                            return None

                        def _get_key_description(key: Any) -> Union[str, None]:
                            """Get the description associated to a key (as specified in a Literal object). Return None if not a Literal"""
                            if isinstance(key, Optional):
                                return _get_key_description(key.schema)

                            if isinstance(key, Literal):
                                return key.description

                            return None

                        def _get_key_name(key: Any) -> Any:
                            """Get the name of a key (as specified in a Literal object). Return the key unchanged if not a Literal"""
                            if isinstance(key, Optional):
                                return _get_key_name(key.schema)

                            if isinstance(key, Literal):
                                return key.schema

                            return key

                        additional_properties = (
                            additional_properties
                            or _key_allows_additional_properties(key)
                        )
                        sub_schema = _to_schema(s[key], ignore_extra_keys=i)
                        key_name = _get_key_name(key)

                        if isinstance(key_name, str):
                            if not isinstance(key, Optional):
                                required_keys.append(key_name)
                            expanded_schema[key_name] = _json_schema(
                                sub_schema,
                                is_main_schema=False,
                                title=_get_key_title(key),
                                description=_get_key_description(key),
                            )
                            if isinstance(key, Optional) and hasattr(key, "default"):
                                expanded_schema[key_name]["default"] = _to_json_type(
                                    _invoke_with_optional_kwargs(key.default, **kwargs)
                                    if callable(key.default)
                                    else key.default
                                )
                        elif isinstance(key_name, Or):
                            # JSON schema does not support having a key named one name or another, so we just add both options
                            # This is less strict because we cannot enforce that one or the other is required

                            for or_key in key_name.args:
                                expanded_schema[_get_key_name(or_key)] = _json_schema(
                                    sub_schema,
                                    is_main_schema=False,
                                    description=_get_key_description(or_key),
                                )

                    return_schema.update(
                        {
                            "type": "object",
                            "properties": expanded_schema,
                            "required": required_keys,
                            "additionalProperties": additional_properties,
                        }
                    )

            if is_main_schema:
                return_schema.update(
                    {
                        "$id": schema_id,
                        "$schema": "http://json-schema.org/draft-07/schema#",
                    }
                )
                if self._name:
                    return_schema["title"] = self._name

                if definitions_by_name:
                    return_schema["definitions"] = {}
                    for definition_name, definition in definitions_by_name.items():
                        return_schema["definitions"][definition_name] = definition

            return _create_or_use_ref(return_schema)

        return _json_schema(self, True)


class Optional(Schema):
    """Marker for an optional part of the validation Schema."""

    _MARKER = object()

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        default: Any = kwargs.pop("default", self._MARKER)
        super(Optional, self).__init__(*args, **kwargs)
        if default is not self._MARKER:
            if _priority(self._schema) != COMPARABLE:
                raise TypeError(
                    "Optional keys with defaults must have simple, "
                    "predictable values, like literal strings or ints. "
                    f'"{self._schema!r}" is too complex.'
                )
            self.default = default
            self.key = str(self._schema)

    def __hash__(self) -> int:
        return hash(self._schema)

    def __eq__(self, other: Any) -> bool:
        return (
            self.__class__ is other.__class__
            and getattr(self, "default", self._MARKER)
            == getattr(other, "default", self._MARKER)
            and self._schema == other._schema
        )

    def reset(self) -> None:
        if hasattr(self._schema, "reset"):
            self._schema.reset()


class Hook(Schema):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.handler: Callable[..., Any] = kwargs.pop("handler", lambda *args: None)
        super(Hook, self).__init__(*args, **kwargs)
        self.key = self._schema


class Forbidden(Hook):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        kwargs["handler"] = self._default_function
        super(Forbidden, self).__init__(*args, **kwargs)

    @staticmethod
    def _default_function(nkey: Any, data: Any, error: Any) -> NoReturn:
        raise SchemaForbiddenKeyError(
            f"Forbidden key encountered: {nkey!r} in {data!r}", error
        )


class Literal:
    def __init__(
        self,
        value: Any,
        description: Union[str, None] = None,
        title: Union[str, None] = None,
    ) -> None:
        self._schema: Any = value
        self._description: Union[str, None] = description
        self._title: Union[str, None] = title

    def __str__(self) -> str:
        return str(self._schema)

    def __repr__(self) -> str:
        return f'Literal("{self._schema}", description="{self._description or ""}")'

    @property
    def description(self) -> Union[str, None]:
        return self._description

    @property
    def title(self) -> Union[str, None]:
        return self._title

    @property
    def schema(self) -> Any:
        return self._schema


class Const(Schema):
    def validate(self, data: Any, **kwargs: Any) -> Any:
        super(Const, self).validate(data, **kwargs)
        return data


def _callable_str(callable_: Callable[..., Any]) -> str:
    if hasattr(callable_, "__name__"):
        return callable_.__name__
    return str(callable_)


def _plural_s(sized: Sized) -> str:
    return "s" if len(sized) > 1 else ""
