"""
Schema and Compiler

schema is instance of Schema or object which has __schema__ attribute,
and the __schema__ is instance of Schema.

compiler can compile schema:

    compiler.compile(schema) -> validate function.

validate function has __schema__ attribute, it's also a schema.

the Builder's instance T can build schema.
in addition, T can be called directly, which convert schema-like things to
instance of Builder:

    T(JSON) -> Isomorph Schema
    T(func) -> Schema of validate func
    T(Schema) -> Copy of Schema
    T(Model) -> Schema of Model

Builder support schema slice:

    T[...keys] -> sub schema

relations:

    T(schema) -> T
    T.__schema__ -> Schema
"""
import copy
import enum
import inspect
import json

from pyparsing import (
    Group,
    Keyword,
    Optional,
    ParseBaseException,
    StringEnd,
    StringStart,
    Suppress,
    ZeroOrMore,
    pyparsing_common,
    quotedString,
    removeQuotes,
    replaceWith,
)

from .validator import SchemaError, builtin_validators
from .validator import py_mark_index as mark_index
from .validator import py_mark_key as mark_key


def _make_keyword(kwd_str, kwd_value):
    return Keyword(kwd_str).setParseAction(replaceWith(kwd_value))


def _define_value():
    TRUE = _make_keyword('true', True)
    FALSE = _make_keyword('false', False)
    NULL = _make_keyword('null', None)
    STRING = quotedString().setParseAction(removeQuotes)
    NUMBER = pyparsing_common.number()
    return TRUE | FALSE | NULL | STRING | NUMBER


def _define_element():
    VALIDATOR = pyparsing_common.identifier.setName('validator').setResultsName('validator')
    ITEMS = _define_value().setName('items').setResultsName('items')
    ITEMS_WRAPPER = Optional(Suppress('(') + ITEMS + Suppress(')'))
    PARAMS_KEY = pyparsing_common.identifier.setName('key').setResultsName('key')
    PARAMS_VALUE = _define_value().setName('value').setResultsName('value')
    PARAMS_VALUE_WRAPPER = Optional(Suppress('(') + PARAMS_VALUE + Suppress(')'))
    PARAMS_KEY_VALUE = Group(Suppress('.') + PARAMS_KEY + PARAMS_VALUE_WRAPPER)
    PARAMS = Group(ZeroOrMore(PARAMS_KEY_VALUE)).setName('params').setResultsName('params')
    return StringStart() + VALIDATOR + ITEMS_WRAPPER + PARAMS + StringEnd()


ELEMENT_GRAMMAR = _define_element()


def _dump_value(value):
    if value is None:
        return 'null'
    elif value is False:
        return 'false'
    elif value is True:
        return 'true'
    elif isinstance(value, str):
        return repr(value)  # single quotes by default
    elif isinstance(value, Schema):
        return value.validator
    else:
        return str(value)  # number


def _pair(k, v):
    return '{}({})'.format(k, _dump_value(v))


def _sort_schema_params(params):
    def key(item):
        k, v = item
        if k == 'desc':
            return 3
        if k == 'optional':
            return 2
        if k == 'default':
            return 1
        if isinstance(v, bool):
            return -1
        if isinstance(v, str):
            return -2
        else:
            return -3
    return list(sorted(params, key=key))


def _schema_of(obj) -> "Schema":
    if hasattr(obj, '__schema__'):
        obj = obj.__schema__
    return obj


def _schema_copy_of(obj) -> "Schema":
    if isinstance(obj, Schema):
        obj = obj.copy()
    return obj


def _schema_primitive_of(obj):
    if isinstance(obj, Schema):
        obj = obj.to_primitive()
    return obj


def _is_model(obj):
    return inspect.isclass(obj) and hasattr(obj, '__schema__')


class Schema:

    def __init__(self, *, validator=None, items=None, params=None):
        self.validator = validator
        self.items = items
        self.params = params or {}

    def __eq__(self, other):
        other = _schema_of(other)
        if not isinstance(other, Schema):
            return False
        return (self.validator == other.validator and
                self.items == other.items and
                self.params == other.params)

    def __hash__(self):
        params = tuple(sorted(self.params.items()))
        items = self.items
        if isinstance(items, dict):
            items = tuple(sorted(items.items()))
        elif isinstance(items, list):
            items = tuple(items)
        return hash((self.validator, items, params))

    def __str__(self):
        return json.dumps(self.to_primitive(), indent=4,
                          ensure_ascii=False, sort_keys=True)

    def repr(self, *, prefix=True, desc=True):
        if not self.validator:
            return 'T' if prefix else ''
        ret = ['T'] if prefix else []
        if self.items is None:
            ret.append(self.validator)
        else:
            if self.validator == 'dict':
                keys = ', '.join(sorted(self.items)) if self.items else ''
                ret.append('{}({})'.format(self.validator, '{' + keys + '}'))
            elif self.validator == 'list':
                ret.append('{}({})'.format(self.validator, self.items.validator))
            elif self.validator == 'enum':
                values = ', '.join(map(_dump_value, self.items)) if self.items else ''
                ret.append('{}({})'.format(self.validator, '{' + values + '}'))
            elif self.validator == 'union':
                if self.items and isinstance(self.items, list):
                    keys = ', '.join(x.validator for x in self.items)
                    ret.append('{}([{}])'.format(self.validator, keys))
                else:
                    keys = ', '.join(sorted(self.items)) if self.items else ''
                    ret.append('{}({})'.format(self.validator, '{' + keys + '}'))
            elif self.validator == 'model' and self.items is not None:
                ret.append('{}({})'.format(self.validator, self.items.__name__))
            else:
                ret.append(_pair(self.validator, self.items))
        for k, v in _sort_schema_params(self.params.items()):
            if not desc and k == 'desc':
                continue
            if v is False:
                continue
            if v is True:
                ret.append(k)
            else:
                ret.append(_pair(k, v))
        return '.'.join(ret)

    def __repr__(self):
        r = self.repr(prefix=False)
        return '{}<{}>'.format(type(self).__name__, r)

    def copy(self):
        params = {k: _schema_copy_of(v)for k, v in self.params.items()}
        schema = type(self)(validator=self.validator, params=params)
        if self.validator == 'dict' and self.items is not None:
            items = {k: _schema_copy_of(v) for k, v in self.items.items()}
        elif self.validator == 'list' and self.items is not None:
            items = _schema_copy_of(self.items)
        elif self.validator == 'union' and self.items is not None:
            if isinstance(self.items, list):
                items = [_schema_copy_of(x) for x in self.items]
            else:
                items = {k: _schema_copy_of(v) for k, v in self.items.items()}
        else:
            items = copy.copy(self.items)
        schema.items = items
        return schema

    def __copy__(self):
        return self.copy()

    def __deepcopy__(self, memo):
        return self.copy()

    def to_primitive(self):
        if not self.validator:
            return None
        # T.model not support in JSON, convert it to T.dict
        if self.validator == 'model':
            if self.items is None:
                items = None
            else:
                items = _schema_of(self.items).items
            self = Schema(validator='dict', items=items, params=self.params)
        ret = []
        if self.validator in {'dict', 'list', 'union', 'enum'} or self.items is None:
            ret.append(self.validator)
        else:
            ret.append(_pair(self.validator, self.items))
        for k, v in _sort_schema_params(self.params.items()):
            if self.validator == 'dict' and k in {'key', 'value'}:
                continue
            if v is False:
                continue
            if v is True:
                ret.append(k)
            else:
                ret.append(_pair(k, v))
        ret = '.'.join(ret)
        if self.validator == 'dict':
            ret = {'$self': ret}
            for pkey in ['key', 'value']:
                pvalue = self.params.get(pkey)
                if pvalue is not None:
                    ret['$self_{}'.format(pkey)] = _schema_primitive_of(pvalue)
            if self.items is not None:
                for k, v in self.items.items():
                    ret[k] = _schema_primitive_of(v)
        elif self.validator == 'list' and self.items is not None:
            ret = [ret, _schema_primitive_of(self.items)]
        elif self.validator == 'enum' and self.items is not None:
            ret = [ret, *self.items]
        elif self.validator == 'union' and self.items is not None:
            if isinstance(self.items, list):
                ret = [ret]
                for x in self.items:
                    ret.append(_schema_primitive_of(x))
            else:
                ret = {'$self': ret}
                for k, v in self.items.items():
                    ret[k] = _schema_primitive_of(v)
        return ret

    @classmethod
    def parse_element(cls, text):
        if text is None:
            raise SchemaError("can't parse None")
        text = text.strip()
        if not text:
            raise SchemaError("can't parse empty string")
        try:
            result = ELEMENT_GRAMMAR.parseString(text, parseAll=True)
        except ParseBaseException as ex:
            msg = 'invalid syntax in col {} of {!r}'.format(ex.col, repr(ex.line))
            raise SchemaError(msg) from None
        validator = result['validator']
        items = None
        if 'items' in result:
            items = result['items']
        params = {}
        for item in result['params']:
            value = True
            if 'value' in item:
                value = item['value']
            params[item['key']] = value
        return cls(validator=validator, items=items, params=params)

    @classmethod
    def parse_isomorph_schema(cls, obj):
        if isinstance(obj, str):
            return cls.parse_element(obj)
        elif isinstance(obj, dict):
            e = cls.parse_element(obj.pop('$self', 'dict'))
            items = {}
            for k, v in obj.items():
                with mark_key(k):
                    items[k] = cls.parse_isomorph_schema(v)
            for pkey in ['key', 'value']:
                pvalue = items.pop('$self_{}'.format(pkey), None)
                if pvalue is not None:
                    e.params[pkey] = pvalue
            return cls(validator=e.validator, items=items, params=e.params)
        elif isinstance(obj, list):
            if len(obj) == 1:
                validator = 'list'
                params = None
                items = cls.parse_isomorph_schema(obj[0])
            elif len(obj) >= 2:
                e = cls.parse_element(obj[0])
                validator = e.validator
                params = e.params
                if validator == 'list':
                    if len(obj) > 2:
                        raise SchemaError('invalid list schema')
                    with mark_index():
                        items = cls.parse_isomorph_schema(obj[1])
                elif validator == 'enum':
                    items = list(obj[1:])
                elif validator == 'union':
                    items = []
                    for i, x in enumerate(obj[1:]):
                        with mark_index(i):
                            items.append(cls.parse_isomorph_schema(x))
                else:
                    raise SchemaError('unknown {} schema'.format(validator))
            else:
                raise SchemaError('invalid list schema')
            return cls(validator=validator, items=items, params=params)
        else:
            raise SchemaError('{} object is not schema'.format(type(obj)))


class Compiler:

    def __init__(self, validators=None, is_dump=False):
        self.validators = builtin_validators.copy()
        if validators:
            self.validators.update(validators)
        self.is_dump = is_dump

    def compile(self, schema):
        schema = _schema_of(schema)
        if not isinstance(schema, Schema):
            raise SchemaError('{} object is not schema'.format(type(schema)))
        if not schema.validator:
            raise SchemaError('incomplete schema')
        validator = self.validators.get(schema.validator)
        if not validator:
            raise SchemaError('validator {!r} not found'.format(schema.validator))
        return validator(self, schema)


_BUILDER_INIT = 'init'
_EXP_ATTR = 'expect-attr'
_EXP_ATTR_OR_ITEMS = 'expect-attr-or-items'
_EXP_ATTR_OR_CALL = 'expect-attr-or-call'


class Builder:

    def __init__(self, state=_BUILDER_INIT, *, validator=None,
                 items=None, params=None, last_attr=None):
        self._state = state
        self._schema = Schema(validator=validator, items=items, params=params)
        self._last_attr = last_attr

    @property
    def __schema__(self):
        return self._schema

    def __repr__(self):
        return self._schema.repr()

    def __str__(self):
        return self._schema.__str__()

    def __eq__(self, other):
        return self._schema == _schema_of(other)

    def __hash__(self):
        return self._schema.__hash__()

    def __getitem__(self, keys):
        if not self._schema.validator:
            raise ValueError('can not slice empty schema')
        if self._schema.validator != 'dict':
            raise ValueError('can not slice non-dict schema')
        if not isinstance(keys, (list, tuple)):
            keys = (keys,)
        schema = Schema(validator=self._schema.validator,
                        params=self._schema.params.copy())
        schema.items = {}
        items = self._schema.items or {}
        for k in keys:
            if k not in items:
                raise ValueError('key {!r} is not exists'.format(k))
            schema.items[k] = items[k]
        return T(schema)

    def __getattr__(self, name):
        if name.startswith('__') and name.endswith('__'):
            raise AttributeError('{!r} object has no attribute {!r}'.format(
                type(self).__name__, name))
        if self._state == _BUILDER_INIT:
            return Builder(_EXP_ATTR_OR_ITEMS, validator=name)
        else:
            params = self._schema.params.copy()
            params[name] = True
            return Builder(
                _EXP_ATTR_OR_CALL, validator=self._schema.validator,
                items=self._schema.items, params=params, last_attr=name)

    def __call__(self, *args, **kwargs):
        if self._state == _BUILDER_INIT:
            return self._load_schema(*args, **kwargs)
        if self._state not in [_EXP_ATTR_OR_ITEMS, _EXP_ATTR_OR_CALL]:
            raise SchemaError('current state not callable')
        if self._state == _EXP_ATTR_OR_ITEMS:
            if args and kwargs:
                raise SchemaError("can't call with both positional argument and keyword argument")
            if len(args) > 1:
                raise SchemaError("can't call with more than one positional argument")
            if self._schema.validator in {'dict', 'union'}:
                items = args[0] if args else kwargs
            elif self._schema.validator == 'model':
                if len(args) != 1 or kwargs:
                    raise SchemaError('require exactly one positional argument')
                items = args[0]
            else:
                if kwargs:
                    raise SchemaError("can't call with keyword argument")
                if not args:
                    raise SchemaError('require one positional argument')
                items = args[0]
            items = self._check_items(items)
            params = self._schema.params
        else:
            if kwargs:
                raise SchemaError("can't call with keyword argument")
            if not args:
                raise SchemaError('require one positional argument')
            if len(args) > 1:
                raise SchemaError("can't call with more than one positional argument")
            param_value = self._check_param_value(self._last_attr, args[0])
            items = self._schema.items
            params = self._schema.params.copy()
            params[self._last_attr] = param_value
        return Builder(_EXP_ATTR, validator=self._schema.validator,
                       items=items, params=params, last_attr=None)

    def _load_schema(self, obj):
        obj = _schema_of(obj)
        if isinstance(obj, Schema):
            obj = obj.copy()
        elif isinstance(obj, (str, list, dict)):
            obj = Schema.parse_isomorph_schema(obj)
        else:
            raise SchemaError('{} object is not schema'.format(type(obj)))
        if not obj.validator:
            state = _BUILDER_INIT
        elif not obj.items and not obj.params:
            state = _EXP_ATTR_OR_ITEMS
        else:
            state = _EXP_ATTR
        return Builder(state, validator=obj.validator,
                       items=obj.items, params=obj.params, last_attr=None)

    def _check_dict_items(self, items):
        if not isinstance(items, dict):
            raise SchemaError('items must be dict')
        ret = {}
        for k, v in items.items():
            v = _schema_of(v)
            if not isinstance(v, Schema):
                raise SchemaError('items[{}] is not schema'.format(k))
            ret[k] = v
        return ret

    def _check_items(self, items):
        if self._schema.validator == 'dict':
            ret = self._check_dict_items(items)
        elif self._schema.validator == 'list':
            items = _schema_of(items)
            if not isinstance(items, Schema):
                raise SchemaError('items is not schema')
            ret = items
        elif self._schema.validator == 'enum':
            if isinstance(items, str):
                items = set(items.replace(',', ' ').strip().split())
            if inspect.isclass(items) and issubclass(items, enum.Enum):
                items = [x.value for x in items.__members__.values()]
            if not isinstance(items, (list, tuple, set)):
                raise SchemaError('items is not list or set')
            ret = []
            for i, v in enumerate(items):
                if not isinstance(v, (bool, int, float, str)):
                    raise SchemaError('enum value must be bool, int, float or str')
                ret.append(v)
            ret = list(sorted(set(ret), key=lambda x: (str(type(x)), str(x))))
        elif self._schema.validator == 'union':
            if isinstance(items, list):
                ret = []
                for i, v in enumerate(items):
                    v = _schema_of(v)
                    if not isinstance(v, Schema):
                        raise SchemaError('items[{}] is not schema'.format(i))
                    ret.append(v)
            else:
                ret = self._check_dict_items(items)
        elif self._schema.validator == 'model':
            if not _is_model(items):
                raise SchemaError('items must be model class')
            ret = items
        else:
            if not isinstance(items, (bool, int, float, str)):
                raise SchemaError('items must be bool, int, float or str')
            ret = items
        return ret

    def _check_param_value(self, key, value):
        if self._schema.validator == 'dict':
            if key in {'key', 'value'}:
                return self._check_dict_param_value(key, value)
        if value is not None and not isinstance(value, (bool, int, float, str)):
            raise SchemaError('parameter value must be bool, int, float or str')
        return value

    def _check_dict_param_value(self, key, value):
        value = _schema_of(value)
        if value is not None and not isinstance(value, Schema):
            raise SchemaError('dict {} parameter is not schema'.format(key))
        return value


T = Builder()
