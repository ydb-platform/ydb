#
# Copyright (c), 2018-2026, SISSA (International School for Advanced Studies).
# All rights reserved.
# This file is distributed under the terms of the MIT License.
# See the file 'LICENSE' in the root directory of the present
# distribution, or http://opensource.org/licenses/MIT.
#
# @author Davide Brunato <brunato@sissa.it>
#
from itertools import zip_longest
from functools import cache
from typing import cast, Any

import elementpath.aliases as ta

from elementpath.datatypes import builtin_atomic_types, builtin_list_types, QName, \
    NumericProxy, AnyAtomicType
from elementpath.exceptions import ElementPathKeyError, ElementPathValueError, xpath_error
from elementpath.namespaces import XSD_NAMESPACE, XSD_ERROR, XSD_DATETIME_STAMP, \
    XSD_NUMERIC, XSD_UNTYPED, XSD_UNTYPED_ATOMIC, get_expanded_name
from elementpath.helpers import collapse_white_spaces, Patterns
from elementpath.xpath_nodes import XPathNode, DocumentNode, ElementNode, AttributeNode
from elementpath.xpath_tokens import XPathToken

XSD_EXTENDED_PREFIX = f'{{{XSD_NAMESPACE}}}'
XSD_10_UNSUPPORTED = frozenset(
    ('xs:error', 'xs:dateTimeStamp', XSD_ERROR, XSD_DATETIME_STAMP)
)
COMMON_SEQUENCE_TYPES = frozenset((
    'xs:anyType', 'xs:anySimpleType', 'xs:numeric', 'xs:untyped', 'attribute()',
    'attribute(*)', 'element()', 'element(*)', 'text()', 'document-node()',
    'comment()', 'processing-instruction()', 'item()', 'node()', 'numeric'
))
XSD11_ONLY_TYPES = frozenset(
    (XSD_ERROR, XSD_DATETIME_STAMP, 'xs:error', 'xs:dateTimeStamp')
)


def get_function_signatures(qname: QName,
                            nargs: ta.NargsType,
                            sequence_types: tuple[str, ...] = ()) -> dict[tuple[QName, int], str]:
    """
    Returns the signatures for a function.

    :param qname: A QName instance that represents the FQDN of the function.
    :param nargs: The number of arguments that the function takes, could the `None`, \
    a non-negative integer or a couple non-negative integers or a non-negative integer \
    followed by `None`.
    :param sequence_types: A sequence of sequence type specifications, must match \
    the number of arguments that the function takes plus the return type.
    """
    function_signatures: dict[tuple[QName, int], str] = {}
    if not sequence_types:
        return function_signatures

    if any(not is_sequence_type(st) for st in sequence_types):
        msg = "Error in provided sequence types: {!r}"
        raise ElementPathValueError(msg.format(sequence_types))
    elif nargs is None:
        if len(sequence_types) != 1:
            raise ElementPathValueError("Mismatched number of sequence types provided")
        function_signatures[(qname, 0)] = f'function() as {sequence_types[0]}'
    elif isinstance(nargs, int):
        if len(sequence_types) != nargs + 1:
            raise ElementPathValueError("Mismatched number of sequence types provided")
        function_signatures[(qname, nargs)] = 'function({}) as {}'.format(
            ', '.join(sequence_types[:-1]), sequence_types[-1]
        )
    elif nargs[1] is None:
        if len(sequence_types) != nargs[0] + 1:
            raise ElementPathValueError("Mismatched number of sequence types provided")
        function_signatures[(qname, nargs[0])] = 'function({}, ...) as {}'.format(
            ', '.join(sequence_types[:-1]), sequence_types[-1]
        )
    else:
        if len(sequence_types) != nargs[1] + 1:
            raise ElementPathValueError("Mismatched number of sequence types provided")
        for arity in range(nargs[0], nargs[1] + 1):
            function_signatures[(qname, arity)] = 'function({}) as {}'.format(
                ', '.join(sequence_types[:arity]), sequence_types[-1]
            )

    return function_signatures


###
# Sequence type checking
@cache
def normalize_sequence_type(sequence_type: str) -> str:
    sequence_type = collapse_white_spaces(sequence_type)
    sequence_type = Patterns.sequence_type.sub(r'\1', sequence_type)
    return sequence_type.replace(',', ', ').replace(')as', ') as')


@cache
def is_sequence_type_restriction(st1: str, st2: str) -> bool:
    """Returns `True` if st2 is a restriction of st1."""
    st1, st2 = normalize_sequence_type(st1), normalize_sequence_type(st2)

    if not st1 or st1[0] == '{' or not st2 or st2[0] == '{':
        return False
    elif st2 in ('empty-sequence()', 'none') and \
            (st1 in ('empty-sequence()', 'none') or st1.endswith(('?', '*'))):
        return True

    # check occurrences
    if st1[-1] not in '?+*':
        if st2[-1] in '+*':
            return False
        elif st2[-1] == '?':
            st2 = st2[:-1]

    elif st1[-1] == '+':
        st1 = st1[:-1]
        if st2[-1] in '?*':
            return False
        elif st2[-1] == '+':
            st2 = st2[:-1]

    elif st1[-1] == '*':
        st1 = st1[:-1]
        if st2[-1] in '?+':
            return False
        elif st2[-1] == '*':
            st2 = st2[:-1]

    else:
        st1 = st1[:-1]
        if st2[-1] in '+*':
            return False
        elif st2[-1] == '?':
            st2 = st2[:-1]

    if st1 == st2:
        return True
    elif st1 == 'item()':
        return True
    elif st2 == 'item()':
        return False
    elif st1 == 'node()':
        return st2.startswith(('element(', 'attribute(', 'comment(', 'text(',
                               'processing-instruction(', 'document(', 'namespace('))
    elif st2 == 'node()':
        return False
    elif st2 in builtin_atomic_types:
        if st1 not in builtin_atomic_types:
            return False
        return issubclass(builtin_atomic_types[st2], builtin_atomic_types[st1])
    elif st2 in builtin_list_types:
        if st1 in ('xs:anyType', 'xs:anySimpleType'):
            return True
        elif st1 not in builtin_list_types:
            return False
        return issubclass(builtin_list_types[st2], builtin_list_types[st1])
    elif not st1.startswith('function('):
        return False

    if st1 == 'function(*)':
        return st2.startswith('function(')

    parts1 = st1[9:].partition(') as ')
    parts2 = st2[9:].partition(') as ')

    for st1, st2 in zip_longest(parts1[0].split(', '), parts2[0].split(', ')):
        if st1 is None or st2 is None:
            return False
        if not is_sequence_type_restriction(st2, st1):
            return False
    else:
        if not is_sequence_type_restriction(parts1[2], parts2[2]):
            return False
        return True


def is_instance(obj: Any, type_qname: str, parser: ta.XPathParserType | None = None) -> bool:
    """Checks an instance against an XSD type."""
    if isinstance(obj, list) and len(obj) == 1:
        return is_instance(obj[0], type_qname, parser)
    elif type_qname in builtin_atomic_types:
        if type_qname in XSD11_ONLY_TYPES:
            if parser is not None and parser.xsd_version == '1.0':
                return False
        return isinstance(obj, builtin_atomic_types[type_qname])
    elif type_qname in builtin_list_types:
        if isinstance(obj, AnyAtomicType):
            raise xpath_error(
                'XPST0051', 'an atomic value cannot be tested as an instance of list types')
        return isinstance(obj, builtin_list_types[type_qname])
    elif type_qname in (XSD_NUMERIC, 'xs:numeric', 'numeric'):
        return isinstance(obj, NumericProxy)

    elif parser is not None and parser.schema is not None:
        type_qname = get_expanded_name(type_qname, parser.namespaces)
        try:
            return parser.schema.is_instance(obj, type_qname)
        except KeyError:
            pass

    raise ElementPathKeyError("unknown type %r" % type_qname)


def is_sequence_type(value: str, parser: ta.XPathParserType | None = None) -> bool:
    """Checks if a string is a sequence type specification."""

    @cache
    def is_st(st: str) -> bool:
        if not st or st[0] == '{':
            return False
        elif st == 'empty-sequence()' or st == 'none':
            return True
        elif st[-1] in ('*', '+', '?'):
            st = st[:-1]

        if st in COMMON_SEQUENCE_TYPES:
            return True
        elif st in builtin_atomic_types:
            if st in ('xs:dateTimeStamp', 'xs:error'):
                return getattr(parser, 'xsd_version', '1.1') != '1.0'
            return True
        elif st in builtin_list_types:
            return True
        elif st.startswith(('map(', 'array(')):
            if parser and parser.version < '3.1' or not st.endswith(')'):
                return False

            if st in ('map(*)', 'array(*)'):
                return True

            if st.startswith('map('):
                key_type, _, value_type = st[4:-1].partition(', ')
                return key_type.startswith('xs:') and \
                    not key_type.endswith(('+', '*')) and \
                    is_st(key_type) and \
                    is_st(key_type)
            else:
                return is_st(st[6:-1])

        elif st.startswith('element(') and st.endswith(')'):
            if ',' not in st:
                return Patterns.extended_qname.match(st[8:-1]) is not None

            try:
                arg1, arg2 = st[8:-1].split(', ')
            except ValueError:
                return False
            else:
                return (arg1 == '*' or Patterns.extended_qname.match(arg1) is not None) \
                       and Patterns.extended_qname.match(arg2) is not None

        elif st.startswith('document-node(') and st.endswith(')'):
            if not st.startswith('document-node(element('):
                return False
            return is_st(st[14:-1])

        elif st.startswith('function('):
            if parser and parser.version < '3.0':
                return False
            elif st == 'function(*)':
                return True
            elif ' as ' in st:
                pass
            elif not st.endswith(')'):
                return False
            else:
                return is_st(st[9:-1])

            st, return_type = st.rsplit(' as ', 1)
            if not is_st(return_type):
                return False
            elif st == 'function()':
                return True

            st = st[9:-1]
            if st.endswith(', ...'):
                st = st[:-5]

            if 'function(' not in st:
                return all(is_st(x) for x in st.split(', '))
            elif st.startswith('function(*)') and 'function(' not in st[11:]:
                return all(is_st(x) for x in st.split(', '))

            # Cover only if function() spec is the last argument
            k = st.index('function(')
            if not is_st(st[k:]):
                return False
            return all(is_st(x) for x in st[:k].split(', ') if x)

        elif QName.pattern.match(st) is None:
            return False

        if parser is None:
            return False

        try:
            is_instance(None, st, parser)
        except (KeyError, ValueError):
            return False
        else:
            return True

    return is_st(normalize_sequence_type(value))


def match_sequence_type(value: Any,
                        sequence_type: str,
                        parser: ta.XPathParserType | None = None,
                        strict: bool = True) -> bool:
    """
    Checks a value instance against a sequence type.

    :param value: the instance to check.
    :param sequence_type: a string containing the sequence type spec.
    :param parser: an optional parser instance for type checking.
    :param strict: if `False` match xs:anyURI with strings.
    """
    def match_st(v: Any, st: str, occurrence: str | None = None) -> bool:
        if st[-1] in ('*', '+', '?') and ') as ' not in st:
            return match_st(v, st[:-1], st[-1])
        elif v is None or v == []:
            return st in ('empty-sequence()', 'none') or occurrence in ('?', '*')
        elif st in ('empty-sequence()', 'none'):
            return False
        elif isinstance(v, list):
            if len(v) == 1:
                return match_st(v[0], st)
            elif occurrence is None or occurrence == '?':
                return False
            else:
                return all(match_st(x, st) for x in v)
        elif st == 'item()':
            return isinstance(v, (XPathNode, AnyAtomicType,
                                  XPathToken.registry.function_token))
        elif st == 'numeric' or st == 'xs:numeric':
            return isinstance(v, NumericProxy)
        elif st.startswith('function('):
            if not isinstance(v, XPathToken.registry.function_token):
                return False
            return v.match_function_test(st)

        elif st.startswith('array('):
            if not isinstance(v, XPathToken.registry.array_token):
                return False
            if st == 'array(*)':
                return True

            item_st = st[6:-1]
            assert isinstance(v, XPathToken.registry.array_token)
            return all(match_st(x, item_st) for x in v.items())

        elif st.startswith('map('):
            if not isinstance(v, XPathToken.registry.map_token):
                return False
            if st == 'map(*)':
                return True

            key_st, _, value_st = st[4:-1].partition(', ')
            if key_st.endswith(('+', '*')):
                raise xpath_error('XPST0003', 'no multiples occurs for a map key')

            return all(match_st(k, key_st) and match_st(v, value_st) for k, v in v.items())

        if isinstance(v, XPathNode):
            node_kind = v.node_kind
        elif '(' in st:
            return False
        elif not strict and st == 'xs:anyURI' and isinstance(v, str):
            return True
        else:
            try:
                return is_instance(v, st, parser)
            except (KeyError, ValueError):
                raise xpath_error('XPST0051')

        if st == 'node()':
            return True
        elif not st.startswith(node_kind) or not st.endswith(')'):
            return False
        elif st == f'{node_kind}()':
            return True
        elif node_kind == 'document':
            element_test = st[14:-1]
            if not element_test:
                return True
            document = cast(DocumentNode, v)
            return any(
                match_st(e, element_test) for e in document if isinstance(e, ElementNode)
            )
        elif node_kind not in ('element', 'attribute'):
            return False

        _, params = st[:-1].split('(')
        if ', ' not in st:
            name = params
        else:
            name, type_name = params.rsplit(', ', 1)
            if type_name.endswith('?'):
                type_name = type_name[:-1]
            elif isinstance(v, ElementNode) and v.nilled:
                return False

            if type_name == 'xs:untyped':
                if isinstance(v, AttributeNode) and v.type_name != XSD_UNTYPED_ATOMIC:
                    return False
                if isinstance(v, ElementNode) and v.type_name != XSD_UNTYPED:
                    return False
            else:
                try:
                    if not is_instance(v.typed_value, type_name, parser):
                        return False
                except (KeyError, ValueError):
                    raise xpath_error('XPST0051')

        if name == '*':
            return True
        elif parser is None:
            return v.name == name
        else:
            try:
                return v.name == get_expanded_name(name, parser.namespaces)
            except (KeyError, ValueError):
                return False

    return match_st(value, normalize_sequence_type(sequence_type))
