import functools
import json
import pprint
from pathlib import Path

import pytest
from webencodings import Encoding, lookup

from tinycss2 import (  # isort:skip
    parse_blocks_contents, parse_component_value_list, parse_declaration_list,
    parse_one_component_value, parse_one_declaration, parse_one_rule,
    parse_rule_list, parse_stylesheet, parse_stylesheet_bytes, serialize)
from tinycss2.ast import (  # isort:skip
    AtKeywordToken, AtRule, Comment, CurlyBracketsBlock, Declaration,
    DimensionToken, FunctionBlock, HashToken, IdentToken, LiteralToken,
    NumberToken, ParenthesesBlock, ParseError, PercentageToken, QualifiedRule,
    SquareBracketsBlock, StringToken, UnicodeRangeToken, URLToken,
    WhitespaceToken)
from tinycss2.color3 import RGBA  # isort:skip
from tinycss2.color3 import parse_color as parse_color3  # isort:skip
from tinycss2.color4 import Color  # isort:skip
from tinycss2.color4 import parse_color as parse_color4  # isort:skip
from tinycss2.color5 import parse_color as parse_color5  # isort:skip
from tinycss2.nth import parse_nth  # isort:skip


def generic(func):
    implementations = func()

    @functools.wraps(func)
    def run(value):
        repr(value)  # Test that this does not raise.
        return implementations[type(value)](value)
    return run


@generic
def to_json():
    def numeric(t):
        return [
            t.representation, t.value,
            'integer' if t.int_value is not None else 'number']
    return {
        type(None): lambda _: None,
        str: lambda s: s,
        int: lambda s: s,
        list: lambda li: [to_json(el) for el in li],
        tuple: lambda li: [to_json(el) for el in li],
        Encoding: lambda e: e.name,
        ParseError: lambda e: ['error', e.kind],

        Comment: lambda t: '/* … */',
        WhitespaceToken: lambda t: ' ',
        LiteralToken: lambda t: t.value,
        IdentToken: lambda t: ['ident', t.value],
        AtKeywordToken: lambda t: ['at-keyword', t.value],
        HashToken: lambda t: [
            'hash', t.value, 'id' if t.is_identifier else 'unrestricted'],
        StringToken: lambda t: ['string', t.value],
        URLToken: lambda t: ['url', t.value],
        NumberToken: lambda t: ['number', *numeric(t)],
        PercentageToken: lambda t: ['percentage', *numeric(t)],
        DimensionToken: lambda t: ['dimension', *numeric(t), t.unit],
        UnicodeRangeToken: lambda t: ['unicode-range', t.start, t.end],

        CurlyBracketsBlock: lambda t: ['{}', *to_json(t.content)],
        SquareBracketsBlock: lambda t: ['[]', *to_json(t.content)],
        ParenthesesBlock: lambda t: ['()', *to_json(t.content)],
        FunctionBlock: lambda t: ['function', t.name, *to_json(t.arguments)],

        Declaration: lambda d: ['declaration', d.name, to_json(d.value), d.important],
        AtRule: lambda r: [
            'at-rule', r.at_keyword, to_json(r.prelude), to_json(r.content)],
        QualifiedRule: lambda r: [
            'qualified rule', to_json(r.prelude), to_json(r.content)],

        RGBA: lambda v: [round(c, 6) for c in v],
        Color: lambda v: [
            v.space,
            [round(c, 6) for c in v.params],
            v.function_name,
            [None if arg is None else round(arg, 6) for arg in v.args],
            v.alpha,
        ],
    }


def load_json(filename):
    import yatest.common as yc
    path = Path(yc.source_path(__file__)).parent / 'css-parsing-tests' / filename
    json_data = json.loads(path.read_text(encoding='utf-8'))
    return list(zip(json_data[::2], json_data[1::2]))


def json_test(filename=None):
    def decorator(function):
        filename_ = filename or function.__name__.split('_', 1)[-1] + '.json'

        @pytest.mark.parametrize(('css', 'expected'), load_json(filename_))
        def test(css, expected):
            value = to_json(function(css))
            if value != expected:  # pragma: no cover
                pprint.pprint(value)
                assert value == expected
        return test
    return decorator


SKIP = {'skip_comments': True, 'skip_whitespace': True}


@json_test()
def test_component_value_list(input):
    return parse_component_value_list(input, skip_comments=True)


@json_test()
def test_one_component_value(input):
    return parse_one_component_value(input, skip_comments=True)


@json_test()
def test_declaration_list(input):
    return parse_declaration_list(input, **SKIP)


@json_test()
def test_blocks_contents(input):
    return parse_blocks_contents(input, **SKIP)


@json_test()
def test_one_declaration(input):
    return parse_one_declaration(input, skip_comments=True)


@json_test()
def test_stylesheet(input):
    return parse_stylesheet(input, **SKIP)


@json_test()
def test_rule_list(input):
    return parse_rule_list(input, **SKIP)


@json_test()
def test_one_rule(input):
    return parse_one_rule(input, skip_comments=True)


@json_test(filename='An+B.json')
def test_nth(input):
    return parse_nth(input)


def _number(value):
    if value is None:
        return 'none'
    value = round(value + 0.0000001, 6)
    return str(int(value) if value.is_integer() else value)


def _build_color(color):
    if color is None:
        return
    (*coordinates, alpha) = color
    result = f'color({color.space}'
    for coordinate in coordinates:
        result += f' {_number(coordinate)}'
    if alpha != 1:
        result += f' / {_number(alpha)}'
    result += ')'
    return result


def test_color_currentcolor_3():
    for value in ('currentcolor', 'currentColor', 'CURRENTCOLOR'):
        assert parse_color3(value) == 'currentColor'


def test_color_currentcolor_4():
    for value in ('currentcolor', 'currentColor', 'CURRENTCOLOR'):
        assert parse_color4(value) == 'currentcolor'


def test_color_currentcolor_5():
    for value in ('currentcolor', 'currentColor', 'CURRENTCOLOR'):
        assert parse_color5(value) == 'currentcolor'


@json_test()
def test_color_function_4(input):
    return _build_color(parse_color4(input))


@json_test(filename='color_function_4.json')
def test_color_function_4_with_5(input):
    return _build_color(parse_color5(input))


@json_test()
def test_color_functions_5(input):
    if input.startswith('light-dark'):
        result = []
        result.append(_build_color(parse_color5(input, ('light',))))
        result.append(_build_color(parse_color5(input, ('dark',))))
    else:
        result = _build_color(parse_color5(input))
    return result


@json_test()
def test_color_hexadecimal_3(input):
    if not (color := parse_color3(input)):
        return None
    (*coordinates, alpha) = color
    result = f'rgb{"a" if alpha != 1 else ""}('
    result += f'{", ".join(_number(coordinate * 255) for coordinate in coordinates)}'
    if alpha != 1:
        result += f', {_number(alpha)}'
    result += ')'
    return result


@json_test(filename='color_hexadecimal_3.json')
def test_color_hexadecimal_3_with_4(input):
    if not (color := parse_color4(input)):
        return None
    (*coordinates, alpha) = color
    result = f'rgb{"a" if alpha != 1 else ""}('
    result += f'{", ".join(_number(coordinate * 255) for coordinate in coordinates)}'
    if alpha != 1:
        result += f', {_number(alpha)}'
    result += ')'
    return result


@json_test()
def test_color_hexadecimal_4(input):
    if not (color := parse_color4(input)):
        return None
    assert color.space == 'srgb'
    (*coordinates, alpha) = color
    result = f'rgb{"a" if alpha != 1 else ""}('
    result += f'{", ".join(_number(coordinate * 255) for coordinate in coordinates)}'
    if alpha != 1:
        result += f', {_number(alpha)}'
    result += ')'
    return result


@json_test(filename='color_hexadecimal_4.json')
def test_color_hexadecimal_4_with_5(input):
    if not (color := parse_color5(input)):
        return None
    assert color.space == 'srgb'
    (*coordinates, alpha) = color
    result = f'rgb{"a" if alpha != 1 else ""}('
    result += f'{", ".join(_number(coordinate * 255) for coordinate in coordinates)}'
    if alpha != 1:
        result += f', {_number(alpha)}'
    result += ')'
    return result


@json_test(filename='color_hexadecimal_3.json')
def test_color_hexadecimal_3_with_5(input):
    if not (color := parse_color5(input)):
        return None
    assert color.space == 'srgb'
    (*coordinates, alpha) = color
    result = f'rgb{"a" if alpha != 1 else ""}('
    result += f'{", ".join(_number(coordinate * 255) for coordinate in coordinates)}'
    if alpha != 1:
        result += f', {_number(alpha)}'
    result += ')'
    return result


@json_test()
def test_color_hsl_3(input):
    if not (color := parse_color3(input)):
        return None
    (*coordinates, alpha) = color
    result = f'rgb{"a" if alpha != 1 else ""}('
    result += f'{", ".join(_number(coordinate * 255) for coordinate in coordinates)}'
    if alpha != 1:
        result += f', {_number(alpha)}'
    result += ')'
    return result


@json_test(filename='color_hsl_3.json')
def test_color_hsl_3_with_4(input):
    if not (color := parse_color4(input)):
        return None
    assert color.space == 'hsl'
    (*coordinates, alpha) = color.to('srgb')
    result = f'rgb{"a" if alpha != 1 else ""}('
    result += f'{", ".join(_number(coordinate * 255) for coordinate in coordinates)}'
    if alpha != 1:
        result += f', {_number(alpha)}'
    result += ')'
    return result


@json_test(filename='color_hsl_3.json')
def test_color_hsl_3_with_5(input):
    if not (color := parse_color5(input)):
        return None
    assert color.space == 'hsl'
    (*coordinates, alpha) = color.to('srgb')
    result = f'rgb{"a" if alpha != 1 else ""}('
    result += f'{", ".join(_number(coordinate * 255) for coordinate in coordinates)}'
    if alpha != 1:
        result += f', {_number(alpha)}'
    result += ')'
    return result


@json_test()
def test_color_hsl_4(input):
    if not (color := parse_color4(input)):
        return None
    assert color.space == 'hsl'
    (*coordinates, alpha) = color.to('srgb')
    result = f'rgb{"a" if alpha != 1 else ""}('
    result += f'{", ".join(_number(coordinate * 255) for coordinate in coordinates)}'
    if alpha != 1:
        result += f', {_number(alpha)}'
    result += ')'
    return result


@json_test(filename='color_hsl_4.json')
def test_color_hsl_4_with_5(input):
    if not (color := parse_color5(input)):
        return None
    assert color.space == 'hsl'
    (*coordinates, alpha) = color.to('srgb')
    result = f'rgb{"a" if alpha != 1 else ""}('
    result += f'{", ".join(_number(coordinate * 255) for coordinate in coordinates)}'
    if alpha != 1:
        result += f', {_number(alpha)}'
    result += ')'
    return result


@json_test()
def test_color_hwb_4(input):
    if not (color := parse_color4(input)):
        return None
    assert color.space == 'hwb'
    (*coordinates, alpha) = color.to('srgb')
    result = f'rgb{"a" if alpha != 1 else ""}('
    result += f'{", ".join(_number(coordinate * 255) for coordinate in coordinates)}'
    if alpha != 1:
        result += f', {_number(alpha)}'
    result += ')'
    return result


@json_test(filename='color_hwb_4.json')
def test_color_hwb_4_with_5(input):
    if not (color := parse_color5(input)):
        return None
    assert color.space == 'hwb'
    (*coordinates, alpha) = color.to('srgb')
    result = f'rgb{"a" if alpha != 1 else ""}('
    result += f'{", ".join(_number(coordinate * 255) for coordinate in coordinates)}'
    if alpha != 1:
        result += f', {_number(alpha)}'
    result += ')'
    return result


@json_test()
def test_color_keywords_3(input):
    if not (color := parse_color3(input)):
        return None
    elif isinstance(color, str):
        return color
    (*coordinates, alpha) = color
    result = f'rgb{"a" if alpha != 1 else ""}('
    result += f'{", ".join(_number(coordinate * 255) for coordinate in coordinates)}'
    if alpha != 1:
        result += f', {_number(alpha)}'
    result += ')'
    return result


@json_test(filename='color_keywords_3.json')
def test_color_keywords_3_with_4(input):
    if not (color := parse_color4(input)):
        return None
    elif isinstance(color, str):
        return color
    assert color.space == 'srgb'
    (*coordinates, alpha) = color
    result = f'rgb{"a" if alpha != 1 else ""}('
    result += f'{", ".join(_number(coordinate * 255) for coordinate in coordinates)}'
    if alpha != 1:
        result += f', {_number(alpha)}'
    result += ')'
    return result


@json_test(filename='color_keywords_3.json')
def test_color_keywords_3_with_5(input):
    if not (color := parse_color5(input)):
        return None
    elif isinstance(color, str):
        return color
    assert color.space == 'srgb'
    (*coordinates, alpha) = color
    result = f'rgb{"a" if alpha != 1 else ""}('
    result += f'{", ".join(_number(coordinate * 255) for coordinate in coordinates)}'
    if alpha != 1:
        result += f', {_number(alpha)}'
    result += ')'
    return result


@json_test()
def test_color_keywords_4(input):
    if not (color := parse_color4(input)):
        return None
    elif isinstance(color, str):
        return color
    assert color.space == 'srgb'
    (*coordinates, alpha) = color
    result = f'rgb{"a" if alpha != 1 else ""}('
    result += f'{", ".join(_number(coordinate * 255) for coordinate in coordinates)}'
    if alpha != 1:
        result += f', {_number(alpha)}'
    result += ')'
    return result


@json_test(filename='color_keywords_4.json')
def test_color_keywords_4_with_5(input):
    if not (color := parse_color5(input)):
        return None
    elif isinstance(color, str):
        return color
    assert color.space == 'srgb'
    (*coordinates, alpha) = color
    result = f'rgb{"a" if alpha != 1 else ""}('
    result += f'{", ".join(_number(coordinate * 255) for coordinate in coordinates)}'
    if alpha != 1:
        result += f', {_number(alpha)}'
    result += ')'
    return result


@json_test()
def test_color_lab_4(input):
    if not (color := parse_color4(input)):
        return None
    elif isinstance(color, str):
        return color
    assert color.space == 'lab'
    (*coordinates, alpha) = color
    result = f'{color.space}('
    result += f'{" ".join(_number(coordinate) for coordinate in coordinates)}'
    if alpha != 1:
        result += f' / {_number(alpha)}'
    result += ')'
    return result


@json_test(filename='color_lab_4.json')
def test_color_lab_4_with_5(input):
    if not (color := parse_color5(input)):
        return None
    elif isinstance(color, str):
        return color
    assert color.space == 'lab'
    (*coordinates, alpha) = color
    result = f'{color.space}('
    result += f'{" ".join(_number(coordinate) for coordinate in coordinates)}'
    if alpha != 1:
        result += f' / {_number(alpha)}'
    result += ')'
    return result


@json_test()
def test_color_oklab_4(input):
    if not (color := parse_color4(input)):
        return None
    elif isinstance(color, str):
        return color
    assert color.space == 'oklab'
    (*coordinates, alpha) = color
    result = f'{color.space}('
    result += f'{" ".join(_number(coordinate) for coordinate in coordinates)}'
    if alpha != 1:
        result += f' / {_number(alpha)}'
    result += ')'
    return result


@json_test(filename='color_oklab_4.json')
def test_color_oklab_4_with_5(input):
    if not (color := parse_color5(input)):
        return None
    elif isinstance(color, str):
        return color
    assert color.space == 'oklab'
    (*coordinates, alpha) = color
    result = f'{color.space}('
    result += f'{" ".join(_number(coordinate) for coordinate in coordinates)}'
    if alpha != 1:
        result += f' / {_number(alpha)}'
    result += ')'
    return result


@json_test()
def test_color_lch_4(input):
    if not (color := parse_color4(input)):
        return None
    elif isinstance(color, str):
        return color
    assert color.space == 'lch'
    (*coordinates, alpha) = color
    result = f'{color.space}('
    result += f'{" ".join(_number(coordinate) for coordinate in coordinates)}'
    if alpha != 1:
        result += f' / {_number(alpha)}'
    result += ')'
    return result


@json_test(filename='color_lch_4.json')
def test_color_lch_4_with_5(input):
    if not (color := parse_color5(input)):
        return None
    elif isinstance(color, str):
        return color
    assert color.space == 'lch'
    (*coordinates, alpha) = color
    result = f'{color.space}('
    result += f'{" ".join(_number(coordinate) for coordinate in coordinates)}'
    if alpha != 1:
        result += f' / {_number(alpha)}'
    result += ')'
    return result


@json_test()
def test_color_oklch_4(input):
    if not (color := parse_color4(input)):
        return None
    elif isinstance(color, str):
        return color
    assert color.space == 'oklch'
    (*coordinates, alpha) = color
    result = f'{color.space}('
    result += f'{" ".join(_number(coordinate) for coordinate in coordinates)}'
    if alpha != 1:
        result += f' / {_number(alpha)}'
    result += ')'
    return result


@json_test(filename='color_oklch_4.json')
def test_color_oklch_4_with_5(input):
    if not (color := parse_color5(input)):
        return None
    elif isinstance(color, str):
        return color
    assert color.space == 'oklch'
    (*coordinates, alpha) = color
    result = f'{color.space}('
    result += f'{" ".join(_number(coordinate) for coordinate in coordinates)}'
    if alpha != 1:
        result += f' / {_number(alpha)}'
    result += ')'
    return result


@json_test()
def test_stylesheet_bytes(kwargs):
    kwargs['css_bytes'] = kwargs['css_bytes'].encode('latin1')
    kwargs.pop('comment', None)
    if kwargs.get('environment_encoding'):
        kwargs['environment_encoding'] = lookup(kwargs['environment_encoding'])
    kwargs.update(SKIP)
    return parse_stylesheet_bytes(**kwargs)


@json_test(filename='component_value_list.json')
def test_serialization(css):
    parsed = parse_component_value_list(css, skip_comments=True)
    return parse_component_value_list(serialize(parsed), skip_comments=True)


def test_skip():
    source = '''
    /* foo */
    @media print {
        #foo {
            width: /* bar*/4px;
            color: green;
        }
    }
    '''
    no_ws = parse_stylesheet(source, skip_whitespace=True)
    no_comment = parse_stylesheet(source, skip_comments=True)
    default = parse_component_value_list(source)
    assert serialize(no_ws) != source
    assert serialize(no_comment) != source
    assert serialize(default) == source


def test_comment_eof():
    source = '/* foo '
    parsed = parse_component_value_list(source)
    assert serialize(parsed) == '/* foo */'


def test_parse_declaration_value_color():
    source = 'color:#369'
    declaration = parse_one_declaration(source)
    (value_token,) = declaration.value
    assert parse_color3(value_token) == (.2, .4, .6, 1)
    assert parse_color4(value_token) == (.2, .4, .6, 1)
    assert declaration.serialize() == source


def test_serialize_rules():
    source = '@import "a.css"; foo#bar.baz { color: red } /**/ @media print{}'
    rules = parse_rule_list(source)
    assert serialize(rules) == source


def test_serialize_declarations():
    source = 'color: #123; /**/ @top-left {} width:7px !important;'
    rules = parse_blocks_contents(source)
    assert serialize(rules) == source


def test_serialize_rules_with_functions():
    source = '''
        foo#bar.baz {
            background: url();
            color: rgb(0, 0, 0);
            width: calc(calc());
            height: calc(calc(calc()));
        }
    '''
    rules = parse_rule_list(source)
    assert serialize(rules) == source


def test_backslash_delim():
    source = '\\\nfoo'
    tokens = parse_component_value_list(source)
    assert [t.type for t in tokens] == ['literal', 'whitespace', 'ident']
    assert tokens[0].value == '\\'
    del tokens[1]
    assert [t.type for t in tokens] == ['literal', 'ident']
    assert serialize(tokens) == source


def test_escape_in_at_rule():
    at_rule, = parse_rule_list('@\udca9')
    assert at_rule.type == 'at-rule'
    assert at_rule.at_keyword == '\udca9'


def test_escape_in_ident():
    declaration = parse_one_declaration('background:\udca9')
    assert declaration.type == 'declaration'
    value, = declaration.value
    assert value.value == '\udca9'


def test_escape_in_dimension_token():
    dimension, = parse_component_value_list('0\\dddf')
    assert dimension.type == 'dimension'
    assert dimension.int_value == 0
    assert dimension.unit == '\udddf'


def test_escape_in_function_name():
    function, = parse_component_value_list('\\dddf()')
    assert function.type == 'function'
    assert function.name == '\udddf'
