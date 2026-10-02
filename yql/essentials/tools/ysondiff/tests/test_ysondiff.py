from yql.essentials.tools.ysondiff.lib import compare_contents, normalize_file_content


def test_compare_equal_yson_ignores_key_order():
    left = b'{b=2;a=1}'
    right = b'{a=1;b=2}'
    expected = '{\n    "a" = 1;\n    "b" = 2\n}'
    assert normalize_file_content(left) == expected
    assert normalize_file_content(right) == expected
    assert compare_contents(left, right) is None


def test_compare_yson_table_preserves_row_order():
    # Row order matters for sorted tables (see is_sorted_table / dump_table_yson(sort=...)).
    # ysondiff must not reorder rows; callers with schema decide whether to sort.
    left = b'{a=1};{a=2}'
    right = b'{a=2};{a=1}'
    assert normalize_file_content(left) == (
        '[\n    {\n        "a" = 1\n    };\n    {\n        "a" = 2\n    }\n]'
    )
    assert normalize_file_content(right) == (
        '[\n    {\n        "a" = 2\n    };\n    {\n        "a" = 1\n    }\n]'
    )
    expected_diff = (
        '--- L\n'
        '+++ R\n'
        '@@ -1,8 +1,8 @@\n'
        ' [\n'
        '     {\n'
        '-        "a" = 1\n'
        '+        "a" = 2\n'
        '     };\n'
        '     {\n'
        '-        "a" = 2\n'
        '+        "a" = 1\n'
        '     }\n'
        ' ]'
    )
    assert ''.join(compare_contents(left, right, fromfile='L', tofile='R')) == expected_diff


def test_compare_different_yson():
    expected_diff = (
        '--- L\n'
        '+++ R\n'
        '@@ -1,3 +1,3 @@\n'
        ' {\n'
        '-    "a" = 1\n'
        '+    "a" = 2\n'
        ' }'
    )
    assert ''.join(compare_contents(b'{a=1}', b'{a=2}', fromfile='L', tofile='R')) == expected_diff


def test_compare_equal_json_ignores_key_order():
    left = b'{"b": 2, "a": 1}'
    right = b'{"a": 1, "b": 2}'
    expected = '{\n    "a": 1,\n    "b": 2\n}'
    assert normalize_file_content(left) == expected
    assert normalize_file_content(right) == expected
    assert compare_contents(left, right) is None


def test_compare_different_json():
    expected_diff = (
        '--- L\n'
        '+++ R\n'
        '@@ -1,3 +1,3 @@\n'
        ' {\n'
        '-    "a": 1\n'
        '+    "a": 2\n'
        ' }'
    )
    assert ''.join(compare_contents(b'{"a": 1}', b'{"a": 2}', fromfile='L', tofile='R')) == expected_diff


def test_compare_json_array_preserves_row_order():
    left = b'[{"a":1},{"a":2}]'
    right = b'[{"a":2},{"a":1}]'
    assert normalize_file_content(left) == (
        '[\n    {\n        "a": 1\n    },\n    {\n        "a": 2\n    }\n]'
    )
    assert normalize_file_content(right) == (
        '[\n    {\n        "a": 2\n    },\n    {\n        "a": 1\n    }\n]'
    )
    expected_diff = (
        '--- L\n'
        '+++ R\n'
        '@@ -1,8 +1,8 @@\n'
        ' [\n'
        '     {\n'
        '-        "a": 1\n'
        '+        "a": 2\n'
        '     },\n'
        '     {\n'
        '-        "a": 2\n'
        '+        "a": 1\n'
        '     }\n'
        ' ]'
    )
    assert ''.join(compare_contents(left, right, fromfile='L', tofile='R')) == expected_diff


def test_json_map_all_value_types_diff():
    # JSON analogues: null, number, bool x2, list, object, utf8 string.
    left = (
        '{"null":null,'
        '"double":1.25,'
        '"int":-3,'
        '"bool_change":true,'
        '"bool_delete":false,'
        '"list":[1,"a"],'
        '"utf8":"Привет",'
        '"dict":{"x":1}}'
    ).encode('utf-8')
    right = (
        '{"null":null,'
        '"double":2.5,'
        '"int":-4,'
        '"bool_change":false,'
        '"list":[2,"b"],'
        '"utf8":"Пока",'
        '"dict":{"x":2},'
        '"new_key":"added"}'
    ).encode('utf-8')
    expected_diff = (
        '--- L\n'
        '+++ R\n'
        '@@ -1,15 +1,15 @@\n'
        ' {\n'
        '-    "bool_change": true,\n'
        '-    "bool_delete": false,\n'
        '+    "bool_change": false,\n'
        '     "dict": {\n'
        '-        "x": 1\n'
        '+        "x": 2\n'
        '     },\n'
        '-    "double": 1.25,\n'
        '-    "int": -3,\n'
        '+    "double": 2.5,\n'
        '+    "int": -4,\n'
        '     "list": [\n'
        '-        1,\n'
        '-        "a"\n'
        '+        2,\n'
        '+        "b"\n'
        '     ],\n'
        '+    "new_key": "added",\n'
        '     "null": null,\n'
        '-    "utf8": "Привет"\n'
        '+    "utf8": "Пока"\n'
        ' }'
    )
    assert ''.join(compare_contents(left, right, fromfile='L', tofile='R')) == expected_diff


def test_normalize_keeps_entity():
    assert normalize_file_content(b'{keep=1;null=#}') == (
        '{\n    "keep" = 1;\n    "null" = #\n}'
    )


def test_normalize_keeps_void_string():
    assert normalize_file_content(b'{keep="x";gone=Void}') == (
        '{\n    "gone" = "Void";\n    "keep" = "x"\n}'
    )


def test_normalize_other_list_is_sorted():
    left = b'{_other=[2;1]}'
    right = b'{_other=[1;2]}'
    expected = '{\n    "_other" = [\n        1;\n        2\n    ]\n}'
    assert normalize_file_content(left) == expected
    assert normalize_file_content(right) == expected
    assert compare_contents(left, right) is None


def test_empty_content():
    assert normalize_file_content(b'') == ''
    assert normalize_file_content(b'   \n') == ''
    assert compare_contents(b'', b'') is None


def test_requires_bytes():
    try:
        normalize_file_content('{a=1}')
    except TypeError as exc:
        assert str(exc) == 'content must be bytes, got str'
    else:
        raise AssertionError('expected TypeError')

    try:
        compare_contents('{a=1}', b'{a=1}')
    except TypeError as exc:
        assert str(exc) == 'left must be bytes, got str'
    else:
        raise AssertionError('expected TypeError')

    try:
        compare_contents(b'{a=1}', '{a=1}')
    except TypeError as exc:
        assert str(exc) == 'right must be bytes, got str'
    else:
        raise AssertionError('expected TypeError')


def test_binary_non_utf8_roundtrip():
    # Invalid UTF-8 is shown with C-style \xHH escapes (backslashreplace).
    payload = b'{x="\xff\xfe"}'
    expected = '{\n    "x" = "\\xff\\xfe"\n}'
    assert normalize_file_content(payload) == expected
    assert compare_contents(payload, payload) is None


def test_non_utf8_shown_in_diff():
    left = 'Привет!ёЁ'.encode('utf-8') + b'\xff\xfeHello123'
    right = b'123'
    assert normalize_file_content(left) == 'Привет!ёЁ\\xff\\xfeHello123'
    assert normalize_file_content(right) == '123'
    # No trailing newlines in payloads => difflib does not insert '\n' between '-'/'+' lines.
    expected_diff = (
        '--- L\n'
        '+++ R\n'
        '@@ -1 +1 @@\n'
        '-Привет!ёЁ\\xff\\xfeHello123'
        '+123'
    )
    assert ''.join(compare_contents(left, right, fromfile='L', tofile='R')) == expected_diff


def test_yson_map_all_value_types_diff():
    # One string key per YSON value type. bool has two keys: one changes, one is deleted.
    # Nested "dict" is last so the payload has no '};' (avoids table-normalization path).
    left = (
        b'{'
        b'"entity"=#;'
        b'"double"=1.25;'
        b'"uint"=7u;'
        b'"int"=-3;'
        b'"bool_change"=%true;'
        b'"bool_delete"=%false;'
        b'"list"=[1;"a"];'
        b'"utf8"="\xd0\x9f\xd1\x80\xd0\xb8\xd0\xb2\xd0\xb5\xd1\x82";'
        b'"binary"="\xff\xfe";'
        b'"dict"={"x"=1}'
        b'}'
    )
    right = (
        b'{'
        b'"entity"=#;'
        b'"double"=2.5;'
        b'"uint"=8u;'
        b'"int"=-4;'
        b'"bool_change"=%false;'
        b'"list"=[2;"b"];'
        b'"utf8"="\xd0\x9f\xd0\xbe\xd0\xba\xd0\xb0";'
        b'"binary"="\xff\x00";'
        b'"new_key"="added";'
        b'"dict"={"x"=2}'
        b'}'
    )
    expected_diff = (
        '--- L\n'
        '+++ R\n'
        '@@ -1,17 +1,17 @@\n'
        ' {\n'
        '-    "binary" = "\\xff\\xfe";\n'
        '-    "bool_change" = %true;\n'
        '-    "bool_delete" = %false;\n'
        '+    "binary" = "\\xff\\0";\n'
        '+    "bool_change" = %false;\n'
        '     "dict" = {\n'
        '-        "x" = 1\n'
        '+        "x" = 2\n'
        '     };\n'
        '-    "double" = 1.2500000000000000;\n'
        '+    "double" = 2.5000000000000000;\n'
        '     "entity" = #;\n'
        '-    "int" = -3;\n'
        '+    "int" = -4;\n'
        '     "list" = [\n'
        '-        1;\n'
        '-        "a"\n'
        '+        2;\n'
        '+        "b"\n'
        '     ];\n'
        '-    "uint" = 7u;\n'
        '-    "utf8" = "Привет"\n'
        '+    "new_key" = "added";\n'
        '+    "uint" = 8u;\n'
        '+    "utf8" = "Пока"\n'
        ' }'
    )
    assert ''.join(compare_contents(left, right, fromfile='L', tofile='R')) == expected_diff
