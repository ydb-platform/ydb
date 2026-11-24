"""JSONEachRow contracts for ClickHouseClient.ParseFormat, without canondata.

Expectations follow upstream JSONEachRowRowInputFormat::readRow/readPrefix/
readSuffix and stateless tests 00728_json_each_row_parsing,
01072_json_each_row_data_in_square_brackets and
00374_json_each_row_input_with_noisy_fields. Metadata propagation is implemented
by ClickHouseClient's TStreamValue::Fetch.
"""

import json
import os

import pytest

from udf_test_common import facade_runner


def _case(name, messages, expected=(), error_contains=(), metadata=False, nullable=False,
          column="a", data_type="Int32"):
    return pytest.param(
        dict(name=name, messages=messages, expected=expected, error_contains=error_contains,
             metadata=metadata, nullable=nullable, column=column, data_type=data_type),
        id=name,
    )


SCENARIOS = [
    _case('SingleObject', ('{"a":1}',), (1,)),
    _case('AdjacentObjects', ('{"a":1}{"a":2}',), (1, 2)),
    _case('SpaceSeparatedObjects', ('{"a":1} {"a":2}',), (1, 2)),
    _case('NewlineSeparatedObjects', ('{"a":1}\n{"a":2}',), (1, 2)),
    _case('CrLfSeparatedObjects', ('{"a":1}\r\n{"a":2}',), (1, 2)),
    _case('TabSeparatedObjects', ('{"a":1}\t{"a":2}',), (1, 2)),
    _case('BlankLinesBetweenObjects', ('{"a":1}\n\n \t\n{"a":2}',), (1, 2)),
    _case('CommaSeparatedObjects', ('{"a":1}, {"a":2}',), (1, 2)),
    _case('ArrayOfObjects', ('[{"a":1},{"a":2}]',), (1, 2)),
    _case('MultilineArray', ('[\n {"a":1},\n {"a":2}\n ]',), (1, 2)),
    _case('AdjacentObjectsInArray', ('[{"a":1}{"a":2}]',), (1, 2)),
    _case('MultilineObject', ('{\n "a" : 1\n}\n{\n "a" : 2\n}',), (1, 2)),
    _case('SurroundingWhitespace', (' \r\n\t{"a":1}{"a":2}\t\r\n ',), (1, 2)),
    _case('EmptyStream', (), ()),
    _case('EmptyPayload', ('',), ()),
    _case('SpaceOnlyPayload', (' ',), ()),
    _case('WhitespaceOnlyPayload', ('\n\r\t  \r\n',), ()),
    _case('EmptyArray', ('[]',), ()),
    _case('WhitespaceOnlyArray', ('[ \n\t ]',), ()),
    # Optional a isolates object framing from required-column validation.
    _case('EmptyObject', ('{}',), (None,), nullable=True),
    _case('EmptyObjects', ('{}{}',), (None, None), nullable=True),
    # Schema behavior is tested separately from JSON syntax.
    _case('MissingRequiredField', ('{}',), (0,)),
    _case('EmptyMessagesBetweenObjects', ('', '{"a":1}{"a":2}', ' \n', '[{"a":3}]', ''), (1, 2, 3)),
    _case('SeveralPayloads', ('{"a":1}{"a":2}', '{"a":3}'), (1, 2, 3)),
    _case('QuotedNumber', ('{"a":"1"}{"a":2}',), (1, 2)),
    _case('NestedUnknownFields', ('{"a":1,"unused":{"array":[1,{"x":2}],"text":"} {\\n"}}{"a":2}',), (1, 2)),
    _case('TrailingSemicolon', ('{"a":1};',), (1,)),
    _case('ArrayWithSemicolon', ('[{"a":1},{"a":2}];',), (1, 2)),
    _case('RootNull', ('null',), error_contains=("Cannot parse input: expected '{'",)),
    _case('RootNumber', ('123',), error_contains=("Cannot parse input: expected '{'",)),
    _case('RootBoolean', ('true',), error_contains=("Cannot parse input: expected '{'",)),
    _case('RootString', ('"text"',), error_contains=("Cannot parse input: expected '{'",)),
    _case('ArrayOfScalars', ('[1,2]',), error_contains=("Cannot parse input: expected '{'",)),
    _case('NestedRootArray', ('[[{"a":1}]]',), error_contains=("Cannot parse input: expected '{'",)),
    _case('LeadingComma', (',{"a":1}',), error_contains=("Cannot parse input: expected '{'",)),
    _case('UnclosedObject', ('{"a":1',), error_contains=('Unexpected end of stream',)),
    _case('UnclosedArray', ('[{"a":1}',), error_contains=("Cannot parse input: expected ']'",)),
    _case('MissingColon', ('{"a" 1}',), error_contains=("Cannot parse input: expected ':'",)),
    _case('MissingFieldComma', ('{"a":1 "b":2}',), error_contains=("Cannot parse input: expected ','",)),
    _case('WrongFieldType', ('{"a":"not a number"}',), error_contains=('Cannot parse',)),
    _case('DuplicateField', ('{"a":1,"a":2}',), error_contains=('Duplicate field found while parsing',)),
    _case('GarbageAfterObject', ('{"a":1} garbage',), error_contains=("Cannot parse input: expected '{'",)),
    _case('TruncatedTail', ('{"a":1}{',), error_contains=('Unexpected end of stream',)),
    _case('JsonSplitAcrossMessages', ('{"a":1', '}'), error_contains=('Unexpected end of stream',)),
    _case('RepeatedMessageMetadata', (('{"a":1}{"a":2}', 42), ('{"a":3}', 43)), ((1, 42), (2, 42), (3, 43)), metadata=True),
    _case('EmptyMessagesDoNotShiftMetadata', (('', 41), ('[{"a":1},{"a":2}]', 42), (' \n', 43), ('{"a":3}', 44)), ((1, 42), (2, 42), (3, 44)), metadata=True),
    _case('NullableFieldAndMissingField', ('{"a":1}{"a":null}{}',), (1, None, None), nullable=True),

    # Same payloads, schemas and topic offsets as the additional
    # RowDispatcherJsonCompatibility tests. SkipErrors recovery is RD-specific.
    _case('ArraysAndObjectsAcrossMessages', (
        ('[][{"a":1},{"a":2}]{"a":3}', 42),
        ('[{"a":4}][]', 43),
    ), error_contains=("Cannot parse input: expected 'eof'",), metadata=True),
    _case('ScalarAfterObjectInArray', (('[{"a":1},null]', 42),),
          error_contains=("Cannot parse input: expected '{'",), metadata=True),
    _case('TrailingCommaInArray', (('[{"a":1},]', 42),),
          error_contains=("Cannot parse input: expected '{'",), metadata=True),
    _case('NullableArrayElements', (('[{"a":1},{"a":null},{}]', 42),),
          ((1, 42), (None, 42), (None, 42)), metadata=True, nullable=True),
    _case('SkipBadArrayRow', (('[{"a":1},{"a":"bad"},{},{"a":2}]', 42),),
          error_contains=('Cannot parse',), metadata=True),
    _case('SkipMalformedArrayKeepsNextOffset', (
        ('[{"a":1},null,{"a":2}]', 42),
        ('[{"a":3}]', 43),
    ), error_contains=("Cannot parse input: expected '{'",), metadata=True),
    _case('LargeArray', ((json.dumps([{'a': i} for i in range(2001)], separators=(',', ':')), 42),),
          tuple((i, 42) for i in range(2001)), metadata=True),

    # Same names, payloads, String column and offsets as TestJsonParser in topic_parser_ut.cpp.
    # RD's SkipErrors tests recover; ClickHouse.ParseFormat terminates on malformed JSON.
    _case('BatchOffsetsWithWhitespaceAndEmptyArrays', (
        (' \r\n{"col":"first"}{"col":"second"} \t', 42),
        (' [ \t ] ', 44),
        ('[{"col":"third"},{"col":"fourth"}]', 47),
        ('[]', 48),
        (' \n{"col":"fifth"} ', 51),
    ), (('first', 42), ('second', 42), ('third', 47), ('fourth', 47), ('fifth', 51)),
        metadata=True, column="col", data_type="String"),
    _case('BatchFallbackClearsParsedStrings', (
        ('{"col":"' + 'a' * 128 + '"}', 42),
        ('{"col":"' + 'b' * 128 + '"}{', 43),
        ('{"col":"' + 'c' * 128 + '"}', 44),
    ), error_contains=('Unexpected end of stream',), metadata=True, column="col", data_type="String"),
    _case('BatchFallbackAfterSkippedColumnAndArrayError', (
        ('{"col":"first"}', 42),
        ('{"col":123}', 43),
        ('[{"col":"second"},null,{"col":"ignored"}]', 44),
        ('{"col":"third"}', 45),
    ), error_contains=('Cannot parse',), metadata=True, column="col", data_type="String"),
    # String is binary in ClickHouse: preserve the raw 0xff byte, rather than U+00FF.
    _case('BatchInvalidUtf8KeepsOtherMessageOffsets', (
        ('{"col":"first"}', 42),
        (b'{"col":"\xff"}', 43),
        ('{"col":"second"}', 44),
    ), (('first', 42), (b'\xff', 43), ('second', 44)),
        metadata=True, column="col", data_type="String"),
    _case('BatchArrayCannotCrossMessageBoundary', (
        ('{"col":"first"}', 42),
        ('[{"col":"second"},', 43),
        ('{"col":"third"}]', 44),
    ), error_contains=("Cannot parse input: expected ']'",), metadata=True, column="col", data_type="String"),
    # Same damaged message as RD; the UDF diagnostic does not include topic offsets.
    _case('BatchFallbackReportsOriginalMessageOffset', (
        ('{"col":"first"}', 42),
        ('{"col":"second"}{', 43),
        ('{"col":"third"}', 44),
    ), error_contains=('Unexpected end of stream',), metadata=True, column="col", data_type="String"),
]


RUNNER_FACTORY = facade_runner(
    prov="yt",
    cfg_dir=os.getenv("YQL_CONFIG_DIR") or "yql/essentials/cfg/udf_test",
)


def _sql_list(items, empty_item):
    return "AsList(" + ", ".join(items) + ")" if items else "ListTake(AsList(" + empty_item + "), 0)"


def _sql_literal(value):
    if isinstance(value, bytes):
        # Emit binary YQL string escapes, without UTF-8 decoding or JSON transcoding.
        return '"' + ''.join('\\x%02x' % byte for byte in value) + '"'
    return json.dumps(value)


def _build_program(scenario):
    column = scenario["column"]
    data_type = scenario["data_type"]
    schema = f"Struct<'{column}':{data_type}{'?' if scenario['nullable'] else ''}>"

    def expected_value(value):
        if scenario["nullable"]:
            return f"Nothing(OptionalType({data_type}))" if value is None else "Just(" + _sql_literal(value) + ")"
        return _sql_literal(value)

    if scenario["metadata"]:
        schema = "TupleType(" + schema + ", Uint64)"
        messages = ["AsTuple(" + _sql_literal(payload) + ", " + str(offset) + "ul)"
                    for payload, offset in scenario["messages"]]
        expected = ["AsTuple(" + expected_value(value) + ", " + str(offset) + "ul)"
                    for value, offset in scenario["expected"]]
        projection = """($row) -> {
    $data = $row.0;
    RETURN AsTuple($data.%s, $row.1);
}""" % column
    else:
        messages = [_sql_literal(payload) for payload in scenario["messages"]]
        expected = [expected_value(value) for value in scenario["expected"]]
        projection = "($row) -> ($row." + column + ")"

    program = f"""use plato;
$messages = {_sql_list(messages, '""')};
$input = Yql::Iterator($messages, Yql::DependsOn("{scenario['name']}"));
$parse = YQL::Udf(
    AsAtom("ClickHouseClient.ParseFormat"),
    Void(),
    TupleType(TupleType(TypeOf($input)), Struct<>, {schema}),
    AsAtom("json_each_row")
);
$actual = ListMap(Yql::Collect($parse($input)), {projection});
"""
    if scenario["error_contains"]:
        # Collect forces the complete stream, including malformed tails.
        return program + "SELECT $actual AS actual;\n"

    # Pickling avoids nullable equality returning NULL instead of a boolean.
    condition = "StablePickle($actual) == StablePickle($expected)" if scenario["nullable"] else "$actual == $expected"
    return program + f"""$expected = {_sql_list(expected, '0')};
SELECT Ensure(
    "OK",
    {condition},
    "{scenario['name']}: unexpected parsed rows, order or metadata"
) AS result;
"""


@pytest.mark.parametrize("scenario", SCENARIOS)
def test_json_each_row(scenario):
    expected_error = scenario["error_contains"]
    result = RUNNER_FACTORY("unknown").yql_exec(
        program=_build_program(scenario),
        run_sql=True,
        check_error=not expected_error,
        require_udf_resolver=True,
        scan_udfs=True,
    )
    if expected_error:
        assert result.execution_result.exit_code != 0, "Malformed input was accepted"
        diagnostics = (result.std_err or "") + "\n" + (result.execution_result.std_err or "")
        for fragment in expected_error:
            assert fragment in diagnostics, diagnostics
    else:
        # Ensure compares the complete ordered list, including row count.
        assert result.execution_result.exit_code == 0, result.std_err
        assert '"OK"' in result.results, result.results
