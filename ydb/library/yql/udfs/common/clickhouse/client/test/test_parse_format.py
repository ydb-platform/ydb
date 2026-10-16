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


def _case(name, messages, expected=(), error_contains=(), metadata=False, nullable=False):
    return pytest.param(
        dict(name=name, messages=messages, expected=expected, error_contains=error_contains,
             metadata=metadata, nullable=nullable),
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
]


RUNNER_FACTORY = facade_runner(
    prov="yt",
    cfg_dir=os.getenv("YQL_CONFIG_DIR") or "yql/essentials/cfg/udf_test",
)


def _sql_list(items, empty_item):
    return "AsList(" + ", ".join(items) + ")" if items else "ListTake(AsList(" + empty_item + "), 0)"


def _build_program(scenario):
    schema = "Struct<'a':Int32?>" if scenario["nullable"] else "Struct<'a':Int32>"
    if scenario["metadata"]:
        schema = "TupleType(" + schema + ", Uint64)"
        messages = ["AsTuple(" + json.dumps(payload) + ", " + str(offset) + "ul)"
                    for payload, offset in scenario["messages"]]
        expected = ["AsTuple(" + str(value) + ", " + str(offset) + "ul)"
                    for value, offset in scenario["expected"]]
        projection = """($row) -> {
    $data = $row.0;
    RETURN AsTuple($data.a, $row.1);
}"""
    else:
        messages = [json.dumps(payload) for payload in scenario["messages"]]
        if scenario["nullable"]:
            expected = ["Nothing(OptionalType(Int32))" if value is None else "Just(" + str(value) + ")"
                        for value in scenario["expected"]]
        else:
            expected = [str(value) for value in scenario["expected"]]
        projection = "($row) -> ($row.a)"

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
