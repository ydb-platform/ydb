import pytest
import yatest.common

PUREBENCH = yatest.common.build_path('yql/essentials/tools/purebench/cli/purebench')


def assert_invalid_options(arguments, message):
    result = yatest.common.execute(
        [
            PUREBENCH,
            '--ndebug',
            '--llvm-settings',
            'OFF',
            '-c',
            '0',
            '-w',
            '0',
            '--repeats',
            '0',
            '--repeat-time',
            '0',
            '--calibrate',
            '0',
            '--calibrate-time',
            '0',
        ]
        + arguments,
        text=True,
        check_exit_code=False,
        timeout=5,
    )
    assert result.exit_code == 1, result.stderr
    assert message in result.stderr


@pytest.mark.parametrize(
    "arguments, message",
    [
        pytest.param(['--repeat-time', '0garbage'], '0garbage', id='trailing-text'),
        pytest.param(['--calibrate-time', '0.5'], '0.5', id='fraction'),
        pytest.param(['--repeat-time', '-1'], '-1', id='negative'),
    ],
)
def test_invalid_duration_syntax(arguments, message):
    assert_invalid_options(arguments, message)


def test_duration_overflow():
    assert_invalid_options(['--calibrate-time', '18446744073709551615'], 'Duration in seconds is too large')


def test_conflicting_expression_output():
    assert_invalid_options(['--print-expr', '--expr-file', 'unused-expression.yql'], "can't appear together")


def test_empty_expression_path():
    assert_invalid_options(['--expr-file', ''], '--expr-file requires a non-empty path')
