import pytest
import re
import yatest.common

PUREBENCH = yatest.common.build_path('yql/essentials/tools/purebench/purebench')


def run_purebench_test(cmdline):
    result = yatest.common.execute(cmdline, text=True, check_exit_code=True)
    # Mask elapsed time and duration, since both can change in
    # different environments.
    stdout = result.stdout
    stdout = re.sub(r'(Elapsed: )(\d+([.]\d+)?)', r'\1<DURATION>', stdout)
    stdout = re.sub(r'(Bench score: )(\d+([.]\d+)?|nan)', r'\1<SCORE>', stdout)
    # Dump the masked stdout.
    outfile = yatest.common.output_path('out')
    with open(outfile, "w") as dump:
        dump.write(stdout)
    # Dump the raw stderr, since all the diagnostic is precise.
    errfile = yatest.common.output_path('err')
    with open(errfile, "w") as dump:
        dump.write(result.stderr)
    # Canonize both stdout and stderr dumps locally.
    return [
        yatest.common.canonical_file(outfile, local=True),
        yatest.common.canonical_file(errfile, local=True),
    ]


@pytest.mark.parametrize(
    "useBlocks",
    [
        pytest.param(False, id="scalar"),
        pytest.param(True, id="blocks"),
    ],
)
def test_purebench_smoke(useBlocks):
    return run_purebench_test(
        [
            PUREBENCH,
            '--ndebug',
            '--repeats',
            '1',
            '--blocks-engine',
            'force' if useBlocks else 'disable',
        ]
    )


@pytest.mark.parametrize(
    "langVer",
    [
        pytest.param(None, id="noarg"),
        pytest.param('2025.04', id="2025.04"),
        pytest.param('unknown', id="unknown"),
    ],
)
def test_purebench_langver(langVer):
    cmdline = [PUREBENCH, '--ndebug', '--repeats', '0', '-t', 'SELECT CurrentLanguageVersion()']
    if langVer:
        cmdline.extend(['--langver', langVer])
    return run_purebench_test(cmdline)


@pytest.mark.parametrize(
    "useBlocks",
    [
        pytest.param('disable', id="no-blocks"),
        pytest.param('auto', id="auto-blocks"),
        pytest.param('force', id="use-blocks"),
    ],
)
def test_purebench_inc(useBlocks):
    return run_purebench_test(
        [
            PUREBENCH,
            '--ndebug',
            '--print-expr',
            '--repeats',
            '1',
            '-w',
            '0',
            '--blocks-engine',
            useBlocks,
            '-t',
            'SELECT index + 1 FROM Input',
        ]
    )


@pytest.mark.parametrize(
    "useBlocks",
    [
        pytest.param('disable', id="no-blocks"),
        pytest.param('auto', id="auto-blocks"),
        pytest.param('force', id="use-blocks"),
    ],
)
def test_purebench_inc_for_42(useBlocks):
    return run_purebench_test(
        [
            PUREBENCH,
            '--ndebug',
            '--print-expr',
            '--repeats',
            '1',
            '-w',
            '0',
            '--blocks-engine',
            useBlocks,
            '-t',
            'SELECT index + 1 FROM Input WHERE index = 42',
        ]
    )
