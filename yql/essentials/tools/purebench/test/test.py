import pytest
import re
import yatest.common

PUREBENCH = yatest.common.build_path('yql/essentials/tools/purebench/purebench')


def run_purebench_test(cmdline):
    result = yatest.common.execute(cmdline, text=True, check_exit_code=True)
    # Mask benchmark score and duration, since both can change in
    # different environments.
    stdout = result.stdout
    stdout = re.sub(
        r'(Benchmark completed: )(\d+)( iterations for )(\d+([.]\d+)?)', r'\1<ITERATIONS>\3<DURATION>', stdout
    )
    fraction = r'(\d+([.]\d+(e+\d+)?)?|nan)'
    stdout = re.sub(f'(Bench score: ){fraction}', r'\1<SCORE>', stdout)
    stdout = re.sub(f'(mean wall clock: ){fraction}', r'\1<WALL-CLOCK>', stdout)
    stdout = re.sub(f'(cv: ){fraction}', r'\1<CV>', stdout)
    # Dump the masked stdout.
    outfile = yatest.common.output_path('out')
    with open(outfile, "w") as dump:
        dump.write(stdout)
    # Mask calibration stats, since it can change either.
    stderr = result.stderr
    stderr = re.sub(
        r'(Calibration completed: )(\d+)( iterations for )(\d+([.]\d+)?)', r'\1<ITERATIONS>\3<DURATION>', stderr
    )
    # Dump the masked stderr.
    errfile = yatest.common.output_path('err')
    with open(errfile, "w") as dump:
        dump.write(stderr)
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
            '--calibrate',
            '1',
            '--repeat-time',
            '0',
            '--calibrate-time',
            '0',
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
    cmdline = [
        PUREBENCH,
        '--ndebug',
        '--repeats',
        '0',
        '--repeat-time',
        '0',
        '--calibrate',
        '0',
        '--calibrate-time',
        '0',
        '-t',
        'SELECT CurrentLanguageVersion()',
    ]
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
            '--calibrate',
            '1',
            '--repeat-time',
            '0',
            '--calibrate-time',
            '0',
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
            '--calibrate',
            '1',
            '--repeat-time',
            '0',
            '--calibrate-time',
            '0',
            '-w',
            '0',
            '--blocks-engine',
            useBlocks,
            '-t',
            'SELECT index + 1 FROM Input WHERE index = 42',
        ]
    )
