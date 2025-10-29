import yatest.common
import re

PUREBENCH = yatest.common.build_path('yql/essentials/tools/purebench/purebench')


def test_purebench_smoke():
    result = yatest.common.execute([PUREBENCH, '--ndebug', '-r', '1'], text=True, check_exit_code=True)
    # Mask elapsed time and duration, since both can change in
    # different environments.
    stdout = result.stdout
    stdout = re.sub(r'(Elapsed: )(\d+[.]\d+)', r'\1<DURATION>', stdout)
    stdout = re.sub(r'(Bench score: )(\d+[.]\d+)', r'\1<SCORE>', stdout)
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
