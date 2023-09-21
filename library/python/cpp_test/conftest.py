import difflib


def iter_diff(fr, to):
    for line in difflib.unified_diff(fr.splitlines(), to.splitlines(), fromfile='L', tofile='R'):
        line = line.rstrip('\n')

        if line:
            if line[0] == '-':
                line = '[[bad]]' + line + '[[rst]]'
            elif line[0] == '+':
                line = '[[good]]' + line + '[[rst]]'

        yield line


def pytest_assertrepr_compare(op, left, right):
    return ['failed, show diff'] + list(iter_diff(left, right))
