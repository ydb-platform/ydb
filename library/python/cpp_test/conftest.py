import difflib


def iter_diff(fr, to):
    for l in difflib.unified_diff(fr.splitlines(), to.splitlines(), fromfile='L', tofile='R'):
        l = l.rstrip('\n')

        if l:
            if l[0] == '-':
                l = '[[bad]]' + l + '[[rst]]'
            elif l[0] == '+':
                l = '[[good]]' + l + '[[rst]]'

        yield l


def pytest_assertrepr_compare(op, left, right):
    return ['failed, show diff'] + list(iter_diff(left, right))
