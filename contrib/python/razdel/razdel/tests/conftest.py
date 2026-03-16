

def pytest_assertrepr_compare(op, left, right):
    # add one more line for "assert ..."
    return (
        ['']
        + ['---']
        + ['> ' + _.text for _ in left]
        + ['---']
        + ['> ' + _.text for _ in right]
    )


def pytest_addoption(parser):
    parser.addoption('--int', type='int')


def pytest_generate_tests(metafunc):
    if 'int_test' in metafunc.fixturenames:
        tests = []
        count = metafunc.config.getoption('int')
        if count:
            tests = metafunc.module.int_tests(count)
        metafunc.parametrize('int_test', tests)
