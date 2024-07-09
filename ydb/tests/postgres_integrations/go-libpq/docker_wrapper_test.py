from .conftest import integrations

def test(testname):
    integrations.execute_test(testname)
