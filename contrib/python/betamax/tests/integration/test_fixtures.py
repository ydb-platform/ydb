import os.path

import pytest


@pytest.mark.usefixtures('betamax_session')
class TestPyTestFixtures:
    @pytest.fixture(autouse=True)
    def setup(self, request):
        """After test hook to assert everything."""
        def finalizer():
            test_dir = os.path.abspath('.')
            cassette_name = ('tests.integration.test_fixtures.'  # Module name
                             'TestPyTestFixtures.'  # Class name
                             'test_pytest_fixture'  # Test function name
                             '.json')
            file_name = os.path.join(test_dir, 'tests', 'cassettes',
                                     cassette_name)
            assert os.path.exists(file_name) is True

        request.addfinalizer(finalizer)

    def test_pytest_fixture(self, betamax_session):
        """Exercise the fixture itself."""
        resp = betamax_session.get('https://httpbin.org/get')
        assert resp.ok


@pytest.mark.usefixtures('betamax_parametrized_session')
class TestPyTestParametrizedFixtures:
    @pytest.fixture(autouse=True)
    def setup(self, request):
        """After test hook to assert everything."""
        def finalizer():
            test_dir = os.path.abspath('.')
            cassette_name = ('tests.integration.test_fixtures.'  # Module name
                             'TestPyTestParametrizedFixtures.'  # Class name
                             'test_pytest_fixture'  # Test function name
                             '[https---httpbin.org-get]'  # Parameter
                             '.json')
            file_name = os.path.join(test_dir, 'tests', 'cassettes',
                                     cassette_name)
            assert os.path.exists(file_name) is True

        request.addfinalizer(finalizer)

    @pytest.mark.parametrize('url', ('https://httpbin.org/get',))
    def test_pytest_fixture(self, betamax_parametrized_session, url):
        """Exercise the fixture itself."""
        resp = betamax_parametrized_session.get(url)
        assert resp.ok


@pytest.mark.parametrize('problematic_arg', [r'aaa\bbb', 'ccc:ddd', 'eee*fff'])
def test_pytest_parametrize_with_filesystem_problematic_chars(
        betamax_parametrized_session, problematic_arg):
    """
    Exercice parametrized args containing characters which might cause
    problems when getting translated into file names. """
    assert True
