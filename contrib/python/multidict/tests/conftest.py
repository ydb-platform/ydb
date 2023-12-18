import pickle

import pytest

from multidict._compat import USE_EXTENSIONS

OPTIONAL_CYTHON = (
    ()
    if USE_EXTENSIONS
    else pytest.mark.skip(reason="No extensions available")
)


@pytest.fixture(  # type: ignore[call-overload]
    scope="session",
    params=[
        pytest.param("multidict._multidict", marks=OPTIONAL_CYTHON),  # type: ignore
        "multidict._multidict_py",
    ],
)
def _multidict(request):
    return pytest.importorskip(request.param)


def pytest_generate_tests(metafunc):
    if "pickle_protocol" in metafunc.fixturenames:
        metafunc.parametrize(
            "pickle_protocol", list(range(pickle.HIGHEST_PROTOCOL + 1)), scope="session"
        )
