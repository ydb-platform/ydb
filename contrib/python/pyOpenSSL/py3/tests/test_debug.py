from OpenSSL import version
from OpenSSL.debug import _env_info


def test_debug_info():
    """
    Debug info contains correct data.
    """
    # Just check a sample we control.
    assert version.__version__ in _env_info
