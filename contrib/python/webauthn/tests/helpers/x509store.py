import inspect
from unittest.mock import patch

from OpenSSL.crypto import X509Store
from webauthn.helpers import validate_certificate_chain


def patch_validate_certificate_chain_x509store_getter(func):
    """
    This is a very purpose-built decorator to help set a fixed time for X.509 certificate chain
    validation in unittests. It makes the following assumptions, all of which must be true for this
    decorator to remain useful:

    - X.509 certificate chain validation occurs in **webauthn/helpers/validate_certificate_chain.py::**`validate_certificate_chain`
    - `validate_certificate_chain(...)` uses `OpenSSL.crypto.X509Store` to verify certificate chains
    - **webauthn/helpers/__init__.py** continues to re-export `validate_certificate_chain`

    Usage:

    ```
    from unittest import TestCase
    from datetime import datetime
    from OpenSSL.crypto import X509Store

    class TestX509Validation(TestCase):
        @patch_validate_certificate_chain_x509store_getter
        def test_validate_x509_chain(self, patched_x509store: X509Store):
            patched_x509store.set_time(datetime(2021, 9, 1, 0, 0, 0))
            # ...
    ```
    """

    def wrapper(*args, **kwargs):
        """
        Using `inspect.getmodule(...)` below helps deal with the fact that, in Python 3.9 and
        Python 3.10, `@patch("webauthn.helpers.validate_certificate_chain._generate_new_cert_store")`
        errors out because `webauthn.helpers.validate_certificate_chain` is understood to be the method
        re-exported via `__all__` in **webauthn/helpers/__init__.py**, not the module of the same name.
        """
        with patch.object(
            inspect.getmodule(validate_certificate_chain),
            "_generate_new_cert_store",
        ) as mock_generate_new_cert_store:
            new_cert_store = X509Store()
            mock_generate_new_cert_store.return_value = new_cert_store
            return func(*args, new_cert_store, **kwargs)

    return wrapper
