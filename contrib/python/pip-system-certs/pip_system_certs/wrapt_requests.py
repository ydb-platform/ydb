import ssl
import inspect

_injected = False
_pip_patched = False

def inject_truststore():
    global _injected
    if not _injected:
        try:
            # Try to use pip's vendored truststore first (available in pip 24.2+)
            try:
                from pip._vendor import truststore
            except ImportError:
                # Fallback to standalone truststore for older pip versions
                try:
                    import truststore
                except ImportError:
                    print("pip_system_certs: ERROR: truststore not available")
                    return

            # Check if truststore is already injected globally
            default_context = ssl.create_default_context()
            if isinstance(default_context, truststore.SSLContext):
                _injected = True
                return

            # Inject truststore for all SSL connections (pip, requests, etc.)
            truststore.inject_into_ssl()
            _injected = True

            # Apply defensive patch to pip's Insecure adapters for issue #39
            _patch_pip_insecure_adapters()

        except Exception as ex:
            print("pip_system_certs: ERROR: could not inject truststore:", ex)


def _patch_pip_insecure_adapters():
    """
    Patch pip's InsecureCacheControlAdapter and InsecureHTTPAdapter to fix issue #39.

    These adapters override cert_verify() to force verify=False but don't override
    get_connection_with_tls_context(), causing a ValueError when check_hostname=True
    and verify_mode=CERT_NONE are set together.

    See: https://gitlab.com/alelec/pip-system-certs/-/issues/39
    """
    global _pip_patched
    if _pip_patched:
        return

    try:
        from pip._internal.network.session import (
            InsecureCacheControlAdapter,
            InsecureHTTPAdapter,
            CacheControlAdapter,
            HTTPAdapter
        )
    except ImportError:
        # pip not available or has different structure - skip patch
        return

    # Verify the classes have the expected structure
    if not _verify_adapter_structure(InsecureCacheControlAdapter, CacheControlAdapter):
        return
    if not _verify_adapter_structure(InsecureHTTPAdapter, HTTPAdapter):
        return

    # Check if get_connection_with_tls_context exists and has expected signature
    base_method = getattr(CacheControlAdapter, 'get_connection_with_tls_context', None)
    if base_method is None:
        # Method doesn't exist on base class - pip structure changed
        return

    # Verify method signature matches what we expect
    try:
        sig = inspect.signature(base_method)
        expected_params = {'self', 'request', 'verify', 'proxies', 'cert'}
        actual_params = set(sig.parameters.keys())
        if expected_params != actual_params:
            # Signature changed - don't patch
            return
    except (ValueError, TypeError):
        # Can't inspect signature - be conservative and skip
        return

    # Check if already patched (look for our marker attribute)
    if getattr(InsecureCacheControlAdapter, '_pip_system_certs_patched', False):
        _pip_patched = True
        return

    # Apply the patch
    def _insecure_get_connection_with_tls_context(self, request, verify, proxies=None, cert=None):
        """Override to force verify=False, preventing check_hostname conflict."""
        # Call parent's implementation with verify=False
        return super(self.__class__, self).get_connection_with_tls_context(
            request, verify=False, proxies=proxies, cert=cert
        )

    try:
        InsecureCacheControlAdapter.get_connection_with_tls_context = _insecure_get_connection_with_tls_context
        InsecureCacheControlAdapter._pip_system_certs_patched = True

        InsecureHTTPAdapter.get_connection_with_tls_context = _insecure_get_connection_with_tls_context
        InsecureHTTPAdapter._pip_system_certs_patched = True

        _pip_patched = True
    except Exception:
        # Patching failed - continue without it
        pass


def _verify_adapter_structure(adapter_class, expected_base):
    """Verify that an adapter class has the expected base class."""
    try:
        # Check that the expected base is in the MRO
        if expected_base not in adapter_class.__mro__:
            return False

        # Verify cert_verify method exists (these are Insecure adapters)
        if not hasattr(adapter_class, 'cert_verify'):
            return False

        return True
    except Exception:
        return False
