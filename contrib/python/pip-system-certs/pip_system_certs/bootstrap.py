import sys
import site

_registered = False


def _register_bootstrap_functions():
    # This should in practice only ever be called once, but protect
    # outselves just in case it is somehow called a second time.

    global _registered

    if _registered:
        return 

    try:
        _registered = True

        # Inject truststore into SSL to use system certificates
        # This should be safe to do at this point as module search path is setup
        from . import wrapt_requests
        wrapt_requests.inject_truststore()
    except Exception as ex:
        print("pip_system_certs: ERROR: could not register module:", ex)


def _execsitecustomize_wrapper(wrapped):
    def _execsitecustomize(*args, **kwargs):
        try:
            return wrapped(*args, **kwargs)
        finally:
            # Check whether 'usercustomize' support is actually disabled.
            # In that case we do our work after 'sitecustomize' is loaded.

            if not site.ENABLE_USER_SITE:
                _register_bootstrap_functions()
    return _execsitecustomize


def _execusercustomize_wrapper(wrapped):
    def _execusercustomize(*args, **kwargs):
        try:
            return wrapped(*args, **kwargs)
        finally:
            _register_bootstrap_functions()
    return _execusercustomize


def bootstrap():
    # We want to do our real work as the very last thing in the 'site'
    # module when it is being imported so that the module search path is
    # initialised properly. What is the last thing executed depends on
    # whether 'usercustomize' module support is enabled. Such support
    # will not be enabled in Python virtual enviromments. We therefore
    # wrap the functions for the loading of both the 'sitecustomize' and
    # 'usercustomize' modules but detect when 'usercustomize' support is
    # disabled and in that case do what we need to after 'sitecustomize'
    # is loaded.
    #
    # In wrapping these functions though, we can't actually use wrapt
    # to do so. This is because depending on how wrapt was installed it
    # may technically be dependent on '.pth' evaluation for Python to
    # know where to import it from. The addition of the directory which
    # contains wrapt may not yet have been done. We thus use a simple
    # function wrapper instead.

    site.execsitecustomize = _execsitecustomize_wrapper(site.execsitecustomize)
    site.execusercustomize = _execusercustomize_wrapper(site.execusercustomize)
