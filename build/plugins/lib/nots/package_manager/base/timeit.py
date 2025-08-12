import importlib


def import_optional_module(module_name):
    # Initialize the cache attribute if it does not exist
    if not hasattr(import_optional_module, 'cache'):
        import_optional_module.cache = {}

    # Check if the module is already in the cache
    if module_name in import_optional_module.cache:
        return import_optional_module.cache[module_name]

    # Attempt to import the module
    try:
        module = importlib.import_module(module_name)
    except ImportError:
        module = None

    # Cache the result
    import_optional_module.cache[module_name] = module
    return module


def timeit(func):
    logging = import_optional_module("devtools.frontend_build_platform.libraries.logging")
    if logging:
        return logging.timeit(func)
    else:
        return func


def is_timeit_enabled():
    logging = import_optional_module("devtools.frontend_build_platform.libraries.logging")
    if logging:
        return logging.timeit_options.enabled
    else:
        return False
