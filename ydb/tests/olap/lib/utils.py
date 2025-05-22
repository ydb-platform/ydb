import yatest.common


def get_external_param(name: str, default):
    try:
        return yatest.common.get_param(name, default=default)
    except yatest.common.NoRuntimeFormed:
        return default


def external_param_is_true(name: str) -> bool:
    return get_external_param(name, '').lower() in ['t', 'true', 'yes', '1', 'da']
