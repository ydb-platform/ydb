import warnings

import sqlalchemy


def parse_version(v):
    """
        Take a string version and conver it to a tuple (for easier comparison), e.g.:

            "1.2.3" --> (1, 2, 3)
            "1.2" --> (1, 2, 0)
            "1" --> (1, 0, 0)
    """
    parts = v.split(".")
    # Pad the list to make sure there is three elements so that we get major, minor, point
    # comparisons that default to "0" if not given.  I.e. "1.2" --> (1, 2, 0)
    parts = (parts + 3 * ['0'])[:3]
    return tuple(int(x) for x in parts)


def sqlalchemy_version(op, val):
    sa_ver = parse_version(sqlalchemy.__version__)
    target_ver = parse_version(val)

    assert op in ('<', '>', '<=', '>=', '=='), 'op {} not supported'.format(op)

    if op == '<':
        return sa_ver < target_ver
    if op == '>':
        return sa_ver > target_ver
    if op == '<=':
        return sa_ver <= target_ver
    if op == '>=':
        return sa_ver >= target_ver
    return sa_ver == target_ver


def engine_config_warning(config, version, deprecated_config_key, engine_option):
    if config[deprecated_config_key] is not None:
        warnings.warn(
            'The `{}` config option is deprecated and will be removed in'
            ' v{}.  Use `SQLALCHEMY_ENGINE_OPTIONS[\'{}\']` instead.'
            .format(deprecated_config_key, version, engine_option),
            DeprecationWarning
        )
