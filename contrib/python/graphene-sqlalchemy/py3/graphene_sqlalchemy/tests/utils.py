import re


def to_std_dicts(value):
    """Convert nested ordered dicts to normal dicts for better comparison."""
    if isinstance(value, dict):
        return {k: to_std_dicts(v) for k, v in value.items()}
    elif isinstance(value, list):
        return [to_std_dicts(v) for v in value]
    else:
        return value


def remove_cache_miss_stat(message):
    """Remove the stat from the echoed query message when the cache is missed for sqlalchemy version >= 1.4"""
    # https://github.com/sqlalchemy/sqlalchemy/blob/990eb3d8813369d3b8a7776ae85fb33627443d30/lib/sqlalchemy/engine/default.py#L1177
    return re.sub(r"\[generated in \d+.?\d*s\]\s", "", message)
