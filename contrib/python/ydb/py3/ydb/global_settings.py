import warnings

from . import convert
from . import table


def global_allow_truncated_result(enabled: bool = True):
    if convert._default_allow_truncated_result == enabled:
        return

    if enabled:
        warnings.warn("Global allow truncated response is deprecated behaviour.")

    convert._default_allow_truncated_result = enabled


def global_allow_split_transactions(enabled: bool):
    if table._default_allow_split_transaction == enabled:
        return

    if enabled:
        warnings.warn("Global allow split transaction is deprecated behaviour.")

    table._default_allow_split_transaction = enabled
