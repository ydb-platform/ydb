from . import convert
from . import table


def global_allow_truncated_result(enabled=True):
    """
    call global_allow_truncated_result(False) for more safe execution and compatible with future changes
    """
    convert._default_allow_truncated_result = enabled


def global_allow_split_transactions(enabled):
    """
    call global_allow_truncated_result(False) for more safe execution and compatible with future changes
    """
    table._allow_split_transaction = enabled
