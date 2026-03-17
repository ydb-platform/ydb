try:
    from ._validator_c import *  # noqa: F401,F403
except ImportError:
    from ._validator_py import *  # noqa: F401,F403
