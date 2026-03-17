import os

from wasabi import msg


class ENV_VARS:
    CONFIG_OVERRIDES = "WEASEL_CONFIG_OVERRIDES"


def check_spacy_env_vars():
    if "SPACY_CONFIG_OVERRIDES" in os.environ:
        msg.warn(
            "You've set a `SPACY_CONFIG_OVERRIDES` environment variable, "
            "which is now deprecated. Weasel will not use it. "
            "You can use `WEASEL_CONFIG_OVERRIDES` instead."
        )
    if "SPACY_PROJECT_USE_GIT_VERSION" in os.environ:
        msg.warn(
            "You've set a `SPACY_PROJECT_USE_GIT_VERSION` environment variable, "
            "which is now deprecated. Weasel will not use it."
        )


def check_bool_env_var(env_var: str) -> bool:
    """Convert the value of an environment variable to a boolean. Add special
    check for "0" (falsy) and consider everything else truthy, except unset.

    env_var (str): The name of the environment variable to check.
    RETURNS (bool): Its boolean value.
    """
    value = os.environ.get(env_var, False)
    if value == "0":
        return False
    return bool(value)
