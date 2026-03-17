import os


def is_custom_code_eval_enabled() -> bool:
    """
    Check if custom code evaluation is explicitly allowed.
    """
    return os.getenv("LANGFLOW_ALLOW_CUSTOM_CODE", "true").lower() == "true"
