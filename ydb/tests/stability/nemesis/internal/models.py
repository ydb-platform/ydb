"""
Data models and validation helpers for Nemesis App.

This module provides documentation of expected data structures and validation
helper functions for manual request validation in Flask endpoints.
"""


# Data structure documentation (for reference)
# These are not used for validation anymore, but document the expected structures

class ProcessInfo:
    """Process information structure"""

    def __init__(self, id: int, type: str, command: str, logs: str, ret_code: int, status: str):
        self.id = id
        self.type = type
        self.command = command
        self.logs = logs
        self.ret_code = ret_code
        self.status = status


class ProcessType:
    """Process type structure"""

    def __init__(self, name: str, command: str):
        self.name = name
        self.command = command


# Validation helper functions

def validate_create_process_request(data: dict) -> tuple:
    """
    Validate create process request data.

    Args:
        data: Request data dictionary

    Returns:
        tuple: (is_valid, error_message, validated_data)
    """
    if not data:
        return False, "No data provided", None

    process_type = data.get("type")
    if not process_type:
        return False, "Missing type field", None

    action = data.get("action", "inject")

    validated_data = {
        "type": process_type,
        "action": action
    }

    return True, None, validated_data


def validate_set_schedule_request(data: dict) -> tuple:
    """
    Validate set schedule request data.

    Args:
        data: Request data dictionary

    Returns:
        tuple: (is_valid, error_message, validated_data)
    """
    if not data:
        return False, "No data provided", None

    process_type = data.get("type")
    if not process_type:
        return False, "Missing type field", None

    enabled = data.get("enabled")
    if enabled is None:
        return False, "Missing enabled field", None

    if not isinstance(enabled, bool):
        return False, "enabled must be a boolean", None

    interval = data.get("interval")
    if interval is not None and not isinstance(interval, int):
        return False, "interval must be an integer", None

    validated_data = {
        "type": process_type,
        "enabled": enabled,
        "interval": interval
    }

    return True, None, validated_data


def validate_create_host_process_request(data: dict) -> tuple:
    """
    Validate create host process request data.

    Args:
        data: Request data dictionary

    Returns:
        tuple: (is_valid, error_message, validated_data)
    """
    if not data:
        return False, "No data provided", None

    host = data.get("host")
    if not host:
        return False, "Missing host field", None

    process_type = data.get("type")
    if not process_type:
        return False, "Missing type field", None

    action = data.get("action", "inject")

    validated_data = {
        "host": host,
        "type": process_type,
        "action": action
    }

    return True, None, validated_data
