import json
import os
from typing import Optional, Dict, List, Any, Union

_COMMAND_INFO: Optional[Dict[bytes, List[Any]]] = None


def _encode_obj(obj: Any) -> Any:
    if isinstance(obj, str):
        return obj.encode()
    if isinstance(obj, list):
        return [_encode_obj(x) for x in obj]
    if isinstance(obj, dict):
        return {_encode_obj(k): _encode_obj(obj[k]) for k in obj}
    return obj


def _load_command_info() -> None:
    global _COMMAND_INFO
    if _COMMAND_INFO is None:
        with open(os.path.join(os.path.dirname(__file__), "..", "commands.json")) as f:
            _COMMAND_INFO = _encode_obj(json.load(f))


def get_all_commands_info() -> Dict[bytes, List[Any]]:
    _load_command_info()
    return _COMMAND_INFO


def get_command_info(cmd: bytes) -> Optional[List[Any]]:
    _load_command_info()
    if _COMMAND_INFO is None or cmd not in _COMMAND_INFO:
        return None
    return _COMMAND_INFO.get(cmd, None)


def get_categories() -> List[bytes]:
    _load_command_info()
    if _COMMAND_INFO is None:
        return []
    categories = set()
    for info in _COMMAND_INFO.values():
        categories.update(info[6])
    categories = {x[1:] for x in categories}
    return list(categories)


def get_commands_by_category(category: Union[str, bytes]) -> List[bytes]:
    _load_command_info()
    if _COMMAND_INFO is None:
        return []
    if isinstance(category, str):
        category = category.encode()
    if category[0] != b"@":
        category = b"@" + category
    commands = []
    for cmd, info in _COMMAND_INFO.items():
        if category in info[6]:
            commands.append(cmd)
    return commands
