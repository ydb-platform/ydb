import difflib
import json
from collections import OrderedDict

import cyson


def _to_text(data):
    return data.decode('utf-8', 'backslashreplace')


def _require_bytes(name, value):
    if not isinstance(value, bytes):
        raise TypeError('%s must be bytes, got %s' % (name, type(value).__name__))


def _sort_key(value):
    if isinstance(value, bytes):
        return value.decode('utf-8', 'backslashreplace')
    return value


def normalize_json_value(value):
    if isinstance(value, dict):
        return OrderedDict(
            (key, normalize_json_value(value[key]))
            for key in sorted(value.keys(), key=_sort_key)
        )
    if isinstance(value, list):
        return [normalize_json_value(item) for item in value]
    return value


def normalize_json_text(text):
    data = json.loads(text)
    normalized = normalize_json_value(data)
    return json.dumps(normalized, sort_keys=True, ensure_ascii=False, indent=4)


def normalize_yson_value(value):
    if isinstance(value, list):
        return [normalize_yson_value(item) for item in value]
    if isinstance(value, dict):
        normalized = OrderedDict()
        for key in sorted(value.keys(), key=_sort_key):
            item = value[key]
            if key == b'_other':
                normalized[normalize_yson_value(key)] = sorted(
                    normalize_yson_value(item),
                    key=cyson.dumps,
                )
            else:
                normalized[normalize_yson_value(key)] = normalize_yson_value(item)
        return normalized
    return value


def normalize_yson_table(content):
    payload = content.strip()
    if not payload:
        return ''
    if not payload.startswith(b'['):
        payload = b'[' + payload
        if not payload.endswith(b']'):
            payload = payload + b']'
    rows = normalize_yson_value(cyson.loads(payload))
    return _to_text(cyson.dumps(rows, format='pretty'))


def normalize_yson_document(content):
    value = normalize_yson_value(cyson.loads(content))
    return _to_text(cyson.dumps(value, format='pretty'))


def normalize_file_content(content):
    _require_bytes('content', content)

    stripped = content.strip()
    if not stripped:
        return ''

    # JSON only for strict UTF-8 text.
    try:
        text = stripped.decode('utf-8')
    except UnicodeDecodeError:
        text = None
    if text is not None and text[0] in '{[':
        try:
            return normalize_json_text(text)
        except json.JSONDecodeError:
            pass

    try:
        if stripped.lstrip().startswith(b'{') and b'};' in stripped:
            return normalize_yson_table(stripped)
    except (ValueError, TypeError, UnicodeError):
        pass

    try:
        return normalize_yson_document(stripped)
    except (ValueError, TypeError, UnicodeError):
        pass

    return _to_text(stripped)


def compare_contents(left, right, fromfile='left', tofile='right'):
    """Compare two file payloads after YSON/JSON normalization.

    Returns an iterator of unified-diff lines if contents differ, otherwise None.
    ``left`` / ``right`` must be ``bytes``.
    """
    _require_bytes('left', left)
    _require_bytes('right', right)
    left_lines = normalize_file_content(left).splitlines(keepends=True)
    right_lines = normalize_file_content(right).splitlines(keepends=True)
    if left_lines == right_lines:
        return None
    return difflib.unified_diff(
        left_lines,
        right_lines,
        fromfile=fromfile,
        tofile=tofile,
    )
