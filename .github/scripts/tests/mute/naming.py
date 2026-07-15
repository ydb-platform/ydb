"""Map ``muted_ya`` / ``to_mute`` line format to ``tests_monitor.full_name``."""


def mute_file_line_to_tests_monitor_full_name(line: str) -> str:
    """Convert ``suite_folder test_name`` to monitor ``suite_folder/test_name``.

    Wildcard patterns are returned unchanged (callers usually skip them for YDB key match).
    Lines without a separating space are returned unchanged (already ``full_name`` or opaque).
    """
    if not line:
        return line
    if '*' in line or '?' in line:
        return line
    if ' ' not in line:
        return line
    suite_folder, test_name = line.rsplit(' ', 1)
    return f'{suite_folder}/{test_name}'
