"""Conservative platform selection for optional registry archives.

Keep uncertain cases: downloading an extra archive is preferable to making an
otherwise valid offline install impossible. Constraint matching follows pnpm
12.18 package-is-installable/check_platform.rs.
"""


def package_supports_platform(metadata, platform, supported_architectures=None):
    if not platform:
        return True
    # pnpm also supports named platform tags with ABI/version floors. Do not
    # approximate that grammar here; retain all archives for that form.
    if supported_architectures is not None and not isinstance(supported_architectures, dict):
        return True
    supported_architectures = supported_architectures or {}
    for axis, current in zip(("os", "cpu", "libc"), platform):
        wanted = metadata.get(axis)
        supported = supported_architectures.get(axis)
        if wanted is None or current == "unknown":
            continue
        if not _string_list(wanted) or supported is not None and not _string_list(supported):
            return True
        if wanted == ["any"]:
            continue
        values = [current] if supported is None else supported
        matched = False
        for value in values:
            value = current if value == "current" else value
            for entry in wanted:
                if entry == "!" + value:
                    return False
                if entry == value:
                    matched = True
                    break
        if not matched and not all(entry.startswith("!") for entry in wanted):
            return False
    return True


def _string_list(value):
    return isinstance(value, list) and all(isinstance(item, str) for item in value)
