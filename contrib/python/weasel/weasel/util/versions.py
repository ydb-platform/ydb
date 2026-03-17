from typing import Optional

from packaging.specifiers import InvalidSpecifier, SpecifierSet
from packaging.version import InvalidVersion, Version


def is_compatible_version(
    version: str, constraint: str, prereleases: bool = True
) -> Optional[bool]:
    """Check if a version (e.g. "2.0.0") is compatible given a version
    constraint (e.g. ">=1.9.0,<2.2.1"). If the constraint is a specific version,
    it's interpreted as =={version}.

    version (str): The version to check.
    constraint (str): The constraint string.
    prereleases (bool): Whether to allow prereleases. If set to False,
        prerelease versions will be considered incompatible.
    RETURNS (bool / None): Whether the version is compatible, or None if the
        version or constraint are invalid.
    """
    # Handle cases where exact version is provided as constraint
    if constraint[0].isdigit():
        constraint = f"=={constraint}"
    try:
        spec = SpecifierSet(constraint)
        version = Version(version)  # type: ignore[assignment]
    except (InvalidSpecifier, InvalidVersion):
        return None
    spec.prereleases = prereleases
    return version in spec


def get_minor_version(version: str) -> Optional[str]:
    """Get the major + minor version (without patch or prerelease identifiers).

    version (str): The version.
    RETURNS (str): The major + minor version or None if version is invalid.
    """
    try:
        v = Version(version)
    except (TypeError, InvalidVersion):
        return None
    return f"{v.major}.{v.minor}"


def is_minor_version_match(version_a: str, version_b: str) -> bool:
    """Compare two versions and check if they match in major and minor, without
    patch or prerelease identifiers. Used internally for compatibility checks
    that should be insensitive to patch releases.

    version_a (str): The first version
    version_b (str): The second version.
    RETURNS (bool): Whether the versions match.
    """
    a = get_minor_version(version_a)
    b = get_minor_version(version_b)
    return a is not None and b is not None and a == b
