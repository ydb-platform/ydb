"""
Migrate yoyo's internal table structure
"""

from datetime import datetime
from datetime import timezone

from . import v1
from . import v2


#: Mapping of {schema version number: module}
schema_versions = {0: None, 1: v1, 2: v2}


#: First schema version that supports the yoyo_versions table
USE_VERSION_TABLE_FROM = 2


def needs_upgrading(backend):
    return get_current_version(backend) < max(schema_versions)


def upgrade(backend, version=None):
    """
    Check the currently installed yoyo migrations version and update the
    internal schema
    """
    if version is None:
        desired_version = max(schema_versions)
    else:
        desired_version = version
    current_version = get_current_version(backend)
    with backend.transaction():
        while current_version < desired_version:
            next_version = current_version + 1
            schema_versions[next_version].upgrade(backend)  # type: ignore
            current_version = next_version
            mark_schema_version(backend, current_version)


def get_current_version(backend):
    """
    Return the currently installed yoyo migrations schema version
    """
    tables = set(backend.list_tables())
    version_table = backend.version_table
    if backend.migration_table not in tables:
        return 0
    if version_table not in tables:
        return 1
    qi = backend.quote_identifier
    cursor = backend.execute(
        f"SELECT max({qi('version')}) FROM {qi(version_table)}"
    )
    version = cursor.fetchone()[0]
    assert version in schema_versions
    return version


def mark_schema_version(backend, version):
    """
    Mark the given version as having been applied
    """
    assert version in schema_versions
    if version < USE_VERSION_TABLE_FROM:
        return
    backend.execute(
        f"INSERT INTO {backend.version_table_quoted} VALUES (:version, :when)",
        {"version": version, "when": datetime.now(timezone.utc).replace(tzinfo=None)},
    )
