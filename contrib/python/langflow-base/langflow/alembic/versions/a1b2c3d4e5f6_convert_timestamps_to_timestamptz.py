"""convert timestamps to timestamptz for asyncpg compatibility

Revision ID: a1b2c3d4e5f6
Revises: 182e5471b900
Create Date: 2026-01-15 17:11:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.engine.reflection import Inspector

revision: str = "a1b2c3d4e5f6"
down_revision: str | None = "182e5471b900"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

TIMESTAMP_COLUMNS = [
    ("user", "create_at"),
    ("user", "updated_at"),
    ("user", "last_login_at"),
    ("flow", "updated_at"),
    ("message", "timestamp"),
    ("transaction", "timestamp"),
    ("vertex_build", "timestamp"),
    ("file", "created_at"),
    ("file", "updated_at"),
    ("apikey", "last_used_at"),
]


def upgrade() -> None:
    conn = op.get_bind()
    if conn.dialect.name != "postgresql":
        return

    inspector = Inspector.from_engine(conn)
    table_names = inspector.get_table_names()

    for table_name, column_name in TIMESTAMP_COLUMNS:
        if table_name not in table_names:
            continue
        columns = {col["name"]: col for col in inspector.get_columns(table_name)}
        if column_name not in columns:
            continue
        col_type = columns[column_name]["type"]
        if hasattr(col_type, "timezone") and col_type.timezone:
            continue
        op.execute(
            f'ALTER TABLE "{table_name}" ALTER COLUMN "{column_name}" '
            f"TYPE TIMESTAMP WITH TIME ZONE USING {column_name} AT TIME ZONE 'UTC'"
        )


def downgrade() -> None:
    conn = op.get_bind()
    if conn.dialect.name != "postgresql":
        return

    inspector = Inspector.from_engine(conn)
    table_names = inspector.get_table_names()

    for table_name, column_name in TIMESTAMP_COLUMNS:
        if table_name not in table_names:
            continue
        columns = {col["name"]: col for col in inspector.get_columns(table_name)}
        if column_name not in columns:
            continue
        op.execute(
            f'ALTER TABLE "{table_name}" ALTER COLUMN "{column_name}" '
            f"TYPE TIMESTAMP WITHOUT TIME ZONE"
        )
