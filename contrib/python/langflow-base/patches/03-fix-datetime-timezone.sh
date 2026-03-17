#!/bin/bash
# Fix datetime fields to use DateTime(timezone=True) for asyncpg compatibility
# This ensures proper timezone handling with PostgreSQL asyncpg driver

# Add import to all models to simplify patch
find langflow/services/database/models -name "model.py" -exec sed -i '1i from sqlalchemy import DateTime, Column' {} \;

# Find all model.py files in the database models directory and apply datetime field fixes
find langflow/services/database/models -name "model.py" -exec sed -i \
    -e 's/: datetime = Field(default_factory=lambda: datetime\.now(timezone\.utc))/: datetime = Field(\n        default_factory=lambda: datetime.now(timezone.utc),\n        sa_column=Column(DateTime(timezone=True), nullable=False),\n    )/g' \
    -e 's/last_login_at: datetime | None = Field(default=None, nullable=True)/last_login_at: datetime | None = Field(\n        default=None,\n        sa_column=Column(DateTime(timezone=True), nullable=True),\n    )/g' \
    -e 's/last_used_at: datetime | None = Field(default=None, nullable=True)/last_used_at: datetime | None = Field(\n        default=None,\n        sa_column=Column(DateTime(timezone=True), nullable=True),\n    )/g' \
    -e 's/updated_at: datetime | None = Field(default_factory=lambda: datetime\.now(timezone\.utc), nullable=True)/updated_at: datetime | None = Field(\n        default_factory=lambda: datetime.now(timezone.utc),\n        sa_column=Column(DateTime(timezone=True), nullable=True),\n    )/g' \
    {} \;
