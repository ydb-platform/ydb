from __future__ import annotations

from django.db import migrations

from .. import migrations_utils


class Migration(migrations.Migration):
    operations = [
        migrations_utils.RunProcrastinateSQL(
            name="00.05.00_01_drop_started_at_column.sql"
        )
    ]
    name = "0003_drop_started_at_column"
    dependencies = [("procrastinate", "0002_drop_started_at_column")]
