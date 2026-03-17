from __future__ import annotations

from django.db import migrations

from .. import migrations_utils


class Migration(migrations.Migration):
    operations = [
        migrations_utils.RunProcrastinateSQL(
            name="00.05.00_02_drop_started_at_column.sql"
        )
    ]
    name = "0002_drop_started_at_column"
    dependencies = [("procrastinate", "0001_initial")]
