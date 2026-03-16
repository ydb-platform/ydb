from __future__ import annotations

from django.db import migrations

from .. import migrations_utils


class Migration(migrations.Migration):
    operations = [
        migrations_utils.RunProcrastinateSQL(
            name="00.05.00_03_drop_procrastinate_version_table.sql"
        )
    ]
    name = "0004_drop_procrastinate_version_table"
    dependencies = [("procrastinate", "0003_drop_started_at_column")]
