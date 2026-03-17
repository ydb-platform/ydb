from __future__ import annotations

from django.db import migrations

from .. import migrations_utils


class Migration(migrations.Migration):
    operations = [
        migrations_utils.RunProcrastinateSQL(
            name="00.06.00_01_fix_procrastinate_fetch_job.sql"
        )
    ]
    name = "0005_fix_procrastinate_fetch_job"
    dependencies = [("procrastinate", "0004_drop_procrastinate_version_table")]
