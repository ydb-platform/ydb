from __future__ import annotations

from django.db import migrations

from .. import migrations_utils


class Migration(migrations.Migration):
    operations = [
        migrations_utils.RunProcrastinateSQL(
            name="01.00.00_01_remove_old_finish_job_function.sql"
        )
    ]
    name = "0023_remove_old_finish_job_function"
    dependencies = [("procrastinate", "0022_null_locks_excluded")]
