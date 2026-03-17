from __future__ import annotations

from django.db import migrations

from .. import migrations_utils


class Migration(migrations.Migration):
    operations = [
        migrations_utils.RunProcrastinateSQL(
            name="00.10.00_02_add_defer_job_function.sql"
        )
    ]
    name = "0009_add_defer_job_function"
    dependencies = [("procrastinate", "0008_close_fetch_job_race_condition")]
