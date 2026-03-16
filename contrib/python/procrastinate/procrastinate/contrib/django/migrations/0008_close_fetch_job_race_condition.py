from __future__ import annotations

from django.db import migrations

from .. import migrations_utils


class Migration(migrations.Migration):
    operations = [
        migrations_utils.RunProcrastinateSQL(
            name="00.10.00_01_close_fetch_job_race_condition.sql"
        )
    ]
    name = "0008_close_fetch_job_race_condition"
    dependencies = [("procrastinate", "0007_add_queueing_lock_column")]
