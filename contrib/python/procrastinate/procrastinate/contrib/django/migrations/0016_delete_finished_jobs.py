from __future__ import annotations

from django.db import migrations

from .. import migrations_utils


class Migration(migrations.Migration):
    operations = [
        migrations_utils.RunProcrastinateSQL(
            name="00.17.00_02_delete_finished_jobs.sql"
        )
    ]
    name = "0016_delete_finished_jobs"
    dependencies = [("procrastinate", "0015_add_trigger_on_job_deletion")]
