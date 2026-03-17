from __future__ import annotations

from django.db import migrations

from .. import migrations_utils


class Migration(migrations.Migration):
    operations = [
        migrations_utils.RunProcrastinateSQL(
            name="00.17.00_01_add_trigger_on_job_deletion.sql"
        )
    ]
    name = "0015_add_trigger_on_job_deletion"
    dependencies = [("procrastinate", "0014_add_finish_job_and_retry_job_functions")]
