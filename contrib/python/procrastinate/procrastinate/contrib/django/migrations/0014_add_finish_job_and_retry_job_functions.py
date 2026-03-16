from __future__ import annotations

from django.db import migrations

from .. import migrations_utils


class Migration(migrations.Migration):
    operations = [
        migrations_utils.RunProcrastinateSQL(
            name="00.16.00_01_add_finish_job_and_retry_job_functions.sql"
        )
    ]
    name = "0014_add_finish_job_and_retry_job_functions"
    dependencies = [("procrastinate", "0013_fix_procrastinate_defer_periodic_job")]
