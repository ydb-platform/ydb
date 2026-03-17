from __future__ import annotations

from django.db import migrations

from .. import migrations_utils


class Migration(migrations.Migration):
    operations = [
        migrations_utils.RunProcrastinateSQL(
            name="03.04.00_01_pre_add_retry_failed_job_procedure.sql"
        ),
    ]
    name = "0040_retry_failed_job"
    dependencies = [
        ("procrastinate", "0039_pre_priority_lock_fetch_job"),
    ]
