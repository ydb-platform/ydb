from __future__ import annotations

from django.db import migrations

from .. import migrations_utils


class Migration(migrations.Migration):
    operations = [
        migrations_utils.RunProcrastinateSQL(
            name="00.17.00_04_add_checks_to_retry_job.sql"
        )
    ]
    name = "0018_add_checks_to_retry_job"
    dependencies = [("procrastinate", "0017_add_checks_to_finish_job")]
