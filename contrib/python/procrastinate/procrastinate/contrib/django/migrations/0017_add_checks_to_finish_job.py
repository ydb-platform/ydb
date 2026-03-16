from __future__ import annotations

from django.db import migrations

from .. import migrations_utils


class Migration(migrations.Migration):
    operations = [
        migrations_utils.RunProcrastinateSQL(
            name="00.17.00_03_add_checks_to_finish_job.sql"
        )
    ]
    name = "0017_add_checks_to_finish_job"
    dependencies = [("procrastinate", "0016_delete_finished_jobs")]
