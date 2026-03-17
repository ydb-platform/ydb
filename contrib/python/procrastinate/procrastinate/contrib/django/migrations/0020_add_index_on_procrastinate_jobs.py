from __future__ import annotations

from django.db import migrations

from .. import migrations_utils


class Migration(migrations.Migration):
    operations = [
        migrations_utils.RunProcrastinateSQL(
            name="00.19.00_01_add_index_on_procrastinate_jobs.sql"
        )
    ]
    name = "0020_add_index_on_procrastinate_jobs"
    dependencies = [("procrastinate", "0019_fix_finish_job_compat_issue")]
