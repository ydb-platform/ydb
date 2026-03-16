from __future__ import annotations

from django.db import migrations

from .. import migrations_utils


class Migration(migrations.Migration):
    operations = [
        migrations_utils.RunProcrastinateSQL(
            name="00.18.01_01_fix_finish_job_compat_issue.sql"
        )
    ]
    name = "0019_fix_finish_job_compat_issue"
    dependencies = [("procrastinate", "0018_add_checks_to_retry_job")]
