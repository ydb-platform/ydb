from __future__ import annotations

from django.db import migrations

from .. import migrations_utils


class Migration(migrations.Migration):
    operations = [
        migrations_utils.RunProcrastinateSQL(
            name="03.03.00_01_pre_priority_lock_fetch_job.sql"
        ),
    ]
    name = "0039_pre_priority_lock_fetch_job"
    dependencies = [
        ("procrastinate", "0038_post_batch_defer_jobs"),
    ]
