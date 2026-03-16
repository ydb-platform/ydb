from __future__ import annotations

from django.db import migrations

from .. import migrations_utils


class Migration(migrations.Migration):
    operations = [
        migrations_utils.RunProcrastinateSQL(
            name="03.02.00_50_post_batch_defer_jobs.sql"
        ),
    ]
    name = "0038_post_batch_defer_jobs"
    dependencies = [("procrastinate", "0037_pre_batch_defer_jobs")]
