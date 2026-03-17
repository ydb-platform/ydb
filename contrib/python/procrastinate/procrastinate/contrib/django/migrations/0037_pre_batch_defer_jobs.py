from __future__ import annotations

from django.db import migrations

from .. import migrations_utils


class Migration(migrations.Migration):
    operations = [
        migrations_utils.RunProcrastinateSQL(
            name="03.02.00_01_pre_batch_defer_jobs.sql"
        ),
    ]
    name = "0037_pre_batch_defer_jobs"
    dependencies = [("procrastinate", "0036_add_worker_model")]
