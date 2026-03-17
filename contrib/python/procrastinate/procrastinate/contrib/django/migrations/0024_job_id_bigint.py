from __future__ import annotations

from django.db import migrations

from .. import migrations_utils


class Migration(migrations.Migration):
    operations = [
        migrations_utils.RunProcrastinateSQL(name="01.01.01_01_job_id_bigint.sql")
    ]
    name = "0024_job_id_bigint"
    dependencies = [("procrastinate", "0023_remove_old_finish_job_function")]
