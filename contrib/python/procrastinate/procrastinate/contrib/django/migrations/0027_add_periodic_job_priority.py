from __future__ import annotations

from django.db import migrations

from .. import migrations_utils


class Migration(migrations.Migration):
    operations = [
        migrations_utils.RunProcrastinateSQL(
            name="02.05.00_01_add_periodic_job_priority.sql"
        ),
    ]
    name = "0027_add_periodic_job_priority"
    dependencies = [("procrastinate", "0026_add_job_priority")]
