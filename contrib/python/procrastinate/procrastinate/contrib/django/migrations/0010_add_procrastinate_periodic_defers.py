from __future__ import annotations

from django.db import migrations

from .. import migrations_utils


class Migration(migrations.Migration):
    operations = [
        migrations_utils.RunProcrastinateSQL(
            name="00.11.00_03_add_procrastinate_periodic_defers.sql"
        )
    ]
    name = "0010_add_procrastinate_periodic_defers"
    dependencies = [("procrastinate", "0009_add_defer_job_function")]
