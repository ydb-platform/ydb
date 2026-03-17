from __future__ import annotations

from django.db import migrations

from .. import migrations_utils


class Migration(migrations.Migration):
    operations = [
        migrations_utils.RunProcrastinateSQL(
            name="00.15.02_01_fix_procrastinate_defer_periodic_job.sql"
        )
    ]
    name = "0013_fix_procrastinate_defer_periodic_job"
    dependencies = [("procrastinate", "0012_add_locks_to_periodic_defer")]
