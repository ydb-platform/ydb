from __future__ import annotations

from django.db import migrations

from .. import migrations_utils


class Migration(migrations.Migration):
    operations = [
        migrations_utils.RunProcrastinateSQL(
            name="00.22.00_01_add_kwargs_to_defer_periodic_job.sql"
        )
    ]
    name = "0021_add_kwargs_to_defer_periodic_job"
    dependencies = [("procrastinate", "0020_add_index_on_procrastinate_jobs")]
