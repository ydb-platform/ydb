from __future__ import annotations

from django.db import migrations

from .. import migrations_utils


class Migration(migrations.Migration):
    operations = [
        migrations_utils.RunProcrastinateSQL(
            name="02.14.01_01_add_indexes_for_fetch_job.sql"
        )
    ]
    name = "0031_add_indexes_for_fetch_job"
    dependencies = [("procrastinate", "0030_alter_procrastinateevent_options")]
