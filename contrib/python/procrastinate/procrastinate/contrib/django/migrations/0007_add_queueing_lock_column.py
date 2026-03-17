from __future__ import annotations

from django.db import migrations

from .. import migrations_utils


class Migration(migrations.Migration):
    operations = [
        migrations_utils.RunProcrastinateSQL(
            name="00.08.01_01_add_queueing_lock_column.sql"
        )
    ]
    name = "0007_add_queueing_lock_column"
    dependencies = [("procrastinate", "0006_fix_trigger_status_events_insert")]
