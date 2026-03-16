from __future__ import annotations

from django.db import migrations

from .. import migrations_utils


class Migration(migrations.Migration):
    operations = [
        migrations_utils.RunProcrastinateSQL(
            name="00.07.01_01_fix_trigger_status_events_insert.sql"
        )
    ]
    name = "0006_fix_trigger_status_events_insert"
    dependencies = [("procrastinate", "0005_fix_procrastinate_fetch_job")]
