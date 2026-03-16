from __future__ import annotations

from django.db import migrations, models

from .. import migrations_utils


class Migration(migrations.Migration):
    operations = [
        migrations_utils.RunProcrastinateSQL(
            name="03.00.00_01_pre_cancel_notification.sql"
        ),
        migrations.AddField(
            "procrastinatejob",
            "abort_requested",
            models.BooleanField(),
        ),
    ]
    name = "0032_pre_cancel_notification"
    dependencies = [("procrastinate", "0031_add_indexes_for_fetch_job")]
