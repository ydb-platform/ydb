from __future__ import annotations

from django.db import migrations, models

from .. import migrations_utils


class Migration(migrations.Migration):
    operations = [
        migrations_utils.RunProcrastinateSQL(name="03.01.00_01_pre_add_heartbeat.sql"),
        migrations.AddField(
            "procrastinatejob",
            "heartbeat_updated_at",
            models.DateTimeField(blank=True, null=True),
        ),
    ]
    name = "0034_pre_add_heartbeat"
    dependencies = [("procrastinate", "0033_post_cancel_notification")]
