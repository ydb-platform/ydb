from __future__ import annotations

from django.db import migrations, models

from .. import migrations_utils


class Migration(migrations.Migration):
    operations = [
        migrations_utils.RunProcrastinateSQL(
            name="03.00.00_50_post_cancel_notification.sql"
        ),
        migrations.AlterField(
            "procrastinatejob",
            "status",
            models.CharField(
                choices=[
                    ("todo", "todo"),
                    ("doing", "doing"),
                    ("succeeded", "succeeded"),
                    ("failed", "failed"),
                    ("cancelled", "cancelled"),
                    ("aborted", "aborted"),
                ],
                max_length=32,
            ),
        ),
    ]
    name = "0033_post_cancel_notification"
    dependencies = [("procrastinate", "0032_pre_cancel_notification")]
