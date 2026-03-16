from __future__ import annotations

from django.db import migrations, models

from .. import migrations_utils


class Migration(migrations.Migration):
    operations = [
        migrations_utils.RunProcrastinateSQL(name="02.06.00_01_add_cancel_states.sql"),
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
                    ("aborting", "aborting"),
                    ("aborted", "aborted"),
                ],
                max_length=32,
            ),
        ),
        migrations.AlterField(
            "procrastinateevent",
            "type",
            models.CharField(
                choices=[
                    ("deferred", "deferred"),
                    ("started", "started"),
                    ("deferred_for_retry", "deferred_for_retry"),
                    ("failed", "failed"),
                    ("succeeded", "succeeded"),
                    ("cancelled", "cancelled"),
                    ("abort_requested", "abort_requested"),
                    ("aborted", "aborted"),
                    ("scheduled", "scheduled"),
                ],
                max_length=32,
            ),
        ),
    ]
    name = "0028_add_cancel_states"
    dependencies = [("procrastinate", "0027_add_periodic_job_priority")]
