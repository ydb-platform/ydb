from __future__ import annotations

from django.db import migrations

from .. import migrations_utils


class Migration(migrations.Migration):
    operations = [
        migrations_utils.RunProcrastinateSQL(name="03.01.00_50_post_add_heartbeat.sql"),
    ]
    name = "0035_post_add_heartbeat"
    dependencies = [("procrastinate", "0034_pre_add_heartbeat")]
