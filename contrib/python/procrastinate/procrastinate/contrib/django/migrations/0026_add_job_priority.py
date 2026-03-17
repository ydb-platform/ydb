from __future__ import annotations

from django.db import migrations, models

from .. import migrations_utils


class Migration(migrations.Migration):
    operations = [
        migrations_utils.RunProcrastinateSQL(name="02.00.03_01_add_job_priority.sql"),
        migrations.AddField("procrastinatejob", "priority", models.IntegerField()),
    ]
    name = "0026_add_job_priority"
    dependencies = [("procrastinate", "0025_add_models")]
