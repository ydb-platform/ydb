from __future__ import annotations

from django.db import migrations

from .. import migrations_utils


class Migration(migrations.Migration):
    operations = [
        migrations_utils.RunProcrastinateSQL(name="00.23.00_01_null_locks_excluded.sql")
    ]
    name = "0022_null_locks_excluded"
    dependencies = [("procrastinate", "0021_add_kwargs_to_defer_periodic_job")]
