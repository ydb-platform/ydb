from __future__ import annotations

from django.db import migrations

from .. import migrations_utils


class Migration(migrations.Migration):
    operations = [
        migrations_utils.RunProcrastinateSQL(
            name="00.14.00_01_add_locks_to_periodic_defer.sql"
        )
    ]
    name = "0012_add_locks_to_periodic_defer"
    dependencies = [("procrastinate", "0011_add_foreign_key_index")]
