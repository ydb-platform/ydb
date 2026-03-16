from __future__ import annotations

from django.db import migrations

from .. import migrations_utils


class Migration(migrations.Migration):
    operations = [
        migrations_utils.RunProcrastinateSQL(
            name="00.12.00_01_add_foreign_key_index.sql"
        )
    ]
    name = "0011_add_foreign_key_index"
    dependencies = [("procrastinate", "0010_add_procrastinate_periodic_defers")]
