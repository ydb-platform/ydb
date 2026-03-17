from __future__ import annotations

from django.db import migrations

from .. import migrations_utils


class Migration(migrations.Migration):
    initial = True
    operations = [migrations_utils.RunProcrastinateSQL(name="00.00.00_01_initial.sql")]
    name = "0001_initial"
