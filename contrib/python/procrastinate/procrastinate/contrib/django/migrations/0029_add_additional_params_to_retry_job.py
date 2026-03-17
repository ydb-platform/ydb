from __future__ import annotations

from django.db import migrations

from .. import migrations_utils


class Migration(migrations.Migration):
    operations = [
        migrations_utils.RunProcrastinateSQL(
            name="02.08.00_01_add_additional_params_to_retry_job.sql"
        ),
    ]
    name = "0029_add_additional_params_to_retry_job"
    dependencies = [("procrastinate", "0028_add_cancel_states")]
