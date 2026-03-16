from __future__ import annotations

from django.db import migrations, models

import procrastinate.contrib.django.models


class Migration(migrations.Migration):
    operations = [
        migrations.CreateModel(
            name="ProcrastinateWorker",
            fields=[
                ("id", models.AutoField(primary_key=True, serialize=False)),
                ("last_heartbeat", models.DateTimeField()),
            ],
            options={
                "db_table": "procrastinate_workers",
                "managed": False,
            },
            bases=(
                procrastinate.contrib.django.models.ProcrastinateReadOnlyModelMixin,
                models.Model,
            ),
        ),
        migrations.AddField(
            "procrastinatejob",
            "worker",
            models.ForeignKey(
                blank=True,
                null=True,
                on_delete=models.SET_NULL,
                to="procrastinate.procrastinateworker",
            ),
        ),
    ]
    name = "0036_add_worker_model"
    dependencies = [("procrastinate", "0035_post_add_heartbeat")]
