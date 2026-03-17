from __future__ import annotations

from django.db import migrations, models

import procrastinate.contrib.django.models


class Migration(migrations.Migration):
    dependencies = [
        ("procrastinate", "0024_job_id_bigint"),
    ]

    operations = [
        migrations.CreateModel(
            name="ProcrastinateEvent",
            fields=[
                ("id", models.BigAutoField(primary_key=True, serialize=False)),
                (
                    "type",
                    models.CharField(
                        choices=[
                            ("deferred", "deferred"),
                            ("started", "started"),
                            ("deferred_for_retry", "deferred_for_retry"),
                            ("failed", "failed"),
                            ("succeeded", "succeeded"),
                            ("cancelled", "cancelled"),
                            ("scheduled", "scheduled"),
                        ],
                        max_length=32,
                    ),
                ),
                ("at", models.DateTimeField(blank=True, null=True)),
            ],
            options={
                "db_table": "procrastinate_events",
                "managed": False,
            },
            bases=(
                procrastinate.contrib.django.models.ProcrastinateReadOnlyModelMixin,
                models.Model,
            ),
        ),
        migrations.CreateModel(
            name="ProcrastinateJob",
            fields=[
                ("id", models.BigAutoField(primary_key=True, serialize=False)),
                ("queue_name", models.CharField(max_length=128)),
                ("task_name", models.CharField(max_length=128)),
                ("priority", models.IntegerField()),
                ("lock", models.TextField(blank=True, null=True, unique=True)),
                ("args", models.JSONField()),
                (
                    "status",
                    models.CharField(
                        choices=[
                            ("todo", "todo"),
                            ("doing", "doing"),
                            ("succeeded", "succeeded"),
                            ("failed", "failed"),
                        ],
                        max_length=32,
                    ),
                ),
                ("scheduled_at", models.DateTimeField(blank=True, null=True)),
                ("attempts", models.IntegerField()),
                ("queueing_lock", models.TextField(blank=True, null=True, unique=True)),
            ],
            options={
                "db_table": "procrastinate_jobs",
                "managed": False,
            },
            bases=(
                procrastinate.contrib.django.models.ProcrastinateReadOnlyModelMixin,
                models.Model,
            ),
        ),
        migrations.CreateModel(
            name="ProcrastinatePeriodicDefer",
            fields=[
                ("id", models.BigAutoField(primary_key=True, serialize=False)),
                ("task_name", models.CharField(max_length=128)),
                ("defer_timestamp", models.BigIntegerField(blank=True, null=True)),
                ("periodic_id", models.CharField(max_length=128)),
            ],
            options={
                "db_table": "procrastinate_periodic_defers",
                "managed": False,
            },
        ),
    ]
