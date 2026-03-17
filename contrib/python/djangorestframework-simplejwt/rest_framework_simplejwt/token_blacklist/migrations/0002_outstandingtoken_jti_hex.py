from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("token_blacklist", "0001_initial"),
    ]

    operations = [
        migrations.AddField(
            model_name="outstandingtoken",
            name="jti_hex",
            field=models.CharField(blank=True, null=True, max_length=255),
        ),
    ]
