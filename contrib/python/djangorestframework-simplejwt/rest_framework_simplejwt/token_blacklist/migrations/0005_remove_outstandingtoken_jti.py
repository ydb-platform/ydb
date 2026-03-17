from django.db import migrations


class Migration(migrations.Migration):
    dependencies = [
        ("token_blacklist", "0004_auto_20171017_2013"),
    ]

    operations = [
        migrations.RemoveField(
            model_name="outstandingtoken",
            name="jti",
        ),
    ]
