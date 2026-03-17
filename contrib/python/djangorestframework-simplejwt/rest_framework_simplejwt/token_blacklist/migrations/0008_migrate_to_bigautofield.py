from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("token_blacklist", "0007_auto_20171017_2214"),
    ]

    operations = [
        migrations.AlterField(
            model_name="blacklistedtoken",
            name="id",
            field=models.BigAutoField(
                auto_created=True, primary_key=True, serialize=False, verbose_name="ID"
            ),
        ),
        migrations.AlterField(
            model_name="outstandingtoken",
            name="id",
            field=models.BigAutoField(
                auto_created=True, primary_key=True, serialize=False, verbose_name="ID"
            ),
        ),
    ]
