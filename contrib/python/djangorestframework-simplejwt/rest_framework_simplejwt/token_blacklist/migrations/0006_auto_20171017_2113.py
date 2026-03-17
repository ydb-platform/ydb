from django.db import migrations


class Migration(migrations.Migration):
    dependencies = [
        ("token_blacklist", "0005_remove_outstandingtoken_jti"),
    ]

    operations = [
        migrations.RenameField(
            model_name="outstandingtoken",
            old_name="jti_hex",
            new_name="jti",
        ),
    ]
