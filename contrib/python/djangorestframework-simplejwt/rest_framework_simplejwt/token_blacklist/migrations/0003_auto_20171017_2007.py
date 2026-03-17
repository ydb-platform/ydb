from uuid import UUID

from django.db import migrations


def populate_jti_hex(apps, schema_editor):
    OutstandingToken = apps.get_model("token_blacklist", "OutstandingToken")

    db_alias = schema_editor.connection.alias
    for token in OutstandingToken.objects.using(db_alias).all():
        token.jti_hex = token.jti.hex
        token.save()


def reverse_populate_jti_hex(apps, schema_editor):  # pragma: no cover
    OutstandingToken = apps.get_model("token_blacklist", "OutstandingToken")

    db_alias = schema_editor.connection.alias
    for token in OutstandingToken.objects.using(db_alias).all():
        token.jti = UUID(hex=token.jti_hex)
        token.save()


class Migration(migrations.Migration):
    dependencies = [
        ("token_blacklist", "0002_outstandingtoken_jti_hex"),
    ]

    operations = [
        migrations.RunPython(populate_jti_hex, reverse_populate_jti_hex),
    ]
