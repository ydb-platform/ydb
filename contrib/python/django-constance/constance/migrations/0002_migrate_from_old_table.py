from django.core.management.color import no_style
from django.db import migrations, connection, DatabaseError


def _migrate_from_old_table(apps, schema_editor) -> None:
    """
    Copies values from old table.
    On new installations just ignore error that table does not exist.
    """
    try:
        with connection.cursor() as cursor:
            cursor.execute('INSERT INTO constance_constance ( id, key, value ) SELECT id, key, value FROM constance_config', [])
            cursor.execute('DROP TABLE constance_config', [])

    except DatabaseError:
        pass

    Constance = apps.get_model('constance', 'Constance')
    sequence_sql = connection.ops.sequence_reset_sql(no_style(), [Constance])
    with connection.cursor() as cursor:
        for sql in sequence_sql:
            cursor.execute(sql)


class Migration(migrations.Migration):

    dependencies = [('constance', '0001_initial')]

    atomic = False

    operations = [
        migrations.RunPython(_migrate_from_old_table, reverse_code=lambda x, y: None),
    ]
