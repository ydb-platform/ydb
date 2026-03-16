# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import migrations


def forwards(apps, schema_editor):
    User = apps.get_model('accounts', 'User')

    if 'auth_user' in schema_editor.connection.introspection.table_names():
        db_columns = [f.get_attname_column()[1] for f in User._meta.fields]
        sql_for_copy_rows = (
            'INSERT INTO accounts_user ({0}) '
            'SELECT {0} FROM auth_user '
            'WHERE id > 0'.format(', '.join(db_columns))
        )
        cursor = schema_editor.connection.cursor()
        cursor.execute(sql_for_copy_rows)

    if User.objects.count() > 0:
        Team = apps.get_model('accounts', 'Team')
        Member = apps.get_model('accounts', 'TeamMember')

        team = Team.objects.create(
            name='Default', description='Auto created during migration')
        for user in User.objects.all():
            Member.objects.create(
                team=team, user=user,
                role='owner' if user.is_superuser else 'developer')


class Migration(migrations.Migration):

    dependencies = [
        ('accounts', '0001_initial'),
    ]

    operations = [
        migrations.RunPython(code=forwards),
    ]
