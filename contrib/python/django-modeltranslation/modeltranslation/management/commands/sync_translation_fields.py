# -*- coding: utf-8 -*-
"""
Detect new translatable fields in all models and sync database structure.

You will need to execute this command in two cases:

    1. When you add new languages to settings.LANGUAGES.
    2. When you add new translatable fields to your models.

Credits: Heavily inspired by django-transmeta's sync_transmeta_db command.
"""
from django import VERSION
from django.core.management.base import BaseCommand
from django.core.management.color import no_style
from django.db import connection
from six import moves

from modeltranslation.settings import AVAILABLE_LANGUAGES
from modeltranslation.translator import translator
from modeltranslation.utils import build_localized_fieldname


def ask_for_confirmation(sql_sentences, model_full_name, interactive):
    print('\nSQL to synchronize "%s" schema:' % model_full_name)
    for sentence in sql_sentences:
        print('   %s' % sentence)
    while True:
        prompt = '\nAre you sure that you want to execute the previous SQL: (y/n) [n]: '
        if interactive:
            answer = moves.input(prompt).strip()
        else:
            answer = 'y'
        if answer == '':
            return False
        elif answer not in ('y', 'n', 'yes', 'no'):
            print('Please answer yes or no')
        elif answer == 'y' or answer == 'yes':
            return True
        else:
            return False


def print_missing_langs(missing_langs, field_name, model_name):
    print(
        'Missing languages in "%s" field from "%s" model: %s'
        % (field_name, model_name, ", ".join(missing_langs))
    )


class Command(BaseCommand):
    help = (
        'Detect new translatable fields or new available languages and'
        ' sync database structure. Does not remove columns of removed'
        ' languages or undeclared fields.'
    )

    if VERSION < (1, 8):
        from optparse import make_option

        option_list = BaseCommand.option_list + (
            make_option(
                '--noinput',
                action='store_false',
                dest='interactive',
                default=True,
                help='Do NOT prompt the user for input of any kind.',
            ),
        )
    else:

        def add_arguments(self, parser):
            parser.add_argument(
                '--noinput',
                action='store_false',
                dest='interactive',
                default=True,
                help='Do NOT prompt the user for input of any kind.',
            ),

    def handle(self, *args, **options):
        """
        Command execution.
        """
        self.cursor = connection.cursor()
        self.introspection = connection.introspection
        self.interactive = options['interactive']

        found_missing_fields = False
        models = translator.get_registered_models(abstract=False)
        for model in models:
            db_table = model._meta.db_table
            model_name = model._meta.model_name
            model_full_name = '%s.%s' % (model._meta.app_label, model_name)
            opts = translator.get_options_for_model(model)
            for field_name, fields in opts.local_fields.items():
                # Take `db_column` attribute into account
                try:
                    field = list(fields)[0]
                except IndexError:
                    # Ignore IndexError for ProxyModel
                    # maybe there is better way to handle this
                    continue
                column_name = field.db_column if field.db_column else field_name
                missing_langs = list(self.get_missing_languages(column_name, db_table))
                if missing_langs:
                    found_missing_fields = True
                    print_missing_langs(missing_langs, field_name, model_full_name)
                    sql_sentences = self.get_sync_sql(field_name, missing_langs, model)
                    execute_sql = ask_for_confirmation(
                        sql_sentences, model_full_name, self.interactive
                    )
                    if execute_sql:
                        print('Executing SQL...')
                        for sentence in sql_sentences:
                            self.cursor.execute(sentence)
                        print('Done')
                    else:
                        print('SQL not executed')

        if not found_missing_fields:
            print('No new translatable fields detected')

    def get_table_fields(self, db_table):
        """
        Gets table fields from schema.
        """
        db_table_desc = self.introspection.get_table_description(self.cursor, db_table)
        return [t[0] for t in db_table_desc]

    def get_missing_languages(self, field_name, db_table):
        """
        Gets only missings fields.
        """
        db_table_fields = self.get_table_fields(db_table)
        for lang_code in AVAILABLE_LANGUAGES:
            if build_localized_fieldname(field_name, lang_code) not in db_table_fields:
                yield lang_code

    def get_sync_sql(self, field_name, missing_langs, model):
        """
        Returns SQL needed for sync schema for a new translatable field.
        """
        qn = connection.ops.quote_name
        style = no_style()
        sql_output = []
        db_table = model._meta.db_table
        for lang in missing_langs:
            new_field = build_localized_fieldname(field_name, lang)
            f = model._meta.get_field(new_field)
            col_type = f.db_type(connection=connection)
            field_sql = [style.SQL_FIELD(qn(f.column)), style.SQL_COLTYPE(col_type)]
            # column creation
            stmt = "ALTER TABLE %s ADD COLUMN %s" % (qn(db_table), ' '.join(field_sql))
            if not f.null:
                stmt += " " + style.SQL_KEYWORD('NOT NULL')
            sql_output.append(stmt + ";")
        return sql_output
