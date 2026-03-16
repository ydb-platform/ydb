from django.db.models import Index, Q
from django.utils import six
from django.utils.encoding import force_bytes
import hashlib
import warnings


from . import query


def validate_where(where='', where_postgresql='', where_sqlite=''):
    if where:
        if where_postgresql or where_sqlite:
            raise ValueError('If providing a common where predicate, must not provide where_postgresql or where_sqlite.')
        if isinstance(where, six.string_types):
            warnings.warn(
                'Text-based where predicates are deprecated, will be removed in a future release. ' +
                'Please upgrade to where=PQ().',
                DeprecationWarning)
        elif isinstance(where, query.PQ):
            pass
        else:
            raise ValueError('Where predicate must be a string or a partial_index.PQ object.')
    else:
        if not where_postgresql and not where_sqlite:
            raise ValueError('A where predicate must be provided.')
        if where_postgresql == where_sqlite:
            raise ValueError('If providing a separate where_postgresql and where_sqlite, then they must be different.' +
                             'If the same expression works for both, just use single where.')
        if not isinstance(where_postgresql, six.string_types) or not isinstance(where_sqlite, six.string_types):
            raise ValueError('where_postgresql and where_sqlite must be strings.')
        warnings.warn(
            'Text-based where predicates are deprecated, will be removed in a future release. ' +
            'Please upgrade to where=PQ().',
            DeprecationWarning)
    return where, where_postgresql, where_sqlite


class PartialIndex(Index):
    suffix = 'partial'
    # Allow an index name longer than 30 characters since this index can only be used on PostgreSQL and SQLite,
    # and the Django default 30 character limit for cross-database compatibility isn't applicable.
    # The "partial" suffix is 4 letters longer than the default "idx".
    max_name_length = 34
    sql_create_index = {
        'postgresql': 'CREATE%(unique)s INDEX %(name)s ON %(table)s%(using)s (%(columns)s)%(extra)s WHERE %(where)s',
        'sqlite': 'CREATE%(unique)s INDEX %(name)s ON %(table)s%(using)s (%(columns)s) WHERE %(where)s',
    }

    # Mutable default fields=[] looks wrong, but it's copied from super class.
    def __init__(self, fields=[], name=None, unique=None, where='', where_postgresql='', where_sqlite=''):
        if unique not in [True, False]:
            raise ValueError('Unique must be True or False')
        self.unique = unique
        self.where, self.where_postgresql, self.where_sqlite = \
            validate_where(where=where, where_postgresql=where_postgresql, where_sqlite=where_sqlite)
        super(PartialIndex, self).__init__(fields=fields, name=name)

    def __repr__(self):
        if self.where:
            if isinstance(self.where, query.PQ):
                anywhere = "where=%s" % repr(self.where)
            else:
                anywhere = "where='%s'" % self.where
        else:
            anywhere = "where_postgresql='%s', where_sqlite='%s'" % (self.where_postgresql, self.where_sqlite)

        return "<%(name)s: fields=%(fields)s, unique=%(unique)s, %(anywhere)s>" % {
            'name': self.__class__.__name__,
            'fields': "'{}'".format(', '.join(self.fields)),
            'unique': self.unique,
            'anywhere': anywhere
        }

    def deconstruct(self):
        path, args, kwargs = super(PartialIndex, self).deconstruct()
        if path.startswith('partial_index.index'):
            path = path.replace('partial_index.index', 'partial_index')
        kwargs['unique'] = self.unique
        if self.where:
            kwargs['where'] = self.where
        else:
            kwargs['where_postgresql'] = self.where_postgresql
            kwargs['where_sqlite'] = self.where_sqlite
        return path, args, kwargs

    def get_sql_create_template_values(self, model, schema_editor, using):
        # This method exists on Django 1.11 Index class, but has been moved to the SchemaEditor on Django 2.0.
        # This makes it complex to call superclass methods and avoid duplicating code.
        # Can be simplified if Django 1.11 support is dropped one day.

        # Copied from Django 1.11 Index.get_sql_create_template_values(), which does not exist in Django 2.0:
        fields = [model._meta.get_field(field_name) for field_name, order in self.fields_orders]
        tablespace_sql = schema_editor._get_index_tablespace_sql(model, fields)
        quote_name = schema_editor.quote_name
        columns = [
            ('%s %s' % (quote_name(field.column), order)).strip()
            for field, (field_name, order) in zip(fields, self.fields_orders)
        ]
        parameters = {
            'table': quote_name(model._meta.db_table),
            'name': quote_name(self.name),
            'columns': ', '.join(columns),
            'using': using,
            'extra': tablespace_sql,
        }

        # PartialIndex updates:
        parameters['unique'] = ' UNIQUE' if self.unique else ''
        # Note: the WHERE predicate is not yet checked for syntax or field names, and is inserted into the CREATE INDEX query unescaped.
        # This is bad for usability, but is not a security risk, as the string cannot come from user input.
        vendor = query.get_valid_vendor(schema_editor)
        if isinstance(self.where, query.PQ):
            parameters['where'] = query.q_to_sql(self.where, model, schema_editor)
        elif vendor == 'postgresql':
            parameters['where'] = self.where_postgresql or self.where
        elif vendor == 'sqlite':
            parameters['where'] = self.where_sqlite or self.where
        else:
            raise ValueError('Should never happen')
        return parameters

    def create_sql(self, model, schema_editor, using=''):
        vendor = query.get_valid_vendor(schema_editor)
        sql_template = self.sql_create_index[vendor]
        sql_parameters = self.get_sql_create_template_values(model, schema_editor, using)
        return sql_template % sql_parameters

    def name_hash_extra_data(self):
        return [str(self.unique), self.where, self.where_postgresql, self.where_sqlite]

    def set_name_with_model(self, model):
        """Sets an unique generated name for the index.

        PartialIndex would like to only override "hash_data = ...", but the entire method must be duplicated for that.
        """
        table_name = model._meta.db_table
        column_names = [model._meta.get_field(field_name).column for field_name, order in self.fields_orders]
        column_names_with_order = [
            (('-%s' if order else '%s') % column_name)
            for column_name, (field_name, order) in zip(column_names, self.fields_orders)
        ]
        # The length of the parts of the name is based on the default max
        # length of 30 characters.
        hash_data = [table_name] + column_names_with_order + [self.suffix] + self.name_hash_extra_data()
        self.name = '%s_%s_%s' % (
            table_name[:11],
            column_names[0][:7],
            '%s_%s' % (self._hash_generator(*hash_data), self.suffix),
        )
        assert len(self.name) <= self.max_name_length, (
            'Index too long for multiple database support. Is self.suffix '
            'longer than 3 characters?'
        )
        self.check_name()

    @staticmethod
    def _hash_generator(*args):
        """Copied from Django 2.1 for compatibility. In Django 2.2 this has been moved into django.db.backends.utils.names_digest().

        Note that even if Django changes the hash calculation in the future, we should not - that would cause index renames on Django version upgrade.
        """
        h = hashlib.md5()
        for arg in args:
            h.update(force_bytes(arg))
        return h.hexdigest()[:6]
