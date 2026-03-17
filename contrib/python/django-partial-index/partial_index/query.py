"""Django Q object to SQL string conversion."""
from django.db.models import expressions, Q, F
from django.db.models.sql import Query


class Vendor(object):
    POSTGRESQL = 'postgresql'
    SQLITE = 'sqlite'


class PQ(Q):
    """Compatibility class for Q-objects.

    Django 2.0 Q-objects are suitable on their own, but Django 1.11 needs a better deep equality comparison and a deconstruct() method.

    PartialIndex definitions in model classes should use PQ to avoid problems when upgrading projects.
    """

    def __eq__(self, other):
        """Copied from Django 2.0 django.utils.tree.Node.__eq__()"""
        if self.__class__ != other.__class__:
            return False
        if (self.connector, self.negated) == (other.connector, other.negated):
            return self.children == other.children
        return False

    def deconstruct(self):
        """Copied from Django 2.0 django.db.models.query_utils.Q.deconstruct()"""
        path = '%s.%s' % (self.__class__.__module__, self.__class__.__name__)
        # Keep imports clean in migrations
        if path.startswith('partial_index.query.'):
            path = path.replace('partial_index.query.', 'partial_index.')

        args, kwargs = (), {}
        if len(self.children) == 1 and not isinstance(self.children[0], Q):
            child = self.children[0]
            kwargs = {child[0]: child[1]}
        else:
            args = tuple(self.children)
            if self.connector != self.default:
                kwargs = {'_connector': self.connector}
        if self.negated:
            kwargs['_negated'] = True
        return path, args, kwargs


class PF(F):
    """Compatibility class for F-expressions.

    Django 2.0 F-expressions are suitable on their own, but Django 1.11 a deconstruct() method.

    PartialIndex definitions in model classes should use PF to avoid problems when upgrading projects.
    """

    def __eq__(self, other):
        if self.__class__ != other.__class__:
            return False
        return self.name == other.name

    def deconstruct(self):
        path = '%s.%s' % (self.__class__.__module__, self.__class__.__name__)
        # Keep imports clean in migrations
        if path.startswith('partial_index.query.'):
            path = path.replace('partial_index.query.', 'partial_index.')

        args = (self.name, )
        kwargs = {}
        return path, args, kwargs



def get_valid_vendor(schema_editor):
    vendor = schema_editor.connection.vendor
    if vendor not in [Vendor.POSTGRESQL, Vendor.SQLITE]:
        raise ValueError('Database vendor %s is not supported by django-partial-index.' % vendor)
    return vendor


def q_to_sql(q, model, schema_editor):
    # Q -> SQL conversion based on code from Ian Foote's Check Constraints pull request:
    # https://github.com/django/django/pull/7615/

    query = Query(model)
    where = query._add_q(q, used_aliases=set(), allow_joins=False)[0]
    connection = schema_editor.connection
    compiler = connection.ops.compiler('SQLCompiler')(query, connection, 'default')
    sql, params = where.as_sql(compiler, connection)
    params = tuple(map(schema_editor.quote_value, params))
    where_sql = sql % params
    return where_sql


def expression_mentioned_fields(exp):
    if isinstance(exp, expressions.Col):
        field = exp.output_field or exp.field  # TODO: which one makes sense to use here?
        if field and field.name:
            return [field.name]
    elif hasattr(exp, 'get_source_expressions'):
        child_fields = []
        for source in exp.get_source_expressions():
            child_fields.extend(expression_mentioned_fields(source))
        return child_fields
    else:
        raise NotImplementedError('Unexpected expression class %s=%s when looking up mentioned fields.' % (exp.__class__.__name__, exp))


def q_mentioned_fields(q, model):
    """Returns list of field names mentioned in Q object.

    Q(a__isnull=True, b=F('c')) -> ['a', 'b', 'c']
    """
    query = Query(model)
    where = query._add_q(q, used_aliases=set(), allow_joins=False)[0]
    return list(sorted(set(expression_mentioned_fields(where))))
