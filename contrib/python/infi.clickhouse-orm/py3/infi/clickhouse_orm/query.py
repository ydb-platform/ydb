from __future__ import unicode_literals

import pytz
from copy import copy, deepcopy
from math import ceil
from datetime import date, datetime
from .utils import comma_join, string_or_func, arg_to_sql


# TODO
# - check that field names are valid

class Operator(object):
    """
    Base class for filtering operators.
    """

    def to_sql(self, model_cls, field_name, value):
        """
        Subclasses should implement this method. It returns an SQL string
        that applies this operator on the given field and value.
        """
        raise NotImplementedError   # pragma: no cover

    def _value_to_sql(self, field, value, quote=True):
        from infi.clickhouse_orm.funcs import F
        if isinstance(value, F):
            return value.to_sql()
        return field.to_db_string(field.to_python(value, pytz.utc), quote)


class SimpleOperator(Operator):
    """
    A simple binary operator such as a=b, a<b, a>b etc.
    """

    def __init__(self, sql_operator, sql_for_null=None):
        self._sql_operator = sql_operator
        self._sql_for_null = sql_for_null

    def to_sql(self, model_cls, field_name, value):
        field = getattr(model_cls, field_name)
        value = self._value_to_sql(field, value)
        if value == '\\N' and self._sql_for_null is not None:
            return ' '.join([field_name, self._sql_for_null])
        return ' '.join([field_name, self._sql_operator, value])


class InOperator(Operator):
    """
    An operator that implements IN.
    Accepts 3 different types of values:
    - a list or tuple of simple values
    - a string (used verbatim as the contents of the parenthesis)
    - a queryset (subquery)
    """

    def to_sql(self, model_cls, field_name, value):
        field = getattr(model_cls, field_name)
        if isinstance(value, QuerySet):
            value = value.as_sql()
        elif isinstance(value, str):
            pass
        else:
            value = comma_join([self._value_to_sql(field, v) for v in value])
        return '%s IN (%s)' % (field_name, value)


class LikeOperator(Operator):
    """
    A LIKE operator that matches the field to a given pattern. Can be
    case sensitive or insensitive.
    """

    def __init__(self, pattern, case_sensitive=True):
        self._pattern = pattern
        self._case_sensitive = case_sensitive

    def to_sql(self, model_cls, field_name, value):
        field = getattr(model_cls, field_name)
        value = self._value_to_sql(field, value, quote=False)
        value = value.replace('\\', '\\\\').replace('%', '\\\\%').replace('_', '\\\\_')
        pattern = self._pattern.format(value)
        if self._case_sensitive:
            return '%s LIKE \'%s\'' % (field_name, pattern)
        else:
            return 'lowerUTF8(%s) LIKE lowerUTF8(\'%s\')' % (field_name, pattern)


class IExactOperator(Operator):
    """
    An operator for case insensitive string comparison.
    """

    def to_sql(self, model_cls, field_name, value):
        field = getattr(model_cls, field_name)
        value = self._value_to_sql(field, value)
        return 'lowerUTF8(%s) = lowerUTF8(%s)' % (field_name, value)


class NotOperator(Operator):
    """
    A wrapper around another operator, which negates it.
    """

    def __init__(self, base_operator):
        self._base_operator = base_operator

    def to_sql(self, model_cls, field_name, value):
        # Negate the base operator
        return 'NOT (%s)' % self._base_operator.to_sql(model_cls, field_name, value)


class BetweenOperator(Operator):
    """
    An operator that implements BETWEEN.
    Accepts list or tuple of two elements and generates sql condition:
    - 'BETWEEN value[0] AND value[1]' if value[0] and value[1] are not None and not empty
    Then imitations of BETWEEN, where one of two limits is missing
    - '>= value[0]' if value[1] is None or empty
    - '<= value[1]' if value[0] is None or empty
    """

    def to_sql(self, model_cls, field_name, value):
        field = getattr(model_cls, field_name)
        value0 = self._value_to_sql(field, value[0]) if value[0] is not None or len(str(value[0])) > 0 else None
        value1 = self._value_to_sql(field, value[1]) if value[1] is not None or len(str(value[1])) > 0 else None
        if value0 and value1:
            return '%s BETWEEN %s AND %s' % (field_name, value0, value1)
        if value0 and not value1:
            return ' '.join([field_name, '>=', value0])
        if value1 and not value0:
            return ' '.join([field_name, '<=', value1])

# Define the set of builtin operators

_operators = {}

def register_operator(name, sql):
    _operators[name] = sql

register_operator('eq',          SimpleOperator('=', 'IS NULL'))
register_operator('ne',          SimpleOperator('!=', 'IS NOT NULL'))
register_operator('gt',          SimpleOperator('>'))
register_operator('gte',         SimpleOperator('>='))
register_operator('lt',          SimpleOperator('<'))
register_operator('lte',         SimpleOperator('<='))
register_operator('between',     BetweenOperator())
register_operator('in',          InOperator())
register_operator('not_in',      NotOperator(InOperator()))
register_operator('contains',    LikeOperator('%{}%'))
register_operator('startswith',  LikeOperator('{}%'))
register_operator('endswith',    LikeOperator('%{}'))
register_operator('icontains',   LikeOperator('%{}%', False))
register_operator('istartswith', LikeOperator('{}%', False))
register_operator('iendswith',   LikeOperator('%{}', False))
register_operator('iexact',      IExactOperator())


class Cond(object):
    """
    An abstract object for storing a single query condition Field + Operator + Value.
    """

    def to_sql(self, model_cls):
        raise NotImplementedError


class FieldCond(Cond):
    """
    A single query condition made up of Field + Operator + Value.
    """
    def __init__(self, field_name, operator, value):
        self._field_name = field_name
        self._operator = _operators.get(operator)
        if self._operator is None:
            # The field name contains __ like my__field
            self._field_name = field_name + '__' + operator
            self._operator = _operators['eq']
        self._value = value

    def to_sql(self, model_cls):
        return self._operator.to_sql(model_cls, self._field_name, self._value)

    def __deepcopy__(self, memodict={}):
        res = copy(self)
        res._value = deepcopy(self._value)
        return res


class Q(object):

    AND_MODE = 'AND'
    OR_MODE = 'OR'

    def __init__(self, *filter_funcs, **filter_fields):
        self._conds = list(filter_funcs) + [self._build_cond(k, v) for k, v in filter_fields.items()]
        self._children = []
        self._negate = False
        self._mode = self.AND_MODE

    @property
    def is_empty(self):
        """
        Checks if there are any conditions in Q object
        Returns: Boolean
        """
        return not bool(self._conds or self._children)

    @classmethod
    def _construct_from(cls, l_child, r_child, mode):
        if mode == l_child._mode and not l_child._negate:
            q = deepcopy(l_child)
            q._children.append(deepcopy(r_child))
        elif mode == r_child._mode and not r_child._negate:
            q = deepcopy(r_child)
            q._children.append(deepcopy(l_child))
        else:
            # Different modes
            q = Q()
            q._children = [l_child, r_child]
            q._mode = mode  # AND/OR

        return q

    def _build_cond(self, key, value):
        if '__' in key:
            field_name, operator = key.rsplit('__', 1)
        else:
            field_name, operator = key, 'eq'
        return FieldCond(field_name, operator, value)

    def to_sql(self, model_cls):
        condition_sql = []

        if self._conds:
            condition_sql.extend([cond.to_sql(model_cls) for cond in self._conds])

        if self._children:
            condition_sql.extend([child.to_sql(model_cls) for child in self._children if child])

        if not condition_sql:
            # Empty Q() object returns everything
            sql = '1'
        elif len(condition_sql) == 1:
            # Skip not needed brackets over single condition
            sql = condition_sql[0]
        else:
            # Each condition must be enclosed in brackets, or order of operations may be wrong
            sql = '(%s)' % ') {} ('.format(self._mode).join(condition_sql)

        if self._negate:
            sql = 'NOT (%s)' % sql

        return sql

    def __or__(self, other):
        return Q._construct_from(self, other, self.OR_MODE)

    def __and__(self, other):
        return Q._construct_from(self, other, self.AND_MODE)

    def __invert__(self):
        q = copy(self)
        q._negate = True
        return q

    def __bool__(self):
        return not self.is_empty

    def __deepcopy__(self, memodict={}):
        q = Q()
        q._conds = [deepcopy(cond) for cond in self._conds]
        q._negate = self._negate
        q._mode = self._mode

        if self._children:
            q._children = [deepcopy(child) for child in self._children]

        return q


class QuerySet(object):
    """
    A queryset is an object that represents a database query using a specific `Model`.
    It is lazy, meaning that it does not hit the database until you iterate over its
    matching rows (model instances).
    """

    def __init__(self, model_cls, database):
        """
        Initializer. It is possible to create a queryset like this, but the standard
        way is to use `MyModel.objects_in(database)`.
        """
        self.model = model_cls
        self._model_cls = model_cls
        self._database = database
        self._order_by = []
        self._where_q = Q()
        self._prewhere_q = Q()
        self._grouping_fields = []
        self._grouping_with_totals = False
        self._fields = model_cls.fields().keys()
        self._limits = None
        self._limit_by = None
        self._limit_by_fields = None
        self._distinct = False
        self._final = False

    def __iter__(self):
        """
        Iterates over the model instances matching this queryset
        """
        return self._database.select(self.as_sql(), self._model_cls)

    def __bool__(self):
        """
        Returns true if this queryset matches any rows.
        """
        return bool(self.count())

    def __nonzero__(self):      # Python 2 compatibility
        return type(self).__bool__(self)

    def __str__(self):
        return self.as_sql()

    def __getitem__(self, s):
        if isinstance(s, int):
            # Single index
            assert s >= 0, 'negative indexes are not supported'
            qs = copy(self)
            qs._limits = (s, 1)
            return next(iter(qs))
        else:
            # Slice
            assert s.step in (None, 1), 'step is not supported in slices'
            start = s.start or 0
            stop = s.stop or 2**63 - 1
            assert start >= 0 and stop >= 0, 'negative indexes are not supported'
            assert start <= stop, 'start of slice cannot be smaller than its end'
            qs = copy(self)
            qs._limits = (start, stop - start)
            return qs

    def limit_by(self, offset_limit, *fields_or_expr):
        """
        Adds a LIMIT BY clause to the query.
        - `offset_limit`: either an integer specifying the limit, or a tuple of integers (offset, limit).
        - `fields_or_expr`: the field names or expressions to use in the clause.
        """
        if isinstance(offset_limit, int):
            # Single limit
            offset_limit = (0, offset_limit)
        offset = offset_limit[0]
        limit = offset_limit[1]
        assert offset >= 0 and limit >= 0, 'negative limits are not supported'
        qs = copy(self)
        qs._limit_by = (offset, limit)
        qs._limit_by_fields = fields_or_expr
        return qs

    def select_fields_as_sql(self):
        """
        Returns the selected fields or expressions as a SQL string.
        """
        fields = '*'
        if self._fields:
            fields = comma_join('`%s`' % field for field in self._fields)
        return fields

    def as_sql(self):
        """
        Returns the whole query as a SQL string.
        """
        distinct = 'DISTINCT ' if self._distinct else ''
        final = ' FINAL' if self._final else ''
        table_name = '`%s`' % self._model_cls.table_name()
        if self._model_cls.is_system_model():
            table_name = '`system`.' + table_name
        params = (distinct, self.select_fields_as_sql(), table_name, final)
        sql = u'SELECT %s%s\nFROM %s%s' % params

        if self._prewhere_q and not self._prewhere_q.is_empty:
            sql += '\nPREWHERE ' + self.conditions_as_sql(prewhere=True)

        if self._where_q and not self._where_q.is_empty:
            sql += '\nWHERE ' + self.conditions_as_sql(prewhere=False)

        if self._grouping_fields:
            sql += '\nGROUP BY %s' % comma_join('`%s`' % field for field in self._grouping_fields)

            if self._grouping_with_totals:
                sql += ' WITH TOTALS'

        if self._order_by:
            sql += '\nORDER BY ' + self.order_by_as_sql()

        if self._limit_by:
            sql += '\nLIMIT %d, %d' % self._limit_by
            sql += ' BY %s' % comma_join(string_or_func(field) for field in self._limit_by_fields)

        if self._limits:
            sql += '\nLIMIT %d, %d' % self._limits

        return sql

    def order_by_as_sql(self):
        """
        Returns the contents of the query's `ORDER BY` clause as a string.
        """
        return comma_join([
            '%s DESC' % field[1:] if isinstance(field, str) and field[0] == '-' else str(field)
            for field in self._order_by
        ])

    def conditions_as_sql(self, prewhere=False):
        """
        Returns the contents of the query's `WHERE` or `PREWHERE` clause as a string.
        """
        q_object = self._prewhere_q if prewhere else self._where_q
        return q_object.to_sql(self._model_cls)

    def count(self):
        """
        Returns the number of matching model instances.
        """
        if self._distinct or self._limits:
            # Use a subquery, since a simple count won't be accurate
            sql = u'SELECT count() FROM (%s)' % self.as_sql()
            raw = self._database.raw(sql)
            return int(raw) if raw else 0

        # Simple case
        conditions = (self._where_q & self._prewhere_q).to_sql(self._model_cls)
        return self._database.count(self._model_cls, conditions)

    def order_by(self, *field_names):
        """
        Returns a copy of this queryset with the ordering changed.
        """
        qs = copy(self)
        qs._order_by = field_names
        return qs

    def only(self, *field_names):
        """
        Returns a copy of this queryset limited to the specified field names.
        Useful when there are large fields that are not needed,
        or for creating a subquery to use with an IN operator.
        """
        qs = copy(self)
        qs._fields = field_names
        return qs

    def _filter_or_exclude(self, *q, **kwargs):
        from .funcs import F

        inverse = kwargs.pop('_inverse', False)
        prewhere = kwargs.pop('prewhere', False)

        qs = copy(self)

        condition = Q()
        for arg in q:
            if isinstance(arg, Q):
                condition &= arg
            elif isinstance(arg, F):
                condition &= Q(arg)
            else:
                raise TypeError('Invalid argument "%r" to queryset filter' % arg)

        if kwargs:
            condition &= Q(**kwargs)

        if inverse:
            condition = ~condition

        condition = copy(self._prewhere_q if prewhere else self._where_q) & condition
        if prewhere:
            qs._prewhere_q = condition
        else:
            qs._where_q = condition

        return qs

    def filter(self, *q, **kwargs):
        """
        Returns a copy of this queryset that includes only rows matching the conditions.
        Pass `prewhere=True` to apply the conditions as PREWHERE instead of WHERE.
        """
        return self._filter_or_exclude(*q, **kwargs)

    def exclude(self, *q, **kwargs):
        """
        Returns a copy of this queryset that excludes all rows matching the conditions.
        Pass `prewhere=True` to apply the conditions as PREWHERE instead of WHERE.
        """
        return self._filter_or_exclude(*q, _inverse=True, **kwargs)

    def paginate(self, page_num=1, page_size=100):
        """
        Returns a single page of model instances that match the queryset.
        Note that `order_by` should be used first, to ensure a correct
        partitioning of records into pages.

        - `page_num`: the page number (1-based), or -1 to get the last page.
        - `page_size`: number of records to return per page.

        The result is a namedtuple containing `objects` (list), `number_of_objects`,
        `pages_total`, `number` (of the current page), and `page_size`.
        """
        from .database import Page
        count = self.count()
        pages_total = int(ceil(count / float(page_size)))
        if page_num == -1:
            page_num = pages_total
        elif page_num < 1:
            raise ValueError('Invalid page number: %d' % page_num)
        offset = (page_num - 1) * page_size
        return Page(
            objects=list(self[offset : offset + page_size]),
            number_of_objects=count,
            pages_total=pages_total,
            number=page_num,
            page_size=page_size
        )

    def distinct(self):
        """
        Adds a DISTINCT clause to the query, meaning that any duplicate rows
        in the results will be omitted.
        """
        qs = copy(self)
        qs._distinct = True
        return qs

    def final(self):
        """
        Adds a FINAL modifier to table, meaning data will be collapsed to final version.
        Can be used with the `CollapsingMergeTree` and `ReplacingMergeTree` engines only.
        """
        from .engines import CollapsingMergeTree, ReplacingMergeTree
        if not isinstance(self._model_cls.engine, (CollapsingMergeTree, ReplacingMergeTree)):
            raise TypeError('final() method can be used only with the CollapsingMergeTree and ReplacingMergeTree engines')

        qs = copy(self)
        qs._final = True
        return qs

    def delete(self):
        """
        Deletes all records matched by this queryset's conditions.
        Note that ClickHouse performs deletions in the background, so they are not immediate.
        """
        self._verify_mutation_allowed()
        conditions = (self._where_q & self._prewhere_q).to_sql(self._model_cls)
        sql = 'ALTER TABLE $db.`%s` DELETE WHERE %s' % (self._model_cls.table_name(), conditions)
        self._database.raw(sql)
        return self

    def update(self, **kwargs):
        """
        Updates all records matched by this queryset's conditions.
        Keyword arguments specify the field names and expressions to use for the update.
        Note that ClickHouse performs updates in the background, so they are not immediate.
        """
        assert kwargs, 'No fields specified for update'
        self._verify_mutation_allowed()
        fields = comma_join('`%s` = %s' % (name, arg_to_sql(expr)) for name, expr in kwargs.items())
        conditions = (self._where_q & self._prewhere_q).to_sql(self._model_cls)
        sql = 'ALTER TABLE $db.`%s` UPDATE %s WHERE %s' % (self._model_cls.table_name(), fields, conditions)
        self._database.raw(sql)
        return self

    def _verify_mutation_allowed(self):
        '''
        Checks that the queryset's state allows mutations. Raises an AssertionError if not.
        '''
        assert not self._limits, 'Mutations are not allowed after slicing the queryset'
        assert not self._limit_by, 'Mutations are not allowed after calling limit_by(...)'
        assert not self._distinct, 'Mutations are not allowed after calling distinct()'
        assert not self._final, 'Mutations are not allowed after calling final()'

    def aggregate(self, *args, **kwargs):
        """
        Returns an `AggregateQuerySet` over this query, with `args` serving as
        grouping fields and `kwargs` serving as calculated fields. At least one
        calculated field is required. For example:
        ```
            Event.objects_in(database).filter(date__gt='2017-08-01').aggregate('event_type', count='count()')
        ```
        is equivalent to:
        ```
            SELECT event_type, count() AS count FROM event
            WHERE data > '2017-08-01'
            GROUP BY event_type
        ```
        """
        return AggregateQuerySet(self, args, kwargs)


class AggregateQuerySet(QuerySet):
    """
    A queryset used for aggregation.
    """

    def __init__(self, base_qs, grouping_fields, calculated_fields):
        """
        Initializer. Normally you should not call this but rather use `QuerySet.aggregate()`.

        The grouping fields should be a list/tuple of field names from the model. For example:
        ```
            ('event_type', 'event_subtype')
        ```
        The calculated fields should be a mapping from name to a ClickHouse aggregation function. For example:
        ```
            {'weekday': 'toDayOfWeek(event_date)', 'number_of_events': 'count()'}
        ```
        At least one calculated field is required.
        """
        super(AggregateQuerySet, self).__init__(base_qs._model_cls, base_qs._database)
        assert calculated_fields, 'No calculated fields specified for aggregation'
        self._fields = grouping_fields
        self._grouping_fields = grouping_fields
        self._calculated_fields = calculated_fields
        self._order_by = list(base_qs._order_by)
        self._where_q = base_qs._where_q
        self._prewhere_q = base_qs._prewhere_q
        self._limits = base_qs._limits
        self._distinct = base_qs._distinct

    def group_by(self, *args):
        """
        This method lets you specify the grouping fields explicitly. The `args` must
        be names of grouping fields or calculated fields that this queryset was
        created with.
        """
        for name in args:
            assert name in self._fields or name in self._calculated_fields, \
                   'Cannot group by `%s` since it is not included in the query' % name
        qs = copy(self)
        qs._grouping_fields = args
        return qs

    def only(self, *field_names):
        """
        This method is not supported on `AggregateQuerySet`.
        """
        raise NotImplementedError('Cannot use "only" with AggregateQuerySet')

    def aggregate(self, *args, **kwargs):
        """
        This method is not supported on `AggregateQuerySet`.
        """
        raise NotImplementedError('Cannot re-aggregate an AggregateQuerySet')

    def select_fields_as_sql(self):
        """
        Returns the selected fields or expressions as a SQL string.
        """
        return comma_join([str(f) for f in self._fields] + ['%s AS %s' % (v, k) for k, v in self._calculated_fields.items()])

    def __iter__(self):
        return self._database.select(self.as_sql()) # using an ad-hoc model

    def count(self):
        """
        Returns the number of rows after aggregation.
        """
        sql = u'SELECT count() FROM (%s)' % self.as_sql()
        raw = self._database.raw(sql)
        return int(raw) if raw else 0

    def with_totals(self):
        """
        Adds WITH TOTALS modifier ot GROUP BY, making query return extra row
        with aggregate function calculated across all the rows. More information:
        https://clickhouse.tech/docs/en/query_language/select/#with-totals-modifier
        """
        qs = copy(self)
        qs._grouping_with_totals = True
        return qs

    def _verify_mutation_allowed(self):
        raise AssertionError('Cannot mutate an AggregateQuerySet')


# Expose only relevant classes in import *
__all__ = [c.__name__ for c in [Q, QuerySet, AggregateQuerySet]]
