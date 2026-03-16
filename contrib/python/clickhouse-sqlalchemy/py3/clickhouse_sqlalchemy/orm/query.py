from functools import partial

from sqlalchemy import exc
from sqlalchemy.sql.base import _generative
from sqlalchemy.orm.query import Query as BaseQuery

from ..ext.clauses import (
    ArrayJoin,
    LeftArrayJoin,
    LimitByClause,
    sample_clause,
)


def _compile_state_factory(orig_compile_state_factory, query, statement,
                           *args, **kwargs):
    rv = orig_compile_state_factory(statement, *args, **kwargs)
    new_stmt = rv.statement
    new_stmt._with_cube = query._with_cube
    new_stmt._with_rollup = query._with_rollup
    new_stmt._with_totals = query._with_totals
    new_stmt._final_clause = query._final
    new_stmt._sample_clause = sample_clause(query._sample)
    new_stmt._limit_by_clause = query._limit_by
    new_stmt._array_join = query._array_join
    return rv


class Query(BaseQuery):
    _with_cube = False
    _with_rollup = False
    _with_totals = False
    _final = None
    _sample = None
    _limit_by = None
    _array_join = None

    def _statement_20(self, *args, **kwargs):
        statement = super(Query, self)._statement_20(*args, **kwargs)
        statement._compile_state_factory = partial(
            _compile_state_factory, statement._compile_state_factory, self
        )

        return statement

    @_generative
    def with_cube(self):
        if not self._group_by_clauses:
            raise exc.InvalidRequestError(
                "Query.with_cube() can be used only with specified "
                "GROUP BY, call group_by()"
            )
        if self._with_rollup:
            raise exc.InvalidRequestError(
                "Query.with_cube() and Query.with_rollup() are mutually "
                "exclusive"
            )

        self._with_cube = True

    @_generative
    def with_rollup(self):
        if not self._group_by_clauses:
            raise exc.InvalidRequestError(
                "Query.with_rollup() can be used only with specified "
                "GROUP BY, call group_by()"
            )
        if self._with_cube:
            raise exc.InvalidRequestError(
                "Query.with_cube() and Query.with_rollup() are mutually "
                "exclusive"
            )

        self._with_rollup = True

    @_generative
    def with_totals(self):
        if not self._group_by_clauses:
            raise exc.InvalidRequestError(
                "Query.with_totals() can be used only with specified "
                "GROUP BY, call group_by()"
            )

        self._with_totals = True

    def _add_array_join(self, columns, left):
        join_type = ArrayJoin if not left else LeftArrayJoin
        self._array_join = join_type(*columns)

    @_generative
    def array_join(self, *columns, **kwargs):
        left = kwargs.get("left", False)
        self._add_array_join(columns, left=left)

    @_generative
    def left_array_join(self, *columns):
        self._add_array_join(columns, left=True)

    @_generative
    def final(self):
        self._final = True

    @_generative
    def sample(self, sample):
        self._sample = sample

    @_generative
    def limit_by(self, by_clauses, limit, offset=None):
        self._limit_by = LimitByClause(by_clauses, limit, offset)

    def join(self, *props, **kwargs):
        spec = {
            'type': kwargs.pop('type', None),
            'strictness': kwargs.pop('strictness', None),
            'distribution': kwargs.pop('distribution', None)
        }
        rv = super(Query, self).join(*props, **kwargs)

        x = rv._legacy_setup_joins[-1]
        x_spec = dict(spec)
        # use 'full' key to pass extra flags
        x_spec['full'] = x[-1]['full']
        x[-1]['full'] = tuple(x_spec.items())

        return rv

    def outerjoin(self, *props, **kwargs):
        kwargs['type'] = kwargs.get('type') or 'LEFT OUTER'
        return self.join(*props, **kwargs)
