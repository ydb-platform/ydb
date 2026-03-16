from sqlalchemy.sql.base import _generative
from sqlalchemy.sql.selectable import (
    Select as StandardSelect,
    Join as StandardJoin,
)

from ..ext.clauses import (
    ArrayJoin,
    LimitByClause,
    sample_clause,
)


__all__ = ('Select', 'select')


class Join(StandardJoin):

    def __init__(self, left, right,
                 onclause=None, isouter=False, full=False,
                 type=None, strictness=None, distribution=None):
        if type is not None:
            type = type.upper()
        super(Join, self).__init__(left, right, onclause,
                                   isouter=isouter, full=full)
        self.strictness = None
        if strictness:
            self.strictness = strictness
        self.distribution = distribution
        self.type = type


class Select(StandardSelect):
    _with_totals = False
    _final_clause = None
    _sample_clause = None
    _limit_by_clause = None
    _array_join = None

    @_generative
    def with_totals(self):
        self._with_totals = True

    @_generative
    def final(self):
        self._final_clause = True

    @_generative
    def sample(self, sample):
        self._sample_clause = sample_clause(sample)

    @_generative
    def limit_by(self, by_clauses, limit, offset=None):
        self._limit_by_clause = LimitByClause(by_clauses, limit, offset)

    @_generative
    def array_join(self, *columns):
        self._array_join = ArrayJoin(*columns)

    def join(self, right, onclause=None, isouter=False, full=False, type=None,
             strictness=None, distribution=None):
        return Join(self, right,
                    onclause=onclause, type=type,
                    isouter=isouter, full=full,
                    strictness=strictness, distribution=distribution)


select = Select
join = Join
