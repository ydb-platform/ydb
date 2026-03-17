"""
This module contains all the classes that are required for storing query signature data.

While looking at the code, the important thing to note here are all the "python magic methods" defined like
__add__, __radd__ etc, which help us in keeping the logic in one place - inside the class itself.
As an example, see "QuerySignatureStatistics __add__(self, other) function, where by defining this function, the object
becomes additive -- which is what it should be.
"""

import json
import operator
import re
import typing
from collections import Counter, OrderedDict, defaultdict
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Callable, Dict, Optional, Tuple

from django.utils.functional import cached_property

RE_STATEMENT_NAME = re.compile(r'(\S+)')


@dataclass(frozen=True)
class StackTraceElement:
    module_name: str
    function_name: str
    line_number: Optional[int] = None  # None happens for django stack-trace

    def __str__(self):
        return f'{self.module_name.replace(".", "/")}.py#{self.line_number} {self.function_name}()'

    @staticmethod
    def django_stacktrace_element(module_name, function_name) -> 'StackTraceElement':
        """
        We are passing None for line_number.  This is important because when we want to compare django stack-traces,
        we want two stack-traces which have the same (module_name, function_name) combination to be same
        """
        return StackTraceElement(module_name=module_name, function_name=function_name)

    @staticmethod
    def app_stacktrace_element(module_name, function_name, line_number) -> 'StackTraceElement':
        return StackTraceElement(module_name=module_name, function_name=function_name, line_number=line_number)


class QuerySignatureAnalyzeResult(Enum):
    UNKNOWN = False
    MISSING_SELECT_RELATED = True
    GET = False
    MISSING_PREFETCH_RELATED = True
    PREFETCHED_RELATED = True
    FILTER = False

    def __init__(self, visible_in_ui):
        self.visible_in_ui = visible_in_ui


@dataclass(frozen=True)
class QuerySignature:
    query_without_params: str
    app_stack_trace: Tuple[StackTraceElement]
    django_stack_trace: Tuple[StackTraceElement]
    target_db: str

    def __str__(self):
        app_stack_trace_str = '\n'.join(map(str, self.app_stack_trace))
        django_stack_trace_str = '\n'.join(map(str, self.django_stack_trace))

        return f'''Query: \n {self.query_without_params}  \n{'##' * 20}\n app-stack-trace: \n {app_stack_trace_str}
                \n{'##' * 20}\n django-stack-trace:\n {django_stack_trace_str} \n{'**' * 80}\n'''

    @cached_property
    def analysis(self) -> QuerySignatureAnalyzeResult:
        from django_query_profiler.query_profiler_storage.django_stack_trace_analyze import code_recommendation
        return code_recommendation(self)

    @property
    def is_fake(self) -> bool:
        """
        A fake query signature would happen when we have not collected any stack-trace, which would happen ONLY in
        one scenario - when the type of profiling is QUERY.
        A Query signature is the grouping unit that help us know when its a N+1 call.  This flag help us know if we
        can, or cannot determine N+1 signature
        """
        return not self.django_stack_trace and not self.app_stack_trace


@dataclass(frozen=True)
class QuerySignatureStatistics:
    frequency: int
    query_execution_time_in_micros: int
    db_row_count: Optional[int]

    def __add__(self, other) -> 'QuerySignatureStatistics':
        """
        Python magic function to make one QuerySignatureStatistics object ability to be added to another
        """
        total_db_row_count = (self.db_row_count + other.db_row_count
                              if self.db_row_count is not None and other.db_row_count is not None else None)
        return QuerySignatureStatistics(
            frequency=self.frequency + other.frequency,
            query_execution_time_in_micros=self.query_execution_time_in_micros + other.query_execution_time_in_micros,
            db_row_count=total_db_row_count)


class SqlStatement(Enum):
    SELECT = ('SELECT',)
    INSERT = ('INSERT',)
    UPDATE = ('UPDATE',)
    DELETE = ('DELETE',)
    TRANSACTIONALS = ('BEGIN', 'END')
    OTHER = ()

    def __init__(self, *statements: Tuple[str]):
        self.statements = statements

    @staticmethod
    def from_sql(query_without_params: str) -> 'SqlStatement':
        sql_statement_str = RE_STATEMENT_NAME.search(query_without_params).groups()[0].upper()
        for sql_statement in SqlStatement:
            if sql_statement_str in sql_statement.statements:
                return sql_statement
        return SqlStatement.OTHER


@dataclass(frozen=True)
class QueryProfiledSummaryData:
    sql_statement_type_counter: typing.Counter[SqlStatement]
    exact_query_duplicates: int
    total_query_execution_time_in_micros: int
    total_db_row_count: Optional[int]
    potential_n_plus1_query_count: Optional[int]

    @cached_property
    def total_query_count(self) -> int:
        return sum(self.sql_statement_type_counter.values())

    def as_dict(self) -> Dict:
        dict_representation: Dict = asdict(self)
        dict_representation.pop('sql_statement_type_counter', None)
        dict_representation.update({sql_statement.name: self.sql_statement_type_counter[sql_statement]
                                    for sql_statement in SqlStatement})
        return dict_representation

    def __str__(self):
        return json.dumps(self.as_dict(), indent=2)


@dataclass(frozen=True)
class QueryProfiledData:
    """
    This is the main class that helps in collecting data as part of query profiling. In a way, all other classes in this
    module are designed to support this class.
    """
    query_signature_to_query_signature_statistics: Dict[QuerySignature, QuerySignatureStatistics] = field(
        default_factory=dict)
    time_spent_profiling_in_micros: int = 0
    _query_params_db_hash_counter: typing.Counter[str] = field(default_factory=Counter)

    @cached_property
    def summary(self) -> QueryProfiledSummaryData:
        exact_query_duplicates = sum(value for value in self._query_params_db_hash_counter.values() if value > 1)

        sql_statement_type_to_count = defaultdict(int)
        total_query_execution_time_in_micros = 0
        total_db_row_count = 0
        potential_n_plus1_query_count = 0
        is_any_query_signature_fake = False
        for query_signature, query_signature_statistics in self.query_signature_to_query_signature_statistics.items():
            sql_statement_str = RE_STATEMENT_NAME.search(query_signature.query_without_params).groups()[0]
            sql_statement_type = SqlStatement.from_sql(sql_statement_str)
            sql_statement_type_to_count[sql_statement_type] += query_signature_statistics.frequency

            total_query_execution_time_in_micros += query_signature_statistics.query_execution_time_in_micros
            total_db_row_count = (total_db_row_count + query_signature_statistics.db_row_count
                                  if query_signature_statistics.db_row_count is not None else None)
            potential_n_plus1_query_count += (
                query_signature_statistics.frequency if query_signature_statistics.frequency > 1 else 0)

            is_any_query_signature_fake |= query_signature.is_fake

        return QueryProfiledSummaryData(
            sql_statement_type_counter=Counter(sql_statement_type_to_count),
            exact_query_duplicates=exact_query_duplicates,
            total_query_execution_time_in_micros=total_query_execution_time_in_micros,
            total_db_row_count=total_db_row_count,
            potential_n_plus1_query_count=None if is_any_query_signature_fake else potential_n_plus1_query_count)

    @cached_property
    def flamegraph_stack(self) -> Dict:
        tree = {
            'name': "<request>",
            'value': 0,
            'children': {}
        }

        def add_child(node, name, count):
            children = node['children']
            current = children.setdefault(name, {
                'name': name,
                'value': 0,
                'children': {}
            })
            current['value'] += count
            return current

        def dedictify(current):
            children = list(current['children'].values())
            current['children'] = children
            for child in children:
                dedictify(child)

        for query_signature, query_signature_statistics in self.query_signature_to_query_signature_statistics.items():
            current = tree
            for stack in reversed(query_signature.app_stack_trace):
                name = "%s:%s" % (stack.module_name, stack.function_name)
                current = add_child(current, name, query_signature_statistics.frequency)
        dedictify(tree)

        if len(tree['children']) == 1:
            tree = tree['children'][0]
        else:
            tree['value'] = sum(node['value'] for node in tree['children'])
        return tree

    def __add__(self, other) -> 'QueryProfiledData':
        """
        This class is not a very obvious choice of additive object.  But, the ability to add instances of this class
        makes sense when we think what this class represent - its the output of profiling query in a context manager
        If we profile query in a nested block, the output of the outer block would be the sum of query profiling
        of all inner block
        """
        combined_query_signature_to_query_signature_statistics = merge_dicts(
            first_dict=self.query_signature_to_query_signature_statistics,
            second_dict=other.query_signature_to_query_signature_statistics,
            op=operator.add)

        combined_query_params_db_hash_counter = self._query_params_db_hash_counter + other._query_params_db_hash_counter
        combined_profiling_time = self.time_spent_profiling_in_micros + other.time_spent_profiling_in_micros
        return QueryProfiledData(
            _query_params_db_hash_counter=combined_query_params_db_hash_counter,
            time_spent_profiling_in_micros=combined_profiling_time,
            query_signature_to_query_signature_statistics=combined_query_signature_to_query_signature_statistics)

    def __radd__(self, other) -> 'QueryProfiledData':
        """
        This is needed for using sum() function to sum up a list of instances.
        See http://www.marinamele.com/2014/04/modifying-add-method-of-python-class.html for more details
        """
        return self if other == 0 else self.__add__(other)


class QueryProfilerLevel(Enum):
    QUERY_SIGNATURE = (500, True)
    QUERY = (0, False)

    def __init__(self, stack_trace_depth: int, normalize_sql: bool):
        self.stack_trace_depth = stack_trace_depth
        self.normalize_sql = normalize_sql

    def __add__(self, other) -> 'QueryProfilerLevel':
        """
        One other example where its not obvious why we would like to add, though in this case we don't have a very good
        reason.
        The reasoning is - adding instances of an enum has to result in an instance of the enum only.  And in our case,
        what we are trying to convey is -- QUERY_SIGNATURE has a higher weightage
        We would use this when we have nested blocks of query profiler context manager with different types, and
        we have to decide what kind of query profiler to use
        """
        if QueryProfilerLevel.QUERY_SIGNATURE in (self, other):
            return QueryProfilerLevel.QUERY_SIGNATURE
        else:
            return QueryProfilerLevel.QUERY

    def __radd__(self, other) -> 'QueryProfilerLevel':
        """
        This is needed for using sum() function to sum up a list of instances.
        See http://www.marinamele.com/2014/04/modifying-add-method-of-python-class.html for more details
        """
        return self if other == 0 else self.__add__(other)


def merge_dicts(first_dict: Dict, second_dict: Dict, op: Callable) -> OrderedDict:
    return OrderedDict(list(first_dict.items()) + list(second_dict.items()) +
                       [(key, op(first_dict[key], second_dict[key])) for key in set(second_dict) & set(first_dict)])
