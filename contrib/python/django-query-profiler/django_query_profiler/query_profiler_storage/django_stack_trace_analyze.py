"""
This module contains the function where we would want to "guess" if developer has missed a select_related
or a prefetch_related.  This is more of an experimental thing and this can change with the version of
django.

We rely on stack-traces & query to see if we are able to predict what code changes are required
to convert a N+1 query signature to a non N+1 one OR if its not possible at all
"""

from typing import Callable, Dict, Tuple

from django.db.models.fields.related_descriptors import ForwardManyToOneDescriptor
from django.db.models.query import QuerySet
from mo_sql_parsing import parse

from . import QuerySignature, QuerySignatureAnalyzeResult, SqlStatement, StackTraceElement


def stack_trace_element(func: Callable) -> StackTraceElement:
    return StackTraceElement.django_stacktrace_element(func.__module__, func.__name__)


'''
Django does a very good job of defining different functions for say, when it wants to make a call for a select related
call, and one where it is prefetching some relationship, when the user has used a prefetch_missing/Prefetch.
For others like filter, we came up via observation.
NB: This is not a complete check though.  E.g. in missing prefetch, the stack trace is just one of the check.  Rest of
     the check is in the "code_recommendation" function
'''
MISSING_SELECT_RELATED_STACK_TRACE_ELEMENT = stack_trace_element(ForwardManyToOneDescriptor.get_object)
GET_STACK_TRACE_ELEMENT = stack_trace_element(QuerySet.get)
MISSING_PREFETCH_RELATED_STACK_TRACE_ELEMENT = stack_trace_element(QuerySet._fetch_all)
PREFETCHED_RELATED_STACK_TRACE_ELEMENT = stack_trace_element(QuerySet._prefetch_related_objects)
FILTER_STACK_TRACE_ELEMENTS = {
    stack_trace_element(QuerySet.count),
    stack_trace_element(QuerySet.exists),
    stack_trace_element(QuerySet.first),
    stack_trace_element(QuerySet.last),
    stack_trace_element(QuerySet.__iter__)}


def code_recommendation(query_signature: QuerySignature) -> QuerySignatureAnalyzeResult:

    query_without_params = query_signature.query_without_params
    django_stack_trace = query_signature.django_stack_trace
    sql_statement: SqlStatement = SqlStatement.from_sql(query_without_params)

    if sql_statement != SqlStatement.SELECT or query_signature.is_fake:
        return QuerySignatureAnalyzeResult.UNKNOWN

    if PREFETCHED_RELATED_STACK_TRACE_ELEMENT in django_stack_trace:
        return QuerySignatureAnalyzeResult.PREFETCHED_RELATED

    if MISSING_SELECT_RELATED_STACK_TRACE_ELEMENT in django_stack_trace:
        return QuerySignatureAnalyzeResult.MISSING_SELECT_RELATED

    if GET_STACK_TRACE_ELEMENT in django_stack_trace and 'LIMIT 21' in query_without_params:
        return QuerySignatureAnalyzeResult.GET

    # All easy cases are done now.  Parsing sql now
    try:
        table_names, where_clause_exists, where_equality_key = _parse_sql_for_tables_and_eq(query_without_params)

        # Checking if its missing prefetch related - which is the hardest case
        prefetch_missing_conditions = (
                MISSING_PREFETCH_RELATED_STACK_TRACE_ELEMENT in django_stack_trace
                and table_names
                and where_equality_key
                and '_id' in where_equality_key
                and any(table_name for table_name in table_names if table_name in where_equality_key)
        )
        if prefetch_missing_conditions:
            return QuerySignatureAnalyzeResult.MISSING_PREFETCH_RELATED

        if where_clause_exists and FILTER_STACK_TRACE_ELEMENTS.intersection(django_stack_trace):
            return QuerySignatureAnalyzeResult.FILTER
    except Exception:
        # Don't want to throw exception in case moz parser is not able to parse the sql;  See Issue#21 & Issue#23
        pass

    return QuerySignatureAnalyzeResult.UNKNOWN


def _parse_sql_for_tables_and_eq(query_without_params: str) -> Tuple[list, bool, str]:
    """
    This function parses the sql and returns a tuple of
    1. Table names as list
    2. Does where clause exists
    3. table_name.column_name used in the where equality clause.  Only when equality is the ONLY clause

    NB:  1. This code looks ugly because the underlying library that we are using to parse returns a tree like structure
            and there was no easy way to fetch the information we wanted.
         2. Django's latest version has introduced sqlparse library as a requirement for using django.   We can explore
            that library in future, if that can make this code more readable
         3. The library is trying to write sql parser for a generic case, but there are many types of sql that django
            is never going to produce -- those are esoteric sql that probably only a human can write.
            See the test cases: https://github.com/mozilla/moz-sql-parser/tree/dev/tests to get an idea
    """
    query_with_fake_params = query_without_params.replace('%s', '1')
    parsed_sql: Dict = parse(query_with_fake_params)

    # Table Names
    table_names = []
    from_clause = parsed_sql.get('from', None)
    if from_clause:
        if isinstance(from_clause, str):  # SELECT * FROM test1
            table_names.append(from_clause)
        elif isinstance(from_clause, list):
            for from_clause_part in from_clause:
                if isinstance(from_clause_part, str):  # SELECT * FROM test2, test1
                    table_names.append(from_clause_part)
                elif isinstance(from_clause_part, dict):
                    if 'value' in from_clause_part:  # # SELECT A.f1, B.f1 FROM test1 as A, test1 as B
                        table_names.append(from_clause_part['value'])
                    elif any(key for key in from_clause_part.keys() if 'join' in key):
                        # select a.* from a INNER JOIN b USING (id)
                        table_names.extend(value for key, value in from_clause_part.items() if 'join' in key)

    # Where clause
    where_clause: Dict = parsed_sql.get('where', None)
    where_equality_key = ''
    if where_clause and len(where_clause) == 1:
        eq_clause = where_clause.get('eq', None)
        if eq_clause and isinstance(eq_clause, list):
            where_equality_key = eq_clause[0]

    return table_names, bool(where_clause), where_equality_key
