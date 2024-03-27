import codecs
import os

import pytest
import yatest.common

from file_common import run_file_kqp
from utils import DATA_PATH
from yql_utils import do_custom_query_check

EXCLUDED_SUITES = [
    'match_recognize',  # MATCH_RECOGNIZE is disabled
    'params',  # Params is not supported in KqpRun
    'pg',  # Not fully supported
    'pg-tpcds',  # Not fully supported
    'pg-tpch',  # Not fully supported
    'pg_catalog',  # Not fully supported
    'produce',  # Variant cast errors
    'schema',  # Not fully supported
    'simple_columns',  # Peephole optimization failed for KQP transaction
    'type_literal',  # Not supported
    'view',  # Not fully supported
]

EXCLUDED_TESTS = [
    'action/eval_asatom',  # ATOM evaluation is not supported in YDB queries
    'action/eval_astagged',  # ATOM evaluation is not supported in YDB queries
    'action/eval_capture',  # ATOM evaluation is not supported in YDB queries
    'action/eval_ensuretype',  # ATOM evaluation is not supported in YDB queries
    'action/eval_extract',  # ATOM evaluation is not supported in YDB queries
    'action/eval_pragma',  # ATOM evaluation is not supported in YDB queries
    'action/eval_resourcetype',  # ATOM evaluation is not supported in YDB queries
    'action/eval_result_label',  # ATOM evaluation is not supported in YDB queries
    'action/eval_taggedtype',  # ATOM evaluation is not supported in YDB queries
    'action/runtime_for_select',  # INTERNAL_ERROR

    'expr/as_dict_implicit_cast',  # Unsupported type kind: Void
    'expr/as_table_emptylist2',  # Expected list type, but got: EmptyList
    'expr/cast_variant',  # Unsupported type kind: Variant
    'expr/dict_common_type',  # Unsupported type kind: Void
    'expr/lds_literal',  # Unsupported type kind: Void
    'expr/replace_member',  # ATOM evaluation is not supported in YDB queries
    'expr/struct_builtins',  # ATOM evaluation is not supported in YDB queries
    'expr/struct_literal',  # ATOM evaluation is not supported in YDB queries

    'flatten_by/flatten_mode',  # INTERNAL_ERROR

    'flexible_types/struct_literals_vs_columns',  # ATOM evaluation is not supported in YDB queries

    'hor_join/group_yamr',  # INTERNAL_ERROR
    'hor_join/max_outtables',  # Failed to build query results
    'hor_join/skip_yamr',  # INTERNAL_ERROR
    'hor_join/sorted_out_mix',  # Failed to build query results
    'hor_join/less_outs',  # Failed to build query results

    'in/in_ansi_join',  # INTERNAL_ERROR
    'in/in_scalar_vector_subquery',  # Peephole optimization failed for KQP transaction
    'in/in_tablesource_on_raw_list',  # Failed to build query results
    'in/in_tuple_table',  # Cannot find table 'Root/Plato.[InputWithTuples]'

    'join/convert_check_key_mem',  # FAULT
    'join/inmem_by_uncomparable_structs',  # Peephole optimization failed for KQP transaction
    'join/inmem_by_uncomparable_tuples',  # Peephole optimization failed for KQP transaction
    'join/inmem_with_null_key',  # Error: Not comparable keys: a.a and b.a, Null != Null
    'join/inmem_with_set_key',  # FAULT
    'join/inmem_with_set_key_any',  # FAULT
    'join/join_comp_inmem',  # Peephole optimization failed for KQP transaction
    'join/nopushdown_filter_with_depends_on',  # Invalid YSON

    'limit/dynamic_limit',  # Missed callable: YtTableContent
    'limit/empty_read_after_limit',  # INTERNAL_ERROR

    'optimizers/flatmap_with_non_struct_out',  # Failed to build query results
    'optimizers/yt_shuffle_by_keys',  # Failed to build query results

    'select/tablepathprefix',  # ATOM evaluation is not supported in YDB queries

    'ypath/direct_read_from_dynamic',  # INTERNAL_ERROR
]

EXCLUDED_CANONIZATION = [
    'datetime/current_date',
    'expr/common_type_for_resource_and_data',
    'expr/current_tz',
    'optimizers/yql-10042_disable_flow_fuse_depends_on',
    'optimizers/yql-10042_disable_fuse_depends_on',
    'optimizers/yql-10074_dont_inline_lists_depends_on',
    'union/union_column_extention',
    'union/union_mix',
    'union_all/union_all_with_limits',
    'weak_field/weak_field',
]


def contains_not_commented(sql_query, substr, lower=False):
    if lower:
        sql_query = sql_query.lower()
    count_substr = sql_query.count(substr)
    count_substr_commented = sql_query.count('--' + substr) + sql_query.count('-- ' + substr)
    return count_substr > count_substr_commented


def validate_sql(sql_query):
    # Unsupported constructions
    if contains_not_commented(sql_query, 'define subquery', lower=True):
        pytest.skip('SUBQUERY is not supported in KQP')

    if contains_not_commented(sql_query, 'insert into', lower=True):
        pytest.skip('INSERT is not supported in KQP')

    if contains_not_commented(sql_query, 'discard', lower=True):
        pytest.skip('DISCARD is not supported in KQP')

    if contains_not_commented(sql_query, 'evaluate', lower=True):
        pytest.skip('EVALUATE is not supported in KQP')

    if contains_not_commented(sql_query, 'concat(', lower=True):
        pytest.skip('CONCAT is not supported in KQP')

    if contains_not_commented(sql_query, '.range(', lower=True) or contains_not_commented(sql_query, ' range(', lower=True):
        pytest.skip('RANGE is not supported in KQP')

    if contains_not_commented(sql_query, ' each(', lower=True):
        pytest.skip('EACH is not supported in KQP')

    if contains_not_commented(sql_query, 'drop table', lower=True):
        pytest.skip('DROP TABLE is not supported in KQP for extarnal entities')

    if contains_not_commented(sql_query, 'sample ', lower=True) or contains_not_commented(sql_query, 'sample(', lower=True):
        pytest.skip('SAMPLE is not supported in KQP')

    if contains_not_commented(sql_query, 'count(', lower=True):
        pytest.skip('COUNT is not supported in KQP')

    # Unsupported functions
    if contains_not_commented(sql_query, 'TableName('):
        pytest.skip('TableName is not supported in KQP')

    if contains_not_commented(sql_query, 'QuoteCode('):
        pytest.skip('QuoteCode is not supported in KQP')

    if contains_not_commented(sql_query, 'RangeComputeFor('):
        pytest.skip('RangeComputeFor is not supported in KQP')

    if contains_not_commented(sql_query, 'FromBytes('):
        pytest.skip('FromBytes is not supported in KQP')

    if contains_not_commented(sql_query, 'folder(', lower=True):
        pytest.skip('Folder is not supported in KQP')

    if contains_not_commented(sql_query, 'file(', lower=True) or contains_not_commented(sql_query, 'FileContent('):
        pytest.skip('Files is not supported in KQP')

    # Unsupported pragmas
    if contains_not_commented(sql_query, 'library(', lower=True):
        pytest.skip('Pragma Library is not supported in KQP')

    if contains_not_commented(sql_query, 'refselect', lower=True):
        pytest.skip('Pragma RefSelect is not supported in KQP')

    if contains_not_commented(sql_query, 'optimizerflags', lower=True):
        pytest.skip('Pragma OptimizerFlags is not supported in KQP')

    if contains_not_commented(sql_query, 'disablepullupflatmapoverjoin', lower=True):
        pytest.skip('Pragma DisablePullUpFlatMapOverJoin is not supported in KQP')

    if contains_not_commented(sql_query, 'costbasedoptimizer', lower=True):
        pytest.skip('Pragma CostBasedOptimizer is not supported in KQP')

    # Unsupported types
    if contains_not_commented(sql_query, 'date32', lower=True):
        pytest.skip('Type Date32 is not supported in KQP')

    if contains_not_commented(sql_query, 'datetime64', lower=True):
        pytest.skip('Type Datetime64 is not supported in KQP')

    if contains_not_commented(sql_query, 'timestamp64', lower=True):
        pytest.skip('Type Timestamp64 is not supported in KQP')

    if contains_not_commented(sql_query, 'interval64', lower=True):
        pytest.skip('Type Interval64 is not supported in KQP')

    if contains_not_commented(sql_query, 'interval64', lower=True):
        pytest.skip('Type Interval64 is not supported in KQP')

    if contains_not_commented(sql_query, 'void(', lower=True):
        pytest.skip('Type Void is not supported in KQP')

    if contains_not_commented(sql_query, 'variant(', lower=True):
        pytest.skip('Type Variant is not supported in KQP')


def run_test(suite, case, cfg):
    if suite in EXCLUDED_SUITES:
        pytest.skip('skip sute ' + suite)

    full_test_name = suite + '/' + case
    if full_test_name in EXCLUDED_TESTS:
        pytest.skip('skip case ' + suite + '/' + suite)

    program_sql = os.path.join(DATA_PATH, suite, '%s.sql' % case)
    with codecs.open(program_sql, encoding='utf-8') as program_file_descr:
        sql_query = program_file_descr.read()
        validate_sql(sql_query)

    result = run_file_kqp(suite, case, cfg)

    if do_custom_query_check(result, sql_query):
        return None

    if os.path.exists(result.results_file) and full_test_name not in EXCLUDED_CANONIZATION:
        return yatest.common.canonical_file(result.results_file)
