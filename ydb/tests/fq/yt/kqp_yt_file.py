import pytest

from file_common import check_provider, get_sql_query
from kqprun import KqpRun
from utils import DATA_PATH, get_config, get_parameters_files, replace_vars
from yql_utils import KSV_ATTR, get_files, get_http_files, get_tables, is_xfail, yql_binary_path

EXCLUDED_SUITES = [
]

EXCLUDED_TESTS = [
    # FAULT, CalcHash(): requirement false failed, YQ-3139
    'pg/pg_types_dict',
    # FAULT, requirement !HasNullInKey(key1) failed, YQ-3141
    'join/convert_check_key_mem',
    'join/inmem_with_set_key',
    # INTERNAL_ERROR, Visit(): requirement stagePlanNode.StageProto failed, YQ-3137
    'action/runtime_for_select',
    'pg_catalog/pg_set_config',
    # INTERNAL_ERROR, Peephole optimization failed, YQ-3138
    'flatten_by/flatten_mode',
    'in/in_scalar_vector_subquery',
    'join/inmem_with_set_key_any',
    # INTERNAL_ERROR, Cannot cast type Variant to Struct, YQ-3173
    'produce/reduce_multi_in',
    'produce/reduce_multi_in_difftype',
    'produce/reduce_multi_in_keytuple',
    'produce/reduce_multi_in_keytuple_difftype',
    'produce/reduce_multi_in_presort',
    'produce/reduce_multi_in_ref',
    # GENERIC_ERROR, Expected set on right side but got Dict, YQ-3140
    'in/in_tuple_table',
    'join/inmem_by_uncomparable_structs',
    'join/inmem_by_uncomparable_tuples',
    'join/join_comp_inmem',
    'pg/sublink_having_in',
    'pg/sublink_columns_in_test_expr_columns',
    # GENERIC_ERROR, JOIN with null type key, YQ-3142
    'join/inmem_with_null_key',
    # GENERIC_ERROR, Failed to build query results, YQ-3149
    'hor_join/max_outtables',
    'hor_join/sorted_out_mix',
    'hor_join/less_outs',
    'in/in_tablesource_on_raw_list',
    'optimizers/flatmap_with_non_struct_out',
    'optimizers/yt_shuffle_by_keys',
    # GENERIC_ERROR, Mismatch dict key types: Int64 and Optional<Int64>, YQ-3164
    'simple_columns/simple_columns_join_coalesce_all_1',
    'simple_columns/simple_columns_join_coalesce_all_2',
    'simple_columns/simple_columns_join_coalesce_bug8923',
    'simple_columns/simple_columns_join_coalesce_qualified_all_disable',
    'simple_columns/simple_columns_join_coalesce_qualified_all_enable',
    # PRECONDITION_FAILED, Unexpected flow status, YQ-3174
    'produce/reduce_typeinfo',
]


def contains_insert(sql_query):
    sql_query = sql_query.lower()
    return sql_query.count('insert into') > sql_query.count('--insert into') + sql_query.count('-- insert into')


def validate_sql(sql_query):
    # Unsupported constructions
    if 'define subquery' in sql_query.lower():
        pytest.skip('Using of system \'kikimr\' is not allowed in SUBQUERY')

    if 'concat(' in sql_query.lower() or 'each(' in sql_query.lower():
        pytest.skip('CONCAT is not supported on Kikimr clusters')

    if (
        'range(' in sql_query.lower()
        or 'regexp(' in sql_query.lower()
        or 'filter(' in sql_query.lower()
        or 'like(' in sql_query.lower()
        or '_strict(' in sql_query.lower()
    ):
        pytest.skip('RANGE is not supported on Kikimr clusters')

    if 'discard' in sql_query.lower():
        pytest.skip('DISCARD not supported in YDB queries')

    if 'commit' in sql_query.lower():
        pytest.skip('COMMIT not supported inside YDB query')

    if 'drop table' in sql_query.lower():
        pytest.skip('DROP TABLE is not supported for extarnal entities')

    if 'yt:' in sql_query.lower():
        pytest.skip('Explicit data source declaration is not supported for external entities')

    if contains_insert(sql_query):
        pytest.skip('INSERT is not supported for external data source YT')

    # Unsupported functions
    if 'TableName(' in sql_query:
        pytest.skip('TableName is not supported in KQP')

    if 'RangeComputeFor(' in sql_query:  # INTERNAL_ERROR if used, YQ-3134
        pytest.skip('RangeComputeFor is not supported in KQP')

    if 'folder(' in sql_query.lower():
        pytest.skip('Folder is not supported in KQP')

    if 'library(' in sql_query.lower() or 'file(' in sql_query.lower() or 'filecontent(' in sql_query.lower():
        pytest.skip('Attaching files and libraries is not supported in KQP')

    # Unsupported pragmas
    if 'refselect' in sql_query.lower():
        pytest.skip('RefSelect mode isn\'t supported by provider: kikimr')

    if 'optimizerflags' in sql_query.lower():
        pytest.skip('Pragma OptimizerFlags is not supported in KQP')

    if 'disablepullupflatmapoverjoin' in sql_query.lower():
        pytest.skip('Pragma DisablePullUpFlatMapOverJoin is not supported in KQP')

    if 'costbasedoptimizer' in sql_query.lower():
        pytest.skip('Pragma CostBasedOptimizer is not supported in KQP')

    if 'emitaggapply' in sql_query.lower():
        pytest.skip('Pragma EmitAggApply is not supported in KQP')

    if 'validateudf' in sql_query.lower():
        pytest.skip('Pragma ValidateUdf is not supported in KQP')

    if 'pragma dq.' in sql_query.lower():
        pytest.skip('DQ pragmas is not supported in KQP')

    if 'direct_read' in sql_query.lower() or 'directread' in sql_query.lower():
        pytest.skip('Pragma DirectRead is not supported for external data source YT')

    # Unsupported cases
    if 'as_table([])' in sql_query.lower():
        pytest.skip('AS_TABLE from empty list is not supported in KQP')

    if '--!syntax_pg' in sql_query and 'plato.' in sql_query.lower():
        pytest.skip('Dynamic cluster declaration is not supported in pg syntax')

    if '--!syntax_pg' not in sql_query and 'pg_catalog.' in sql_query.lower():
        pytest.skip('Cluster pg_catalog supported only in pg syntax due to common KQP table path prefix')

    if sql_query.count('plato') != sql_query.lower().count('plato'):
        pytest.skip('External data source name are case sensitive')

    # Unsupported test constructions
    if 'custom check:' in sql_query:
        pytest.skip('custom checks is not supported for KqpRun output format')


def run_test(suite, case, cfg):
    if suite in EXCLUDED_SUITES:
        pytest.skip('skip sute ' + suite)

    full_test_name = suite + '/' + case
    if full_test_name in EXCLUDED_TESTS:
        pytest.skip('skip case ' + full_test_name)

    run_file_kqp(suite, case, cfg)


def run_file_kqp_no_cache(suite, case, cfg):
    config = get_config(suite, case, cfg)
    in_tables = get_tables(suite, config, DATA_PATH, def_attr=KSV_ATTR)[0]

    sql_query = get_sql_query('yt', suite, case, config)
    sql_query = replace_vars(sql_query, "yqlrun_var")

    check_provider('yt', config)
    validate_sql(sql_query)

    if get_parameters_files(suite, config):
        pytest.skip('params is not supported in KqpRun')

    if get_files(suite, config, DATA_PATH) or get_http_files(suite, config, DATA_PATH):
        pytest.skip('file attachment is not supported in KQP')

    if is_xfail(config):
        pytest.skip('skip fail tests')

    kqprun = KqpRun(udfs_dir=yql_binary_path('ydb/library/yql/tests/common/test_framework/udfs_deps'))

    return kqprun.yql_exec(program=sql_query, verbose=True, check_error=True, tables=in_tables)


def run_file_kqp(suite, case, cfg):
    if (suite, case, cfg) not in run_file_kqp.cache:
        run_file_kqp.cache[(suite, case, cfg)] = run_file_kqp_no_cache(suite, case, cfg)

    return run_file_kqp.cache[(suite, case, cfg)]


run_file_kqp.cache = {}
