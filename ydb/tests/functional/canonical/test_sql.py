import os
import json
import copy
import pytest
import decimal
import functools
import uuid

from ydb.tests.library.common import yatest_common
from ydb.tests.library.harness.kikimr_cluster import kikimr_cluster_factory
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.oss.canonical import set_canondata_root, is_oss
from ydb.tests.oss.ydb_sdk_import import ydb
from ydb.public.api.protos import ydb_table_pb2
from google.protobuf import text_format
import logging


logger = logging.getLogger(__name__)


def mute_sdk_loggers():
    loggers = (
        'ydb.connection',
        'ydb._sp_impl.SessionPoolImpl',
        'urllib3.connectionpool',
        'ydb.resolver.DiscoveryEndpointsResolver'
    )

    for logger in loggers:
        li = logging.getLogger(logger)
        li.setLevel(logging.CRITICAL)


mute_sdk_loggers()


def find_files(data_folder, ext):
    resources_root = yatest_common.source_path(data_folder)
    directories = [resources_root]
    files = []
    while len(directories) > 0:
        current_directory = directories.pop()
        for candidate in map(lambda x: os.path.join(current_directory, x), os.listdir(current_directory)):
            if os.path.isfile(candidate) and candidate.endswith(ext):
                suite_relative_path = os.path.relpath(candidate, resources_root)
                files.append(suite_relative_path)

            if os.path.isdir(candidate):
                directories.append(candidate)
    return files


def get_queries(data_folder):
    nf = []
    for f in find_files(data_folder, 'sql'):
        nf.append((f, 'plan'))
        nf.append((f, 'result_sets'))

    for f in find_files(data_folder, 'script'):
        nf.append((f, 'script'))
    return nf


def canonical_filename(query, suffix):
    return os.path.join(yatest_common.output_path(), query.replace('/', '_') + suffix)


def write_output_file(data, filename):
    logger.info("writing data to file %s", filename)
    with open(filename, 'w') as writer:
        writer.write(data)
        writer.flush()
    return filename


def pretty_error_report(f):
    @functools.wraps(f)
    def handler(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except ydb.Error as reason:
            raise AssertionError(
                "\n============================"
                "\n Failed to handle {action}."
                "\n Reason: {reason}"
                "\n details: {details}"
                "\n============================"
                "\n".format(
                    action=f.__name__,
                    reason=reason.status.name,
                    details=str(reason)
                )
            )
    return handler


def transform_query_name(query_name):
    return query_name.replace('/', '_').replace('.', '_')


class BaseCanonicalTest(object):
    data_folder = None
    uniq_idx = None

    @classmethod
    def setup_class(cls):
        set_canondata_root('ydb/tests/functional/canonical/canondata')

        cls.database = '/local'
        cls.cluster = kikimr_cluster_factory(
            KikimrConfigGenerator(
                load_udfs=True,
                domain_name='local',
                use_in_memory_pdisks=True,
                disable_iterator_reads=True,
                disable_iterator_lookups=True
            )
        )
        cls.cluster.start()
        cls.driver = ydb.Driver(
            ydb.DriverConfig("%s:%s" % (cls.cluster.nodes[1].host, cls.cluster.nodes[1].port), cls.database))
        cls.pool = ydb.SessionPool(cls.driver)
        cls.logger = logging.getLogger(cls.__name__)
        cls.prefix = ''

    @classmethod
    def teardown_class(cls):
        cls.pool.stop()
        cls.cluster.stop()

    @classmethod
    def format_query(cls, query):
        query_parts = query.split('\n')
        is_v1 = False

        if query_parts[0].startswith('--!syntax_v1'):
            query_parts.pop(0)
            is_v1 = True

        query_parts = ["PRAGMA TablePathPrefix=\"{}\";".format(cls.prefix)] + query_parts
        if is_v1:
            query_parts = ['--!syntax_v1'] + query_parts

        return '\n'.join(query_parts)

    @classmethod
    def initialize_common(cls, query_name, kind):
        query_id = '_'.join([cls.uniq_idx, transform_query_name(query_name), kind])
        cls.prefix = '/local/%s' % query_id
        ydb.retry_operation_sync(lambda: cls.driver.scheme_client.make_directory(cls.prefix))
        for f in find_files(cls.data_folder, '.table'):
            cls.pool.retry_operation_sync(
                lambda session: session.create_table(
                    '',
                    cls.read_table_description(
                        f
                    )
                )
            )

        for s in find_files(cls.data_folder, '.scheme'):
            cls.pool.retry_operation_sync(
                lambda session: session.execute_scheme(
                    cls.format_query(
                        cls.read_query_text(
                            s
                        )
                    )
                )
            )

        for s in find_files(cls.data_folder, '.data'):
            description = cls.describe_table(s)
            batch = []
            for row in cls.read_data_rows(s):
                batch.append(row)
                if len(batch) < 100:
                    continue

                actual = copy.deepcopy(batch)
                batch = []
                cls.pool.retry_operation_sync(
                    lambda session: cls.upload_data(
                        session,
                        s,
                        description,
                        actual,
                    )
                )

            cls.pool.retry_operation_sync(
                lambda session: cls.upload_data(
                    session,
                    s,
                    description,
                    batch,
                )
            )

    @classmethod
    def read_data_rows(cls, s):
        f_path = yatest_common.source_path(os.path.join(cls.data_folder, s))
        with open(f_path, 'r') as r:
            return json.loads(r.read())

    @classmethod
    def to_bytes(cls, data):
        if isinstance(data, bytes):
            return data
        return bytes(data, 'utf-8')

    @classmethod
    def upload_data(cls, session, s, description, batch):
        st = ydb.StructType()
        for column in description.columns:
            st.add_member(column.name, column.type)
        lst = ydb.ListType(st)
        table_name, _ = os.path.splitext(os.path.basename(s))

        cls.logger.info("Upload data batch to table %s", table_name)
        query = (
            "--!syntax_v1\n"
            "PRAGMA TablePathPrefix(\"%s\");\n"
            "DECLARE $rows as %s; UPSERT INTO %s SELECT * FROM AS_TABLE($rows);" % (cls.prefix, str(lst), table_name)
        )
        processed_batch = []
        for row in batch:
            for column in description.columns:
                if row.get(column.name) is None:
                    continue

                if column.type.item == ydb.PrimitiveType.String:
                    row[column.name] = cls.to_bytes(row[column.name])
                if column.type.item == ydb.DecimalType:
                    row[column.name] = decimal.Decimal(row[column.name])
            processed_batch.append(row)

        prep_query = session.prepare(query)
        session.transaction().execute(
            prep_query,
            commit_tx=True,
            parameters={
                '$rows': processed_batch,
            }
        )

    @classmethod
    def describe_table(cls, s):
        table_name, _ = os.path.splitext(os.path.basename(s))
        with cls.pool.checkout() as session:
            return session.describe_table(
                os.path.join(
                    cls.prefix,
                    table_name,
                )
            )

    @classmethod
    def read_table_description(cls, f):
        content = cls.read_query_text(f)
        request = ydb_table_pb2.CreateTableRequest()
        text_format.Parse(content, request)
        table_name = os.path.basename(request.path)
        request.path = os.path.join(cls.prefix, table_name)
        return request

    @classmethod
    def read_query_text(cls, query_name):
        with open(yatest_common.source_path(os.path.join(cls.data_folder, query_name)), 'r') as reader:
            return reader.read()

    @staticmethod
    def pretty_json(j):
        return json.dumps(j, indent=4, sort_keys=True)

    @staticmethod
    def canonical_results(query, results):
        return yatest_common.canonical_file(
            local=True,
            universal_lines=True,
            path=write_output_file(
                results,
                canonical_filename(
                    query, '.results'
                )
            )
        )

    def execute_scheme(self, query):
        self.pool.retry_operation_sync(
            lambda session: session.execute_scheme(
                query,
            )
        )

    @staticmethod
    def canonical_plan(query, query_plan):
        return yatest_common.canonical_file(
            local=True,
            universal_lines=True,
            path=write_output_file(
                json.dumps(json.loads(query_plan), indent=4),
                canonical_filename(
                    query, '.plan'
                )
            )
        )

    @pretty_error_report
    def prepare(self, query):
        return self.pool.retry_operation_sync(
            lambda session: session.prepare(
                query,
            )
        )

    def do_execute(self, session, query, parameters=None):
        if parameters is None:
            parameters = {}

        prepared_query = session.prepare(query)
        result_sets = session.transaction(ydb.SerializableReadWrite()).execute(
            prepared_query,
            commit_tx=True,
            parameters=parameters,
        )
        return result_sets

    @pretty_error_report
    def serializable_execute(self, query, parameters=None):
        if parameters is None:
            parameters = {}

        return self.wrap_result_sets(
            self.pool.retry_operation_sync(
                lambda session: self.do_execute(
                    session,
                    query,
                    parameters,
                )
            )
        )

    def do_explain(self, session, query):
        operation_id = uuid.uuid4()
        self.logger.info(
            "Running explain for query:\nSession ID: %s\nOperation ID: %s\nQuery text: \n%s",
            session.session_id,
            operation_id,
            query)

        response = session.explain(query)
        self.logger.info(
            "Successfully explained query:\nSession ID: %s\nOperation ID: %s\nQuery Plan:\n%s\nQuery Full AST:\n%s",
            session.session_id,
            operation_id,
            response.query_plan,
            response.query_ast)
        return response.query_plan

    @pretty_error_report
    def explain(self, query):
        return self.pool.retry_operation_sync(
            lambda session: self.do_explain(
                session,
                query
            )
        )

    @staticmethod
    def wrap_value(v):
        if isinstance(v, decimal.Decimal):
            return str(v)
        if isinstance(v, bytes):
            return v.decode('utf-8')
        return v

    def wrap_rows(self, columns, rows):
        return [
            {
                column.name: self.wrap_value(row[column.name])
                for column in columns
            }
            for row in rows
        ]

    def wrap_result_set(self, result_set):
        return self.wrap_rows(result_set.columns, result_set.rows)

    def wrap_result_sets(self, result_sets):
        return [self.wrap_result_set(result_set) for result_set in result_sets]

    def read_config(self, query_name):
        dr = os.path.dirname(query_name)
        fl = os.path.basename(query_name)
        cfg_json = yatest_common.source_path(os.path.join(self.data_folder, dr, 'test_config.json'))
        if not os.path.exists(cfg_json):
            return {}
        cfg = json.loads(self.read_query_text(cfg_json))
        return cfg.get(fl, {})

    def read_table(self, table):
        fpath = os.path.join(self.prefix, table)
        with self.pool.checkout() as session:
            it = session.read_table(fpath, ordered=True)
            rows = []
            columns = []
            for chunk in it:
                rows.extend(chunk.rows)
                columns = chunk.columns
        return self.wrap_rows(columns, rows)

    def scan_query(self, query, parameters):
        pq = self.prepare(query)
        scan_query = ydb.ScanQuery(query, pq.parameters_types)
        it = self.driver.table_client.scan_query(scan_query, parameters)
        result_set = None
        for chunk in it:
            if result_set is None:
                result_set = chunk.result_set
            else:
                result_set.rows.extend(chunk.result_set.rows)
        return self.wrap_result_sets([result_set])

    def script_query(self, query, parameters_values=None, parameters_types=None):
        script_client = ydb.ScriptingClient(self.driver)
        parameters_values = {} if parameters_values is None else parameters_values
        parameters_types = {} if parameters_types is None else parameters_types
        for name, pt in parameters_types.items():
            parameters_types[name] = getattr(ydb.PrimitiveType, pt)
        params = ydb.TypedParameters(parameters_types, parameters_values)
        rp = script_client.execute_yql(query, params)
        return self.wrap_result_sets(rp.result_sets)

    def script_explain(self, query):
        script_client = ydb.ScriptingClient(self.driver)
        settings = ydb.ExplainYqlScriptSettings().with_mode(ydb.ExplainYqlScriptSettings.MODE_EXPLAIN)
        rp = script_client.explain_yql(query, settings)
        return rp.plan

    def compare_tables_test(self, canons, config, query_name):
        for table_name in config.get('compare_tables', []):
            canons['table_data_%s' % table_name] = self.canonical_results(
                query_name + '_' + table_name, self.pretty_json(
                    self.read_table(
                        table_name,
                    )
                )
            )

    def remove_optimizer_estimates(self, query_plan):
        if 'Plans' in query_plan:
            for p in query_plan['Plans']:
                self.remove_optimizer_estimates(p)
        if 'Operators' in query_plan:
            for op in query_plan['Operators']:
                for key in ['A-Cpu', 'A-Rows', 'E-Cost', 'E-Rows', 'E-Size']:
                    if key in op:
                        del op[key]

    def run_test_case(self, query_name, kind):
        self.initialize_common(query_name, kind)
        query = self.format_query(self.read_query_text(query_name))
        config = self.read_config(query_name)
        self.logger.info("Test config %s", config)
        canons = {}
        if kind == 'script':
            pv = config.get('parameters_values', {})
            pt = config.get('parameters_types', {})
            result_sets = self.script_query(query, pv, pt)
            canons['script'] = self.canonical_results(query_name, self.pretty_json(result_sets))
            plan = json.loads(self.script_explain(query))
            if 'queries' in plan:
                for q in plan['queries']:
                    if 'SimplifiedPlan' in q:
                        del q['SimplifiedPlan']
                    if 'Plan' in q:
                        self.remove_optimizer_estimates(q['Plan'])
            canons['script_plan'] = self.canonical_plan(query_name, self.pretty_json(plan))
            self.compare_tables_test(canons, config, query_name)
        elif kind == 'plan':
            plan = json.loads(self.explain(query))
            if 'Plan' in plan:
                del plan['Plan']
            if 'SimplifiedPlan' in plan:
                del plan['SimplifiedPlan']
            canons['plan'] = self.canonical_plan(query_name, self.pretty_json(plan))
        elif kind == 'result_sets':
            result_sets = self.serializable_execute(query, config.get('parameters', {}))
            canons['result_sets'] = self.canonical_results(query_name, self.pretty_json(result_sets))

            try:
                query_rows = self.scan_query(query, config.get('parameters', {}))
                assert self.pretty_json(query_rows) == self.pretty_json(result_sets), "Results mismatch: scan query result != data query."
            except ydb.issues.Error as e:
                incompatible_messages = [
                    "Secondary index is not supported for ScanQuery",
                    "Scan query should have a single result set",
                ]

                scan_query_incompatible = False

                for incompatible_message in incompatible_messages:

                    if incompatible_message in str(e):
                        scan_query_incompatible = True

                if not scan_query_incompatible:
                    raise

            self.compare_tables_test(canons, config, query_name)
        if not is_oss:
            return canons


class TestCanonicalFolder1(BaseCanonicalTest):
    data_folder = 'ydb/tests/functional/canonical/sql'
    uniq_idx = 'base'

    @pytest.mark.parametrize(['query_name', 'kind'],
                             get_queries('ydb/tests/functional/canonical/sql'))
    def test_case(self, query_name, kind):
        return self.run_test_case(query_name, kind)
