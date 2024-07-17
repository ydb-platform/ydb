import os
import shutil
import yatest.common
import yql_utils
import cyson as yson
import ydb.library.yql.providers.common.proto.gateways_config_pb2 as gateways_config_pb2
import ydb.library.yql.core.file_storage.proto.file_storage_pb2 as file_storage_pb2

import six

from google.protobuf import text_format

ARCADIA_PREFIX = 'arcadia/'
ARCADIA_TESTS_DATA_PREFIX = 'arcadia_tests_data/'

VAR_CHAR_PREFIX = '$'
FIX_DIR_PREFIXES = {
    'SOURCE': yatest.common.source_path,
    'BUILD': yatest.common.build_path,
    'TEST_SOURCE': yatest.common.test_source_path,
    'DATA': yatest.common.data_path,
    'BINARY': yatest.common.binary_path,
}


class YQLRun(object):

    def __init__(self, udfs_dir=None, prov='yt', use_sql2yql=False, keep_temp=True, binary=None, gateway_config=None, fs_config=None, extra_args=[], cfg_dir=None, support_udfs=True):
        if binary is None:
            self.yqlrun_binary = yql_utils.yql_binary_path(os.getenv('YQL_YQLRUN_PATH') or 'ydb/library/yql/tools/yqlrun/yqlrun')
        else:
            self.yqlrun_binary = binary
        self.extra_args = extra_args

        try:
            self.sql2yql_binary = yql_utils.yql_binary_path(os.getenv('YQL_SQL2YQL_PATH') or 'ydb/library/yql/tools/sql2yql/sql2yql')
        except BaseException:
            self.sql2yql_binary = None

        try:
            self.udf_resolver_binary = yql_utils.yql_binary_path(os.getenv('YQL_UDFRESOLVER_PATH') or 'ydb/library/yql/tools/udf_resolver/udf_resolver')
        except Exception:
            self.udf_resolver_binary = None

        if support_udfs:
            if udfs_dir is None:
                self.udfs_path = yql_utils.get_udfs_path()
            else:
                self.udfs_path = udfs_dir
        else:
            self.udfs_path = None

        res_dir = yql_utils.get_yql_dir(prefix='yqlrun_')
        self.res_dir = res_dir
        self.tables = {}
        self.prov = prov
        self.use_sql2yql = use_sql2yql
        self.keep_temp = keep_temp

        self.gateway_config = gateways_config_pb2.TGatewaysConfig()
        if gateway_config is not None:
            text_format.Merge(gateway_config, self.gateway_config)

        yql_utils.merge_default_gateway_cfg(cfg_dir or 'ydb/library/yql/cfg/tests', self.gateway_config)

        self.fs_config = file_storage_pb2.TFileStorageConfig()

        with open(yql_utils.yql_source_path(os.path.join(cfg_dir or 'ydb/library/yql/cfg/tests', 'fs.conf'))) as f:
            text_format.Merge(f.read(), self.fs_config)

        if fs_config is not None:
            text_format.Merge(fs_config, self.fs_config)

        if yql_utils.get_param('USE_NATIVE_YT_TYPES'):
            attr = self.gateway_config.Yt.DefaultSettings.add()
            attr.Name = 'UseNativeYtTypes'
            attr.Value = 'true'

        if yql_utils.get_param('SQL_FLAGS'):
            flags = yql_utils.get_param('SQL_FLAGS').split(',')
            self.gateway_config.SqlCore.TranslationFlags.extend(flags)

    def yql_exec(self, program=None, program_file=None, files=None, urls=None,
                 run_sql=False, verbose=False, check_error=True, tables=None, pretty_plan=True,
                 wait=True, parameters={}, extra_env={}, require_udf_resolver=False, scan_udfs=True):
        del pretty_plan

        res_dir = self.res_dir

        def res_file_path(name):
            return os.path.join(res_dir, name)

        opt_file = res_file_path('opt.yql')
        results_file = res_file_path('results.txt')
        plan_file = res_file_path('plan.txt')
        err_file = res_file_path('err.txt')

        udfs_dir = self.udfs_path
        prov = self.prov

        program, program_file = yql_utils.prepare_program(program, program_file, res_dir,
                                                          ext='sql' if run_sql else 'yql')

        syntax_version = yql_utils.get_syntax_version(program)
        ansi_lexer = yql_utils.ansi_lexer_enabled(program)

        if run_sql and self.use_sql2yql:
            orig_sql = program_file + '.orig_sql'
            shutil.copy2(program_file, orig_sql)
            cmd = [
                self.sql2yql_binary,
                orig_sql,
                '--yql',
                '--output=' + program_file,
                '--syntax-version=%d' % syntax_version
            ]
            if ansi_lexer:
                cmd.append('--ansi-lexer')
            env = {'YQL_DETERMINISTIC_MODE': '1'}
            env.update(extra_env)
            for var in [
                'LLVM_PROFILE_FILE',
                'GO_COVERAGE_PREFIX',
                'PYTHON_COVERAGE_PREFIX',
                'NLG_COVERAGE_FILENAME',
                'YQL_EXPORT_PG_FUNCTIONS_DIR',
                    ]:
                if var in os.environ:
                    env[var] = os.environ[var]
            yatest.common.process.execute(cmd, cwd=res_dir, env=env)

        with open(program_file) as f:
            yql_program = f.read()
        with open(program_file, 'w') as f:
            f.write(yql_program)

        gateways_cfg_file = res_file_path('gateways.conf')
        with open(gateways_cfg_file, 'w') as f:
            f.write(str(self.gateway_config))

        fs_cfg_file = res_file_path('fs.conf')
        with open(fs_cfg_file, 'w') as f:
            f.write(str(self.fs_config))

        cmd = self.yqlrun_binary + ' '

        if yql_utils.get_param('TRACE_OPT'):
            cmd += '--trace-opt '

        cmd += '-L ' \
            '--program=%(program_file)s ' \
            '--expr-file=%(opt_file)s ' \
            '--result-file=%(results_file)s ' \
            '--plan-file=%(plan_file)s ' \
            '--err-file=%(err_file)s ' \
            '--gateways=%(prov)s ' \
            '--syntax-version=%(syntax_version)d ' \
            '--tmp-dir=%(res_dir)s ' \
            '--gateways-cfg=%(gateways_cfg_file)s ' \
            '--fs-cfg=%(fs_cfg_file)s ' % locals()

        if self.udfs_path is not None:
            cmd += '--udfs-dir=%(udfs_dir)s ' % locals()

        if ansi_lexer:
            cmd += '--ansi-lexer '

        if self.keep_temp:
            cmd += '--keep-temp '

        if self.extra_args:
            cmd += " ".join(self.extra_args) + " "

        cmd += '--mounts=' + yql_utils.get_mount_config_file() + ' '

        if files:
            for f in files:
                if files[f].startswith(ARCADIA_PREFIX):  # how does it work with folders? and does it?
                    files[f] = yatest.common.source_path(files[f][len(ARCADIA_PREFIX):])
                    continue
                if files[f].startswith(ARCADIA_TESTS_DATA_PREFIX):
                    files[f] = yatest.common.data_path(files[f][len(ARCADIA_TESTS_DATA_PREFIX):])
                    continue

                if files[f].startswith(VAR_CHAR_PREFIX):
                    for prefix, func in six.iteritems(FIX_DIR_PREFIXES):
                        if files[f].startswith(VAR_CHAR_PREFIX + prefix):
                            real_path = func(files[f][len(prefix) + 2:])  # $ + prefix + /
                            break
                    else:
                        raise Exception("unknown prefix in file path %s" % (files[f],))
                    copy_dest = os.path.join(res_dir, f)
                    if not os.path.exists(os.path.dirname(copy_dest)):
                        os.makedirs(os.path.dirname(copy_dest))
                    shutil.copy2(
                        real_path,
                        copy_dest,
                    )
                    files[f] = f
                    continue

                if not files[f].startswith('/'):  # why do we check files[f] instead of f here?
                    path_to_copy = os.path.join(
                        yatest.common.work_path(),
                        files[f]
                    )
                    if '/' in files[f]:
                        copy_dest = os.path.join(
                            res_dir,
                            os.path.dirname(files[f])
                        )
                        if not os.path.exists(copy_dest):
                            os.makedirs(copy_dest)
                    else:
                        copy_dest = res_dir
                        files[f] = os.path.basename(files[f])
                    shutil.copy2(path_to_copy, copy_dest)
                else:
                    shutil.copy2(files[f], res_dir)
                    files[f] = os.path.basename(files[f])
            cmd += yql_utils.get_cmd_for_files('--file', files)

        if urls:
            cmd += yql_utils.get_cmd_for_files('--url', urls)

        optimize_only = False
        if tables:
            for table in tables:
                self.tables[table.full_name] = table
                if table.format != 'yson':
                    optimize_only = True
            for name in self.tables:
                cmd += '--table=yt.%s@%s ' % (name, self.tables[name].yqlrun_file)

        if "--lineage" not in self.extra_args:
            if optimize_only:
                cmd += '-O '
            else:
                cmd += '--run '

        if yql_utils.get_param('UDF_RESOLVER') or require_udf_resolver:
            assert self.udf_resolver_binary, "Missing udf_resolver binary"
            cmd += '--udf-resolver=' + self.udf_resolver_binary + ' '
            if scan_udfs:
                cmd += '--scan-udfs '
            if not yatest.common.context.sanitize:
                cmd += '--udf-resolver-filter-syscalls '

        if run_sql and not self.use_sql2yql:
            cmd += '--sql '

        if parameters:
            parameters_file = res_file_path('params.yson')
            with open(parameters_file, 'w') as f:
                f.write(six.ensure_str(yson.dumps(parameters)))
            cmd += '--params-file=%s ' % parameters_file

        if verbose:
            yql_utils.log('prov is ' + self.prov)

        env = {'YQL_DETERMINISTIC_MODE': '1'}
        env.update(extra_env)
        for var in [
            'LLVM_PROFILE_FILE',
            'GO_COVERAGE_PREFIX',
            'PYTHON_COVERAGE_PREFIX',
            'NLG_COVERAGE_FILENAME',
            'YQL_EXPORT_PG_FUNCTIONS_DIR',
                ]:
            if var in os.environ:
                env[var] = os.environ[var]
        if yql_utils.get_param('STDERR'):
            debug_udfs_dir = os.path.join(os.path.abspath('.'), '..', '..', '..')
            env_setters = ";".join("{}={}".format(k, v) for k, v in six.iteritems(env))
            yql_utils.log('GDB launch command:')
            yql_utils.log('(cd "%s" && %s ya tool gdb --args %s)' % (res_dir, env_setters, cmd.replace(udfs_dir, debug_udfs_dir)))

        proc_result = yatest.common.process.execute(cmd.strip().split(), check_exit_code=False, cwd=res_dir, env=env)
        if proc_result.exit_code != 0 and check_error:
            with open(err_file, 'r') as f:
                err_file_text = f.read()
            assert 0, \
                'Command\n%(command)s\n finished with exit code %(code)d, stderr:\n\n%(stderr)s\n\nerror file:\n%(err_file)s' % {
                    'command': cmd,
                    'code': proc_result.exit_code,
                    'stderr': proc_result.std_err,
                    'err_file': err_file_text
                }

        if os.path.exists(results_file) and os.stat(results_file).st_size == 0:
            os.unlink(results_file)  # kikimr yql-exec compatibility

        results, log_results = yql_utils.read_res_file(results_file)
        plan, log_plan = yql_utils.read_res_file(plan_file)
        opt, log_opt = yql_utils.read_res_file(opt_file)
        err, log_err = yql_utils.read_res_file(err_file)

        if verbose:
            yql_utils.log('PROGRAM:')
            yql_utils.log(program)
            yql_utils.log('OPT:')
            yql_utils.log(log_opt)
            yql_utils.log('PLAN:')
            yql_utils.log(log_plan)
            yql_utils.log('RESULTS:')
            yql_utils.log(log_results)
            yql_utils.log('ERROR:')
            yql_utils.log(log_err)

        return yql_utils.YQLExecResult(
            proc_result.std_out,
            yql_utils.normalize_source_code_path(err.replace(res_dir, '<tmp_path>')),
            results,
            results_file,
            opt,
            opt_file,
            plan,
            plan_file,
            program,
            proc_result,
            None
        )

    def create_empty_tables(self, tables):
        pass

    def write_tables(self, tables):
        pass

    def get_tables(self, tables):
        res = {}
        for table in tables:
            # recreate table after yql program was executed
            res[table.full_name] = yql_utils.new_table(
                table.full_name,
                yqlrun_file=self.tables[table.full_name].yqlrun_file,
                res_dir=self.res_dir
            )

            yql_utils.log('YQLRun table ' + table.full_name)
            yql_utils.log(res[table.full_name].content)

        return res
