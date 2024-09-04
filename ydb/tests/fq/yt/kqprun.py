import os

import pytest
import yatest.common

import yql_utils


class KqpRun(object):
    def __init__(self, udfs_dir=None):
        self.kqprun_binary = yql_utils.yql_binary_path('ydb/tests/tools/kqprun/kqprun')

        self.config_file = yql_utils.yql_source_path(os.path.join('ydb/tests/fq/yt/cfg', 'kqprun_config.conf'))
        self.scheme_file = yql_utils.yql_source_path(os.path.join('ydb/tests/fq/yt/cfg', 'kqprun_scheme.sql'))

        self.res_dir = yql_utils.get_yql_dir(prefix='kqprun_')

        if udfs_dir is None:
            self.udfs_dir = yql_utils.get_udfs_path()
        else:
            self.udfs_dir = udfs_dir

    def __res_file_path(self, name):
        return os.path.join(self.res_dir, name)

    def yql_exec(self, program=None, program_file=None, verbose=False, check_error=True, tables=None):
        udfs_dir = self.udfs_dir

        config_file = self.config_file
        program_file = yql_utils.prepare_program(program, program_file, self.res_dir, ext='sql')[1]
        scheme_file = self.scheme_file

        results_file = self.__res_file_path('results.txt')
        log_file = self.__res_file_path('log.txt')

        cmd = self.kqprun_binary + ' '

        cmd += (
            '--emulate-yt '
            '--exclude-linked-udfs '
            '--execution-case query '
            '--app-config=%(config_file)s '
            '--script-query=%(program_file)s '
            '--scheme-query=%(scheme_file)s '
            '--result-file=%(results_file)s '
            '--log-file=%(log_file)s '
            '--udfs-dir=%(udfs_dir)s '
            '--result-format full-proto '
            '--result-rows-limit 0 ' % locals()
        )

        if tables is not None:
            for table in tables:
                if table.format != 'yson':
                    pytest.skip('skip tests containing tables with a non-yson attribute format')
                cmd += '--table=yt.Root/%s@%s ' % (table.full_name, table.yqlrun_file)

        proc_result = yatest.common.process.execute(cmd.strip().split(), check_exit_code=False, cwd=self.res_dir)
        if proc_result.exit_code != 0 and check_error:
            assert 0, (
                'Command\n%(command)s\n finished with exit code %(code)d, stderr:\n\n%(stderr)s\n\nlog file:\n%(log_file)s'
                % {
                    'command': cmd,
                    'code': proc_result.exit_code,
                    'stderr': proc_result.std_err,
                    'log_file': yql_utils.read_res_file(log_file)[1],
                }
            )

        results, log_results = yql_utils.read_res_file(results_file)
        err, log_err = yql_utils.read_res_file(log_file)

        if verbose:
            yql_utils.log('PROGRAM:')
            yql_utils.log(program)
            yql_utils.log('RESULTS:')
            yql_utils.log(log_results)
            yql_utils.log('ERROR:')
            yql_utils.log(log_err)

        return yql_utils.YQLExecResult(
            proc_result.std_out,
            yql_utils.normalize_source_code_path(err.replace(self.res_dir, '<tmp_path>')),
            results,
            results_file,
            None,
            None,
            None,
            None,
            program,
            proc_result,
            None,
        )
