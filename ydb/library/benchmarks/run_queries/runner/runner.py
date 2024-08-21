import argparse
import os
import sys
import tempfile
import pathlib
import yt.yson

from abc import ABC, abstractmethod
from subprocess import run, PIPE, Popen, STDOUT, DEVNULL

from datetime import datetime
from s3_utils import Connection
from library.benchmarks.template import Builder

from prettytable import PrettyTable

class SpillingSource:
    def __init__(self, string):
        if string == "wide_combine":
            self._value = "w"
        elif string == "wide_top_sort":
            self._value = "s"
        elif string == "grace_join":
            self._value = "j"
        else:
            raise ValueError(f"Unknown spilling source: {string}")

    def __str__(self):
        return self._value


class RunStats:
    def __init__(self, duration, user_time, sys_time, rss_gb, spilling_sources, ok):
        self.duration = duration
        self.user_time = user_time
        self.sys_time = sys_time
        self.rss_gb = rss_gb
        self.spilling_sources = spilling_sources
        self.ok = ok
        
    def from_run_output(output):
        durations = []
        spilling_sources = []
        for line in output.splitlines():
            if 'duration:' in line:
                duration = float(line[line.index('duration:')+10:-1])
                durations.append(duration)
            if 'User time (seconds):' in line:
                user_time = float(line[21:])
            if 'System time (seconds):' in line:
                sys_time = float(line[23:])
            # yyyy-mm-dd hh:mm:ss.ms INFO  dqrun(pid=<pid>, tid=<tid>) [default] mkql_<file>.cpp:<line>: switching Memory mode to Spilling
            if 'dqrun' in line and 'switching Memory mode to Spilling' in line:
                file = line.split("mkql_")[-1].split(".cpp")[0]
                spilling_sources.append(SpillingSource(file))
            if 'Maximum resident set size (kbytes):' in line:
                def KB_to_GB(size):
                    return size / (1024 ** 2)

                rss = KB_to_GB(float(line[36:]))
            # Add spilling stats for input and output bytes (and maybe spilling time) when those will be ready
        
        return RunStats(sum(durations), user_time, sys_time, rss, "".join(spilling_sources), len(durations) > 0)


class AbstractRunner(ABC):
    def __init__(self):
        self.name = ''
        self.udfs_dir = '../../../contrib/ydb/library/yql/udfs/common/'
        self.dqrun_path = '../../../contrib/ydb/library/yql/tools/dqrun/dqrun'
        self.profile_dir = '../../../contrib/tools/flame-graph/'

        self.SECRET_ID = 'ver-01gsvz677329ntw9ame9k81hyr'

    def init_args_parser(self):
        parser = argparse.ArgumentParser() 
        parser._remove_action
        parser.add_argument('mode', choices=self.modes)
        parser.add_argument('--query-nums', default=None)
        parser.add_argument('--threads', default=16, type=int)
        parser.add_argument('--profile', action='store_true', default=False)
        parser.add_argument('--repeats', default=10, type=int)
        parser.add_argument('--remote', action='store_true', default=False)
        parser.add_argument('--ci', action='store_true', default=False)
        parser.add_argument('--vm', action='store_true', default=False)
        parser.add_argument('--dqrun', default=self.dqrun_path)
        parser.add_argument('--udfs-dir', default=self.udfs_dir)
        parser.add_argument('--profile-commands-dir', default=None)
        parser.add_argument('--taskset', default=None)
        parser.add_argument('--gateways-cfg', default=None)
        parser.add_argument('--fs-cfg', default=None)
        parser.add_argument('--dqhostport', default=None)
        parser.add_argument('--pragma', default=[], action='append', help='custom pragmas')
        parser.add_argument('--enable-spilling', action='store_true', default=False)
        parser.add_argument('--decimal', default=False)
        parser.add_argument('-o', '--output', type=pathlib.Path, default="./")
        return parser

    def parse_cmd_args(self):
        parser = self.init_args_parser()
        self.args = parser.parse_args()

    @property
    @abstractmethod
    def modes(self):
        pass

    @property
    def remote_path(self):
        return 'yql/library/benchmarks/remote'

    @property
    @abstractmethod
    def timeout_seconds(self):
        pass

    @abstractmethod
    def decode_mode(self, mode):
        pass

    @property
    @abstractmethod
    def arcadia_run_dir(self):
        pass

    @property
    def files(self):
        return [
            'contrib/ydb/library/yql/tools/dqrun/dqrun',
            f'{self.arcadia_run_dir}/run_queries',
            'contrib/tools/flame-graph/stackcollapse-perf.pl',
            'contrib/tools/flame-graph/flamegraph.pl',
            'yql/library/benchmarks/runner.py',
            'yql/library/benchmarks/ya.make',
            'yql/library/benchmarks/s3_utils.py',
            'yql/cfg/local/gateways.conf',
            'yql/cfg/local/fs.conf',
            run('ya tool perf --print-path', shell=True, capture_output=True).stdout.decode().strip()
        ]

    @property
    @abstractmethod
    def download_file(self):
        pass
    
    @property
    def enable_spilling(self):
        return self.args.enable_spilling

    def dirs(self):
        return []

    def packages(self):
        return []

    def get_nums(self):
        return list(range(1, 23))

    def run(self):
        self.parse_cmd_args()
        mode = self.args.mode
        assert (mode in self.modes)
        self.decode_mode(mode)

        if self.args.remote:
            self.run_remotely()
            return

        if self.args.profile:
            print('profiling...')

        nums = self.get_nums()
        nums.sort()
        if self.args.query_nums:
            query_nums = [int(x) for x in self.args.query_nums.split(',')]
            query_nums.sort()
            nums = [num for num in query_nums if num in nums]
            queries = ','.join(f'q{num}' for num in query_nums)
            print(f'filter: {queries}')

        if self.args.remote:
            self.run_remotely()
            return

        result_path = self.args.output.resolve() / f"{self.name}"
        result_path.mkdir(parents=True, exist_ok=True)
        self.process_queries(nums, result_path)

    def run_with_timeout(self, cmd, stdout=PIPE):
        proc = None
        try:
            proc = Popen(cmd, stdout=stdout, stderr=STDOUT)
            stdout, _ = proc.communicate(timeout=self.timeout_seconds)
            return stdout.decode() if stdout is not None else None, proc
        finally:
            stdout = None
            if proc is not None:
                proc.kill()
                stdout, _ = proc.communicate()
            sys.stderr.write(stdout.decode('utf-8', 'ignore') if stdout is not None else '')

    def process_queries(self, nums, result_path):
        b = Builder([self.cases_dir, "."])
        b.add_link("pragmas.sql", self.pragmas_path)
        pragma_keyword = "pragma"
        custom_pragmas = ""
        if self.syntax == "pg":
            pragma_keyword = "set"
        for pragma in self.args.pragma:
            k, v = pragma.split('=')
            custom_pragmas = custom_pragmas + f'{pragma_keyword} {k}="{v}";\n'
        b.add("custom_pragmas", custom_pragmas)
        if self.syntax == "yql":
            if self.args.decimal:
                b.add_link("consts.jinja", "consts_decimal.yql")
            else:
                b.add_link("consts.jinja", "consts.yql")

        tmp_handle, bindings_file = tempfile.mkstemp(".sql")
        with open(tmp_handle, "w", newline="\n", closefd=True) as bindings_out:
            b.add_vars({"data": self.data_path})
            bindings = b.build(self.bindings_file)
            bindings_out.write(bindings)
        
        with open(result_path / "stats.yson", "w") as run_stats_file:
            for num in nums:
                tmp_handle, program_file = tempfile.mkstemp('.sql')

                with open(tmp_handle, 'w', newline='\n', closefd=True) as sql_out:
                    sql = b.build("%s/q%s.sql" % (self.cases_dir, num), True)
                    sql_out.write(sql)
                
                profile_file = result_file / f"q{num}.svg"
                result_file = result_path / f"q{num}-result.txt"
                stat_file = result_path / f"q{num}-stat.txt"
                err_file = result_path / f"q{num}-err.txt"
                self.process_query(num, run_stats_file, program_file, bindings_file, profile_file, result_file, stat_file, err_file)
                
                os.remove(program_file)

                    
        os.remove(bindings_file)
        print('Done')
    
    def process_query(self, num, run_stats_file, program_file, bindings_file, profile_file, result_file, stat_file, err_file):
        try:
            print(f'q{num}')

            udfs_dir = ';'.join(os.path.join(self.args.udfs_dir, udf) for udf in self.udfs)
            args = [
                self.args.dqrun,
                '-s',
                '--program', program_file,
                '--bindings-file', bindings_file,
                '--udfs-dir', udfs_dir,
                '--threads', str(self.args.threads)
            ]

            if self.args.taskset:
                args = ["taskset", "-c", self.args.taskset] + args

            if self.args.gateways_cfg:
                args = args + ["--gateways-cfg", self.args.gateways_cfg]
            if self.args.fs_cfg:
                args = args + ["--fs-cfg", self.args.fs_cfg]
            if self.args.dqhostport:
                (host, port) = self.args.dqhostport.split(':')
                args = args + ["--dq-host", host, "--dq-port", port]
            if self.enable_spilling:
                args = args + ["--enable-spilling"]

            if self.args.profile:
                if self.args.profile_commands_dir:
                    stackcollapse = os.path.join(self.args.profile_commands_dir, 'stackcollapse-perf.pl')
                    flamegraph = os.path.join(self.args.profile_commands_dir, 'flamegraph.pl')
                    perf = os.path.join(self.args.profile_commands_dir, 'perf')
                else:
                    stackcollapse = os.path.join(self.profile_dir, 'stackcollapse-perf.pl')
                    flamegraph = os.path.join(self.profile_dir, 'flamegraph.pl')
                    perf = 'ya tool perf' if not self.args.vm else './perf'

                prof_args = f'{perf} record -F 250 -g --proc-map-timeout=10000 --'.split() + args
                self.run_with_timeout(prof_args, stdout=DEVNULL)
                run(f'{perf} script --no-demangle | gzip -1 > out.perf.gz', shell=True)
                run(f'pv out.perf.gz | zcat | c++filt -n | {stackcollapse} | {flamegraph} > {profile_file}', shell=True)
                os.remove('out.perf.gz')
                os.remove('perf.data')
                print(f'written {profile_file}')

            results = []
            args = ['/usr/bin/time', '-v'] + args
            text = "no duration?"
            for i in range(self.args.repeats):
                result_args = ['--result-file', result_file, '--stat', stat_file, '--err-file', err_file]

                print('.', end='', flush=True)

                output, proc = self.run_with_timeout(args+result_args)

                stats = RunStats.from_run_output(output)

                exitcode = proc.wait()
                if exitcode != 0:
                    stats.ok = False
                    
                if stats.ok:
                    results.append(stats)
                else:
                    with open(err_file, "r", closefd=True) as f:
                        err_text = ''.join(f.readlines()).replace('\n', '\\n')
                        if len(err_text) > 0:
                            text = err_text
                    print(f'q{num}: {text}')
                try:
                    self.print_result(result_file)
                    self.print_stats(stat_file)
                except:
                    pass
                
                if not stats.ok:
                    break

            score = text
            if len(results) > 0:
                # best_results = sorted(results)[0:1 + len(results)*2//3]
                # score_number = math.exp(sum(math.log(x) for x in best_results) * 1/len(best_results))
                score = yt.yson.dumps(yt.yson.to_yson_type(results[0].__dict__), yson_format="pretty", yson_type="map_fragment", indent=4).decode()
            # print(f' {score}s {mem_usage:.2f} GB')
            if run_stats_file:
                run_stats_file.write(f'q{num}={score};\n')
                run_stats_file.flush()
        except Exception as e:
            raise e

    def print_stats(self, st_file):
        with open(st_file, "rb", closefd=True) as f:
            data = yt.yson.loads(f.read())
            data = data["ExecutionStatistics"]["dq"]["TaskRunner"]

            tasksPerStage = []
            totalTasks = 0
            for k, v in data.items():
                tasks = v["Task"]["total"]["TasksCount"]["sum"]
                if k == "Stage=Total":
                    totalTasks = tasks
                else:
                    tasksPerStage.append(tasks)

            tasksPerStage.sort()
            print("tasks per stage: %s" % (tasksPerStage))
            print("total tasks: %d" % (totalTasks))

    def print_result(self, res_file):
        with open(res_file, "rb", closefd=True) as rf:
            table = PrettyTable()
            data = yt.yson.loads(rf.read())
            # [{'Position': {'File': '<main>', 'Row': 2, 'Column': 30}, 'Write': [{'Type': ['ListType', ['StructType', [['s_acctbal', ['PgType', 'numeric']], ['s_name', ['PgType', 'text']],
            # ['n_name', ['PgType', 'text']], ['p_partkey', ['PgType', 'int8']], ['p_mfgr', ['PgType', 'text']], ['s_address', ['PgType', 'text']], ['s_phone', ['PgType', 'text']],
            # ['s_comment', ['PgType', 'text']]]]]

            # header
            fields = []
            for f in data[0]['Write'][0]['Type'][1][1]:
                fields.append(f[0])
            table.align = 'l'
            rows = 0
            for r in data[0]['Write'][0]['Data']:
                row = []
                for c in r:
                    if isinstance(c, list) and len(c) == 1:
                        row.append(c[0])
                    else:
                        row.append(c)
                table.add_row(row)
                rows = rows + 1

            table.field_names = fields
            print(table)
            print("(%d rows)" % (rows))

    def run_remotely(self):
        op_id = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
        if not self.args.ci:
            from library.python.vault_client.instances import Production as VaultClient

            secret = VaultClient(decode_files=True).get_version(self.SECRET_ID)['value']
            access_key_id = secret['s3.sa.access_key_id']
            secret_access_key = secret['s3.sa.secret_access_key']
        else:
            access_key_id = run('cat $s3_sa_access_key_id', capture_output=True, shell=True).stdout.decode()
            secret_access_key = run('cat $s3_sa_secret_access_key', capture_output=True, shell=True).stdout.decode()

        print(f'Operation: {op_id}')

        try:
            import subprocess
            connection = Connection(access_key_id, secret_access_key, op_id)

            os.chdir('./../../../')

            print(f"CWD: {os.getcwd()}")

            for file in self.files:
                print(f'Passing file {file}')
                if not os.path.exists(file):
                    raise Exception(f"No such file {file}")
                if file.endswith("dqrun") or file.endswith(".so"):
                    print(f'Strip {file}')
                    r = subprocess.run(["strip", file])
                    print(f'Return code {r.returncode}')
                connection.upload(file)

            for dir in self.dirs:
                print(f'Passing directory {dir}')
                connection.upload_dir(dir)

            connection.upload_dir(self.arcadia_run_dir, suffix='.txt')

            run_cmd = (
                f'./run_queries {self.args.mode}'
                + ' '
                + (f'--query-nums {self.args.query_nums}' if self.args.query_nums else '')
                + ' '
                + ('--profile' if self.args.profile else '')
                + ' '
                + ('--enable-spilling' if self.enable_spilling else '')
                + ' '
                + '--vm'
                + ' '
                + '--gateways-cfg ../../cfg/local/gateways.conf'
                + ' '
                + '--fs-cfg ../../cfg/local/fs.conf'
            )

            with open(f'{self.remote_path}/cloudinit_template.sh', 'r') as template, open(f'{self.remote_path}/cloudinit.sh', 'w+') as cloudinit:
                replacements = {
                    'access_key_id': access_key_id,
                    'secret_access_key': secret_access_key,
                    'run_cmd': run_cmd,
                    'op_id': op_id,
                    'download_file': self.download_file,
                    'arcadia_run_dir': self.arcadia_run_dir
                }
                cloudinit.write(template.read().format_map(replacements))

            print('Creating virtual machine')
            output = run([f'{self.remote_path}/vm_create.sh', op_id], capture_output=True)
            if output.stderr is not None:
                print(output.stderr.decode())

            os.chdir(self.arcadia_run_dir)
            connection.download('marker')
            os.remove('marker')
            connection.download_dir('.txt')
            connection.download_dir('.svg')
        finally:
            os.chdir('../../../')
            print('Deleting s3 folder')
            connection.delete_folder()

            print('Deleting virtual machine')
            output = run([f'{self.remote_path}/vm_delete.sh', op_id], capture_output=True)
            if output.stderr is not None:
                print(output.stderr.decode())
            os.chdir(self.arcadia_run_dir)

        with open('stdout.txt', 'r') as f:
            print(f.read())

        print(f'Done {op_id}')
