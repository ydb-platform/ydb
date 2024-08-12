from __future__ import print_function

import hashlib
import io
import os
import os.path
import six
import sys
import re
import tempfile
import shutil

from google.protobuf import text_format
from collections import namedtuple, defaultdict, OrderedDict
from functools import partial
import codecs
import decimal
from threading import Lock

import pytest
import yatest.common
import cyson

import logging
import getpass

logger = logging.getLogger(__name__)

KSV_ATTR = '''{_yql_row_spec={
    Type=[StructType;
        [[key;[DataType;String]];
        [subkey;[DataType;String]];
        [value;[DataType;String]]]]}}'''


def get_param(name, default=None):
    name = 'YQL_' + name.upper()
    return yatest.common.get_param(name, os.environ.get(name) or default)


def do_custom_query_check(res, sql_query):
    custom_check = re.search(r"/\* custom check:(.*)\*/", sql_query)
    if not custom_check:
        return False
    custom_check = custom_check.group(1)
    yt_res_yson = res.results
    yt_res_yson = cyson.loads(yt_res_yson) if yt_res_yson else cyson.loads("[]")
    yt_res_yson = replace_vals(yt_res_yson)
    assert eval(custom_check), 'Condition "%(custom_check)s" fails\nResult:\n %(yt_res_yson)s\n' % locals()
    return True


def get_gateway_cfg_suffix():
    default_suffix = None
    return get_param('gateway_config_suffix', default_suffix) or ''


def get_gateway_cfg_filename():
    suffix = get_gateway_cfg_suffix()
    if suffix == '':
        return 'gateways.conf'
    else:
        return 'gateways-' + suffix + '.conf'


def merge_default_gateway_cfg(cfg_dir, gateway_config):

    with open(yql_source_path(os.path.join(cfg_dir, 'gateways.conf'))) as f:
        text_format.Merge(f.read(), gateway_config)

    suffix = get_gateway_cfg_suffix()
    if suffix:
        with open(yql_source_path(os.path.join(cfg_dir, 'gateways-' + suffix + '.conf'))) as f:
            text_format.Merge(f.read(), gateway_config)


def find_file(path):
    arcadia_root = '.'
    while '.arcadia.root' not in os.listdir(arcadia_root):
        arcadia_root = os.path.join(arcadia_root, '..')
    res = os.path.abspath(os.path.join(arcadia_root, path))
    assert os.path.exists(res)
    return res


output_path_cache = {}


def yql_output_path(*args, **kwargs):
    if not get_param('LOCAL_BENCH_XX'):
        # abspath is needed, because output_path may be relative when test is run directly (without ya make).
        return os.path.abspath(yatest.common.output_path(*args, **kwargs))

    else:
        if args and args in output_path_cache:
            return output_path_cache[args]
        res = os.path.join(tempfile.mkdtemp(prefix='yql_tmp_'), *args)
        if args:
            output_path_cache[args] = res
        return res


def yql_binary_path(*args, **kwargs):
    if not get_param('LOCAL_BENCH_XX'):
        return yatest.common.binary_path(*args, **kwargs)

    else:
        return find_file(args[0])


def yql_source_path(*args, **kwargs):
    if not get_param('LOCAL_BENCH_XX'):
        return yatest.common.source_path(*args, **kwargs)
    else:
        return find_file(args[0])


def yql_work_path():
    return os.path.abspath('.')


YQLExecResult = namedtuple('YQLExecResult', (
    'std_out',
    'std_err',
    'results',
    'results_file',
    'opt',
    'opt_file',
    'plan',
    'plan_file',
    'program',
    'execution_result',
    'statistics'
))

Table = namedtuple('Table', (
    'name',
    'full_name',
    'content',
    'file',
    'yqlrun_file',
    'attr',
    'format',
    'exists'
))


def new_table(full_name, file_path=None, yqlrun_file=None, content=None, res_dir=None,
              attr=None, format_name='yson', def_attr=None, should_exist=False, src_file_alternative=None):
    assert '.' in full_name, 'expected name like cedar.Input'
    name = '.'.join(full_name.split('.')[1:])

    if res_dir is None:
        res_dir = get_yql_dir('table_')

    exists = True
    if content is None:
        # try read content from files
        src_file = file_path or yqlrun_file
        if src_file is None:
            # nonexistent table, will be output for query
            content = ''
            exists = False
        else:
            if os.path.exists(src_file):
                with open(src_file, 'rb') as f:
                    content = f.read()
            elif src_file_alternative and os.path.exists(src_file_alternative):
                with open(src_file_alternative, 'rb') as f:
                    content = f.read()
                src_file = src_file_alternative
                yqlrun_file, src_file_alternative = src_file_alternative, yqlrun_file
            else:
                content = ''
                exists = False

    file_path = os.path.join(res_dir, name + '.txt')
    new_yqlrun_file = os.path.join(res_dir, name + '.yqlrun.txt')

    if exists:
        with open(file_path, 'wb') as f:
            f.write(content)

        # copy or create yqlrun_file in proper dir
        if yqlrun_file is not None:
            shutil.copyfile(yqlrun_file, new_yqlrun_file)
        else:
            with open(new_yqlrun_file, 'wb') as f:
                f.write(content)
    else:
        assert not should_exist, locals()

    if attr is None:
        # try read from file
        attr_file = None
        if os.path.exists(file_path + '.attr'):
            attr_file = file_path + '.attr'
        elif yqlrun_file is not None and os.path.exists(yqlrun_file + '.attr'):
            attr_file = yqlrun_file + '.attr'
        elif src_file_alternative is not None and os.path.exists(src_file_alternative + '.attr'):
            attr_file = src_file_alternative + '.attr'

        if attr_file is not None:
            with open(attr_file) as f:
                attr = f.read()

    if attr is None:
        attr = def_attr

    if attr is not None:
        # probably we get it, now write attr file to proper place
        attr_file = new_yqlrun_file + '.attr'
        with open(attr_file, 'w') as f:
            f.write(attr)

    return Table(
        name,
        full_name,
        content,
        file_path,
        new_yqlrun_file,
        attr,
        format_name,
        exists
    )


def ensure_dir_exists(dir):
    # handle race between isdir and mkdir
    if os.path.isdir(dir):
        return

    try:
        os.mkdir(dir)
    except OSError:
        if not os.path.isdir(dir):
            raise


def get_yql_dir(prefix):
    yql_dir = yql_output_path('yql')
    ensure_dir_exists(yql_dir)
    res_dir = tempfile.mkdtemp(prefix=prefix, dir=yql_dir)
    os.chmod(res_dir, 0o755)
    return res_dir


def get_cmd_for_files(arg, files):
    cmd = ' '.join(
        arg + ' ' + name + '@' + files[name]
        for name in files
    )
    cmd += ' '
    return cmd


def read_res_file(file_path):
    if os.path.exists(file_path):
        with codecs.open(file_path, encoding="utf-8") as descr:
            res = descr.read().strip()
            if res == '':
                log_res = '<EMPTY>'
            else:
                log_res = res
    else:
        res = ''
        log_res = '<NOTHING>'
    return res, log_res


def normalize_yson(y):
    from cyson import YsonBoolean, YsonEntity
    if isinstance(y, YsonBoolean) or isinstance(y, bool):
        return 'true' if y else 'false'
    if isinstance(y, YsonEntity) or y is None:
        return None
    if isinstance(y, list):
        return [normalize_yson(i) for i in y]
    if isinstance(y, dict):
        return {normalize_yson(k): normalize_yson(v) for k, v in six.iteritems(y)}
    s = str(y) if not isinstance(y, six.text_type) else y.encode('utf-8', errors='xmlcharrefreplace')
    return s


volatile_attrs = {'DataSize', 'ModifyTime', 'Id', 'Revision'}
current_user = getpass.getuser()


def _replace_vals_impl(y):
    if isinstance(y, list):
        return [_replace_vals_impl(i) for i in y]
    if isinstance(y, dict):
        return {_replace_vals_impl(k): _replace_vals_impl(v) for k, v in six.iteritems(y) if k not in volatile_attrs}
    if isinstance(y, str):
        s = y.replace('tmp/yql/' + current_user + '/', 'tmp/')
        s = re.sub(r'tmp/[0-9a-f]+-[0-9a-f]+-[0-9a-f]+-[0-9a-f]+', 'tmp/<temp_table_guid>', s)
        return s
    return y


def replace_vals(y):
    y = normalize_yson(y)
    y = _replace_vals_impl(y)
    return y


def patch_yson_vals(y, patcher):
    if isinstance(y, list):
        return [patch_yson_vals(i, patcher) for i in y]
    if isinstance(y, dict):
        return {patch_yson_vals(k, patcher): patch_yson_vals(v, patcher) for k, v in six.iteritems(y)}
    if isinstance(y, str):
        return patcher(y)
    return y


floatRe = re.compile(r'^-?\d*\.\d+$')
floatERe = re.compile(r'^-?(\d*\.)?\d+e[\+\-]?\d+$', re.IGNORECASE)
specFloatRe = re.compile(r'^(-?inf|nan)$', re.IGNORECASE)


def fix_double(x):
    if floatRe.match(x) and len(x.replace('.', '').replace('-', '')) > 10:
        # Emulate the same double precision as C++ code has
        decimal.getcontext().rounding = decimal.ROUND_HALF_DOWN
        decimal.getcontext().prec = 10
        return str(decimal.Decimal(0) + decimal.Decimal(x)).rstrip('0')
    if floatERe.match(x):
        # Emulate the same double precision as C++ code has
        decimal.getcontext().rounding = decimal.ROUND_HALF_DOWN
        decimal.getcontext().prec = 10
        return str(decimal.Decimal(0) + decimal.Decimal(x)).lower()
    if specFloatRe.match(x):
        return x.lower()
    return x


def remove_volatile_ast_parts(ast):
    return re.sub(r"\(KiClusterConfig '\('\(.*\) '\"\d\" '\"\d\" '\"\d\"\)\)", "(KiClusterConfig)", ast)


def prepare_program(program, program_file, yql_dir, ext='yql'):
    assert not (program is None and program_file is None), 'Needs program or program_file'

    if program is None:
        with codecs.open(program_file, encoding='utf-8') as program_file_descr:
            program = program_file_descr.read()

    program_file = os.path.join(yql_dir, 'program.' + ext)
    with codecs.open(program_file, 'w', encoding='utf-8') as program_file_descr:
        program_file_descr.write(program)

    return program, program_file


def get_program_cfg(suite, case, DATA_PATH):
    ret = []
    config = os.path.join(DATA_PATH, suite if suite else '', case + '.cfg')
    if not os.path.exists(config):
        config = os.path.join(DATA_PATH, suite if suite else '', 'default.cfg')

    if os.path.exists(config):
        for line in open(config, 'r'):
            if line.strip():
                ret.append(tuple(line.split()))
    else:
        in_filename = case + '.in'
        in_path = os.path.join(DATA_PATH, in_filename)
        default_filename = 'default.in'
        default_path = os.path.join(DATA_PATH, default_filename)
        for filepath in [in_path, in_filename, default_path, default_filename]:
            if os.path.exists(filepath):
                try:
                    shutil.copy2(filepath, in_path)
                except shutil.Error:
                    pass
                ret.append(('in', 'yamr.plato.Input', in_path))
                break

    if not is_os_supported(ret):
        pytest.skip('%s not supported here' % sys.platform)

    return ret


def find_user_file(suite, path, DATA_PATH):
    source_path = os.path.join(DATA_PATH, suite, path)
    if os.path.exists(source_path):
        return source_path
    else:
        try:
            return yql_binary_path(path)
        except Exception:
            raise Exception('Can not find file ' + path)


def get_input_tables(suite, cfg, DATA_PATH, def_attr=None):
    in_tables = []
    for item in cfg:
        if item[0] in ('in', 'out'):
            io, table_name, file_name = item
            if io == 'in':
                in_tables.append(new_table(
                    full_name=table_name.replace('yamr.', '').replace('yt.', ''),
                    yqlrun_file=os.path.join(DATA_PATH, suite if suite else '', file_name),
                    src_file_alternative=os.path.join(yql_work_path(), suite if suite else '', file_name),
                    def_attr=def_attr,
                    should_exist=True
                ))
    return in_tables


def get_tables(suite, cfg, DATA_PATH, def_attr=None):
    in_tables = []
    out_tables = []
    suite_dir = os.path.join(DATA_PATH, suite)
    res_dir = get_yql_dir('table_')

    for splitted in cfg:
        if splitted[0] == 'udf' and yatest.common.context.sanitize == 'undefined':
            pytest.skip("udf under ubsan")

        if len(splitted) == 4:
            type_name, table, file_name, format_name = splitted
        elif len(splitted) == 3:
            type_name, table, file_name = splitted
            format_name = 'yson'
        else:
            continue
        yqlrun_file = os.path.join(suite_dir, file_name)
        if type_name == 'in':
            in_tables.append(new_table(
                full_name='plato.' + table if '.' not in table else table,
                yqlrun_file=yqlrun_file,
                format_name=format_name,
                def_attr=def_attr,
                res_dir=res_dir
            ))
        if type_name == 'out':
            out_tables.append(new_table(
                full_name='plato.' + table if '.' not in table else table,
                yqlrun_file=yqlrun_file if os.path.exists(yqlrun_file) else None,
                res_dir=res_dir
            ))
    return in_tables, out_tables


def get_supported_providers(cfg):
    providers = 'yt', 'kikimr', 'dq', 'hybrid'
    for item in cfg:
        if item[0] == 'providers':
            providers = [i.strip() for i in ''.join(item[1:]).split(',')]
    return providers


def is_os_supported(cfg):
    for item in cfg:
        if item[0] == 'os':
            return any(sys.platform.startswith(_os) for _os in item[1].split(','))
    return True


def is_xfail(cfg):
    for item in cfg:
        if item[0] == 'xfail':
            return True
    return False


def is_skip_forceblocks(cfg):
    for item in cfg:
        if item[0] == 'skip_forceblocks':
            return True
    return False


def is_canonize_peephole(cfg):
    for item in cfg:
        if item[0] == 'canonize_peephole':
            return True
    return False


def is_peephole_use_blocks(cfg):
    for item in cfg:
        if item[0] == 'peephole_use_blocks':
            return True
    return False


def is_canonize_lineage(cfg):
    for item in cfg:
        if item[0] == 'canonize_lineage':
            return True
    return False


def is_canonize_yt(cfg):
    for item in cfg:
        if item[0] == 'canonize_yt':
            return True
    return False

def is_with_final_result_issues(cfg):
    for item in cfg:
        if item[0] == 'with_final_result_issues':
            return True
    return False

def skip_test_if_required(cfg):
    for item in cfg:
        if item[0] == 'skip_test':
            pytest.skip(item[1])


def get_pragmas(cfg):
    pragmas = []
    for item in cfg:
        if item[0] == 'pragma':
            pragmas.append(' '.join(item))
    return pragmas


def execute(
        klass=None,
        program=None,
        program_file=None,
        files=None,
        urls=None,
        run_sql=False,
        verbose=False,
        check_error=True,
        input_tables=None,
        output_tables=None,
        pretty_plan=True,
        parameters={},
):
    '''
    Executes YQL/SQL

    :param klass: KiKiMRForYQL if  instance (default: YQLRun)
    :param program: string with YQL or SQL program
    :param program_file: file with YQL or SQL program (optional, if :param program: is None)
    :param files: dict like {'name': '/path'} with extra files
    :param urls: dict like {'name': url} with extra files urls
    :param run_sql: execute sql instead of yql
    :param verbose: log all results and diagnostics
    :param check_error: fail on non-zero exit code
    :param input_tables: list of Table (will be written if not exist)
    :param output_tables: list of Table (will be returned)
    :param pretty_plan: whether to use pretty printing for plan or not
    :param parameters: query parameters as dict like {name: json_value}
    :return: YQLExecResult
    '''

    if input_tables is None:
        input_tables = []
    else:
        assert isinstance(input_tables, list)
    if output_tables is None:
        output_tables = []

    klass.write_tables(input_tables + output_tables)

    res = klass.yql_exec(
        program=program,
        program_file=program_file,
        files=files,
        urls=urls,
        run_sql=run_sql,
        verbose=verbose,
        check_error=check_error,
        tables=(output_tables + input_tables),
        pretty_plan=pretty_plan,
        parameters=parameters
    )

    try:
        res_tables = klass.get_tables(output_tables)
    except Exception:
        if check_error:
            raise
        res_tables = {}

    return res, res_tables


execute_sql = partial(execute, run_sql=True)


def log(s):
    if get_param('STDERR'):
        print(s, file=sys.stderr)
    else:
        logger.debug(s)


def tmpdir_module(request):
    return tempfile.mkdtemp(prefix='kikimr_test_')


@pytest.fixture(name='tmpdir_module', scope='module')
def tmpdir_module_fixture(request):
    return tmpdir_module(request)


def escape_backslash(s):
    return s.replace('\\', '\\\\')


def get_default_mount_point_config_content():
    return '''
        MountPoints {
            RootAlias: '/lib'
            MountPoint: '%s'
            Library: true
        }
    ''' % (
        escape_backslash(yql_source_path('ydb/library/yql/mount/lib'))
    )


def get_mount_config_file(content=None):
    config = yql_output_path('mount.cfg')
    if not os.path.exists(config):
        with open(config, 'w') as f:
            f.write(content or get_default_mount_point_config_content())
    return config


def run_command(program, cmd, tmpdir_module=None, stdin=None,
                check_exit_code=True, env=None, stdout=None):
    if tmpdir_module is None:
        tmpdir_module = tempfile.mkdtemp()

    stdin_stream = None
    if isinstance(stdin, six.string_types):
        with tempfile.NamedTemporaryFile(
                prefix='stdin_',
                dir=tmpdir_module,
                delete=False
        ) as stdin_file:
            stdin_file.write(stdin.encode() if isinstance(stdin, str) else stdin)
        stdin_stream = open(stdin_file.name)
    elif isinstance(stdin, io.IOBase):
        stdin_stream = stdin
    elif stdin is not None:
        assert 0, 'Strange stdin ' + repr(stdin)

    if isinstance(cmd, six.string_types):
        cmd = cmd.split()
    else:
        cmd = [str(c) for c in cmd]
    log(' '.join('\'%s\'' % c if ' ' in c else c for c in cmd))
    cmd = [program] + cmd

    stderr_stream = None
    stdout_stream = None

    if stdout:
        stdout_stream = stdout

    res = yatest.common.execute(
        cmd,
        cwd=tmpdir_module,
        stdin=stdin_stream,
        stdout=stdout_stream,
        stderr=stderr_stream,
        check_exit_code=check_exit_code,
        env=env,
        wait=True
    )

    if res.std_err:
        log(res.std_err)
    if res.std_out:
        log(res.std_out)
    return res


def yson_to_csv(yson_content, columns=None, with_header=True, strict=False):
    import cyson as yson
    if columns:
        headers = sorted(columns)
    else:
        headers = set()
        for item in yson.loads(yson_content, yson_type='list_fragment'):
            headers.update(six.iterkeys(item))
        headers = sorted(headers)
    csv_content = []
    if with_header:
        csv_content.append(';'.join(headers))
    for item in yson.loads(yson_content, yson_type='list_fragment'):
        if strict and sorted(six.iterkeys(item)) != headers:
            return None
        csv_content.append(';'.join([str(item[h]).replace('YsonEntity', '').encode('string_escape') if h in item else '' for h in headers]))
    return '\n'.join(csv_content)


udfs_lock = Lock()


def get_udfs_path(extra_paths=None):
    udfs_build_path = yatest.common.build_path('yql/udfs')
    ydb_udfs_build_path = yatest.common.build_path('ydb/library/yql/udfs')
    contrib_ydb_udfs_build_path = yatest.common.build_path('contrib/ydb/library/yql/udfs')
    rthub_udfs_build_path = yatest.common.build_path('robot/rthub/yql/udfs')
    kwyt_udfs_build_path = yatest.common.build_path('robot/kwyt/yql/udfs')

    try:
        udfs_bin_path = yatest.common.binary_path('yql/udfs')
    except Exception:
        udfs_bin_path = None

    try:
        udfs_project_path = yql_binary_path('yql/library/test_framework/udfs_deps')
    except Exception:
        udfs_project_path = None

    try:
        ydb_udfs_project_path = yql_binary_path('ydb/library/yql/tests/common/test_framework/udfs_deps')
    except Exception:
        ydb_udfs_project_path = None

    merged_udfs_path = yql_output_path('yql_udfs')
    with udfs_lock:
        if not os.path.isdir(merged_udfs_path):
            os.mkdir(merged_udfs_path)

        udfs_paths = [udfs_project_path, ydb_udfs_project_path, udfs_bin_path, udfs_build_path, ydb_udfs_build_path, contrib_ydb_udfs_build_path, rthub_udfs_build_path, kwyt_udfs_build_path]
        if extra_paths is not None:
            udfs_paths += extra_paths

        log('process search UDF in: %s, %s, %s, %s' % (udfs_project_path, ydb_udfs_project_path, udfs_bin_path, udfs_build_path))
        for _udfs_path in udfs_paths:
            if _udfs_path:
                for dirpath, dnames, fnames in os.walk(_udfs_path):
                    for f in fnames:
                        if f.endswith('.so'):
                            f = os.path.join(dirpath, f)
                            if not os.path.exists(f) and os.path.lexists(f):  # seems like broken symlink
                                try:
                                    os.unlink(f)
                                except OSError:
                                    pass
                            link_name = os.path.join(merged_udfs_path, os.path.basename(f))
                            if not os.path.exists(link_name):
                                os.symlink(f, link_name)
                                log('Added UDF: ' + f)
        return merged_udfs_path


def get_test_prefix():
    return 'yql_tmp_' + hashlib.md5(yatest.common.context.test_name).hexdigest()


def normalize_plan_ids(plan, no_detailed=False):
    remapOps = {}

    for node in sorted(filter(lambda n: n["type"] == "in", plan["Basic"]["nodes"]), key=lambda n: n.get("name")):
        if node["id"] not in remapOps:
            remapOps[node["id"]] = len(remapOps) + 1

    for node in plan["Basic"]["nodes"]:
        if node["id"] not in remapOps:
            remapOps[node["id"]] = len(remapOps) + 1

    def subst_basic(y):
        if isinstance(y, list):
            return [subst_basic(i) for i in y]
        if isinstance(y, dict):
            res = {}
            for k, v in six.iteritems(y):
                if k in {'source', 'target', 'id'}:
                    res[k] = remapOps.get(v)
                elif k == "links":
                    res[k] = sorted(subst_basic(v), key=lambda x: (x["source"], x["target"]))
                elif k == "nodes":
                    res[k] = sorted(subst_basic(v), key=lambda x: x["id"])
                else:
                    res[k] = subst_basic(v)
            return res
        return y

    # Sort and normalize input ids
    def subst_detailed(y):
        if isinstance(y, list):
            return [subst_detailed(i) for i in y]
        if isinstance(y, dict):
            res = {}
            for k, v in six.iteritems(y):
                if k == "DependsOn":
                    res[k] = sorted([remapOps.get(i) for i in v])
                elif k == "Providers":
                    res[k] = v
                elif k in {'OperationRoot', 'Id'}:
                    res[k] = remapOps.get(v)
                else:
                    res[k] = subst_detailed(v)
            return res
        return y

    if no_detailed:
        return {"Basic": subst_basic(plan["Basic"])}
    return {"Basic": subst_basic(plan["Basic"]), "Detailed": subst_detailed(plan["Detailed"])}


def normalized_plan_stats(plan):
    renameMap = {
        "MrLMap!": "YtMap!",
        "MrMapReduce!": "YtMapReduce!",
        "MrLReduce!": "YtMapReduce!",
        "MrOrderedReduce!": "YtReduce!",
        "MrSort!": "YtSort!",
        "MrCopy!": "YtCopy!",
        "YtMerge!": "YtCopy!",
        "MrFill!": "YtFill!",
        "MrDrop!": "YtDropTable!",
        "YtTouch!": None,
        "MrReadTable!": None,
        "YtReadTable!": None,
        "MrPublish!": "YtPublish!",
        "MrReadTableScheme!": "YtReadTableScheme!",
    }

    normalizedStat = defaultdict(int)

    for op, stat in six.iteritems(plan["Detailed"]["OperationStats"]):
        renamedOp = renameMap.get(op, op)
        if renamedOp is not None:
            normalizedStat[renamedOp] += stat

    return normalizedStat


def normalize_table_yson(y):
    from cyson import YsonEntity
    if isinstance(y, list):
        return [normalize_table_yson(i) for i in y]
    if isinstance(y, dict):
        normDict = OrderedDict()
        for k, v in sorted(six.iteritems(y), key=lambda x: x[0], reverse=True):
            if k == "_other":
                normDict[normalize_table_yson(k)] = sorted(normalize_table_yson(v))
            elif v != "Void" and v is not None and not isinstance(v, YsonEntity):
                normDict[normalize_table_yson(k)] = normalize_table_yson(v)
        return normDict
    return y


def dump_table_yson(res_yson, sort=True):
    rows = normalize_table_yson(cyson.loads('[' + res_yson + ']'))
    if sort:
        rows = sorted(rows)
    return cyson.dumps(rows, format="pretty")


def normalize_source_code_path(s):
    # remove contrib/
    s = re.sub(r'\b(contrib/)(ydb/library/yql.*)', r'\2', s)
    # replace line number in source code with 'xxx'
    s = re.sub(r'\b(yql/[\w/]+(?:\.cpp|\.h)):(?:\d+)', r'\1:xxx', s)
    return re.sub(r'(/lib/yql/[\w/]+(?:\.yql|\.sql)):(?:\d+):(?:\d+)', r'\1:xxx:yyy', s)


def do_get_files(suite, config, DATA_PATH, config_key):
    files = dict()
    suite_dir = os.path.join(DATA_PATH, suite)
    res_dir = None
    for line in config:
        if line[0] == config_key:
            _, name, path = line
            userpath = find_user_file(suite, path, DATA_PATH)
            relpath = os.path.relpath(userpath, suite_dir)
            if os.path.exists(os.path.join('cwd', relpath)):
                path = relpath
            else:
                path = userpath

            if not res_dir:
                res_dir = get_yql_dir('file_')

            new_path = os.path.join(res_dir, os.path.basename(path))
            shutil.copyfile(path, new_path)

            files[name] = new_path

    return files


def get_files(suite, config, DATA_PATH):
    return do_get_files(suite, config, DATA_PATH, 'file')


def get_http_files(suite, config, DATA_PATH):
    return do_get_files(suite, config, DATA_PATH, 'http_file')


def get_yt_files(suite, config, DATA_PATH):
    return do_get_files(suite, config, DATA_PATH, 'yt_file')


def get_syntax_version(program):
    syntax_version_param = get_param('SYNTAX_VERSION')
    default_syntax_version = 1
    if 'syntax version 0' in program:
        return 0
    elif 'syntax version 1' in program:
        return 1
    elif syntax_version_param:
        return int(syntax_version_param)
    else:
        return default_syntax_version


def ansi_lexer_enabled(program):
    return 'ansi_lexer' in program


def pytest_get_current_part(path):
    folder = os.path.dirname(path)
    folder_name = os.path.basename(folder)
    assert folder_name.startswith('part'), "Current folder is {}".format(folder_name)
    current = int(folder_name[len('part'):])

    parent = os.path.dirname(folder)
    maxpart = max([int(part[len('part'):]) if part.startswith('part') else -1 for part in os.listdir(parent)])
    assert maxpart > 0, "Cannot find parts in {}".format(parent)
    return (current, 1 + maxpart)


def normalize_result(res, sort):
    res = cyson.loads(res) if res else cyson.loads("[]")
    res = replace_vals(res)
    for r in res:
        for data in r['Write']:
            if sort and 'Data' in data:
                data['Data'] = sorted(data['Data'])
            if 'Ref' in data:
                data['Ref'] = []
                data['Truncated'] = True
            if 'Data' in data and len(data['Data']) == 0:
                del data['Data']
    return res


def stable_write(writer, node):
    if hasattr(node, 'attributes'):
        writer.begin_attributes()
        for k in sorted(node.attributes.keys()):
            writer.key(k)
            stable_write(writer, node.attributes[k])
        writer.end_attributes()
    if isinstance(node, list):
        writer.begin_list()
        for r in node:
            stable_write(writer, r)
        writer.end_list()
        return
    if isinstance(node, dict):
        writer.begin_map()
        for k in sorted(node.keys()):
            writer.key(k)
            stable_write(writer, node[k])
        writer.end_map()
        return
    writer.write(node)


def stable_result_file(res):
    path = res.results_file
    assert os.path.exists(path)
    with open(path) as f:
        res = f.read()
    res = cyson.loads(res)
    res = replace_vals(res)
    for r in res:
        for data in r['Write']:
            if 'Unordered' in r and 'Data' in data:
                data['Data'] = sorted(data['Data'])
    with open(path, 'w') as f:
        writer = cyson.Writer(stream=cyson.OutputStream.from_file(f), format='pretty', mode='node')
        writer.begin_stream()
        stable_write(writer, res)
        writer.end_stream()
    with open(path) as f:
        return f.read()


def stable_table_file(table):
    path = table.file
    assert os.path.exists(path)
    assert table.attr is not None
    is_sorted = False
    for column in cyson.loads(table.attr)['schema']:
        if 'sort_order' in column:
            is_sorted = True
            break
    if not is_sorted:
        with open(path) as f:
            r = cyson.Reader(cyson.InputStream.from_file(f), mode='list_fragment')
            lst = sorted(list(r.list_fragments()))
        with open(path, 'w') as f:
            writer = cyson.Writer(stream=cyson.OutputStream.from_file(f), format='pretty', mode='list_fragment')
            writer.begin_stream()
            for r in lst:
                stable_write(writer, r)
            writer.end_stream()
    with open(path) as f:
        return f.read()


class LoggingDowngrade(object):

    def __init__(self, loggers, level=logging.CRITICAL):
        self.loggers = [(name, logging.getLogger(name).getEffectiveLevel()) for name in loggers]
        self.level = level

    def __enter__(self):
        self.prev_levels = []
        for name, _ in self.loggers:
            log = logging.getLogger(name)
            log.setLevel(self.level)
        return self

    def __exit__(self, exc_type, exc_value, tb):
        for name, level in self.loggers:
            log = logging.getLogger(name)
            log.setLevel(level)
        return True
