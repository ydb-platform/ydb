import bz2
import getopt
import glob
import gzip
import logging
import os
import re
import requests
import shutil
import tempfile
import subprocess as sp
import tarfile
import yatest.common as yat

from library.python.testing.recipe import set_env
from library.recipes.common import find_free_ports
from library.recipes.common import start_daemon

ZK_RECIPE_ENVIRONMENT_VAR = 'RECIPE_ZOOKEEPER_HOST'

CLICKHOUSE_RESOURCE_PATH = 'clickhouse/resource.tar.gz'
CLICKHOUSE_EXECUTABLE_PATH = 'contrib/clickhouse/programs/clickhouse'

CLICKHOUSE_CONFIG_DEFAULT = 'library/recipes/clickhouse/recipe/config/config_default.xml'
CLICKHOUSE_ZK_CONFIG_DEFAULT = 'library/recipes/clickhouse/recipe/config/config_with_zookeeper.xml'
CLICKHOUSE_REGIONS_CONFIG_DEFAULT = 'library/recipes/clickhouse/recipe/config/config_with_regions.xml'

CLICKHOUSE_DIR = '{}clickhouse'
CLICKHOUSE_CONFIG_DIR = '{}clickhouse/config'
CLICKHOUSE_REGIONS_FILES_DIR = '{}clickhouse/config/regions'
CLICKHOUSE_REGIONS_FILE_HIERARCHY = '{}clickhouse/config/regions/regions_hierarchy.txt'
CLICKHOUSE_TMP_DIR = '{}clickhouse/tmp'
CLICKHOUSE_USER_FILES_DIR = '{}clickhouse/user_files'
CLICKHOUSE_FORMAT_SCHEMA_DIR = '{}clickhouse/format_schema'
CLICKHOUSE_SERVER_LOG = '{}clickhouse-server.log'
CLICKHOUSE_SERVER_PID_FILE = '{}recipe.clickhouse.pid'

TABLE_NAME_RE = re.compile(r'^\w+(\.\w+)?')
PLACEHOLDER_RE = re.compile(r'\${(.*?)}')

BLOCK_SIZE = 8 * 1024

logger = logging.getLogger('clickhouse.recipe')


def start(argv):
    opts = _parse_argv(argv)
    prefix = opts["prefix"] if "prefix" in opts else ""

    clickhouse = _find_clickhouse_executable(prefix)

    http_port, native_port, interserver_port = find_free_ports(3)

    environment = _update_environment(http_port, native_port, interserver_port, prefix, clickhouse)

    config = _prepare_clickhouse_config(opts)

    logger.info('Start ClickHouse. Http port: %d, native port: %d, config: %s', http_port, native_port, config)

    start_daemon(
        command=[clickhouse, 'server', '--config', config],
        environment=environment,
        is_alive_check=lambda: _is_alive(http_port),
        pid_file_name=yat.work_path(CLICKHOUSE_SERVER_PID_FILE.format(prefix)),
    )

    logger.info('ClickHouse started.')

    if 'execute' in opts:
        _execute_queries(clickhouse, native_port, opts['execute'], environment, 'expand-vars' in opts)

    if 'execute-file' in opts:
        _execute_queries_file(clickhouse, native_port, opts['execute-file'])

    if 'insert-csv' in opts:
        _execute_insert_csv(clickhouse, native_port, opts['insert-csv'])


def stop(argv):
    opts = _parse_argv(argv)
    prefix = opts["prefix"] if "prefix" in opts else ""
    _terminate_clickhouse(prefix)


def _error(message, *args):
    logger.error(message, args)
    raise RuntimeError(message % args)


def _find_clickhouse_executable(prefix):
    clickhouse_resource = yat.work_path(CLICKHOUSE_RESOURCE_PATH)
    if os.path.isfile(clickhouse_resource):
        clickhouse_dir = prefix + os.path.dirname(clickhouse_resource)
        with tarfile.open(clickhouse_resource, 'r') as resource:
            resource.extractall(clickhouse_dir)

        clickhouse = os.path.join(clickhouse_dir, 'clickhouse')
    else:
        clickhouse = yat.binary_path(CLICKHOUSE_EXECUTABLE_PATH)

    if os.path.isfile(clickhouse):
        logger.info('ClickHouse executable: %s', clickhouse)
        return clickhouse

    _error('Can not find ClickHouse executable!')


def _parse_argv(argv):
    opts, argv = getopt.getopt(
        argv, '', ['config=', 'execute=', 'execute-file=', 'insert-csv=', 'expand-vars', 'prefix=']
    )
    opt_dict = {}
    for opt, arg in opts:
        if opt == '--config':
            opt_dict['config'] = arg
        elif opt == '--execute':
            opt_dict.setdefault('execute', []).extend(_collect_files(opt, arg))
        elif opt == '--execute-file':
            opt_dict.setdefault('execute-file', []).extend(_collect_files(opt, arg))
        elif opt == '--insert-csv':
            opt_dict.setdefault('insert-csv', []).extend(_collect_files(opt, arg))
        elif opt == '--expand-vars':
            opt_dict['expand-vars'] = True
        elif opt == '--prefix':
            opt_dict['prefix'] = arg

    return opt_dict


def _collect_files(opt, glob_path):
    files = glob.glob(yat.work_path(glob_path))
    if len(files) > 0:
        return files
    files = glob.glob(yat.source_path(glob_path))
    if len(files) > 0:
        return files

    _error('File(s) from %s \'%s\' option not found!', opt, glob_path)


def _update_environment(http_port, native_port, interserver_port, prefix, clickhouse_bin):
    variables = {
        'RECIPE_CLICKHOUSE_HOST': '127.0.0.1',
        'RECIPE_CLICKHOUSE_HTTP_PORT': str(http_port),
        'RECIPE_CLICKHOUSE_NATIVE_PORT': str(native_port),
        'RECIPE_CLICKHOUSE_INTERSERVER_PORT': str(interserver_port),
        'RECIPE_CLICKHOUSE_USER': 'default',
        'RECIPE_CLICKHOUSE_PASSWORD': '',
        'RECIPE_CLICKHOUSE_LOG': yat.output_path(CLICKHOUSE_SERVER_LOG.format(prefix)),
        "RECIPE_CLICKHOUSE_DIR": yat.work_path(CLICKHOUSE_DIR.format(prefix)),
        "RECIPE_CLICKHOUSE_BIN": clickhouse_bin,
        "RECIPE_CLICKHOUSE_TMP_DIR": yat.work_path(CLICKHOUSE_TMP_DIR.format(prefix)),
        "RECIPE_CLICKHOUSE_REGIONS_FILE_HIERARCHY": yat.work_path(CLICKHOUSE_REGIONS_FILE_HIERARCHY.format(prefix)),
        "RECIPE_CLICKHOUSE_REGIONS_FILES_DIR": yat.work_path(CLICKHOUSE_REGIONS_FILES_DIR.format(prefix)),
        "RECIPE_CLICKHOUSE_USER_FILES_DIR": yat.work_path(CLICKHOUSE_USER_FILES_DIR.format(prefix)),
        "RECIPE_CLICKHOUSE_FORMAT_SCHEMA_DIR": yat.work_path(CLICKHOUSE_FORMAT_SCHEMA_DIR.format(prefix)),
    }

    for variable in variables.items():
        (k, v) = variable
        k = prefix + k
        set_env(k, v)

    environment = os.environ.copy()
    environment.update(variables)

    return environment


def _prepare_clickhouse_config(opts):
    config = yat.source_path(opts.get('config', _get_clickhouse_default_config()))
    config_dir = os.path.dirname(config)

    prefix = opts["prefix"] if "prefix" in opts else ""
    work_config_dir = yat.work_path(CLICKHOUSE_CONFIG_DIR.format(prefix))

    logger.info('Copy ClickHouse config files from: \'%s\' to: \'%s\'', config_dir, work_config_dir)
    shutil.copytree(config_dir, work_config_dir)

    logger.info('Config path: ' + config)

    return os.path.join(work_config_dir, os.path.basename(config))


def _get_clickhouse_default_config():
    if _is_zk_enabled():
        return CLICKHOUSE_ZK_CONFIG_DEFAULT
    else:
        return CLICKHOUSE_CONFIG_DEFAULT


def _is_zk_enabled():
    return ZK_RECIPE_ENVIRONMENT_VAR in os.environ


def _is_alive(port):
    try:
        response = requests.get('http://localhost:{}'.format(port), timeout=1)
        response.raise_for_status()
    except Exception as err:
        logger.debug('ClickHouse port check result: ' + str(err))
        return False
    else:
        logger.info('ClickHouse is up with http port {}'.format(port))
        return True


def _execute_queries(clickhouse, port, query_paths, environment, expand_vars):
    logger.info('Executing queries...')

    args = ['--multiline', '--multiquery']
    for query_path in query_paths:
        logger.info('Executing queries from: ' + query_path)
        if expand_vars:
            tmp = tempfile.NamedTemporaryFile(delete=False)
            logger.info('Expanding variables to tmp file: %', tmp.name)
            try:
                tmp_path = _expand_vars(query_path, tmp, environment)
                _execute_clickhouse_client(clickhouse, port, args, tmp_path)
            finally:
                tmp.close()
                os.unlink(tmp.name)
        else:
            _execute_clickhouse_client(clickhouse, port, args, query_path)

    logger.info('All query files processed.')


def _execute_queries_file(clickhouse, port, queries_files):
    logger.info('Executing queries...')

    for queries_file in queries_files:
        logger.info('Executing queries from file: ' + queries_file)
        args = ['--queries-file', queries_file]
        _execute_clickhouse_client(clickhouse, port, args)

    logger.info('All query files processed.')


def _expand_vars(source, tmp, environment):
    with open(source) as original:
        for line in original:
            tmp.write(re.sub(PLACEHOLDER_RE, lambda match: environment[match.group(1)], line))
        tmp.flush()
    return tmp.name


def _execute_insert_csv(clickhouse, port, csv_paths):
    logger.info('Inserting data from CSV files...')

    for csv_path in csv_paths:
        csv_file = os.path.basename(csv_path)
        match = TABLE_NAME_RE.match(csv_file)
        table_name = match.group(0) if match else csv_file

        args = ['--query=INSERT INTO {} FORMAT CSV'.format(table_name)]
        logger.info('Insert data into \'%s\' from CSV file: %s', table_name, csv_path)
        _execute_clickhouse_client(clickhouse, port, args, csv_path)

    logger.info('All data are inserted.')


def _execute_clickhouse_client(clickhouse, port, args, input_file=None):
    command = [clickhouse, 'client', '--port', str(port)]
    command.extend(args)

    if input_file:
        if input_file.endswith('.gz'):
            with gzip.open(input_file, 'r') as reader:
                _execute(command, reader=reader)
        elif input_file.endswith('.bz2'):
            with bz2.BZ2File(input_file, 'r') as reader:
                _execute(command, reader=reader)
        else:
            with open(input_file, 'r') as fin:
                _execute(command, stdin=fin)
    else:
        _execute(command)


def _execute(command, stdin=None, reader=None):
    if stdin:
        yat.execute(command, stdin=stdin)
    elif reader:
        executor = yat.execute(command, stdin=sp.PIPE, wait=False)
        process_input = executor.process.stdin
        chunk = reader.read(BLOCK_SIZE)
        while chunk:
            process_input.write(chunk)
            chunk = reader.read(BLOCK_SIZE)
        process_input.close()
        executor.wait()
    else:
        yat.execute(command)


def _terminate_clickhouse(prefix):
    logger.info('Terminating server...')
    try:
        with open(yat.work_path(CLICKHOUSE_SERVER_PID_FILE.format(prefix))) as fin:
            pid = fin.read()
    except IOError:
        logger.warn('Can not find server PID.')
    else:
        logger.info('Terminate ClickHouse server PID: %s', pid)
        os.kill(int(pid), 9)
        logger.info('Server terminated.')
