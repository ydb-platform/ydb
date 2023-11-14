import argparse
import errno
import logging
import os
import tarfile
import tempfile
import time
import glob
import platform

import yatest.common
from library.python.testing.recipe import declare_recipe, set_env
from library.python.testing.recipe.ports import get_port_range, release_port_range

# copy-paste from yql/library/postgresql/recipe

POSTGRES_TGZ = 'postgres.tgz'


def wait(cmd, process):
    res = process.communicate()
    code = process.poll()

    if code != 0:
        raise RuntimeError("cmd failed: %s, code: %s, stdout/stderr: %s / %s", (cmd, code, res[0], res[1]))

    logging.debug(res[1])


class PG:
    def __init__(self, install_dir):

        self.bin_dir = os.path.join(install_dir, "bin")

        lib_dir = os.path.join(install_dir, "lib")
        self.env = os.environ.copy()
        if 'LD_LIBRARY_PATH' in self.env:
            self.env['LD_LIBRARY_PATH'] += ':' + lib_dir
        else:
            self.env['LD_LIBRARY_PATH'] = lib_dir
        self.env['LANG'] = 'en_US.UTF-8'
        self.env['LC_MESSAGES'] = 'en_US.UTF-8'

    def run(self, dbdir, port, user, dbname, max_conn):
        self.port = port
        self.user = user
        self.dbname = dbname
        self.dbdir = dbdir
        self.max_conn = max_conn

        self._init_db()
        self._run()
        self._create_db()

    def migrate(self, migrations_dir):
        logging.info("migrations dir: " + migrations_dir)
        files = sorted(glob.glob(migrations_dir + "/*.sql"))
        for fname in files:
            self._execute_sql_file(fname)

    def get_pid(self):
        return self.handle.process.pid

    def _init_db(self):
        logging.info("Executing initdb...")
        initdb_path = os.path.join(self.bin_dir, 'initdb')
        cmd = [initdb_path, '-A trust', '-D', self.dbdir , '-U', self.user]
        yatest.common.execute(cmd, env=self.env)
        logging.info("Database initiated")

    def _run(self):
        logging.info("Running postgres...")
        postgres_path = os.path.join(self.bin_dir, 'postgres')
        cmd = [postgres_path, '-D', self.dbdir, '--port={}'.format(self.port), '-N{}'.format(self.max_conn), '-c', 'timezone=UTC']
        self.handle = yatest.common.execute(cmd, env=self.env, wait=False)
        logging.info("Postgres is running on port {} with pid {}".format(self.port, self.get_pid()))

    def _create_db(self):
        logging.info("Creating the database...")
        createdb_path = os.path.join(self.bin_dir, 'createdb')

        locale = "en_US.utf8"
        if platform.system() == "Darwin":
            locale = "en_US.UTF-8"

        # Try 10 times, since postgres might not be initialized and accepting connections yet
        ex = None
        for attempt in range(10):
            cmd = [createdb_path,
                   '-p', str(self.port),
                   '-U', self.user, self.dbname,
                   '-E', 'UTF8', f'--locale={locale}', '--template=template0']
            ex = yatest.common.execute(cmd, check_exit_code=False, env=self.env)
            if ex.returncode == 0:
                logging.info("Database created")
                return
            logging.warn("Database creation failed (attempt {}): {}".format(attempt, ex.stderr))
            time.sleep(1)
        raise Exception(f"cannot create Database, {ex.stderr}")

    def _execute_sql_file(self, file_name):
        logging.info('Executing {}...'.format(file_name))
        psql_path = os.path.join(self.bin_dir, 'psql')
        cmd = [psql_path, '-p', str(self.port), '-U', self.user, '-q', '-A', '-d', self.dbname, '-f', file_name]
        yatest.common.execute(cmd, env=self.env)
        logging.info('Success')


def start(argv):
    logging.info("Start postgresql")

    def parse_argv(argv):
        parser = argparse.ArgumentParser()
        parser.add_argument('--archive', metavar='<file>', type=str,
                            help='postgres.tgz path',
                            default=POSTGRES_TGZ)
        parser.add_argument('-s', '--schema-migrations-dir', type=str, help='directory with DB migrations to run for each schema')
        parser.add_argument('-m', '--migrations-dir', type=str, help='directory with DB migrations run')
        parser.add_argument('-n', '--max_connections', type=int, default=20, help='set max number of connections to DB')
        return parser.parse_args(argv)

    def parse_dir(arg):
        return [yatest.common.source_path(d.strip()) for d in arg.split(',') if d.strip()]

    args = parse_argv(argv)

    postgres_dir = tempfile.mkdtemp(prefix='postgres_')
    logging.info("Postgresql dir:" + postgres_dir)

    working_dir = os.path.join(postgres_dir, 'wd')
    os.mkdir(working_dir)
    logging.info("Working dir:" + working_dir)

    tgz = tarfile.open(args.archive)
    tgz.extractall(path=postgres_dir)

    port = get_port_range()
    dbname = 'test-db'
    username = 'test'

    pg = PG(os.path.join(postgres_dir, 'postgres'))
    pg.run(dbdir=working_dir, port=port, dbname=dbname, user=username, max_conn=args.max_connections)
    if args.schema_migrations_dir:
        set_env("PG_MIGRATIONS_DIR", ','.join(parse_dir(args.schema_migrations_dir)))

    if args.migrations_dir:
        for dir in parse_dir(args.migrations_dir):
            pg.migrate(dir)

    with open("postgres_recipe.pid", "w") as pid_file:
        pid_file.write(str(pg.get_pid()))

    set_env("POSTGRES_RECIPE_HOST", 'localhost')
    set_env("POSTGRES_RECIPE_PORT", str(port))
    set_env("POSTGRES_RECIPE_DBNAME", dbname)
    set_env("POSTGRES_RECIPE_USER", username)
    set_env("POSTGRES_RECIPE_MAX_CONNECTIONS", str(args.max_connections))
    set_env("POSTGRES_RECIPE_BIN_DIR", pg.bin_dir)


def is_running(pid):
    try:
        os.kill(pid, 0)
    except OSError as err:
        if err.errno == errno.ESRCH:
            return False
    return True


def stop(argv):
    if len(yatest.common.get_param("hang_with_pg", "")):
        while True:
            continue
    release_port_range()
    logging.info("Stop postgresql")
    with open("postgres_recipe.pid", "r") as f:
        pid = int(f.read())

    os.kill(pid, 15)

    _SHUTDOWN_TIMEOUT = 10
    seconds = _SHUTDOWN_TIMEOUT
    while is_running(pid) and seconds > 0:
        time.sleep(1)
    seconds -= 1

    if is_running(pid):
        logging.error('postgres is still running after %d seconds' % seconds)
        os.kill(pid, 9)

    if is_running(pid):
        logging.error("postgres failed to shutdown after kill 9!")


if __name__ == "__main__":
    declare_recipe(start, stop)
