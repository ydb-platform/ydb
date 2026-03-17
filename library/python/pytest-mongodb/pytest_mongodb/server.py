# -*- coding: utf-8 -*-
import json
import logging
import os
import retry
import shutil
import signal
import time
import yatest.common
from functools import wraps

logger = logging.getLogger(__name__)


class MongoNotStarted(Exception):
    pass


class MongoNotRunning(Exception):
    pass


class MongoDBServer(object):

    def __init__(self, config):
        self.config = config
        self.is_running = None

        self.mongod_local_path = os.path.join(self.config.work_path, 'mongodb/bin/mongod')
        self.mongo_local_path = os.path.join(self.config.work_path, 'mongodb/bin/mongo')
        self._prepare_files()

    def _cmd(self, *cmd):
        # non-shell execution may be cause of modified logpath: "<cwd>'<logs_path>'"
        shell_cmd = ' '.join(cmd)
        logger.debug('Running command: `%s`', shell_cmd)
        return yatest.common.process.execute(
            shell_cmd, cwd=self.config.work_dir, shell=True
        )

    def _mongo(self, *cmd):
        return self._cmd(
            self.mongo_local_path,
            *cmd
        )

    def _mongod(self, *cmd):
        return self._cmd(
            self.mongod_local_path,
            *cmd
        )

    def _prepare_files(self):
        if os.path.exists(self.config.work_dir):
            shutil.rmtree(self.config.work_dir)
        os.makedirs(self.config.work_dir)
        if os.path.exists(self.config.db_path):
            shutil.rmtree(self.config.db_path)
        os.makedirs(self.config.db_path)

    def _timing(method):
        @wraps(method)
        def wrap(self, *args, **kwargs):
            start_time = time.time()
            ret = method(self, *args, **kwargs)
            finish_time = time.time()
            logger.debug("%s time: %s", method.__name__, finish_time - start_time)
            return ret
        return wrap

    def _check_running(self):
        try:
            mongoshell = self._mongo(
                "--port", str(self.config.port),
                "--eval", "'JSON.stringify(db.isMaster())'",
                "--quiet",
            )
            is_master = json.loads(mongoshell.std_out)
        except Exception as e:
            logger.error(
                "Errors while checking status of mongodb: %s", str(e), exc_info=True
            )
            with open(self.config.log_path, 'r') as f:
                lines = f.readlines()[-100:]
            logger.info('Mongo last 100 lines of logs: %s', ''.join(lines))
            raise
        if is_master.get("ismaster") is not True:
            raise MongoNotRunning()

    def _start_local(self):
        try:
            self._mongod(
                "--port", str(self.config.port),
                "--dbpath", "'{}'".format(self.config.db_path),
                "--pidfilepath", "'{}'".format(self.config.pid_file),
                "--logpath", "'{}'".format(self.config.log_path),
                "--fork",
                '--nounixsocket',
            )
            retry.retry_call(
                self._check_running,
                tries=3,
                delay=3,
                logger=logger,
                exceptions=MongoNotRunning,
            )

        except Exception as e:
            logger.error("Failed to start local mongod: %s", str(e), exc_info=True)
            self._kill()
            raise MongoNotStarted()

        self.is_running = True

    def _kill(self):
        for pid in self._get_mongo_pids():
            try:
                os.kill(pid, signal.SIGKILL)
            except OSError:
                pass

    def _shutdown_mongod(self):
        try:
            self._kill()
            self.is_running = False
        except Exception as e:
            logger.error("Errors while stopping local Mongodb: %s", str(e), exc_info=True)
            raise

    def _get_mongo_pids(self):
        try:
            with open(self.config.pid_file, 'r') as pids:
                return [int(pid) for pid in pids]
        except IOError:  # pid file does not exists
            logging.warning('PID file does not exists')
            return []

    @_timing
    def start_local(self):
        retry.retry_call(
            self._start_local,
            tries=3,
            delay=3,
            max_delay=60,
            jitter=5,
            logger=logger,
            exceptions=MongoNotStarted,
        )

    @_timing
    def stop_local(self):
        if self.is_running:
            self._shutdown_mongod()
        if not self.config.keep_work_dir:
            shutil.rmtree(self.config.work_dir, ignore_errors=True)
