import concurrent.futures
import logging
import os
import time
import uuid
import ydb
import tempfile
from library.python import resource
import subprocess

import stat


class Workload:
    def __init__(self, endpoint, database, duration, sqs_endpoint):
        self.driver = ydb.Driver(ydb.DriverConfig(endpoint, database))
        self.database = database
        self.endpoint = endpoint
        self.sqs_endpoint = sqs_endpoint
        self.duration = duration
        self.id = f"{uuid.uuid1()}".replace("-", "_")
        self.topic_name = f"topic_{self.id}"
        self.dlq_topic_name = f"dlq_topic_{self.id}"
        self._unpack_resource('ydb_cli')

    def _unpack_resource(self, name):
        self.tempdir = tempfile.TemporaryDirectory(dir=os.getcwd())
        self.working_dir = os.path.join(self.tempdir.name, "mixed_ydb_cli")
        os.makedirs(self.working_dir, exist_ok=True)
        res = resource.find(name)
        path_to_unpack = os.path.join(self.working_dir, name)
        with open(path_to_unpack, "wb") as f:
            f.write(res)

        st = os.stat(path_to_unpack)
        os.chmod(path_to_unpack, st.st_mode | stat.S_IEXEC)
        self.cli_path = path_to_unpack

    def cmd_run(self, cmd):
        logging.debug(f"Running cmd {cmd}")
        print(f"Running cmd {cmd} at {time.time()}")
        r = subprocess.run(cmd, check=True, text=True)
        print(f"End at {time.time()}")
        return r

    def create_topic(self):
        with ydb.QuerySessionPool(self.driver) as session_pool:
            session_pool.execute_with_retries(f"""
                    CREATE TOPIC `{self.dlq_topic_name}`;
                """)
            session_pool.execute_with_retries(f"""
                    CREATE TOPIC `{self.topic_name}`
                      (CONSUMER `shared_consumer` 
                        WITH (
                          type='shared',
                          keep_messages_order = true,
                          default_processing_timeout = Interval('PT5S'),
                          max_processing_attempts = 1,
                          dead_letter_policy = 'move',
                          dead_letter_queue = '{self.dlq_topic_name}'
                        )
                ) """)

    def get_command(self, subcmds: list[str]) -> list[str]:
        return (
            [
                self.cli_path,
                "-vvv",
                "--verbose",
                "--endpoint", self.endpoint,
                "--database", self.database,
                "workload",
                "sqs",
                "run",
            ]
            + subcmds
        )
    def write_to_topic(self, duration):
        logging.info(f"Writing to topic for {duration} seconds. SQS endpoint: {self.sqs_endpoint}")
        subcmds = [
            'write',
            '-s', str(duration),
            '--workers', '50',
            '--queue-endpoint',  self.sqs_endpoint,
            '--queue-url', '/v1/' + str(len(self.database)) + '/' + self.database + '/' + str(len(self.topic_name)) + '/' + self.topic_name + '/15/shared_consumer',
            '--percentile', '99',
            '--message-groups-amount', '0',
            '--max-unique-messages', '0',
        ]
        self.cmd_run(self.get_command(subcmds=subcmds))

    def read_from_topic(self):
        pass

    def loop(self):
        self.create_topic()

        with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
            logging.info("Starting workload")
            runners = [
                executor.submit(self.write_to_topic, duration=self.duration),
            ]

            logging.info("Waiting for workload task")

            for future in concurrent.futures.as_completed(runners):
                try:
                    future.result()
                    logging.info("Workload task completed")
                except Exception:
                    logging.exception("Workload task failed")
                    pass

            logging.info("Checking results")
            for runner in runners:
                runner.result()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.driver.stop()
        self.tempdir.cleanup()