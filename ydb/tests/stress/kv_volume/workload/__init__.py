import logging
from datetime import datetime, timedelta
import random
from collections import defaultdict, deque
from itertools import chain
import uuid
import asyncio

from ydb.public.api.protos import ydb_status_codes_pb2 as ydb_status_codes


from ydb.tests.stress.common.common import WorkloadBase
from ydb.tests.library.clients.kikimr_keyvalue_client import KeyValueClient

logger = logging.getLogger("YdbKvWorkload")

DEFAULT_YDB_KV_PORT = 2135


def parse_int_with_default(s, default=None):
    try:
        return int(s)
    except ValueError:
        return default
    except TypeError:
        return default


def parse_endpoint(endpoint):
    # Normalize and split endpoint into host and port (strip scheme if present)
    hostport = endpoint.split('://', 1)[1] if '://' in endpoint else endpoint
    parts = hostport.rsplit(':', 1)  # Split from end, max 1 time
    host = parts[0]
    s_port = parts[1] if len(parts) > 1 else None
    return host, parse_int_with_default(s_port, DEFAULT_YDB_KV_PORT)


class Worker:
    class ActionRunner:
        def __init__(self, config, workload, worker_semaphore=None, global_semaphore=None):
            self.config = config
            self.workload = workload
            self.task = None
            self.completed_actions = set()
            self.worker_semaphore = worker_semaphore
            self.global_semaphore = global_semaphore
            
        async def execute_command(self, cmd, data_context=None):
            # TODO(kruall): rewrite all logic
            cmd_type = cmd.WhichOneof('Command')
            if cmd_type == 'print':
                print(f"[Worker Action: {self.config.name}] {cmd.print.msg}")
            elif cmd_type == 'read':
                partition_id = random.randrange(0, self.workload.partition_count)
                key = self.workload._get_init_pair_key(random.randint(0, 63))
                offset = 0
                if cmd.read.size != self.workload.INIT_DATA_VALUE_SIZE:
                    offset = random.randint(0, self.workload.INIT_DATA_VALUE_SIZE - cmd.read.size)
                response = await self.workload.client.a_kv_read(
                    self.workload._volume_path(),
                    partition_id,
                    key,
                    offset=offset,
                    size=cmd.read.size,
                    version=self.workload.version
                )
                if response is None or self.workload.get_status(response) != ydb_status_codes.StatusIds.StatusCode.SUCCESS:
                    logger.error(f"Read failed for action {self.config.name}")
            elif cmd_type == 'write':
                partition_id = random.randrange(0, self.workload.partition_count)
                key = self.workload._get_init_pair_key(random.randint(0, 63))
                pattern_repeats = (cmd.write.size + len(self.workload.INIT_DATA_PATTERN)) // len(self.workload.INIT_DATA_PATTERN)
                data = self.workload.INIT_DATA_PATTERN * pattern_repeats
                data = data[:cmd.write.size]
                response = await self.workload.client.a_kv_write(
                    self.workload._volume_path(),
                    partition_id,
                    key,
                    data,
                    channel=cmd.write.channel,
                    version=self.workload.version
                )
                if response is None or self.workload.get_status(response) != ydb_status_codes.StatusIds.StatusCode.SUCCESS:
                    logger.error(f"Write failed for action {self.config.name}")
        
        async def run_commands(self):
            if self.worker_semaphore:
                async with self.worker_semaphore:
                    if self.global_semaphore:
                        async with self.global_semaphore:
                            data_context = {}
                            for cmd in self.config.action_command:
                                await self.execute_command(cmd, data_context)
                    else:
                        data_context = {}
                        for cmd in self.config.action_command:
                            await self.execute_command(cmd, data_context)
            elif self.global_semaphore:
                async with self.global_semaphore:
                    data_context = {}
                    for cmd in self.config.action_command:
                        await self.execute_command(cmd, data_context)
            else:
                data_context = {}
                for cmd in self.config.action_command:
                    await self.execute_command(cmd, data_context)
        
        async def run_periodic(self, end_time):
            period = self.config.period_us / 1000000.0
            while datetime.now() < end_time:
                await self.run_commands()
                await asyncio.sleep(period)
        
        async def run_once(self):
            await self.run_commands()
    
    def __init__(self, worker_id, workload):
        self.worker_id = worker_id
        self.workload = workload
        self.actions = {}
        self.runners = {}
        self.global_semaphores = {}
        
    def add_action(self, action_config):
        worker_sem = None
        if action_config.HasField('worker_max_in_flight'):
            worker_sem = asyncio.Semaphore(action_config.worker_max_in_flight)
        
        global_sem = None
        if action_config.HasField('global_max_in_flight'):
            if action_config.name not in self.global_semaphores:
                self.global_semaphores[action_config.name] = asyncio.Semaphore(action_config.global_max_in_flight)
            global_sem = self.global_semaphores[action_config.name]
        
        runner = self.ActionRunner(action_config, self.workload, worker_sem, global_sem)
        self.actions[action_config.name] = action_config
        self.runners[action_config.name] = runner
        
    async def arun(self):
        end_time = datetime.now() + timedelta(seconds=self.workload.duration)
        
        task_map = {}
        
        for name, action in self.actions.items():
            parent = action.parent_action if action.HasField('parent_action') else None
            if parent is None:
                if action.HasField('period_us'):
                    task_map[name] = asyncio.create_task(self.runners[name].run_periodic(end_time))
                else:
                    task_map[name] = asyncio.create_task(self.runners[name].run_once())
        
        for name, action in self.actions.items():
            parent = action.parent_action if action.HasField('parent_action') else None
            if parent is not None and parent in task_map:
                async def wait_and_run(parent_task=task_map[parent], runner=self.runners[name]):
                    await parent_task
                    if action.HasField('period_us'):
                        await runner.run_periodic(end_time)
                    else:
                        await runner.run_once()
                task_map[name] = asyncio.create_task(wait_and_run())
        
        if task_map:
            await asyncio.gather(*task_map.values())
    
    def run(self):
        asyncio.run(self.arun())

class WorkerBuilder:
    def __init__(self, config, workload):
        self.config = config
        self.workload = workload
        
    def build(self, worker_id):
        worker = Worker(worker_id, self.workload)
        for action in self.config.actions:
            worker.add_action(action)
        return worker
        


class YdbKeyValueVolumeWorkload(WorkloadBase):
    INIT_DATA_PAIRS = 64
    INIT_INLINE_DATA_PAIRS = 8
    INIT_DATA_VALUE_SIZE = 2 * 1024 * 1024
    INIT_DATA_PATTERN = '0123456789ABCDEF'

    def __init__(self, endpoint, database, duration, kv_load_type, worker_count, version, config):
        super().__init__(None, '', 'kv_volume', None)
        fqdn, port = parse_endpoint(endpoint)
        self.fqdn = fqdn
        self.port = port
        self.database = database
        self.duration = duration
        self.kv_load_type = kv_load_type
        self.worker_count = worker_count
        self.begin_time = None
        self.end_time = None
        self.version = version
        self.init_data_keys = set()
        self.config = config
        self.client = None

    @property
    def partition_count(self):
        return self.config.volume_config.partition_count

    @property
    def storage_channels(self):
        return self.config.volume_config.channel_media

    @property
    def path(self):
        return self.config.volume_config.path

    def _volume_path(self):
        return f'{self.database}/{self.path}'

    def _create_volume(self, client):
        return client.create_tablets(self.partition_count, self._volume_path(), self.storage_channels)

    def _drop_volume(self, client):
        return client.drop_tablets(self._volume_path())

    def _write(self, client, partition_id, key, value, channel):
        return client.kv_write(self._volume_path(), partition_id, key, value, channel=channel, version=self.version)

    def _get_init_pair_key(self, pair_id):
        return f'init_{pair_id}'

    def _fill_init_data_template(self, client, pairs, channel):
        t = type(self)
        pattern = t.INIT_DATA_PATTERN
        pattern_repeats = (t.INIT_DATA_VALUE_SIZE + len(pattern)) // len(pattern)
        data = pattern * pattern_repeats
        # Pre-populate keys set used by workers
        for pair_id in range(pairs):
            self.init_data_keys.add(self._get_init_pair_key(pair_id))
        for partition_id in range(self.partition_count):
            for pair_id in range(pairs):
                self._write(client, partition_id, self._get_init_pair_key(pair_id), data, channel=channel)

    def _fill_init_data(self, client):
        # TODO(kruall): move this to the config
        t = type(self)
        # Reset keys set before filling
        self.init_data_keys.clear()
        if self.kv_load_type == 'read':
            self._fill_init_data_template(client, t.INIT_DATA_PAIRS, 2)
        elif self.kv_load_type == 'read-inline':
            self._fill_init_data_template(client, t.INIT_INLINE_DATA_PAIRS, 1)

    def _pre_start(self):
        print('Init keyvalue volume')
        self.client = KeyValueClient(self.fqdn, self.port)
        create_reponse = self._create_volume(self.client)
        if create_reponse is None or create_reponse.operation.status != ydb_status_codes.StatusIds.StatusCode.SUCCESS:
            print('Create volume failed')
            print('response:', create_reponse)
            return False
        self._fill_init_data(self.client)
        self.begin_time = datetime.now()
        self.end_time = self.begin_time + timedelta(seconds=self.duration)
        print('Start Load; Begin Time:', self.begin_time, '; End Time:', self.end_time)
        return True

    def _post_stop(self):
        print('Stop Load; Drop Volume')
        if self.client:
            self._drop_volume(self.client)
        return True

    def get_status(self, response):
        if response is None:
            return None
        if hasattr(response, 'operation'):
            return response.operation.status
        return response.status

    def __enter__(self):
        self._pre_start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._post_stop()
        return False

    def get_workload_thread_funcs(self):
        workers = []
        for i in range(self.worker_count):
            builder = WorkerBuilder(self.config, self)
            workers.append(lambda w=builder.build(i): w.run())
        return workers
