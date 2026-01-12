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

DEFAULT_YDB_KV_PORT = 2135
DEFAULT_DATA_PATTERN = '0123456789ABCDEF'

logger = logging.getLogger("YdbKvWorkload")


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
        def __init__(self, config, workload, worker_semaphore=None, global_semaphore=None, init_keys=None, results=None, parent_names=None, parent_chain=None):
            self.config = config
            self.workload = workload
            self.task = None
            self.completed_actions = set()
            self.worker_semaphore = worker_semaphore
            self.global_semaphore = global_semaphore
            self.init_keys = init_keys or set()
            self.results = results if results is not None else defaultdict(lambda: defaultdict(set))
            self.parent_names = parent_names or set()
            self.parent_chain = parent_chain or {}
            self.instance_id = None

        async def execute_command(self, cmd, data_context=None):
            cmd_type = cmd.WhichOneof('Command')
            if cmd_type == 'print':
                print(f"[Worker Action: {self.config.name}] {cmd.print.msg}")
            elif cmd_type == 'read':
                keys = self._get_keys()
                if not keys:
                    logger.warning(f"No keys available for read in action {self.config.name}")
                    return
                partition_id = random.randrange(0, self.workload.partition_count)
                key = random.choice(keys)
                offset = 0
                if cmd.read.size != 0:
                    offset = random.randint(0, cmd.read.size - 1) if cmd.read.size > 1 else 0
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
                key = self.workload._generate_write_key()
                partition_id = random.randrange(0, self.workload.partition_count)
                pattern_repeats = (cmd.write.size + len(DEFAULT_DATA_PATTERN)) // len(DEFAULT_DATA_PATTERN)
                data = DEFAULT_DATA_PATTERN * pattern_repeats
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
                self._store_key(key)

        def _get_keys(self):
            mode = self.config.action_data_mode.WhichOneof('Mode')
            if mode == 'worker':
                return list(self.init_keys) if self.init_keys else []
            elif mode == 'from_prev_actions':
                keys = set()
                for action_name in self.config.action_data_mode.from_prev_actions.action_name:
                    if action_name in self.results and action_name in self.parent_chain:
                        instance_id = self.parent_chain[action_name]
                        if instance_id and instance_id in self.results[action_name]:
                            keys.update(self.results[action_name][instance_id])
                return list(keys) if keys else []
            else:
                return list(self.init_keys) if self.init_keys else []

        def _store_key(self, key):
            self.results[self.config.name][self.instance_id].add(key)
        
        async def run_commands(self, parent_chain_update=None):
            self.instance_id = self.workload._generate_instance_id(self.config.name)
            if parent_chain_update:
                parent_chain_update(self.config.name, self.instance_id)
            if self.worker_semaphore:
                async with self.worker_semaphore:
                    if self.global_semaphore:
                        async with self.global_semaphore:
                            for cmd in self.config.action_command:
                                await self.execute_command(cmd)
                    else:
                        for cmd in self.config.action_command:
                            await self.execute_command(cmd)
            elif self.global_semaphore:
                async with self.global_semaphore:
                    for cmd in self.config.action_command:
                        await self.execute_command(cmd)
            else:
                for cmd in self.config.action_command:
                    await self.execute_command(cmd)

        async def run_periodic(self, end_time, parent_chain_update=None, on_iteration_complete=None):
            period = self.config.period_us / 1000000.0
            while datetime.now() < end_time:
                await self.run_commands(parent_chain_update)
                if on_iteration_complete:
                    await on_iteration_complete()
                await asyncio.sleep(period)

        async def run_once(self, parent_chain_update=None):
            await self.run_commands(parent_chain_update)
    
    def __init__(self, worker_id, workload):
        self.worker_id = worker_id
        self.workload = workload
        self.actions = {}
        self.runners = {}
        self.global_semaphores = {}
        self.init_keys = self._generate_init_keys()
        self.results = defaultdict(lambda: defaultdict(set))
        self.write_key_counter = 0
        self.instance_counter = 0

    def _generate_instance_id(self, action_name):
        self.instance_counter += 1
        return f'{action_name}_{self.instance_counter}'

    def _generate_init_key(self, pair_id):
        return f'worker_{self.worker_id}_init_pair_{pair_id}'

    def _generate_write_key(self):
        self.write_key_counter += 1
        return f'worker_{self.worker_id}_write_key_{self.write_key_counter}'

    def _generate_init_keys(self):
        init_keys = set()
        if not hasattr(self.workload.config, 'initial_data'):
            return init_keys

        for write_cmd in self.workload.config.initial_data.write_commands:
            for pair_id in range(write_cmd.count):
                init_keys.add(self._generate_init_key(pair_id))
        return init_keys

    async def _fill_init_data(self):
        if not hasattr(self.workload.config, 'initial_data'):
            return

        for write_cmd in self.workload.config.initial_data.write_commands:
            pattern = DEFAULT_DATA_PATTERN
            pattern_repeats = (write_cmd.size + len(pattern)) // len(pattern)
            data = pattern * pattern_repeats
            data = data[:write_cmd.size]
            for pair_id in range(write_cmd.count):
                key = self._generate_init_key(pair_id)
                for partition_id in range(self.workload.partition_count):
                    await self.workload.client.a_kv_write(
                        self.workload._volume_path(),
                        partition_id,
                        key,
                        data,
                        channel=write_cmd.channel,
                        version=self.workload.version
                    )

    def add_action(self, action_config, parent_names=None):
        worker_sem = None
        if action_config.HasField('worker_max_in_flight'):
            worker_sem = asyncio.Semaphore(action_config.worker_max_in_flight)

        global_sem = None
        if action_config.HasField('global_max_in_flight'):
            if action_config.name not in self.global_semaphores:
                self.global_semaphores[action_config.name] = asyncio.Semaphore(action_config.global_max_in_flight)
            global_sem = self.global_semaphores[action_config.name]

        parent_names = parent_names.copy() if parent_names is not None else set()
        parent_chain = {name: None for name in parent_names}
        runner = self.ActionRunner(action_config, self.workload, worker_sem, global_sem, self.init_keys, self.results, parent_names, parent_chain)
        self.actions[action_config.name] = action_config
        self.runners[action_config.name] = runner
        
    def _update_parent_chains(self, action_name, instance_id, parent_chains):
        for child_name, child_chain in parent_chains.items():
            if action_name in child_chain:
                child_chain[action_name] = instance_id

    async def arun(self):
        await self._fill_init_data()
        end_time = datetime.now() + timedelta(seconds=self.workload.duration)

        task_map = {}
        parent_chains = {}
        
        parent_names_map = {}
        for name, action in self.actions.items():
            parent_names_map[name] = set()
            current = action.parent_action if action.HasField('parent_action') else None
            while current and current in self.actions:
                parent_names_map[name].add(current)
                current = self.actions[current].parent_action if self.actions[current].HasField('parent_action') else None

        for name in self.actions:
            parent_chains[name] = {parent: None for parent in parent_names_map[name]}

        child_actions_map = defaultdict(list)

        for name, action in self.actions.items():
            parent = action.parent_action if action.HasField('parent_action') else None
            if parent is not None:
                child_actions_map[parent].append(name)

        def update_parent_chains(action_name, instance_id):
            self._update_parent_chains(action_name, instance_id, parent_chains)

        async def run_child_actions(parent_name):
            child_tasks = []
            for child_name in child_actions_map[parent_name]:
                child_action = self.actions[child_name]
                runner = self.runners[child_name]
                runner.parent_chain = parent_chains[child_name].copy()
                if child_action.HasField('period_us'):
                    child_task = asyncio.create_task(runner.run_periodic(end_time, update_parent_chains))
                else:
                    child_task = asyncio.create_task(runner.run_once(update_parent_chains))
                child_tasks.append(child_task)
            return child_tasks

        for name, action in self.actions.items():
            parent = action.parent_action if action.HasField('parent_action') else None
            if parent is None:
                if action.HasField('period_us'):
                    async def run_periodic_with_children(runner=self.runners[name], end_time=end_time, action_name=name):
                        await runner.run_periodic(end_time, update_parent_chains, lambda an=action_name: run_child_actions(an))
                    task_map[name] = asyncio.create_task(run_periodic_with_children())
                else:
                    original_task = asyncio.create_task(self.runners[name].run_once(update_parent_chains))
                    async def run_once_then_children(task=original_task, an=name):
                        await task
                        child_tasks = await run_child_actions(an)
                        if child_tasks:
                            await asyncio.gather(*child_tasks)
                    task_map[name] = asyncio.create_task(run_once_then_children())

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
        
        parent_names_map = {}
        for action in self.config.actions:
            parent_names_map[action.name] = set()
            current = action.parent_action if action.HasField('parent_action') else None
            while current:
                parent_names_map[action.name].add(current)
                current = next((a.parent_action for a in self.config.actions if a.name == current and a.HasField('parent_action')), None)
        
        for action in self.config.actions:
            parent_names = parent_names_map[action.name].copy()
            worker.add_action(action, parent_names)
        return worker
        

class YdbKeyValueVolumeWorkload(WorkloadBase):
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

    def _pre_start(self):
        print('Init keyvalue volume')
        self.client = KeyValueClient(self.fqdn, self.port)
        create_reponse = self._create_volume(self.client)
        if create_reponse is None or create_reponse.operation.status != ydb_status_codes.StatusIds.StatusCode.SUCCESS:
            print('Create volume failed')
            print('response:', create_reponse)
            return False
        self.begin_time = datetime.now()
        self.end_time = self.begin_time + timedelta(seconds=self.duration)
        print('Start Load; Begin Time:', self.begin_time, '; End Time:', self.end_time)
        return True

    async def _post_stop(self):
        print('Stop Load; Drop Volume')
        if self.client:
            self._drop_volume(self.client)
            await self.client.aclose()
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
        asyncio.run(self._post_stop())
        return False

    def get_workload_thread_funcs(self):
        workers = []
        for i in range(self.worker_count):
            builder = WorkerBuilder(self.config, self)
            worker = builder.build(i)
            workers.append(lambda w=worker, worker_id=i: w.run())
        return workers
