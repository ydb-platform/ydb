import sys
from datetime import datetime, timedelta
import random
from collections import defaultdict
import asyncio
import threading
import time

from ydb.public.api.protos import ydb_status_codes_pb2 as ydb_status_codes


from ydb.tests.stress.common.common import WorkloadBase
from ydb.tests.library.clients.kikimr_keyvalue_client import KeyValueClient

DEFAULT_YDB_KV_PORT = 2135
DEFAULT_DATA_PATTERN = '0123456789ABCDEF'


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


def generate_pattern_data(size, pattern=DEFAULT_DATA_PATTERN):
    repeats = (size + len(pattern)) // len(pattern)
    return (pattern * repeats)[:size]


class Worker:
    class ActionRunner:
        def __init__(self, config, workload, worker=None, worker_semaphore_limit=None, init_keys=None, results=None, parent_names=None, parent_chain=None):
            self.config = config
            self.workload = workload
            self.worker = worker
            self.task = None
            self.completed_actions = set()
            self.worker_semaphore = None
            self.worker_semaphore_limit = worker_semaphore_limit
            self.init_keys = init_keys or {}
            self.results = results if results is not None else defaultdict(lambda: defaultdict(dict))
            self.parent_names = parent_names or set()
            self.parent_chain = parent_chain or {}
            self.instance_id = None

        @property
        def verbose(self):
            return self.workload.verbose

        def _generate_instance_id(self, action_name):
            return self.worker._generate_instance_id(action_name)

        def _generate_write_key(self):
            return self.worker._generate_write_key()

        async def execute_command(self, cmd, data_context=None):
            cmd_type = cmd.WhichOneof('Command')
            if cmd_type == 'print':
                print(f"[Worker Action: {self.config.name}] {cmd.print.msg}", file=sys.stderr)
            elif cmd_type == 'read':
                keys_with_partitions = self._get_keys()
                if not keys_with_partitions:
                    print(f"No keys available for read in action {self.config.name}", file=sys.stderr)
                    return
                key, key_info = random.choice(list(keys_with_partitions.items()))
                partition_id, key_size = key_info
                offset = 0
                if cmd.read.size != 0:
                    max_offset = key_size - cmd.read.size if key_size > cmd.read.size else 0
                    offset = random.randint(0, max_offset) if max_offset > 1 else 0
                if self.verbose:
                    print(f"READ action: key={key}, partition_id={partition_id}, offset={offset}, size={cmd.read.size}, version={self.workload.version}", file=sys.stderr)
                client = self.worker.client
                response = client.kv_read(
                    self.workload._volume_path(),
                    partition_id,
                    key,
                    offset=offset,
                    size=cmd.read.size,
                    version=self.workload.version
                )
                if response is None or self.workload.get_status(response) != ydb_status_codes.StatusIds.StatusCode.SUCCESS:
                    print(f"Read failed for key {key} in action {self.config.name}", file=sys.stderr)
                elif cmd.read.verify_data:
                    from ydb.public.api.protos import ydb_keyvalue_pb2 as keyvalue_pb
                    if self.workload.version == 'v2':
                        read_result = response
                    else:
                        read_result = keyvalue_pb.ReadResult()
                        if not response.operation.result.Unpack(read_result):
                            print(f"Failed to unpack read result for key {key} in action {self.config.name}", file=sys.stderr)
                            return
                    expected_data = generate_pattern_data(key_size)[offset:offset + cmd.read.size]
                    actual_data = read_result.value
                    if actual_data != expected_data.encode():
                        print(f"Data verification failed for key {key} in action {self.config.name}: key_size={key_size}, offset={offset}, read_size={cmd.read.size}, actual_len={len(actual_data)}, expected_len={len(expected_data)}", file=sys.stderr)
                        if len(actual_data) > 0 and len(expected_data) > 0:
                            print(f"  First 20 bytes of actual: {actual_data[:20]}", file=sys.stderr)
                            print(f"  First 20 bytes of expected: {expected_data[:20]}", file=sys.stderr)
                    elif self.verbose:
                        print(f"READ verification: key={key}, data verified successfully", file=sys.stderr)
                self.worker._increment_action_stat('read')
            elif cmd_type == 'delete':
                keys_with_partitions = self._get_keys()
                if not keys_with_partitions:
                    print(f"No keys available for delete in action {self.config.name}", file=sys.stderr)
                    return
                count = min(cmd.delete.count, len(keys_with_partitions)) if cmd.delete.count > 0 else 0
                if count == 0:
                    return
                keys_to_delete = list(keys_with_partitions.items())[:count]
                for key, key_info in keys_to_delete:
                    partition_id = key_info[0]
                    if self.verbose:
                        print(f"DELETE action: key={key}, partition_id={partition_id}, version={self.workload.version}", file=sys.stderr)
                    client = self.worker.client
                    response = client.kv_delete_range(
                        self.workload._volume_path(),
                        partition_id,
                        from_key=key,
                        to_key=key,
                        from_inclusive=True,
                        to_inclusive=True,
                        version=self.workload.version
                    )
                    if response is not None and self.workload.get_status(response) == ydb_status_codes.StatusIds.StatusCode.SUCCESS:
                        self._remove_key(key)
                        if self.verbose:
                            print(f"DELETE successful: key={key}", file=sys.stderr)
                        self.worker._increment_action_stat('delete')
                    else:
                        print(f"Delete failed for key {key} in action {self.config.name}", file=sys.stderr)
            elif cmd_type == 'write':
                key = self._generate_write_key()
                partition_id = random.randrange(0, self.workload.partition_count)
                data = generate_pattern_data(cmd.write.size)
                if self.verbose:
                    print(f"WRITE action: key={key}, partition_id={partition_id}, size={cmd.write.size}, channel={cmd.write.channel}, version={self.workload.version}", file=sys.stderr)
                client = self.worker.client
                response = client.kv_write(
                    self.workload._volume_path(),
                    partition_id,
                    key,
                    data,
                    channel=cmd.write.channel,
                    version=self.workload.version
                )
                if response is None or self.workload.get_status(response) != ydb_status_codes.StatusIds.StatusCode.SUCCESS:
                    print(f"Write failed for key {key} in action {self.config.name}", file=sys.stderr)
                else:
                    self._store_key(key, partition_id, cmd.write.size)
                    if self.verbose:
                        print(f"WRITE successful: key={key}", file=sys.stderr)
                    self.worker._increment_action_stat('write')

        def _get_keys(self):
            mode = self.config.action_data_mode.WhichOneof('Mode')
            if mode == 'worker':
                return dict(self.init_keys) if self.init_keys else {}
            elif mode == 'from_prev_actions':
                keys = {}
                for action_name in self.config.action_data_mode.from_prev_actions.action_name:
                    if action_name in self.results and action_name in self.parent_chain:
                        instance_id = self.parent_chain[action_name]
                        if instance_id and instance_id in self.results[action_name]:
                            keys.update(self.results[action_name][instance_id])
                return keys
            else:
                return dict(self.init_keys) if self.init_keys else {}

        def _store_key(self, key, partition_id, size):
            self.results[self.config.name][self.instance_id][key] = (partition_id, size)

        def _remove_key(self, key):
            self.results[self.config.name][self.instance_id].pop(key, None)
            mode = self.config.action_data_mode.WhichOneof('Mode')
            if mode == 'worker':
                self.init_keys.pop(key, None)

        async def run_commands(self, parent_chain_update=None):
            self.instance_id = self._generate_instance_id(self.config.name)
            if self.verbose:
                print(f"ActionRunner: running commands for action '{self.config.name}', instance_id={self.instance_id}", file=sys.stderr)
            if parent_chain_update:
                parent_chain_update(self.config.name, self.instance_id)
            if self.worker_semaphore is None and self.worker_semaphore_limit is not None:
                self.worker_semaphore = asyncio.Semaphore(self.worker_semaphore_limit)
            if self.worker_semaphore:
                async with self.worker_semaphore:
                    for cmd in self.config.action_command:
                        await self.execute_command(cmd)
            else:
                for cmd in self.config.action_command:
                    await self.execute_command(cmd)
            if self.verbose:
                print(f"ActionRunner: completed commands for action '{self.config.name}', instance_id={self.instance_id}", file=sys.stderr)

        async def run_periodic(self, end_time, parent_chain_update=None, on_iteration_complete=None):
            period = self.config.period_us / 1000000.0
            iteration = 0
            if self.verbose:
                print(f"ActionRunner: starting periodic action '{self.config.name}', period={period}s", file=sys.stderr)
            while datetime.now() < end_time:
                iteration += 1
                if self.verbose:
                    print(f"ActionRunner: iteration {iteration} for action '{self.config.name}'", file=sys.stderr)
                start = datetime.now()
                await self.run_commands(parent_chain_update)
                if on_iteration_complete:
                    await on_iteration_complete()
                elapsed = (datetime.now() - start).total_seconds()
                sleep_time = max(0, period - elapsed)
                await asyncio.sleep(sleep_time)
            if self.verbose:
                print(f"ActionRunner: finished periodic action '{self.config.name}', total iterations={iteration}", file=sys.stderr)

        async def run_once(self, parent_chain_update=None):
            await self.run_commands(parent_chain_update)

    def __init__(self, worker_id, workload, stats_queue=None):
        self.worker_id = worker_id
        self.workload = workload
        self.database = str(self.workload.database)
        self.path = str(self.workload.path)
        self.actions = {}
        self.runners = {}
        #self._client = None
        self._client = self.workload.client
        self.init_keys = self._generate_init_keys()
        self.results = defaultdict(lambda: defaultdict(dict))
        self.write_key_counter = 0
        self.instance_counter = 0
        self.stats_queue = stats_queue
        self.show_stats = self.workload.show_stats if hasattr(self.workload, 'show_stats') else False
        self.action_stats = defaultdict(int)
        self.stats_lock = threading.Lock()
        if self.verbose:
            print(f"Worker {self.worker_id} initialized with {len(self.init_keys)} init keys", file=sys.stderr)

    def volume_path(self):
        return f'{self.database}/{self.path}'

    @property
    def verbose(self):
        return self.workload.verbose
    
    @property
    def client(self):
        if self._client is None:
            if self.verbose:
                print(f"Worker {self.worker_id}: creating client", file=sys.stderr)
            self._client = KeyValueClient(self.workload.fqdn, self.workload.port)
            if self.verbose:
                print(f"Worker {self.worker_id}: created client", file=sys.stderr)
        return self._client

    def _generate_instance_id(self, action_name):
        self.instance_counter += 1
        return f'{action_name}_{self.instance_counter}'

    def _generate_init_key(self, pair_id):
        return f'worker_{self.worker_id}_init_pair_{pair_id}'

    def _generate_write_key(self):
        self.write_key_counter += 1
        return f'worker_{self.worker_id}_write_key_{self.write_key_counter}'

    def _generate_init_keys(self):
        init_keys = {}
        if not hasattr(self.workload.config, 'initial_data'):
            if self.verbose:
                print(f"Worker {self.worker_id}: no initial data configured", file=sys.stderr)
            return init_keys

        for write_cmd in self.workload.config.initial_data.write_commands:
            for pair_id in range(write_cmd.count):
                key = self._generate_init_key(pair_id)
                partition_id = random.randrange(0, self.workload.partition_count)
                init_keys[key] = (partition_id, write_cmd.size)
        if self.verbose:
            print(f"Worker {self.worker_id}: generated {len(init_keys)} init keys", file=sys.stderr)
        return init_keys

    def _stats_collector(self):
        if self.stats_queue:
            self.stats_queue.put('start')

        while True:
            time.sleep(0.1)
            with self.stats_lock:
                if not self.action_stats:
                    continue
                stats_snapshot = dict(self.action_stats)
                self.action_stats.clear()

            if self.stats_queue:
                self.stats_queue.put(stats_snapshot)

    def _increment_action_stat(self, action_type):
        if self.show_stats:
            with self.stats_lock:
                self.action_stats[action_type] += 1

    async def _fill_init_data(self):
        if not hasattr(self.workload.config, 'initial_data'):
            return

        verbose = self.workload.verbose if hasattr(self.workload, 'verbose') else False
        if verbose:
            print(f"Worker {self.worker_id}: filling init data", file=sys.stderr)
        for write_cmd in self.workload.config.initial_data.write_commands:
            data = generate_pattern_data(write_cmd.size)
            for pair_id in range(write_cmd.count):
                key = self._generate_init_key(pair_id)
                key_info = self.init_keys.get(key)
                if key_info is not None:
                    partition_id = key_info[0]
                    if verbose:
                        print(f"Init data WRITE: key={key}, partition_id={partition_id}, size={write_cmd.size}, channel={write_cmd.channel}", file=sys.stderr)
                    self.client.kv_write(
                        self.volume_path(),
                        partition_id,
                        key,
                        data,
                        channel=write_cmd.channel,
                        version=self.workload.version
                    )
                    if verbose:
                        print(f"Init data WRITE completed: key={key}, partition_id={partition_id}, size={write_cmd.size}, channel={write_cmd.channel}", file=sys.stderr)
        if verbose:
            print(f"Worker {self.worker_id}: init data filled successfully", file=sys.stderr)

    def add_action(self, action_config, parent_names=None):
        worker_sem_limit = None
        if action_config.HasField('worker_max_in_flight'):
            worker_sem_limit = action_config.worker_max_in_flight

        parent_names = parent_names or set()
        parent_chain = {name: None for name in parent_names}
        runner = self.ActionRunner(action_config, self.workload, self, worker_sem_limit, self.init_keys, self.results, parent_names, parent_chain)
        self.actions[action_config.name] = action_config
        self.runners[action_config.name] = runner
        if self.verbose:
            period_str = f", period={action_config.period_us}us" if action_config.HasField('period_us') else ", one-shot"
            print(f"Worker {self.worker_id}: added action '{action_config.name}'{period_str}, commands={len(action_config.action_command)}", file=sys.stderr)

    def _update_parent_chains(self, action_name, instance_id, parent_chains):
        for child_name, child_chain in parent_chains.items():
            if action_name in child_chain:
                child_chain[action_name] = instance_id

    async def arun(self):
        if self.verbose:
            print(f"Worker {self.worker_id}: starting arun", file=sys.stderr)
        await self._fill_init_data()
        end_time = datetime.now() + timedelta(seconds=self.workload.duration)

        stats_thread = None
        if self.show_stats:
            stats_thread = threading.Thread(target=self._stats_collector, daemon=True)
            stats_thread.start()

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
        if self.verbose:
            print(f"Worker {self.worker_id}: configured {len(self.actions)} actions", file=sys.stderr)

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
        self.client.close()
        if self.verbose:
            print(f"Worker {self.worker_id}: finished arun", file=sys.stderr)

    def run(self):
        if self.verbose:
            print(f'Worker {self.worker_id} started', file=sys.stderr)
        asyncio.run(self.arun())


class WorkerBuilder:
    def __init__(self, config, workload):
        self.config = config
        self.workload = workload

    @property
    def verbose(self):
        return self.workload.verbose

    def build(self, worker_id, stats_queue=None):
        if self.verbose:
            print(f"WorkerBuilder: building worker {worker_id} with {len(self.config.actions)} actions", file=sys.stderr)
        worker = Worker(worker_id, self.workload, stats_queue)

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
        if self.verbose:
            print(f"WorkerBuilder: worker {worker_id} built successfully", file=sys.stderr)
        return worker


class YdbKeyValueVolumeWorkload(WorkloadBase):
    def __init__(self, endpoint, database, duration, worker_count, version, config, verbose=False, show_stats=False):
        super().__init__(None, '', 'kv_volume', None)
        fqdn, port = parse_endpoint(endpoint)
        self.fqdn = fqdn
        self.port = port
        self.database = database
        self.duration = duration
        self.worker_count = worker_count
        self.begin_time = None
        self.end_time = None
        self.version = version
        self.config = config
        self.client = None
        self.verbose = verbose
        self.show_stats = show_stats
        self.stats_queue = None
        self.stats_thread = None

    @property
    def partition_count(self):
        return self.config.volume_config.partition_count

    @property
    def storage_channels(self):
        return list(self.config.volume_config.channel_media)

    @property
    def path(self):
        return self.config.volume_config.path

    def _volume_path(self):
        return f'{self.database}/{self.path}'

    def _create_volume(self, client):
        return client.create_tablets(self.partition_count, self._volume_path(), self.storage_channels)

    def _drop_volume(self, client):
        return client.drop_tablets(self._volume_path())

    def _stats_listener(self):
        start_messages = 0
        stats_accumulator = defaultdict(int)
        last_print_time = time.time()
        action_types = set()
        header_printed = False
        second_counter = 0

        while True:
            msg = self.stats_queue.get()

            if msg == 'start':
                start_messages += 1
                if start_messages >= self.worker_count:
                    print("Stats: All workers started", file=sys.stderr)
            elif msg == 'stop':
                break
            elif isinstance(msg, dict):
                for action_type, count in msg.items():
                    stats_accumulator[action_type] += count
                    action_types.add(action_type)

            current_time = time.time()
            if current_time - last_print_time >= 1.0:
                if action_types and not header_printed:
                    sorted_actions = sorted(action_types)
                    col_width = max(8, max(len(a) for a in sorted_actions))

                    print("Time     |", end="", file=sys.stderr)
                    for action in sorted_actions:
                        print(f" {action.ljust(col_width)}|", end="", file=sys.stderr)
                    print("", file=sys.stderr)

                    separator = "-" * 9 + "+"
                    for action in sorted_actions:
                        separator += "-" * (col_width + 2) + "+"
                    print(separator, file=sys.stderr)
                    header_printed = True

                if header_printed:
                    sorted_actions = sorted(action_types)
                    col_width = max(8, max(len(a) for a in sorted_actions))

                    print(f"{second_counter:3d}s      |", end="", file=sys.stderr)
                    for action in sorted_actions:
                        count = stats_accumulator.get(action, 0)
                        print(f" {str(count).ljust(col_width)}|", end="", file=sys.stderr)
                    print("", file=sys.stderr)

                    stats_accumulator.clear()
                    second_counter += 1
                last_print_time = current_time

    def _pre_start(self):
        if self.verbose:
            print(f"Initializing KeyValue volume: endpoint={self.fqdn}:{self.port}, database={self.database}, path={self._volume_path()}", file=sys.stderr)
        self.client = KeyValueClient(self.fqdn, self.port)
        create_reponse = self._create_volume(self.client)
        if create_reponse is None or create_reponse.operation.status != ydb_status_codes.StatusIds.StatusCode.SUCCESS:
            print('Create volume failed')
            print('response:', create_reponse)
            return False
        self.begin_time = datetime.now()
        self.end_time = self.begin_time + timedelta(seconds=self.duration)
        print('Start Load; Begin Time:', self.begin_time, '; End Time:', self.end_time)
        if self.verbose:
            print(f"Start Load: duration={self.duration}s, workers={self.worker_count}, version={self.version}", file=sys.stderr)

        if self.show_stats:
            from multiprocessing import Queue
            self.stats_queue = Queue()
            self.stats_thread = threading.Thread(target=self._stats_listener, daemon=True)
            self.stats_thread.start()

        return True

    def _post_stop(self):
        asyncio.run(self._apost_stop())

    async def _apost_stop(self):
        if self.verbose:
            print(f"Stop Load: dropping volume {self._volume_path()}", file=sys.stderr)
        if self.client:
            self._drop_volume(self.client)
            self.client.close()

        if self.show_stats and self.stats_queue:
            self.stats_queue.put('stop')
            if self.stats_thread:
                self.stats_thread.join(timeout=2.0)

        return True

    def get_status(self, response):
        if response is None:
            return None
        if hasattr(response, 'operation'):
            return response.operation.status
        return response.status

    def get_workload_thread_funcs(self):
        workers = []
        for i in range(self.worker_count):
            builder = WorkerBuilder(self.config, self)
            worker = builder.build(i, self.stats_queue if self.show_stats else None)
            workers.append(lambda w=worker, worker_id=i: w.run())
        return workers
