import sys
from datetime import datetime, timedelta
import random
from collections import defaultdict
import threading
import time

from queue import Queue, PriorityQueue, Empty

from ydb.public.api.protos import ydb_status_codes_pb2 as ydb_status_codes
from ydb.public.api.protos import ydb_keyvalue_pb2 as keyvalue_pb
import ydb.tests.stress.kv_volume.protos.config_pb2 as config_pb


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
    
    
class RWLock:
    def __init__(self):
        self._cond = threading.Condition(threading.RLock())
        self._readers = 0
        self._writer = False
        self._writers_waiting = 0

    def acquire_read(self):
        with self._cond:
            # Если есть активный writer или writer(ы) ждут — читаем позже
            while self._writer or self._writers_waiting > 0:
                self._cond.wait()
            self._readers += 1

    def release_read(self):
        with self._cond:
            self._readers -= 1
            if self._readers == 0:
                self._cond.notify_all()

    def acquire_write(self):
        with self._cond:
            self._writers_waiting += 1
            try:
                while self._writer or self._readers > 0:
                    self._cond.wait()
                self._writer = True
            finally:
                self._writers_waiting -= 1

    def release_write(self):
        with self._cond:
            self._writer = False
            self._cond.notify_all()


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


def get_status(response):
    if response is None:
        return None
    if hasattr(response, 'operation'):
        return response.operation.status
    return response.status


class Worker:
    class WorkerDataContext:
        def __init__(self):
            self.keys = {}
            self.mutex = RWLock()

        def add_key(self, action_name, key, partition_id, key_size):
            self.acquire_write()
            self.keys[key] = (partition_id, key_size)
            self.release_write()

        def acquire_read(self):
            self.mutex.acquire_read()

        def release_read(self):
            self.mutex.release_read()
            
        def acquire_write(self):
            self.mutex.acquire_write()
            
        def release_write(self):
            self.mutex.release_write()

        def _get_keys_by_name(self, name):
            if name == '__initial__':
                return (self, self.keys)
            return []

        def _delete_keys(self, keys):
            for key in keys:
                del self.keys[key]

        def get_keys_to_delete(self, count):
            self.acquire_write()
            count = min(count, len(self.keys))
            keys = random.sample(list(self.keys.keys()), count)
            result = {key: self.keys[key] for key in keys}
            for key in keys:
                del self.keys[key]
            self.release_write()
            return result

        def get_keys_to_read(self, count):
            self.acquire_read
            count = min(count, len(self.keys))
            keys = random.sample(list(self.keys.keys()), count)
            result = {key: self.keys[key] for key in keys}
            self.release_read()
            return result

        def clear_clone(self):
            return self

    class LayeredDataContext:
        def __init__(self, previous_data_context):
            self.mutex = RWLock()
            self.previous_data_context = previous_data_context
            self.keys_by_name = defaultdict(dict)
            self.key_to_name = {}

        def acquire_read(self):
            self.mutex.acquire_read()
            self.previous_data_context.acquire_read()
            
        def release_read(self):
            self.previous_data_context.release_read()
            self.mutex.release_read()

        def acquire_write(self):
            self.mutex.acquire_write()
            self.previous_data_context.acquire_write()
        
        def release_write(self):
            self.previous_data_context.release_write()
            self.mutex.release_write()

        def _get_keys_by_name(self, name):
            if name in self.keys_by_name:
                return self, self.keys_by_name[name]
            return self.previous_data_context._get_keys_by_name(name)

        def _delete_keys(self, keys):
            for key in keys:
                name = self.key_to_name[key]
                del self.keys_by_name[name][key]
                del self.key_to_name[key]

        def add_key(self, action_name, key, partition_id, key_size):
            self.mutex.acquire_write()
            self.keys_by_name[action_name][key] = (partition_id, key_size)
            self.keys[key] = (partition_id, key_size)
            self.key_to_name[key] = action_name
            self.mutex.release_write()

        def _get_keys_structs(self, action_names):
            name_to_keys = {}
            name_to_dc = {}
            key_to_name = {}
            for action_name in action_names:
                dc, keys_dict = self._get_keys_by_name(action_name)
                name_to_dc[action_name] = dc
                name_to_keys[action_name] = keys_dict
                for key in keys_dict:
                    key_to_name[key] = action_name
            return name_to_keys, name_to_dc, key_to_name

        def get_keys_to_delete(self, action_names, count):
            self.acquire_write()
            name_to_keys, name_to_dc, key_to_name = self._get_keys_structs(action_names)

            if not key_to_name:
                self.release_write
                return {}

            count = min(count, len(key_to_name))
            keys = random.sample(sorted(key_to_name), count)

            to_delete = defaultdict(list)
            result = {}
            for key in keys:
                action_name = key_to_name[key]
                to_delete[action_name].append(key)
                result[key] = name_to_keys[action_name][key]

            for name, keys in to_delete.items():
                dc = name_to_dc[name]
                dc._delete_keys(keys)

            self.release_write()
            return result

        def get_keys_to_read(self, action_names, count):
            self.acquire_read()
            name_to_keys, name_to_dc, key_to_name = self._get_keys_structs(action_names)

            if not name_to_keys:
                self.release_read()
                return {}

            count = min(count, len(key_to_name))
            keys = random.sample(sorted(key_to_name), count)

            result = {}
            for key in keys:
                action_name = key_to_name[key]
                result[key] = name_to_keys[action_name][key]

            self.release_read()
            return result

        def clear_clone(self):
            return Worker.LayeredDataContext(self.previous_data_context)

    class ActionRunner:
        def __init__(self, config, workload, worker=None, worker_semaphore=None, instance_id=None, worker_partition_id=None, data_context=None):
            self.config = config
            self.workload = workload
            self.worker = worker
            self.worker_semaphore = worker_semaphore
            self.instance_id = instance_id
            self.write_idx = 0
            self.data_context = data_context
            self._worker_partition_id = worker_partition_id

        @property
        def verbose(self):
            return self.workload.verbose

        @property
        def version(self):
            return self.workload.version

        @property
        def name(self):
            return self.config.name

        @property
        def client(self):
            return self.worker.client

        @property
        def worker_partition_id(self):
            if self._worker_partition_id is None:
                return random.randrange(0, self.workload.partition_count)
            return self._worker_partition_id

        def print(self, print_cmd):
            print(f"[Worker Action: {self.config.name}] {print_cmd.msg}", file=sys.stderr)

        def get_action_names(self):
            action_data_mode = self.config.action_data_mode
            if action_data_mode is None:
                return ['__initial__']
            mode = action_data_mode.WhichOneof('Mode')
            if mode == "worker":
                return ['__initial__']
            return list(action_data_mode.from_prev_actions.action_name)

        def read(self, read_cmd):
            action_names = self.get_action_names()
            keys_with_partitions = self.data_context.get_keys_to_read(action_names, read_cmd.count)

            for key, key_info in keys_with_partitions.items():
                partition_id, key_size = key_info
                max_offset = key_size - read_cmd.size if key_size > read_cmd.size else 0
                offset = random.randint(0, max_offset) if max_offset > 1 else 0

                if self.verbose:
                    print(f"READ action: key={key}, partition_id={partition_id}, offset={offset}, size={read_cmd.size}, version={self.version}", file=sys.stderr)

                response = self.worker.client.kv_read(
                    self.workload._volume_path(),
                    partition_id,
                    key,
                    offset=offset,
                    size=read_cmd.size,
                    version=self.workload.version
                )

                if response is None or get_status(response) != ydb_status_codes.StatusIds.StatusCode.SUCCESS:
                    raise ValueError(f"Read failed for key {key} in action {self.config.name}")

                if read_cmd.verify_data:
                    if self.workload.version == 'v2':
                        read_result = response
                    else:
                        read_result = keyvalue_pb.ReadResult()
                        if not response.operation.result.Unpack(read_result):
                            raise ValueError(f"Failed to unpack read result for key {key} in action {self.config.name}")

                    expected_data = generate_pattern_data(key_size)[offset:offset + read_cmd.size]
                    actual_data = read_result.value
                    if actual_data != expected_data.encode():
                        msg = (
                            f"Data verification failed for key {key} in action {self.config.name}: "
                            f"key_size={key_size}, offset={offset}, read_size={read_cmd.size},"
                            f"actual_len={len(actual_data)}, expected_len={len(expected_data)}"
                            f"actual_data={actual_data[:min(10, len(actual_data))]}, expected_data={expected_data[:min(10, len(expected_data))]}"
                        )
                        raise ValueError(msg)
                    if self.verbose:
                        print(f"READ verification: key={key}, data verified successfully", file=sys.stderr)

        def delete(self, delete_cmd):
            action_names = self.get_action_names()
            keys_to_delete = self.data_context.get_keys_to_delete(action_names, delete_cmd.count)
            if len(keys_to_delete) == 0:
                return

            for key, key_info in keys_to_delete:
                partition_id = key_info[0]
                if self.verbose:
                    print(f"DELETE action: key={key}, partition_id={partition_id}, version={self.workload.version}", file=sys.stderr)

                response = self.worker.client.kv_delete_range(
                    self.workload._volume_path(),
                    partition_id,
                    from_key=key,
                    to_key=key,
                    from_inclusive=True,
                    to_inclusive=True,
                    version=self.workload.version
                )
                if response is None or get_status(response) != ydb_status_codes.StatusIds.StatusCode.SUCCESS:
                    raise ValueError(f"Delete failed for key {key} in action {self.config.name}")

                if self.verbose:
                    print(f"DELETE successful: key={key}", file=sys.stderr)

        def generate_key(self):
            self.write_idx += 1
            return f"{self.config.name}_{self.instance_id}_{self.write_idx}"

        def write(self, write_cmd):
            kv_pairs = []
            for i in range(write_cmd.count):
                key = self.generate_key()
                data = generate_pattern_data(write_cmd.size)
                kv_pairs.append((key, data))
            partition_id = self.worker_partition_id

            response = self.worker.client.kv_writes(
                self.worker.volume_path,
                partition_id,
                kv_pairs,
                channel=write_cmd.channel,
                version=self.workload.version
            )

            if response is None or get_status(response) != ydb_status_codes.StatusIds.StatusCode.SUCCESS:
                raise ValueError(f"Write failed for key {key} in action {self.config.name}")

            for key, data in kv_pairs:
                self.data_context.add_key(self.name, key, partition_id, len(data))
            if self.verbose:
                print("WRITE successful", file=sys.stderr)

        def execute_command(self, cmd):
            cmd_type = cmd.WhichOneof('Command')
            if cmd_type == 'print':
                self.print(cmd.print)
            elif cmd_type == 'read':
                self.read(cmd.read)
            elif cmd_type == 'write':
                self.write(cmd.write)
            elif cmd_type == 'delete':
                self.delete(cmd.delete)

        def notify_about_ending(self):
            self.worker.notify_about_ending(self.instance_id, self.data_context)

        def run(self):
            if self.verbose:
                print(f"ActionRunner: running commands for action '{self.config.name}', instance_id={self.instance_id}", file=sys.stderr)

            if self.worker_semaphore:
                with self.worker_semaphore:
                    for cmd in self.config.action_command:
                        self.execute_command(cmd)
            else:
                for cmd in self.config.action_command:
                    self.execute_command(cmd)
            if self.verbose:
                print(f"ActionRunner: completed commands for action '{self.config.name}', instance_id={self.instance_id}", file=sys.stderr)

            self.worker._increment_action_stat(self.name)
            self.notify_about_ending()

    def __init__(self, worker_id, workload, worker_partition_id, stats_queue=None):
        self.worker_id = worker_id
        self.next_instance_id = worker_id
        self.workload = workload
        self.database = str(self.workload.database)
        self.path = str(self.workload.path)
        self._client = None
        self.worker_partition_id = worker_partition_id

        self.event_queue = None
        self.scheduled_queue = None
        self.actions = {}

        self.write_key_counter = 0
        self.stats_queue = stats_queue
        self.show_stats = self.workload.show_stats
        self.action_stats = defaultdict(int)
        self.stats_lock = threading.Lock()

    @property
    def volume_path(self):
        return f'{self.database}/{self.path}'

    @property
    def verbose(self):
        return self.workload.verbose

    @property
    def client(self):
        if self._client is None:
            self._client = KeyValueClient(str(self.workload.fqdn), int(self.workload.port))
        return self._client

    def generate_instance_id(self):
        id = self.next_instance_id
        self.next_instance_id += self.workload.worker_count
        return id

    def _generate_write_key(self):
        self.write_key_counter += 1
        return f'worker_{self.worker_id}_write_key_{self.write_key_counter}'

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

    def write(self, write_cmd, data_context):
        kv_pairs = []
        for i in range(write_cmd.count):
            key = self._generate_write_key()
            data = generate_pattern_data(write_cmd.size)
            kv_pairs.append((key, data))
        partition_id = self.worker_partition_id

        response = self.client.kv_writes(
            self.volume_path,
            partition_id,
            kv_pairs,
            channel=write_cmd.channel,
            version=self.workload.version
        )

        status = None
        if response is None or (status := get_status(response)) != ydb_status_codes.StatusIds.StatusCode.SUCCESS:
            raise ValueError(f"Write failed in init, status: {status}")

        for key, data in kv_pairs:
            data_context.add_key('', key, partition_id, len(data))
        if self.verbose:
            print("WRITE successful", file=sys.stderr)

    def write_initial_data(self, data_context):
        initial_data = self.workload.config.initial_data
        if self.verbose:
            print(f"Worker {self.worker_id}: filling init data", file=sys.stderr)

        for write_cmd in initial_data.write_commands:
            self.write(write_cmd, data_context)

    def add_action(self, action_config):
        self.actions[action_config.name] = action_config
        if self.verbose:
            period_str = f", period={action_config.period_us}us" if action_config.HasField('period_us') else ", one-shot"
            print(f"Worker {self.worker_id}: added action '{action_config.name}'{period_str}, commands={len(action_config.action_command)}", file=sys.stderr)

    def notify_about_ending(self, id, data_context):
        self.event_queue.put(('end', id, data_context))

    def run(self):
        self.event_queue = Queue()
        self.scheduled_queue = PriorityQueue()

        if self.verbose:
            print(f"Worker {self.worker_id}: starting run", file=sys.stderr)

        initial_dc = Worker.WorkerDataContext()
        self.write_initial_data(initial_dc)

        end_time = datetime.now() + timedelta(seconds=self.workload.duration + 2)

        stats_thread = None
        if self.show_stats:
            stats_thread = threading.Thread(target=self._stats_collector, daemon=True)
            stats_thread.start()

        period_actions = {}
        children_map = defaultdict(set)
        semaphores = {}
        for name, action in self.actions.items():
            if action.worker_max_in_flight:
                semaphores[name] = threading.Semaphore(action.worker_max_in_flight)
            if action.period_us:
                period_actions[name] = action.period_us
            if not action.parent_action:
                self.event_queue.put(('run', name, Worker.LayeredDataContext(initial_dc)))
                continue
            children_map[action.parent_action].add(name)

        threads = {}
        id_to_action_name = {}
        while datetime.now() < end_time:
            next_time = None
            while (now := datetime.now()) < end_time:
                try:
                    boxed = self.scheduled_queue.get_nowait()
                except Empty:
                    break
                if boxed is None:
                    break
                time, action_name, dc = boxed
                if time > now:
                    self.scheduled_queue.put(boxed)
                    next_time = time
                    break
                self.event_queue.put(('run', action_name, dc))

            while (now := datetime.now()) < end_time:
                try:
                    if next_time and now < next_time:
                        boxed = self.event_queue.get(timeout=(max(next_time - now, 0,1)).total_seconds())
                    else:
                        boxed = self.event_queue.get_nowait()
                except Empty:
                    break
                if boxed is None:
                    break
                command, q, dc = boxed
                if command == 'end':
                    id = q
                    name = id_to_action_name[id]
                    for child in children_map[name]:
                        self.event_queue.put(('run', child, Worker.LayeredDataContext(dc)))
                    if name in period_actions:
                        self.scheduled_queue.put((now + timedelta(microseconds=period_actions[name]), name, dc.clear_clone()))
                    del id_to_action_name[id]
                elif command == 'run':
                    name = q
                    instance_id = self.generate_instance_id()
                    runner = Worker.ActionRunner(self.actions[name], self.workload, self, semaphores.get(name), instance_id, self.worker_partition_id, data_context=dc)
                    id_to_action_name[instance_id] = name
                    runner.run()

        self.client.close()
        if self.verbose:
            print(f"Worker {self.worker_id}: finished arun", file=sys.stderr)


class WorkerBuilder:
    def __init__(self, config, workload, worker_partition_id):
        self.config = config
        self.workload = workload
        self.worker_partition_id = worker_partition_id

    @property
    def verbose(self):
        return self.workload.verbose

    def build(self, worker_id, stats_queue=None):
        if self.verbose:
            print(f"WorkerBuilder: building worker {worker_id} with {len(self.config.actions)} actions", file=sys.stderr)
        worker = Worker(worker_id, self.workload, stats_queue=stats_queue, worker_partition_id=self.worker_partition_id)

        for action in self.config.actions:
            worker.add_action(action)
        if self.verbose:
            print(f"WorkerBuilder: worker {worker_id} built successfully", file=sys.stderr)
        return worker


class YdbKeyValueVolumeWorkload(WorkloadBase):
    def __init__(self, endpoint, database, duration, worker_count, version, config, verbose=False, show_stats=False):
        super().__init__(None, '', 'kv_volume', None)
        fqdn, port = parse_endpoint(endpoint)
        self.fqdn = fqdn
        self.port = int(port)
        self.database = database
        self.duration = duration
        self.worker_count = worker_count
        self.version = version
        self.config = config
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
        sum_stats_accumulator = defaultdict(int)
        last_print_time = time.time()
        second_counter = 0

        action_names = []
        for action in self.config.actions:
            if action.name:
                action_names.append(action.name)

        while True:
            msg = self.stats_queue.get()

            if msg == 'start':
                start_messages += 1
                if start_messages >= self.worker_count:
                    break

        begin_time = datetime.now()
        end_time = begin_time + timedelta(seconds=self.duration)
        print('Start Load; Begin Time:', begin_time, '; End Time:', end_time)

        sorted_actions = sorted(action_names)
        col_width = max(8, max(len(a) for a in sorted_actions))

        separator = "+" + "-" * 10 + "+"
        for action in sorted_actions:
            separator += "-" * (col_width + 1) + "+"
        print(separator)

        print("| Time     |", end="")
        for action in sorted_actions:
            print(f" {action.ljust(col_width)}|", end="")
        print("")
        print(separator)

        while second_counter <= self.duration:
            msg = self.stats_queue.get()

            if msg == 'stop':
                break
            elif isinstance(msg, dict):
                for action_type, count in msg.items():
                    stats_accumulator[action_type] += count
                    sum_stats_accumulator[action_type] += count

            current_time = time.time()

            if current_time - last_print_time >= 1.0:
                if second_counter != 0:
                    print(f"| {second_counter:7d}s |", end="")
                    for action in sorted_actions:
                        count = stats_accumulator.get(action, 0)
                        print(f"{str(count).rjust(col_width)} |", end="")
                    print("")
                else:
                    sum_stats_accumulator.clear()

                stats_accumulator.clear()
                second_counter += 1
                last_print_time = current_time

        print(separator)

        print("|      avg |", end="")
        for action in sorted_actions:
            count = sum_stats_accumulator.get(action, 0)
            print(f"{str(count // self.duration).rjust(col_width)} |", end="")
        print("")

        print("|      sum |", end="")
        for action in sorted_actions:
            count = sum_stats_accumulator.get(action, 0)
            print(f"{str(count).rjust(col_width)} |", end="")
        print("")

        print(separator)

    def _pre_start(self):
        if self.verbose:
            print(f"Initializing KeyValue volume: endpoint={self.fqdn}:{self.port}, database={self.database}, path={self._volume_path()}", file=sys.stderr)
        client = KeyValueClient(self.fqdn, self.port)
        create_reponse = self._create_volume(client)
        client.close()
        if create_reponse is None or create_reponse.operation.status != ydb_status_codes.StatusIds.StatusCode.SUCCESS:
            print('Create volume failed')
            print('response:', create_reponse)
            return False
        if self.verbose:
            print(f"Start Load: duration={self.duration}s, workers={self.worker_count}, version={self.version}", file=sys.stderr)

        if self.show_stats:
            from multiprocessing import Queue
            self.stats_queue = Queue()
            self.stats_thread = threading.Thread(target=self._stats_listener, daemon=True)
            self.stats_thread.start()

        return True

    def _post_stop(self):
        if self.verbose:
            print(f"Stop Load: dropping volume {self._volume_path()}", file=sys.stderr)

        client = KeyValueClient(self.fqdn, self.port)
        self._drop_volume(client)
        client.close()

        if self.show_stats and self.stats_queue:
            self.stats_queue.put('stop')
            if self.stats_thread:
                self.stats_thread.join(timeout=2.0)

        return True

    def get_workload_thread_funcs(self):
        workers = []
        for i in range(self.worker_count):
            worker_partition_id = None
            if self.config.partition_mode == config_pb.PartitionMode.Code.OnePartition:
                worker_partition_id = i % self.config.volume_config.partition_count
            builder = WorkerBuilder(self.config, self, worker_partition_id=worker_partition_id)
            worker = builder.build(i, self.stats_queue if self.show_stats else None)
            workers.append(lambda w=worker, worker_id=i: w.run())
        return workers
