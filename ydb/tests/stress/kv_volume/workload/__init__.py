import logging
from datetime import datetime, timedelta
import random

from ydb.public.api.protos import ydb_status_codes_pb2 as ydb_status_codes
# from library.python import resource


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
    parts = hostport.rsplit(':', 1)  # Разделить с конца, максимум 1 раз
    host = parts[0]
    s_port = parts[1] if len(parts) > 1 else None
    return host, parse_int_with_default(s_port, DEFAULT_YDB_KV_PORT)


class YdbKeyValueVolumeWorkload(WorkloadBase):
    INIT_DATA_PAIRS = 64
    INIT_INLINE_DATA_PAIRS = 8
    INIT_DATA_VALUE_SIZE = 2 * 1024 * 1024
    INIT_DATA_PATTERN = '0123456789ABCDEF'

    def __init__(self, endpoint, database, duration, path, partitions, storage_channels, kv_load_type, inflight, version):
        super().__init__(None, '', 'kv_volume', None)
        fqdn, port = parse_endpoint(endpoint)
        self.fqdn = fqdn
        self.port = port
        self.database = database
        self.duration = duration
        self.path = path
        self.partitions = partitions
        self.storage_channels = storage_channels
        self.kv_load_type = kv_load_type
        self.inflight = inflight
        self.begin_time = None
        self.end_time = None
        self.version = version
        self.init_data_keys = set()

    def _volume_path(self):
        return f'{self.database}/{self.path}'

    def _create_volume(self, client):
        return client.create_tablets(self.partitions, self._volume_path(), self.storage_channels)

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
        for partition_id in range(self.partitions):
            for pair_id in range(pairs):
                self._write(client, partition_id, self._get_init_pair_key(pair_id), data, channel=channel)

    def _fill_init_data(self, client):
        t = type(self)
        # Reset keys set before filling
        self.init_data_keys.clear()
        if self.kv_load_type == 'read':
            self._fill_init_data_template(client, t.INIT_DATA_PAIRS, 2)
        elif self.kv_load_type == 'read-inline':
            self._fill_init_data_template(client, t.INIT_INLINE_DATA_PAIRS, 1)

    def _pre_start(self):
        print('Init keyvalue volume')
        client = KeyValueClient(self.fqdn, self.port)
        create_reponse = self._create_volume(client)
        if create_reponse is None or create_reponse.operation.status != ydb_status_codes.StatusIds.StatusCode.SUCCESS:
            print('Create volume failed')
            print('response:', create_reponse)
            return False
        self._fill_init_data(client)
        self.begin_time = datetime.now()
        self.end_time = self.begin_time + timedelta(seconds=self.duration)
        print('Start Load; Begin Time:', self.begin_time, '; End Time:', self.end_time)
        return True

    def _post_stop(self):
        print('Stop Load; Drop Volume')
        client = KeyValueClient(self.fqdn, self.port)
        self._drop_volume(client)
        return True
    
    def get_status(self, response):
        if response is None:
            return None
        if hasattr(response, 'operation'):
            return response.operation.status
        return response.status

    def run_worker(self, worker_id):
        t = type(self)
        client = KeyValueClient(self.fqdn, self.port)
        # Determine available keys
        keys = list(self.init_data_keys)
        if not keys:
            default_pairs = t.INIT_DATA_PAIRS if self.kv_load_type == 'read' else t.INIT_INLINE_DATA_PAIRS
            keys = [self._get_init_pair_key(i) for i in range(default_pairs)]
        while datetime.now() < self.end_time:
            partition_id = random.randrange(0, self.partitions)
            key = random.choice(keys)
            size = random.randint(1, t.INIT_DATA_VALUE_SIZE)
            if size != t.INIT_DATA_VALUE_SIZE:
                offset = random.randint(0, t.INIT_DATA_VALUE_SIZE - size)
            else:
                offset = 0
            response = client.kv_read(self._volume_path(), partition_id, key, offset=offset, size=size, version=self.version)
            status = self.get_status(response)
            if response is None or status != ydb_status_codes.StatusIds.StatusCode.SUCCESS:
                print('Read failed')
                print('response:', response)
                return

    def _get_worker_action(self, worker_id):
        return lambda: self.run_worker(worker_id)

    def get_workload_thread_funcs(self):
        return [self._get_worker_action(i) for i in range(self.inflight)]
