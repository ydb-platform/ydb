"""gRPC-only NbsDbgLike client and crash-recoverable sequential runner."""
import copy
import json
import os
from pathlib import Path
import sys
import tempfile
import time
import uuid

from google.protobuf import json_format, text_format
from ydb.core.protos import load_test_pb2, test_shard_control_pb2 as control

Control = control.TNbsDbgLikeLoadControl
Result = control.TNbsDbgLikeLoadResult
TERMINAL = (Result.SUCCEEDED, Result.FAILED, Result.CANCELLED)
# Server-side bound on the single startup deadline.
MAX_STARTUP_TIMEOUT = 3600
CHECKPOINT_FIELDS = ('version', 'kind', 'database', 'node_id', 'incarnation', 'poll', 'startup',
                     'rpc_timeout', 'allow_io_errors', 'wait_timeout', 'trials')
TRIAL_FIELDS = ('inflight', 'trial', 'state', 'request')


class LoadError(Exception):
    def __init__(self, message, response=None):
        super().__init__(message)
        self.response = response


class WaitTimeout(LoadError):
    pass


def as_json(message):
    # Protobuf JSON encodes all uint64 values as strings.
    return json_format.MessageToDict(message, preserving_proto_field_name=True)


def histogram_percentiles_us(histogram):
    """Return p50/p95/p99 from the serialized HDR buckets used by the load service."""
    values = histogram.get('Values', ())
    counts = histogram.get('Counts', ())
    if not values or len(values) != len(counts):
        return None
    lowest = int(histogram.get('Lowest', 1))
    digits = int(histogram.get('SignificantDigits', 2))
    if lowest < 1 or not 1 <= digits <= 5:
        return None
    buckets = sorted((int(value), int(count)) for value, count in zip(values, counts))
    if any(value < 0 or count < 0 for value, count in buckets):
        return None
    total = sum(count for _, count in buckets)
    if not total:
        return None
    unit_magnitude = lowest.bit_length() - 1
    sub_bucket_magnitude = max(1, (2 * 10**digits - 1).bit_length())
    sub_bucket_count = 1 << sub_bucket_magnitude
    sub_bucket_mask = (sub_bucket_count - 1) << unit_magnitude
    ranks = [max(1, int((percentile / 100) * total + 0.5)) for percentile in (50, 95, 99)]
    result = []
    cumulative = 0
    for value, count in buckets:
        cumulative += count
        if cumulative < ranks[len(result)]:
            continue
        bucket = (value | sub_bucket_mask).bit_length() - unit_magnitude - sub_bucket_magnitude
        shift = bucket + unit_magnitude
        sub_bucket_index = value >> shift
        highest = (sub_bucket_index << shift) + (
            1 << (shift + (sub_bucket_index >= sub_bucket_count))) - 1
        while len(result) < len(ranks) and cumulative >= ranks[len(result)]:
            result.append(highest)
        if len(result) == len(ranks):
            return tuple(result)
    return None


def make_allocation(pool_name='ddp1', num_groups=32, target_num_vchunks=1,
                    vchunk_size_bytes=128 * 1024 * 1024, hosts_per_dbg=5,
                    tablet_storage_pools=(), ddisk_pool_name=None, pb_pool_name=None):
    tablet_storage_pools = tuple(tablet_storage_pools)
    ddisk_pool_name = ddisk_pool_name if ddisk_pool_name is not None else pool_name
    pb_pool_name = pb_pool_name if pb_pool_name is not None else pool_name
    if not ddisk_pool_name or not pb_pool_name or any(not pool for pool in tablet_storage_pools):
        raise LoadError('pool names must be nonempty')
    if (num_groups <= 0 or target_num_vchunks <= 0 or vchunk_size_bytes <= 0
            or vchunk_size_bytes % 4096 or not 3 <= hosts_per_dbg <= 5):
        raise LoadError('invalid allocation geometry')
    allocation = load_test_pb2.TEvLoadTestRequest.TNbsDbgLikeLoad.TAllocConfig()
    try:
        allocation.DDiskPoolName = ddisk_pool_name
        allocation.PersistentBufferDDiskPoolName = pb_pool_name
        allocation.NumDirectBlockGroups = num_groups
        allocation.TargetNumVChunks = target_num_vchunks
        allocation.VChunkSizeBytes = vchunk_size_bytes
        allocation.HostsPerDbg = hosts_per_dbg
        allocation.TabletStoragePools.extend(tablet_storage_pools)
    except (TypeError, ValueError) as error:
        raise LoadError('allocation option exceeds the protobuf field range') from error
    return allocation


def parse_target(spec):
    parts = spec.split('@')
    if len(parts) > 2 or any(not part or not part.isdecimal() for part in parts):
        raise LoadError('--target must be TABLET_ID or TABLET_ID@NODE_ID')
    tablet_id = int(parts[0])
    node_id = int(parts[1]) if len(parts) == 2 else None
    if not 0 < tablet_id < 2**64 or (node_id is not None and not 0 <= node_id < 2**32):
        raise LoadError('--target tablet or node ID is outside the protobuf range')
    return tablet_id, node_id


def inflight_values(inflight=None, inflight_from=None, inflight_to=None, trials=1):
    if trials <= 0 or (trials > 1 and trials % 2 == 0):
        raise LoadError('--trials must be positive and odd, as on the monitoring page')
    if inflight is not None:
        if inflight_from is not None or inflight_to is not None:
            raise LoadError('use --inflight or both --inflight-from and --inflight-to')
        values = [inflight]
    else:
        if (inflight_from is None or inflight_to is None or inflight_from <= 0
                or inflight_from > inflight_to or inflight_to >= 2**32):
            raise LoadError('use --inflight or both --inflight-from and --inflight-to')
        values = []
        value = inflight_from
        while value <= inflight_to:
            values.append(value)
            value *= 2
    if not values or any(not 0 < value < 2**32 for value in values) or len(values) * trials > 1024:
        raise LoadError('inflight values must be positive uint32 values with at most 1024 trials')
    return values


class AutoLifecycle:
    """Checkpoint an owned allocation before CREATE and delete only after confirmed drain."""
    def __init__(self, client, directory, state):
        self.client = client
        self.directory = Path(directory)
        self.state = state

    @classmethod
    def create(cls, client, directory, allocation, config, inflights, trials, startup,
               allow_io_errors, wait_timeout):
        directory = Path(directory)
        directory.mkdir(parents=True, exist_ok=False)
        state = {'version': 1, 'database': client.database, 'node_id': client.node_id,
                 'incarnation': client.incarnation, 'poll': client.poll,
                 'rpc_timeout': getattr(client.transport, 'timeout', 90),
                 'owner_index': uuid.uuid4().int & ((1 << 63) - 1),
                 'allocation': as_json(allocation), 'config': as_json(config),
                 'inflights': inflights, 'trials': trials, 'startup': startup,
                 'allow_io_errors': allow_io_errors, 'wait_timeout': wait_timeout,
                 'state': 'prepared'}
        lifecycle = cls(client, directory, state)
        lifecycle.save()
        return lifecycle

    @classmethod
    def resume(cls, client, directory):
        directory = Path(directory)
        state = json.loads((directory / 'auto.json').read_text())
        required = ('version', 'database', 'node_id', 'incarnation', 'owner_index',
                    'allocation', 'config', 'inflights', 'trials', 'startup', 'state')
        if state.get('version') != 1 or any(field not in state for field in required):
            raise LoadError('automatic allocation checkpoint is incomplete')
        client.database, client.node_id, client.incarnation = (
            state['database'], state['node_id'], state['incarnation'])
        client.poll = state['poll']
        if hasattr(client.transport, 'timeout'):
            client.transport.timeout = state['rpc_timeout']
        return cls(client, directory, state)

    def save(self):
        atomic_json(self.directory / 'auto.json', self.state)

    def prepare_runner(self):
        if (self.directory / 'checkpoint.json').exists():
            runner = Runner.resume(self.client, self.directory)
            self.check_runner(runner)
            return runner
        if self.state['state'] == 'deleted':
            raise LoadError('automatic allocation was deleted before a run checkpoint was saved')
        self.client.pin((Control.CREATE, Control.DESCRIBE, Control.START, Control.GET,
                         Control.STOP, Control.DELETE))
        self.state['state'] = 'creating'
        self.save()
        allocation = json_format.ParseDict(self.state['allocation'],
                                           load_test_pb2.TEvLoadTestRequest.TNbsDbgLikeLoad.TAllocConfig())
        self.client.call(self.client.request(Control.CREATE,
                         OwnerIndex=self.state['owner_index'], Allocation=allocation))
        ready = self.client.ready(self.state['owner_index'], self.state['startup'])
        tablet_id = ready.Tablets[0].TabletId
        if not tablet_id or (self.state.get('tablet_id') and self.state['tablet_id'] != str(tablet_id)):
            raise LoadError('automatic allocation tablet identity changed')
        self.state['tablet_id'] = str(tablet_id)
        self.state['state'] = 'ready'
        self.save()
        config = json_format.ParseDict(self.state['config'], load_test_pb2.TEvLoadTestRequest())
        config.NbsDbgLikeLoad.NbsDbgLikeTabletId = tablet_id
        validate_run_config(config)
        kind = 'sweep' if len(self.state['inflights']) * self.state['trials'] > 1 else 'run'
        return Runner.create(self.client, self.directory, config, self.state['startup'],
                             self.state['inflights'], self.state['trials'],
                             self.state['allow_io_errors'], self.state['wait_timeout'],
                             kind, existing=True)

    def check_runner(self, runner):
        state = self.state
        checkpoint = runner.checkpoint
        if (checkpoint['database'] != state['database'] or checkpoint['node_id'] != state['node_id']
                or checkpoint['incarnation'] != state['incarnation'] or not state.get('tablet_id')):
            raise LoadError('automatic allocation and run checkpoint identities differ')
        for index in range(len(checkpoint['trials'])):
            cmd = runner.trial_request(index).Load.NbsDbgLikeLoad
            if cmd.Targets or str(cmd.NbsDbgLikeTabletId) != state['tablet_id']:
                raise LoadError('automatic allocation and run tablet identities differ')
        if state['state'] == 'deleted' and any(
                entry['state'] != 'complete' for entry in checkpoint['trials']):
            raise LoadError('automatic allocation was deleted with unfinished trials')

    def cleanup(self, runner):
        if self.state['state'] == 'deleted':
            return
        if not runner.safe_to_delete():
            raise LoadError('termination is unconfirmed; allocation retained in %s' % self.directory)
        self.client.pin((Control.DESCRIBE, Control.DELETE))
        response = self.client.call(self.client.request(Control.LIST))
        tablets = [tablet for tablet in response.Tablets
                   if tablet.OwnerIndex == self.state['owner_index']]
        if not tablets and self.state['state'] == 'deleting':
            self.state['state'] = 'deleted'
            self.save()
            return
        if len(tablets) != 1 or str(tablets[0].TabletId) != self.state['tablet_id']:
            raise LoadError('automatic allocation identity changed; refusing deletion')
        self.state['state'] = 'deleting'
        self.save()
        self.client.call(self.client.request(Control.DELETE, OwnerIndex=self.state['owner_index']))
        self.state['state'] = 'deleted'
        self.save()


def make_run_config(tablet_id=None, targets=(), duration_seconds=None,
                    delay_before_measurements_seconds=None, num_groups_to_use=None,
                    max_inflight=None, read_ratio=None, sequential=False,
                    read_write_size_kib=None, stop_on_writes_done_count=None,
                    max_inflight_lsns=None, flush_batch_size=None, erase_batch_size=None,
                    sync_requests_batch_size=None, pbuffer_reply_timeout_us=None,
                    disable_replication=False, disable_checksums=False):
    if duration_seconds is None:
        raise LoadError('--duration-seconds is required')
    message = load_test_pb2.TEvLoadTestRequest()
    cmd = message.NbsDbgLikeLoad
    try:
        if tablet_id is not None:
            cmd.NbsDbgLikeTabletId = tablet_id
        for spec in targets:
            target_id, node_id = parse_target(spec)
            target = cmd.Targets.add()
            target.TabletId = target_id
            if node_id is not None:
                target.NodeId = node_id
        wc = cmd.WorkloadConfig
        for field, value in (
                ('DurationSeconds', duration_seconds),
                ('DelayBeforeMeasurementsSeconds', delay_before_measurements_seconds),
                ('NumDirectBlockGroupsToUse', num_groups_to_use),
                ('MaxInFlight', max_inflight),
                ('ReadRatio', read_ratio),
                ('ReadWriteSizeKiB', read_write_size_kib),
                ('StopOnWritesDoneCount', stop_on_writes_done_count)):
            if value is not None:
                setattr(wc, field, value)
        if sequential:
            wc.Sequential = True
        tc = wc.TabletConfig
        for field, value in (
                ('MaxInflightLsns', max_inflight_lsns),
                ('FlushBatchSize', flush_batch_size),
                ('EraseBatchSize', erase_batch_size),
                ('SyncRequestsBatchSize', sync_requests_batch_size),
                ('PBufferReplyTimeoutMicroseconds', pbuffer_reply_timeout_us)):
            if value is not None:
                setattr(tc, field, value)
        if disable_replication:
            tc.DisableReplication = True
        if disable_checksums:
            tc.EnableChecksums = False
    except (TypeError, ValueError) as error:
        raise LoadError('workload option exceeds the protobuf field range') from error
    return validate_run_config(message)


def validate_run_config(message, tablet_id=None):
    if message.WhichOneof('Command') != 'NbsDbgLikeLoad':
        raise LoadError('configuration must wrap only NbsDbgLikeLoad in TEvLoadTestRequest')
    if any(field.name != 'NbsDbgLikeLoad' for field, _ in message.ListFields()):
        raise LoadError('client-supplied service bookkeeping is forbidden')
    cmd = message.NbsDbgLikeLoad
    wc = cmd.WorkloadConfig
    if (cmd.HasField('Tag') or cmd.HasField('RequireReady') or cmd.HasField('StartupTimeoutSeconds')
            or wc.HasField('Tag') or wc.TabletConfig.HasField('ConfigurationId')):
        raise LoadError('client-supplied service bookkeeping is forbidden')
    if tablet_id is not None:
        if cmd.NbsDbgLikeTabletId or cmd.Targets:
            raise LoadError('--tablet-id is allowed only when the file supplies no targets')
        cmd.NbsDbgLikeTabletId = tablet_id
    targets = [item.TabletId for item in cmd.Targets]
    if cmd.NbsDbgLikeTabletId:
        if targets:
            raise LoadError('specify single tablet ID or Targets, not both')
        targets = [cmd.NbsDbgLikeTabletId]
    if not targets or any(not item for item in targets) or len(targets) != len(set(targets)):
        raise LoadError('nonzero unique target tablet IDs are required')
    if not wc.DurationSeconds or wc.DelayBeforeMeasurementsSeconds >= wc.DurationSeconds:
        raise LoadError('positive DurationSeconds greater than DelayBeforeMeasurementsSeconds required')
    if wc.TabletConfig.DisableReplication and wc.ReadRatio:
        raise LoadError('invalid read ratio or reads with replication disabled')
    if wc.ReadWriteSizeKiB < 4 or wc.ReadWriteSizeKiB % 4:
        raise LoadError('ReadWriteSizeKiB must be a positive multiple of 4')
    if wc.MaxInFlight <= 0:
        raise LoadError('MaxInFlight must be positive')
    return message


def parse_config(data, fmt, allocation=False, tablet_id=None):
    message = (load_test_pb2.TEvLoadTestRequest.TNbsDbgLikeLoad.TAllocConfig()
               if allocation else load_test_pb2.TEvLoadTestRequest())
    try:
        if fmt == 'json':
            json_format.Parse(data, message)
        elif fmt == 'textproto':
            text_format.Parse(data, message)
        else:
            raise LoadError('configuration format must be json or textproto')
    except (ValueError, text_format.ParseError, json_format.ParseError) as error:
        # Never include the supplied configuration in a diagnostic.
        raise LoadError('invalid configuration for the selected protobuf schema') from error
    if allocation:
        if (not message.NumDirectBlockGroups or not message.TargetNumVChunks
                or not message.VChunkSizeBytes or message.VChunkSizeBytes % 4096
                or not 3 <= message.HostsPerDbg <= 5):
            raise LoadError('invalid allocation geometry')
        if message.HasField('TabletId'):
            raise LoadError('allocation TabletId is assigned by Hive')
        return message
    return validate_run_config(message, tablet_id)


def read_config(path, fmt=None, allocation=False, tablet_id=None):
    if path == '-':
        if not fmt:
            raise LoadError('--config-format is required for stdin')
        data = sys.stdin.read()
    else:
        data = Path(path).read_text()
        fmt = fmt or ('json' if Path(path).suffix == '.json' else 'textproto')
    return parse_config(data, fmt, allocation, tablet_id)


def load_checkpoint(directory):
    # A truncated or foreign checkpoint must be reported, not raise KeyError.
    checkpoint = json.loads((Path(directory) / 'checkpoint.json').read_text())
    if not isinstance(checkpoint, dict) or any(field not in checkpoint for field in CHECKPOINT_FIELDS):
        raise LoadError('checkpoint is missing required fields')
    trials = checkpoint['trials']
    if not isinstance(trials, list) or not trials or any(
            not isinstance(entry, dict) or any(field not in entry for field in TRIAL_FIELDS)
            for entry in trials):
        raise LoadError('checkpoint is missing required fields')
    return checkpoint


def atomic_json(path, value):
    path = Path(path)
    fd, temporary = tempfile.mkstemp(prefix='.' + path.name, dir=path.parent)
    try:
        with os.fdopen(fd, 'w') as output:
            json.dump(value, output, indent=2, sort_keys=True)
            output.write('\n')
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


class GrpcTransport:
    """Uses dstool's endpoint, CA and credential loading, with no HTTP discovery."""
    def __init__(self, params, timeout=90, clock=time.monotonic):
        self.params = params
        self.timeout = timeout
        self.clock = clock

    def __call__(self, command, deadline=None):
        import grpc
        from ydb.core.protos.grpc_pb2_grpc import TGRpcServerStub
        endpoints = list(self.params.grpc_endpoints.values())
        if not endpoints:
            raise LoadError('an explicit grpc:// or grpcs:// endpoint is required')
        request = control.TTestShardControlRequest(NbsDbgLikeLoadControl=command)
        if self.params.token is not None:
            request.SecurityToken = self.params.token
        # Lifecycle mutations have no run dedup key. Do not retry an ambiguous
        # delete: an operator could have recreated its owner index meanwhile.
        retryable = command.Operation in (Control.CAPABILITIES, Control.LIST, Control.DESCRIBE,
                                           Control.START, Control.GET, Control.STOP)
        attempts = endpoints if retryable else endpoints[:1]
        deadline = min(deadline, self.clock() + self.timeout) if deadline is not None else self.clock() + self.timeout
        for index, endpoint in enumerate(attempts):
            remaining = deadline - self.clock()
            if remaining <= 0:
                raise WaitTimeout('TestShardControl RPC deadline expired; outcome may be unknown')
            request.NbsDbgLikeLoadControl.RpcTimeoutMs = max(1, int(remaining * 1000))
            options = [('grpc.max_receive_message_length', 256 << 20)]
            channel = (grpc.secure_channel(endpoint.host_with_grpc_port,
                       grpc.ssl_channel_credentials(self.params.get_cafile_data()), options)
                       if endpoint.protocol == 'grpcs' else
                       grpc.insecure_channel(endpoint.host_with_grpc_port, options))
            try:
                with channel:
                    remaining = deadline - self.clock()
                    if remaining <= 0:
                        raise WaitTimeout('TestShardControl RPC deadline expired; outcome may be unknown')
                    request.NbsDbgLikeLoadControl.RpcTimeoutMs = max(1, int(remaining * 1000))
                    response = TGRpcServerStub(channel).TestShardControl(request, timeout=remaining)
            except grpc.RpcError as error:
                if (retryable and index + 1 < len(attempts)
                        and error.code() in (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED)):
                    continue
                # gRPC details and request debug strings may contain credentials.
                error_type = WaitTimeout if error.code() == grpc.StatusCode.DEADLINE_EXCEEDED else LoadError
                raise error_type('TestShardControl transport failure (%s); outcome may be unknown'
                                % error.code().name) from None
            if response.Status != 1 and response.HasField('NbsDbgLikeLoadControl'):
                if self.params.token:
                    response.NbsDbgLikeLoadControl.Error = response.NbsDbgLikeLoadControl.Error.replace(self.params.token, '<redacted>')
                return response.NbsDbgLikeLoadControl
            if response.Status != 1:
                reason = response.ErrorReason
                if self.params.token:
                    reason = reason.replace(self.params.token, '<redacted>')
                error_type = WaitTimeout if reason.startswith('control deadline exceeded;') else LoadError
                raise error_type(reason or 'TestShardControl rejected request')
            if not response.HasField('NbsDbgLikeLoadControl'):
                raise LoadError('server does not support NbsDbgLikeLoadControl; deploy server support first')
            return response.NbsDbgLikeLoadControl
        raise LoadError('no gRPC endpoints')


class Client:
    def __init__(self, transport, database, node_id=0, incarnation='', poll=2,
                 clock=time.monotonic, sleep=time.sleep):
        self.transport = transport
        self.database = database
        self.node_id = node_id
        self.incarnation = incarnation
        self.poll = poll
        self.clock = clock
        self.sleep = sleep

    def request(self, operation, **fields):
        return Control(Operation=operation, Database=self.database,
                       CoordinatorNodeId=self.node_id, Incarnation=self.incarnation,
                       RpcTimeoutMs=max(1, int(getattr(self.transport, 'timeout', 90) * 1000)), **fields)

    def call(self, request, deadline=None):
        if deadline is not None:
            remaining = deadline - self.clock()
            if remaining <= 0:
                raise WaitTimeout('operation deadline expired; outcome may be unknown')
            request.RpcTimeoutMs = max(1, min(request.RpcTimeoutMs, int(remaining * 1000)))
        response = (self.transport(request, deadline=deadline) if isinstance(self.transport, GrpcTransport)
                    else self.transport(request))
        if deadline is not None and self.clock() >= deadline:
            raise WaitTimeout('operation deadline expired', response=response)
        if response.Status != 1:
            raise LoadError((response.Error or 'control request failed') +
                            ('; tablet IDs: ' + ', '.join(str(t.TabletId) for t in response.Tablets)
                             if response.Tablets else ''))
        if response.Database != self.database:
            raise LoadError('server database mismatch')
        if request.Operation != Control.CAPABILITIES:
            if response.Incarnation != self.incarnation or response.CoordinatorNodeId != self.node_id:
                raise LoadError('coordinator identity changed; remote workers may still be running')
        return response

    def pin(self, operations):
        if not self.database:
            raise LoadError('--database is required')
        response = self.call(self.request(Control.CAPABILITIES))
        if response.ProtocolVersion != 1 or not set(operations).issubset(response.Operations):
            raise LoadError('unsupported server capabilities')
        if self.incarnation and (response.Incarnation != self.incarnation or response.CoordinatorNodeId != self.node_id):
            raise LoadError('coordinator incarnation lost; remote workers may still be running')
        if not response.Incarnation or not response.CoordinatorNodeId:
            raise LoadError('server returned incomplete coordinator identity')
        self.node_id, self.incarnation = response.CoordinatorNodeId, response.Incarnation
        return response

    def get(self, request_id, deadline=None):
        response = self.call(self.request(Control.GET, RequestId=request_id), deadline)
        if not response.HasField('Run'):
            raise LoadError('server returned no run state')
        return response

    def wait(self, request_id, timeout, deadline=None):
        deadline = deadline if deadline is not None else self.clock() + timeout
        while True:
            response = self.get(request_id, deadline)
            if response.Run.State in TERMINAL:
                return response
            remaining = deadline - self.clock()
            if remaining <= 0:
                raise WaitTimeout('run wait timed out')
            self.sleep(min(self.poll, remaining))

    def stop(self, request_id, timeout):
        deadline = self.clock() + timeout
        self.call(self.request(Control.STOP, RequestId=request_id), deadline)
        response = self.wait(request_id, timeout, deadline)
        if not response.Run.TerminationConfirmed:
            raise LoadError('termination not confirmed; remote workers may still be running', response=response)
        return response

    def ready(self, owner_index, timeout):
        deadline = self.clock() + timeout
        while True:
            response = self.call(self.request(Control.DESCRIBE, OwnerIndex=owner_index), deadline)
            if len(response.Tablets) != 1:
                raise LoadError('describe returned no tablet identity')
            summary = response.Tablets[0].Summary
            if (summary.AutomationProtocolVersion >= 1 and summary.NumDirectBlockGroups > 0
                    and summary.NumReadyDirectBlockGroups == summary.NumDirectBlockGroups):
                return response
            if self.clock() >= deadline:
                raise WaitTimeout('tablet readiness timed out; allocation retained')
            self.sleep(min(self.poll, deadline - self.clock()))


def verdict(response, allow_io_errors=False):
    run = response.Run
    if not response.HasField('Run') or run.State != Result.SUCCEEDED:
        return False
    if run.ExecutionError or not run.HasField('Stats') or not run.TerminationConfirmed:
        return False
    return allow_io_errors or not fully_failed_directions(run.Stats)


def fully_failed_directions(stats):
    return tuple(direction for direction, good, errors in (
        ('writes', stats.WritesOk, stats.WritesErr),
        ('reads', stats.ReadsOk, stats.ReadsErr)) if errors and not good)


def measured_io_errors(response):
    if not response.HasField('Run') or not response.Run.HasField('Stats'):
        return None
    stats = response.Run.Stats
    result = {}
    for direction, good, errors in (
            ('writes', stats.WritesOk, stats.WritesErr),
            ('reads', stats.ReadsOk, stats.ReadsErr)):
        total = good + errors
        result[direction] = {'count': str(errors), 'total': str(total),
                             'percent': 100 * errors / total if total else None}
    return result


def format_error_percent(percent):
    if percent is None:
        return '-'
    return '<0.01%' if 0 < percent < 0.01 else '%.2f%%' % percent


def verdict_reason(response, allow_io_errors=False, outcome=None):
    if outcome:
        return 'trial %s' % outcome
    if not response.HasField('Run'):
        return 'server returned no run state'
    run = response.Run
    if run.State != Result.SUCCEEDED:
        state = {Result.IN_PROGRESS: 'IN_PROGRESS', Result.STOPPING: 'STOPPING',
                 Result.FAILED: 'FAILED', Result.CANCELLED: 'CANCELLED'}.get(run.State, 'unknown')
        return 'workload execution %s%s' % (state, ': ' + run.ExecutionError if run.ExecutionError else '')
    if run.ExecutionError:
        return 'workload execution error: %s' % run.ExecutionError
    if not run.TerminationConfirmed:
        return 'worker drain was not confirmed'
    if not run.HasField('Stats'):
        return 'measured statistics are missing'
    failed = fully_failed_directions(run.Stats)
    if not allow_io_errors and failed:
        errors = measured_io_errors(response)
        details = ['%s (%s errors)' % (direction, errors[direction]['count']) for direction in failed]
        return '100% measured I/O errors in ' + ', '.join(details)
    return None


def run_result_payload(response, allow_io_errors=False, outcome=None, **fields):
    payload = dict(fields)
    payload['response'] = as_json(response)
    payload['measured_io_errors'] = measured_io_errors(response)
    if response.HasField('Run') and response.Run.State in TERMINAL:
        payload['passed'] = verdict(response, allow_io_errors) and not outcome
        if not payload['passed']:
            payload['failure_reason'] = verdict_reason(response, allow_io_errors, outcome)
    else:
        payload['passed'] = None
    return payload


class Runner:
    def __init__(self, client, directory, checkpoint):
        self.client = client
        self.directory = Path(directory)
        self.checkpoint = checkpoint

    @classmethod
    def create(cls, client, directory, config, startup=60, inflights=None, trials=1,
               allow_io_errors=False, wait_timeout=None, kind='run', existing=False):
        if trials <= 0 or (inflights is not None and (not inflights or any(n <= 0 for n in inflights))):
            raise LoadError('positive trial count and inflight values required')
        directory = Path(directory)
        if existing:
            if not directory.is_dir() or (directory / 'checkpoint.json').exists():
                raise LoadError('prepared artifact directory is missing or already submitted')
        else:
            directory.mkdir(parents=True, exist_ok=False)
        values = inflights if inflights is not None else [config.NbsDbgLikeLoad.WorkloadConfig.MaxInFlight]
        entries = []
        for value in values:
            for trial in range(trials):
                trial_config = copy.deepcopy(config)
                if inflights is not None:
                    trial_config.NbsDbgLikeLoad.WorkloadConfig.MaxInFlight = value
                request = client.request(Control.START, RequestId=str(uuid.uuid4()),
                                         Load=trial_config, StartupTimeoutSeconds=startup)
                entries.append({'inflight': value, 'trial': trial + 1, 'state': 'prepared',
                                'request': as_json(request)})
        checkpoint = {'version': 1, 'kind': kind, 'database': client.database,
                      'node_id': client.node_id, 'incarnation': client.incarnation,
                      'poll': client.poll, 'startup': startup,
                      'rpc_timeout': getattr(client.transport, 'timeout', 90),
                      'allow_io_errors': allow_io_errors, 'wait_timeout': wait_timeout,
                      'trials': entries}
        runner = cls(client, directory, checkpoint)
        atomic_json(directory / 'config.json', as_json(config))
        runner.save()
        return runner

    @classmethod
    def resume(cls, client, directory, kind=None, reconcile=True):
        checkpoint = load_checkpoint(directory)
        if (checkpoint.get('version') != 1 or checkpoint.get('kind') not in ('run', 'sweep')
                or (kind is not None and checkpoint['kind'] != kind)):
            raise LoadError('checkpoint version or command mismatch')
        client.database = checkpoint['database']
        client.node_id = checkpoint['node_id']
        client.incarnation = checkpoint['incarnation']
        client.poll = checkpoint['poll']
        if hasattr(client.transport, 'timeout'):
            client.transport.timeout = checkpoint['rpc_timeout']
        runner = cls(client, directory, checkpoint)
        if reconcile:
            runner.reconcile_results()
        return runner

    @classmethod
    def from_handle(cls, client, directory, reconcile=True):
        checkpoint = load_checkpoint(directory)
        if checkpoint.get('kind') != 'run' or len(checkpoint['trials']) != 1:
            raise LoadError('use explicit request identity for a sweep trial')
        return cls.resume(client, directory, 'run', reconcile)

    def trial_request(self, index=0):
        entry = self.checkpoint['trials'][index]
        request = json_format.ParseDict(entry['request'], Control())
        if (request.Database != self.checkpoint['database']
                or request.CoordinatorNodeId != self.checkpoint['node_id']
                or request.Incarnation != self.checkpoint['incarnation']):
            raise LoadError('checkpoint request identity mismatch')
        return request

    def saved_response(self, index=0):
        entry = self.checkpoint['trials'][index]
        filename = entry.get('result')
        if not filename:
            if entry['state'] == 'complete':
                raise LoadError('completed checkpoint has no saved result')
            return None
        response = json_format.ParseDict(json.loads((self.directory / filename).read_text()),
                                         control.TNbsDbgLikeLoadControlResponse())
        request = self.trial_request(index)
        if (response.Database != request.Database or response.Incarnation != request.Incarnation
                or response.CoordinatorNodeId != request.CoordinatorNodeId
                or response.RequestId != request.RequestId or response.Run.State not in TERMINAL):
            raise LoadError('saved result does not match checkpoint identity or is not terminal')
        return response

    def reconcile_results(self):
        # A process can die after result fsync and before checkpoint fsync.
        # Recover that saved result without depending on server retention.
        changed = False
        for index, entry in enumerate(self.checkpoint['trials']):
            filename = 'result-%04d.json' % index
            path = self.directory / filename
            if entry['state'] == 'complete' or not path.exists():
                continue
            entry['result'], entry['state'] = filename, 'complete'
            self.saved_response(index)
            changed = True
        if changed:
            self.save()

    def save(self):
        atomic_json(self.directory / 'checkpoint.json', self.checkpoint)

    def record_result(self, index, response):
        entry = self.checkpoint['trials'][index]
        request = self.trial_request(index)
        if (response.Database != request.Database or response.Incarnation != request.Incarnation
                or response.CoordinatorNodeId != request.CoordinatorNodeId
                or response.RequestId != request.RequestId or response.Run.State not in TERMINAL):
            raise LoadError('terminal result identity mismatch')
        filename = 'result-%04d.json' % index
        # Result is fsynced before a checkpoint can claim it is complete.
        atomic_json(self.directory / filename, as_json(response))
        entry['result'] = filename
        entry['state'] = 'complete'
        self.save()

    def safe_to_delete(self):
        for index, entry in enumerate(self.checkpoint['trials']):
            if entry['state'] == 'prepared':
                continue
            if entry['state'] != 'complete' or not self.saved_response(index).Run.TerminationConfirmed:
                return False
        return True

    def execute(self, emit, no_wait=False):
        allow_errors = self.checkpoint['allow_io_errors']
        for index, entry in enumerate(self.checkpoint['trials']):
            request = self.trial_request(index)
            timeout = self.checkpoint['wait_timeout'] or (
                request.StartupTimeoutSeconds + request.Load.NbsDbgLikeLoad.WorkloadConfig.DurationSeconds + 210)
            if entry['state'] == 'complete':
                response = self.saved_response(index)
            else:
                deadline = self.client.clock() + timeout
                try:
                    if entry['state'] == 'prepared':
                        entry['state'] = 'submitting'
                        self.save()
                        response = self.client.call(request, deadline)
                        entry['state'] = 'accepted'
                        self.save()
                    else:
                        # A submitting checkpoint may precede an ambiguous RPC.
                        # GET must resolve it; unknown never launches replacement work.
                        response = self.client.get(request.RequestId, deadline)
                    if no_wait:
                        if response.Run.State in TERMINAL:
                            self.record_result(index, response)
                        emit(run_result_payload(response, allow_errors, entry.get('outcome'),
                             handle={'Database': request.Database, 'CoordinatorNodeId': request.CoordinatorNodeId,
                                     'Incarnation': request.Incarnation, 'RequestId': request.RequestId},
                             output_dir=str(self.directory)))
                        return (entry.get('outcome') is None and
                                (response.Run.State not in TERMINAL or verdict(response, allow_errors)))
                    if response.Run.State not in TERMINAL:
                        response = self.client.wait(request.RequestId, timeout, deadline)
                    self.record_result(index, response)
                except (WaitTimeout, KeyboardInterrupt) as error:
                    entry['outcome'] = 'timed_out' if isinstance(error, WaitTimeout) else 'interrupted'
                    self.save()
                    late = error.response if isinstance(error, WaitTimeout) else None
                    if late is not None and late.HasField('Run') and late.Run.State in TERMINAL:
                        self.record_result(index, late)
                        if late.Run.TerminationConfirmed:
                            raise LoadError('trial timed out; terminal results saved', response=late) from error
                    try:
                        stopped = self.client.stop(request.RequestId, request.StartupTimeoutSeconds + 210)
                        if stopped.Run.State in TERMINAL:
                            self.record_result(index, stopped)
                    except (LoadError, KeyboardInterrupt) as stopped_error:
                        if (isinstance(stopped_error, LoadError) and stopped_error.response is not None
                                and stopped_error.response.HasField('Run')
                                and stopped_error.response.Run.State in TERMINAL):
                            self.record_result(index, stopped_error.response)
                            if stopped_error.response.Run.TerminationConfirmed:
                                raise LoadError('trial timed out; cancellation completed; results saved',
                                                response=stopped_error.response) from error
                        entry['outcome'] = 'unresolved; termination not confirmed'
                        self.save()
                        raise LoadError('interrupted; cancellation outcome unresolved; resume saved handle') from None
                    raise LoadError('interrupted or timed out; cancellation completed; results saved') from error
            result = run_result_payload(response, allow_errors, entry.get('outcome'),
                                        inflight=entry['inflight'], trial=entry['trial'],
                                        result=entry.get('result'))
            passed = result['passed']
            emit(result)
            if not passed:
                return False
        if self.checkpoint['kind'] == 'sweep':
            summaries = []
            # Preserve ordering and every trial, including repeated inflight values.
            entries = self.checkpoint['trials']
            groups = []
            for entry in entries:
                if entry['trial'] == 1:
                    groups.append([])
                response = json_format.ParseDict(json.loads((self.directory / entry['result']).read_text()),
                                                 control.TNbsDbgLikeLoadControlResponse())
                groups[-1].append((response.Run.WriteIOPS, entry))
            for group in groups:
                selected = sorted(group, key=lambda item: item[0])[len(group) // 2][1]
                summaries.append({'inflight': selected['inflight'], 'median_trial': selected['trial'],
                                  'result': selected['result']})
            atomic_json(self.directory / 'summary.json', summaries)
            emit({'summary': summaries})
        return True
