from dataclasses import dataclass
from enum import Enum, IntEnum
import math
import re
from typing import FrozenSet, Optional, Tuple

import yaml
from google.protobuf import json_format
from google.protobuf.descriptor import FieldDescriptor

import ydb.apps.dstool.protos.cluster_workload_pb2 as cluster_workload


DEFAULT_SLEEP_BETWEEN_ROUNDS_SECONDS = 1.0
DEFAULT_MAX_NODE_RESTARTS_PER_MINUTE = 3
DEFAULT_WEIGHT = 1.0
DEFAULT_RESTART_SIGNAL = 'KILL'


class CmsAvailabilityMode(IntEnum):
    MAX_AVAILABILITY = cluster_workload.MODE_MAX_AVAILABILITY
    KEEP_AVAILABLE = cluster_workload.MODE_KEEP_AVAILABLE
    FORCE_RESTART = cluster_workload.MODE_FORCE_RESTART
    SMART_AVAILABILITY = cluster_workload.MODE_SMART_AVAILABILITY


class NodeType(Enum):
    STORAGE = 'storage'
    DYNAMIC = 'dynamic'


class ReadOnlyComponent(Enum):
    VDISK = 'vdisk'
    PDISK = 'pdisk'


class SwitchPileMode(Enum):
    SOFT = 'soft'
    HARD = 'hard'


class DisconnectPileOperation(Enum):
    DISCONNECT = 'disconnect'
    RECONNECT = 'reconnect'


class TabletType(Enum):
    BLOB_DEPOT = 'blob_depot'
    DATA_SHARD = 'data_shard'


@dataclass(frozen=True)
class CmsPermissionConfig:
    availability_mode: CmsAvailabilityMode


@dataclass(frozen=True)
class NodeFilter:
    exclude_node_ids: Optional[FrozenSet[int]] = None
    only_node_ids: Optional[FrozenSet[int]] = None
    exclude_types: Optional[FrozenSet[NodeType]] = None
    only_types: Optional[FrozenSet[NodeType]] = None
    exclude_tenants: Optional[FrozenSet[str]] = None
    only_tenants: Optional[FrozenSet[str]] = None

    def matches(self, node_id, node_type, tenants):
        if self.exclude_node_ids is not None and node_id in self.exclude_node_ids:
            return False
        if self.only_node_ids is not None and node_id not in self.only_node_ids:
            return False
        if self.exclude_types is not None and node_type in self.exclude_types:
            return False
        if self.only_types is not None and node_type not in self.only_types:
            return False

        has_tenant_filter = self.exclude_tenants is not None or self.only_tenants is not None
        if has_tenant_filter:
            if node_type != NodeType.DYNAMIC:
                return False
            tenant_set = set(tenants)
            if self.exclude_tenants is not None and tenant_set.intersection(self.exclude_tenants):
                return False
            if self.only_tenants is not None and not tenant_set.intersection(self.only_tenants):
                return False

        return True


@dataclass(frozen=True)
class DiskActionConfig:
    weight: float
    ask_cms: Optional[CmsPermissionConfig]


@dataclass(frozen=True)
class ReadOnlyActionConfig:
    weight: float
    ask_cms: Optional[CmsPermissionConfig]
    component: ReadOnlyComponent
    read_only: bool


@dataclass(frozen=True)
class RestartNodeActionConfig:
    weight: float
    ask_cms: Optional[CmsPermissionConfig]
    keep_down_for: float
    signal: str
    node_filter: NodeFilter


@dataclass(frozen=True)
class DisconnectSocketActionConfig:
    weight: float
    source: NodeFilter
    target: NodeFilter
    symmetrical: bool


@dataclass(frozen=True)
class SwitchPileActionConfig:
    weight: float
    mode: SwitchPileMode


@dataclass(frozen=True)
class DisconnectPileActionConfig:
    weight: float
    operation: DisconnectPileOperation
    pile: Optional[int]


@dataclass(frozen=True)
class KillTabletActionConfig:
    weight: float
    exclude_tablet_ids: Optional[FrozenSet[int]]
    only_tablet_ids: Optional[FrozenSet[int]]
    exclude_tablet_types: Optional[FrozenSet[TabletType]]
    only_tablet_types: Optional[FrozenSet[TabletType]]

    def matches(self, tablet_id, tablet_type):
        if self.exclude_tablet_ids is not None and tablet_id in self.exclude_tablet_ids:
            return False
        if self.only_tablet_ids is not None and tablet_id not in self.only_tablet_ids:
            return False
        if self.exclude_tablet_types is not None and tablet_type in self.exclude_tablet_types:
            return False
        if self.only_tablet_types is not None and tablet_type not in self.only_tablet_types:
            return False
        return True


@dataclass(frozen=True)
class ClusterWorkloadActions:
    wipe_vdisk: Tuple[DiskActionConfig, ...]
    evict_vdisk: Tuple[DiskActionConfig, ...]
    set_read_only: Tuple[ReadOnlyActionConfig, ...]
    restart_node: Tuple[RestartNodeActionConfig, ...]
    change_pdisk_key: Tuple[DiskActionConfig, ...]
    restart_pdisk: Tuple[DiskActionConfig, ...]
    obliterate_pdisk: Tuple[DiskActionConfig, ...]
    kill_tablet: Tuple[KillTabletActionConfig, ...]
    switch_pile: Tuple[SwitchPileActionConfig, ...]
    disconnect_pile: Tuple[DisconnectPileActionConfig, ...]
    disconnect_socket: Tuple[DisconnectSocketActionConfig, ...]


@dataclass(frozen=True)
class ClusterWorkloadConfig:
    sleep_between_rounds: float
    check_fail_model: bool
    random_seed: Optional[int]
    max_node_restarts_per_minute: int
    actions: ClusterWorkloadActions

    @classmethod
    def from_proto(cls, proto):
        _validate_proto_config(proto)

        actions = proto.Actions
        return cls(
            sleep_between_rounds=(
                _duration_seconds(proto.SleepBetweenRounds)
                if proto.HasField('SleepBetweenRounds')
                else DEFAULT_SLEEP_BETWEEN_ROUNDS_SECONDS
            ),
            check_fail_model=proto.CheckFailModel if proto.HasField('CheckFailModel') else True,
            random_seed=proto.RandomSeed if proto.HasField('RandomSeed') else None,
            max_node_restarts_per_minute=(
                proto.MaxNodeRestartsPerMinute
                if proto.HasField('MaxNodeRestartsPerMinute')
                else DEFAULT_MAX_NODE_RESTARTS_PER_MINUTE
            ),
            actions=ClusterWorkloadActions(
                wipe_vdisk=tuple(_disk_action_from_proto(action) for action in actions.WipeVDisk),
                evict_vdisk=tuple(_disk_action_from_proto(action) for action in actions.EvictVDisk),
                set_read_only=tuple(_read_only_action_from_proto(action) for action in actions.SetReadOnly),
                restart_node=tuple(_restart_node_action_from_proto(action) for action in actions.RestartNode),
                change_pdisk_key=tuple(_disk_action_from_proto(action) for action in actions.ChangePDiskKey),
                restart_pdisk=tuple(_disk_action_from_proto(action) for action in actions.RestartPDisk),
                obliterate_pdisk=tuple(_disk_action_from_proto(action) for action in actions.ObliteratePDisk),
                kill_tablet=tuple(_kill_tablet_action_from_proto(action) for action in actions.KillTablet),
                switch_pile=tuple(_switch_pile_action_from_proto(action) for action in actions.SwitchPile),
                disconnect_pile=tuple(_disconnect_pile_action_from_proto(action) for action in actions.DisconnectPile),
                disconnect_socket=tuple(_disconnect_socket_action_from_proto(action) for action in actions.DisconnectSocket),
            ),
        )


class _UniqueKeyLoader(yaml.SafeLoader):
    pass


def _construct_unique_mapping(loader, node, deep=False):
    mapping = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        if key in mapping:
            raise yaml.constructor.ConstructorError(
                'while constructing a mapping',
                node.start_mark,
                'found duplicate key %r' % key,
                key_node.start_mark,
            )
        mapping[key] = loader.construct_object(value_node, deep=deep)
    return mapping


_UniqueKeyLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
    _construct_unique_mapping,
)


def _canonical_name(name):
    return re.sub(r'[^a-zA-Z0-9]', '', name).lower()


def _field_by_yaml_name(descriptor, name, path):
    canonical_name = _canonical_name(name)
    matching_fields = [
        field
        for field in descriptor.fields
        if canonical_name in (_canonical_name(field.name), _canonical_name(field.json_name))
    ]
    if len(matching_fields) != 1:
        raise ValueError('%s: unknown field %r' % (path, name))
    return matching_fields[0]


def _normalize_enum(value, enum_descriptor, path):
    if isinstance(value, str):
        canonical_value = _canonical_name(value)
        matching_values = [
            item.name
            for item in enum_descriptor.values
            if _canonical_name(item.name) == canonical_value
        ]
        if len(matching_values) != 1:
            raise ValueError(
                '%s: unknown value %r; expected one of %s'
                % (path, value, ', '.join(item.name for item in enum_descriptor.values))
            )
        return matching_values[0]
    return value


def _normalize_field_value(value, field, path):
    if field.label == FieldDescriptor.LABEL_REPEATED:
        if not isinstance(value, list):
            raise ValueError('%s: expected a list' % path)
        return [
            _normalize_singular_value(item, field, '%s[%d]' % (path, index))
            for index, item in enumerate(value)
        ]
    return _normalize_singular_value(value, field, path)


def _normalize_singular_value(value, field, path):
    if field.type == FieldDescriptor.TYPE_MESSAGE:
        if field.message_type.full_name == 'google.protobuf.Duration':
            return value
        return _normalize_mapping(value, field.message_type, path)
    if field.type == FieldDescriptor.TYPE_ENUM:
        return _normalize_enum(value, field.enum_type, path)
    return value


def _normalize_mapping(value, descriptor, path):
    if not isinstance(value, dict):
        raise ValueError('%s: expected a mapping' % path)

    result = {}
    for key, item in value.items():
        if not isinstance(key, str):
            raise ValueError('%s: field names must be strings' % path)
        field = _field_by_yaml_name(descriptor, key, path)
        if field.name in result:
            raise ValueError('%s: field %r is specified more than once' % (path, field.name))
        result[field.name] = _normalize_field_value(item, field, '%s.%s' % (path, key))
    return result


def parse_config(text, source='<string>'):
    try:
        document = yaml.load(text, Loader=_UniqueKeyLoader)
    except yaml.YAMLError as error:
        raise ValueError('%s: invalid YAML: %s' % (source, error)) from error

    if document is None:
        document = {}

    try:
        normalized = _normalize_mapping(
            document,
            cluster_workload.TClusterWorkloadConfig.DESCRIPTOR,
            source,
        )
        proto = cluster_workload.TClusterWorkloadConfig()
        json_format.ParseDict(normalized, proto)
    except (json_format.ParseError, TypeError, ValueError) as error:
        if isinstance(error, ValueError) and str(error).startswith(source):
            raise
        raise ValueError('%s: invalid workload configuration: %s' % (source, error)) from error

    return ClusterWorkloadConfig.from_proto(proto)


def load_config(path):
    try:
        with open(path, 'r', encoding='utf-8') as stream:
            return parse_config(stream.read(), path)
    except OSError as error:
        raise ValueError('failed to read workload configuration %r: %s' % (path, error)) from error


def _duration_seconds(duration):
    return duration.seconds + duration.nanos / 1_000_000_000


def _weight_from_proto(action):
    return action.Weight if action.HasField('Weight') else DEFAULT_WEIGHT


def _signal_from_proto(action):
    return action.Signal if action.HasField('Signal') else DEFAULT_RESTART_SIGNAL


def _cms_from_proto(action):
    if not action.HasField('AskCMS'):
        return None
    return CmsPermissionConfig(
        availability_mode=CmsAvailabilityMode(action.AskCMS.AvailabilityMode),
    )


def _node_filter_from_proto(proto):
    node_id_filter = proto.WhichOneof('NodeIdFilter')
    node_type_filter = proto.WhichOneof('NodeTypeFilter')
    tenant_filter = proto.WhichOneof('TenantFilter')
    type_mapping = {
        cluster_workload.STORAGE_NODE: NodeType.STORAGE,
        cluster_workload.DYNAMIC_NODE: NodeType.DYNAMIC,
    }
    return NodeFilter(
        exclude_node_ids=(
            frozenset(proto.ExcludeNodes.Ids)
            if node_id_filter == 'ExcludeNodes'
            else None
        ),
        only_node_ids=(
            frozenset(proto.OnlyNodes.Ids)
            if node_id_filter == 'OnlyNodes'
            else None
        ),
        exclude_types=(
            frozenset(type_mapping[value] for value in proto.ExcludeTypes.Types)
            if node_type_filter == 'ExcludeTypes'
            else None
        ),
        only_types=(
            frozenset(type_mapping[value] for value in proto.OnlyTypes.Types)
            if node_type_filter == 'OnlyTypes'
            else None
        ),
        exclude_tenants=(
            frozenset(proto.ExcludeTenants.Paths)
            if tenant_filter == 'ExcludeTenants'
            else None
        ),
        only_tenants=(
            frozenset(proto.OnlyTenants.Paths)
            if tenant_filter == 'OnlyTenants'
            else None
        ),
    )


def _disk_action_from_proto(action):
    return DiskActionConfig(
        weight=_weight_from_proto(action),
        ask_cms=_cms_from_proto(action),
    )


def _read_only_action_from_proto(action):
    component_mapping = {
        cluster_workload.TReadOnlyActionConfig.VDISK: ReadOnlyComponent.VDISK,
        cluster_workload.TReadOnlyActionConfig.PDISK: ReadOnlyComponent.PDISK,
    }
    return ReadOnlyActionConfig(
        weight=_weight_from_proto(action),
        ask_cms=_cms_from_proto(action),
        component=component_mapping[action.Component],
        read_only=action.ReadOnly,
    )


def _restart_node_action_from_proto(action):
    return RestartNodeActionConfig(
        weight=_weight_from_proto(action),
        ask_cms=_cms_from_proto(action),
        keep_down_for=(
            _duration_seconds(action.KeepDownFor)
            if action.HasField('KeepDownFor')
            else 0
        ),
        signal=_signal_from_proto(action),
        node_filter=_node_filter_from_proto(action.Filter),
    )


def _disconnect_socket_action_from_proto(action):
    return DisconnectSocketActionConfig(
        weight=_weight_from_proto(action),
        source=_node_filter_from_proto(action.Source),
        target=_node_filter_from_proto(action.Target),
        symmetrical=action.Symmetrical if action.HasField('Symmetrical') else False,
    )


def _switch_pile_action_from_proto(action):
    mode_mapping = {
        cluster_workload.TSwitchPileActionConfig.SOFT: SwitchPileMode.SOFT,
        cluster_workload.TSwitchPileActionConfig.HARD: SwitchPileMode.HARD,
    }
    return SwitchPileActionConfig(
        weight=_weight_from_proto(action),
        mode=mode_mapping[action.Mode],
    )


def _disconnect_pile_action_from_proto(action):
    operation_mapping = {
        cluster_workload.TDisconnectPileActionConfig.DISCONNECT: DisconnectPileOperation.DISCONNECT,
        cluster_workload.TDisconnectPileActionConfig.RECONNECT: DisconnectPileOperation.RECONNECT,
    }
    return DisconnectPileActionConfig(
        weight=_weight_from_proto(action),
        operation=operation_mapping[action.Operation],
        pile=action.Pile if action.HasField('Pile') else None,
    )


def _kill_tablet_action_from_proto(action):
    tablet_id_filter = action.WhichOneof('TabletFilter')
    tablet_type_filter = action.WhichOneof('TabletTypeFilter')
    type_mapping = {
        cluster_workload.BLOB_DEPOT: TabletType.BLOB_DEPOT,
        cluster_workload.DATA_SHARD: TabletType.DATA_SHARD,
    }
    return KillTabletActionConfig(
        weight=_weight_from_proto(action),
        exclude_tablet_ids=(
            frozenset(action.ExcludeTablets.Ids)
            if tablet_id_filter == 'ExcludeTablets'
            else None
        ),
        only_tablet_ids=(
            frozenset(action.OnlyTablets.Ids)
            if tablet_id_filter == 'OnlyTablets'
            else None
        ),
        exclude_tablet_types=(
            frozenset(type_mapping[value] for value in action.ExcludeTabletTypes.Types)
            if tablet_type_filter == 'ExcludeTabletTypes'
            else None
        ),
        only_tablet_types=(
            frozenset(type_mapping[value] for value in action.OnlyTabletTypes.Types)
            if tablet_type_filter == 'OnlyTabletTypes'
            else None
        ),
    )


def tablet_type_from_name(name):
    normalized_name = ''.join(character for character in name.lower() if character.isalnum())
    return {
        'blobdepot': TabletType.BLOB_DEPOT,
        'datashard': TabletType.DATA_SHARD,
    }.get(normalized_name)


def _validate_duration(duration, path, allow_zero):
    try:
        duration.ToTimedelta()
    except (OverflowError, ValueError) as error:
        raise ValueError('%s: invalid duration: %s' % (path, error)) from error

    value = _duration_seconds(duration)
    if value < 0 or (not allow_zero and value == 0):
        qualifier = 'non-negative' if allow_zero else 'positive'
        raise ValueError('%s must be %s' % (path, qualifier))


def _validate_weight(action, path):
    weight = _weight_from_proto(action)
    if not math.isfinite(weight) or weight <= 0:
        raise ValueError('%s.weight must be a finite positive value' % path)


def _iter_proto_actions(actions):
    for field in actions.DESCRIPTOR.fields:
        for index, action in enumerate(getattr(actions, field.name)):
            yield field.name, index, action


def _validate_proto_config(proto):
    if proto.HasField('SleepBetweenRounds'):
        _validate_duration(proto.SleepBetweenRounds, 'sleep_between_rounds', allow_zero=False)

    action_count = 0
    for action_name, index, action in _iter_proto_actions(proto.Actions):
        action_count += 1
        path = 'actions.%s[%d]' % (action_name, index)
        _validate_weight(action, path)

        if action.DESCRIPTOR.full_name == 'NDSTool.TReadOnlyActionConfig':
            if not action.HasField('Component'):
                raise ValueError('%s.component is required' % path)
            if not action.HasField('ReadOnly'):
                raise ValueError('%s.read_only is required' % path)
        elif action.DESCRIPTOR.full_name == 'NDSTool.TRestartNodeActionConfig':
            if action.HasField('KeepDownFor'):
                _validate_duration(action.KeepDownFor, path + '.keep_down_for', allow_zero=True)
            signal = _signal_from_proto(action)
            if not signal or not re.fullmatch(r'[A-Za-z0-9]+', signal):
                raise ValueError('%s.signal must be a non-empty signal name or number' % path)
        elif action.DESCRIPTOR.full_name == 'NDSTool.TDisconnectPileActionConfig':
            if not action.HasField('Operation'):
                raise ValueError('%s.operation is required' % path)

    if action_count == 0:
        raise ValueError('actions must contain at least one action')


def _set_duration(duration, seconds):
    duration.seconds = math.floor(seconds)
    duration.nanos = round((seconds - duration.seconds) * 1_000_000_000)
    if duration.nanos == 1_000_000_000:
        duration.seconds += 1
        duration.nanos = 0


def _legacy_proto_from_args(args):
    proto = cluster_workload.TClusterWorkloadConfig()
    _set_duration(proto.SleepBetweenRounds, args.sleep_before_rounds)
    proto.CheckFailModel = not args.no_fail_model_check
    proto.MaxNodeRestartsPerMinute = DEFAULT_MAX_NODE_RESTARTS_PER_MINUTE
    actions = proto.Actions

    if not args.disable_wipes:
        actions.WipeVDisk.add()
    if not args.disable_evicts:
        actions.EvictVDisk.add()
    if not args.disable_readonly:
        for read_only in (True, False):
            action = actions.SetReadOnly.add()
            action.Component = cluster_workload.TReadOnlyActionConfig.VDISK
            action.ReadOnly = read_only
    if not args.disable_restarts:
        action = actions.RestartNode.add()
        action.Weight = args.weight_restarts
        action.Signal = args.kill_signal
    if args.enable_pdisk_encryption_keys_changes:
        actions.ChangePDiskKey.add()
    if args.enable_restart_pdisks:
        actions.RestartPDisk.add()
    if args.enable_readonly_pdisks:
        for read_only in (True, False):
            action = actions.SetReadOnly.add()
            action.Component = cluster_workload.TReadOnlyActionConfig.PDISK
            action.ReadOnly = read_only
    if args.enable_kill_tablets:
        actions.KillTablet.add().Weight = args.weight_kill_tablets
    if args.enable_kill_blob_depot:
        action = actions.KillTablet.add()
        action.OnlyTabletTypes.Types.append(cluster_workload.BLOB_DEPOT)
    if args.enable_soft_switch_piles:
        actions.SwitchPile.add().Mode = cluster_workload.TSwitchPileActionConfig.SOFT
    if args.enable_hard_switch_piles:
        actions.SwitchPile.add().Mode = cluster_workload.TSwitchPileActionConfig.HARD
    if args.enable_disconnect_piles:
        action = actions.DisconnectPile.add()
        action.Operation = cluster_workload.TDisconnectPileActionConfig.DISCONNECT
        if args.fixed_pile_for_disconnect is not None:
            action.Pile = args.fixed_pile_for_disconnect
    if args.enable_soft_switch_piles or args.enable_hard_switch_piles or args.enable_disconnect_piles:
        actions.DisconnectPile.add().Operation = cluster_workload.TDisconnectPileActionConfig.RECONNECT

    return proto


def parse_workload_config(args):
    if args.config_file:
        return load_config(args.config_file)
    return ClusterWorkloadConfig.from_proto(_legacy_proto_from_args(args))
