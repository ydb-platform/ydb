from dataclasses import dataclass
from enum import Enum, IntEnum
import math
import re
from typing import FrozenSet, List, Optional

import yaml
from google.protobuf import json_format
from google.protobuf.descriptor import FieldDescriptor

import ydb.apps.dstool.protos.cluster_workload_pb2 as cluster_workload


DEFAULT_SLEEP_BETWEEN_ROUNDS_SECONDS = 1.0
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

    def matches(self, node_id, node_type, tenant):
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
            if node_type != NodeType.DYNAMIC or tenant is None:
                return False
            if self.exclude_tenants is not None and tenant in self.exclude_tenants:
                return False
            if self.only_tenants is not None and tenant not in self.only_tenants:
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
    wipe_vdisk: List[DiskActionConfig]
    evict_vdisk: List[DiskActionConfig]
    set_read_only: List[ReadOnlyActionConfig]
    restart_node: List[RestartNodeActionConfig]
    change_pdisk_key: List[DiskActionConfig]
    restart_pdisk: List[DiskActionConfig]
    obliterate_pdisk: List[DiskActionConfig]
    kill_tablet: List[KillTabletActionConfig]
    switch_pile: List[SwitchPileActionConfig]
    disconnect_pile: List[DisconnectPileActionConfig]
    disconnect_socket: List[DisconnectSocketActionConfig]


@dataclass(frozen=True)
class ClusterWorkloadConfig:
    sleep_between_rounds: float
    check_fail_model: bool
    random_seed: Optional[int]
    max_node_restarts_per_minute: Optional[int]
    actions: ClusterWorkloadActions

    @classmethod
    def from_proto(cls, proto, source='<proto>'):
        source = str(source)
        _validate_proto_config(proto, source)

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
                else None
            ),
            actions=ClusterWorkloadActions(
                wipe_vdisk=[_disk_action_from_proto(action) for action in actions.WipeVDisk],
                evict_vdisk=[_disk_action_from_proto(action) for action in actions.EvictVDisk],
                set_read_only=[_read_only_action_from_proto(action) for action in actions.SetReadOnly],
                restart_node=[_restart_node_action_from_proto(action) for action in actions.RestartNode],
                change_pdisk_key=[_disk_action_from_proto(action) for action in actions.ChangePDiskKey],
                restart_pdisk=[_disk_action_from_proto(action) for action in actions.RestartPDisk],
                obliterate_pdisk=[_disk_action_from_proto(action) for action in actions.ObliteratePDisk],
                kill_tablet=[_kill_tablet_action_from_proto(action) for action in actions.KillTablet],
                switch_pile=[_switch_pile_action_from_proto(action) for action in actions.SwitchPile],
                disconnect_pile=[_disconnect_pile_action_from_proto(action) for action in actions.DisconnectPile],
                disconnect_socket=[
                    _disconnect_socket_action_from_proto(action)
                    for action in actions.DisconnectSocket
                ],
            ),
        )


class _UniqueKeyLoader(yaml.SafeLoader):
    pass


def _construct_unique_mapping(loader, node, deep=False):
    loader.flatten_mapping(node)
    mapping = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        try:
            duplicate = key in mapping
        except TypeError:
            raise yaml.constructor.ConstructorError(
                'while constructing a mapping',
                node.start_mark,
                'found an unhashable key',
                key_node.start_mark,
            )
        if duplicate:
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


def _enum_values(enum_descriptor):
    return ', '.join('%s (%d)' % (item.name, item.number) for item in enum_descriptor.values)


def _enum_value_by_number(value, enum_descriptor):
    matching_values = [item for item in enum_descriptor.values if item.number == value]
    return matching_values[0] if matching_values else None


def _normalize_enum(value, enum_descriptor, path):
    if isinstance(value, str):
        canonical_value = _canonical_name(value)
        matching_values = [
            item
            for item in enum_descriptor.values
            if _canonical_name(item.name) == canonical_value
        ]
        if len(matching_values) != 1:
            raise ValueError(
                '%s: unknown enum value %r; expected one of %s'
                % (path, value, _enum_values(enum_descriptor))
            )
        return matching_values[0].name

    # bool is an int subclass, but it is not a protobuf enum number in a
    # workload document.  Converting known numbers to names also avoids relying
    # on the proto3 unknown-enum behaviour, which differs between protobuf
    # runtime versions.
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(
            '%s: expected an enum name or number; expected one of %s'
            % (path, _enum_values(enum_descriptor))
        )

    enum_value = _enum_value_by_number(value, enum_descriptor)
    if enum_value is None:
        raise ValueError(
            '%s: unknown numeric enum value %r; expected one of %s'
            % (path, value, _enum_values(enum_descriptor))
        )
    return enum_value.name


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
            if not isinstance(value, str):
                raise ValueError('%s: expected a protobuf duration string' % path)
            return value
        return _normalize_mapping(value, field.message_type, path)
    if field.type == FieldDescriptor.TYPE_ENUM:
        return _normalize_enum(value, field.enum_type, path)
    return _normalize_scalar(value, field, path)


_SIGNED_32_TYPES = frozenset((
    FieldDescriptor.TYPE_INT32,
    FieldDescriptor.TYPE_SFIXED32,
    FieldDescriptor.TYPE_SINT32,
))
_UNSIGNED_32_TYPES = frozenset((
    FieldDescriptor.TYPE_FIXED32,
    FieldDescriptor.TYPE_UINT32,
))
_SIGNED_64_TYPES = frozenset((
    FieldDescriptor.TYPE_INT64,
    FieldDescriptor.TYPE_SFIXED64,
    FieldDescriptor.TYPE_SINT64,
))
_UNSIGNED_64_TYPES = frozenset((
    FieldDescriptor.TYPE_FIXED64,
    FieldDescriptor.TYPE_UINT64,
))
_INTEGER_TYPES = _SIGNED_32_TYPES | _UNSIGNED_32_TYPES | _SIGNED_64_TYPES | _UNSIGNED_64_TYPES
_FLOAT_TYPES = frozenset((FieldDescriptor.TYPE_DOUBLE, FieldDescriptor.TYPE_FLOAT))


def _normalize_scalar(value, field, path):
    if field.type == FieldDescriptor.TYPE_BOOL:
        if not isinstance(value, bool):
            raise ValueError('%s: expected a boolean' % path)
        return value

    if field.type in _INTEGER_TYPES:
        if isinstance(value, bool) or not isinstance(value, int):
            raise ValueError('%s: expected an integer' % path)
        if field.type in _SIGNED_32_TYPES:
            minimum, maximum = -(2 ** 31), 2 ** 31 - 1
        elif field.type in _UNSIGNED_32_TYPES:
            minimum, maximum = 0, 2 ** 32 - 1
        elif field.type in _SIGNED_64_TYPES:
            minimum, maximum = -(2 ** 63), 2 ** 63 - 1
        else:
            minimum, maximum = 0, 2 ** 64 - 1
        if value < minimum or value > maximum:
            raise ValueError(
                '%s: integer %r is outside the allowed range [%d, %d]'
                % (path, value, minimum, maximum)
            )
        return value

    if field.type in _FLOAT_TYPES:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError('%s: expected a number' % path)
        try:
            finite = math.isfinite(value)
        except OverflowError:
            finite = False
        if not finite:
            raise ValueError('%s: expected a finite number' % path)
        return value

    if field.type == FieldDescriptor.TYPE_STRING:
        if not isinstance(value, str):
            raise ValueError('%s: expected a string' % path)
        return value

    if field.type == FieldDescriptor.TYPE_BYTES:
        if not isinstance(value, str):
            raise ValueError('%s: expected a base64-encoded string' % path)
        return value

    return value


def _normalize_mapping(value, descriptor, path):
    if not isinstance(value, dict):
        raise ValueError('%s: expected a mapping' % path)

    result = {}
    field_names = {}
    oneof_fields = {}
    for key, item in value.items():
        if not isinstance(key, str):
            raise ValueError('%s: field names must be strings' % path)

        field = _field_by_yaml_name(descriptor, key, path)
        if field.name in field_names:
            raise ValueError(
                '%s: field %r is specified more than once (as %r and %r)'
                % (path, field.name, field_names[field.name], key)
            )

        oneof = field.containing_oneof
        if oneof is not None and oneof.name in oneof_fields:
            raise ValueError(
                '%s: fields %r and %r belong to oneof %r and cannot both be set'
                % (path, oneof_fields[oneof.name], key, oneof.name)
            )

        field_names[field.name] = key
        if oneof is not None:
            oneof_fields[oneof.name] = key
        result[field.name] = _normalize_field_value(item, field, '%s.%s' % (path, key))
    return result


def parse_config(text, source='<string>'):
    source = str(source)
    try:
        document = yaml.load(text, Loader=_UniqueKeyLoader)
    except (yaml.YAMLError, TypeError, ValueError) as error:
        raise ValueError('%s: invalid YAML: %s' % (source, error)) from error

    if document is None:
        document = {}

    try:
        normalized = _normalize_mapping(
            document,
            cluster_workload.TClusterWorkloadConfig.DESCRIPTOR,
            source,
        )
    except ValueError:
        raise
    except (OverflowError, TypeError) as error:
        raise ValueError('%s: invalid workload configuration: %s' % (source, error)) from error

    try:
        proto = cluster_workload.TClusterWorkloadConfig()
        json_format.ParseDict(normalized, proto)
    except (json_format.ParseError, OverflowError, TypeError, ValueError) as error:
        raise ValueError('%s: invalid workload configuration: %s' % (source, error)) from error

    return ClusterWorkloadConfig.from_proto(proto, source)


def load_config(path):
    try:
        with open(path, 'r', encoding='utf-8') as stream:
            return parse_config(stream.read(), path)
    except (OSError, UnicodeError) as error:
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
    if duration.seconds < -315576000000 or duration.seconds > 315576000000:
        raise ValueError(
            '%s: invalid duration: seconds must be in range '
            '[-315576000000, 315576000000]' % path
        )
    if duration.nanos <= -1_000_000_000 or duration.nanos >= 1_000_000_000:
        raise ValueError(
            '%s: invalid duration: nanos must be in range '
            '[-999999999, 999999999]' % path
        )
    if (duration.seconds < 0 < duration.nanos) or (duration.nanos < 0 < duration.seconds):
        raise ValueError('%s: invalid duration: seconds and nanos have different signs' % path)

    value = _duration_seconds(duration)
    if value < 0 or (not allow_zero and value == 0):
        qualifier = 'non-negative' if allow_zero else 'positive'
        raise ValueError('%s must be %s' % (path, qualifier))


def _validate_weight(action, path):
    weight = _weight_from_proto(action)
    if not math.isfinite(weight) or weight <= 0:
        raise ValueError('%s.weight must be a finite positive value' % path)


_LINUX_TERMINATING_SIGNALS = {
    'INT': 2,
    'QUIT': 3,
    'ILL': 4,
    'TRAP': 5,
    'ABRT': 6,
    'BUS': 7,
    'FPE': 8,
    'KILL': 9,
    'USR1': 10,
    'SEGV': 11,
    'USR2': 12,
    'ALRM': 14,
    'TERM': 15,
    'STKFLT': 16,
    'XCPU': 24,
    'XFSZ': 25,
    'VTALRM': 26,
    'PROF': 27,
    'POLL': 29,
    'IO': 29,
    'PWR': 30,
    'SYS': 31,
}
_TERMINATING_SIGNAL_NUMBERS = frozenset(_LINUX_TERMINATING_SIGNALS.values())
_LINUX_REALTIME_SIGNAL_MIN = 34
_LINUX_REALTIME_SIGNAL_MAX = 64


def _is_terminating_signal_number(number):
    return (
        number in _TERMINATING_SIGNAL_NUMBERS
        or _LINUX_REALTIME_SIGNAL_MIN <= number <= _LINUX_REALTIME_SIGNAL_MAX
    )


def _restart_signal_number(value):
    normalized = value.upper()
    if re.fullmatch(r'[0-9]+', normalized):
        signal_number = int(normalized, 10)
    else:
        signal_name = normalized[3:] if normalized.startswith('SIG') else normalized
        realtime = re.fullmatch(r'RT(MIN|MAX)(?:([+-])([0-9]+))?', signal_name)
        if realtime:
            boundary, operator, offset_text = realtime.groups()
            offset = int(offset_text or 0)
            if boundary == 'MIN':
                signal_number = _LINUX_REALTIME_SIGNAL_MIN + (offset if operator != '-' else -offset)
            else:
                signal_number = _LINUX_REALTIME_SIGNAL_MAX + (offset if operator == '+' else -offset)
        else:
            signal_number = _LINUX_TERMINATING_SIGNALS.get(signal_name)
            if signal_number is None:
                return None
    return signal_number if _is_terminating_signal_number(signal_number) else None


def _validate_restart_signal(value, path):
    if _restart_signal_number(value) is None:
        raise ValueError(
            '%s.signal must name a terminating signal such as KILL, SIGKILL, or 9'
            % path
        )


def restart_signal_number(value):
    """Return the Linux signal number for a validated workload signal."""
    number = _restart_signal_number(value)
    if number is None:
        raise ValueError('invalid restart signal %r' % value)
    return number


def _validate_enum_number(value, field, path):
    if _enum_value_by_number(value, field.enum_type) is None:
        raise ValueError(
            '%s: unknown numeric enum value %r; expected one of %s'
            % (path, value, _enum_values(field.enum_type))
        )


def _validate_cms(action, path):
    if 'AskCMS' not in action.DESCRIPTOR.fields_by_name or not action.HasField('AskCMS'):
        return
    cms = action.AskCMS
    field = cms.DESCRIPTOR.fields_by_name['AvailabilityMode']
    _validate_enum_number(cms.AvailabilityMode, field, path + '.ask_cms.availability_mode')


def _validate_node_filter(node_filter, path):
    node_id_filter = node_filter.WhichOneof('NodeIdFilter')
    if node_id_filter == 'OnlyNodes' and not node_filter.OnlyNodes.Ids:
        raise ValueError('%s.only_nodes.ids must not be empty' % path)

    node_type_filter = node_filter.WhichOneof('NodeTypeFilter')
    if node_type_filter is not None:
        type_list = getattr(node_filter, node_type_filter)
        field = type_list.DESCRIPTOR.fields_by_name['Types']
        segment = 'exclude_types' if node_type_filter == 'ExcludeTypes' else 'only_types'
        for index, value in enumerate(type_list.Types):
            _validate_enum_number(value, field, '%s.%s.types[%d]' % (path, segment, index))
        if node_type_filter == 'OnlyTypes' and not type_list.Types:
            raise ValueError('%s.only_types.types must not be empty' % path)
        if (
            node_type_filter == 'ExcludeTypes'
            and set(type_list.Types) == {cluster_workload.STORAGE_NODE, cluster_workload.DYNAMIC_NODE}
        ):
            raise ValueError('%s.exclude_types excludes every node type' % path)

    tenant_filter = node_filter.WhichOneof('TenantFilter')
    if tenant_filter is None:
        return

    tenant_list = getattr(node_filter, tenant_filter)
    segment = 'exclude_tenants' if tenant_filter == 'ExcludeTenants' else 'only_tenants'
    if tenant_filter == 'OnlyTenants' and not tenant_list.Paths:
        raise ValueError('%s.only_tenants.paths must not be empty' % path)
    for index, tenant in enumerate(tenant_list.Paths):
        if not tenant:
            raise ValueError('%s.%s.paths[%d] must not be empty' % (path, segment, index))

    dynamic_is_excluded = (
        node_type_filter == 'ExcludeTypes'
        and cluster_workload.DYNAMIC_NODE in node_filter.ExcludeTypes.Types
    )
    dynamic_is_not_selected = (
        node_type_filter == 'OnlyTypes'
        and cluster_workload.DYNAMIC_NODE not in node_filter.OnlyTypes.Types
    )
    if dynamic_is_excluded or dynamic_is_not_selected:
        raise ValueError('%s tenant filter requires dynamic nodes' % path)


def _validate_disconnect_socket_filters(action, path):
    _validate_node_filter(action.Source, path + '.source')
    _validate_node_filter(action.Target, path + '.target')

    source_ids = (
        set(action.Source.OnlyNodes.Ids)
        if action.Source.WhichOneof('NodeIdFilter') == 'OnlyNodes'
        else None
    )
    target_ids = (
        set(action.Target.OnlyNodes.Ids)
        if action.Target.WhichOneof('NodeIdFilter') == 'OnlyNodes'
        else None
    )
    if source_ids is not None and target_ids is not None and len(source_ids | target_ids) < 2:
        raise ValueError('%s source and target filters must allow two distinct nodes' % path)


def _validate_tablet_filter(action, path):
    tablet_filter = action.WhichOneof('TabletFilter')
    if tablet_filter == 'OnlyTablets' and not action.OnlyTablets.Ids:
        raise ValueError('%s.only_tablets.ids must not be empty' % path)

    tablet_type_filter = action.WhichOneof('TabletTypeFilter')
    if tablet_type_filter is None:
        return

    type_list = getattr(action, tablet_type_filter)
    field = type_list.DESCRIPTOR.fields_by_name['Types']
    segment = (
        'exclude_tablet_types'
        if tablet_type_filter == 'ExcludeTabletTypes'
        else 'only_tablet_types'
    )
    for index, value in enumerate(type_list.Types):
        _validate_enum_number(value, field, '%s.%s.types[%d]' % (path, segment, index))
    if tablet_type_filter == 'OnlyTabletTypes' and not type_list.Types:
        raise ValueError('%s.only_tablet_types.types must not be empty' % path)


_ACTION_PATH_NAMES = {
    'WipeVDisk': 'wipe_vdisk',
    'EvictVDisk': 'evict_vdisk',
    'SetReadOnly': 'set_read_only',
    'RestartNode': 'restart_node',
    'ChangePDiskKey': 'change_pdisk_key',
    'RestartPDisk': 'restart_pdisk',
    'ObliteratePDisk': 'obliterate_pdisk',
    'KillTablet': 'kill_tablet',
    'SwitchPile': 'switch_pile',
    'DisconnectPile': 'disconnect_pile',
    'DisconnectSocket': 'disconnect_socket',
}


def _iter_proto_actions(actions):
    for field in actions.DESCRIPTOR.fields:
        action_name = _ACTION_PATH_NAMES.get(field.name, field.name)
        for index, action in enumerate(getattr(actions, field.name)):
            yield action_name, index, action


def _validate_proto_config(proto, source):
    if proto.HasField('SleepBetweenRounds'):
        _validate_duration(
            proto.SleepBetweenRounds,
            source + '.sleep_between_rounds',
            allow_zero=False,
        )

    action_count = 0
    restart_action_count = 0
    total_weight = 0.0
    for action_name, index, action in _iter_proto_actions(proto.Actions):
        action_count += 1
        path = '%s.actions.%s[%d]' % (source, action_name, index)
        _validate_weight(action, path)
        total_weight += _weight_from_proto(action)
        _validate_cms(action, path)

        if action.DESCRIPTOR is cluster_workload.TReadOnlyActionConfig.DESCRIPTOR:
            if not action.HasField('Component'):
                raise ValueError('%s.component is required' % path)
            component = action.DESCRIPTOR.fields_by_name['Component']
            _validate_enum_number(action.Component, component, path + '.component')
            if not action.HasField('ReadOnly'):
                raise ValueError('%s.read_only is required' % path)
        elif action.DESCRIPTOR is cluster_workload.TRestartNodeActionConfig.DESCRIPTOR:
            restart_action_count += 1
            if action.HasField('KeepDownFor'):
                _validate_duration(action.KeepDownFor, path + '.keep_down_for', allow_zero=True)
            signal = _signal_from_proto(action)
            _validate_restart_signal(signal, path)
            _validate_node_filter(action.Filter, path + '.filter')
        elif action.DESCRIPTOR is cluster_workload.TDisconnectSocketActionConfig.DESCRIPTOR:
            _validate_disconnect_socket_filters(action, path)
        elif action.DESCRIPTOR is cluster_workload.TSwitchPileActionConfig.DESCRIPTOR:
            mode = action.DESCRIPTOR.fields_by_name['Mode']
            _validate_enum_number(action.Mode, mode, path + '.mode')
        elif action.DESCRIPTOR is cluster_workload.TDisconnectPileActionConfig.DESCRIPTOR:
            if not action.HasField('Operation'):
                raise ValueError('%s.operation is required' % path)
            operation = action.DESCRIPTOR.fields_by_name['Operation']
            _validate_enum_number(action.Operation, operation, path + '.operation')
        elif action.DESCRIPTOR is cluster_workload.TKillTabletActionConfig.DESCRIPTOR:
            _validate_tablet_filter(action, path)

    if action_count == 0:
        raise ValueError('%s.actions must contain at least one action' % source)
    if (
        proto.HasField('MaxNodeRestartsPerMinute')
        and proto.MaxNodeRestartsPerMinute == 0
        and action_count == restart_action_count
    ):
        raise ValueError(
            '%s.max_node_restarts_per_minute is zero, so every configured action is disabled'
            % source
        )
    if not math.isfinite(total_weight):
        raise ValueError('%s.actions total weight must be finite' % source)


def _set_duration(duration, seconds):
    if isinstance(seconds, bool) or not isinstance(seconds, (int, float)):
        raise ValueError('sleep_before_rounds must be a finite number')
    try:
        finite = math.isfinite(seconds)
    except OverflowError:
        finite = False
    if not finite:
        raise ValueError('sleep_before_rounds must be a finite number')

    duration.seconds = math.trunc(seconds)
    duration.nanos = round((seconds - duration.seconds) * 1_000_000_000)
    if duration.nanos == 1_000_000_000:
        duration.seconds += 1
        duration.nanos = 0
    elif duration.nanos == -1_000_000_000:
        duration.seconds -= 1
        duration.nanos = 0


def _legacy_proto_from_args(args):
    proto = cluster_workload.TClusterWorkloadConfig()
    _set_duration(proto.SleepBetweenRounds, args.sleep_before_rounds)
    proto.CheckFailModel = not args.no_fail_model_check
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
    return ClusterWorkloadConfig.from_proto(_legacy_proto_from_args(args), '<legacy options>')
