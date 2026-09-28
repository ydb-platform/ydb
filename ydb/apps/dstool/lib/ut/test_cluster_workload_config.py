from types import SimpleNamespace
import re

import pytest

import ydb.apps.dstool.lib.cluster_workload_config as workload_config


def test_parse_complete_config():
    config = workload_config.parse_config(
        '''
sleep_between_rounds: 1.5s
check_fail_model: false
random_seed: 42
max_node_restarts_per_minute: 7
actions:
  wipe_vdisk:
    - weight: 2.5
      ask_cms:
        availability_mode: mode_keep_available
  evict_v_disk:
    - {}
  set_read_only:
    - component: vdisk
      read_only: true
    - component: pdisk
      read_only: false
  restart_node:
    - keep_down_for: 2s
      signal: term
      filter:
        only_nodes:
          ids: [1, 2]
        only_types:
          types: [storage_node]
  change_pdisk_key:
    - {}
  restart_pdisk:
    - {}
  obliterate_pdisk:
    - {}
  kill_tablet:
    - only_tablets:
        ids: [72075186224037888]
      only_tablet_types:
        types: [data_shard]
  switch_pile:
    - mode: hard
  disconnect_pile:
    - operation: disconnect
      pile: 3
  disconnect_socket:
    - symmetrical: true
      source:
        only_tenants:
          paths: [/Root/database]
      target:
        exclude_nodes:
          ids: [10]
''',
        'workload.yaml',
    )

    assert config.sleep_between_rounds == 1.5
    assert not config.check_fail_model
    assert config.random_seed == 42
    assert config.max_node_restarts_per_minute == 7

    assert len(config.actions.wipe_vdisk) == 1
    assert isinstance(config.actions.wipe_vdisk, list)
    assert config.actions.wipe_vdisk[0].weight == 2.5
    assert (
        config.actions.wipe_vdisk[0].ask_cms.availability_mode
        == workload_config.CmsAvailabilityMode.KEEP_AVAILABLE
    )
    assert len(config.actions.evict_vdisk) == 1

    vdisk_read_only, pdisk_read_only = config.actions.set_read_only
    assert vdisk_read_only.component == workload_config.ReadOnlyComponent.VDISK
    assert vdisk_read_only.read_only
    assert pdisk_read_only.component == workload_config.ReadOnlyComponent.PDISK
    assert not pdisk_read_only.read_only

    restart = config.actions.restart_node[0]
    assert restart.keep_down_for == 2
    assert restart.signal == 'term'
    assert restart.node_filter.only_node_ids == frozenset([1, 2])
    assert restart.node_filter.only_types == frozenset([workload_config.NodeType.STORAGE])

    tablet = config.actions.kill_tablet[0]
    assert tablet.only_tablet_ids == frozenset([72075186224037888])
    assert tablet.only_tablet_types == frozenset([workload_config.TabletType.DATA_SHARD])
    assert config.actions.switch_pile[0].mode == workload_config.SwitchPileMode.HARD
    assert config.actions.disconnect_pile[0].pile == 3
    assert config.actions.disconnect_socket[0].symmetrical


def test_defaults_and_optional_false_values_are_preserved():
    config = workload_config.parse_config(
        '''
check_fail_model: false
actions:
  disconnect_socket:
    - symmetrical: false
'''
    )

    assert config.sleep_between_rounds == workload_config.DEFAULT_SLEEP_BETWEEN_ROUNDS_SECONDS
    assert not config.check_fail_model
    assert config.random_seed is None
    assert config.max_node_restarts_per_minute is None
    assert config.actions.disconnect_socket[0].weight == workload_config.DEFAULT_WEIGHT
    assert not config.actions.disconnect_socket[0].symmetrical


def test_known_numeric_enum_values_are_accepted():
    config = workload_config.parse_config(
        '''
actions:
  set_read_only:
    - component: 1
      read_only: true
      ask_cms: {availability_mode: 2}
  restart_node:
    - filter:
        only_types: {types: [1]}
  kill_tablet:
    - only_tablet_types: {types: [0]}
  switch_pile:
    - mode: 1
  disconnect_pile:
    - operation: 1
'''
    )

    read_only = config.actions.set_read_only[0]
    assert read_only.component == workload_config.ReadOnlyComponent.PDISK
    assert read_only.ask_cms.availability_mode == workload_config.CmsAvailabilityMode.FORCE_RESTART
    assert config.actions.restart_node[0].node_filter.only_types == frozenset(
        [workload_config.NodeType.DYNAMIC]
    )
    assert config.actions.kill_tablet[0].only_tablet_types == frozenset(
        [workload_config.TabletType.BLOB_DEPOT]
    )
    assert config.actions.switch_pile[0].mode == workload_config.SwitchPileMode.HARD
    assert (
        config.actions.disconnect_pile[0].operation
        == workload_config.DisconnectPileOperation.RECONNECT
    )


@pytest.mark.parametrize(
    'yaml_text, error',
    [
        ('actions: {}', r'workload\.yaml\.actions must contain at least one action'),
        (
            'actions: {wipe_vdisk: [{weight: 0}]}',
            r'workload\.yaml\.actions\.wipe_vdisk\[0\]\.weight must be a finite positive value',
        ),
        (
            'actions: {wipe_vdisk: [{weight: .inf}]}',
            r'workload\.yaml\.actions\.wipe_vdisk\[0\]\.weight: expected a finite number',
        ),
        (
            'actions: {set_read_only: [{component: vdisk}]}',
            r'workload\.yaml\.actions\.set_read_only\[0\]\.read_only is required',
        ),
        (
            'actions: {set_read_only: [{read_only: false}]}',
            r'workload\.yaml\.actions\.set_read_only\[0\]\.component is required',
        ),
        (
            'actions: {disconnect_pile: [{}]}',
            r'workload\.yaml\.actions\.disconnect_pile\[0\]\.operation is required',
        ),
        (
            'actions: {restart_node: [{signal: SIG-TERM}]}',
            r'workload\.yaml\.actions\.restart_node\[0\]\.signal',
        ),
        (
            'actions: {restart_node: [{keep_down_for: -1s}]}',
            r'workload\.yaml\.actions\.restart_node\[0\]\.keep_down_for must be non-negative',
        ),
        (
            'sleep_between_rounds: 0s\nactions: {wipe_vdisk: [{}]}',
            r'workload\.yaml\.sleep_between_rounds must be positive',
        ),
        (
            'unknown: true\nactions: {wipe_vdisk: [{}]}',
            r'workload\.yaml: unknown field',
        ),
        (
            'actions: {wipe_vdisk: [{weight: 1, weight: 2}]}',
            r'duplicate key',
        ),
        (
            'actions: {wipe_vdisk: {}, evict_vdisk: [{}]}',
            r'workload\.yaml\.actions\.wipe_vdisk: expected a list',
        ),
        (
            'actions: {wipe_vdisk: null, evict_vdisk: [{}]}',
            r'workload\.yaml\.actions\.wipe_vdisk: expected a list',
        ),
        (
            '''
actions:
  restart_node:
    - filter:
        only_nodes: {ids: [1]}
        exclude_nodes: {ids: [2]}
''',
            r'workload\.yaml\.actions\.restart_node\[0\]\.filter: fields .* oneof',
        ),
    ],
)
def test_invalid_config(yaml_text, error):
    with pytest.raises(ValueError, match=error):
        workload_config.parse_config(yaml_text, 'workload.yaml')


@pytest.mark.parametrize(
    'yaml_text, path',
    [
        (
            'actions: {set_read_only: [{component: 99, read_only: true}]}',
            'workload.yaml.actions.set_read_only[0].component',
        ),
        (
            'actions: {wipe_vdisk: [{ask_cms: {availability_mode: 99}}]}',
            'workload.yaml.actions.wipe_vdisk[0].ask_cms.availability_mode',
        ),
        (
            'actions: {restart_node: [{filter: {only_types: {types: [99]}}}]}',
            'workload.yaml.actions.restart_node[0].filter.only_types.types[0]',
        ),
        (
            'actions: {kill_tablet: [{only_tablet_types: {types: [99]}}]}',
            'workload.yaml.actions.kill_tablet[0].only_tablet_types.types[0]',
        ),
        (
            'actions: {switch_pile: [{mode: 99}]}',
            'workload.yaml.actions.switch_pile[0].mode',
        ),
        (
            'actions: {disconnect_pile: [{operation: 99}]}',
            'workload.yaml.actions.disconnect_pile[0].operation',
        ),
    ],
)
def test_unknown_numeric_enum_has_full_yaml_path(yaml_text, path):
    with pytest.raises(ValueError) as error:
        workload_config.parse_config(yaml_text, 'workload.yaml')

    message = str(error.value)
    assert message.startswith(path + ': unknown numeric enum value 99')
    assert 'expected one of' in message


@pytest.mark.parametrize('value', ['true', '1.5'])
def test_non_integer_numeric_enum_is_rejected_with_path(value):
    with pytest.raises(
        ValueError,
        match=re.escape('workload.yaml.actions.switch_pile[0].mode: expected an enum name or number'),
    ):
        workload_config.parse_config(
            'actions: {switch_pile: [{mode: %s}]}' % value,
            'workload.yaml',
        )


@pytest.mark.parametrize(
    'yaml_text, path, expected_type',
    [
        (
            'check_fail_model: 1\nactions: {wipe_vdisk: [{}]}',
            'workload.yaml.check_fail_model',
            'boolean',
        ),
        (
            'random_seed: 1.0\nactions: {wipe_vdisk: [{}]}',
            'workload.yaml.random_seed',
            'integer',
        ),
        (
            'max_node_restarts_per_minute: true\nactions: {wipe_vdisk: [{}]}',
            'workload.yaml.max_node_restarts_per_minute',
            'integer',
        ),
        (
            'actions: {wipe_vdisk: [{weight: true}]}',
            'workload.yaml.actions.wipe_vdisk[0].weight',
            'number',
        ),
        (
            'actions: {set_read_only: [{component: vdisk, read_only: 1}]}',
            'workload.yaml.actions.set_read_only[0].read_only',
            'boolean',
        ),
        (
            'actions: {restart_node: [{signal: 9}]}',
            'workload.yaml.actions.restart_node[0].signal',
            'string',
        ),
        (
            'actions: {restart_node: [{filter: {only_nodes: {ids: [1.0]}}}]}',
            'workload.yaml.actions.restart_node[0].filter.only_nodes.ids[0]',
            'integer',
        ),
        (
            'actions: {disconnect_socket: [{source: {only_tenants: {paths: [1]}}}]}',
            'workload.yaml.actions.disconnect_socket[0].source.only_tenants.paths[0]',
            'string',
        ),
    ],
)
def test_scalar_types_are_validated_at_the_yaml_path(yaml_text, path, expected_type):
    with pytest.raises(ValueError) as error:
        workload_config.parse_config(yaml_text, 'workload.yaml')

    article = 'an' if expected_type == 'integer' else 'a'
    assert str(error.value) == '%s: expected %s %s' % (path, article, expected_type)


def test_integer_range_is_validated_at_the_yaml_path():
    with pytest.raises(ValueError) as error:
        workload_config.parse_config(
            'random_seed: -1\nactions: {wipe_vdisk: [{}]}',
            'workload.yaml',
        )

    assert str(error.value).startswith('workload.yaml.random_seed: integer -1 is outside')


@pytest.mark.parametrize(
    'signal_value',
    ['KILL', 'kill', 'SIGKILL', "'9'", 'TERM', 'SIGTERM', 'RTMIN+1', 'SIGRTMAX-1'],
)
def test_terminating_restart_signals_are_accepted(signal_value):
    config = workload_config.parse_config(
        'actions: {restart_node: [{signal: %s}]}' % signal_value,
        'workload.yaml',
    )

    expected = signal_value.strip("'")
    assert config.actions.restart_node[0].signal == expected
    assert workload_config.restart_signal_number(expected) in range(1, 65)


@pytest.mark.parametrize(
    'signal_value',
    [
        "'0'", "'1'", "'13'", 'HUP', 'SIGHUP', 'PIPE', 'SIGPIPE', 'STOP',
        'SIGSTOP', 'CONT', 'SIGCONT', 'CHLD', 'NOT_A_SIGNAL',
        "'999999'", "'９'", "'²'", 'RTMIN-1', 'RTMAX+1',
    ],
)
def test_non_terminating_or_unknown_restart_signals_are_rejected(signal_value):
    with pytest.raises(
        ValueError,
        match=re.escape(
            'workload.yaml.actions.restart_node[0].signal must name a terminating signal'
        ),
    ):
        workload_config.parse_config(
            'actions: {restart_node: [{signal: %s}]}' % signal_value,
            'workload.yaml',
        )


def test_canonical_field_aliases_cannot_specify_a_field_twice():
    with pytest.raises(ValueError) as error:
        workload_config.parse_config(
            '''
actions:
  wipe_vdisk: [{}]
  wipe-v-disk: [{}]
''',
            'workload.yaml',
        )

    message = str(error.value)
    assert message.startswith('workload.yaml.actions: field')
    assert 'specified more than once' in message
    assert 'wipe_vdisk' in message
    assert 'wipe-v-disk' in message


def test_nested_canonical_field_aliases_cannot_be_repeated():
    with pytest.raises(ValueError, match='specified more than once'):
        workload_config.parse_config(
            '''
actions:
  wipe_vdisk:
    - ask_cms: {}
      ask-cms: {}
'''
        )


def test_total_action_weight_must_be_finite():
    with pytest.raises(ValueError, match=r'workload\.yaml\.actions total weight must be finite'):
        workload_config.parse_config(
            '''
actions:
  wipe_vdisk:
    - {weight: 1.0e+308}
    - {weight: 1.0e+308}
''',
            'workload.yaml',
        )


@pytest.mark.parametrize(
    'yaml_text, error',
    [
        (
            'actions: {restart_node: [{filter: {only_nodes: {ids: []}}}]}',
            'only_nodes.ids must not be empty',
        ),
        (
            'actions: {disconnect_socket: [{source: {only_types: {types: []}}}]}',
            'only_types.types must not be empty',
        ),
        (
            'actions: {restart_node: [{filter: {only_tenants: {paths: []}}}]}',
            'only_tenants.paths must not be empty',
        ),
        (
            'actions: {kill_tablet: [{only_tablets: {ids: []}}]}',
            'only_tablets.ids must not be empty',
        ),
        (
            'actions: {kill_tablet: [{only_tablet_types: {types: []}}]}',
            'only_tablet_types.types must not be empty',
        ),
        (
            'actions: {restart_node: [{filter: {only_types: {types: [storage_node]}, '
            'only_tenants: {paths: [/Root/db]}}}]}',
            'tenant filter requires dynamic nodes',
        ),
        (
            'actions: {restart_node: [{filter: {exclude_types: '
            '{types: [storage_node, dynamic_node]}}}]}',
            'exclude_types excludes every node type',
        ),
        (
            'max_node_restarts_per_minute: 0\nactions: {restart_node: [{}]}',
            'every configured action is disabled',
        ),
        (
            'actions: {disconnect_socket: [{source: {only_nodes: {ids: [7]}}, '
            'target: {only_nodes: {ids: [7]}}}]}',
            'source and target filters must allow two distinct nodes',
        ),
    ],
)
def test_statically_ineligible_actions_are_rejected(yaml_text, error):
    with pytest.raises(ValueError, match=error):
        workload_config.parse_config(yaml_text, 'workload.yaml')


def test_zero_restart_limit_is_allowed_when_another_action_can_run():
    config = workload_config.parse_config(
        '''
max_node_restarts_per_minute: 0
actions:
  restart_node: [{}]
  wipe_vdisk: [{}]
''',
        'workload.yaml',
    )

    assert config.max_node_restarts_per_minute == 0


def test_from_proto_validates_unknown_enum_and_invalid_duration_with_paths():
    proto = workload_config.cluster_workload.TClusterWorkloadConfig()
    proto.Actions.SwitchPile.add().Mode = 99
    with pytest.raises(ValueError, match=r'proto\.config\.actions\.switch_pile\[0\]\.mode'):
        workload_config.ClusterWorkloadConfig.from_proto(proto, 'proto.config')

    proto = workload_config.cluster_workload.TClusterWorkloadConfig()
    proto.Actions.WipeVDisk.add()
    proto.SleepBetweenRounds.seconds = 1
    proto.SleepBetweenRounds.nanos = -1
    with pytest.raises(ValueError, match=r'proto\.config\.sleep_between_rounds: invalid duration'):
        workload_config.ClusterWorkloadConfig.from_proto(proto, 'proto.config')


def _legacy_args(**overrides):
    values = {
        'config_file': None,
        'disable_wipes': False,
        'disable_readonly': False,
        'disable_evicts': False,
        'disable_restarts': False,
        'enable_pdisk_encryption_keys_changes': False,
        'enable_kill_tablets': False,
        'enable_kill_blob_depot': False,
        'enable_restart_pdisks': False,
        'enable_readonly_pdisks': False,
        'kill_signal': 'KILL',
        'sleep_before_rounds': 1,
        'no_fail_model_check': False,
        'enable_soft_switch_piles': False,
        'enable_hard_switch_piles': False,
        'enable_disconnect_piles': False,
        'fixed_pile_for_disconnect': None,
        'weight_restarts': 1.0,
        'weight_kill_tablets': 1.0,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def test_legacy_defaults_are_translated_to_config():
    config = workload_config.parse_workload_config(_legacy_args())

    assert len(config.actions.wipe_vdisk) == 1
    assert len(config.actions.evict_vdisk) == 1
    assert len(config.actions.set_read_only) == 2
    assert len(config.actions.restart_node) == 1
    assert config.sleep_between_rounds == 1
    assert config.check_fail_model
    assert config.max_node_restarts_per_minute is None


def test_invalid_legacy_values_are_validated():
    with pytest.raises(ValueError, match='finite positive'):
        workload_config.parse_workload_config(_legacy_args(weight_restarts=-1))

    with pytest.raises(ValueError, match='sleep_before_rounds must be a finite number'):
        workload_config.parse_workload_config(_legacy_args(sleep_before_rounds=float('nan')))

    with pytest.raises(ValueError, match='sleep_between_rounds must be positive'):
        workload_config.parse_workload_config(_legacy_args(sleep_before_rounds=-0.5))


def test_node_filter_combines_id_type_and_tenant_filters():
    node_filter = workload_config.NodeFilter(
        only_node_ids=frozenset([2, 3]),
        only_types=frozenset([workload_config.NodeType.DYNAMIC]),
        only_tenants=frozenset(['/Root/database']),
    )

    assert node_filter.matches(2, workload_config.NodeType.DYNAMIC, '/Root/database')
    assert not node_filter.matches(1, workload_config.NodeType.STORAGE, '/Root/database')
    assert not node_filter.matches(3, workload_config.NodeType.DYNAMIC, '/Root/other')
    assert not node_filter.matches(3, workload_config.NodeType.DYNAMIC, None)


def test_tablet_filter_combines_id_and_type_filters():
    action = workload_config.KillTabletActionConfig(
        weight=1,
        exclude_tablet_ids=frozenset([10]),
        only_tablet_ids=None,
        exclude_tablet_types=None,
        only_tablet_types=frozenset([workload_config.TabletType.DATA_SHARD]),
    )

    assert action.matches(11, workload_config.TabletType.DATA_SHARD)
    assert not action.matches(10, workload_config.TabletType.DATA_SHARD)
    assert not action.matches(11, workload_config.TabletType.BLOB_DEPOT)


def test_yaml_config_ignores_legacy_options(tmp_path):
    config_path = tmp_path / 'workload.yaml'
    config_path.write_text('actions: {wipe_vdisk: [{}]}', encoding='utf-8')

    config = workload_config.parse_workload_config(
        _legacy_args(
            config_file=str(config_path),
            disable_wipes=True,
            weight_restarts=-1,
        )
    )

    assert len(config.actions.wipe_vdisk) == 1
    assert not config.actions.restart_node


def test_load_config_wraps_file_errors(tmp_path):
    missing = tmp_path / 'missing.yaml'
    with pytest.raises(ValueError, match='failed to read workload configuration'):
        workload_config.load_config(missing)

    invalid_utf8 = tmp_path / 'invalid.yaml'
    invalid_utf8.write_bytes(b'\xff')
    with pytest.raises(ValueError, match='failed to read workload configuration'):
        workload_config.load_config(invalid_utf8)
