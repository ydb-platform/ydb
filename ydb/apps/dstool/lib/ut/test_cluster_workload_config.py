from types import SimpleNamespace

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
    assert tablet.only_tablet_types == frozenset([workload_config.TabletType.DATA_SHARD])
    assert config.actions.switch_pile[0].mode == workload_config.SwitchPileMode.HARD
    assert config.actions.disconnect_pile[0].pile == 3
    assert config.actions.disconnect_socket[0].symmetrical


@pytest.mark.parametrize(
    'yaml_text, error',
    [
        ('actions: {}', 'at least one action'),
        (
            'actions: {wipe_vdisk: [{weight: 0}]}',
            'finite positive',
        ),
        (
            'actions: {set_read_only: [{component: vdisk}]}',
            'read_only is required',
        ),
        (
            'actions: {disconnect_pile: [{}]}',
            'operation is required',
        ),
        (
            'sleep_between_rounds: 0s\nactions: {wipe_vdisk: [{}]}',
            'must be positive',
        ),
        (
            'unknown: true\nactions: {wipe_vdisk: [{}]}',
            'unknown field',
        ),
        (
            'actions: {wipe_vdisk: [{weight: 1, weight: 2}]}',
            'duplicate key',
        ),
        (
            'actions: {wipe_vdisk: {}, evict_vdisk: [{}]}',
            'expected a list',
        ),
        (
            '''
actions:
  restart_node:
    - filter:
        only_nodes: {ids: [1]}
        exclude_nodes: {ids: [2]}
''',
            'oneof',
        ),
    ],
)
def test_invalid_config(yaml_text, error):
    with pytest.raises(ValueError, match=error):
        workload_config.parse_config(yaml_text, 'workload.yaml')


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


def test_invalid_legacy_weight_is_validated():
    with pytest.raises(ValueError, match='finite positive'):
        workload_config.parse_workload_config(_legacy_args(weight_restarts=-1))


def test_node_filter_combines_id_type_and_tenant_filters():
    node_filter = workload_config.NodeFilter(
        only_node_ids=frozenset([2, 3]),
        only_types=frozenset([workload_config.NodeType.DYNAMIC]),
        only_tenants=frozenset(['/Root/database']),
    )

    assert node_filter.matches(2, workload_config.NodeType.DYNAMIC, ['/Root/database'])
    assert not node_filter.matches(1, workload_config.NodeType.STORAGE, ['/Root/database'])
    assert not node_filter.matches(3, workload_config.NodeType.DYNAMIC, ['/Root/other'])


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
    config_path.write_text('actions: {wipe_vdisk: [{}]}')

    config = workload_config.parse_workload_config(
        _legacy_args(
            config_file=str(config_path),
            disable_wipes=True,
            weight_restarts=-1,
        )
    )

    assert len(config.actions.wipe_vdisk) == 1
    assert not config.actions.restart_node
