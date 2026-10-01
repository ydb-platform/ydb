"""Per-process YAML overlays for ydbd_slice.

Named ``process_profiles`` are deep-merged onto the base node YAML and written
as ``config.p_<id>.yaml``. Storage processes point ``kikimr.cfg`` at the
matching file; dynnode slots get a copy as ``${tenant_main_dir}/config.yaml``.

CMS caveat: if Console YAML is active (``yaml_config_enabled`` and CMS returns
yaml), ``InitDynamicNode`` replaces local ``--yaml-config``. Per-slot files are
then lost. This matches today's ``--yaml-config`` slice when CMS yaml is empty
(``GetConfig`` fails and the local file is kept). v2 ``metadata`` /
``replace_config`` is unsupported.
"""
import copy
import os
import re

import yaml

from ydb.tools.cfg.utils import write_to_file

ROOT_SLICE_ONLY_KEYS = (
    'process_profiles',
)

HOST_SLICE_ONLY_KEYS = (
    'storage',
    'dynamic_slots',
    'storage_profile',
    'dynamic_profiles',
)

_SAFE_ID_RE = re.compile(r'[^A-Za-z0-9._-]+')

_COMPACT_EXECUTORS = (
    ('system', 'System', 'BASIC', {'threads': 9, 'spin_threshold': 1}),
    ('user', 'User', 'BASIC', {'threads': 16, 'spin_threshold': 1}),
    ('batch', 'Batch', 'BASIC', {'threads': 7, 'spin_threshold': 1}),
    ('io', 'IO', 'IO', {'threads': 1}),
    ('ic', 'IC', 'BASIC', {'threads': 3, 'spin_threshold': 10, 'time_per_mailbox_micro_secs': 100}),
)


def normalize_profile_id(profile_id):
    if profile_id is None:
        raise ValueError('process profile id is required')
    return str(profile_id)


def profile_yaml_filename(profile_id):
    safe = _SAFE_ID_RE.sub('_', normalize_profile_id(profile_id))
    if not safe or safe in ('.', '..'):
        raise ValueError('invalid process profile id: %r' % (profile_id,))
    return 'config.p_%s.yaml' % safe


def dump_yaml(data):
    return yaml.dump(data, sort_keys=False, default_flow_style=False, allow_unicode=True)


def deep_merge(base, overlay):
    """Deep-merge overlay onto base. Nested maps merge; lists and scalars replace."""
    if not isinstance(overlay, dict):
        return copy.deepcopy(overlay)
    if not isinstance(base, dict):
        return copy.deepcopy(overlay)
    result = copy.deepcopy(base)
    for key, value in overlay.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = deep_merge(result[key], value)
        else:
            result[key] = copy.deepcopy(value)
    return result


def compact_sys_to_actor_system_config(sys_overlay):
    """Translate compact cluster.yaml ``sys`` into native ``actor_system_config``."""
    sys_overlay = sys_overlay or {}
    executors_overlay = sys_overlay.get('executors') or {}
    executor_list = []
    for compact_name, yaml_name, executor_type, defaults in _COMPACT_EXECUTORS:
        item = dict(defaults)
        item['name'] = yaml_name
        item['type'] = executor_type
        extra = executors_overlay.get(compact_name) or {}
        for key, value in extra.items():
            if key == 'TimePerMailboxMicroSecs':
                item['time_per_mailbox_micro_secs'] = value
            else:
                item[key] = value
        executor_list.append(item)

    scheduler = {
        'resolution': 64,
        'spin_threshold': 0,
        'progress_threshold': 10000,
    }
    scheduler_overlay = sys_overlay.get('scheduler') or {}
    scheduler.update(scheduler_overlay)

    config = {
        'executor': executor_list,
        'sys_executor': 0,
        'user_executor': 1,
        'batch_executor': 2,
        'io_executor': 3,
        'service_executor': [
            {'service_name': 'Interconnect', 'executor_id': 4},
        ],
        'scheduler': scheduler,
    }
    if sys_overlay.get('use_auto_config'):
        config['use_auto_config'] = True
        if 'node_type' in sys_overlay:
            config['node_type'] = sys_overlay['node_type']
        if 'cpu_count' in sys_overlay:
            config['cpu_count'] = sys_overlay['cpu_count']
    return config


def prepare_overlay(overlay, convert_sys=True):
    overlay = copy.deepcopy(overlay) if overlay else {}
    overlay.pop('id', None)
    if convert_sys and 'sys' in overlay:
        actor = compact_sys_to_actor_system_config(overlay.pop('sys'))
        if 'actor_system_config' in overlay:
            overlay['actor_system_config'] = deep_merge(actor, overlay['actor_system_config'])
        else:
            overlay['actor_system_config'] = actor
    return overlay


def strip_slice_only_fields(cfg):
    cfg = copy.deepcopy(cfg)
    for key in ROOT_SLICE_ONLY_KEYS:
        cfg.pop(key, None)
    for host in cfg.get('hosts') or []:
        if isinstance(host, dict):
            for key in HOST_SLICE_ONLY_KEYS:
                host.pop(key, None)
    return cfg


def needs_strip(cfg):
    if any(key in cfg for key in ROOT_SLICE_ONLY_KEYS):
        return True
    for host in cfg.get('hosts') or []:
        if isinstance(host, dict) and any(key in host for key in HOST_SLICE_ONLY_KEYS):
            return True
    return False


def parse_process_profiles(template):
    raw = template.get('process_profiles') or []
    catalog = {}
    for item in raw:
        if not isinstance(item, dict) or 'id' not in item:
            raise ValueError('each process_profiles entry must be a map with id')
        profile_id = normalize_profile_id(item['id'])
        if profile_id in catalog:
            raise ValueError('duplicate process profile id: %s' % profile_id)
        catalog[profile_id] = item
    return catalog


def yaml_uses_process_profiles(template):
    if template.get('process_profiles'):
        return True
    for host in template.get('hosts') or []:
        if not isinstance(host, dict):
            continue
        if 'storage_profile' in host or 'dynamic_profiles' in host:
            return True
    return False


def require_flag_if_used(template, enabled):
    if yaml_uses_process_profiles(template) and not enabled:
        raise ValueError(
            "cluster YAML uses process_profiles / storage_profile / dynamic_profiles; "
            "pass --process-profiles"
        )


def _host_profile_list(value):
    if value is None:
        return []
    if not isinstance(value, list):
        raise ValueError('dynamic_profiles must be a list of profile ids')
    return [normalize_profile_id(item) for item in value]


def validate_host_profiles(template, catalog, domain_slot_count):
    known = set(catalog)
    for host in template.get('hosts') or []:
        if not isinstance(host, dict):
            continue
        hostname = host.get('name') or host.get('host') or '<unknown>'
        storage_enabled = bool(host.get('storage', True))
        if 'storage_profile' in host:
            if not storage_enabled:
                raise ValueError('storage_profile is forbidden when storage: false (%s)' % hostname)
            profile_id = normalize_profile_id(host['storage_profile'])
            if profile_id not in known:
                raise ValueError('unknown storage_profile %s on host %s' % (profile_id, hostname))
        if 'dynamic_profiles' in host:
            profiles = _host_profile_list(host['dynamic_profiles'])
            if 'dynamic_slots' in host:
                expected = int(host['dynamic_slots'])
            else:
                expected = domain_slot_count
            if len(profiles) != expected:
                raise ValueError(
                    'dynamic_profiles length %d does not match slot count %d on host %s'
                    % (len(profiles), expected, hostname)
                )
            for profile_id in profiles:
                if profile_id not in known:
                    raise ValueError('unknown dynamic profile %s on host %s' % (profile_id, hostname))


def host_storage_profiles(template):
    result = {}
    for host in template.get('hosts') or []:
        if not isinstance(host, dict) or 'storage_profile' not in host:
            continue
        hostname = host.get('name') or host.get('host')
        if hostname is None:
            continue
        result[hostname] = normalize_profile_id(host['storage_profile'])
    return result


def host_dynamic_profiles(template):
    result = {}
    for host in template.get('hosts') or []:
        if not isinstance(host, dict) or 'dynamic_profiles' not in host:
            continue
        hostname = host.get('name') or host.get('host')
        if hostname is None:
            continue
        result[hostname] = _host_profile_list(host['dynamic_profiles'])
    return result


def merge_profile(base_cfg, overlay, convert_sys=True):
    prepared = prepare_overlay(overlay, convert_sys=convert_sys)
    return strip_slice_only_fields(deep_merge(base_cfg, prepared))


def emit_profile_yaml_files(config_yaml_path, template, convert_sys=True):
    """Rewrite stripped base config.yaml and write config.p_<id>.yaml next to it."""
    with open(config_yaml_path, 'r') as f:
        base_cfg = yaml.safe_load(f) or {}

    stripped_base = strip_slice_only_fields(base_cfg)
    write_to_file(config_yaml_path, dump_yaml(stripped_base))

    catalog = parse_process_profiles(template)
    cfg_dir = os.path.dirname(config_yaml_path)
    written = {}
    for profile_id, overlay in catalog.items():
        merged = merge_profile(stripped_base, overlay, convert_sys=convert_sys)
        filename = profile_yaml_filename(profile_id)
        path = os.path.join(cfg_dir, filename)
        write_to_file(path, dump_yaml(merged))
        written[profile_id] = path
    return written
