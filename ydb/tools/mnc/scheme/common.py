def optional(tp):
    if isinstance(tp, (type, tuple, list)):
        return {'__type__': tp, '__optional__': True}
    elif isinstance(tp, dict):
        tp['__optional__'] = True
        return tp
    assert False, f'unexpected tp: {type(tp)}'


def list_with(tp):
    return {'__type__': list, '__inner__': tp}


def object_with(**fields):
    return {
        '__type__': dict,
        **fields,
    }


def one_of(**fields):
    return {
        '__type__': dict,
        '__one_of__': [list(fields)],
        **fields
    }


def object_with_additional_fields(**fields):
    return {
        '__type__': dict,
        '__additional_fields__': True,
        **fields
    }


def with_default(tp, default):
    if isinstance(tp, (type, tuple, list)):
        return {'__type__': tp, '__default__': default}
    elif isinstance(tp, dict):
        tp['__default__'] = default
        return tp
    assert False, f'unexpected tp: {type(tp)}'


erasure_type = ('none', 'block-4-2', 'mirror-3of4', 'mirror-3-dc')
device_type = ('ROT', 'SSD', 'NVME')

sector_map_profile = ('NONE', 'HDD', 'SSD', 'NVME')

logging_levels = ('trace', 'debug', 'info', 'notice', 'warn', 'error', 'crit', 'alert', 'emerg')

deploy_flags = (
    'do_strip',
    'do_not_strip',
    'do_rebuild',
    'do_not_rebuild',
    'do_redeploy_bin',
    'do_not_redeploy_bin',
    'transit_bin_through_first_node',
    'secure',
)

range_list = list_with(str)

opposite_deploy_flags = {
    'do_strip': 'do_not_strip',
    'do_not_strip': 'do_strip',
    'do_rebuild': 'do_not_rebuild',
    'do_not_rebuild': 'do_rebuild',
    'do_redeploy_bin': 'do_not_redeploy_bin',
    'do_not_redeploy_bin': 'do_redeploy_bin',
}


def merge_deploy_flags(config_flags, cli_flags):
    config_flags = set(config_flags) if config_flags else set()
    if cli_flags:
        for flag in cli_flags:
            opposite_flag = opposite_deploy_flags.get(flag)
            if opposite_flag is not None and opposite_flag in config_flags:
                config_flags.remove(opposite_flag)
            if flag not in config_flags:
                config_flags.add(flag)
    return list(config_flags)
