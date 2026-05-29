import os

from ydb.tools.mnc.scheme import mnc, multinode, agent


deploy_path = '/Berkanavt'
work_directory = './'
binary_project = 'ydb/apps/ydbd'
relative_binary_path = 'ydb/apps/ydbd/ydbd'
transit_bin_through_first_node = False
do_rebuild = True
do_strip = True
do_redeploy_bin = True
affinity = None

git_ydb_root = None

source_root = None
source_ya_path = None

bin_filename = 'ydbd'
path_to_bin = None
is_manual_path_to_bin = False
stripped_bin_suffix = '_stripped'

ports = dict()

secure = False

# Paths for TLS certs (local and remote)
certs_local_dir = None
ca_filename = 'ca.crt'
cert_filename = 'node.crt'
key_filename = 'node.key'
mon_cert_filename = 'web.pem'

ca_remote_path = '/opt/ydb/certs/ca.crt'
cert_remote_path = '/opt/ydb/certs/node.crt'
key_remote_path = '/opt/ydb/certs/node.key'
mon_cert_remote_path = '/opt/ydb/certs/web.pem'


def get_multinode_home_dir():
    return f"/home/{os.environ.get('USER', 'ydb')}/multinode_home"


def get_local_certs_dir():
    return certs_local_dir or os.path.join(work_directory, 'certs')


def get_local_cert_path(filename: str):
    return os.path.join(get_local_certs_dir(), filename)


def is_stripped_bin_path(bin_path: str):
    return bin_path.endswith(stripped_bin_suffix)


def get_stripped_bin_path(bin_path: str):
    if is_stripped_bin_path(bin_path):
        return bin_path
    return f'{bin_path}{stripped_bin_suffix}'


def apply_cfg_mnc(cfg: mnc.scheme):
    global git_ydb_root
    git_ydb_root = cfg['git_ydb_root']

    global source_root, source_ya_path
    source_root = git_ydb_root
    source_ya_path = os.path.join(source_root, 'ya')

    global path_to_bin
    path_to_bin = f'{source_root}/{relative_binary_path}'


def apply_cfg_multinode(cfg: multinode.scheme):
    global deploy_path, affinity
    if cfg.get('use_home_dir'):
        deploy_path = get_multinode_home_dir()
    if cfg.get('affinity'):
        affinity = cfg['affinity']

    global transit_bin_through_first_node, do_rebuild
    global do_strip, do_redeploy_bin, path_to_bin
    for flag in cfg.get('deploy_flags', []):
        if flag == 'do_rebuild':
            do_rebuild = True
        if flag == 'do_not_rebuild':
            do_rebuild = False
        if flag == 'do_strip':
            path_to_bin = f'{source_root}/{binary_project}/{bin_filename}'
            do_strip = True
        if flag == 'do_not_strip':
            path_to_bin = f'{source_root}/{binary_project}/{bin_filename}'
            do_strip = False
        if flag == 'do_redeploy_bin':
            do_redeploy_bin = True
        if flag == 'do_not_redeploy_bin':
            do_redeploy_bin = False
        if flag == 'transit_bin_through_first_node':
            transit_bin_through_first_node = True
        if flag == 'secure':
            set_secure(True)


def set_secure(value: bool):
    global secure
    secure = bool(value)


def apply_cfg_server(cfg):
    global deploy_path, do_strip
    deploy_path = get_multinode_home_dir()
    do_strip = False


def apply_cfg_agent(cfg: agent.scheme):
    global deploy_path, do_strip
    deploy_path = get_multinode_home_dir()
    do_strip = False


def apply_cfg(cfg, scheme):
    if '__name__' not in scheme or scheme['__name__'] == 'multinode':
        return apply_cfg_multinode(cfg)
    if scheme['__name__'] == 'server':
        return apply_cfg_server(cfg)
    if scheme['__name__'] == 'agent':
        return apply_cfg_agent(cfg)


def update_path_to_bin(bin_path):
    global path_to_bin, is_manual_path_to_bin
    path_to_bin = bin_path
    is_manual_path_to_bin = True
