import re
import os
import sys
import logging

from collections import defaultdict
from kubernetes.client import Configuration

from ydb.tools.ydbd_slice import nodes
from ydb.tools.ydbd_slice.kube import api, kubectl, yaml, generate, cms, dynconfig


logger = logging.getLogger(__name__)


VALID_NAME_PATTERN = '[a-z0-9]([-a-z0-9]*[a-z0-9])?'


CLUSTER_RESOURCES = [
    'namespace'
]


def check_cluster_requires_nodeclaim():
    config = Configuration.get_default_copy()
    if '2a0d:d6c0' in config.host:
        return True
    if 'cloud.yandex.net' in config.host:
        return True
    return False


def get_all_manifests(directory):
    result = []
    objects = defaultdict(set)
    for file in os.listdir(directory):
        path = os.path.abspath(os.path.join(directory, file))
        if not (file.endswith('.yaml') or file.endswith('.yml')):
            logger.info('skipping file: %s, not yaml file extension', path)
            continue
        try:
            with open(path) as file:
                data = yaml.load(file)
        except Exception as e:
            logger.error('failed to open and parse file: %s, error: %s', path, str(e))
            continue

        # check basic fields
        if not ('apiVersion' in data and 'kind' in data):
            logger.info('skipping file: %s, not kubernetes manifest', path)
            continue
        api_version = data['apiVersion']
        kind = data['kind'].lower()

        # check for explicit namespace
        if kind not in CLUSTER_RESOURCES and 'namespace' not in data['metadata']:
            logger.error(f'manifest {path} does not have metadata.namespace specified')
            sys.exit(2)

        namespace = data['metadata'].get('namespace')
        name = data['metadata']['name']

        # check for duplicate names
        type_key = (api_version, kind)
        obj_key = (namespace, name)
        if obj_key in objects[type_key]:
            logger.error(
                f'manifest for {api_version} {kind} with duplicated namespace and name {namespace}/{name} '
                f'found in {path}'
            )
            sys.exit(2)
        objects[type_key].add(obj_key)

        result.append((path, api_version, kind, namespace, name, data))

    # check if nodeclaims required
    cluster_requires_node_claim = check_cluster_requires_nodeclaim()
    if cluster_requires_node_claim and len(objects[('ydb.tech/v1alpha1', 'nodeclaim')]) == 0:
        logger.error('Cluster from kubeconfig requires NodeClaim object to be created. '
                     'Please create NodeClaim mainfest')
        sys.exit(2)

    return result


def validate_components_selector(value):
    if not re.match(r'^[a-zA-Z][a-zA-Z0-9\-]*$', value):
        raise ValueError('invalid value: %s' % value)


def parse_components_selector(value):
    result = {}
    items = value.strip().split(';')
    for item in items:
        if ':' in item:
            kind, names = item.split(':')
            validate_components_selector(kind)
            names = names.split(',')
            for name in names:
                validate_components_selector(name)
            result[kind] = names
        else:
            kind = item
            validate_components_selector(kind)
            result[kind] = []
    return result


def update_image(data, image):
    if 'version' in data['spec']:
        data['spec'].pop('version')
    image_data = data['spec'].setdefault('image', {})
    image_data['name'] = image
    image_data['pullPolicy'] = 'Always'


def update_manifest(path, data):
    if os.path.exists(path):
        with open(path) as file:
            old_data = yaml.load(file.read())
        if old_data == data:
            return

    logger.debug(f'updating manifest {path}')
    tmp_path = "%s.tmp" % path
    with open(tmp_path, 'w') as file:
        yaml.dump(data, file)
    os.rename(tmp_path, path)


def get_nodes(api_client, project_path, manifests):
    node_list = []
    for (path, api_version, kind, namespace, name, data) in manifests:
        if not (api_version in ['ydb.tech/v1alpha1'] and kind in ['nodeclaim']):
            continue
        namespace = data['metadata']['namespace']
        name = data['metadata']['name']
        try:
            obj_nodes = api.get_nodeclaim_nodes(api_client, namespace, name)
            node_list.extend(obj_nodes)
        except api.ApiException as e:
            if e.status == 404:
                logger.warning(f'NodeClaim {namespace}/{name} not found in cluster, skipping')
            else:
                raise
    node_list.sort()
    return node_list


def get_domain(api_client, project_path, manifests):
    for (_, _, kind, _, _, data) in manifests:
        if kind != 'storage':
            continue
        return data['spec']['domain']


def manifests_ydb_set_image(project_path, manifests, image):
    for (path, api_version, kind, namespace, name, data) in manifests:
        if not (kind in ['storage', 'database'] and api_version in ['ydb.tech/v1alpha1']):
            continue
        update_image(data, image)
        update_manifest(path, data)


def manifests_ydb_filter_components(project_path, manifests, update_components):
    result = []
    for (path, api_version, kind, namespace, name, data) in manifests:
        if api_version in ['ydb.tech/v1alpha1'] and kind in ['storage', 'database']:
            name = data['metadata']['name']
            if update_components is not None:
                if kind not in update_components:
                    logger.info(f'skipping manifest {path}, not specified in components')
                    continue
                names_to_update = update_components.get(kind, [])
                if len(names_to_update) > 0 and name not in names_to_update:
                    logger.info(f'skipping manifest {path}, not specified in names')
                    continue
            result.append((path, api_version, kind, namespace, name, data))
        else:
            result.append((path, api_version, kind, namespace, name, data))
    return result


#
# macro level nodeclaim functions
def slice_namespace_apply(api_client, project_path, manifests):
    for (path, _, kind, _, _, data) in manifests:
        if kind != 'namespace':
            continue
        api.apply_namespace(api_client, data, path)


def slice_namespace_delete(api_client, project_path, manifests):
    for (_, _, kind, _, name, _) in manifests:
        if kind != 'namespace':
            continue
        api.delete_namespace(api_client, name)


def slice_nodeclaim_apply(api_client, project_path, manifests):
    for (path, api_version, kind, _, _, data) in manifests:
        if not (kind in ['nodeclaim'] and api_version in ['ydb.tech/v1alpha1']):
            continue
        api.apply_nodeclaim(api_client, data, path)


def slice_nodeclaim_wait_ready(api_client, project_path, manifests):
    for (path, api_version, kind, namespace, name, data) in manifests:
        if not (kind in ['nodeclaim'] and api_version in ['ydb.tech/v1alpha1']):
            continue
        namespace = data['metadata']['namespace']
        name = data['metadata']['name']
        try:
            api.wait_nodeclaim_state_ok(api_client, namespace, name)
        except TimeoutError as e:
            sys.exit(e.args[0])


def slice_nodeclaim_nodes(api_client, project_path, manifests):
    node_list = get_nodes(api_client, project_path, manifests)
    if len(node_list) == 0:
        sys.exit('No nodes was claimed. Please check nodeclaim presense and status.')
    for node in node_list:
        print(node)


def slice_nodeclaim_format(api_client, project_path, manifests):
    node_list = get_nodes(api_client, project_path, manifests)
    if len(node_list) == 0:
        logger.info('no nodes found, nothing to format.')
        return
    node_list = nodes.Nodes(node_list)
    cmd = r"sudo find /dev/disk/ -path '*/by-partlabel/kikimr_*' " \
          r"-exec dd if=/dev/zero of={} bs=1M count=1 status=none \;"
    node_list.execute_async(cmd)


def slice_nodeclaim_delete(api_client, project_path, manifests):
    nodeclaims = []
    for (path, api_version, kind, namespace, name, data) in manifests:
        if not (kind in ['nodeclaim'] and api_version in ['ydb.tech/v1alpha1']):
            continue
        namespace = data['metadata']['namespace']
        name = data['metadata']['name']
        api.delete_nodeclaim(api_client, namespace, name)
        nodeclaims.append((namespace, name))
    if len(nodeclaims) == 0:
        logger.info(f'no nodeclaims found in {project_path}')
    for namespace, name in nodeclaims:
        try:
            api.wait_nodeclaim_deleted(api_client, namespace, name)
        except TimeoutError as e:
            sys.exit(e.args[0])


def wait_for_storage(api_client, project_path, manifests):
    for (path, api_version, kind, namespace, name, data) in manifests:
        if not (kind in ['storage'] and api_version in ['ydb.tech/v1alpha1']):
            continue
        namespace = data['metadata']['namespace']
        name = data['metadata']['name']
        try:
            api.wait_storage_state_ready(api_client, namespace, name)
        except TimeoutError as e:
            sys.exit(e.args[0])


# macro level ydb functions
def slice_ydb_apply(api_client, project_path, manifests, dynamic_config_type):
    # process storages first
    for (path, api_version, kind, namespace, name, data) in manifests:
        if not (kind in ['storage', 'database'] and api_version in ['ydb.tech/v1alpha1']):
            continue
        namespace = data['metadata']['namespace']
        name = data['metadata']['name']

        if kind == 'storage':
            api.apply_storage(api_client, data, path)
            new_data = api.get_storage(api_client, namespace, name)
            if 'status' in new_data:
                new_data.pop('status')
            api.drop_kubectl_last_applied_configuration(new_data)
            data['spec'] = new_data['spec']
            update_manifest(path, data)

    if dynamic_config_type in ['both', 'yaml']:
        local_config = dynconfig.get_local_config(project_path)
        if local_config is not None:
            wait_for_storage(api_client, project_path, manifests)

            node_list = get_nodes(api_client, project_path, manifests)
            domain = get_domain(api_client, project_path, manifests)

            with dynconfig.Client(node_list, domain) as dynconfig_client:
                remote_config = dynconfig.get_remote_config(dynconfig_client)

                if remote_config is None or remote_config != local_config:
                    new_config = dynconfig.apply_config(dynconfig_client, project_path)
                    dynconfig.write_local_config(project_path, new_config)

    if dynamic_config_type in ['both', 'proto']:
        config_items = cms.get_from_files(project_path)
        if config_items is not None:
            logger.debug(
                f'found {len(config_items)} legacy cms config items, '
                'need to wait for storage to become ready to apply configs'
            )
            # if configs present, then wait for storage
            wait_for_storage(api_client, project_path, manifests)

            # and apply configs
            node_list = get_nodes(api_client, project_path, manifests)
            if len(node_list) == 0:
                raise RuntimeError('no nodes found, cannot apply legacy cms config items.')
            cms.apply_legacy_cms_config_items(config_items, [f'grpc://{i}:2135' for i in node_list])

    # process databases later
    for (path, api_version, kind, namespace, name, data) in manifests:
        if not (kind in ['storage', 'database'] and api_version in ['ydb.tech/v1alpha1']):
            continue
        namespace = data['metadata']['namespace']
        name = data['metadata']['name']

        if kind == 'database':
            api.apply_database(api_client, data, path)
            new_data = api.get_database(api_client, namespace, name)
            if 'status' in new_data:
                new_data.pop('status')
            api.drop_kubectl_last_applied_configuration(new_data)
            data['spec'] = new_data['spec']
            update_manifest(path, data)


def slice_ydb_wait_ready(api_client, project_path, manifests, wait_ready):
    if not wait_ready:
        return
    for (path, api_version, kind, namespace, name, data) in manifests:
        if not (kind in ['storage', 'database'] and api_version in ['ydb.tech/v1alpha1']):
            continue
        namespace = data['metadata']['namespace']
        name = data['metadata']['name']
        if kind == 'storage':
            try:
                api.wait_storage_state_ready(api_client, namespace, name)
            except TimeoutError as e:
                sys.exit(e.args[0])
        if kind == 'database':
            try:
                api.wait_database_state_ready(api_client, namespace, name)
            except TimeoutError as e:
                sys.exit(e.args[0])


def slice_ydb_restart(api_client, project_path, manifests):
    pods_to_restart = set()
    for (path, api_version, kind, namespace, name, data) in manifests:
        if not (kind in ['storage', 'database'] and api_version in ['ydb.tech/v1alpha1']):
            continue
        namespace = data['metadata']['namespace']
        name = data['metadata']['name']
        if kind == 'storage':
            pods_to_restart.update(
                kubectl.get_pods_by_selector(
                    'app.kubernetes.io/component=storage-node,app.kubernetes.io/instance=%s' % name, namespace
                )
            )
        if kind == 'database':
            pods_to_restart.update(
                kubectl.get_pods_by_selector(
                    'app.kubernetes.io/component=dynamic-node,app.kubernetes.io/instance=%s' % name, namespace
                )
            )
    if len(pods_to_restart) > 0:
        kubectl.restart_pods_in_parallel(pods_to_restart)


def slice_ydb_delete(api_client, project_path, manifests):
    storages = []
    databases = []
    for (path, api_version, kind, namespace, name, data) in manifests:
        if not (kind in ['storage', 'database'] and api_version in ['ydb.tech/v1alpha1']):
            continue
        namespace = data['metadata']['namespace']
        name = data['metadata']['name']

        if kind == 'database':
            api.delete_database(api_client, namespace, name)
            databases.append((namespace, name))
        if kind == 'storage':
            api.delete_storage(api_client, namespace, name)
            storages.append((namespace, name))
    for namespace, name in databases:
        try:
            api.wait_database_deleted(api_client, namespace, name)
        except TimeoutError as e:
            sys.exit(e.args[0])
    for namespace, name in storages:
        try:
            api.wait_storage_deleted(api_client, namespace, name)
        except TimeoutError as e:
            sys.exit(e.args[0])


def slice_ydb_storage_wait_pods_deleted(api_client, project_path, manifests):
    pods_to_delete = set()
    for (path, api_version, kind, namespace, name, data) in manifests:
        if not (kind in ['storage', 'database'] and api_version in ['ydb.tech/v1alpha1']):
            continue
        namespace = data['metadata']['namespace']
        name = data['metadata']['name']
        if kind == 'storage':
            pods_to_delete.update(
                kubectl.get_pods_by_selector(
                    'app.kubernetes.io/component=storage-node,app.kubernetes.io/instance=%s' % name, namespace
                )
            )
    if len(pods_to_delete) > 0:
        api.wait_pods_deleted(api_client, pods_to_delete)


def validate_name(name, field):
    if not re.fullmatch(VALID_NAME_PATTERN, name):
        raise RuntimeError(
            f"Cannot use \"{name}\" as {field}. {field.capitalize()} must consist of lower case alphanumeric "
            "characters, must start and end with an alphanumeric character "
            f"(e.g. 'my-name',  or '123-abc', regex used for validation is \"{VALID_NAME_PATTERN}\")"
        )
    return name


#
# generate scenarios
def slice_generate_8_node_block_4_2(project_path, user, slice_name, node_flavor):
    slice_name = validate_name(slice_name, 'slice name')
    namespace_name = validate_name(f'dev-{user}-{slice_name}', 'namespace name')
    nodeclaim_name = slice_name
    storage_name = slice_name
    database_name = validate_name(f'{slice_name}-db1', 'database name')

    generate.generate_8_node_block_4_2(
        project_path=project_path,
        user=user,
        namespace_name=namespace_name,
        nodeclaim_name=nodeclaim_name,
        node_flavor=node_flavor,
        storage_name=storage_name,
        database_name=database_name,
    )


def slice_generate(project_path, user, slice_name, template, template_vars):
    if template == '8-node-block-4-2':
        if 'node_flavor' not in template_vars:
            sys.exit(f'Template {template} requires node_flavor to be specified. '
                     'Please use argument: -v node_flavor=<your_desired_node_flavor_here>')
        slice_generate_8_node_block_4_2(project_path, user, slice_name, node_flavor=template_vars['node_flavor'])

    else:
        sys.exit(f'Slice template {template} not implemented.')


def slice_install(project_path, manifests, wait_ready, dynamic_config_type):
    with api.ApiClient() as api_client:
        slice_namespace_apply(api_client, project_path, manifests)
        slice_nodeclaim_apply(api_client, project_path, manifests)
        slice_nodeclaim_wait_ready(api_client, project_path, manifests)
        slice_ydb_delete(api_client, project_path, manifests)
        slice_ydb_storage_wait_pods_deleted(api_client, project_path, manifests)
        slice_nodeclaim_format(api_client, project_path, manifests)
        slice_ydb_apply(api_client, project_path, manifests, dynamic_config_type)
        slice_ydb_wait_ready(api_client, project_path, manifests, wait_ready)


def slice_update(project_path, manifests, wait_ready, dynamic_config_type):
    with api.ApiClient() as api_client:
        slice_nodeclaim_apply(api_client, project_path, manifests)
        slice_nodeclaim_wait_ready(api_client, project_path, manifests)
        slice_ydb_apply(api_client, project_path, manifests, dynamic_config_type)
        slice_ydb_restart(api_client, project_path, manifests)
        slice_ydb_wait_ready(api_client, project_path, manifests, wait_ready)


def slice_stop(project_path, manifests):
    with api.ApiClient() as api_client:
        slice_ydb_delete(api_client, project_path, manifests)


def slice_start(project_path, manifests, wait_ready, dynamic_config_type):
    with api.ApiClient() as api_client:
        slice_ydb_apply(api_client, project_path, manifests, dynamic_config_type)
        slice_ydb_wait_ready(api_client, project_path, manifests, wait_ready)


def slice_restart(project_path, manifests):
    with api.ApiClient() as api_client:
        slice_ydb_restart(api_client, project_path, manifests)


def slice_nodes(project_path, manifests):
    with api.ApiClient() as api_client:
        slice_nodeclaim_nodes(api_client, project_path, manifests)


def slice_format(project_path, manifests, wait_ready, dynamic_config_type):
    with api.ApiClient() as api_client:
        slice_ydb_delete(api_client, project_path, manifests)
        slice_ydb_storage_wait_pods_deleted(api_client, project_path, manifests)
        slice_nodeclaim_format(api_client, project_path, manifests)
        slice_ydb_apply(api_client, project_path, manifests, dynamic_config_type)
        slice_ydb_wait_ready(api_client, project_path, manifests, wait_ready)


def slice_clear(project_path, manifests):
    with api.ApiClient() as api_client:
        slice_ydb_delete(api_client, project_path, manifests)
        slice_ydb_storage_wait_pods_deleted(api_client, project_path, manifests)
        slice_nodeclaim_format(api_client, project_path, manifests)


def slice_uninstall(project_path, manifests):
    with api.ApiClient() as api_client:
        slice_ydb_delete(api_client, project_path, manifests)
        slice_ydb_storage_wait_pods_deleted(api_client, project_path, manifests)
        slice_nodeclaim_format(api_client, project_path, manifests)
        slice_nodeclaim_delete(api_client, project_path, manifests)
        slice_namespace_delete(api_client, project_path, manifests)
