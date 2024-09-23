import copy
import json
import time
import logging
from kubernetes.client import ApiClient as KubeApiClient, CustomObjectsApi, ApiException, CoreV1Api
from kubernetes.config import load_kube_config
from ydb.tools.ydbd_slice.kube import kubectl


logger = logging.getLogger(__name__)


KUBECTL_ANNOTATION = 'kubectl.kubernetes.io/last-applied-configuration'


def add_kubectl_last_applied_configuration(body):
    body = copy.deepcopy(body)
    if 'status' in body:
        body.pop('status')
    annotations = body.get('metadata', {}).get('annotations', {})
    if KUBECTL_ANNOTATION in annotations:
        annotations.pop(KUBECTL_ANNOTATION)
    body_str = json.dumps(body)
    annotations[KUBECTL_ANNOTATION] = body_str
    return body


def drop_kubectl_last_applied_configuration(body):
    body = copy.deepcopy(body)
    annotations = body.get('metadata', {}).get('annotations', {})
    if KUBECTL_ANNOTATION in annotations:
        annotations.pop(KUBECTL_ANNOTATION)
    return body


class ApiClient(KubeApiClient):
    def __init__(self, *args, **kwargs):
        load_kube_config()
        super().__init__(*args, **kwargs)

    def close(self):
        self.rest_client.pool_manager.clear()
        super().close()


def get_namespace(api_client, name):
    logger.debug(f'getting namespace: {name}')
    api = CoreV1Api(api_client)
    return api.read_namespace(name=name)


def create_namespace(api_client, body):
    name = body['metadata']['name']
    logger.debug(f'creating namespace: {name}')
    api = CoreV1Api(api_client)
    body = add_kubectl_last_applied_configuration(body)
    return api.create_namespace(body=body)


def apply_namespace(api_client, body, manifest_path):
    changed = True
    try:
        create_namespace(api_client, body)
        return changed
    except ApiException as e:
        if e.status != 409:
            raise
        logger.debug('namespace already created, using kubectl apply')
        rc, stdout, _ = kubectl.apply(manifest_path)
        if rc != 0:
            raise RuntimeError(f'failed to apply manifest {manifest_path}')
        if 'unchanged' in stdout:
            changed = False
        else:
            changed = True
        return changed


def delete_namespace(api_client, name):
    logger.debug(f'deleting namespace: {name}')
    api = CoreV1Api(api_client)
    try:
        return api.delete_namespace(name=name)
    except ApiException as e:
        if e.status == 404:
            return
        raise


def wait_namespace_deleted(api_client, name, timeout=300):
    logger.debug(f'waiting for namespace to delete: {name}')
    end_ts = time.time() + timeout
    while time.time() < end_ts:
        try:
            get_namespace(api_client, name)
        except ApiException as e:
            if e.status == 404:
                return
        time.sleep(5)
    raise TimeoutError(f'waiting for namespace {name} to delete timed out')


def get_nodeclaim(api_client, namespace, name):
    logger.debug(f'getting nodeclaim: {namespace}/{name}')
    api = CustomObjectsApi(api_client)
    return api.get_namespaced_custom_object(
        group='ydb.tech',
        version='v1alpha1',
        namespace=namespace,
        plural='nodeclaims',
        name=name,
    )


def get_nodeclaim_nodes(api_client, namespace, name):
    obj = get_nodeclaim(api_client, namespace, name)
    namespace = obj['metadata']['namespace']
    name = obj['metadata']['name']
    node_list = []
    status = obj.get('status', {})
    if status.get('state') != 'Ok':
        logger.warning(f'NodeClaim {namespace}/{name} not in Ok state. '
                       'Not all requested nodes was claimed for your slice.')
    for item in status.get('nodes', []):
        if 'name' in item:
            node_list.append(item['name'])
        elif 'flavor' in item:
            for name in item['flavor']['nodes']:
                node_list.append(name)
    return node_list


def create_nodeclaim(api_client, body):
    namespace = body['metadata']['namespace']
    name = body['metadata']['name']
    logger.debug(f'creating nodeclaim: {namespace}/{name}')
    api = CustomObjectsApi(api_client)
    body = add_kubectl_last_applied_configuration(body)
    return api.create_namespaced_custom_object(
        group='ydb.tech',
        version='v1alpha1',
        namespace=namespace,
        plural='nodeclaims',
        body=body,
    )


def apply_nodeclaim(api_client, body, manifest_path):
    changed = True
    try:
        create_nodeclaim(api_client, body)
        return changed
    except ApiException as e:
        if e.status != 409:
            raise
        logger.debug('nodeclaim already created, using kubectl apply')
        rc, stdout, _ = kubectl.apply(manifest_path)
        if rc != 0:
            raise RuntimeError(f'failed to apply manifest {manifest_path}')
        if 'unchanged' in stdout:
            changed = False
        else:
            changed = True
        return changed


def wait_nodeclaim_state_ok(api_client, namespace, name, timeout=300):
    logger.debug(f'waiting for nodeclaim to transfer to Ok state: {namespace}/{name}')
    end_ts = time.time() + timeout
    while time.time() < end_ts:
        obj = get_nodeclaim(api_client, namespace, name)
        state = obj.get('status', {}).get('state')
        logger.debug(f'nodeclaim {namespace}/{name} state {state}')
        if state == 'Ok':
            return
        if state == 'Failed':
            raise RuntimeError(f'nodeclaim {namespace}/{name} cannot acquire requested nodes, please change nodeclaim')
        time.sleep(5)
    raise TimeoutError(f'waiting for nodeclaim {namespace}/{name} timed out')


def delete_nodeclaim(api_client, namespace, name):
    logger.debug(f'deleting nodeclaim: {namespace}/{name}')
    api = CustomObjectsApi(api_client)
    try:
        return api.delete_namespaced_custom_object(
            group='ydb.tech',
            version='v1alpha1',
            namespace=namespace,
            plural='nodeclaims',
            name=name,
        )
    except ApiException as e:
        if e.status == 404:
            return
        raise


def wait_nodeclaim_deleted(api_client, namespace, name, timeout=300):
    logger.debug(f'waiting for nodeclaim to delete: {namespace}/{name}')
    end_ts = time.time() + timeout
    while time.time() < end_ts:
        try:
            get_nodeclaim(api_client, namespace, name)
        except ApiException as e:
            if e.status == 404:
                return
        time.sleep(5)
    raise TimeoutError(f'waiting for nodeclaim {namespace}/{name} to delete timed out')


def get_storage(api_client, namespace, name):
    logger.debug(f'getting storage: {namespace}/{name}')
    api = CustomObjectsApi(api_client)
    return api.get_namespaced_custom_object(
        group='ydb.tech',
        version='v1alpha1',
        namespace=namespace,
        plural='storages',
        name=name,
    )


def create_storage(api_client, body):
    namespace = body['metadata']['namespace']
    name = body['metadata']['name']
    logger.debug(f'creating storage: {namespace}/{name}')
    api = CustomObjectsApi(api_client)
    body = add_kubectl_last_applied_configuration(body)
    return api.create_namespaced_custom_object(
        group='ydb.tech',
        version='v1alpha1',
        namespace=namespace,
        plural='storages',
        body=body,
    )


def apply_storage(api_client, body, manifest_path):
    changed = True
    try:
        create_storage(api_client, body)
        return changed
    except ApiException as e:
        if e.status != 409:
            raise
        logger.debug('storage already created, using kubectl apply')
        rc, stdout, _ = kubectl.apply(manifest_path)
        if rc != 0:
            raise RuntimeError(f'failed to apply manifest {manifest_path}')
        if 'unchanged' in stdout:
            changed = False
        else:
            changed = True
        return changed


def wait_storage_state_ready(api_client, namespace, name, timeout=900):
    logger.debug(f'waiting for storage to transfer to Ready state: {namespace}/{name}')
    end_ts = time.time() + timeout
    while time.time() < end_ts:
        obj = get_storage(api_client, namespace, name)
        state = obj.get('status', {}).get('state')
        logger.debug(f'storage {namespace}/{name} state {state}')
        if state == 'Ready':
            return
        time.sleep(5)
    raise TimeoutError(f'waiting for storage {namespace}/{name} timed out')


def delete_storage(api_client, namespace, name):
    logger.debug(f'deleting storage: {namespace}/{name}')
    api = CustomObjectsApi(api_client)
    try:
        return api.delete_namespaced_custom_object(
            group='ydb.tech',
            version='v1alpha1',
            namespace=namespace,
            plural='storages',
            name=name,
        )
    except ApiException as e:
        if e.status == 404:
            return
        raise


def wait_storage_deleted(api_client, namespace, name, timeout=300):
    logger.debug(f'waiting for storage to delete: {namespace}/{name}')
    end_ts = time.time() + timeout
    while time.time() < end_ts:
        try:
            get_storage(api_client, namespace, name)
        except ApiException as e:
            if e.status == 404:
                return
        time.sleep(5)
    raise TimeoutError(f'waiting for storage {namespace}/{name} to delete timed out')


def get_database(api_client, namespace, name):
    logger.debug(f'getting database: {namespace}/{name}')
    api = CustomObjectsApi(api_client)
    return api.get_namespaced_custom_object(
        group='ydb.tech',
        version='v1alpha1',
        namespace=namespace,
        plural='databases',
        name=name,
    )


def create_database(api_client, body):
    namespace = body['metadata']['namespace']
    name = body['metadata']['name']
    logger.debug(f'creating database: {namespace}/{name}')
    api = CustomObjectsApi(api_client)
    body = add_kubectl_last_applied_configuration(body)
    return api.create_namespaced_custom_object(
        group='ydb.tech',
        version='v1alpha1',
        namespace=namespace,
        plural='databases',
        body=body,
    )


def apply_database(api_client, body, manifest_path):
    changed = True
    try:
        create_database(api_client, body)
        return changed
    except ApiException as e:
        if e.status != 409:
            raise
        logger.debug('database already created, using kubectl apply')
        rc, stdout, _ = kubectl.apply(manifest_path)
        if rc != 0:
            raise RuntimeError(f'failed to apply manifest {manifest_path}')
        if 'unchanged' in stdout:
            changed = False
        else:
            changed = True
        return changed


def wait_database_state_ready(api_client, namespace, name, timeout=900):
    logger.debug(f'waiting for database to transfer to Ready state: {namespace}/{name}')
    end_ts = time.time() + timeout
    while time.time() < end_ts:
        obj = get_database(api_client, namespace, name)
        state = obj.get('status', {}).get('state')
        logger.debug(f'database {namespace}/{name} state {state}')
        if state == 'Ready':
            return
        time.sleep(5)
    raise TimeoutError(f'waiting for database {namespace}/{name} timed out')


def delete_database(api_client, namespace, name):
    logger.debug(f'deleting database: {namespace}/{name}')
    api = CustomObjectsApi(api_client)
    try:
        return api.delete_namespaced_custom_object(
            group='ydb.tech',
            version='v1alpha1',
            namespace=namespace,
            plural='databases',
            name=name,
        )
    except ApiException as e:
        if e.status == 404:
            return
        raise


def wait_database_deleted(api_client, namespace, name, timeout=1800):
    logger.debug(f'waiting for database to delete: {namespace}/{name}')
    end_ts = time.time() + timeout
    while time.time() < end_ts:
        try:
            get_storage(api_client, namespace, name)
        except ApiException as e:
            if e.status == 404:
                return
        time.sleep(5)
    raise TimeoutError(f'waiting for database {namespace}/{name} to delete timed out')


def get_pod(api_client, namespace, name):
    api = CoreV1Api(api_client)
    return api.read_namespaced_pod(name, namespace)


def wait_pods_deleted(api_client, pods, timeout=1800):
    logger.debug('waiting for pods to delete')
    pod_present = {i: True for i in pods}
    end_ts = time.time() + timeout
    while time.time() < end_ts:
        if not any(pod_present.values()):
            return
        logger.debug(f'pods left: {len([v for v in pod_present.values() if v])}/{len(pod_present)}')
        for key, present in pod_present.items():
            if not present:
                continue

            pod_namespace, pod_name, pod_uid = key
            try:
                pod = get_pod(api_client, pod_namespace, pod_name)
                if pod.metadata.uid != pod_uid:
                    pod_present[key] = False
            except ApiException as e:
                if e.status == 404:
                    pod_present[key] = False
                elif e.status >= 500:
                    pass
                else:
                    raise

            time.sleep(0.05)

        time.sleep(1)

    raise TimeoutError('waiting for pods to delete timed out')
