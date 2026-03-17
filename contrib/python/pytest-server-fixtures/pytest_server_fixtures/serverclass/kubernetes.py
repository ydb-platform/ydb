"""
Kubernetes server class implementation.
"""
from __future__ import absolute_import

import os
import logging
import uuid

from kubernetes import config
from kubernetes import client as k8sclient
from kubernetes.client.rest import ApiException
from retry import retry
from pytest_server_fixtures import CONFIG
from .common import (ServerClass,
                     merge_dicts,
                     ServerFixtureNotRunningException,
                     ServerFixtureNotTerminatedException)

log = logging.getLogger(__name__)

IN_CLUSTER = os.path.exists('/var/run/secrets/kubernetes.io/namespace')
fixture_namespace = CONFIG.k8s_namespace

if IN_CLUSTER:
    config.load_incluster_config()
    if not fixture_namespace:
        with open('/var/run/secrets/kubernetes.io/namespace', 'r') as f:
            fixture_namespace = f.read().strp()
        log.info("SERVER_FIXTURES_K8S_NAMESPACE is not set, using current namespace '%s'", fixture_namespace)

if CONFIG.k8s_local_test:
    log.info("====== Running K8S Server Class in Test Mode =====")
    config.load_kube_config()
    fixture_namespace = 'default'


class NotRunningInKubernetesException(Exception):
    """Thrown when code is not running as a Pod inside a Kubernetes cluster."""
    pass


class KubernetesServer(ServerClass):
    """Kubernetes server class."""

    def __init__(self,
                server_type,
                cmd,
                get_args,
                env,
                image,
                labels={}):
        super(KubernetesServer, self).__init__(cmd, get_args, env)

        if not fixture_namespace:
            raise NotRunningInKubernetesException()

        self._image = image
        self._labels = merge_dicts(labels, {
            'server-fixtures': 'kubernetes-server-fixtures',
            'server-fixtures/server-type': server_type,
            'server-fixtures/session-id': CONFIG.session_id,
        })

        self._v1api = k8sclient.CoreV1Api()

    def launch(self):
        try:
            log.debug('%s Launching pod' % self._log_prefix)
            self._create_pod()
            self._wait_until_running()
            log.debug('%s Pod is running' % self._log_prefix)
        except ApiException as e:
            log.warning('%s Error while launching pod: %s', self._log_prefix, e)
            raise

    def run(self):
        pass

    def teardown(self):
        self._delete_pod()
        # TODO: provide an flag to skip the wait to speed up the tests?
        self._wait_until_teardown()

    @property
    def is_running(self):
        try:
            return self._get_pod_status().phase == 'Running'
        except ApiException as e:
            if e.status == 404:
                # return false if pod does not exists
                return False
            raise

    @property
    def hostname(self):
        if not self.is_running:
            raise ServerFixtureNotRunningException()
        return self._get_pod_status().pod_ip

    @property
    def namespace(self):
        return fixture_namespace

    @property
    def labels(self):
        return self._labels

    def _get_pod_spec(self):
        container = k8sclient.V1Container(
            name='fixture',
            image=self._image,
            command=self._get_cmd(),
            env=[k8sclient.V1EnvVar(name=k, value=v) for k, v in self._env.iteritems()],
        )

        return k8sclient.V1PodSpec(
            containers=[container]
        )

    def _create_pod(self):
        try:
            pod = k8sclient.V1Pod()
            pod.metadata = k8sclient.V1ObjectMeta(name=self.name, labels=self._labels)
            pod.spec = self._get_pod_spec()
            self._v1api.create_namespaced_pod(namespace=self.namespace, body=pod)
        except ApiException as e:
            log.error("%s Failed to create pod: %s", self._log_prefix, e.reason)
            raise

    def _delete_pod(self):
        try:
            body = k8sclient.V1DeleteOptions()
            # delete the pod without waiting
            body.grace_period_seconds = 1
            self._v1api.delete_namespaced_pod(namespace=self.namespace, name=self.name, body=body)
        except ApiException as e:
            log.error("%s Failed to delete pod: %s", self._log_prefix, e.reason)

    def _get_pod_status(self):
        try:
            resp = self._v1api.read_namespaced_pod_status(namespace=self.namespace, name=self.name)
            return resp.status
        except ApiException as e:
            log.error("%s Failed to read pod status: %s", self._log_prefix, e.reason)
            raise

    @retry(ServerFixtureNotRunningException, tries=28, delay=1, backoff=2, max_delay=10)
    def _wait_until_running(self):
        log.debug("%s Waiting for pod status to become running", self._log_prefix)
        if not self.is_running:
            raise ServerFixtureNotRunningException()

    @retry(ServerFixtureNotTerminatedException, tries=28, delay=1, backoff=2, max_delay=10)
    def _wait_until_teardown(self):
        try:
            self._get_pod_status()
            # waiting for pod to be deleted (expect ApiException with status 404)
            raise ServerFixtureNotTerminatedException()
        except ApiException as e:
            if e.status == 404:
                return
            raise

    @property
    def _log_prefix(self):
        return "[K8S %s:%s]" % (self.namespace, self.name)
