"""
Docker server class implementation.
"""
from __future__ import absolute_import

import logging
import docker

from retry import retry
from pytest_server_fixtures import CONFIG
from .common import (ServerClass,
                     merge_dicts,
                     ServerFixtureNotRunningException,
                     ServerFixtureNotTerminatedException)

log = logging.getLogger(__name__)


class DockerServer(ServerClass):
    """Docker server class."""

    def __init__(self,
                 server_type,
                 cmd,
                 get_args,
                 env,
                 image,
                 labels={}):
        super(DockerServer, self).__init__(cmd, get_args, env)

        self._image = image
        self._labels = merge_dicts(labels, {
            'server-fixtures': 'docker-server-fixtures',
            'server-fixtures/server-type': server_type,
            'server-fixtures/session-id': CONFIG.session_id,
        })

        self._client = docker.from_env()
        self._container = None

    def launch(self):
        try:
            log.debug('Launching container')
            self._container = self._client.containers.run(
                image=self._image,
                name=self.name,
                command=[self._cmd] + self._get_args(),
                environment=self._env,
                labels=self._labels,
                detach=True,
                auto_remove=True,
            )
            self._wait_until_running()
            log.debug('Container is running at %s', self.hostname)
        except docker.errors.ImageNotFound as err:
            log.warning("Failed to start container, image %s not found", self._image)
            log.debug(err)
            raise
        except docker.errors.APIError as e:
            log.warning("Failed to start container: %s", e)
            raise

        self.start()

    def run(self):
        try:
            self._container.wait()
        except docker.errors.APIError as e:
            log.warning("Error while waiting for container: %s", e)
            log.debug(self._container.logs())

    def teardown(self):
        if not self._container:
            return

        try:
            # stopping container will also remove it as 'auto_remove' is set
            self._container.stop()
            self._wait_until_terminated()
        except docker.errors.APIError as e:
            log.warning("Error when stopping the container: %s", e)

    @property
    def is_running(self):
        if not self._container:
            return False

        return self._get_status() == 'running'

    @property
    def hostname(self):
        if not self.is_running:
            raise ServerFixtureNotRunningException()
        return self._container.attrs['NetworkSettings']['IPAddress']

    def _get_status(self):
        try:
            self._container.reload()
            return self._container.status
        except docker.errors.APIError as e:
            log.warning("Failed to get container status: %s", e)
            raise

    @retry(ServerFixtureNotRunningException,
           tries=28,
           delay=1,
           backoff=2,
           max_delay=10)
    def _wait_until_running(self):
        if not self.is_running:
            raise ServerFixtureNotRunningException()

    @retry(ServerFixtureNotTerminatedException,
           tries=28,
           delay=1,
           backoff=2,
           max_delay=10)
    def _wait_until_terminated(self):
        try:
            self._get_status()
        except docker.errors.APIError as e:
            if e.response.status_code == 404:
                return
            raise
