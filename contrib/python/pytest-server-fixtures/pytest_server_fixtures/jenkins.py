'''
Created on 25 Apr 2012

@author: eeaston
'''
from __future__ import absolute_import

import os.path
import shutil

import pytest
import six

from pytest_server_fixtures import CONFIG
from pytest_fixture_config import yield_requires_config

from .http import HTTPTestServer


@pytest.yield_fixture(scope='session')
@yield_requires_config(CONFIG, ['jenkins_war', 'java_executable'])
def jenkins_server():
    """ Session-scoped Jenkins server instance

        Attributes
        ----------
        api (`jenkins.Jenkins`)  : python-jenkins client API connected to this server
        .. also inherits all attributes from the `workspace` fixture
    """
    with JenkinsTestServer() as p:
        p.start()
        yield p


@pytest.yield_fixture(scope='module')
@yield_requires_config(CONFIG, ['jenkins_war', 'java_executable'])
def jenkins_server_module():
    """ Module-scoped Jenkins server instance

        Attributes
        ----------
        api (`jenkins.Jenkins`)  : python-jenkins client API connected to this server
        .. also inherits all attributes from the `workspace` fixture
    """
    with JenkinsTestServer() as p:
        p.start()
        yield p


class JenkinsTestServer(HTTPTestServer):
    port_seed = 65533
    kill_retry_delay = 2

    def __init__(self, **kwargs):
        global jenkins
        try:
            import jenkins
        except ImportError:
            pytest.skip('python-jenkins not installed, skipping test')
        super(JenkinsTestServer, self).__init__(**kwargs)
        self.env = dict(JENKINS_HOME=self.workspace,
                        JENKINS_RUN=self.workspace / 'run',
                        # Use at most 1GB of RAM for the server
                        JAVA_ARGS='-Xms1G -Xmx1G',
                        RUN_STANDALONE='true',
                        JENKINS_LOG=self.workspace / 'jenkins.log',
                        )
        self.api = jenkins.Jenkins(self.uri)

    @property
    def run_cmd(self):
        if not CONFIG.jenkins_war:
            raise ValueError("jenkins_war missing from org config")

        return [CONFIG.java_executable,
                '-jar', CONFIG.jenkins_war,
                '--httpPort=%s' % self.port,
                '--httpListenAddress=%s' % self.hostname,
                '--ajp13Port=-1',
                '--webroot={0}'.format(self.workspace / 'run' / 'war'),
                ]

    def load_plugins(self, plugins_repo, plugins=None):
        """plugins_repo is the place from which the plugins can be copied to this jenskins instance
           is plugins is None, all plugins will be copied, else is should be a list of the plugin names
        """

        if not os.path.isdir(plugins_repo):
            raise ValueError('Plugin repository "%s" does not exist' % plugins_repo)

        # copy the plugins to the jenkins plugin directory
        available_plugins = dict(((os.path.splitext(os.path.basename(x))[0], os.path.join(plugins_repo, x))
                                  for x in os.listdir(plugins_repo) if x.endswith('.hpi')))

        if plugins is None:
            plugins = available_plugins.keys()
        else:
            if isinstance(plugins, six.string_types):
                plugins = [plugins]

            errors = []
            for p in plugins:
                if p not in available_plugins:
                    if p not in errors:
                        errors.append(p)
            if errors:
                if len(errors) == 1:
                    e = 'Plugin "%s" is not present in the repository' % errors[0]
                else:
                    e = 'Plugins %s are not present in the repository' % sorted(errors)
                raise ValueError(e)

        for p in plugins:
            tgt = os.path.join(self.plugins_dir, '%s.hpi' % p)
            shutil.copy(available_plugins[p], tgt)

    @property
    def plugins_dir(self):
        return os.path.normpath(os.path.join(self.workspace, 'plugins'))
