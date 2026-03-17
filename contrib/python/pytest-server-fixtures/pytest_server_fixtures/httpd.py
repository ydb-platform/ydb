import os
import platform
import string
import logging

import pytest
from pathlib import Path

from pytest_fixture_config import yield_requires_config
from pytest_server_fixtures import CONFIG

from .http import HTTPTestServer

log = logging.getLogger(__name__)


def is_rhel():
    """"Check if OS is RHEL/Centos"""
    return 'el' in platform.uname()[2]

@pytest.yield_fixture(scope='function')
@yield_requires_config(CONFIG, ['httpd_executable', 'httpd_modules'])
def httpd_server():
    """ Function-scoped httpd server in a local thread.

        Methods
        -------
        get()   : Query url relative to the server root.
        ..        Parse as json and retry failures by default.
        post()  : Post payload to url relative to the server root.
        ..        Parse as json and retry failures by default.
    """
    test_server = HTTPDServer()
    test_server.start()
    yield test_server
    test_server.teardown()


class HTTPDServer(HTTPTestServer):
    port_seed = 65531

    cfg_modules_template = """
      LoadModule headers_module $modules/mod_headers.so
      LoadModule proxy_module $modules/mod_proxy.so
      LoadModule proxy_http_module $modules/mod_proxy_http.so
      LoadModule proxy_connect_module $modules/mod_proxy_connect.so
      LoadModule alias_module $modules/mod_alias.so
      LoadModule dir_module $modules/mod_dir.so
      LoadModule autoindex_module $modules/mod_autoindex.so
      <IfModule !mod_log_config.c>
          LoadModule log_config_module $modules/mod_log_config.so
      </IfModule>
      LoadModule mime_module $modules/mod_mime.so
      LoadModule authz_core_module $modules/mod_authz_core.so
    """

    cfg_rhel_template = """
      LoadModule unixd_module modules/mod_unixd.so
    """

    cfg_mpm_template = """
      LoadModule mpm_prefork_module $modules/mod_mpm_prefork.so
      StartServers       1
      MinSpareServers    1
      MaxSpareServers   4
      ServerLimit      4
      MaxClients       4
      MaxRequestsPerChild  10000
    """

    cfg_template = """
      TypesConfig /etc/mime.types

      ServerRoot $server_root
      Listen $listen_addr
      PidFile $server_root/run/httpd.pid

      ErrorLog $log_dir/error.log
      LogFormat "%h %l %u %t \\"%r\\" %>s %b" common
      CustomLog $log_dir/access.log common
      LogLevel info

      $proxy_rules

      Alias / $document_root/

      <Directory $server_root>
          Options +Indexes
      </Directory>
    """

    def __init__(self, proxy_rules=None, extra_cfg='', document_root=None, log_dir=None, **kwargs):
        """ httpd Proxy Server

        Parameters
        ----------
        proxy_rules: `dict`
            { proxy_src: proxy_dest }. Eg   {'/downstream_url/' : server.uri}
        extra_cfg: `str`
            Any extra Apache config
        document_root : `str`
            Server document root, defaults to temporary workspace
        log_dir : `str`
            Server log directory, defaults to $(workspace)/logs
        """
        self.proxy_rules = proxy_rules if proxy_rules is not None else {}

        if not is_rhel():
            self.cfg_template = string.Template(self.cfg_modules_template +
                                                self.cfg_mpm_template +
                                                self.cfg_template +
                                                extra_cfg)
        else:
            self.cfg_template = string.Template(self.cfg_modules_template +
                                                self.cfg_rhel_template +
                                                self.cfg_mpm_template +
                                                self.cfg_template +
                                                extra_cfg)

        # Always print debug output for this process
        os.environ['DEBUG'] = '1'

        kwargs['hostname'] = kwargs.get('hostname', CONFIG.fixture_hostname)

        super(HTTPDServer, self).__init__(**kwargs)

        self.document_root = document_root or self.workspace
        self.document_root = Path(self.document_root)
        self.log_dir = log_dir or self.workspace / 'logs'
        self.log_dir = Path(self.log_dir)

    def pre_setup(self):
        """ Write out the config file
        """
        self.config = self.workspace / 'httpd.conf'
        rules = []
        for source in self.proxy_rules:
            rules.append("ProxyPass {0} {1}".format(source, self.proxy_rules[source]))
            rules.append("ProxyPassReverse {0} {1} \n".format(source, self.proxy_rules[source]))
        cfg = self.cfg_template.substitute(
            server_root=self.workspace,
            document_root=self.document_root,
            log_dir=self.log_dir,
            listen_addr="{host}:{port}".format(host=self.hostname, port=self.port),
            proxy_rules='\n'.join(rules),
            modules=CONFIG.httpd_modules,
        )
        self.config.write_text(cfg)
        log.debug("=========== HTTPD Server Config =============\n{}".format(cfg))

        # This is where it stores PID files
        (self.workspace / 'run').mkdir()
        if not os.path.exists(self.log_dir):
            self.log_dir.mkdir()

    @property
    def pid(self):
        try:
            return int((self.workspace / 'run' / 'httpd.pid').read_text())
        except FileNotFoundError:
            return None

    @property
    def run_cmd(self):
        return [CONFIG.httpd_executable, '-f', str(self.config)]

    def kill(self, retries=5):
        pid = self.pid
        if pid is not None:
            self.kill_by_pid(self.pid, retries)
