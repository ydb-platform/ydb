#
#  Copyright (C) 2010-2011, 2011 Canonical Ltd. All Rights Reserved
#
#  This file was originally taken from txzookeeper and modified later.
#
#  Authors:
#   Kapil Thangavelu and the Kazoo team
#
#  txzookeeper is free software: you can redistribute it and/or modify
#  it under the terms of the GNU Lesser General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  txzookeeper is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU Lesser General Public License for more details.
#
#  You should have received a copy of the GNU Lesser General Public License
#  along with txzookeeper.  If not, see <http://www.gnu.org/licenses/>.


import code
from collections import namedtuple
from glob import glob
from itertools import chain
import logging
import os
import os.path
import pathlib
import shutil
import signal
import subprocess
import tempfile
import traceback

import OpenSSL
import jks


log = logging.getLogger(__name__)


def debug(sig, frame):
    """Interrupt running process, and provide a python prompt for
    interactive debugging."""
    d = {"_frame": frame}  # Allow access to frame object.
    d.update(frame.f_globals)  # Unless shadowed by global
    d.update(frame.f_locals)

    i = code.InteractiveConsole(d)
    message = "Signal recieved : entering python shell.\nTraceback:\n"
    message += "".join(traceback.format_stack(frame))
    i.interact(message)


def listen():
    if os.name != "nt":  # SIGUSR1 is not supported on Windows
        signal.signal(signal.SIGUSR1, debug)  # Register handler


listen()


def to_java_compatible_path(path):
    if os.name == "nt":
        path = path.replace("\\", "/")
    return path


ServerInfo = namedtuple(
    "ServerInfo",
    "server_id client_port secure_client_port "
    "election_port leader_port admin_port peer_type",
)


class ManagedZooKeeper(object):
    """Class to manage the running of a ZooKeeper instance for testing.

    Note: no attempt is made to probe the ZooKeeper instance is
    actually available, or that the selected port is free. In the
    future, we may want to do that, especially when run in a
    Hudson/Buildbot context, to ensure more test robustness."""

    def __init__(
        self,
        software_path,
        server_info,
        peers=(),
        classpath=None,
        configuration_entries=(),
        java_system_properties=(),
        jaas_config=None,
        ssl_configuration=None,
    ):
        """Define the ZooKeeper test instance.

        @param install_path: The path to the install for ZK
        @param port: The port to run the managed ZK instance
        """
        self.install_path = software_path
        self._classpath = classpath
        self.server_info = server_info
        self.host = "127.0.0.1"
        self.peers = peers
        self.working_path = tempfile.mkdtemp()
        self._running = False
        self.configuration_entries = configuration_entries
        self.java_system_properties = java_system_properties
        self.jaas_config = jaas_config
        self.ssl_configuration = (
            ssl_configuration if ssl_configuration is not None else {}
        )

    def run(self):
        """Run the ZooKeeper instance under a temporary directory.

        Writes ZK log messages to zookeeper.log in the current directory.
        """
        if self.running:
            return
        config_path = os.path.join(self.working_path, "zoo.cfg")
        jaas_config_path = os.path.join(self.working_path, "jaas.conf")
        log_path = os.path.join(self.working_path, "log")
        log4j_path = os.path.join(self.working_path, "log4j.properties")
        data_path = os.path.join(self.working_path, "data")
        truststore_path = os.path.join(self.working_path, "truststore.jks")
        keystore_path = os.path.join(self.working_path, "keystore.jks")

        # various setup steps
        if not os.path.exists(self.working_path):
            os.mkdir(self.working_path)
        if not os.path.exists(log_path):
            os.mkdir(log_path)
        if not os.path.exists(data_path):
            os.mkdir(data_path)

        try:
            self.ssl_configuration["truststore"].save(
                truststore_path, "apassword"
            )
            self.ssl_configuration["keystore"].save(keystore_path, "apassword")
        except Exception:
            log.exception("Unable to perform SSL configuration: ")
            raise

        with open(config_path, "w") as config:
            config.write(
                """
tickTime=2000
dataDir=%s
clientPort=%s
secureClientPort=%s
maxClientCnxns=0
admin.serverPort=%s
serverCnxnFactory=org.apache.zookeeper.server.NettyServerCnxnFactory
authProvider.1=org.apache.zookeeper.server.auth.SASLAuthenticationProvider
ssl.keyStore.location=%s
ssl.keyStore.password=apassword
ssl.trustStore.location=%s
ssl.trustStore.password=apassword
%s
"""
                % (
                    to_java_compatible_path(data_path),
                    self.server_info.client_port,
                    self.server_info.secure_client_port,
                    self.server_info.admin_port,
                    to_java_compatible_path(keystore_path),
                    to_java_compatible_path(truststore_path),
                    "\n".join(self.configuration_entries),
                )
            )  # NOQA

        # setup a replicated setup if peers are specified
        if self.peers:
            servers_cfg = []
            for p in chain((self.server_info,), self.peers):
                servers_cfg.append(
                    "server.%s=localhost:%s:%s:%s"
                    % (
                        p.server_id,
                        p.leader_port,
                        p.election_port,
                        p.peer_type,
                    )
                )

            with open(config_path, "a") as config:
                config.write(
                    """
initLimit=4
syncLimit=2
%s
peerType=%s
"""
                    % ("\n".join(servers_cfg), self.server_info.peer_type)
                )

        # Write server ids into datadir
        with open(os.path.join(data_path, "myid"), "w") as myid_file:
            myid_file.write(str(self.server_info.server_id))
        # Write JAAS configuration
        with open(jaas_config_path, "w") as jaas_file:
            jaas_file.write(self.jaas_config or "")
        with open(log4j_path, "w") as log4j:
            log4j.write(
                """
# DEFAULT: console appender only
log4j.rootLogger=INFO, ROLLINGFILE
log4j.appender.ROLLINGFILE.layout=org.apache.log4j.PatternLayout
log4j.appender.ROLLINGFILE.layout.ConversionPattern=%d{ISO8601} \
[myid:%X{myid}] - %-5p [%t:%C{1}@%L] - %m%n
log4j.appender.ROLLINGFILE=org.apache.log4j.RollingFileAppender
log4j.appender.ROLLINGFILE.Threshold=DEBUG
log4j.appender.ROLLINGFILE.File="""
                + to_java_compatible_path(  # NOQA
                    self.working_path + os.sep + "zookeeper.log\n"
                )
            )

        args = (
            [
                "java",
                "-cp",
                self.classpath,
                # make_digest_acl_credential assumes UTF-8, but ZK decodes
                # digest auth packets using the JVM's default "charset"--which
                # depends on the environment.  Force it to use UTF-8 to avoid
                # test failures.
                "-Dfile.encoding=UTF-8",
                # "-Dlog4j.debug",
                "-Dreadonlymode.enabled=true",
                "-Dzookeeper.log.dir=%s" % log_path,
                "-Dzookeeper.root.logger=INFO,CONSOLE",
                "-Dlog4j.configuration=file:%s" % log4j_path,
                # OS X: Prevent java from appearing in menu bar, process dock
                # and from activation of the main workspace on run.
                "-Djava.awt.headless=true",
                # JAAS configuration for SASL authentication
                "-Djava.security.auth.login.config=%s" % jaas_config_path,
            ]
            + list(self.java_system_properties)
            + [
                "org.apache.zookeeper.server.quorum.QuorumPeerMain",
                config_path,
            ]
        )
        self.process = subprocess.Popen(args=args)
        log.info(
            "Started zookeeper process %s on port %s using args %s",
            self.process.pid,
            self.server_info.client_port,
            args,
        )
        self._running = True

    @property
    def classpath(self):
        """Get the classpath necessary to run ZooKeeper."""

        if self._classpath:
            return self._classpath

        # Two possibilities, as seen in zkEnv.sh:
        # Check for a release - top-level zookeeper-*.jar?
        jars = glob((os.path.join(self.install_path, "zookeeper-*.jar")))
        if jars:
            # Release build (`ant package`)
            jars.extend(glob(os.path.join(self.install_path, "lib", "*.jar")))
            jars.extend(glob(os.path.join(self.install_path, "*.jar")))
            # support for different file locations on Debian/Ubuntu
            jars.extend(glob(os.path.join(self.install_path, "log4j-*.jar")))
            jars.extend(
                glob(os.path.join(self.install_path, "slf4j-api-*.jar"))
            )
            jars.extend(
                glob(os.path.join(self.install_path, "slf4j-log4j*.jar"))
            )
        else:
            # Development build (plain `ant`)
            jars = glob(
                (os.path.join(self.install_path, "build", "zookeeper-*.jar"))
            )
            jars.extend(
                glob(os.path.join(self.install_path, "build", "lib", "*.jar"))
            )

        return os.pathsep.join(jars)

    @property
    def address(self):
        """Get the address of the ZooKeeper instance."""
        return "%s:%s" % (self.host, self.client_port)

    @property
    def secure_address(self):
        """Get the address of the SSL ZooKeeper instance."""
        return "%s:%s" % (self.host, self.secure_client_port)

    @property
    def running(self):
        return self._running

    @property
    def client_port(self):
        return self.server_info.client_port

    @property
    def secure_client_port(self):
        return self.server_info.secure_client_port

    def reset(self):
        """Stop the zookeeper instance, cleaning out its on disk-data."""
        self.stop()
        shutil.rmtree(os.path.join(self.working_path, "data"), True)
        os.mkdir(os.path.join(self.working_path, "data"))
        with open(os.path.join(self.working_path, "data", "myid"), "w") as fh:
            fh.write(str(self.server_info.server_id))

    def stop(self):
        """Stop the Zookeeper instance, retaining on disk state."""
        if not self.running:
            return
        self.process.terminate()
        self.process.wait()
        if self.process.returncode != 0:
            log.warn(
                "Zookeeper process %s failed to terminate with"
                " non-zero return code (it terminated with %s return"
                " code instead)",
                self.process.pid,
                self.process.returncode,
            )
        self._running = False

    def destroy(self):
        """Stop the ZooKeeper instance and destroy its on disk-state"""
        # called by at exit handler, reimport to avoid cleanup race.
        self.stop()

        shutil.rmtree(self.working_path, True)

    def get_logs(self, num_lines=100):
        log_path = pathlib.Path(self.working_path, "zookeeper.log")
        if log_path.exists():
            log_file = log_path.open("r")
            lines = log_file.readlines()
            return lines[-num_lines:]
        return []


class ZookeeperCluster(object):
    def __init__(
        self,
        install_path=None,
        classpath=None,
        size=3,
        port_offset=20000,
        observer_start_id=-1,
        configuration_entries=(),
        java_system_properties=(),
        jaas_config=None,
    ):
        self._install_path = install_path
        self._classpath = classpath
        self._servers = []
        self._ssl_configuration = {}
        self.perform_ssl_certs_generation()

        # Calculate ports and peer group
        port = port_offset
        peers = []

        for i in range(size):
            server_id = i + 1
            if observer_start_id != -1 and server_id >= observer_start_id:
                peer_type = "observer"
            else:
                peer_type = "participant"
            info = ServerInfo(
                server_id,
                port,
                port + 4,
                port + 1,
                port + 2,
                port + 3,
                peer_type,
            )
            peers.append(info)
            port += 10

        # Instantiate Managed ZK Servers
        for i in range(size):
            server_peers = list(peers)
            server_info = server_peers.pop(i)
            self._servers.append(
                ManagedZooKeeper(
                    self._install_path,
                    server_info,
                    server_peers,
                    classpath=self._classpath,
                    configuration_entries=configuration_entries,
                    java_system_properties=java_system_properties,
                    jaas_config=jaas_config,
                    ssl_configuration=dict(self._ssl_configuration),
                )
            )

    def __getitem__(self, k):
        return self._servers[k]

    def __iter__(self):
        return iter(self._servers)

    def start(self):
        # Zookeeper client expresses a preference for either lower ports or
        # lexicographical ordering of hosts, to ensure that all servers have a
        # chance to startup, start them in reverse order.
        for server in reversed(list(self)):
            server.run()
        # Giving the servers a moment to start, decreases the overall time
        # required for a client to successfully connect (2s vs. 4s without
        # the sleep).
        import time

        time.sleep(2)

    def stop(self):
        for server in self:
            server.stop()
        self._servers = []

    def terminate(self):
        for server in self:
            server.destroy()

    def reset(self):
        for server in self:
            server.reset()

    def get_logs(self):
        logs = []
        for server in self:
            logs += server.get_logs()
        return logs

    def perform_ssl_certs_generation(self):
        if self._ssl_configuration:
            return

        # generate CA key
        ca_key = OpenSSL.crypto.PKey()
        ca_key.generate_key(OpenSSL.crypto.TYPE_RSA, 2048)

        # generate CA
        ca_cert = OpenSSL.crypto.X509()
        ca_cert.set_version(2)
        ca_cert.set_serial_number(1)
        ca_cert.get_subject().CN = "ca.kazoo.org"
        ca_cert.gmtime_adj_notBefore(0)
        ca_cert.gmtime_adj_notAfter(24 * 60 * 60)
        ca_cert.set_issuer(ca_cert.get_subject())
        ca_cert.set_pubkey(ca_key)
        ca_cert.add_extensions(
            [
                OpenSSL.crypto.X509Extension(
                    b"basicConstraints", True, b"CA:TRUE, pathlen:0"
                ),
                OpenSSL.crypto.X509Extension(
                    b"keyUsage", True, b"keyCertSign, cRLSign"
                ),
                OpenSSL.crypto.X509Extension(
                    b"subjectKeyIdentifier", False, b"hash", subject=ca_cert
                ),
            ]
        )
        ca_cert.sign(ca_key, "sha256")

        # generate server cert
        server_key = OpenSSL.crypto.PKey()
        server_key.generate_key(OpenSSL.crypto.TYPE_RSA, 2048)
        server_cert = OpenSSL.crypto.X509()
        server_cert.get_subject().CN = "localhost"
        server_cert.set_serial_number(2)
        server_cert.gmtime_adj_notBefore(0)
        server_cert.gmtime_adj_notAfter(24 * 60 * 60)
        server_cert.set_issuer(ca_cert.get_subject())
        server_cert.set_pubkey(server_key)
        server_cert.sign(ca_key, "sha256")

        # generate client cert
        client_key = OpenSSL.crypto.PKey()
        client_key.generate_key(OpenSSL.crypto.TYPE_RSA, 2048)
        client_cert = OpenSSL.crypto.X509()
        client_cert.get_subject().CN = "client"
        client_cert.set_serial_number(3)
        client_cert.gmtime_adj_notBefore(0)
        client_cert.gmtime_adj_notAfter(24 * 60 * 60)
        client_cert.set_issuer(ca_cert.get_subject())
        client_cert.set_pubkey(client_key)
        client_cert.sign(ca_key, "sha256")

        dumped_ca_cert = OpenSSL.crypto.dump_certificate(
            OpenSSL.crypto.FILETYPE_ASN1, ca_cert
        )

        tce = jks.TrustedCertEntry.new("kazoo ca", dumped_ca_cert)
        truststore = jks.KeyStore.new("jks", [tce])

        dumped_server_cert = OpenSSL.crypto.dump_certificate(
            OpenSSL.crypto.FILETYPE_ASN1, server_cert
        )
        dumped_server_key = OpenSSL.crypto.dump_privatekey(
            OpenSSL.crypto.FILETYPE_ASN1, server_key
        )

        server_pke = jks.PrivateKeyEntry.new(
            "server cert", [dumped_server_cert], dumped_server_key, "rsa_raw"
        )

        keystore = jks.KeyStore.new("jks", [server_pke])

        self._ssl_configuration = {
            "ca_cert": ca_cert,
            "ca_key": ca_key,
            "ca_cert_pem": OpenSSL.crypto.dump_certificate(
                OpenSSL.crypto.FILETYPE_PEM, ca_cert
            ),
            "server_cert": server_cert,
            "server_key": server_key,
            "client_cert": client_cert,
            "client_key": client_key,
            "client_cert_pem": OpenSSL.crypto.dump_certificate(
                OpenSSL.crypto.FILETYPE_PEM, client_cert
            ),
            "client_key_pem": OpenSSL.crypto.dump_privatekey(
                OpenSSL.crypto.FILETYPE_PEM, client_key
            ),
            "truststore": truststore,
            "keystore": keystore,
        }

    def get_ssl_client_configuration(self):
        if not self._ssl_configuration:
            raise RuntimeError("SSL not configured yet.")
        return {
            "client_key": self._ssl_configuration["client_key_pem"],
            "client_cert": self._ssl_configuration["client_cert_pem"],
            "ca_cert": self._ssl_configuration["ca_cert_pem"],
        }
