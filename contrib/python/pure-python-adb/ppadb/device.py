import re
import os

from ppadb.command.transport import Transport
from ppadb.command.serial import Serial

from ppadb.plugins.device.input import Input
from ppadb.plugins.device.utils import Utils
from ppadb.plugins.device.wm import WM
from ppadb.plugins.device.traffic import Traffic
from ppadb.plugins.device.cpustat import CPUStat
from ppadb.plugins.device.batterystats import BatteryStats

from ppadb.sync import Sync

from ppadb.utils.logger import AdbLogging

from ppadb import InstallError

logger = AdbLogging.get_logger(__name__)

try:
    FileNotFoundError
except NameError:
    FileNotFoundError = IOError

try:
    from shlex import quote as cmd_quote
except ImportError:
    from pipes import quote as cmd_quote


class Device(Transport, Serial, Input, Utils, WM, Traffic, CPUStat, BatteryStats):
    INSTALL_RESULT_PATTERN = "(Success|Failure|Error)\s?(.*)"
    UNINSTALL_RESULT_PATTERN = "(Success|Failure.*|.*Unknown package:.*)"

    def __init__(self, client, serial):
        self.client = client
        self.serial = serial

    def create_connection(self, set_transport=True, timeout=None):
        conn = self.client.create_connection(timeout=timeout)

        if set_transport:
            self.transport(conn)

        return conn

    def _push(self, src, dest, mode, progress):
        # Create a new connection for file transfer
        sync_conn = self.sync()
        sync = Sync(sync_conn)

        with sync_conn:
            sync.push(src, dest, mode, progress)

    def push(self, src, dest, mode=0o644, progress=None):
        if not os.path.exists(src):
            raise FileNotFoundError("Cannot find {}".format(src))
        elif os.path.isfile(src):
            self._push(src, dest, mode, progress)
        elif os.path.isdir(src):
            basename = os.path.basename(src)

            for root, dirs, files in os.walk(src):
                root_dir_path = os.path.join(basename, root.replace(src, ""))

                self.shell("mkdir -p {}/{}".format(dest, root_dir_path))

                for item in files:
                    self._push(os.path.join(root, item), os.path.join(dest, root_dir_path, item), mode, progress)

    def pull(self, src, dest):
        sync_conn = self.sync()
        sync = Sync(sync_conn)

        with sync_conn:
            return sync.pull(src, dest)

    def install(self, path,
                forward_lock=False,  # -l
                reinstall=False,  # -r
                test=False,  # -t
                installer_package_name="",  # -i {installer_package_name}
                shared_mass_storage=False,  # -s
                internal_system_memory=False,  # -f
                downgrade=False,  # -d
                grand_all_permissions=False  # -g
                ):
        dest = Sync.temp(path)
        self.push(path, dest)

        parameters = []
        if forward_lock: parameters.append("-l")
        if reinstall: parameters.append("-r")
        if test: parameters.append("-t")
        if len(installer_package_name) > 0: parameters.append("-i {}".format(installer_package_name))
        if shared_mass_storage: parameters.append("-s")
        if internal_system_memory: parameters.append("-f")
        if downgrade: parameters.append("-d")
        if grand_all_permissions: parameters.append("-g")

        try:
            result = self.shell("pm install {} {}".format(" ".join(parameters), cmd_quote(dest)))
            match = re.search(self.INSTALL_RESULT_PATTERN, result)

            if match and match.group(1) == "Success":
                return True
            elif match:
                groups = match.groups()
                raise InstallError(dest, groups[1])
            else:
                raise InstallError(dest, result)
        finally:
            self.shell("rm -f {}".format(dest))

    def is_installed(self, package):
        result = self.shell('pm path {}'.format(package))

        if "package:" in result:
            return True
        else:
            return False

    def uninstall(self, package):
        result = self.shell('pm uninstall {}'.format(package))

        m = re.search(self.UNINSTALL_RESULT_PATTERN, result)

        if m and m.group(1) == "Success":
            return True
        elif m:
            logger.error(m.group(1))
            return False
        else:
            logger.error("There is no message after uninstalling")
            return False
