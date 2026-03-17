import re
import time

from ppadb import ClearError
from ppadb.command import Command

from ppadb.utils.logger import AdbLogging

logger = AdbLogging.get_logger(__name__)


class Transport(Command):
    def transport(self, connection):
        cmd = "host:transport:{}".format(self.serial)
        connection.send(cmd)

        return connection

    def shell(self, cmd, handler=None, timeout=None):
        conn = self.create_connection(timeout=timeout)

        cmd = "shell:{}".format(cmd)
        conn.send(cmd)

        if handler:
            handler(conn)
        else:
            result = conn.read_all()
            conn.close()
            return result.decode('utf-8')

    def sync(self):
        conn = self.create_connection()

        cmd = "sync:"
        conn.send(cmd)

        return conn

    def screencap(self):
        conn = self.create_connection()

        with conn:
            cmd = "shell:/system/bin/screencap -p"
            conn.send(cmd)
            result = conn.read_all()

        if result and len(result) > 5 and result[5] == 0x0d:
            return result.replace(b'\r\n', b'\n')
        else:
            return result

    def clear(self, package):
        clear_result_pattern = "(Success|Failed)"

        result = self.shell("pm clear {}".format(package))
        m = re.search(clear_result_pattern, result)

        if m is not None and m.group(1) == "Success":
            return True
        else:
            logger.error(result)
            raise ClearError(package, result.strip())

    def framebuffer(self):
        raise NotImplemented()

    def list_features(self):
        result = self.shell("pm list features 2>/dev/null")

        result_pattern = "^feature:(.*?)(?:=(.*?))?\r?$"
        features = {}
        for line in result.split('\n'):
            m = re.match(result_pattern, line)
            if m:
                value = True if m.group(2) is None else m.group(2)
                features[m.group(1)] = value

        return features

    def list_packages(self):
        result = self.shell("pm list packages 2>/dev/null")
        result_pattern = "^package:(.*?)\r?$"

        packages = []
        for line in result.split('\n'):
            m = re.match(result_pattern, line)
            if m:
                packages.append(m.group(1))

        return packages

    def get_properties(self):
        result = self.shell("getprop")
        result_pattern = "^\[([\s\S]*?)\]: \[([\s\S]*?)\]\r?$"

        properties = {}
        for line in result.split('\n'):
            m = re.match(result_pattern, line)
            if m:
                properties[m.group(1)] = m.group(2)

        return properties

    def list_reverses(self):
        conn = self.create_connection()
        with conn:
            cmd = "reverse:list-forward"
            conn.send(cmd)
            result = conn.receive()

        reverses = []
        for line in result.split('\n'):
            if not line:
                continue

            serial, remote, local = line.split()
            reverses.append(
                {
                    'remote': remote,
                    'local': local
                }
            )

        return reverses

    def local(self, path):
        if ":" not in path:
            path = "localfilesystem:{}".format(path)

        conn = self.create_connection()
        conn.send(path)

        return conn

    def log(self, name):
        conn = self.create_connection()
        cmd = "log:{}".format(name)

        conn.send(cmd)

        return conn

    def logcat(self, clear=False):
        raise NotImplemented()

    def reboot(self):
        conn = self.create_connection()

        with conn:
            conn.send("reboot:")
            conn.read_all()

        return True

    def remount(self):
        conn = self.create_connection()

        with conn:
            conn.send("remount:")

        return True

    def reverse(self, remote, local):
        cmd = "reverse:forward:{remote}:{local}".format(
            remote=remote,
            local=local
        )

        conn = self.create_connection()
        with conn:
            conn.send(cmd)

            # Check status again, the first check is send cmd status, the second time is check the forward status.
            conn.check_status()

        return True

    def root(self):
        # Restarting adbd as root
        conn = self.create_connection()
        with conn:
            conn.send("root:")
            result = conn.read_all().decode('utf-8')

            if "restarting adbd as root" in result:
                return True
            else:
                raise RuntimeError(result.strip())

    def wait_boot_complete(self, timeout=60, timedelta=1):
        """
        :param timeout: second
        :param timedelta: second
        """
        cmd = 'getprop sys.boot_completed'

        end_time = time.time() + timeout

        while True:
            try:
                result = self.shell(cmd)
            except RuntimeError as e:
                logger.warning(e)
                continue

            if result.strip() == "1":
                return True

            if time.time() > end_time:
                raise TimeoutError()
            elif timedelta > 0:
                time.sleep(timedelta)
