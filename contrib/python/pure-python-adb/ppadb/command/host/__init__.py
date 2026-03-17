from ppadb.device import Device
from ppadb.command import Command


class Host(Command):
    CONNECT_RESULT_PATTERN = "(connected to|already connected)"

    OFFLINE = "offline"
    DEVICE = "device"
    BOOTLOADER = "bootloader"

    def _execute_cmd(self, cmd, with_response=True):
        with self.create_connection() as conn:
            conn.send(cmd)
            if with_response:
                result = conn.receive()
                return result
            else:
                conn.check_status()

    def devices(self, state=None):
        cmd = "host:devices"
        result = self._execute_cmd(cmd)

        devices = []

        for line in result.split('\n'):
            if not line:
                break

            tokens = line.split()
            if state and len(tokens) > 1 and tokens[1] != state:
                continue

            devices.append(Device(self, tokens[0]))

        return devices

    def features(self):
        cmd = "host:features"
        result = self._execute_cmd(cmd)
        features = result.split(",")
        return features

    def version(self):
        with self.create_connection() as conn:
            conn.send("host:version")
            version = conn.receive()
            return int(version, 16)

    def kill(self):
        """
            Ask the ADB server to quit immediately. This is used when the
            ADB client detects that an obsolete server is running after an
            upgrade.
        """
        with self.create_connection() as conn:
            conn.send("host:kill")

        return True

    def killforward_all(self):
        cmd = "host:killforward-all"
        self._execute_cmd(cmd, with_response=False)

    def list_forward(self):
        cmd = "host:list-forward"
        result = self._execute_cmd(cmd)

        device_forward_map = {}
        for line in result.split('\n'):
            if line:
                serial, local, remote = line.rsplit(' ', 2)
                if serial not in device_forward_map:
                    device_forward_map[serial] = {}

                device_forward_map[serial][local] = remote

        return device_forward_map

    def remote_connect(self, host, port):
        cmd = "host:connect:%s:%d" % (host, port)
        result = self._execute_cmd(cmd)

        return "connected" in result

    def remote_disconnect(self, host=None, port=None):
        cmd = "host:disconnect:"
        if host:
            cmd = "host:disconnect:{}".format(host)
            if port:
                cmd = "{}:{}".format(cmd, port)

        return self._execute_cmd(cmd)
