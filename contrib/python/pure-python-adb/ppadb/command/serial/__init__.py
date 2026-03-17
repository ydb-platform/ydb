from ppadb.command import Command


class Serial(Command):
    def _execute_cmd(self, cmd, with_response=True):
        conn = self.create_connection(set_transport=False)

        with conn:
            conn.send(cmd)
            if with_response:
                result = conn.receive()
                return result
            else:
                conn.check_status()

    def forward(self, local, remote, norebind=False):
        if norebind:
            cmd = "host-serial:{serial}:forward:norebind:{local};{remote}".format(
                serial=self.serial,
                local=local,
                remote=remote)
        else:
            cmd = "host-serial:{serial}:forward:{local};{remote}".format(
                serial=self.serial,
                local=local,
                remote=remote)

        self._execute_cmd(cmd, with_response=False)

    def list_forward(self):
        # According to https://android.googlesource.com/platform/system/core/+/master/adb/adb_listeners.cpp#129
        # And https://android.googlesource.com/platform/system/core/+/master/adb/SERVICES.TXT#130
        # The 'list-forward' always lists all existing forward connections from the adb server
        # So we need filter these by self.
        cmd = "host-serial:{serial}:list-forward".format(serial=self.serial)
        result = self._execute_cmd(cmd)

        forward_map = {}

        for line in result.split('\n'):
            if line:
                serial, local, remote = line.split()
                if serial == self.serial:
                    forward_map[local] = remote

        return forward_map

    def killforward(self, local):
        cmd = "host-serial:{serial}:killforward:{local}".format(serial=self.serial, local=local)
        self._execute_cmd(cmd, with_response=False)

    def killforward_all(self):
        # killforward-all command ignores the <host-prefix> and remove all the forward mapping.
        # So we need to implement this function by self
        forward_map = self.list_forward()
        for local, remote in forward_map.items():
            self.killforward(local)

    def get_device_path(self):
        cmd = "host-serial:{serial}:get-devpath".format(serial=self.serial)
        return self._execute_cmd(cmd)

    def get_serial_no(self):
        cmd = "host-serial:{serial}:get-serialno".format(serial=self.serial)
        return self._execute_cmd(cmd)

    def get_state(self):
        cmd = "host-serial:{serial}:get-state".format(serial=self.serial)
        return self._execute_cmd(cmd)
