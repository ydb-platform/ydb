import sys
import re
from io import BytesIO as StringIO
from subprocess import Popen, check_output, PIPE, STDOUT

from ncclient.transport.errors import SessionCloseError, TransportError, PermissionError
from ncclient.transport.ssh import SSHSession

MSG_DELIM = b"]]>]]>"
NETCONF_SHELL = 'netconf'


class IOProc(SSHSession):

    def __init__(self, device_handler):
        SSHSession.__init__(self, device_handler)
        self._host_keys = None
        self._transport = None
        self._connected = False
        self._channel = None
        self._channel_id = None
        self._channel_name = None
        self._buffer = StringIO()  # for incoming data
        # parsing-related, see _parse()
        self._parsing_state = 0
        self._parsing_pos = 0
        self._device_handler = device_handler

    def close(self):
        stdout, stderr = self._channel.communicate()
        self._channel = None
        self._connected = False

    def connect(self):
        stdoutdata = check_output(NETCONF_SHELL, shell=True, stdin=PIPE,
                                  stderr=STDOUT).decode(encoding="utf8")
        if 'error: Restricted user session' in stdoutdata:
            obj = re.search(r'<error-message>\n?(.*)\n?</error-message>', stdoutdata, re.M)
            if obj:
                raise PermissionError(obj.group(1))
            else:
                raise PermissionError('Restricted user session')
        elif 'xml-mode: command not found' in stdoutdata:
            raise PermissionError('xml-mode: command not found')
        self._channel = Popen([NETCONF_SHELL],
                              stdin=PIPE, stdout=PIPE, stderr=STDOUT)
        self._connected = True
        self._channel_id = self._channel.pid
        self._channel_name = "netconf-shell"
        self._post_connect()
        return

    def run(self):
        chan = self._channel
        q = self._q
        try:
            while True:
                # write
                data = q.get().encode() + MSG_DELIM
                chan.stdin.write(data)
                chan.stdin.flush()
                # read
                data = []
                while True:
                    line = chan.stdout.readline()
                    data.append(line)
                    if MSG_DELIM in line:
                        break
                self._buffer.write(b''.join(data))
                self._parse()
        except Exception as e:
            self._dispatch_error(e)
            self.close()

    @property
    def transport(self):
        "Underlying `paramiko.Transport <http://www.lag.net/paramiko/docs/paramiko.Transport-class.html>`_ object. This makes it possible to call methods like :meth:`~paramiko.Transport.set_keepalive` on it."
        return self._transport
