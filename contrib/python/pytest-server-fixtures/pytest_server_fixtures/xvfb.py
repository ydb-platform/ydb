import codecs
import os
import signal
import subprocess
import time
from tempfile import mkdtemp

import pytest

from pytest_shutil.workspace import Workspace
from pytest_fixture_config import yield_requires_config

from pytest_server_fixtures import CONFIG


@pytest.yield_fixture(scope='function')
@yield_requires_config(CONFIG, ['xvfb_executable'])
def xvfb_server():
    """ Function-scoped Xvfb (X-Windows Virtual Frame Buffer) in a local thread.
    """
    test_server = XvfbServer()
    yield test_server
    test_server.close()


@pytest.yield_fixture(scope='session')
@yield_requires_config(CONFIG, ['xvfb_executable'])
def xvfb_server_sess():
    """ Session-scoped Xvfb (X-Windows Virtual Frame Buffer) in a local thread.
    """
    test_server = XvfbServer()
    yield test_server
    test_server.close()



# TODO: make this a TestServer, clean up print statements for proper logging
class XvfbServer(object):
    # see https://github.com/revnode/xvfb-run/blob/master/xvfb-run
    xvfb_command = 'Xvfb'
    xvfb_args = '-screen 0 1024x768x24 -nolisten tcp -reset -terminate'.split()
    display, authfile, process, fbmem = None, None, None, None

    def __init__(self):
        tmpdir = mkdtemp(prefix='XvfbServer.', dir=Workspace.get_base_tempdir())
        for servernum in range(os.getpid(), 65536):
            if os.path.exists('/tmp/.X{0}-lock'.format(servernum)):
                continue
            self.display = ':' + str(servernum)
            self.authfile = os.path.join(tmpdir, 'Xauthority.' + self.display)
            mcookie = codecs.encode(os.urandom(16), "hex_codec")
            subprocess.check_call(['xauth', '-f', self.authfile, 'add', self.display, '.', mcookie])
            errfile = os.path.join(tmpdir, 'Xvfb.' + self.display + '.err')
            with open(errfile, 'w') as f:  # use a file instead of a pipe to simplify polling
                p = subprocess.Popen([self.xvfb_command, self.display, '-fbdir', tmpdir] + self.xvfb_args,
                                     stderr=f, env=dict(os.environ, XAUTHORITY=self.authfile))
                self.fbmem = os.path.join(tmpdir, 'Xvfb_screen0')
                while not os.path.exists(self.fbmem):
                    if p.poll() is not None:
                        break
                    time.sleep(0.1)
                else:
                    p.poll()
                if p.returncode is not None:
                    with open(errfile) as f:
                        err = f.read()
                    if 'Server is already active for display' in err:
                        continue
                    else:
                        raise RuntimeError('Failed to start Xvfb', p.returncode, err)
            print('Xvfb started in ' + tmpdir)  # for debugging
            self.process = p
            # If we terminate abnormally, ensure the Xvfb server is cleaned up after us.
            self._cleanup_script = subprocess.Popen("""
while kill -0 {0} 2>/dev/null; do sleep 1; done
kill -INT {1}
while kill -0 {1} 2>/dev/null; do sleep 1; done
""".format(os.getpid(), p.pid), shell=True)
            break
        else:
            raise RuntimeError('Unable to find a free server number to start Xvfb')

    def close(self):
        if self.process is not None:
            if self.process.poll() is None:
                self.process.send_signal(signal.SIGINT)
                self.process.wait()
            self._cleanup_script.kill()
        self.process, self._cleanup_script = None, None

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        self.close()
