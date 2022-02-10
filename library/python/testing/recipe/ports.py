import os
import sys
import subprocess
import time

from yatest.common.network import PortManager


def __get_port_range():
    port_count = int(sys.argv[1])
    pid_filename = sys.argv[2]
    port_manager = PortManager()
    start_port = port_manager.get_port_range(None, port_count)
    sys.stderr.write(str(start_port) + "\n")
    with open(pid_filename, 'w') as afile:
        afile.write(str(os.getpid()))
    while 1:
        time.sleep(1)


def get_port_range(port_count=1, pid_filename="recipe_port.pid"):
    env = os.environ.copy()
    env["Y_PYTHON_ENTRY_POINT"] = "library.python.testing.recipe.ports:__get_port_range"
    res = subprocess.Popen([sys.argv[0], str(port_count), pid_filename], env=env, cwd=os.getcwd(), stderr=subprocess.PIPE)
    while not os.path.exists(pid_filename):
        time.sleep(0.01)
    port_start = int(res.stderr.readline())
    return port_start


def release_port_range(pid_filename="recipe_port.pid"):
    with open(pid_filename, 'r') as afile:
        os.kill(int(afile.read()), 9)
