import os.path
import psutil
import socket
import subprocess
import time
import yatest.common


def find_free_ports(count):
    sockets = []
    ports = []

    for _ in range(count):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(('', 0))
        ports.append(sock.getsockname()[1])
        sockets.append(sock)

    for sock in sockets:
        sock.close()
    return ports


def start_daemon(command, environment, is_alive_check, pid_file_name, timeout=60, daemon_name=None):
    daemon_name = daemon_name or os.path.basename(command[0])
    stdout_path = yatest.common.output_path('{}.out.log').format(daemon_name)
    stderr_path = yatest.common.output_path('{}.err.log').format(daemon_name)

    process = subprocess.Popen(
        command,
        stdout=open(stdout_path, 'w'),
        stderr=open(stderr_path, 'w'),
        env=environment)
    with open(pid_file_name, 'w') as fout:
        fout.write(str(process.pid))

    for attempts in range(timeout):
        result = process.poll()
        if result is not None:
            raise RuntimeError(
                'Could not launch "{}" with exit code {}\nStdout: {}\nStderr: {}'
                .format(daemon_name, result, stdout_path, stderr_path)
            )

        if is_alive_check():
            return

        time.sleep(1)

    raise RuntimeError(
        'Could not launch "{}" for {} seconds\nStdout: {}\nStderr: {}'
        .format(daemon_name, timeout, stdout_path, stderr_path)
    )


def pid_exists(pid):
    try:
        if psutil.Process(pid).status() == psutil.STATUS_ZOMBIE:
            return False
    except psutil.NoSuchProcess:
        return False
    return True


def stop_daemon(pid, signal=15):
    pid = int(pid)
    if not pid_exists(pid):
        return False

    os.kill(pid, signal)
    while pid_exists(pid):
        time.sleep(1)

    return True
