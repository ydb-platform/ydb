# -*- coding: utf-8 -*-
import time
import logging
import tempfile
import subprocess
from concurrent import futures


logger = logging.getLogger(__name__)


def execute_command(command, timeout=60):
    logger.info("Running: {}".format(command))
    process = subprocess.Popen(command)
    wait_timeout(process, timeout)
    return process.returncode


def execute_command_with_output(command, timeout=60):
    logger.info("Running command = {}".format(command))
    list_of_lines = []
    with tempfile.TemporaryFile() as f_out, tempfile.TemporaryFile() as f_err:
        process = subprocess.Popen(command, stdout=f_out, stderr=f_err)
        wait_timeout(process, timeout)
        process_return_code = process.returncode
        f_err.flush()
        f_err.seek(0)
        std_err_lines = list(f_err.readlines())
        std_err_lines = [line.decode("utf-8", errors='replace') for line in std_err_lines]
        if process_return_code is not None:
            f_out.flush()
            f_out.seek(0)
            list_of_lines = list(f_out.readlines())
            list_of_lines = [line.decode("utf-8", errors='replace') for line in list_of_lines]

    logger.info("Finished execution command = {}".format(command))
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug('Command output = \n{}'.format('\n'.join(list_of_lines)))
        logger.debug('Stderr = \n{}'.format('\n'.join(std_err_lines)))
    return process_return_code, list_of_lines


def wait_timeout(process, timeout):
    start_time = time.time()
    while time.time() < start_time + timeout:
        process.poll()
        if process.returncode is not None:
            return
        time.sleep(0.5)
    process.kill()
    process.wait()


def execute_command_with_output_on_hosts(list_of_hosts, command, per_host_timeout=60, username=None):
    full_output = []
    full_retcode = 0
    pool = futures.ProcessPoolExecutor(8)
    fs = []
    for host in list_of_hosts:
        fs.append(
            pool.submit(
                execute_command_with_output_single_host,
                host, command,
                timeout=per_host_timeout,
                username=username
            )
        )

    for f in fs:
        retcode, output = f.result()
        if retcode:
            full_retcode = retcode
        full_output.extend(output)
    return full_retcode, full_output


def execute_command_with_output_single_host(host, command, timeout=60, username=None):
    username_at_host = host if username is None else username + '@' + host
    full_cmd = ["ssh", "-o", "StrictHostKeyChecking=no", "-o", "UserKnownHostsFile=/dev/null", "-A", username_at_host] + command

    retcode, output = execute_command_with_output(full_cmd, timeout=timeout)
    return retcode, output
