# -*- coding: utf-8 -*-
import os

import yatest

DEFAULT_COMMAND_TIMEOUT = 60


def cluster_endpoint(cluster):
    return f'{cluster.nodes[1].host}:{cluster.nodes[1].grpc_port}'


def cluster_http_endpoint(cluster):
    return f'{cluster.nodes[1].host}:{cluster.nodes[1].mon_port}'


def _execute(full_cmd, token, timeout, check_exit_code):
    try:
        proc_result = yatest.common.process.execute(
            full_cmd, check_exit_code=False, env={'YDB_TOKEN': token}, timeout=timeout
        )
    except yatest.common.ExecutionTimeoutError as e:
        assert False, (
            f'Command\n{full_cmd}\n exceeded {timeout}s timeout: {e}'
        )
    if check_exit_code and proc_result.exit_code != 0:
        assert (
            False
        ), f'Command\n{full_cmd}\n finished with exit code {proc_result.exit_code}, stderr:\n\n{proc_result.std_err.decode("utf-8")}\n\nstdout:\n{proc_result.std_out.decode("utf-8")}'
    return proc_result


def execute_ydbd(cluster, token, cmd, check_exit_code=True, timeout=DEFAULT_COMMAND_TIMEOUT):
    ydbd_binary_path = cluster.nodes[1].binary_path
    full_cmd = [ydbd_binary_path, '-s', f'grpc://{cluster_endpoint(cluster)}']
    full_cmd += cmd
    return _execute(full_cmd, token, timeout, check_exit_code)


def get_dstool_binary_path():
    return yatest.common.binary_path(os.getenv('YDB_DSTOOL_BINARY'))


def execute_dstool_grpc(cluster, token, cmd, check_exit_code=True, return_process=False,
                        timeout=DEFAULT_COMMAND_TIMEOUT):
    full_cmd = [get_dstool_binary_path(), '--endpoint', f'grpc://{cluster_endpoint(cluster)}']
    full_cmd += cmd
    proc_result = _execute(full_cmd, token, timeout, check_exit_code)
    if return_process:
        return proc_result
    return proc_result.std_out
