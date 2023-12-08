import re
import time
import logging
import subprocess as sp
from collections import deque


logger = logging.getLogger(__name__)


def run(cmd, timeout=10, stop_timeout=10, log_stdout=False, log_stderr=False):
    if not isinstance(cmd, list):
        raise ValueError('cmd must be list')
    command = ['kubectl'] + cmd
    proc = sp.Popen(command, stdout=sp.PIPE, stderr=sp.PIPE, text=True)
    try:
        stdout, stderr = proc.communicate(timeout=timeout)
    except sp.TimeoutExpired:
        logger.info('kubectl command timed out, terminating')
        proc.terminate()
        try:
            stdout, stderr = proc.communicate(timeout=stop_timeout)
        except sp.TimeoutExpired:
            logger.info('kubectl command stop timed out, killing')
            proc.kill()
            stdout, stderr = proc.communicate()
    logger.debug(f'kubectl return code: {proc.returncode}')
    if log_stdout:
        for line in stdout.split('\n'):
            logger.debug(f'stdout: {line}')
    if log_stderr:
        for line in stderr.split('\n'):
            logger.debug(f'stderr: {line}')
    return proc.returncode, stdout, stderr


def apply(path, namespace=None):
    logger.info('applying manifest %s, started', path)
    command = []
    if namespace:
        command.extend(['-n', namespace])
    command.extend(['apply', '-f', path])
    return run(command)


def get_pods_by_selector(label_selector, namespace=None):
    logger.info(f'getting pods by selector {label_selector} in namespace {namespace}')
    command = []
    if namespace:
        command.extend(['-n', namespace])
    command.extend(['get', 'pods', '-l', label_selector, '-o', 'custom-columns=NAME:.metadata.name,UID:.metadata.uid'])
    rc, stdout, stderr = run(command)
    if rc != 0:
        raise RuntimeError(
            f"kubectl get pods: failed with code {rc}, error: {stderr}"
        )
    pod_list = []
    for n, line in enumerate(stdout.strip().split('\n')):
        if n == 0:
            continue
        name, uid = re.split(r'\s+', line)
        pod_list.append((namespace, name, uid))
    return pod_list


def restart_pods_in_parallel(pods, window=20):
    logger.info(f'restarting {len(pods)} pods, started')

    pods_failed = []
    pods_waiting = deque(sorted(pods))
    pods_processing = {}
    pods_restart_count = 0
    while len(pods_waiting) > 0 or len(pods_processing) > 0:
        while len(pods_waiting) > 0 and len(pods_processing) < window:
            pod_namespace, pod_name, _ = pods_waiting.popleft()
            pods_restart_count += 1
            logger.info('restarting pod %s/%s (%d/%d)', pod_namespace, pod_name, pods_restart_count, len(pods))
            proc = sp.Popen(
                ['kubectl', '-n', pod_namespace, 'delete', 'pod', '--now=true', '--wait=false', pod_name],
                stdout=sp.PIPE, stderr=sp.PIPE,
            )
            pods_processing[(pod_namespace, pod_name)] = proc
            time.sleep(0.05)

        pods_done = []
        for (pod_namespace, pod_name), proc in pods_processing.items():
            rc = proc.poll()
            if rc is not None:
                if rc != 0:
                    _, stderr = proc.communicate()
                    logger.error("kubectl delete pod %s/%s: failed with code %d, error: %s" % (
                        pod_namespace, pod_name, proc.returncode, stderr)
                    )
                    pods_failed.append((pod_namespace, pod_name))
                pods_done.append((pod_namespace, pod_name))

        for pod_namespace, pod_name in pods_done:
            pods_processing.pop((pod_namespace, pod_name))

        time.sleep(0.05)

    if len(pods_failed) > 0:
        logger.info('restarting pods failed for %d pods', len(pods_failed))
        raise RuntimeError('failed to restart pods: %s' % pods_failed)
    else:
        logger.info('restarting pods succeeded')
