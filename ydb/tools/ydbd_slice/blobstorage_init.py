import logging
import sys

import tenacity as tc

logger = logging.getLogger(__name__)

BLOBSTORAGE_INIT_RETRY_DEADLINE_SECONDS = 300


class BlobstorageConfigInitFailed(Exception):
    def __init__(self, host, cmd, retcode, stdout, stderr):
        self.host = host
        self.cmd = cmd
        self.retcode = retcode
        self.stdout = stdout
        self.stderr = stderr
        super().__init__(
            "execution '{cmd}' finished with '{retcode}' retcode".format(cmd=cmd, retcode=retcode)
        )


def _execute_on_node(nodes, host, cmd):
    results = {}
    running_jobs = nodes.execute_async_ret(cmd, check_retcode=False, nodes=[host], results=results)
    nodes._check_async_execution(running_jobs, check_retcode=False, results=results)
    return results.get(host, {'retcode': -1, 'stdout': '', 'stderr': ''})


def _log_before_retry(retry_state):
    exc = retry_state.outcome.exception()
    if not isinstance(exc, BlobstorageConfigInitFailed):
        return
    logger.warning(
        "blobstorage config init failed on host=%s (attempt=%d, retcode=%s, command=%s)\n"
        "stdout is:\n%s\nstderr is:\n%s",
        exc.host,
        retry_state.attempt_number,
        exc.retcode,
        exc.cmd,
        exc.stdout,
        exc.stderr,
    )


@tc.retry(
    retry=tc.retry_if_exception_type(BlobstorageConfigInitFailed),
    wait=tc.wait_exponential(max=60),
    stop=tc.stop_after_delay(BLOBSTORAGE_INIT_RETRY_DEADLINE_SECONDS),
    reraise=True,
    before_sleep=_log_before_retry,
)
def _run_blobstorage_config_init(nodes, host, cmd):
    logger.info("Running blobstorage config init on host=%s: %s", host, cmd)
    result = _execute_on_node(nodes, host, cmd)
    retcode = result.get('retcode')
    stdout = result.get('stdout', '')
    stderr = result.get('stderr', '')
    if retcode == 0:
        logger.info("blobstorage config init succeeded on host=%s", host)
        return
    raise BlobstorageConfigInitFailed(host, cmd, retcode, stdout, stderr)


def run_blobstorage_config_init_with_retry(nodes, host, cmd):
    try:
        _run_blobstorage_config_init(nodes, host, cmd)
    except BlobstorageConfigInitFailed as exc:
        status_line = str(exc)
        logger.critical(
            "{status_line}\n"
            "stdout is:\n"
            "{stdout}\n"
            "stderr is:\n"
            "{stderr}".format(
                status_line=status_line,
                stdout=exc.stdout,
                stderr=exc.stderr,
            )
        )
        sys.exit(status_line)
