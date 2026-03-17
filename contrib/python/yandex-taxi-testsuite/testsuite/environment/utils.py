import os
import socket
import time

DOCKERTEST_WORKER = os.getenv('DOCKERTEST_WORKER', '')


class BaseError(Exception):
    pass


class RootUserForbiddenError(BaseError):
    pass


class EnvironmentVariableError(BaseError):
    pass


def test_tcp_connection(host: str, port: int, timeout: float = 1.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def wait_tcp_connection(host: str, port: int, timeout: float = 1.0) -> bool:
    start_time = time.perf_counter()
    while True:
        time_passed = time.perf_counter() - start_time
        if time_passed >= timeout:
            return False

        if test_tcp_connection(
            host=host,
            port=port,
            timeout=timeout - time_passed,
        ):
            return True

        time.sleep(0.1)


def ensure_non_root_user() -> None:
    if not os.getenv('TESTSUITE_ALLOW_ROOT') and os.getuid() == 0:
        raise RootUserForbiddenError('Running testsuite as root is forbidden')


def getenv_str(key: str, default: str) -> str:
    env_value = os.getenv(key)
    if env_value is None:
        return default
    return env_value


def getenv_int(key: str, default: int) -> int:
    env_value = os.getenv(key)
    if env_value is None:
        return default
    try:
        result = int(env_value)
    except ValueError as err:
        raise EnvironmentVariableError(
            f'{key} environment variable is expected to have integer value. '
            f'Actual value: {env_value}',
        ) from err
    else:
        return result


def getenv_float(key: str, default: float) -> float:
    env_value = os.getenv(key)
    if env_value is None:
        return default
    try:
        result = float(env_value)
    except ValueError as err:
        raise EnvironmentVariableError(
            f'{key} environment variable is expected to have float value. '
            f'Actual value: {env_value}',
        ) from err
    else:
        return result


def getenv_ints(
    key: str,
    default: tuple[int, ...],
) -> tuple[int, ...]:
    env_value = os.getenv(key, None)
    if env_value is None:
        return default
    try:
        result = tuple(int(value) for value in env_value.split(','))
    except ValueError as err:
        raise EnvironmentVariableError(
            f'{key} environment variable is expected to be a comma-separated '
            f'list of integers. Actual value: {env_value}',
        ) from err
    else:
        return result
