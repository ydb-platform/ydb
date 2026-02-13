import subprocess
import os
import shutil
import signal
import time
import pytest
from typing import Tuple, List, Generator


# 1 GiB should be enough
DISK_IMAGE_SIZE = 1024 * 1024 * 1024
WORK_DIR = "work"
TEST_SERVER_BINARY_ENV_PATH = "TEST_SERVER_BINARY"
BLKIO_BENCH_ENV_PATH = "BLKIO_BENCH_BINARY"


def base_dir_abs_path() -> str:
    return os.path.dirname(os.path.abspath(__file__))


def build_dir() -> str:
    return os.path.join(base_dir_abs_path(), os.pardir, "build")


@pytest.fixture(scope="session")
def blkio_bench() -> str:
    env_path = os.environ.get(BLKIO_BENCH_ENV_PATH)
    if env_path and os.path.exists(env_path):
        return env_path

    blkio_bench_path = os.path.join(
        build_dir(), "subprojects", "libblkio", "examples", "blkio-bench"
    )
    if os.path.exists(blkio_bench_path):
        return blkio_bench_path

    raise RuntimeError("This test requires blkio-bench example program "
                       "which comes with libblkio")


@pytest.fixture(scope="session")
def vhost_user_test_server() -> str:
    env_path = os.environ.get(TEST_SERVER_BINARY_ENV_PATH)
    if env_path and os.path.exists(env_path):
        return env_path

    server_path = os.path.join(
        build_dir(), "tests", "vhost-user-blk-test-server"
    )
    if os.path.exists(server_path):
        return server_path

    raise RuntimeError("A valid path to the test server must be specified "
                       f"in the {TEST_SERVER_BINARY_ENV_PATH} variable")


@pytest.fixture(scope="session")
def work_dir() -> Generator[str, None, None]:
    work_dir_path = os.path.join(base_dir_abs_path(), WORK_DIR)

    os.makedirs(work_dir_path, exist_ok=True)
    yield work_dir_path
    shutil.rmtree(work_dir_path)


@pytest.fixture(scope="session")
def disk_image(work_dir: str) -> Generator[str, None, None]:
    disk_image_path = os.path.join(work_dir, "disk-image.raw")

    with open(disk_image_path, "wb+") as f:
        f.seek(DISK_IMAGE_SIZE - 1)
        f.write(bytearray(1))

    yield disk_image_path
    os.remove(disk_image_path)


def create_server(
    work_dir: str, disk_image: str, vhost_user_test_server: str,
    pte_flush_threshold: int = 0
) -> Generator[str, None, None]:
    socket_path = os.path.join(work_dir, "server.sock")

    process = subprocess.Popen([
        vhost_user_test_server, "--disk",
        f"socket-path={socket_path},blk-file={disk_image}"
        f",serial=helloworld,pte-flush-threshold={pte_flush_threshold}"
    ])

    retry = 0
    retry_limit = 5

    while True:
        if os.path.exists(socket_path):
            break

        if retry < retry_limit:
            retry += 1
            time.sleep(10)
        else:
            raise RuntimeError("Failed to start test server!")

    yield socket_path

    process.send_signal(signal.SIGINT)
    process.wait(10)


@pytest.fixture(scope="class")
def server_socket(
    work_dir: str, disk_image: str, vhost_user_test_server: str
) -> Generator[str, None, None]:
    yield from create_server(work_dir, disk_image, vhost_user_test_server)


@pytest.fixture(scope="class")
def server_socket_with_pte_flush(
    request: pytest.FixtureRequest, work_dir: str, disk_image: str,
    vhost_user_test_server: str
) -> Generator[str, None, None]:
    yield from create_server(work_dir, disk_image, vhost_user_test_server,
                             request.param)


def pretty_print_blkio_config(param: List[str]) -> str:
    return f"{param[0]}, blocksize={param[1]}"


def check_run_blkio_bench(
    path: str, type: str, blocksize: int, time: int, socket: str,
    threads: int = 1
) -> None:
    subprocess.check_call([
        path, f"--blocksize={blocksize}", f"--runtime={time}",
        f"--readwrite={type}", f"--num-threads={threads}",
        "virtio-blk-vhost-user", f"path={socket}"
    ], timeout=time + 10)


class TestBasic:
    @pytest.mark.parametrize(
        'config',
        [
            ["read", 1024 * 1024],
            ["write", 1024 * 1024],
            ["randread", 4096],
            ["randwrite", 4096],
        ],
        ids=pretty_print_blkio_config
    )
    def test_basic_operations(
        self, server_socket: str, blkio_bench: str, config: Tuple[str, int]
    ) -> None:
        check_run_blkio_bench(blkio_bench, *config, 30, server_socket)


@pytest.mark.parametrize(
    'server_socket_with_pte_flush, time',
    [
        # Flush every 1 byte processed for 5 seconds
        [1, 5],
        # Flush every 50MiB processed for 30 seconds
        [50 * 1024 * 1024, 30],
    ],
    indirect=['server_socket_with_pte_flush']
)
class TestPTEFlush:
    def test_pte_flush(
        self, server_socket_with_pte_flush: str, time: int,
        blkio_bench: str
    ) -> None:
        check_run_blkio_bench(blkio_bench, "randread", 4096, time,
                              server_socket_with_pte_flush)
