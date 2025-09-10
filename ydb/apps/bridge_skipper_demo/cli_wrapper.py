import logging
import random
import subprocess

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional


logger = logging.getLogger(__name__)


def execute_cli_command(
        path_to_cli: str, cmd: List[str], endpoints: List[str], strict_order: bool = False)-> Optional[subprocess.CompletedProcess]:

    random_order_endpoints = list(endpoints)

    if not strict_order:
        random.shuffle(random_order_endpoints)

    for endpoint in random_order_endpoints:
        try:
            full_cmd = [path_to_cli, "-e", f"grpc://{endpoint}:2135"] + cmd
            result = subprocess.run(full_cmd, capture_output=True)
            if result.returncode == 0:
                return result
            logger.debug(f"{cmd} failed for {endpoint} with code {result.returncode}, stdout: {result.stdout}")
        except Exception as e:
            logger.debug(f"CLI command failed for endpoint {endpoint}: {e}")
            continue

    return None


def execute_cli_command_parallel(path_to_cli: str, cmd: List[str], endpoints: List[str]) -> Optional[subprocess.CompletedProcess]:
    """Run the CLI command against majority of provided endpoints concurrently and return the first completed result.

    Other in-flight calls are ignored once the first completes.
    """
    if not endpoints:
        return None

    shuffled = list(endpoints)
    random.shuffle(shuffled)
    # Use only a strict majority of endpoints
    majority_size = (len(shuffled) // 2) + 1 if shuffled else 0
    targets = shuffled[:majority_size]

    if len(targets) <= 1:
        return execute_cli_command(path_to_cli, cmd, targets)

    with ThreadPoolExecutor(max_workers=min(len(targets), 8)) as executor:
        future_map = {executor.submit(execute_cli_command, path_to_cli, cmd, [ep]): ep for ep in targets}
        for future in as_completed(future_map.keys()):
            try:
                res = future.result()
            except Exception:
                res = None
            # Return immediately regardless of success; caller may check returncode
            return res

    return None
