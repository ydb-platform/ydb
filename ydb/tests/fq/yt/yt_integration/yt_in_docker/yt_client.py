"""YT client for integration tests.

Provides a simple interface to interact with YT cluster running in Docker.
The cluster lifecycle is managed by the yatest docker_compose recipe — this
client only connects to the already-running cluster.
"""

import json
import logging
import os
import subprocess
import time
import urllib.error
import urllib.request
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode
import yatest.common


logger = logging.getLogger(__name__)

_DOCKER_COMPOSE_FILE_PATH = "ydb/tests/fq/yt/yt_integration/yt_in_docker/docker-compose.yml"


class YtClient:
    """Client for interacting with YT cluster managed by docker_compose recipe.

    The cluster is started by the recipe before tests and stopped after.
    This client discovers the running container and connects to it.

    Example:
        client = YtClient()
        client.create_table("//tmp/my_table")
        client.write_table("//tmp/my_table", [{"key": "value"}])
        rows = client.read_table("//tmp/my_table")
    """

    def __init__(self, max_attempts: int = 90, sleep_interval: float = 2.0) -> None:
        self._compose_project_name: str = self._get_recipe_project_name()
        self._container_name: str = self._discover_container_name()
        self._proxy_url: str = self._resolve_proxy_url()
        self._rpc_proxy_address: Optional[str] = None  # resolved lazily on first access
        self._wait_for_healthy(max_attempts, sleep_interval)
        self._configure_cluster()

    def __enter__(self) -> "YtClient":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> bool:
        self.stop()
        return False

    def stop(self) -> None:
        # Cluster is managed by the recipe — nothing to stop here
        pass

    @property
    def proxy_url(self) -> str:
        """Return the YT HTTP proxy URL for HTTP API access."""
        return self._proxy_url

    @property
    def rpc_proxy_address(self) -> str:
        """Return the YT RPC proxy address (host:port) for RPC-based tools like qyt_cli.

        Resolved lazily on first access to avoid paying the ``docker compose port``
        overhead when only HTTP API methods are used.
        """
        if self._rpc_proxy_address is None:
            self._rpc_proxy_address = self._resolve_rpc_proxy_address()
        return self._rpc_proxy_address

    def _get_compose_file_abs_path(self) -> str:
        return yatest.common.source_path(_DOCKER_COMPOSE_FILE_PATH)

    @staticmethod
    def _get_recipe_project_name() -> str:
        """Derive the compose project name used by the docker_compose recipe.

        The recipe exposes the resolved compose file path via the
        ``DOCKER_COMPOSE_FILE`` environment variable (set by
        ``library.python.testing.recipe.set_env``).  Docker Compose derives
        the project name from the directory that contains the compose file when
        no ``-p`` flag is given, so we do the same.  Falls back to the
        expected directory name if the variable is not set.
        """
        compose_file = os.environ.get("DOCKER_COMPOSE_FILE", "")
        if compose_file:
            return os.path.basename(os.path.dirname(compose_file))
        return "yt_in_docker"

    def _discover_container_name(self) -> str:
        """Discover the running container name for this project."""
        compose_file = self._get_compose_file_abs_path()
        cmd = [
            "docker", "compose",
            "-f", compose_file,
            "-p", self._compose_project_name,
            "ps", "--format", "{{.Name}}", "--filter", "status=running",
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30, check=False)
        if result.returncode != 0:
            raise RuntimeError(f"Failed to discover container: {result.stderr}")
        containers = [c.strip() for c in result.stdout.strip().split("\n") if c.strip()]
        if not containers:
            raise RuntimeError("No running containers found for YT cluster")
        return containers[0]

    def _resolve_rpc_proxy_address(self) -> str:
        compose_file = self._get_compose_file_abs_path()
        cmd = [
            "docker", "compose",
            "-f", compose_file,
            "-p", self._compose_project_name,
            "port", "yt", "8443",
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30, check=False)
        if result.returncode != 0:
            raise RuntimeError(f"Failed to get YT RPC proxy port: {result.stderr}")
        output = result.stdout.strip()
        if not output:
            raise RuntimeError("docker compose port (8443) returned empty output")
        port = int(output.rsplit(":", 1)[1])
        return f"localhost:{port}"

    def _resolve_proxy_url(self) -> str:
        compose_file = self._get_compose_file_abs_path()
        cmd = [
            "docker", "compose",
            "-f", compose_file,
            "-p", self._compose_project_name,
            "port", "yt", "80",
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30, check=False)
        if result.returncode != 0:
            raise RuntimeError(f"Failed to get YT port: {result.stderr}")

        output = result.stdout.strip()
        if not output:
            raise RuntimeError("docker compose port returned empty output")

        port = int(output.rsplit(":", 1)[1])
        return f"http://localhost:{port}"

    def _configure_cluster(self) -> None:
        """Apply one-time configuration to the cluster after it becomes healthy.

        1. Waits until the default tablet cell bundle is healthy so that
           dynamic tables can be mounted.  The container is started with
           --wait-tablet-cell-initialization so a cell already exists; this
           loop just waits for it to finish initialisation.
        2. Raises the tablet count limit for the //tmp account.
        """
        # Wait for the default tablet bundle to become healthy (up to 60 s).
        logger.info("Waiting for tablet cell bundle to become healthy")
        for attempt in range(60):
            result = self._run_yt_cli(
                ["get", "//sys/tablet_cell_bundles/default/@health"],
                check=False, timeout=10,
            )
            if "good" in result.stdout:
                logger.info("Tablet bundle healthy after %d attempts", attempt + 1)
                break
            time.sleep(1)
        else:
            logger.warning("Tablet cell bundle did not become healthy in time")

        # Increase tablet count limit for the tmp account.
        try:
            self._run_yt_cli(
                ["set", "//sys/accounts/tmp/@resource_limits/tablet_count", "1000"],
                check=True, timeout=30,
            )
        except Exception as e:
            logger.warning("Failed to configure cluster (tablet count limit): %s", e)

    def _wait_for_healthy(self, max_attempts: int, sleep_interval: float) -> None:
        logger.info("Waiting for YT cluster to become healthy (%d attempts)", max_attempts)
        for attempt in range(max_attempts):
            try:
                result = self.list("//tmp")
                if "value" in result:
                    logger.info("YT cluster is healthy after %d attempts", attempt + 1)
                    return
            except Exception as e:
                logger.debug("YT health check attempt %d failed: %s", attempt, e)
            if attempt < max_attempts - 1:
                time.sleep(sleep_interval)
        raise RuntimeError("YT cluster did not become healthy in time")

    def _api_call(
        self,
        method: str,
        params: Optional[Dict[str, str]] = None,
        data: Optional[str] = None,
        timeout: int = 60,
        max_retries: int = 2,
        http_method: str = "GET",
    ) -> Dict[str, Any]:
        url = f"{self._proxy_url}/api/v4/{method}"
        if params:
            url += f"?{urlencode(params)}"

        last_error: Optional[BaseException] = None
        for attempt in range(max_retries + 1):
            req = urllib.request.Request(url, method=http_method)
            if data is not None:
                req.data = data.encode()
                req.add_header("Content-Type", "application/json")

            try:
                with urllib.request.urlopen(req, timeout=timeout) as resp:
                    return json.loads(resp.read().decode())
            except urllib.error.HTTPError as e:
                error_body = e.read().decode()[:1000]
                last_error = RuntimeError(f"YT API error: {e.code} {error_body}")
                # Retry on 5xx server errors
                if e.code >= 500 and attempt < max_retries:
                    time.sleep(0.5 * (attempt + 1))
                    continue
                raise last_error
            except (urllib.error.URLError, OSError) as e:
                last_error = RuntimeError(f"YT API connection error: {e}")
                if attempt < max_retries:
                    time.sleep(0.5 * (attempt + 1))
                    continue
                raise last_error

    def _run_yt_cli(
        self,
        args: List[str],
        check: bool = True,
        timeout: int = 60,
        input_data: Optional[str] = None,
        max_retries: int = 1,
    ) -> subprocess.CompletedProcess[str]:
        """Run yt CLI command inside the Docker container via docker exec."""
        exec_flags = ["-i"] if input_data is not None else []
        cmd = ["docker", "exec"] + exec_flags + [self._container_name, "yt", "--proxy", "localhost:80"] + args

        last_error: Optional[BaseException] = None
        for attempt in range(max_retries + 1):
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, check=False, input=input_data)
            if not check or result.returncode == 0:
                return result
            last_error = RuntimeError(f"yt command failed: {result.stderr[:500]}")
            if attempt < max_retries:
                time.sleep(0.5 * (attempt + 1))
                continue
            raise last_error

    @staticmethod
    def _parse_json_lines(output: str) -> List[Dict[str, Any]]:
        """Parse newline-delimited JSON output (one JSON object per line) into a list of dicts."""
        rows: List[Dict[str, Any]] = []
        for line in output.strip().split("\n"):
            line = line.strip()
            if line:
                try:
                    rows.append(json.loads(line))
                except json.JSONDecodeError as e:
                    raise RuntimeError(f"Failed to parse JSON line: {line!r} — {e}") from None
        return rows

    def list(self, path: str) -> Dict[str, Any]:
        return self._api_call("list", {"path": path})

    def create_table(self, path: str, columns: Dict[str, str]) -> None:
        """Create a static table at the given path with provided schema.

        ``columns`` maps column name to YT type string, e.g. ``{"key": "string", "value": "int64"}``.
        """
        attributes = {
            "schema": [
                {"name": name, "type": col_type}
                for name, col_type in columns.items()
            ]
        }
        self._api_call(
            "create",
            params={"path": path, "type": "table"},
            data=json.dumps({"attributes": attributes}),
            http_method="POST",
        )

    def exists(self, path: str) -> bool:
        """Check if a node exists at the given path.

        Uses ``yt get <path>`` which exits with code 0 when the node exists
        and non-zero when it does not.  This avoids fragile string-matching
        on either HTTP error messages or YSON boolean output.
        """
        result = self._run_yt_cli(["get", path], check=False, timeout=30)
        return result.returncode == 0

    def remove(self, path: str, recursive: bool = True) -> None:
        if not self.exists(path):
            return
        # Attempt to unmount dynamic tables before removal; errors are suppressed
        # for static tables that don't support unmount.
        try:
            self._run_yt_cli(
                ["unmount-table", "--sync", path],
                check=True, timeout=60,
            )
        except Exception:
            pass
        params = {"path": path}
        if recursive:
            params["recursive"] = "true"
        self._api_call("remove", params=params, http_method="POST")

    def create_queue(self, path: str, data_column: str = "data", timeout: int = 60) -> None:
        """Create an ordered dynamic table at the given path and mount it as a queue.

        The schema includes the user-defined data column plus the two system
        columns required by the YT Queue Agent (``$timestamp`` and
        ``$cumulative_data_weight``).  The table is mounted synchronously so
        it is ready for queue operations immediately after this call returns.
        """
        attrs = json.dumps({
            "dynamic": True,
            "schema": [
                {"name": data_column, "type": "string"},
                {"name": "$timestamp", "type": "uint64"},
                {"name": "$cumulative_data_weight", "type": "int64"},
            ],
        })
        self._run_yt_cli(
            ["create", "table", path, "--attributes-format", "json", "--attributes", attrs],
            check=True, timeout=timeout,
        )
        self._run_yt_cli(
            ["mount-table", path, "--sync"],
            check=True, timeout=timeout,
        )

    def insert_rows(self, path: str, rows: List[Dict[str, Any]], timeout: int = 120) -> None:
        """Insert rows into a mounted dynamic table via JSON newline-delimited format."""
        if not rows:
            return
        data = "\n".join(json.dumps(row) for row in rows) + "\n"
        self._run_yt_cli(
            ["insert-rows", "--format=json", path],
            input_data=data, check=True, timeout=timeout,
        )

    def write_table(self, path: str, rows: List[Dict[str, Any]], timeout: int = 120) -> None:
        if not rows:
            return
        data = "\n".join(json.dumps(row) for row in rows) + "\n"
        self._run_yt_cli(
            ["write-table", "--format=json", path],
            input_data=data, check=True, timeout=timeout,
        )

    def read_table(self, path: str) -> List[Dict[str, Any]]:
        result = self._run_yt_cli(
            ["read-table", "--format=json", path],
            timeout=60,
        )
        return self._parse_json_lines(result.stdout)

    def get_attribute(self, path: str) -> Any:
        """Read a single attribute value from the given path (e.g. @tablet_count)."""
        result = self._api_call("get", params={"path": path})
        return result.get("value")

    def get_attribute_cli(self, path: str, timeout: int = 60) -> str:
        """Read a single attribute value using yt CLI (returns raw stdout)."""
        result = self._run_yt_cli(
            ["get", path],
            check=True, timeout=timeout,
        )
        return result.stdout.strip()

    def mount_table(self, path: str, sync: bool = True, timeout: int = 60) -> None:
        """Mount a dynamic table."""
        args = ["mount-table", path]
        if sync:
            args.append("--sync")
        self._run_yt_cli(args, check=True, timeout=timeout)

    def set_attribute(
        self,
        path: str,
        value: Any,
        as_json: bool = False,
        timeout: int = 60,
    ) -> None:
        """Set an attribute value at the given path.

        If *as_json* is True, *value* will be serialized as JSON and passed
        with --attributes-format=json to the yt CLI.
        """
        if as_json:
            self._run_yt_cli(
                ["set", "--attributes-format", "json", path, json.dumps(value)],
                check=True, timeout=timeout,
            )
        else:
            self._run_yt_cli(
                ["set", path, str(value)],
                check=True, timeout=timeout,
            )

    def create_node(self, path: str, type_: str, timeout: int = 60) -> None:
        """Create a node of the given type at the specified path."""
        self._run_yt_cli(
            ["create", type_, path],
            check=True, timeout=timeout,
        )

    def download(self, path: str, timeout: int = 60) -> str:
        """Download content from a file node."""
        result = self._run_yt_cli(
            ["download", path],
            check=True, timeout=timeout,
        )
        return result.stdout

    # — Queue API methods —

    def create_queue_consumer(self, path: str, timeout: int = 60) -> None:
        """Create a queue consumer at the given path."""
        self._run_yt_cli(
            ["create", "queue_consumer", path],
            check=True, timeout=timeout,
        )

    def register_consumer(
        self,
        queue_path: str,
        consumer_path: str,
        vital: bool = False,
        timeout: int = 60,
    ) -> None:
        """Register a YT queue consumer linking consumer_path to queue_path."""
        vital_flag = "--vital" if vital else "--non-vital"
        self._run_yt_cli(
            ["register-queue-consumer", queue_path, consumer_path, vital_flag],
            check=True, timeout=timeout,
        )

    def list_queue_consumer_registrations(
        self,
        queue_path: str,
        timeout: int = 60,
    ) -> subprocess.CompletedProcess[str]:
        """List queue consumer registrations for a queue."""
        return self._run_yt_cli(
            ["list-queue-consumer-registrations", "--queue-path", queue_path],
            check=True, timeout=timeout,
        )

    def get_queue_status(self, queue_path: str, timeout: int = 60) -> str:
        """Get queue status from //queue_agent."""
        return self.get_attribute_cli(f"{queue_path}/@queue_status", timeout)

    def get_consumer_status(self, consumer_path: str, timeout: int = 60) -> str:
        """Get consumer status from //queue_agent."""
        return self.get_attribute_cli(f"{consumer_path}/@queue_consumer_status", timeout)

    def pull_queue_consumer(
        self,
        consumer_path: str,
        queue_path: str,
        partition_index: int = 0,
        offset: int = 0,
        max_row_count: int = 5,
        timeout: int = 60,
    ) -> List[Dict[str, Any]]:
        """Pull data from a queue via consumer.

        Returns a list of parsed row dicts (one JSON object per line in output).
        """
        result = self._run_yt_cli([
            "pull-queue-consumer", consumer_path, queue_path,
            "--partition-index", str(partition_index),
            "--offset", str(offset),
            "--max-row-count", str(max_row_count),
            "--format", "json",
        ], check=True, timeout=timeout)
        return self._parse_json_lines(result.stdout)

    def advance_queue_consumer(
        self,
        consumer_path: str,
        queue_path: str,
        new_offset: int,
        partition_index: int = 0,
        old_offset: int = 0,
        timeout: int = 60,
    ) -> None:
        """Advance a queue consumer offset.

        ``new_offset`` is required — there is no meaningful default value.
        ``old_offset`` is the current consumer offset (used for optimistic
        concurrency control by the Queue Agent); defaults to 0.
        """
        self._run_yt_cli([
            "advance-queue-consumer", consumer_path, queue_path,
            "--partition-index", str(partition_index),
            "--old-offset", str(old_offset),
            "--new-offset", str(new_offset),
        ], check=True, timeout=timeout)

    def create_queue_producer(self, path: str, timeout: int = 60) -> None:
        """Create a queue producer at the given path."""
        self._run_yt_cli(
            ["create", "queue_producer", path],
            check=True, timeout=timeout,
        )

    def create_queue_producer_session(
        self,
        queue_path: str,
        producer_path: str,
        session_id: str,
        timeout: int = 60,
    ) -> None:
        """Create a queue producer session."""
        self._run_yt_cli([
            "create-queue-producer-session",
            "--queue-path", queue_path,
            "--producer-path", producer_path,
            "--session-id", session_id,
        ], check=True, timeout=timeout)

    def push_queue_producer(
        self,
        producer_path: str,
        queue_path: str,
        session_id: str,
        epoch: int = 0,
        rows: Optional[List[Dict[str, Any]]] = None,
        input_data: Optional[str] = None,
        input_format: str = "json",
        timeout: int = 60,
    ) -> None:
        """Push rows via queue producer.

        If *rows* is provided, it will be serialized as newline-delimited JSON.
        Otherwise *input_data* is used as-is (raw YSON or other format).
        """
        if rows is not None:
            input_data = "\n".join(json.dumps(row) for row in rows) + "\n"
        self._run_yt_cli([
            "push-queue-producer", producer_path, queue_path,
            "--session-id", session_id,
            "--epoch", str(epoch),
            "--input-format", input_format,
        ], input_data=input_data, check=True, timeout=timeout)

    def pull_queue(
        self,
        queue_path: str,
        offset: int = 0,
        partition_index: int = 0,
        timeout: int = 60,
    ) -> List[Dict[str, Any]]:
        """Pull rows directly from a queue.

        Returns a list of parsed row dicts (one JSON object per line in output).
        """
        result = self._run_yt_cli([
            "pull-queue", queue_path,
            "--offset", str(offset),
            "--partition-index", str(partition_index),
            "--format", "json",
        ], check=True, timeout=timeout)
        return self._parse_json_lines(result.stdout)
