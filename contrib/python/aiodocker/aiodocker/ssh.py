"""SSH connector for aiodocker."""

from __future__ import annotations

import asyncio
import logging
import os
import re
import tempfile
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import aiohttp
from aiohttp.connector import Connection

from .exceptions import DockerError


try:
    import asyncssh
except ImportError:
    asyncssh = None  # type: ignore

# Try to import SSH config parser (preferably paramiko like docker-py)
try:
    from paramiko import SSHConfig
except ImportError:
    SSHConfig = None  # type: ignore

log = logging.getLogger(__name__)

# Constants
DEFAULT_SSH_PORT = 22
DANGEROUS_ENV_VARS = ["LD_LIBRARY_PATH", "SSL_CERT_FILE", "SSL_CERT_DIR", "PYTHONPATH"]

__all__ = ["SSHConnector"]


class SSHConnector(aiohttp.UnixConnector):
    """SSH tunnel connector that forwards Docker socket connections over SSH."""

    def __init__(
        self,
        ssh_url: str,
        strict_host_keys: bool = True,
        **kwargs: Any,
    ) -> None:
        """Initialize SSH connector.

        Args:
            ssh_url: SSH connection URL (ssh://[user@]host[:port]).
                The username is optional and can be inferred from ~/.ssh/config.
            strict_host_keys: Enforce strict host key verification (default: True)
            **kwargs: Additional SSH connection options

        Note:
            This connector uses 'docker system dial-stdio' to connect to the remote
            Docker daemon, which automatically discovers and uses the correct socket
            path on the remote host (works with standard, rootless, and custom setups).
        """
        if asyncssh is None:
            raise DockerError(
                500,
                "asyncssh is required for SSH connections. "
                "Install with: pip install aiodocker[ssh]",
            )

        # Validate and parse SSH URL
        parsed = urlparse(ssh_url)
        if parsed.scheme != "ssh":
            raise DockerError(400, f"Invalid SSH URL scheme: {parsed.scheme}")

        if not parsed.hostname:
            raise DockerError(400, "SSH URL must include hostname")

        self._ssh_host = parsed.hostname
        self._ssh_port = parsed.port or DEFAULT_SSH_PORT
        self._ssh_username = parsed.username
        self._ssh_password = parsed.password
        self._strict_host_keys = strict_host_keys

        # Load SSH config and merge with provided options
        ssh_config = self._load_ssh_config()
        self._ssh_options = {**ssh_config, **kwargs}

        # Validate and enforce host key verification
        self._setup_host_key_verification()

        # Warn about password in URL
        if self._ssh_password:
            log.warning(
                "Password provided in SSH URL. Consider using SSH key authentication "
                "for better security. Passwords may be exposed in logs or memory dumps."
            )

        # Connection state
        self._ssh_conn: asyncssh.SSHClientConnection | None = None
        self._ssh_context: Any | None = None
        self._tunnel_lock = asyncio.Lock()
        self._socket_server: asyncio.Server | None = None
        self._relay_tasks: set[asyncio.Task[None]] = set()

        # Create secure temporary directory (system chooses location and sets permissions)
        self._temp_dir = tempfile.TemporaryDirectory()
        self._local_socket_path = os.path.join(self._temp_dir.name, "docker.sock")

        # Initialize as Unix connector with our local socket
        super().__init__(path=self._local_socket_path)

    def _load_ssh_config(self) -> dict[str, Any]:
        """Load SSH configuration from ~/.ssh/config like docker-py does."""
        if SSHConfig is None:
            log.debug("SSH config parsing not available (paramiko not installed)")
            return {}

        config_options = {}
        ssh_config_path = Path.home() / ".ssh" / "config"

        if ssh_config_path.exists():
            try:
                config = SSHConfig.from_path(ssh_config_path)
                host_config = config.lookup(self._ssh_host)

                # Map SSH config options to asyncssh parameters
                # Only use config port if not specified in URL
                if "port" in host_config and self._ssh_port == DEFAULT_SSH_PORT:
                    self._ssh_port = int(host_config["port"])
                # Only use config user if not specified in URL
                if "user" in host_config and not self._ssh_username:
                    self._ssh_username = host_config["user"]
                # Map file paths directly
                if "identityfile" in host_config:
                    config_options["client_keys"] = host_config["identityfile"]
                if "userknownhostsfile" in host_config:
                    config_options["known_hosts"] = host_config["userknownhostsfile"]

                log.debug("Loaded SSH config for %s", self._ssh_host)

            except Exception:
                log.exception("Failed to parse SSH config")

        return config_options

    def _setup_host_key_verification(self) -> None:
        """Setup host key verification following docker-py security principles."""
        known_hosts = self._ssh_options.get("known_hosts")

        # If no known_hosts specified in config, use default location
        if known_hosts is None:
            default_known_hosts = Path.home() / ".ssh" / "known_hosts"
            if default_known_hosts.exists():
                self._ssh_options["known_hosts"] = str(default_known_hosts)
                known_hosts = str(default_known_hosts)

        if known_hosts is None and self._strict_host_keys:
            # Docker-py equivalent: enforce host key checking
            raise DockerError(
                400,
                "Host key verification is required for security. "
                "Either add the host to ~/.ssh/known_hosts or set strict_host_keys=False. "
                "SECURITY WARNING: Disabling host key verification makes connections "
                "vulnerable to man-in-the-middle attacks.",
            )
        elif known_hosts is None:
            # Allow but warn (similar to docker-py's WarningPolicy)
            log.warning(
                "SECURITY WARNING: Host key verification disabled for %(ssh_host)s. "
                "Connection is vulnerable to man-in-the-middle attacks. "
                "Add host to ~/.ssh/known_hosts or run: ssh-keyscan -H %(ssh_host)s >> ~/.ssh/known_hosts",
                {"ssh_host": self._ssh_host},
            )

    def _sanitize_error_message(self, error: Exception) -> str:
        """Sanitize error messages to prevent credential leakage."""
        message = str(error)

        # Remove password from error messages
        if self._ssh_password:
            message = message.replace(self._ssh_password, "***REDACTED***")

        # Remove password from SSH URLs in error messages
        message = re.sub(
            r"ssh://([^:/@]+):([^@]+)@", r"ssh://\1:***REDACTED***@", message
        )

        return message

    def _clean_environment(self) -> dict[str, str]:
        """Clean environment variables for security like docker-py does."""
        env = os.environ.copy()
        for var in DANGEROUS_ENV_VARS:
            env.pop(var, None)
        return env

    async def _relay_data(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        """Relay data between two streams."""
        try:
            while True:
                data = await reader.read(8192)
                if not data:
                    break
                writer.write(data)
                await writer.drain()
        except (asyncio.CancelledError, ConnectionError, BrokenPipeError):
            pass
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    async def _handle_docker_connection(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        """Handle a Docker API connection by executing dial-stdio and relaying data."""
        process = None
        try:
            if self._ssh_conn is None:
                raise DockerError(500, "SSH connection not established")

            log.debug("Handling new Docker connection via dial-stdio")

            # Execute docker system dial-stdio on remote host
            # This automatically connects to the correct Docker socket
            # Use encoding=None for binary mode (stdin/stdout handle bytes, not strings)
            process = await self._ssh_conn.create_process(
                "docker system dial-stdio", encoding=None
            )  # type: ignore

            # Create relay tasks for bidirectional communication
            send_task = asyncio.create_task(
                self._relay_data(reader, process.stdin)  # type: ignore
            )
            recv_task = asyncio.create_task(
                self._relay_data(process.stdout, writer)  # type: ignore
            )

            # Track tasks for cleanup
            self._relay_tasks.add(send_task)
            self._relay_tasks.add(recv_task)

            # Wait for either direction to complete
            done, pending = await asyncio.wait(
                [send_task, recv_task], return_when=asyncio.FIRST_COMPLETED
            )

            # Cancel remaining task
            for task in pending:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

            # Remove from tracking
            self._relay_tasks.discard(send_task)
            self._relay_tasks.discard(recv_task)

        except Exception as e:
            sanitized_error = self._sanitize_error_message(e)
            log.error("Error in dial-stdio relay: %s", sanitized_error)
        finally:
            # Clean up process
            if process:
                try:
                    process.terminate()
                    await process.wait()
                except Exception:
                    pass

            # Close writer
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    async def _ensure_ssh_tunnel(self) -> None:
        """Ensure SSH connection and local Unix socket server are established."""
        # Use lock to prevent concurrent tunnel creation (docker-py principle)
        async with self._tunnel_lock:
            # Re-check condition after acquiring lock
            if self._ssh_conn is None or self._ssh_conn.is_closed():
                log.debug(
                    "Establishing SSH connection to %s@%s:%s",
                    self._ssh_username,
                    self._ssh_host,
                    self._ssh_port,
                )

                try:
                    # Clean environment like docker-py does
                    clean_env = self._clean_environment()

                    # Use asyncssh context manager properly
                    self._ssh_context = asyncssh.connect(
                        host=self._ssh_host,
                        port=self._ssh_port,
                        username=self._ssh_username,
                        password=self._ssh_password,
                        env=clean_env,
                        **self._ssh_options,
                    )
                    self._ssh_conn = await self._ssh_context.__aenter__()

                    # Create Unix socket server that handles connections via dial-stdio
                    self._socket_server = await asyncio.start_unix_server(
                        self._handle_docker_connection,
                        path=self._local_socket_path,
                    )
                    log.debug(
                        "SSH connection established, Unix socket server listening at %s",
                        self._local_socket_path,
                    )

                    # Clear password from memory after successful connection
                    if self._ssh_password:
                        self._ssh_password = None

                except Exception as e:
                    sanitized_error = self._sanitize_error_message(e)
                    log.error("Failed to establish SSH connection: %s", sanitized_error)

                    # Clean up context if it was created
                    if self._ssh_context:
                        try:
                            await self._ssh_context.__aexit__(
                                type(e), e, e.__traceback__
                            )
                        except Exception:
                            pass
                        self._ssh_context = None
                        self._ssh_conn = None

                    # Wrap in DockerError if not already one
                    if isinstance(e, DockerError):
                        raise
                    raise DockerError(
                        900,
                        f"Cannot connect to Docker via SSH {self._ssh_username}@{self._ssh_host}:{self._ssh_port}: {sanitized_error}",
                    ) from e

    async def connect(
        self, req: aiohttp.ClientRequest, traces: Any, timeout: aiohttp.ClientTimeout
    ) -> Connection:
        """Connect through SSH tunnel."""
        await self._ensure_ssh_tunnel()
        return await super().connect(req, traces, timeout)

    async def close(self) -> None:  # type: ignore[override]
        """Close SSH connection and clean up resources with proper error handling."""
        await super().close()

        # Close socket server
        if self._socket_server:
            try:
                self._socket_server.close()
                await self._socket_server.wait_closed()
            except Exception as e:
                log.warning("Error closing socket server: %s", type(e).__name__)
            finally:
                self._socket_server = None

        # Cancel all relay tasks
        for task in list(self._relay_tasks):
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        self._relay_tasks.clear()

        # Close SSH context manager properly
        if self._ssh_context:
            try:
                await self._ssh_context.__aexit__(None, None, None)
            except Exception as e:
                sanitized_error = self._sanitize_error_message(e)
                log.warning("Error closing SSH connection: %s", sanitized_error)
            finally:
                self._ssh_context = None
                self._ssh_conn = None

        # Clean up temporary directory (removes socket file automatically)
        try:
            self._temp_dir.cleanup()
        except Exception as e:
            # Don't log full path for security
            temp_name = self._temp_dir.name[-8:] if self._temp_dir.name else "unknown"
            log.warning(
                "Failed to clean up temporary directory <temp-%s>: %s",
                temp_name,
                type(e).__name__,
            )

        # Clear any remaining sensitive data
        self._ssh_password = None
