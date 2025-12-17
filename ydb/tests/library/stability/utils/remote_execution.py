"""
Utilities for executing commands on remote hosts and localhost.

Provides classes and functions for:
- Remote command execution via SSH
- Local command execution
- File transfers
- Host verification
"""

import os
import subprocess
import logging
import yatest.common
import shutil
import allure
from time import time
from typing import Union, Optional, Tuple, List, Dict, Any, NamedTuple

LOGGER = logging.getLogger(__name__)


class ExecutionResult(NamedTuple):

    stdout: str
    stderr: str
    is_timeout: bool
    exit_code: Optional[int] = None


class RemoteExecutor:
    """Class for executing commands on remote hosts and localhost.

    Handles:
    - Command execution with timeout
    - Error handling and logging
    - Output processing
    - Local vs remote execution detection
    """

    @staticmethod
    def get_ssh_options() -> List[str]:
        """
        Returns common SSH options including ControlMaster configuration.
        """
        # Use a fixed socket path depending on user, host and port.
        # %r - remote user name
        # %h - remote host name
        # %p - remote port
        # os.getuid() is added for uniqueness of the local user running the test.
        return [
            "-o", "StrictHostKeyChecking=no",
            "-o", "UserKnownHostsFile=/dev/null",
            "-o", "ControlMaster=auto",
            "-o", "ControlPersist=60s",
            "-o", f"ControlPath=/tmp/ssh-{os.getuid()}-%r@%h:%p"
        ]

    @staticmethod
    def _safe_decode(data) -> str:
        """
        Safely decodes data into a string.

        Args:
            data: Input data to decode (str, bytes or None)

        Returns:
            str: Decoded string (empty string if input is None)
        """
        if data is None:
            return ""
        if isinstance(data, bytes):
            return data.decode('utf-8', errors='replace')
        return str(data)

    @staticmethod
    def _filter_ssh_warnings(stderr: str) -> str:
        """
        Filters SSH warnings from stderr output.

        Args:
            stderr: stderr output string to filter

        Returns:
            str: Filtered stderr without SSH warnings
        """
        if not stderr:
            return stderr

        # Filter lines starting with "Warning: Permanently added"
        filtered_lines = []
        for line in stderr.splitlines():
            if (
                not line.startswith('Warning: Permanently added') and not line.startswith('(!) New version of YDB CL')
            ):
                filtered_lines.append(line)

        return '\n'.join(filtered_lines)

    @staticmethod
    def _is_localhost(hostname: str) -> bool:
        """
        Checks if host is localhost.

        Args:
            hostname: Hostname to check

        Returns:
            bool: True if host is localhost, False otherwise
        """
        if not hostname:
            return False

        # Import socket here to avoid circular imports
        import socket

        # Obvious localhost cases
        localhost_names = {
            'localhost',
            '127.0.0.1',
            '::1',
            socket.gethostname(),
            socket.getfqdn(),
        }

        if hostname in localhost_names:
            return True

        try:
            # Get host IP address
            host_ip = socket.gethostbyname(hostname)

            # Get local IP addresses
            local_ips = set()

            # Add localhost addresses
            local_ips.update(['127.0.0.1', '::1'])

            # Get IP addresses of all network interfaces
            hostname_local = socket.gethostname()
            try:
                local_ips.add(socket.gethostbyname(hostname_local))
            except socket.gaierror:
                pass

            # Check if host IP matches local IPs
            return host_ip in local_ips

        except (socket.gaierror, socket.herror):
            # If we can't resolve hostname, assume it's not localhost
            return False

    @classmethod
    def execute_command(
        cls, host: str, cmd: Union[str, list], raise_on_error: bool = True,
        timeout: Optional[float] = 10, raise_on_timeout: bool = True
    ) -> ExecutionResult:
        """
        Executes a command on a host via SSH or locally

        Args:
            host: hostname to execute command on
            cmd: command to execute (string or list)
            raise_on_error: whether to raise exception on error
            timeout: command execution timeout in seconds
            raise_on_timeout: whether to raise exception on timeout (default True)

        Returns:
            ExecutionResult: результат выполнения команды
        """

        def _handle_timeout_error(
            e: yatest.common.ExecutionTimeoutError,
            full_cmd: Union[str, list],
            is_local: bool
        ) -> ExecutionResult:
            """Handles command timeout errors."""
            cmd_type = "Local" if is_local else "SSH"
            timeout_info = f"{cmd_type} command timed out after {timeout} seconds on {host}"
            stdout = ""
            stderr = ""

            # Extract information from execution_result
            if hasattr(e, 'execution_result') and e.execution_result:
                execution_obj = e.execution_result
                if hasattr(execution_obj, 'std_out') and execution_obj.std_out:
                    stdout = cls._safe_decode(execution_obj.std_out)
                if hasattr(execution_obj, 'std_err') and execution_obj.std_err:
                    stderr = cls._safe_decode(execution_obj.std_err)
                    if not is_local:
                        stderr = cls._filter_ssh_warnings(stderr)
                if hasattr(execution_obj, 'command'):
                    timeout_info += f"\nCommand: {execution_obj.command}"

            # Logging
            if raise_on_timeout:
                LOGGER.error(f"{cmd_type} command timed out after {timeout} seconds on {host}")
                LOGGER.error(f"Full command: {full_cmd}")
                LOGGER.error(f"Original command: {cmd}")
            else:
                LOGGER.warning(f"{cmd_type} command timed out after {timeout} seconds on {host}")
                LOGGER.info(f"Full command: {full_cmd}")
                LOGGER.info(f"Original command: {cmd}")

            if stdout or stderr:
                LOGGER.info(f"Partial stdout before timeout:\n{stdout}")
                LOGGER.info(f"Partial stderr before timeout:\n{stderr}")
            else:
                LOGGER.debug("No partial output available")

            if raise_on_timeout:
                raise subprocess.TimeoutExpired(full_cmd, timeout, output=stdout, stderr=stderr)

            return ExecutionResult(
                stdout=f"{timeout_info}\n{stdout}" if stdout else timeout_info,
                stderr=stderr,
                is_timeout=True
            )

        def _handle_execution_error(
            e: yatest.common.ExecutionError,
            full_cmd: Union[str, list],
            is_local: bool
        ) -> ExecutionResult:
            """Handles command execution errors."""
            cmd_type = "Local" if is_local else "SSH"
            stdout = ""
            stderr = ""
            exit_code = 1

            # Extract information from execution_result
            if hasattr(e, 'execution_result') and e.execution_result:
                execution_obj = e.execution_result
                if hasattr(execution_obj, 'std_out') and execution_obj.std_out:
                    stdout = cls._safe_decode(execution_obj.std_out)
                if hasattr(execution_obj, 'std_err') and execution_obj.std_err:
                    stderr = cls._safe_decode(execution_obj.std_err)
                if hasattr(execution_obj, 'exit_code'):
                    exit_code = execution_obj.exit_code
                elif hasattr(execution_obj, 'returncode'):
                    exit_code = execution_obj.returncode

            # Filter SSH warnings for remote commands
            if not is_local:
                stderr = cls._filter_ssh_warnings(stderr)

            if raise_on_error:
                # Log detailed error information
                LOGGER.error(f"{cmd_type} command failed with exit code {exit_code} on {host}")
                LOGGER.error(f"Full command: {full_cmd}")
                LOGGER.error(f"Original command: {cmd}")
                if stdout:
                    LOGGER.error(f"Command stdout:\n{stdout}")
                if stderr:
                    LOGGER.error(f"Command stderr:\n{stderr}")

                raise subprocess.CalledProcessError(exit_code, full_cmd, stdout, stderr)

            LOGGER.error(f"Error executing {cmd_type.lower()} command on {host}: {e}")
            if stdout:
                LOGGER.error(f"Command stdout:\n{stdout}")
            if stderr:
                LOGGER.error(f"Command stderr:\n{stderr}")

            return ExecutionResult(
                stdout=stdout,
                stderr=stderr,
                is_timeout=False,
                exit_code=exit_code
            )

        def _execute_local_command(cmd: Union[str, list]) -> ExecutionResult:
            """Executes command locally."""
            LOGGER.info(f"Detected localhost ({host}), executing command locally: {cmd}")

            full_cmd = cmd
            try:
                execution = yatest.common.execute(
                    full_cmd,
                    wait=True,
                    check_exit_code=False,
                    timeout=timeout,
                    shell=isinstance(cmd, str)
                )

                stdout = (cls._safe_decode(execution.std_out)
                          if hasattr(execution, 'std_out') and execution.std_out else "")
                stderr = (cls._safe_decode(execution.std_err)
                          if hasattr(execution, 'std_err') and execution.std_err else "")

                exit_code = getattr(execution, 'exit_code', getattr(execution, 'returncode', 0))
                # If command failed but stderr is empty, add synthetic message
                if exit_code != 0 and not stderr.strip():
                    stderr = f"Command failed with exit code {exit_code}, but stderr is empty"
                    LOGGER.warning(f"Local command failed with exit code {exit_code} but produced no stderr output on {host}")
                if exit_code != 0 and raise_on_error:
                    raise subprocess.CalledProcessError(exit_code, full_cmd, stdout, stderr)

                return ExecutionResult(
                    stdout=stdout,
                    stderr=stderr,
                    is_timeout=False,
                    exit_code=exit_code
                )

            except yatest.common.ExecutionTimeoutError as e:
                return _handle_timeout_error(e, full_cmd, is_local=True)
            except yatest.common.ExecutionError as e:
                return _handle_execution_error(e, full_cmd, is_local=True)
            except Exception as e:
                if raise_on_error:
                    raise
                LOGGER.error(f"Unexpected error executing local command on {host}: {e}")
                return ExecutionResult(
                    stdout="",
                    stderr="",
                    is_timeout=False,
                    exit_code=None
                )

        def _execute_ssh_command(cmd: Union[str, list]) -> ExecutionResult:
            """Executes command via SSH."""
            LOGGER.info(f"Executing SSH command on {host}: {cmd}")

            ssh_cmd = ['ssh'] + cls.get_ssh_options()

            # Add SSH user and key
            ssh_user = os.getenv('SSH_USER')
            if ssh_user is not None:
                ssh_cmd += ['-l', ssh_user]
            ssh_key_file = os.getenv('SSH_KEY_FILE')
            if ssh_key_file is not None:
                ssh_cmd += ['-i', ssh_key_file]

            if isinstance(cmd, list):
                full_cmd = ssh_cmd + [host] + cmd
            else:
                full_cmd = ssh_cmd + [host, cmd]

            try:
                execution = yatest.common.execute(
                    full_cmd,
                    wait=True,
                    check_exit_code=False,
                    timeout=timeout
                )

                stdout = (cls._safe_decode(execution.std_out)
                          if hasattr(execution, 'std_out') and execution.std_out else "")
                stderr = (cls._safe_decode(execution.std_err)
                          if hasattr(execution, 'std_err') and execution.std_err else "")

                stderr = cls._filter_ssh_warnings(stderr)

                exit_code = getattr(execution, 'exit_code', getattr(execution, 'returncode', 0))
                # If command failed but stderr is empty, add synthetic message
                if exit_code != 0 and not stderr.strip():
                    stderr = f"Command failed with exit code {exit_code}, but stderr is empty"
                    LOGGER.warning(f"SSH command failed with exit code {exit_code} but produced no stderr output on {host}")
                if exit_code != 0 and raise_on_error:
                    raise subprocess.CalledProcessError(exit_code, full_cmd, stdout, stderr)

                return ExecutionResult(
                    stdout=stdout,
                    stderr=stderr,
                    is_timeout=False,
                    exit_code=exit_code
                )

            except yatest.common.ExecutionTimeoutError as e:
                return _handle_timeout_error(e, full_cmd, is_local=False)
            except yatest.common.ExecutionError as e:
                return _handle_execution_error(e, full_cmd, is_local=False)
            except Exception as e:
                if raise_on_error:
                    raise
                LOGGER.error(f"Unexpected error executing SSH command on {host}: {e}")
                return ExecutionResult(
                    stdout="",
                    stderr="",
                    is_timeout=False,
                    exit_code=None
                )

        # Main logic: choose local or SSH execution
        if cls._is_localhost(host):
            return _execute_local_command(cmd)
        else:
            return _execute_ssh_command(cmd)


# Convenience functions for direct use
def execute_command(
    host: str, cmd: Union[str, list], raise_on_error: bool = True,
    timeout: Optional[float] = 10, raise_on_timeout: bool = True
) -> ExecutionResult:
    """
    Convenience function for executing a command on a host.

    Args:
        host: Target hostname
        cmd: Command to execute (str or list)
        raise_on_error: Whether to raise exception on error
        timeout: Command timeout in seconds
        raise_on_timeout: Whether to raise exception on timeout

    Returns:
        ExecutionResult: Command execution results including:
            - stdout: str
            - stderr: str
            - is_timeout: bool
            - exit_code: Optional[int]
    """
    return RemoteExecutor.execute_command(host, cmd, raise_on_error, timeout, raise_on_timeout)


def execute_command_legacy(
    host: str, cmd: Union[str, list], raise_on_error: bool = True,
    timeout: Optional[float] = None, raise_on_timeout: bool = True
) -> Tuple[str, str]:
    """
    Backward compatibility function for old API.

    Args:
        host: Target hostname
        cmd: Command to execute (str or list)
        raise_on_error: Whether to raise exception on error
        timeout: Command timeout in seconds
        raise_on_timeout: Whether to raise exception on timeout

    Returns:
        Tuple[str, str]: (stdout, stderr) command output
    """
    result = RemoteExecutor.execute_command(host, cmd, raise_on_error, timeout, raise_on_timeout)
    return result.stdout, result.stderr


def is_localhost(hostname: str) -> bool:
    """
    Convenience function to check if host is localhost.

    Args:
        hostname: Hostname to check

    Returns:
        bool: True if host is localhost, False otherwise
    """
    return RemoteExecutor._is_localhost(hostname)


def ensure_directory_with_permissions(host: str, path: str, raise_on_error: bool = True) -> bool:
    """
    Ensures directory exists with 777 permissions.

    Args:
        host: Target hostname
        path: Directory path to create
        raise_on_error: Whether to raise exception on failure

    Returns:
        bool: True if directory was created/set, False on error
    """
    try:
        created_without_sudo = execute_command(host, f"mkdir -p {path} && chmod 777 {path}", raise_on_error=False, timeout=10).exit_code == 0
        if created_without_sudo:
            return True
        created_with_sudo = execute_command(host, f"sudo mkdir -p {path} && sudo chmod 777 {path}", raise_on_error=raise_on_error, timeout=10).exit_code == 0
        return created_with_sudo
    except Exception as e:
        if raise_on_error:
            raise RuntimeError(f"Failed to ensure directory {path}: {e}") from e
        return False


def copy_file(local_path: str, host: str, remote_path: str, raise_on_error: bool = True) -> str:
    """
    Copies file to host via SCP or locally.

    Args:
        local_path: Local file path
        host: Target hostname
        remote_path: Destination path on host
        raise_on_error: Whether to raise exception on error

    Returns:
        str: Copy result message or None on error
    """
    if not os.path.exists(local_path):
        error_msg = f"Local file does not exist: {local_path}"
        if raise_on_error:
            raise FileNotFoundError(error_msg)
        return None

    try:
        if is_localhost(host):
            return _copy_file_unified(local_path, host, remote_path, is_local=True)
        else:
            return _copy_file_unified(local_path, host, remote_path, is_local=False)
    except Exception as e:
        if raise_on_error:
            raise
        LOGGER.error(f"Failed to copy {local_path} to {host}:{remote_path}: {e}")
        return None


def _copy_file_unified(local_path: str, host: str, remote_path: str, is_local: bool) -> str:
    """
    Unified file copy logic for local and remote hosts.

    Args:
        local_path: Source file path
        host: Target hostname
        remote_path: Destination path
        is_local: Whether host is localhost

    Returns:
        str: Copy result message
    """
    # Create target directory
    remote_dir = os.path.dirname(remote_path)
    if remote_dir and remote_dir != '/':
        ensure_directory_with_permissions(host, remote_dir)

    if is_local:
        # Local copying
        try:
            shutil.copy2(local_path, remote_path)
            return f"Local copy successful: {remote_path}"
        except PermissionError:
            # Use temporary file and sudo
            import tempfile
            with tempfile.NamedTemporaryFile(delete=False, prefix="deploy_") as tmp_file:
                temp_path = tmp_file.name
            shutil.copy2(local_path, temp_path)
            execute_command("localhost", f"sudo mv {temp_path} {remote_path}", timeout=30)
            return f"Local copy with sudo successful: {remote_path}"
    else:
        # Remote copying via SCP + temporary file
        timestamp = int(time())
        filename = os.path.basename(local_path)
        tmp_path = f"/tmp/{filename}.{timestamp}.tmp"

        # SCP to /tmp
        scp_cmd = ['scp'] + RemoteExecutor.get_ssh_options()

        ssh_user = os.getenv('SSH_USER')
        scp_host = f"{ssh_user}@{host}" if ssh_user else host

        ssh_key_file = os.getenv('SSH_KEY_FILE')
        if ssh_key_file:
            scp_cmd += ['-i', ssh_key_file]

        scp_cmd += [local_path, f"{scp_host}:{tmp_path}"]

        yatest.common.execute(scp_cmd, wait=True, check_exit_code=True)

        # Move from /tmp to target location
        if execute_command(host, f"mv {tmp_path} {remote_path}", raise_on_error=False, timeout=30).exit_code != 0:
            execute_command(host, f"sudo mv {tmp_path} {remote_path}", raise_on_error=True, timeout=30)
        return f"SCP copy successful: {remote_path}"


def deploy_binary(local_path: str, host: str, target_dir: str, make_executable: bool = True) -> dict:
    """
    Deploys binary file to host.

    Args:
        local_path: Local binary file path
        host: Target hostname
        target_dir: Destination directory on host
        make_executable: Whether to set executable permissions

    Returns:
        dict: Deployment result with keys:
            - name: str - Binary filename
            - path: str - Full destination path
            - success: bool - Whether deployment succeeded
            - output: str - Success message or error details
    """
    binary_name = os.path.basename(local_path)
    target_path = os.path.join(target_dir, binary_name)

    try:
        # Create directory
        ensure_directory_with_permissions(host, target_dir, raise_on_error=True)

        # Copy file
        copy_result = copy_file(local_path, host, target_path)
        if copy_result is None:
            raise Exception("File copy failed")

        # Make executable
        if make_executable:
            if execute_command(host, f"chmod +x {target_path}", raise_on_error=False, timeout=10).exit_code != 0:
                execute_command(host, f"sudo chmod +x {target_path}", raise_on_error=True, timeout=10)

        return {
            'name': binary_name,
            'path': target_path,
            'success': True,
            'output': f'Deployed {binary_name} to {target_path}'
        }
    except Exception as e:
        return {
            'name': binary_name,
            'path': target_path,
            'success': False,
            'error': str(e)
        }


@allure.step('Deploy binaries to hosts')
def deploy_binaries_to_hosts(
    binary_files: List[str],
    hosts: List[str],
    target_dir: str = '/tmp/stress_binaries/'
) -> Dict[str, Dict[str, Any]]:
    """
    Deploys binary files to specified hosts.

    Args:
        binary_files: List of binary file paths
        hosts: List of target hostnames
        target_dir: Destination directory on hosts

    Returns:
        Dict[str, Dict[str, Any]]: Deployment results by host and binary
    """
    results = {}

    for host in set(hosts):  # Automatic duplicate removal
        host_results = {}

        # Create directory on host once
        ensure_directory_with_permissions(host, target_dir, raise_on_error=False)

        # Copy each binary file
        for binary_file in binary_files:
            result = deploy_binary(binary_file, host, target_dir)
            host_results[os.path.basename(binary_file)] = result
            # Log only critical events
            if not result['success']:
                LOGGER.error(f"Failed to deploy {result['name']} to {host}: {result.get('error', 'Unknown error')}")

        results[host] = host_results

    return results


def fix_binaries_directory_permissions(hosts: List[str], target_dir: str = '/tmp/stress_binaries/') -> Dict[str, bool]:
    """
    Fixes directory permissions for binaries on specified hosts.

    Args:
        hosts: List of target hostnames
        target_dir: Directory path to fix permissions for

    Returns:
        Dict[str, bool]: Permission fix results by host
    """
    results = {}

    for host in hosts:
        try:
            success = ensure_directory_with_permissions(host, target_dir, raise_on_error=False)
            results[host] = success
            if not success:
                LOGGER.error(f"Failed to fix permissions for {target_dir} on {host}")
        except Exception as e:
            LOGGER.error(f"Error fixing permissions on {host}: {e}")
            results[host] = False

    return results


def execute_ssh(host: str, cmd: str):
    local = is_localhost(host)
    if local:
        ssh_cmd = cmd
    else:
        ssh_cmd = ['ssh'] + RemoteExecutor.get_ssh_options()
        ssh_user = os.getenv('SSH_USER')
        if ssh_user is not None:
            ssh_cmd += ['-l', ssh_user]
        ssh_key_file = os.getenv('SSH_KEY_FILE')
        if ssh_key_file is not None:
            ssh_cmd += ['-i', ssh_key_file]
        ssh_cmd += [host, cmd]
    return yatest.common.execute(ssh_cmd, wait=False, text=True, shell=local)
