"""
Утилиты для выполнения команд на удаленных хостах и localhost
"""

import os
import subprocess
import logging
import yatest.common
import shutil
import shlex
import allure
import random
from contextlib import AbstractContextManager
from time import time
from typing import Union, Optional, Tuple, List, Dict, Any, NamedTuple

LOGGER = logging.getLogger(__name__)


class ExecutionResult(NamedTuple):

    stdout: str
    stderr: str
    is_timeout: bool
    exit_code: Optional[int] = None


class RemoteExecutor:
    """Класс для выполнения команд на удаленных хостах и localhost"""

    @staticmethod
    def _safe_decode(data) -> str:
        """
        Безопасно декодирует данные в строку

        Args:
            data: данные для декодирования (str, bytes или None)

        Returns:
            str: декодированная строка
        """
        if data is None:
            return ""
        if isinstance(data, bytes):
            return data.decode('utf-8', errors='replace')
        return str(data)

    @staticmethod
    def _filter_ssh_warnings(stderr: str) -> str:
        """
        Фильтрует SSH предупреждения из stderr

        Args:
            stderr: строка с выводом stderr

        Returns:
            str: отфильтрованная строка stderr без SSH предупреждений
        """
        if not stderr:
            return stderr

        # Фильтруем строки, начинающиеся с "Warning: Permanently added"
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
        Проверяет, является ли хост localhost

        Args:
            hostname: имя хоста для проверки

        Returns:
            bool: True если хост является localhost
        """
        if not hostname:
            return False

        # Импортируем socket здесь, чтобы избежать циклических импортов
        import socket

        # Очевидные случаи localhost
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
            # Получаем IP адрес хоста
            host_ip = socket.gethostbyname(hostname)

            # Получаем локальные IP адреса
            local_ips = set()

            # Добавляем localhost адреса
            local_ips.update(['127.0.0.1', '::1'])

            # Получаем IP адреса всех сетевых интерфейсов
            hostname_local = socket.gethostname()
            try:
                local_ips.add(socket.gethostbyname(hostname_local))
            except socket.gaierror:
                pass

            # Проверяем, совпадает ли IP хоста с локальными IP
            return host_ip in local_ips

        except (socket.gaierror, socket.herror):
            # Если не можем разрешить имя хоста, считаем что это не localhost
            return False

    @classmethod
    def execute_command(
        cls, host: str, cmd: Union[str, list], raise_on_error: bool = True,
        timeout: Optional[float] = None, raise_on_timeout: bool = True
    ) -> ExecutionResult:
        """
        Выполняет команду на хосте через SSH или локально

        Args:
            host: имя хоста для выполнения команды
            cmd: команда для выполнения (строка или список)
            raise_on_error: вызывать ли исключение при ошибке
            timeout: таймаут выполнения команды в секундах
            raise_on_timeout: вызывать ли исключение при таймауте (по умолчанию True)

        Returns:
            ExecutionResult: результат выполнения команды
        """

        def _handle_timeout_error(
            e: yatest.common.ExecutionTimeoutError,
            full_cmd: Union[str, list],
            is_local: bool
        ) -> ExecutionResult:
            """Обрабатывает ошибки таймаута"""
            cmd_type = "Local" if is_local else "SSH"
            timeout_info = f"{cmd_type} command timed out after {timeout} seconds on {host}"
            stdout = ""
            stderr = ""

            # Извлекаем информацию из execution_result
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

            # Логирование
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
            """Обрабатывает ошибки выполнения"""
            cmd_type = "Local" if is_local else "SSH"
            stdout = ""
            stderr = ""
            exit_code = 1

            # Извлекаем информацию из execution_result
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

            # Фильтруем SSH предупреждения для удаленных команд
            if not is_local:
                stderr = cls._filter_ssh_warnings(stderr)

            if raise_on_error:
                # Логируем детальную информацию об ошибке
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
            """Выполняет команду локально"""
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
                # Если команда завершилась с ошибкой, но stderr пустой, добавляем синтетическое сообщение
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
            """Выполняет команду через SSH"""
            LOGGER.info(f"Executing SSH command on {host}: {cmd}")

            ssh_cmd = ['ssh', "-o", "StrictHostKeyChecking=no", "-o", "UserKnownHostsFile=/dev/null"]

            # Добавляем SSH пользователя и ключ
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
                # Если команда завершилась с ошибкой, но stderr пустой, добавляем синтетическое сообщение
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

        # Основная логика: выбираем локальное или SSH выполнение
        if cls._is_localhost(host):
            return _execute_local_command(cmd)
        else:
            return _execute_ssh_command(cmd)


# Удобные функции для прямого использования
def execute_command(
    host: str, cmd: Union[str, list], raise_on_error: bool = True,
    timeout: Optional[float] = None, raise_on_timeout: bool = True
) -> ExecutionResult:
    """
    Удобная функция для выполнения команды на хосте

    Args:
        host: имя хоста для выполнения команды
        cmd: команда для выполнения (строка или список)
        raise_on_error: вызывать ли исключение при ошибке
        timeout: таймаут выполнения команды в секундах
        raise_on_timeout: вызывать ли исключение при таймауте

    Returns:
        ExecutionResult: результат выполнения команды
    """
    return RemoteExecutor.execute_command(host, cmd, raise_on_error, timeout, raise_on_timeout)


def execute_command_legacy(
    host: str, cmd: Union[str, list], raise_on_error: bool = True,
    timeout: Optional[float] = None, raise_on_timeout: bool = True
) -> Tuple[str, str]:
    """
    Backward compatibility функция для старого API

    Args:
        host: имя хоста для выполнения команды
        cmd: команда для выполнения (строка или список)
        raise_on_error: вызывать ли исключение при ошибке
        timeout: таймаут выполнения команды в секундах
        raise_on_timeout: вызывать ли исключение при таймауте

    Returns:
        Tuple[str, str]: (stdout, stderr) - вывод команды
    """
    result = RemoteExecutor.execute_command(host, cmd, raise_on_error, timeout, raise_on_timeout)
    return result.stdout, result.stderr


def is_localhost(hostname: str) -> bool:
    """
    Удобная функция для проверки, является ли хост localhost

    Args:
        hostname: имя хоста для проверки

    Returns:
        bool: True если хост является localhost
    """
    return RemoteExecutor._is_localhost(hostname)


def ensure_directory_with_permissions(host: str, path: str, raise_on_error: bool = True) -> bool:
    """
    Создает директорию и устанавливает права 777
    """
    try:
        if execute_command(host, f"mkdir -p {path} && chmod 777 {path}", raise_on_error=False, timeout=10).exit_code == 0:
            return True
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
    Копирует файл на хост через SCP или локально

    Args:
        local_path: путь к локальному файлу
        host: имя хоста
        remote_path: путь на хосте
        raise_on_error: вызывать ли исключение при ошибке

    Returns:
        str: результат копирования или None при ошибке
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
    Единая логика копирования файлов для локального и удаленного хостов
    """
    # Создаем целевую директорию
    remote_dir = os.path.dirname(remote_path)
    if remote_dir and remote_dir != '/':
        ensure_directory_with_permissions(host, remote_dir)

    if is_local:
        # Локальное копирование
        try:
            shutil.copy2(local_path, remote_path)
            return f"Local copy successful: {remote_path}"
        except PermissionError:
            # Используем временный файл и sudo
            import tempfile
            with tempfile.NamedTemporaryFile(delete=False, prefix="deploy_") as tmp_file:
                temp_path = tmp_file.name
            shutil.copy2(local_path, temp_path)
            execute_command("localhost", f"sudo mv {temp_path} {remote_path}")
            return f"Local copy with sudo successful: {remote_path}"
    else:
        # Удаленное копирование через SCP + временный файл
        timestamp = int(time())
        filename = os.path.basename(local_path)
        tmp_path = f"/tmp/{filename}.{timestamp}.tmp"

        # SCP в /tmp
        scp_cmd = ['scp', "-o", "StrictHostKeyChecking=no", "-o", "UserKnownHostsFile=/dev/null"]

        ssh_user = os.getenv('SSH_USER')
        scp_host = f"{ssh_user}@{host}" if ssh_user else host

        ssh_key_file = os.getenv('SSH_KEY_FILE')
        if ssh_key_file:
            scp_cmd += ['-i', ssh_key_file]

        scp_cmd += [local_path, f"{scp_host}:{tmp_path}"]

        yatest.common.execute(scp_cmd, wait=True, check_exit_code=True)

        # Перемещаем из /tmp в целевое место
        if execute_command(host, f"mv {tmp_path} {remote_path}", raise_on_error=False).exit_code != 0:
            execute_command(host, f"sudo mv {tmp_path} {remote_path}", raise_on_error=True)
        return f"SCP copy successful: {remote_path}"


def deploy_binary(local_path: str, host: str, target_dir: str, make_executable: bool = True) -> dict:
    """
    Разворачивает бинарный файл на хосте

    Args:
        local_path: путь к локальному бинарному файлу
        host: имя хоста
        target_dir: директория на хосте
        make_executable: делать ли файл исполняемым

    Returns:
        dict: результат деплоя
    """
    binary_name = os.path.basename(local_path)
    target_path = os.path.join(target_dir, binary_name)

    try:
        # Создаем директорию
        ensure_directory_with_permissions(host, target_dir, raise_on_error=True)

        # Копируем файл
        copy_result = copy_file(local_path, host, target_path)
        if copy_result is None:
            raise Exception("File copy failed")

        # Делаем исполняемым
        if make_executable:
            if execute_command(host, f"chmod +x {target_path}", raise_on_error=False).exit_code != 0:
                execute_command(host, f"sudo chmod +x {target_path}", raise_on_error=True)

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
    Разворачивает бинарные файлы на указанных хостах

    Args:
        binary_files: список путей к бинарным файлам
        hosts: список хостов для развертывания
        target_dir: директория для размещения файлов на хостах

    Returns:
        Dict: словарь с результатами деплоя по хостам
    """
    results = {}

    for host in set(hosts):  # Автоматическое удаление дубликатов
        host_results = {}

        # Создаем директорию на хосте один раз
        ensure_directory_with_permissions(host, target_dir, raise_on_error=True)

        # Копируем каждый бинарный файл
        for binary_file in binary_files:
            result = deploy_binary(binary_file, host, target_dir)
            host_results[os.path.basename(binary_file)] = result
            # Логируем только критичные события
            if not result['success']:
                LOGGER.error(f"Failed to deploy {result['name']} to {host}: {result.get('error', 'Unknown error')}")

        results[host] = host_results

    return results


def fix_binaries_directory_permissions(hosts: List[str], target_dir: str = '/tmp/stress_binaries/') -> Dict[str, bool]:
    """
    Исправляет права доступа к директории binaries на всех указанных хостах
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


def get_remote_tmp_path(host: str, *localpath: str) -> str:
    tmpdir = '/tmp'
    if is_localhost(host):
        tmpdir = os.getenv('TMP') or os.getenv('TMPDIR') or yatest.common.work_path()
    if not localpath:
        return tmpdir
    return os.path.join(tmpdir, *localpath)


class LongRemoteExecution(AbstractContextManager):
    def __init__(self, host: str, *cmd: str):
        self.host = host
        self.cmd = shlex.join(cmd)
        self_hash = f're_{abs(hash((self.host, self.cmd)))}_{random.randint(0, 1000000)}'
        tmpdir = get_remote_tmp_path(self.host, self_hash)
        self._script_path = os.path.join(tmpdir, f'run_{self_hash}.sh')
        self._out_path = os.path.join(tmpdir, 'out.txt')
        self._err_path = os.path.join(tmpdir, 'err.txt')
        self._return_code_path = os.path.join(tmpdir, 'rc')
        self._pid_path = os.path.join(tmpdir, 'pid')
        self._return_code = None
        self._out = None
        self._err = None
        self._finished = False
        self.allure = None

    def start(self) -> None:
        assert not self._finished, 'command cannot be started again'
        local_script_path = yatest.common.work_path(os.path.basename(self._script_path))
        with open(local_script_path, 'w') as script_file:
            script_file.write(f'''#!/bin/bash
                {self.cmd} >{self._out_path} 2>{self._err_path}
                echo -n $? >{self._return_code_path}
            ''')
        deploy_result = deploy_binary(local_script_path, self.host, os.path.dirname(self._script_path))
        assert deploy_result.get('success', False), deploy_result.get('error', '')
        execute_command(self.host, f'start-stop-daemon --start --pidfile {self._pid_path} --make-pid --background --no-close --exec {self._script_path}')

    def is_running(self) -> bool:
        if self._finished:
            return False
        cmd = f'start-stop-daemon --status --pidfile {self._pid_path}'
        self._finished = execute_command(self.host, cmd, raise_on_error=False).exit_code != 0
        return not self._finished

    def terminate(self) -> None:
        execute_command(self.host, f'start-stop-daemon --stop --pidfile {self._pid_path}', raise_on_error=False)

    def _get_content(self, path: str) -> Optional[str]:
        res = execute_command(self.host, f'cat {path}', raise_on_error=False)
        if res.exit_code != 0:
            return None
        return res.stdout

    @property
    def stdout(self) -> str:
        if self._out is None:
            self._out = self._get_content(self._out_path)
        return self._out or ''

    @property
    def stderr(self) -> str:
        if self._err is None:
            self._err = self._get_content(self._err_path)
        return self._err or ''

    @property
    def return_code(self) -> Optional[int]:
        if self._return_code is None:
            rc = self._get_content(self._return_code_path)
            if rc is not None:
                self._return_code = int(rc)
        return self._return_code

    def __enter__(self):
        self.allure = allure.step('Run remote command')
        self.allure.params = {'host': self.host, 'command': self.cmd}
        self.allure.__enter__()
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.terminate()
        if self.return_code is not None:
            allure.attach(str(self.return_code), 'return code', allure.attachment_type.TEXT)
        allure.attach(self.stdout, 'remote stdout', allure.attachment_type.TEXT)
        allure.attach(self.stderr, 'remote stderr', allure.attachment_type.TEXT)
        self.allure.__exit__(exc_type, exc_val, exc_tb)
