"""
Утилиты для выполнения команд на удаленных хостах и localhost
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
    """
    Результат выполнения команды
    
    Attributes:
        stdout: Стандартный вывод команды
        stderr: Ошибки команды  
        is_timeout: True если команда была прервана по таймауту
        exit_code: Код завершения (если доступен)
    """
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
                not line.startswith('Warning: Permanently added') and
                not line.startswith('(!) New version of YDB CL')
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


def mkdir(host: str, path: str, raise_on_error: bool = False, use_sudo: bool = True) -> str:
    """
    Создает директорию на хосте

    Args:
        host: имя хоста
        path: путь к создаваемой директории
        raise_on_error: вызывать ли исключение при ошибке
        use_sudo: использовать ли sudo для создания директории

    Returns:
        str: вывод команды
    """
    cmd_prefix = "sudo " if use_sudo else ""
    result = execute_command(host, f"{cmd_prefix}mkdir -p {path}", raise_on_error)
    return result.stdout


def chmod(host: str, path: str, mode: str = "+x", raise_on_error: bool = True, use_sudo: bool = True) -> str:
    """
    Изменяет права доступа к файлу на хосте

    Args:
        host: имя хоста
        path: путь к файлу
        mode: права доступа (по умолчанию +x)
        raise_on_error: вызывать ли исключение при ошибке
        use_sudo: использовать ли sudo для изменения прав

    Returns:
        str: вывод команды
    """
    cmd_prefix = "sudo " if use_sudo else ""
    result = execute_command(host, f"{cmd_prefix}chmod {mode} {path}", raise_on_error)
    return result.stdout


def copy_file(local_path: str, host: str, remote_path: str, raise_on_error: bool = True) -> str:
    """
    Копирует файл на хост через SCP или локально

    Args:
        local_path: путь к локальному файлу
        host: имя хоста
        remote_path: путь на хосте
        raise_on_error: вызывать ли исключение при ошибке

    Returns:
        str: вывод команды копирования
    """
    # Проверяем существование локального файла
    if not os.path.exists(local_path):
        error_msg = f"Local file does not exist: {local_path}"
        LOGGER.error(error_msg)
        if raise_on_error:
            raise FileNotFoundError(error_msg)
        return None

    # Логируем размер файла для диагностики
    try:
        file_size = os.path.getsize(local_path)
        LOGGER.debug(f"File size: {file_size} bytes")
    except OSError as e:
        LOGGER.warning(f"Could not get file size: {e}")

    # Проверяем, является ли хост localhost
    if is_localhost(host):
        LOGGER.info(f"Detected localhost ({host}), copying file locally: {local_path} -> {remote_path}")

        try:
            # Создаем директорию назначения, если она не существует
            remote_dir = os.path.dirname(remote_path)
            if remote_dir and not os.path.exists(remote_dir):
                os.makedirs(remote_dir, exist_ok=True)
                LOGGER.debug(f"Created directory: {remote_dir}")

            # Проверяем, не является ли целевой файл занятым
            if os.path.exists(remote_path):
                try:
                    # Пытаемся открыть файл для записи, чтобы проверить, не занят ли он
                    with open(remote_path, 'r+b'):
                        pass
                except (OSError, IOError) as e:
                    if "Text file busy" in str(e) or "Resource temporarily unavailable" in str(e):
                        # Генерируем новое имя файла с постфиксом
                        timestamp = int(time())
                        remote_filename = os.path.basename(remote_path)
                        new_remote_filename = f"{remote_filename}.{timestamp}"
                        remote_path = os.path.join(remote_dir, new_remote_filename)
                        LOGGER.warning(f"Target file is busy, using new filename: {remote_path}")

            # Копируем файл
            shutil.copy2(local_path, remote_path)

            # Проверяем, что файл скопирован успешно
            if os.path.exists(remote_path):
                copied_size = os.path.getsize(remote_path)
                original_size = os.path.getsize(local_path)
                if copied_size == original_size:
                    LOGGER.info(f"Successfully copied file locally: {local_path} -> {remote_path}")
                    return f"Local copy successful: {remote_path}"
                else:
                    error_msg = f"File size mismatch after copy: original={original_size}, copied={copied_size}"
                    LOGGER.error(error_msg)
                    if raise_on_error:
                        raise IOError(error_msg)
                    return None
            else:
                error_msg = f"File was not created at destination: {remote_path}"
                LOGGER.error(error_msg)
                if raise_on_error:
                    raise IOError(error_msg)
                return None

        except Exception as e:
            error_msg = f"Error copying file locally: {e}"
            LOGGER.error(error_msg)
            if raise_on_error:
                raise IOError(error_msg) from e
            return None

    # Для удаленных хостов используем более надежный подход с /tmp и sudo
    return _copy_file_via_tmp_and_sudo(local_path, host, remote_path, raise_on_error)


def _copy_file_via_tmp_and_sudo(local_path: str, host: str, remote_path: str, raise_on_error: bool = True) -> str:
    """
    Копирует файл через промежуточную стадию в /tmp с использованием sudo для финального перемещения
    
    Args:
        local_path: путь к локальному файлу
        host: имя хоста
        remote_path: путь на хосте
        raise_on_error: вызывать ли исключение при ошибке
        
    Returns:
        str: вывод команды копирования
    """
    # Генерируем уникальное имя временного файла
    timestamp = int(time())
    filename = os.path.basename(local_path)
    tmp_filename = f"{filename}.{timestamp}.tmp"
    tmp_path = f"/tmp/{tmp_filename}"
    
    LOGGER.info(f"Copying {local_path} to {host} via /tmp staging: {tmp_path} -> {remote_path}")
    
    # Шаг 1: Копируем файл в /tmp через SCP
    def _scp_to_tmp() -> Tuple[bool, str, str]:
        """Копирует файл в /tmp на удаленном хосте"""
        scp_cmd = ['scp', "-o", "StrictHostKeyChecking=no", "-o", "UserKnownHostsFile=/dev/null"]

        # Добавляем SSH пользователя, если указан
        ssh_user = os.getenv('SSH_USER')
        scp_host = host
        if ssh_user is not None:
            scp_host = f"{ssh_user}@{scp_host}"

        # Добавляем ключ SSH, если указан
        ssh_key_file = os.getenv('SSH_KEY_FILE')
        if ssh_key_file is not None:
            scp_cmd += ['-i', ssh_key_file]

        # Добавляем источник и назначение
        scp_cmd += [local_path, f"{scp_host}:{tmp_path}"]

        LOGGER.info(f"Step 1: SCP to /tmp - {scp_cmd}")

        try:
            execution = yatest.common.execute(
                scp_cmd,
                wait=True,
                check_exit_code=False
            )

            stdout = (RemoteExecutor._safe_decode(execution.std_out)
                      if hasattr(execution, 'std_out') and execution.std_out else "")
            stderr = (RemoteExecutor._safe_decode(execution.std_err)
                      if hasattr(execution, 'std_err') and execution.std_err else "")

            stderr_filtered = RemoteExecutor._filter_ssh_warnings(stderr)
            exit_code = getattr(execution, 'exit_code', getattr(execution, 'returncode', 0))
            
            return exit_code == 0, stdout, stderr_filtered

        except Exception as e:
            LOGGER.error(f"SCP to /tmp failed: {e}")
            return False, "", str(e)

    # Выполняем копирование в /tmp
    scp_success, scp_stdout, scp_stderr = _scp_to_tmp()
    
    if not scp_success:
        error_msg = f"Failed to copy file to /tmp on {host}. Error: {scp_stderr}"
        LOGGER.error(error_msg)
        if raise_on_error:
            raise subprocess.CalledProcessError(1, ['scp', local_path, f"{host}:{tmp_path}"], scp_stdout, scp_stderr)
        return None

    LOGGER.info(f"Step 1 completed: File copied to {tmp_path} on {host}")

    # Шаг 2: Создаем целевую директорию и перемещаем файл с помощью sudo
    try:
        remote_dir = os.path.dirname(remote_path)
        
        # Создаем целевую директорию если нужно (используем обновленную функцию mkdir)
        if remote_dir and remote_dir != '/':
            mkdir(host, remote_dir, raise_on_error=False)
        
        # Перемещаем файл из /tmp в целевое место
        mv_cmd = f"sudo mv {tmp_path} {remote_path}"
        LOGGER.info(f"Step 2b: Moving file - {mv_cmd}")
        result = execute_command(host, mv_cmd, raise_on_error=True)
        
        LOGGER.info(f"Step 2 completed: File moved to {remote_path}")
        
        # Шаг 3: Проверяем, что файл существует в целевом месте
        check_cmd = f"ls -la {remote_path}"
        LOGGER.info(f"Step 3: Verifying file - {check_cmd}")
        check_result = execute_command(host, check_cmd, raise_on_error=False)
        
        if check_result.stderr.strip() and "No such file or directory" in check_result.stderr:
            error_msg = f"File verification failed: {remote_path} does not exist after copy"
            LOGGER.error(error_msg)
            if raise_on_error:
                raise IOError(error_msg)
            return None
        else:
            LOGGER.info(f"File verification successful: {check_result.stdout.strip()}")
            return f"Successfully copied via /tmp staging: {remote_path}"
            
    except Exception as e:
        # Очищаем временный файл в случае ошибки
        cleanup_cmd = f"rm -f {tmp_path}"
        LOGGER.info(f"Cleanup: Removing temporary file - {cleanup_cmd}")
        try:
            execute_command(host, cleanup_cmd, raise_on_error=False)
        except:
            pass  # Игнорируем ошибки очистки
            
        error_msg = f"Failed to move file from /tmp to destination: {e}"
        LOGGER.error(error_msg)
        if raise_on_error:
            raise IOError(error_msg) from e
        return None


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
    import os

    binary_name = os.path.basename(local_path)
    target_path = os.path.join(target_dir, binary_name)
    result = {
        'name': binary_name,
        'path': target_path,
        'success': False
    }

    try:
        # Создаем директорию (с sudo по умолчанию)
        mkdir(host, target_dir, raise_on_error=True)

        # Копируем файл (используется новый метод с /tmp и sudo)
        copy_result = copy_file(local_path, host, target_path)
        if copy_result is None:
            raise Exception("File copy failed")

        # Делаем файл исполняемым (с sudo по умолчанию), если нужно
        if make_executable:
            chmod(host, target_path, raise_on_error=True)

        # Проверяем, что файл скопирован успешно
        ls_result = execute_command(host, f"ls -la {target_path}")

        result.update({
            'success': True,
            'output': ls_result.stdout
        })

        return result
    except Exception as e:
        result.update({
            'error': str(e)
        })
        return result


@allure.step('Deploy binaries to hosts')
def deploy_binaries_to_hosts(
    binary_files: List[str],
    hosts: List[str],
    target_dir: str = '/tmp/binaries/'
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
    processed_hosts = set()

    for host in hosts:
        # Избегаем дублирования обработки одного хоста
        if host in processed_hosts:
            continue
        processed_hosts.add(host)

        host_results = {}
        allure.attach(f"Host: {host}", "Host Info", attachment_type=allure.attachment_type.TEXT)

        # Создаем директорию на хосте
        mkdir(host, target_dir)

        # Копируем каждый бинарный файл
        for binary_file in binary_files:
            try:
                result = deploy_binary(binary_file, host, target_dir)
                host_results[os.path.basename(binary_file)] = result

                if result['success']:
                    allure.attach(
                        f"Successfully deployed {result['name']} to {host}:"
                        f"{result['path']}\n{result.get('output', '')}",
                        f"Deploy {result['name']} to {host}",
                        attachment_type=allure.attachment_type.TEXT
                    )
                else:
                    allure.attach(
                        f"Failed to deploy {result['name']} to {host}: "
                        f"{result.get('error', 'Unknown error')}",
                        f"Deploy {result['name']} to {host} failed",
                        attachment_type=allure.attachment_type.TEXT
                    )
            except Exception as e:
                error_msg = str(e)
                host_results[os.path.basename(binary_file)] = {
                    'success': False,
                    'error': error_msg
                }

                allure.attach(
                    f"Exception when deploying {os.path.basename(binary_file)} "
                    f"to {host}: {error_msg}",
                    f"Deploy {os.path.basename(binary_file)} to {host} failed",
                    attachment_type=allure.attachment_type.TEXT
                )

        # Store the host results in the main results dictionary
        results[host] = host_results

    return results
