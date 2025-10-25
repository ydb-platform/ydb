#!/usr/bin/env python3

import configparser
import datetime
import os
import time
import uuid
import ydb
import threading
import queue
from typing import List, Dict, Any, Optional, Callable
from contextlib import contextmanager

class YDBWrapper:
    """Обертка для работы с YDB с логированием статистики"""
    
    def __init__(self, config_path: str = None, enable_statistics: bool = None, connection_timeout: int = 10):
        if config_path is None:
            dir = os.path.dirname(__file__)
            config_path = f"{dir}/../../config/ydb_qa_db.ini"
        
        self.config = configparser.ConfigParser()
        self.config.read(config_path)
        
        # Основная база данных
        self.database_endpoint = self.config["QA_DB"]["DATABASE_ENDPOINT"]
        self.database_path = self.config["QA_DB"]["DATABASE_PATH"]
        
        # База данных для статистики
        self.stats_endpoint = self.config["STATISTICS_DB"]["DATABASE_ENDPOINT"]
        self.stats_path = self.config["STATISTICS_DB"]["DATABASE_PATH"]
        self.stats_table = self.config["STATISTICS_DB"]["STATISTICS_TABLE"]
        
        # Настройки статистики
        if enable_statistics is None:
            enable_statistics = os.environ.get("YDB_ENABLE_STATISTICS", "true").lower() in ("true", "1", "yes")
        
        self._enable_statistics = enable_statistics
        self._cluster_version = None
        self._stats_available = None
        self._session_id = str(uuid.uuid4())  # Уникальный ID для сессии выполнения скрипта
        
        # Настройка таймаута подключения
        if connection_timeout == 10:  # Значение по умолчанию
            env_timeout = os.environ.get("YDB_CONNECTION_TIMEOUT")
            if env_timeout:
                try:
                    self._connection_timeout = int(env_timeout)
                except ValueError:
                    self._connection_timeout = 10
            else:
                self._connection_timeout = 10
        else:
            self._connection_timeout = connection_timeout
        
        # Асинхронная очередь для статистики
        self._stats_queue = queue.Queue()
        self._stats_thread = None
        self._stats_thread_running = False
    
    def _start_stats_thread(self):
        """Запуск потока для асинхронной записи статистики"""
        if self._stats_thread is None or not self._stats_thread.is_alive():
            self._stats_thread_running = True
            self._stats_thread = threading.Thread(target=self._stats_worker, daemon=True)
            self._stats_thread.start()
            self._log("info", "Statistics thread started")
    
    def _stop_stats_thread(self):
        """Остановка потока статистики"""
        if self._stats_thread and self._stats_thread.is_alive():
            self._stats_thread_running = False
            # Добавляем сигнал завершения в очередь
            self._stats_queue.put(None)
            self._stats_thread.join(timeout=5)
            self._log("info", "Statistics thread stopped")
    
    def _stats_worker(self):
        """Рабочий поток для записи статистики"""
        while self._stats_thread_running:
            try:
                # Получаем данные статистики из очереди с таймаутом
                stats_data = self._stats_queue.get(timeout=1)
                if stats_data is None:  # Сигнал завершения
                    break
                
                # Записываем статистику
                self._write_stats_sync(stats_data)
                self._stats_queue.task_done()
                
            except queue.Empty:
                continue
            except Exception as e:
                self._log("warning", f"Error in stats worker: {e}")
    
    def _log(self, level: str, message: str, details: str = ""):
        """Универсальное логирование"""
        timestamp = datetime.datetime.now().strftime("%H:%M:%S")
        icons = {"start": "🚀", "progress": "⏳", "success": "✅", "error": "❌", "info": "ℹ️", "warning": "⚠️"}
        if details:
            print(f"🕐 [{timestamp}] {icons.get(level, '📝')} {message} | {details}")
        else:
            print(f"🕐 [{timestamp}] {icons.get(level, '📝')} {message}")
    
    def _setup_credentials(self):
        """Настройка учетных данных YDB"""
        if "CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS" not in os.environ:
            raise RuntimeError("Env variable CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS is missing")
        
        os.environ["YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS"] = os.environ[
            "CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS"
        ]
    
    def check_credentials(self):
        """Проверка наличия учетных данных YDB"""
        if "CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS" not in os.environ:
            print("Error: Env variable CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS is missing, skipping")
            return False
        
        # Do not set up 'real' variable from gh workflows because it interfere with ydb tests
        # So, set up it locally
        os.environ["YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS"] = os.environ[
            "CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS"
        ]
        return True
    
    def __enter__(self):
        """Контекстный менеджер - вход"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Контекстный менеджер - выход с автоматическим закрытием"""
        self.close()
    
    def close(self):
        """Корректное завершение работы с остановкой потока статистики"""
        self._stop_stats_thread()
    
    def _get_cluster_version(self, driver) -> str:
        """Получение версии кластера YDB"""
        if self._cluster_version is not None:
            return self._cluster_version
            
        try:
            tc_settings = ydb.TableClientSettings().with_native_date_in_result_sets(enabled=True)
            table_client = ydb.TableClient(driver, tc_settings)
            scan_query = ydb.ScanQuery("SELECT Version() as version", {})
            it = table_client.scan_query(scan_query)
            result = next(it)
            
            if result.result_set.rows:
                self._cluster_version = result.result_set.rows[0]['version']
                return self._cluster_version
            else:
                raise RuntimeError("No version data returned from cluster")
                
        except Exception as e:
            raise RuntimeError(f"Failed to get cluster version: {e}")
    
    @contextmanager
    def get_driver(self):
        """Контекстный менеджер для получения драйвера YDB"""
        self._setup_credentials()
        
        with ydb.Driver(
            endpoint=self.database_endpoint,
            database=self.database_path,
            credentials=ydb.credentials_from_env_variables(),
        ) as driver:
            try:
                driver.wait(timeout=self._connection_timeout, fail_fast=True)
            except TimeoutError as e:
                self._log("error", f"Failed to connect to YDB: {e}")
                self._log("error", f"Endpoint: {self.database_endpoint}")
                self._log("error", f"Database: {self.database_path}")
                self._log("error", f"Timeout: {self._connection_timeout} seconds")
                self._log("error", "Possible causes: network issues, YDB service unavailable, or invalid credentials")
                self._log("error", f"Try increasing timeout with: export YDB_CONNECTION_TIMEOUT=30")
                raise RuntimeError(f"YDB connection timeout after {self._connection_timeout}s: {e}") from e
            except Exception as e:
                self._log("error", f"Failed to connect to YDB: {e}")
                self._log("error", f"Endpoint: {self.database_endpoint}")
                self._log("error", f"Database: {self.database_path}")
                self._log("error", f"Timeout: {self._connection_timeout} seconds")
                self._log("error", "Check your YDB credentials and network connectivity")
                raise RuntimeError(f"YDB connection failed: {e}") from e
            
            # Получаем версию кластера при первом подключении
            if self._cluster_version is None:
                self._get_cluster_version(driver)
            
            yield driver
    
    def _check_stats_availability(self) -> bool:
        """Проверка доступности базы данных статистики"""
        try:
            # Настраиваем credentials
            self._setup_credentials()
            
            # Подключаемся к базе статистики с тем же таймаутом, что и для основной базы
            driver = ydb.Driver(
                endpoint=self.stats_endpoint,
                database=self.stats_path,
                credentials=ydb.credentials_from_env_variables()
            )
            try:
                driver.wait(timeout=self._connection_timeout, fail_fast=True)
                tc_settings = ydb.TableClientSettings().with_native_date_in_result_sets(enabled=True)
                table_client = ydb.TableClient(driver, tc_settings)
                scan_query = ydb.ScanQuery("SELECT 1 as test", {})
                it = table_client.scan_query(scan_query)
                next(it)
                return True
            finally:
                driver.stop()
        except Exception:
            return False
    
    def _log_statistics(self, operation_type: str, query: str, duration: float, 
                       status: str, error: str = None, rows_affected: int = None,
                       script_name: str = None, cluster_version: str = None,
                       table_path: str = None):
        """Логирование статистики выполнения операций (асинхронно)"""
        # Проверяем, нужно ли логировать статистику
        if not self._enable_statistics:
            # Логируем только один раз в начале сессии, что статистика отключена
            if not hasattr(self, '_stats_disabled_logged'):
                self._log("info", "Statistics logging disabled")
                self._stats_disabled_logged = True
            return
        
        # Запускаем поток статистики если еще не запущен
        if not self._stats_thread_running:
            self._start_stats_thread()
        
        # Подготавливаем данные для асинхронной записи
        stats_data = {
            'timestamp': datetime.datetime.now(),
            'session_id': self._session_id,
            'operation_type': operation_type,
            'query': query,
            'duration_ms': int(duration * 1000),
            'status': status,
            'error': error,
            'rows_affected': rows_affected,
            'script_name': script_name,
            'cluster_version': cluster_version,
            'database_endpoint': self.database_endpoint,
            'database_path': self.database_path,
            'table_path': self._normalize_table_path(table_path),
            'github_workflow_name': self._get_github_action_info()['workflow_name'],
            'github_run_id': self._get_github_action_info()['run_id'],
            'github_run_url': self._get_github_action_info()['run_url']
        }
        
        # Добавляем в очередь для асинхронной обработки
        try:
            self._stats_queue.put(stats_data, timeout=1)
        except queue.Full:
            self._log("warning", "Statistics queue is full, dropping stats entry")
    
    def _write_stats_sync(self, stats_data):
        """Синхронная запись статистики (вызывается из потока)"""
        if self._stats_available is None:
            self._stats_available = self._check_stats_availability()
        
        if not self._stats_available:
            # Попробуем еще раз проверить доступность базы статистики
            self._stats_available = self._check_stats_availability()
            if not self._stats_available:
                return
        
        try:
            # Используем тот же таймаут для статистики, что и для основной базы
            # Настраиваем credentials для базы статистики
            self._setup_credentials()
            
            driver = ydb.Driver(
                endpoint=self.stats_endpoint,
                database=self.stats_path,
                credentials=ydb.credentials_from_env_variables()
            )
            try:
                driver.wait(timeout=self._connection_timeout, fail_fast=True)
                # Создаем таблицу статистики если не существует
                self._ensure_stats_table_exists(driver)
                
                # Подготавливаем данные для вставки
                stats_data_list = [stats_data]
                
                # Вставляем статистику
                table_client = ydb.TableClient(driver)
                column_types = (
                    ydb.BulkUpsertColumns()
                    .add_column("timestamp", ydb.OptionalType(ydb.PrimitiveType.Timestamp))
                    .add_column("session_id", ydb.OptionalType(ydb.PrimitiveType.Utf8))
                    .add_column("operation_type", ydb.OptionalType(ydb.PrimitiveType.Utf8))
                    .add_column("query", ydb.OptionalType(ydb.PrimitiveType.Utf8))
                    .add_column("duration_ms", ydb.OptionalType(ydb.PrimitiveType.Uint64))
                    .add_column("status", ydb.OptionalType(ydb.PrimitiveType.Utf8))
                    .add_column("error", ydb.OptionalType(ydb.PrimitiveType.Utf8))
                    .add_column("rows_affected", ydb.OptionalType(ydb.PrimitiveType.Uint64))
                    .add_column("script_name", ydb.OptionalType(ydb.PrimitiveType.Utf8))
                    .add_column("cluster_version", ydb.OptionalType(ydb.PrimitiveType.Utf8))
                    .add_column("database_endpoint", ydb.OptionalType(ydb.PrimitiveType.Utf8))
                    .add_column("database_path", ydb.OptionalType(ydb.PrimitiveType.Utf8))
                    .add_column("table_path", ydb.OptionalType(ydb.PrimitiveType.Utf8))
                    .add_column("github_workflow_name", ydb.OptionalType(ydb.PrimitiveType.Utf8))
                    .add_column("github_run_id", ydb.OptionalType(ydb.PrimitiveType.Utf8))
                    .add_column("github_run_url", ydb.OptionalType(ydb.PrimitiveType.Utf8))
                )
                
                full_path = f"{self.stats_path}/{self.stats_table}"
                table_client.bulk_upsert(full_path, stats_data_list, column_types)
                
                # Логируем успешную запись статистики (только для первой операции в сессии)
                if not hasattr(self, '_stats_logged'):
                    self._log("info", f"Statistics logging enabled - session_id: {self._session_id}")
                    self._stats_logged = True
            finally:
                driver.stop()
                
        except TimeoutError as e:
            # Отключаем статистику при таймауте подключения
            self._log("warning", f"Statistics disabled due to connection timeout (timeout={self._connection_timeout}s)")
            self._stats_available = False
        except Exception as e:
            # Логируем более детальную информацию об ошибке
            error_type = type(e).__name__
            error_msg = str(e)
            self._log("warning", f"Failed to log statistics ({error_type}): {error_msg}")
            
            # Если это ошибка подключения, попробуем сбросить статус доступности
            if "timeout" in error_msg.lower() or "connection" in error_msg.lower() or "endpoint" in error_msg.lower():
                self._stats_available = None  # Сбросим статус для повторной проверки
    
    def _ensure_stats_table_exists(self, driver):
        """Создание таблицы статистики если не существует"""
        def create_stats_table(session):
            create_sql = f"""
                CREATE TABLE IF NOT EXISTS `{self.stats_table}` (
                    `timestamp` Timestamp NOT NULL,
                    `session_id` Utf8 NOT NULL,
                    `operation_type` Utf8 NOT NULL,
                    `query` Utf8,
                    `duration_ms` Uint64 NOT NULL,
                    `status` Utf8 NOT NULL,
                    `error` Utf8,
                    `rows_affected` Uint64,
                    `script_name` Utf8 NOT NULL,
                    `cluster_version` Utf8,
                    `database_endpoint` Utf8,
                    `database_path` Utf8,
                    `table_path` Utf8,
                    `github_workflow_name` Utf8,
                    `github_run_id` Utf8,
                    `github_run_url` Utf8,
                    PRIMARY KEY (`timestamp`, `session_id`, `operation_type`, `script_name`)
                )
                PARTITION BY HASH(`session_id`)
                WITH (STORE = COLUMN)
            """
            session.execute_scheme(create_sql)
        
        with ydb.SessionPool(driver) as pool:
            pool.retry_operation_sync(create_stats_table)
    
    def _execute_with_logging(self, operation_type: str, operation_func: Callable, 
                             query: str = None, script_name: str = None, table_path: str = None) -> Any:
        """Универсальный метод выполнения операций с логированием"""
        start_time = time.time()
        
        # Логируем начало операции
        self._log("start", f"Executing {operation_type}")
        
        if query:
            # Для bulk_upsert не показываем детали для каждого batch'а
            if operation_type != "bulk_upsert":
                self._log("info", f"Query details:\n{query}")
        
        status = "success"
        error = None
        rows_affected = 0
        cluster_version = None
        
        try:
            with self.get_driver() as driver:
                cluster_version = self._cluster_version
                
                if operation_type == "scan_query":
                    tc_settings = ydb.TableClientSettings().with_native_date_in_result_sets(enabled=True)
                    table_client = ydb.TableClient(driver, tc_settings)
                    scan_query = ydb.ScanQuery(query, {})
                    it = table_client.scan_query(scan_query)
                    
                    results = []
                    batch_count = 0
                    
                    while True:
                        try:
                            result = next(it)
                            batch_count += 1
                            batch_size = len(result.result_set.rows) if result.result_set.rows else 0
                            results = results + result.result_set.rows
                            rows_affected += batch_size
                            
                            # Логируем прогресс только каждые 50 батчей или каждые 10000 строк
                            if batch_count % 50 == 0 or rows_affected % 10000 == 0:
                                elapsed = time.time() - start_time
                                self._log("progress", f"Batch {batch_count}: {batch_size} rows (total: {rows_affected})", f"{elapsed:.2f}s")
                            
                        except StopIteration:
                            break
                    
                    end_time = time.time()
                    duration = end_time - start_time
                    
                    self._log("success", f"Scan query completed", f"Total results: {rows_affected} rows, Duration: {duration:.2f}s")
                    
                    # Логируем статистику
                    self._log_statistics(
                        operation_type=operation_type,
                        query=query,
                        duration=duration,
                        status=status,
                        rows_affected=rows_affected,
                        script_name=script_name,
                        cluster_version=cluster_version,
                        table_path=table_path  # scan_query может не иметь конкретной таблицы
                    )
                    
                    return results
                
                else:
                    # Для других операций
                    result = operation_func(driver)
                    
                    # Если операция вернула число, используем его как rows_affected
                    if isinstance(result, (int, float)) and operation_type in ["bulk_upsert", "create_table"]:
                        rows_affected = int(result)
                    # Если операция вернула кортеж (данные + метаданные), извлекаем количество строк
                    elif isinstance(result, tuple) and len(result) == 2 and operation_type == "scan_query":
                        data, metadata = result
                        rows_affected = len(data) if isinstance(data, list) else 0
                    # Если scan_query вернул просто список, считаем количество строк
                    elif isinstance(result, list) and operation_type == "scan_query":
                        rows_affected = len(result)
                    
                    end_time = time.time()
                    duration = end_time - start_time
                    
                    self._log("success", f"{operation_type} completed", f"Duration: {duration:.2f}s")
                    
                    # Логируем статистику
                    self._log_statistics(
                        operation_type=operation_type,
                        query=query or f"{operation_type} operation",
                        duration=duration,
                        status=status,
                        rows_affected=rows_affected,
                        script_name=script_name,
                        cluster_version=cluster_version,
                        table_path=table_path
                    )
                    
                    return result
                
        except Exception as e:
            end_time = time.time()
            duration = end_time - start_time
            status = "error"
            error = str(e)
            
            self._log("error", f"{operation_type} failed", f"Error: {error}, Duration: {duration:.2f}s")
            
            # Логируем статистику ошибки
            self._log_statistics(
                operation_type=operation_type,
                query=query or f"{operation_type} operation",
                duration=duration,
                status=status,
                error=error,
                script_name=script_name,
                cluster_version=cluster_version,
                table_path=table_path if operation_type == "bulk_upsert" else None
            )
            
            raise
    
    
    def execute_scan_query(self, query: str, script_name: str = None) -> List[Dict[str, Any]]:
        """Выполнение scan query с логированием"""
        return self._execute_with_logging("scan_query", None, query, script_name, None)
    
    def execute_scan_query_with_metadata(self, query: str, script_name: str = None) -> tuple[List[Dict[str, Any]], List[tuple[str, Any]]]:
        """Выполнение scan query с возвратом данных и метаданных колонок"""
        def operation(driver):
            tc_settings = ydb.TableClientSettings().with_native_date_in_result_sets(enabled=True)
            table_client = ydb.TableClient(driver, tc_settings)
            scan_query = ydb.ScanQuery(query, {})
            it = table_client.scan_query(scan_query)
            
            results = []
            column_types = None
            
            while True:
                try:
                    result = next(it)
                    if column_types is None:
                        column_types = [(col.name, col.type) for col in result.result_set.columns]
                    
                    results.extend(result.result_set.rows)
                
                except StopIteration:
                    break
            
            # Если нет результатов, column_types может быть None
            if column_types is None:
                column_types = []
            
            return results, column_types
        
        return self._execute_with_logging("scan_query", operation, query, script_name, None)
    
    def create_table(self, table_path: str, create_sql: str, script_name: str = None):
        """Создание таблицы с логированием"""
        def operation(driver):
            def callee(session):
                session.execute_scheme(create_sql)
            
            with ydb.SessionPool(driver) as pool:
                pool.retry_operation_sync(callee)
            return 1  # Возвращаем 1 для обозначения создания таблицы
        
        return self._execute_with_logging("create_table", operation, create_sql, script_name, table_path)
    
    def bulk_upsert(self, table_path: str, rows: List[Dict[str, Any]], 
                   column_types: ydb.BulkUpsertColumns, script_name: str = None):
        """Выполнение bulk upsert с логированием"""
        rows_count = len(rows) if rows else 0
        
        def operation(driver):
            table_client = ydb.TableClient(driver)
            table_client.bulk_upsert(table_path, rows, column_types)
            return rows_count  # Возвращаем количество строк для статистики
        
        # Всегда выполняем операцию с логированием статистики
        return self._execute_with_logging("bulk_upsert", operation, f"BULK_UPSERT to {table_path}", script_name, table_path)
    
    def get_session_id(self) -> str:
        """Получение ID текущей сессии"""
        return self._session_id
    
    def _get_github_action_info(self) -> dict:
        """Получение информации о GitHub Action если скрипт запущен в GitHub Actions"""
        github_info = {
            "workflow_name": None,
            "run_id": None,
            "run_url": None
        }
        
        try:
            # GitHub Actions устанавливает эти переменные окружения
            workflow_name = os.environ.get("GITHUB_WORKFLOW")
            run_id = os.environ.get("GITHUB_RUN_ID")
            repository = os.environ.get("GITHUB_REPOSITORY")
            
            if workflow_name:
                github_info["workflow_name"] = workflow_name
            
            if run_id and repository:
                github_info["run_id"] = run_id
                github_info["run_url"] = f"https://github.com/{repository}/actions/runs/{run_id}"
                
        except Exception:
            # Игнорируем ошибки получения GitHub информации
            pass
            
        return github_info
    
    def _normalize_table_path(self, table_path: str) -> str:
        """Нормализация пути к таблице - исключаем database_path для краткости"""
        if not table_path:
            return None
            
        # Если путь начинается с database_path, убираем его
        if self.database_path and table_path.startswith(self.database_path):
            normalized = table_path[len(self.database_path):]
            # Убираем ведущий слеш если есть
            if normalized.startswith('/'):
                normalized = normalized[1:]
            return normalized
            
        return table_path
    
    def get_cluster_info(self) -> Dict[str, Any]:
        """Получение информации о кластере и статистике"""
        try:
            with self.get_driver() as driver:
                version = self._cluster_version
                
                # Проверяем статус статистики
                stats_status = "disabled"
                if self._enable_statistics:
                    if self._stats_available is None:
                        self._stats_available = self._check_stats_availability()
                    
                    stats_status = "enabled_and_available" if self._stats_available else "enabled_but_unavailable"
                
                # Получаем информацию о GitHub Action
                github_info = self._get_github_action_info()
                
                return {
                    'session_id': self._session_id,
                    'version': version,
                    'endpoint': self.database_endpoint,
                    'database': self.database_path,
                    'statistics_enabled': self._enable_statistics,
                    'statistics_status': stats_status,
                    'statistics_endpoint': self.stats_endpoint,
                    'statistics_database': self.stats_path,
                    'statistics_table': self.stats_table,
                    'github_workflow': github_info['workflow_name'],
                    'github_run_id': github_info['run_id'],
                    'github_run_url': github_info['run_url']
                }
                
        except Exception as e:
            return {
                'session_id': self._session_id,
                'version': None,
                'endpoint': self.database_endpoint,
                'database': self.database_path,
                'error': str(e),
                'statistics_enabled': self._enable_statistics,
                'statistics_status': "disabled",
                'statistics_endpoint': self.stats_endpoint,
                'statistics_database': self.stats_path,
                'statistics_table': self.stats_table
            }
