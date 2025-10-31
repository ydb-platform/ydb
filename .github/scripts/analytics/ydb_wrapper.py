#!/usr/bin/env python3

import datetime
import inspect
import json
import os
import time
import uuid
import ydb
from typing import List, Dict, Any, Optional, Callable
from contextlib import contextmanager

class YDBWrapper:
    """Обертка для работы с YDB с логированием статистики"""
    
    def __init__(self, config_path: str = None, enable_statistics: bool = None, script_name: str = None):
        # Приоритет: YDB_QA_CONFIG env > config file (JSON)
        ydb_qa_config_env = os.environ.get("YDB_QA_CONFIG")
        
        if ydb_qa_config_env:
            # Парсим JSON из ENV
            try:
                config_dict = json.loads(ydb_qa_config_env)
                enable_statistics = self._load_config_from_dict(config_dict, enable_statistics)
                
            except (json.JSONDecodeError, KeyError) as e:
                raise RuntimeError(f"Invalid YDB_QA_CONFIG format: {e}")
        else:
            # Fallback к JSON файлу для локальной разработки
            if config_path is None:
                dir_path = os.path.dirname(__file__)
                config_path = f"{dir_path}/../../config/ydb_qa_db.json"
            
            # Загружаем JSON конфиг
            try:
                with open(config_path, 'r') as f:
                    config_dict = json.load(f)
                enable_statistics = self._load_config_from_dict(config_dict, enable_statistics)
            except FileNotFoundError:
                raise RuntimeError(f"Config file not found: {config_path}")
            except json.JSONDecodeError as e:
                raise RuntimeError(f"Invalid JSON config file: {e}")
        
        # Настройки статистики
        self._enable_statistics = enable_statistics
        self._cluster_version = None
        self._session_id = str(uuid.uuid4())
        
        # Автоматически определяем script_name если не передан
        if script_name is None:
            script_name = self._get_caller_script_name()
        self._script_name = script_name
        
        # GitHub Action info - получаем один раз
        self._github_info = self._get_github_action_info()
        
        # Получаем версию кластера один раз при инициализации
        try:
            with self.get_driver() as driver:
                self._get_cluster_version(driver)
        except Exception as e:
            self._log("warning", f"Failed to get cluster version: {e}")
            self._cluster_version = "unknown"
        
        # Проверяем доступность stats DB только один раз при инициализации
        self._stats_available = None
        if self._enable_statistics:
            self._stats_available = self._check_stats_availability()
            if self._stats_available:
                self._log("info", f"Statistics logging enabled - session_id: {self._session_id}")
            else:
                self._log("warning", "Statistics database is not available, statistics will not be logged")
        else:
            self._log("info", "Statistics logging disabled")
    
    def _load_config_from_dict(self, config_dict: dict, enable_statistics: bool = None):
        """Загрузка конфигурации из словаря"""
        dbs = config_dict["databases"]
        main = dbs["main"]
        stats = dbs.get("statistics", {})
        variables = config_dict.get("variables", {})
        flags = config_dict.get("flags", {})
        
        # Автоматический маппинг полей
        config_mapping = {
            'database_endpoint': main["endpoint"],
            'database_path': main["path"],
            '_connection_timeout': main.get("connection_timeout", 60),
            '_main_db_tables': main.get("tables", {}),
            'stats_endpoint': stats.get("endpoint", main["endpoint"]),
            'stats_path': stats.get("path", main["path"]),
            '_stats_connection_timeout': stats.get("connection_timeout",60),
            'stats_table': stats.get("tables", {}).get("query_statistics", "analytics/query_statistics"),
            '_stats_db_tables': stats.get("tables", {}),
            'ydbd_path': variables.get("ydbd_path", "ydb/apps/ydbd/ydbd")
        }
        
        for key, value in config_mapping.items():
            setattr(self, key, value)
        
        # Enable statistics из flags (по умолчанию True)
        if enable_statistics is None:
            enable_statistics = flags.get("enable_statistics", True)
        
        return enable_statistics
    
    def _get_caller_script_name(self) -> str:
        """Автоматическое определение имени скрипта, вызвавшего YDBWrapper"""
        try:
            # Получаем стек вызовов
            stack = inspect.stack()
            
            # Ищем первый фрейм, который находится вне ydb_wrapper.py
            for frame_info in stack:
                frame_filename = frame_info.filename
                
                # Пропускаем текущий файл (ydb_wrapper.py)
                if 'ydb_wrapper.py' not in frame_filename:
                    # Получаем имя файла без пути и расширения
                    script_name = os.path.splitext(os.path.basename(frame_filename))[0]
                    return script_name
            
            # Если не нашли, используем дефолтное значение
            return "unknown_script"
            
        except Exception:
            # В случае ошибки возвращаем дефолтное значение
            return "unknown_script"
    
    def _make_full_path(self, table_path: str) -> str:
        """Преобразование относительного пути в полный (с database_path)
        
        Args:
            table_path: Относительный путь к таблице
            
        Returns:
            Полный путь к таблице
        """
        # Если путь уже полный (начинается с database_path), возвращаем как есть
        if table_path.startswith(self.database_path):
            return table_path
        
        # Убираем ведущий слеш если есть
        table_path = table_path.lstrip('/')
        
        # Добавляем database_path
        return f"{self.database_path}/{table_path}"
    
    def __enter__(self):
        """Контекстный менеджер - вход"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Контекстный менеджер - выход"""
        pass  # Больше не нужно останавливать потоки
    
    def _log(self, level: str, message: str, details: str = ""):
        """Универсальное логирование"""
        timestamp = datetime.datetime.now().strftime("%H:%M:%S")
        icons = {"start": "🚀", "progress": "⏳", "success": "✅", "error": "❌", "info": "ℹ️", "warning": "⚠️", "stats": "📊"}
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
            
            # Версия кластера уже получена в __init__
            # Дополнительно получаем её только если она None (fallback)
            if self._cluster_version is None:
                try:
                    self._get_cluster_version(driver)
                except Exception as e:
                    self._log("warning", f"Failed to get cluster version during operation: {e}")
                    self._cluster_version = "unknown"
            
            yield driver
    
    def _check_stats_availability(self) -> bool:
        """Проверка доступности базы данных статистики (вызывается только один раз в __init__)"""
        try:
            # Настраиваем credentials
            self._setup_credentials()
            
            # Подключаемся к базе статистики
            driver = ydb.Driver(
                endpoint=self.stats_endpoint,
                database=self.stats_path,
                credentials=ydb.credentials_from_env_variables()
            )
            try:
                driver.wait(timeout=self._stats_connection_timeout, fail_fast=True)
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
                       cluster_version: str = None, table_path: str = None, query_name: str = None):
        """Log operation statistics (synchronously)
        
        Args:
            query_name: Optional name for the query (e.g., monitoring query filename)
        """
        # Проверяем, нужно ли логировать статистику
        if not self._enable_statistics or not self._stats_available:
            return
        
        # Подготавливаем данные для записи
        # Используем timestamp в микросекундах (как YDB Timestamp)
        timestamp_us = int(time.time() * 1000000)
        
        stats_data = {
            'timestamp': timestamp_us,
            'session_id': self._session_id,
            'operation_type': operation_type,
            'query': query,
            'duration_ms': int(duration * 1000),
            'status': status,
            'error': error,
            'rows_affected': rows_affected,
            'script_name': self._script_name,
            'query_name': query_name,
            'cluster_version': cluster_version,
            'database_endpoint': self.database_endpoint,
            'database_path': self.database_path,
            'table_path': self._normalize_table_path(table_path),
            'github_workflow_name': self._github_info['workflow_name'],
            'github_run_id': self._github_info['run_id'],
            'github_run_url': self._github_info['run_url']
        }
        
        # Логируем детали отправляемой статистики
        self._log("stats", "Sending statistics", 
                  f"operation={operation_type}, status={status}, duration={duration:.2f}s, rows={rows_affected or 0}, cluster={cluster_version or 'unknown'}")
        
        # Записываем статистику синхронно
        send_start = time.time()
        success = self._write_stats_sync(stats_data)
        send_duration = time.time() - send_start
        
        # Логируем результат отправки
        if success:
            self._log("success", f"Statistics sent successfully in {send_duration:.2f}s")
    
    def _write_stats_sync(self, stats_data):
        """Синхронная запись статистики"""
        try:
            # Настраиваем credentials для базы статистики
            self._setup_credentials()
            
            driver = ydb.Driver(
                endpoint=self.stats_endpoint,
                database=self.stats_path,
                credentials=ydb.credentials_from_env_variables()
            )
            try:
                # Подключаемся к базе статистики
                driver.wait(timeout=self._stats_connection_timeout, fail_fast=True)
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
                    .add_column("query_name", ydb.OptionalType(ydb.PrimitiveType.Utf8))
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
                
                return True
                
            finally:
                driver.stop()
                
        except TimeoutError as e:
            self._log("warning", f"Failed to send statistics: connection timeout (timeout={self._stats_connection_timeout}s)")
            self._stats_available = False
            return False
        except Exception as e:
            error_type = type(e).__name__
            error_msg = str(e)
            self._log("warning", f"Failed to send statistics ({error_type}): {error_msg}")
            return False
    
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
                    `query_name` Utf8,
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
                             query: str = None, table_path: str = None, query_name: str = None) -> Any:
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
        
        # Используем _cluster_version или 'unknown' если он None
        cluster_version = self._cluster_version or "unknown"
        
        try:
            with self.get_driver() as driver:
                if operation_type == "scan_query":
                    tc_settings = ydb.TableClientSettings().with_native_date_in_result_sets(enabled=False)
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
                            
                            # Логируем прогресс только каждые 50 батчей или каждые 10000 строк (но не для пустых результатов)
                            if (batch_count % 50 == 0 or (rows_affected > 0 and rows_affected % 10000 == 0)) and rows_affected > 0:
                                elapsed = time.time() - start_time
                                self._log("progress", f"Batch {batch_count}: {batch_size} rows (total: {rows_affected})", f"{elapsed:.2f}s")
                            
                        except StopIteration:
                            break
                    
                    end_time = time.time()
                    duration = end_time - start_time
                    
                    if rows_affected == 0:
                        self._log("success", f"Scan query completed", f"No results found, Duration: {duration:.2f}s")
                    else:
                        self._log("success", f"Scan query completed", f"Total results: {rows_affected} rows, Duration: {duration:.2f}s")
                    
                    # Логируем статистику
                    self._log_statistics(
                        operation_type=operation_type,
                        query=query,
                        duration=duration,
                        status=status,
                        rows_affected=rows_affected,
                        cluster_version=cluster_version,
                        table_path=table_path,
                        query_name=query_name
                    )
                    
                    return results
                
                elif operation_type == "scan_query_with_metadata":
                    # Для scan_query_with_metadata используем operation_func
                    result = operation_func(driver)
                    
                    # operation_func всегда возвращает (results, column_types)
                    data, metadata = result
                    rows_affected = len(data) if isinstance(data, list) else 0
                    
                    end_time = time.time()
                    duration = end_time - start_time
                    
                    if rows_affected == 0:
                        self._log("success", f"Scan query with metadata completed", f"No results found, Duration: {duration:.2f}s")
                    else:
                        self._log("success", f"Scan query with metadata completed", f"Total results: {rows_affected} rows, Duration: {duration:.2f}s")
                    
                    # Логируем статистику (используем "scan_query" для обоих типов scan операций)
                    self._log_statistics(
                        operation_type="scan_query",
                        query=query,
                        duration=duration,
                        status=status,
                        rows_affected=rows_affected,
                        cluster_version=cluster_version,
                        table_path=table_path,
                        query_name=query_name
                    )
                    
                    return result
                
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
                        cluster_version=cluster_version,
                        table_path=table_path,
                        query_name=query_name
                    )
                    
                    return result
                
        except Exception as e:
            end_time = time.time()
            duration = end_time - start_time
            status = "error"
            error = str(e)
            
            self._log("error", f"{operation_type} failed", f"Error: {error}, Duration: {duration:.2f}s")
            
            # Normalize operation_type for statistics (use "scan_query" for both scan operations)
            stats_operation_type = "scan_query" if operation_type == "scan_query_with_metadata" else operation_type
            
            # Log error statistics
            self._log_statistics(
                operation_type=stats_operation_type,
                query=query or f"{operation_type} operation",
                duration=duration,
                status=status,
                error=error,
                cluster_version=cluster_version,
                table_path=table_path if operation_type == "bulk_upsert" else None,
                query_name=query_name
            )
            
            raise
    
    
    def execute_scan_query(self, query: str, query_name: str = None) -> List[Dict[str, Any]]:
        """Выполнение scan query с логированием"""
        return self._execute_with_logging("scan_query", None, query, None, query_name)
    
    def execute_scan_query_with_metadata(self, query: str) -> tuple[List[Dict[str, Any]], List[tuple[str, Any]]]:
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
        
        return self._execute_with_logging("scan_query_with_metadata", operation, query, None)
    
    def create_table(self, table_path: str, create_sql: str):
        """Создание таблицы с логированием
        
        Args:
            table_path: Относительный путь к таблице (например, 'test_results/test_runs')
            create_sql: SQL для создания таблицы
        """
        # Преобразуем в полный путь для YDB (если еще не полный)
        full_path = self._make_full_path(table_path)
        
        def operation(driver):
            def callee(session):
                session.execute_scheme(create_sql)
            
            with ydb.SessionPool(driver) as pool:
                pool.retry_operation_sync(callee)
            return 1  # Возвращаем 1 для обозначения создания таблицы
        
        return self._execute_with_logging("create_table", operation, create_sql, table_path)
    
    def bulk_upsert(self, table_path: str, rows: List[Dict[str, Any]], 
                   column_types: ydb.BulkUpsertColumns):
        """Выполнение bulk upsert с логированием
        
        Args:
            table_path: Относительный путь к таблице (например, 'test_results/test_runs')
        """
        # Преобразуем в полный путь для YDB
        full_path = self._make_full_path(table_path)
        rows_count = len(rows) if rows else 0
        
        def operation(driver):
            table_client = ydb.TableClient(driver)
            table_client.bulk_upsert(full_path, rows, column_types)
            return rows_count  # Возвращаем количество строк для статистики
        
        return self._execute_with_logging("bulk_upsert", operation, f"BULK_UPSERT to {table_path}", table_path)
    
    def bulk_upsert_batches(self, table_path: str, all_rows: List[Dict[str, Any]], 
                           column_types: ydb.BulkUpsertColumns, batch_size: int = 1000):
        """Выполнение bulk upsert с разбивкой на batches и агрегированной статистикой
        
        Args:
            table_path: Относительный путь к таблице (например, 'test_results/test_runs')
            all_rows: Все данные для вставки
            column_types: Типы колонок
            batch_size: Размер batch (по умолчанию 1000)
        """
        # Преобразуем в полный путь для YDB
        full_path = self._make_full_path(table_path)
        
        start_time = time.time()
        total_rows = len(all_rows)
        
        if total_rows == 0:
            self._log("info", "bulk_upsert_batches: no rows to insert")
            return
        
        num_batches = (total_rows - 1) // batch_size + 1
        self._log("start", f"Executing bulk_upsert_batches", 
                  f"{total_rows} rows in {num_batches} batches of {batch_size}")
        
        status = "success"
        error = None
        
        # Используем _cluster_version или 'unknown' если он None
        cluster_version = self._cluster_version or "unknown"
        
        try:
            with self.get_driver() as driver:
                table_client = ydb.TableClient(driver)
                
                for batch_num, start_idx in enumerate(range(0, total_rows, batch_size), 1):
                    batch_rows = all_rows[start_idx:start_idx + batch_size]
                    table_client.bulk_upsert(full_path, batch_rows, column_types)
                    
                    # Логируем прогресс каждые 10 batches или для последнего
                    if batch_num % 10 == 0 or start_idx + batch_size >= total_rows:
                        elapsed = time.time() - start_time
                        processed = min(start_idx + batch_size, total_rows)
                        self._log("progress", f"Batch {batch_num}/{num_batches}", 
                                  f"{processed}/{total_rows} rows, {elapsed:.2f}s")
            
            duration = time.time() - start_time
            self._log("success", f"bulk_upsert_batches completed", 
                      f"Total: {total_rows} rows in {num_batches} batches, Duration: {duration:.2f}s")
            
            # Логируем ОДНУ запись статистики для всей операции
            self._log_statistics(
                operation_type="bulk_upsert",
                query=f"BULK_UPSERT to {table_path} ({total_rows} rows in {num_batches} batches)",
                duration=duration,
                status=status,
                rows_affected=total_rows,
                cluster_version=cluster_version,
                table_path=table_path
            )
            
        except Exception as e:
            duration = time.time() - start_time
            status = "error"
            error = str(e)
            
            self._log("error", f"bulk_upsert_batches failed", f"Error: {error}, Duration: {duration:.2f}s")
            
            self._log_statistics(
                operation_type="bulk_upsert",
                query=f"BULK_UPSERT to {table_path}",
                duration=duration,
                status=status,
                error=error,
                cluster_version=cluster_version,
                table_path=table_path
            )
            raise
    
    def execute_dml(self, query: str, parameters: Dict[str, Any] = None, query_name: str = None):
        """Выполнение DML запроса (INSERT/UPDATE/DELETE) с параметрами
        
        Args:
            query: SQL запрос с DECLARE параметрами
            parameters: Словарь параметров {$param_name: value}
            query_name: Имя запроса для логирования
        """
        def operation(driver):
            def callee(session):
                prepared_query = session.prepare(query)
                with session.transaction() as tx:
                    tx.execute(prepared_query, parameters or {}, commit_tx=True)
                    return 1  # Успешное выполнение
            
            with ydb.SessionPool(driver) as pool:
                return pool.retry_operation_sync(callee)
        
        return self._execute_with_logging("dml_query", operation, query, None, query_name)
    
    
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
    
    def get_table_path(self, table_name: str, database: str = "main") -> str:
        """Получить путь к таблице из конфигурации
        
        Args:
            table_name: Имя таблицы (например, 'test_results')
            database: База данных ('main' или 'statistics')
        
        Returns:
            Путь к таблице относительно базы данных
        
        Raises:
            KeyError: Если таблица не найдена в конфигурации
        """
        if database == "main":
            if table_name not in self._main_db_tables:
                raise KeyError(f"Table '{table_name}' not found in main_database.tables config")
            return self._main_db_tables[table_name]
        elif database == "statistics":
            if table_name not in self._stats_db_tables:
                raise KeyError(f"Table '{table_name}' not found in statistics_database.tables config")
            return self._stats_db_tables[table_name]
        else:
            raise ValueError(f"Unknown database: {database}. Use 'main' or 'statistics'")
    
    def get_available_tables(self, database: str = "main") -> dict:
        """Получить словарь всех доступных таблиц
        
        Args:
            database: База данных ('main' или 'statistics')
        
        Returns:
            Словарь {table_name: table_path}
        """
        if database == "main":
            return self._main_db_tables.copy()
        elif database == "statistics":
            return self._stats_db_tables.copy()
        else:
            raise ValueError(f"Unknown database: {database}")
    
    def get_cluster_info(self) -> Dict[str, Any]:
        """Получение информации о кластере и статистике (без создания нового подключения)"""
        try:
            # Используем уже полученную версию, не создаём новое подключение
            version = self._cluster_version
            
            # Проверяем статус статистики
            stats_status = "disabled"
            if self._enable_statistics:
                stats_status = "enabled_and_available" if self._stats_available else "enabled_but_unavailable"
            
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
                'github_workflow': self._github_info['workflow_name'],
                'github_run_id': self._github_info['run_id'],
                'github_run_url': self._github_info['run_url']
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
