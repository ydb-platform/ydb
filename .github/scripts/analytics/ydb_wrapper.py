#!/usr/bin/env python3

import configparser
import datetime
import os
import time
import uuid
import ydb
from typing import List, Dict, Any, Optional, Callable
from contextlib import contextmanager

class YDBWrapper:
    """Обертка для работы с YDB с логированием статистики"""
    
    def __init__(self, config_path: str = None, enable_statistics: bool = None):
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
    
    def _log(self, level: str, message: str, details: str = ""):
        """Универсальное логирование"""
        timestamp = datetime.datetime.now().strftime("%H:%M:%S")
        icons = {"start": "🚀", "progress": "⏳", "success": "✅", "error": "❌", "info": "ℹ️", "warning": "⚠️"}
        print(f"🕐 [{timestamp}] {icons.get(level, '📝')} {message}")
        if details:
            print(f"   📋 {details}")
    
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
            driver.wait(timeout=10, fail_fast=True)
            
            # Получаем версию кластера при первом подключении
            if self._cluster_version is None:
                self._get_cluster_version(driver)
            
            yield driver
    
    def _check_stats_availability(self) -> bool:
        """Проверка доступности базы данных статистики"""
        try:
            with self.get_driver() as driver:
                tc_settings = ydb.TableClientSettings().with_native_date_in_result_sets(enabled=True)
                table_client = ydb.TableClient(driver, tc_settings)
                scan_query = ydb.ScanQuery("SELECT 1 as test", {})
                it = table_client.scan_query(scan_query)
                next(it)
                return True
        except Exception:
            return False
    
    def _log_statistics(self, operation_type: str, query: str, duration: float, 
                       status: str, error: str = None, rows_affected: int = None,
                       script_name: str = None, cluster_version: str = None,
                       table_name: str = None):
        """Логирование статистики выполнения операций"""
        # Проверяем, нужно ли логировать статистику
        if not self._enable_statistics:
            return
        
        if self._stats_available is None:
            self._stats_available = self._check_stats_availability()
        
        if not self._stats_available:
            self._log("warning", "Skipping statistics logging - database not available")
            return
        
        try:
            with self.get_driver() as driver:
                # Создаем таблицу статистики если не существует
                self._ensure_stats_table_exists(driver)
                
                # Подготавливаем данные для вставки
                stats_data = [{
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
                    'table_name': table_name
                }]
                
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
                    .add_column("table_name", ydb.OptionalType(ydb.PrimitiveType.Utf8))
                )
                
                full_path = f"{self.stats_path}/{self.stats_table}"
                table_client.bulk_upsert(full_path, stats_data, column_types)
                
        except Exception as e:
            self._log("warning", f"Failed to log statistics: {e}")
            self._stats_available = False
    
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
                    `table_name` Utf8,
                    PRIMARY KEY (`timestamp`, `session_id`, `operation_type`, `script_name`)
                )
                PARTITION BY HASH(`session_id`)
                WITH (STORE = COLUMN)
            """
            session.execute_scheme(create_sql)
        
        with ydb.SessionPool(driver) as pool:
            pool.retry_operation_sync(create_stats_table)
    
    def _execute_with_logging(self, operation_type: str, operation_func: Callable, 
                             query: str = None, script_name: str = None) -> Any:
        """Универсальный метод выполнения операций с логированием"""
        start_time = time.time()
        self._log("start", f"Executing {operation_type}")
        
        if query:
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
                    
                    self._log("success", f"Scan query completed", f"Total results: {rows_affected} rows, Version: {cluster_version}, Duration: {duration:.2f}s")
                    
                    # Логируем статистику
                    self._log_statistics(
                        operation_type=operation_type,
                        query=query,
                        duration=duration,
                        status=status,
                        rows_affected=rows_affected,
                        script_name=script_name,
                        cluster_version=cluster_version,
                        table_name=None  # scan_query не работает с конкретной таблицей
                    )
                    
                    return results
                
                else:
                    # Для других операций
                    result = operation_func(driver)
                    
                    # Если операция вернула число, используем его как rows_affected
                    if isinstance(result, (int, float)) and operation_type in ["bulk_upsert", "create_table"]:
                        rows_affected = int(result)
                    
                    end_time = time.time()
                    duration = end_time - start_time
                    
                    self._log("success", f"{operation_type} completed", f"Version: {cluster_version}, Duration: {duration:.2f}s")
                    
                    # Логируем статистику
                    self._log_statistics(
                        operation_type=operation_type,
                        query=query or f"{operation_type} operation",
                        duration=duration,
                        status=status,
                        rows_affected=rows_affected,
                        script_name=script_name,
                        cluster_version=cluster_version,
                        table_name=self._extract_table_name_from_query(query) if operation_type == "bulk_upsert" else None
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
                table_name=self._extract_table_name_from_query(query) if operation_type == "bulk_upsert" else None
            )
            
            raise
    
    def _extract_table_name_from_query(self, query: str) -> str:
        """Извлечение имени таблицы из запроса bulk_upsert"""
        if not query or "BULK_UPSERT to" not in query:
            return None
        
        # Извлекаем имя таблицы из строки "BULK_UPSERT to table_name"
        try:
            parts = query.split("BULK_UPSERT to ")
            if len(parts) > 1:
                table_name = parts[1].strip()
                # Убираем путь к базе данных, оставляем только имя таблицы
                if "/" in table_name:
                    table_name = table_name.split("/")[-1]
                return table_name
        except Exception:
            pass
        
        return None
    
    def execute_scan_query(self, query: str, script_name: str = None) -> List[Dict[str, Any]]:
        """Выполнение scan query с логированием"""
        return self._execute_with_logging("scan_query", None, query, script_name)
    
    def create_table(self, table_path: str, create_sql: str, script_name: str = None):
        """Создание таблицы с логированием"""
        def operation(driver):
            def callee(session):
                session.execute_scheme(create_sql)
            
            with ydb.SessionPool(driver) as pool:
                pool.retry_operation_sync(callee)
            return 1  # Возвращаем 1 для обозначения создания таблицы
        
        return self._execute_with_logging("create_table", operation, create_sql, script_name)
    
    def bulk_upsert(self, table_path: str, rows: List[Dict[str, Any]], 
                   column_types: ydb.BulkUpsertColumns, script_name: str = None):
        """Выполнение bulk upsert с логированием"""
        rows_count = len(rows) if rows else 0
        
        def operation(driver):
            table_client = ydb.TableClient(driver)
            table_client.bulk_upsert(table_path, rows, column_types)
            return rows_count  # Возвращаем количество строк для статистики
        
        return self._execute_with_logging("bulk_upsert", operation, f"BULK_UPSERT to {table_path}", script_name)
    
    def get_session_id(self) -> str:
        """Получение ID текущей сессии"""
        return self._session_id
    
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
                
                return {
                    'session_id': self._session_id,
                    'version': version,
                    'endpoint': self.database_endpoint,
                    'database': self.database_path,
                    'statistics_enabled': self._enable_statistics,
                    'statistics_status': stats_status,
                    'statistics_endpoint': self.stats_endpoint,
                    'statistics_database': self.stats_path,
                    'statistics_table': self.stats_table
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
