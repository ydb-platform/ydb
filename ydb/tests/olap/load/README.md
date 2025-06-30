# YDB OLAP Workload Tests - Руководство разработчика

## Содержание

1. [Добавление нового workload теста](#добавление-нового-workload-теста)
2. [Настройка полей для выгрузки в YDB](#настройка-полей-для-выгрузки-в-ydb)
3. [Определение статуса теста](#определение-статуса-теста)
4. [Добавление функций, выполняемых перед workload](#добавление-функций-выполняемых-перед-workload)
5. [Стадии подготовки workload'ов](#стадии-подготовки-workloadов)
6. [Примеры использования](#примеры-использования)

---

## Добавление нового workload теста

### 1. Создание базового класса теста

Создайте новый файл в `ydb/tests/olap/load/lib/` или добавьте класс в существующий файл:

```python
import pytest
from .workload_executor import WorkloadTestBase
from ydb.tests.olap.lib.ydb_cluster import YdbCluster
from ydb.tests.olap.lib.utils import get_external_param
from enum import Enum

import logging
LOGGER = logging.getLogger(__name__)


class TableType(str, Enum):
    """Тип таблицы для workload"""
    ROW = 'row'
    COLUMN = 'column'


class MyWorkloadBase(WorkloadTestBase):
    """
    Базовый класс для нового workload теста
    """
    
    # Обязательные атрибуты
    workload_binary_name = 'my_workload'  # Имя бинарного файла
    workload_env_var = 'MY_WORKLOAD_BINARY'  # Переменная окружения
    
    def test_workload_my_workload(self):
        """
        Основной тест workload
        """
        # Формируем аргументы команды (без --duration, он будет добавлен автоматически)
        command_args_template = (
            f"--endpoint {YdbCluster.ydb_endpoint} "
            f"--database /{YdbCluster.ydb_database} "
            f"--path my_workload_test"
        )

        # Дополнительная статистика
        additional_stats = {
            "workload_type": "my_workload",
            "path": "my_workload_test"
        }

        # Запускаем тест
        self.execute_workload_test(
            workload_name="MyWorkload",
            command_args=command_args_template,
            duration_value=self.timeout,
            additional_stats=additional_stats,
            use_chunks=True,  # Использовать разбивку на итерации
            duration_param="--duration",
            nemesis=False,  # Включить chaos testing
            nodes_percentage=100  # Процент нод для запуска
        )


class TestMyWorkload(MyWorkloadBase):
    """Тест с таймаутом из get_external_param"""
    timeout = int(get_external_param('workload_duration', 100))
```

### 2. Регистрация теста в pytest

Тест автоматически будет обнаружен pytest'ом, если:
- Файл находится в `ydb/tests/olap/load/lib/`
- Класс наследуется от `WorkloadTestBase`
- Методы начинаются с `test_`

---

## Настройка полей для выгрузки в YDB

### Базовые поля в `additional_stats`

```python
additional_stats = {
    # Обязательные поля
    "workload_type": "my_workload",  # Тип workload
    "path": "my_workload_test",      # Путь к данным
    
    # Опциональные поля
    "table_type": "column_store",    # Тип таблицы
    "operation": "bulk_upsert",      # Тип операции
    "threads": 10,                   # Количество потоков
    "rows_per_batch": 2000,          # Размер батча
    "nemesis_enabled": True,         # Включен ли nemesis
    "nodes_percentage": 100,         # Процент нод
    "scenario": "stability_test"     # Сценарий теста
}
```

### Автоматически собираемые поля

Система автоматически добавляет:
- `total_runs` - общее количество запусков
- `successful_runs` - успешные запуски
- `failed_runs` - неуспешные запуски
- `success_rate` - процент успешности
- `total_execution_time` - общее время выполнения
- `planned_duration` - плановая длительность
- `actual_duration` - фактическая длительность
- `total_iterations` - количество итераций
- `total_threads` - общее количество потоков
- `nodes_with_issues` - количество нод с проблемами

---

## Определение статуса теста

### Логика определения статуса

Статус определяется в порядке приоритета:

1. **FAIL** - если есть cores/OOM на нодах
2. **BROKEN** - если есть ошибки workload
3. **WARNING** - если есть предупреждения
4. **PASS** - успешное выполнение

### Кастомизация логики статуса

```python
def _handle_final_status(self, result, workload_name, node_errors):
    """Кастомная логика определения статуса"""
    
    stats = result.get_stats(workload_name)
    
    # Проверяем кастомные метрики
    success_rate = stats.get("success_rate", 0)
    if success_rate < 0.8:
        result.success = False
        result.add_error(f"Success rate too low: {success_rate:.2f}")
        pytest.fail(f"Success rate critically low: {success_rate:.2f}")
    elif success_rate < 0.9:
        result.add_warning(f"Success rate below optimal: {success_rate:.2f}")
    
    # Вызываем родительский метод для стандартных проверок
    super()._handle_final_status(result, workload_name, node_errors)
```

---

## Добавление функций, выполняемых перед workload

### Переопределение `setup_class`

```python
@classmethod
def setup_class(cls) -> None:
    """Инициализация перед всеми тестами класса"""
    
    # Выполняем подготовку workload
    cls._prepare_workload_environment()
    
    # Создаем необходимые таблицы
    cls._create_workload_tables()
    
    # Загружаем тестовые данные
    cls._load_test_data()
    
    # ОБЯЗАТЕЛЬНО вызываем родительский метод
    super().setup_class()

@classmethod
def _prepare_workload_environment(cls):
    """Подготовка окружения для workload"""
    # Ваш код подготовки
    pass

@classmethod
def _create_workload_tables(cls):
    """Создание таблиц для workload"""
    from ydb.tests.olap.lib.ydb_cluster import YdbCluster
    
    first_node = list(YdbCluster.get_cluster_nodes(db_only=True))[0]
    
    # Пример создания таблиц через ydb_cli
    init_cmd = [
        cls.workload_binary_name,
        '--endpoint', f'grpc://{first_node.host}:{first_node.ic_port}',
        '--database', '/Root/db1',
        'workload', 'my_workload', 'init',
        '--param1', 'value1',
        '--param2', 'value2'
    ]
    
    import yatest.common
    yatest.common.execute(init_cmd, wait=True, check_exit_code=True)

@classmethod
def _load_test_data(cls):
    """Загрузка тестовых данных"""
    # Ваш код загрузки данных
    pass
```

### Переопределение `teardown_class`

```python
@classmethod
def teardown_class(cls):
    """Очистка после всех тестов класса"""
    
    # Очищаем созданные данные
    cls._cleanup_workload_data()
    
    # Вызываем родительский метод
    super().teardown_class()

@classmethod
def _cleanup_workload_data(cls):
    """Очистка данных workload"""
    # Ваш код очистки
    pass
```

---

## Nemesis (Chaos Testing)

### Включение nemesis

Для включения chaos testing с nemesis используйте параметр `nemesis=True`:

```python
def test_workload_with_nemesis(self):
    """Тест с включенным nemesis"""
    
    result = self.execute_workload_test(
        workload_name="workload_with_nemesis",
        command_args="--database /Root/db1 --path test_table",
        duration_value=300,  # 5 минут
        nemesis=True,  # Включаем nemesis
        additional_stats={
            "workload_type": "stress",
            "chaos_testing": True
        }
    )
```

### Как работает nemesis

1. **Запуск** - nemesis запускается через 15 секунд после начала workload
2. **Конфигурация** - если передан параметр `cluster_path`, файл копируется на все ноды кластера как есть
3. **Параллельность** - операции выполняются параллельно на всех нодах
4. **Автоочистка** - nemesis автоматически останавливается в teardown

### Параметры nemesis

- `nemesis=True` - включает chaos testing
- `cluster_path` - путь к файлу конфигурации nemesis (копируется как есть, без изменений)

### Пример с параметризацией

```python
@pytest.mark.parametrize('nemesis_enabled', [True, False], 
                        ids=['nemesis_true', 'nemesis_false'])
def test_workload_with_nemesis_param(self, nemesis_enabled: bool):
    """Тест с параметризованным nemesis"""
    
    self.execute_workload_test(
        workload_name=f"workload_nemesis_{nemesis_enabled}",
        command_args="--database /Root/db1 --path test_table",
        duration_value=300,
        nemesis=nemesis_enabled,
        additional_stats={
            "workload_type": "stress",
            "nemesis_enabled": nemesis_enabled
        }
    )
```

---

## Стадии подготовки workload'ов

### 1. Простые workload'ы (без подготовки)

**Примеры:** `olap_workload`, `oltp_workload`, `simple_queue`

```python
class SimpleWorkloadBase(WorkloadTestBase):
    workload_binary_name = 'simple_workload'
    workload_env_var = 'SIMPLE_WORKLOAD_BINARY'
    
    def test_workload_simple(self):
        command_args_template = (
            f"--endpoint {YdbCluster.ydb_endpoint} "
            f"--database /{YdbCluster.ydb_database} "
            f"--path simple_workload_test"
        )
        
        self.execute_workload_test(
            workload_name="SimpleWorkload",
            command_args=command_args_template,
            duration_value=self.timeout,
            additional_stats={"workload_type": "simple"},
            use_chunks=True,
            duration_param="--duration"
        )
```

### 2. Workload'ы с инициализацией таблиц

**Примеры:** `log_workload`, `topic_workload`

```python
class LogWorkloadBase(WorkloadTestBase):
    workload_binary_name = 'ydb_cli'
    workload_env_var = 'YDB_CLI_BINARY'
    
    @classmethod
    def setup_class(cls):
        """Инициализация таблиц для log workload"""
        cls._init_log_tables()
        super().setup_class()
    
    @classmethod
    def _init_log_tables(cls):
        """Инициализация таблиц для log workload"""
        from ydb.tests.olap.lib.ydb_cluster import YdbCluster
        
        first_node = list(YdbCluster.get_cluster_nodes(db_only=True))[0]
        
        # Инициализация для column storage
        init_cmd = [
            cls.workload_binary_name,
            '--endpoint', f'grpc://{first_node.host}:{first_node.ic_port}',
            '--database', '/Root/db1',
            'workload', 'log', 'init',
            '--min-partitions', '100',
            '--store', 'column',
            '--path', 'log_workload_column_test',
            '--ttl', '4800'
        ]
        
        import yatest.common
        yatest.common.execute(init_cmd, wait=True, check_exit_code=True)
    
    def test_workload_log_column(self):
        command_args_template = (
            f"--endpoint {YdbCluster.ydb_endpoint} --database /{YdbCluster.ydb_database} "
            "workload log run bulk_upsert --rows 2000 --threads 10 "
            "--timestamp_deviation 180 --seconds 600 --path log_workload_column_test"
        )
        
        self.execute_workload_test(
            workload_name="LogWorkloadColumn",
            command_args=command_args_template,
            duration_value=self.timeout,
            additional_stats={
                "workload_type": "log",
                "table_type": "column_store",
                "operation": "bulk_upsert"
            },
            use_chunks=True,
            duration_param="--duration"
        )
```

### 3. Workload'ы с параметризацией

**Примеры:** `simple_queue` с разными типами таблиц

```python
class SimpleQueueBase(WorkloadTestBase):
    workload_binary_name = 'simple_queue'
    workload_env_var = 'SIMPLE_QUEUE_BINARY'
    
    @pytest.mark.parametrize('table_type', ['row', 'column'])
    @pytest.mark.parametrize('nemesis_enabled', [True, False], 
                            ids=['nemesis_true', 'nemesis_false'])
    def test_workload_simple_queue(self, table_type: str, nemesis_enabled: bool):
        command_args_template = (
            f"--endpoint {YdbCluster.ydb_endpoint} "
            f"--database /{YdbCluster.ydb_database} "
            f"--mode {table_type}"
        )
        
        additional_stats = {
            "table_type": table_type,
            "workload_type": "simple_queue",
            "nemesis": nemesis_enabled,
            "nodes_percentage": 100
        }
        
        self.execute_workload_test(
            workload_name=f"SimpleQueue_{table_type}_nemesis_{nemesis_enabled}",
            command_args=command_args_template,
            duration_value=self.timeout,
            additional_stats=additional_stats,
            use_chunks=True,
            duration_param="--duration",
            nemesis=nemesis_enabled,
            nodes_percentage=100
        )
```

### 4. Комплексные workload'ы с множественными стадиями

```python
class ComplexWorkloadBase(WorkloadTestBase):
    workload_binary_name = 'ydb_cli'
    workload_env_var = 'YDB_CLI_BINARY'
    
    @classmethod
    def setup_class(cls):
        """Комплексная инициализация"""
        cls._init_log_tables()
        cls._init_topic()
        cls._init_queues()
        super().setup_class()
    
    @classmethod
    def _init_log_tables(cls):
        """Инициализация log таблиц"""
        # Код инициализации log таблиц
        pass
    
    @classmethod
    def _init_topic(cls):
        """Инициализация topic"""
        # Код инициализации topic
        pass
    
    @classmethod
    def _init_queues(cls):
        """Инициализация очередей"""
        # Код инициализации очередей
        pass
    
    def test_complex_workload(self):
        """Комплексный тест с множественными workload'ами"""
        
        # Запускаем первый workload
        result1 = self.execute_workload_test(
            workload_name="ComplexWorkload1",
            command_args="...",
            duration_value=300,
            additional_stats={"workload_type": "complex_1"},
            use_chunks=True
        )
        
        # Запускаем второй workload
        result2 = self.execute_workload_test(
            workload_name="ComplexWorkload2",
            command_args="...",
            duration_value=300,
            additional_stats={"workload_type": "complex_2"},
            use_chunks=True
        )
        
        # Проверяем результаты
        assert result1.success and result2.success
```

---

## Примеры использования

### Пример 1: Простой workload без инициализации (olap_workload)

```python
from .workload_executor import WorkloadTestBase
from ydb.tests.olap.lib.ydb_cluster import YdbCluster
from ydb.tests.olap.lib.utils import get_external_param

import logging
LOGGER = logging.getLogger(__name__)


class OlapWorkloadBase(WorkloadTestBase):
    workload_binary_name = "olap_workload"
    workload_env_var = "OLAP_WORKLOAD_BINARY"

    def test_workload_olap(self):
        """Тест OLAP workload для аналитических нагрузок"""
        
        command_args_template = (
            f"--endpoint {YdbCluster.ydb_endpoint} "
            f"--database /{YdbCluster.ydb_database} "
            f"--path olap_workload_test"
        )

        additional_stats = {
            "workload_type": "olap",
            "table_type": "column_store",
            "path": "olap_workload_test"
        }

        self.execute_workload_test(
            workload_name="OlapWorkload",
            command_args=command_args_template,
            duration_value=self.timeout,
            additional_stats=additional_stats,
            use_chunks=True,
            duration_param="--duration",
            nemesis=True,  # Включить chaos testing
            nodes_percentage=100  # Запустить на всех нодах
        )


class TestOlapWorkload(OlapWorkloadBase):
    """Тест с таймаутом из get_external_param"""
    timeout = int(get_external_param('workload_duration', 100))
```

### Пример 2: Workload с инициализацией (log workload)

```python
class LogWorkloadBase(WorkloadTestBase):
    workload_binary_name = "ydb_cli"
    workload_env_var = "YDB_CLI_BINARY"
    
    @classmethod
    def setup_class(cls):
        """Инициализация перед всеми тестами - создание таблиц для log workload"""
        cls._init_log_tables()
        super().setup_class()
    
    @classmethod
    def _init_log_tables(cls):
        """Инициализация таблиц для log workload"""
        from ydb.tests.olap.lib.ydb_cluster import YdbCluster
        
        first_node = list(YdbCluster.get_cluster_nodes(db_only=True))[0]
        
        # Инициализируем таблицы для column storage
        init_cmd = [
            cls.workload_binary_name,
            '--endpoint', f'grpc://{first_node.host}:{first_node.ic_port}',
            '--database', '/Root/db1',
            'workload', 'log', 'init',
            '--min-partitions', '100',
            '--partition-size', '10',
            '--auto-partition', '0',
            '--store', 'column',
            '--path', 'log_workload_column_test',
            '--ttl', '4800'
        ]
        
        import yatest.common
        yatest.common.execute(init_cmd, wait=True, check_exit_code=True)
    
    def test_workload_log_column(self):
        """Тест log workload с column storage"""
        
        command_args_template = (
            f"--endpoint {YdbCluster.ydb_endpoint} --database /{YdbCluster.ydb_database} "
            "workload log run bulk_upsert --rows 2000 --threads 10 "
            "--timestamp_deviation 180 --seconds 600 --path log_workload_column_test"
        )
        
        additional_stats = {
            "workload_type": "log",
            "table_type": "column_store",
            "operation": "bulk_upsert",
            "rows_per_batch": 2000,
            "threads": 10,
            "timestamp_deviation": 180,
            "path": "log_workload_column_test"
        }
        
        self.execute_workload_test(
            workload_name="LogWorkloadColumn",
            command_args=command_args_template,
            duration_value=self.timeout,
            additional_stats=additional_stats,
            use_chunks=True,
            duration_param="--duration",
            nemesis=True
        )


class TestLogWorkload(LogWorkloadBase):
    timeout = int(get_external_param('workload_duration', 100))
```

### Пример 3: Workload с параметризацией (simple_queue)

```python
from enum import Enum

class TableType(str, Enum):
    """Тип таблицы"""
    ROW = 'row'
    COLUMN = 'column'


class SimpleQueueBase(WorkloadTestBase):
    workload_binary_name = 'simple_queue'
    workload_env_var = 'SIMPLE_QUEUE_BINARY'

    @pytest.mark.parametrize('table_type', [t.value for t in TableType])
    @pytest.mark.parametrize('nemesis_enabled', [True, False], 
                            ids=['nemesis_true', 'nemesis_false'])
    def test_workload_simple_queue(self, table_type: str, nemesis_enabled: bool):
        command_args_template = (
            f"--endpoint {YdbCluster.ydb_endpoint} "
            f"--database /{YdbCluster.ydb_database} "
            f"--mode {table_type}"
        )

        additional_stats = {
            "table_type": table_type,
            "workload_type": "simple_queue",
            "nemesis": nemesis_enabled,
            "nodes_percentage": 100
        }

        self.execute_workload_test(
            workload_name=f"SimpleQueue_{table_type}_nemesis_{nemesis_enabled}",
            command_args=command_args_template,
            duration_value=self.timeout,
            additional_stats=additional_stats,
            use_chunks=True,
            duration_param="--duration",
            nemesis=nemesis_enabled,
            nodes_percentage=100
        )


class TestSimpleQueue(SimpleQueueBase):
    timeout = int(get_external_param('workload_duration', 100))
```

### Пример 4: Topic workload с инициализацией

```python
class TopicWorkloadBase(WorkloadTestBase):
    workload_binary_name = "ydb_cli"
    workload_env_var = "YDB_CLI_BINARY"
    
    @classmethod
    def setup_class(cls):
        """Инициализация topic workload"""
        cls._init_topic()
        super().setup_class()
    
    @classmethod
    def _init_topic(cls):
        """Инициализация topic для workload"""
        from ydb.tests.olap.lib.ydb_cluster import YdbCluster
        
        first_node = list(YdbCluster.get_cluster_nodes(db_only=True))[0]
        
        init_cmd = [
            cls.workload_binary_name,
            '--verbose',
            '--endpoint', f'grpc://{first_node.host}:{first_node.ic_port}',
            '--database', '/Root/db1',
            'workload', 'topic', 'init',
            '-c', '50',  # consumers
            '-p', '100'  # partitions
        ]
        
        import yatest.common
        yatest.common.execute(init_cmd, wait=True, check_exit_code=False)
    
    def test_workload_topic(self):
        """Тест topic workload"""
        
        command_args_template = (
            f"--verbose --endpoint {YdbCluster.ydb_endpoint} --database /{YdbCluster.ydb_database} "
            "workload topic run full -s 60 --byte-rate 100M --use-tx "
            "--tx-commit-interval 2000 -p 100 -c 50"
        )
        
        additional_stats = {
            "workload_type": "topic",
            "operation": "full",
            "byte_rate": "100M",
            "use_tx": True,
            "tx_commit_interval": 2000,
            "partitions": 100,
            "consumers": 50
        }
        
        self.execute_workload_test(
            workload_name="TopicWorkload",
            command_args=command_args_template,
            duration_value=self.timeout,
            additional_stats=additional_stats,
            use_chunks=True,
            duration_param="--duration",
            nemesis=True
        )


class TestTopicWorkload(TopicWorkloadBase):
    timeout = int(get_external_param('workload_duration', 100))
```

### Пример 5: Кастомная логика статуса

```python
class CustomMetricsWorkloadBase(WorkloadTestBase):
    workload_binary_name = "olap_workload"
    workload_env_var = "OLAP_WORKLOAD_BINARY"
    
    def test_workload_custom_metrics(self):
        """Тест с кастомными метриками и проверками"""
        
        command_args_template = (
            f"--endpoint {YdbCluster.ydb_endpoint} "
            f"--database /{YdbCluster.ydb_database} "
            f"--path custom_metrics_test"
        )
        
        additional_stats = {
            "workload_type": "olap",
            "table_type": "column_store",
            "expected_success_rate": 0.95,
            "min_iterations": 5,
            "max_execution_time": 650
        }
        
        result = self.execute_workload_test(
            workload_name="CustomMetricsWorkload",
            command_args=command_args_template,
            duration_value=self.timeout,
            additional_stats=additional_stats,
            use_chunks=True,
            duration_param="--duration",
            nemesis=True
        )
        
        # Кастомная проверка метрик
        stats = result.get_stats("CustomMetricsWorkload")
        
        # Проверяем success rate
        if stats.get("success_rate", 0) < stats.get("expected_success_rate", 0.9):
            result.success = False
            result.add_error(f"Success rate {stats.get('success_rate', 0)} below expected {stats.get('expected_success_rate', 0.9)}")
        
        # Проверяем количество итераций
        if stats.get("total_iterations", 0) < stats.get("min_iterations", 1):
            result.success = False
            result.add_error(f"Total iterations {stats.get('total_iterations', 0)} below minimum {stats.get('min_iterations', 1)}")
        
        # Проверяем время выполнения
        if stats.get("actual_duration", 0) > stats.get("max_execution_time", 700):
            result.add_warning(f"Execution time {stats.get('actual_duration', 0)} exceeded maximum {stats.get('max_execution_time', 700)}")
        
        assert result.success, f"Custom metrics workload failed: {result.error_message}"
    
    def _handle_final_status(self, result, workload_name, node_errors):
        """Кастомная логика определения статуса с реальными метриками"""
        
        stats = result.get_stats(workload_name)
        
        # Проверяем кастомные метрики
        success_rate = stats.get("success_rate", 0)
        expected_rate = stats.get("expected_success_rate", 0.9)
        
        if success_rate < expected_rate * 0.8:  # Критично низкий success rate
            result.success = False
            result.add_error(f"Critical: Success rate {success_rate:.2f} too low (expected {expected_rate:.2f})")
            pytest.fail(f"Success rate critically low: {success_rate:.2f}")
        elif success_rate < expected_rate:  # Низкий, но не критичный
            result.add_warning(f"Warning: Success rate {success_rate:.2f} below expected {expected_rate:.2f}")
        
        # Проверяем количество итераций
        total_iterations = stats.get("total_iterations", 0)
        min_iterations = stats.get("min_iterations", 1)
        
        if total_iterations < min_iterations:
            result.success = False
            result.add_error(f"Too few iterations: {total_iterations} (minimum {min_iterations})")
            pytest.fail(f"Insufficient iterations: {total_iterations}")
        
        # Вызываем родительский метод для стандартных проверок
        super()._handle_final_status(result, workload_name, node_errors)


class TestCustomMetricsWorkload(CustomMetricsWorkloadBase):
    timeout = int(get_external_param('workload_duration', 100))
```

---

## Быстрый старт

### Шаблон для нового workload

1. **Скопируйте базовый шаблон:**
```python
from .workload_executor import WorkloadTestBase
from ydb.tests.olap.lib.ydb_cluster import YdbCluster
from ydb.tests.olap.lib.utils import get_external_param

import logging
LOGGER = logging.getLogger(__name__)


class MyWorkloadBase(WorkloadTestBase):
    workload_binary_name = 'my_workload'
    workload_env_var = 'MY_WORKLOAD_BINARY'

    def test_workload_my_workload(self):
        command_args_template = (
            f"--endpoint {YdbCluster.ydb_endpoint} "
            f"--database /{YdbCluster.ydb_database} "
            f"--path my_workload_test"
        )

        additional_stats = {
            "workload_type": "my_workload",
            "path": "my_workload_test"
        }

        self.execute_workload_test(
            workload_name="MyWorkload",
            command_args=command_args_template,
            duration_value=self.timeout,
            additional_stats=additional_stats,
            use_chunks=True,
            duration_param="--duration"
        )


class TestMyWorkload(MyWorkloadBase):
    timeout = int(get_external_param('workload_duration', 100))
```

2. **Добавьте инициализацию если нужно:**
```python
@classmethod
def setup_class(cls):
    cls._init_tables()  # Ваша инициализация
    super().setup_class()
```

3. **Настройте параметры:**
- Измените `workload_binary_name` и `workload_env_var`
- Настройте `command_args_template`
- Добавьте нужные поля в `additional_stats`

4. **Запустите тест:**
```bash
pytest ydb/tests/olap/load/lib/test_my_workload.py -v
``` 
A: Используйте параметр `nodes_percentage` (от 1 до 100) в `execute_workload_test()`. 