# YDB Workload Tests - Руководство разработчика

## Содержание

1. [Добавление нового workload теста](#добавление-нового-workload-теста)
2. [Setup и Teardown](#setup-и-teardown)
3. [Генерация уникальных путей и нодо-специфичные endpoint'ы](#генерация-уникальных-путей-и-нодо-специфичные-endpointы)
4. [Механика работы Chunks (Итераций)](#механика-работы-chunks-итераций)
5. [Nemesis (Chaos Testing)](#nemesis-chaos-testing)
6. [Настройка полей для выгрузки в YDB](#настройка-полей-для-выгрузки-в-ydb)
7. [Определение статуса теста](#определение-статуса-теста)
8. [Добавление функций, выполняемых перед workload](#добавление-функций-выполняемых-перед-workload)
9. [Примеры workload'ов разной сложности](#примеры-wokrloadов-разной-сложности)
10. [Примеры использования](#примеры-использования)
11. [Структура файлов и ya.make](#структура-файлов-и-yamake)
12. [Запуск тестов](#запуск-тестов)

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

### 2. Создание wrapper файла

Создайте файл `ydb/tests/olap/load/test_my_workload.py`:

```python
from .lib.workload_my_workload import TestMyWorkload

# Тест автоматически будет импортирован и запущен
```

---

## Setup и Teardown

### Автоматические действия

**Setup (setup_class):**
- Подготовка workload бинарных файлов
- Копирование файлов на ноды кластера
- Инициализация окружения

**Подготовка к каждому тесту (_prepare_workload_execution):**
- **Остановка nemesis** - останавливает nemesis сервис на всех нодах для чистого старта каждого теста
- Сохранение состояния нод для диагностики
- Деплой workload бинарных файлов на ноды
- Создание плана выполнения

**Teardown (teardown_class):**
- Остановка nemesis (если был запущен)
- Остановка процессов workload на всех нодах
- Очистка временных файлов
- Сбор диагностики

### Когда nemesis включается

Nemesis автоматически включается при:
- Передаче параметра `nemesis=True` в `execute_workload_test()`
- Наличии файла конфигурации nemesis в `cluster_path`

### Поведение по умолчанию

```python
# По умолчанию:
# - nemesis = False (не запускается)
# - nodes_percentage = 100 (все ноды)
# - use_chunks = False (без разбивки на итерации)
# - duration_param = "--duration"

# Автоматические действия:
# - В setup: подготовка файлов, копирование на ноды
# - В preparation: остановка nemesis, деплой бинарных файлов, создание плана выполнения
# - В teardown: остановка nemesis, остановка workload процессов, очистка, сбор диагностики
```

---

## Генерация уникальных путей и нодо-специфичные endpoint'ы

### **Проблема: Конфликты при параллельном выполнении**

При параллельном выполнении workload на разных нодах и итерациях может возникнуть конфликт, если все запуски используют одинаковый путь. Это приводит к:

- **Конфликтам данных** - разные workload'ы записывают в одну таблицу
- **Ошибкам блокировок** - одновременный доступ к одним ресурсам
- **Непредсказуемым результатам** - данные смешиваются между запусками

### **Решение: Подстановка переменных в command_args_template**

Система поддерживает подстановку переменных в `command_args_template`, что позволяет:
- Генерировать уникальные пути для каждого инстанса workload
- Использовать нодо-специфичные endpoint'ы вместо глобального
- Создавать изолированные соединения для каждого потока

#### **Поддерживаемые переменные:**

| Переменная | Описание | Пример значения |
|------------|----------|-----------------|
| `{node_host}` | Хост ноды | `ydb-sas-testing-0000.search.yandex.net` |
| `{iteration_num}` | Номер итерации | `1`, `2`, `3`, ... |
| `{thread_id}` | ID потока (обычно хост ноды) | `ydb-sas-testing-0000.search.yandex.net` |
| `{run_id}` | Уникальный ID запуска | `ydb-sas-testing-0000_1_1703123456` |
| `{timestamp}` | Timestamp запуска | `1703123456` |
| `{uuid}` | Короткий UUID (8 символов) | `abc12345` |

#### **Примеры использования:**

```python
def test_workload_with_variables(self, nemesis_enabled: bool):
    """Тест с подстановкой переменных для уникальных путей и endpoint'ов"""
    
    # 1. Глобальный endpoint + уникальные пути
    command_args_template = (
        f"--endpoint {YdbCluster.ydb_endpoint} "  # Глобальный endpoint
        f"--database /{YdbCluster.ydb_database} "
        "--path workload_{node_host}_iter_{iteration_num}_{uuid}"  # Уникальный путь
    )
    
    # 2. Нодо-специфичные endpoint'ы + уникальные пути
    command_args_template = (
        "--endpoint grpc://{node_host}:2135 "  # Подключаемся к конкретной ноде
        f"--database /{YdbCluster.ydb_database} "
        "--path workload_node_{node_host}_iter_{iteration_num}_{uuid}"  # Уникальный путь
    )
    
    # 3. Комбинированный подход
    command_args_template = (
        "--endpoint grpc://{node_host}:2135 "  # Нодо-специфичный endpoint
        f"--database /{YdbCluster.ydb_database} "
        "--path workload_{node_host}_iter_{iteration_num}_{uuid} "
        "--thread-id {thread_id} "  # Идентификация потока
        "--run-id {run_id}"  # Уникальность запуска
    )
    
    additional_stats = {
        "workload_type": "my_workload",
        "path_template": "workload_{node_host}_iter_{iteration_num}_{uuid}",
        "endpoint_method": "node_specific",  # или "global"
        "nemesis": nemesis_enabled,
        "path_generation_method": "variable_substitution",
    }
    
    self.execute_workload_test(
        workload_name="MyWorkloadWithVariables",
        command_args=command_args_template,
        duration_value=self.timeout,
        additional_stats=additional_stats,
        use_chunks=True,
        nemesis=nemesis_enabled
    )
```

#### **Результат выполнения:**

```
# Глобальный endpoint:
Нода 1: --endpoint grpc://lb.ydb-cluster.yandex.net:2135 --path workload_ydb-sas-testing-0000.search.yandex.net_iter_1_abc12345
Нода 2: --endpoint grpc://lb.ydb-cluster.yandex.net:2135 --path workload_ydb-sas-testing-0001.search.yandex.net_iter_1_def67890

# Нодо-специфичные endpoint'ы:
Нода 1: --endpoint grpc://ydb-sas-testing-0000.search.yandex.net:2135 --path workload_node_ydb-sas-testing-0000.search.yandex.net_iter_1_abc12345
Нода 2: --endpoint grpc://ydb-sas-testing-0001.search.yandex.net:2135 --path workload_node_ydb-sas-testing-0001.search.yandex.net_iter_1_def67890
```

#### **Преимущества подстановки переменных:**

- ✅ **Полная уникальность** - каждый инстанс имеет уникальный путь
- ✅ **Локальные соединения** - можно подключаться к конкретным нодам
- ✅ **Изоляция нагрузки** - равномерное распределение по нодам
- ✅ **Лучшая диагностика** - проще отследить проблемы конкретной ноды
- ✅ **Гибкость** - можно комбинировать глобальные и нодо-специфичные параметры

#### **Сценарии использования:**

| Сценарий | Endpoint | Путь | Применение |
|----------|----------|------|------------|
| **Стандартное тестирование** | `{YdbCluster.ydb_endpoint}` | `workload_{node_host}_iter_{iteration_num}_{uuid}` | Обычные тесты |
| **Локальные соединения** | `grpc://{node_host}:2135` | `workload_{node_host}_iter_{iteration_num}_{uuid}` | Нагрузочное тестирование |
| **Тестирование балансировщика** | `{YdbCluster.ydb_endpoint}` | `workload_{node_host}_iter_{iteration_num}_{uuid}` | Тестирование балансировки |
| **Сетевая диагностика** | `grpc://{node_host}:2135` | `workload_{node_host}_iter_{iteration_num}_{uuid}` | Диагностика сетевых проблем |

#### **Что НЕ нужно делать:**

```python
# ❌ НЕПРАВИЛЬНО: Статический путь
command_args_template = (
    f"--endpoint {YdbCluster.ydb_endpoint} "
    f"--database /{YdbCluster.ydb_database} "
    "--path my_workload_test"  # Конфликт при параллельном выполнении!
)

# ❌ НЕПРАВИЛЬНО: Неопределенная переменная в f-string
command_args_template = (
    f"--endpoint grpc://{node_host}:2135 "  # node_host не определен в f-string!
    f"--database /{YdbCluster.ydb_database} "
    "--path workload_test"
)

# ✅ ПРАВИЛЬНО: Использование плейсхолдеров
command_args_template = (
    "--endpoint grpc://{node_host}:2135 "  # Плейсхолдер подставляется позже
    f"--database /{YdbCluster.ydb_database} "
    "--path workload_{node_host}_iter_{iteration_num}_{uuid}"
)
```

#### **Рекомендации:**

1. **Для production тестов**: Используйте `{node_host}_iter_{iteration_num}_{uuid}` в путях
2. **Для нагрузочного тестирования**: Используйте нодо-специфичные endpoint'ы `grpc://{node_host}:2135`
3. **Для диагностики**: Добавьте `{timestamp}` для временной привязки
4. **Для изоляции**: Всегда используйте `{uuid}` для гарантированной уникальности
5. **Документируйте метод**: Указывайте `path_generation_method` и `endpoint_method` в `additional_stats`

---

## Механика работы Chunks (Итераций)

### **Что такое chunks и зачем они нужны?**

**Chunks (итерации)** - это механизм разбиения длительного workload теста на несколько коротких итераций для:

1. **Улучшения диагностики** - каждая итерация выполняется отдельно, что позволяет точно определить, на каком этапе произошла ошибка
2. **Снижения риска потери данных** - если одна итерация упадет, остальные продолжат работу
3. **Более точной статистики** - можно анализировать производительность по каждой итерации отдельно
4. **Лучшего контроля ресурсов** - каждая итерация имеет свой таймаут и может быть остановлена независимо

### **Как работает разбивка на итерации:**

```python
# Пример: тест на 1800 секунд (30 минут) с use_chunks=True
duration_value = 1800  # 30 минут

# Система автоматически разобьет на итерации:
# - Минимум 100 секунд на итерацию
# - Максимум 1 час на итерацию  
# - Если общее время < 200 сек - одна итерация
# - Размер итерации = min(max(total_duration // 10, 100), 3600)

# Для 1800 секунд получится:
# iteration_size = min(max(1800 // 10, 100), 3600) = min(max(180, 100), 3600) = 180 секунд
# Количество итераций = 1800 / 180 = 10 итераций по 3 минуты каждая
```

### **Логика создания итераций:**

```python
def _create_chunks_plan(self, total_duration: float):
    # Вычисляем размер итерации: минимум 100 сек, максимум 1 час
    iteration_size = min(max(total_duration // 10, 100), 3600)
    
    # Если общее время меньше 200 сек - используем одну итерацию
    if total_duration < 200:
        iteration_size = total_duration

    # Создаем итерации
    iterations = []
    remaining_time = total_duration
    iteration_num = 1

    while remaining_time > 0:
        current_iteration_size = min(iteration_size, remaining_time)
        iterations.append({
            'iteration_num': iteration_num,
            'duration': current_iteration_size,
            'start_offset': total_duration - remaining_time
        })
        remaining_time -= current_iteration_size
        iteration_num += 1

    return iterations
```

### **Примеры разбивки:**

| Общее время | Размер итерации | Количество итераций | Примечание |
|-------------|-----------------|---------------------|------------|
| 60 сек | 60 сек | 1 | Одна итерация (меньше 200 сек) |
| 300 сек | 100 сек | 3 | Минимальный размер 100 сек |
| 1800 сек | 180 сек | 10 | Стандартная разбивка |
| 7200 сек | 720 сек | 10 | Максимальный размер 1 час |
| 14400 сек | 3600 сек | 4 | Максимальный размер 1 час |

### **Параллельное выполнение итераций:**

```python
# Каждая итерация выполняется параллельно на всех нодах кластера
# Пример: 10 итераций на 8 нодах = 80 параллельных запусков

# Структура выполнения:
# Итерация 1: [Нода1, Нода2, Нода3, ..., Нода8] - параллельно
# Итерация 2: [Нода1, Нода2, Нода3, ..., Нода8] - параллельно  
# ...
# Итерация 10: [Нода1, Нода2, Нода3, ..., Нода8] - параллельно
```

### **Анализ результатов итераций:**

```python
# Система группирует результаты по номеру итерации
# Успешность итерации = хотя бы один поток в итерации завершился успешно

# Статистика в YDB:
# - total_iterations: общее количество итераций
# - successful_iterations: количество успешных итераций  
# - failed_iterations: количество неуспешных итераций
# - total_threads: общее количество потоков (нод × итерации)
# - avg_threads_per_iteration: среднее количество потоков на итерацию
```

### **Когда использовать chunks:**

```python
# ✅ Рекомендуется для длительных тестов (> 5 минут)
use_chunks=True  # Для тестов на 30+ минут

# ✅ Рекомендуется для stress тестов
use_chunks=True  # Лучшая диагностика при нагрузке

# ✅ Рекомендуется для тестов с nemesis
use_chunks=True  # Изоляция эффектов chaos testing

# ❌ Не нужно для коротких тестов (< 2 минут)
use_chunks=False  # Для быстрых smoke тестов

# ❌ Не нужно для простых проверок
use_chunks=False  # Для простых функциональных тестов
```

### **Пример использования:**

```python
def test_long_running_workload(self):
    """Длительный тест с разбивкой на итерации"""
    
    result = self.execute_workload_test(
        workload_name="LongRunningWorkload",
        command_args="--database /Root/db1 --path test_table",
        duration_value=3600,  # 1 час
        use_chunks=True,  # Разбить на итерации
        additional_stats={
            "workload_type": "stress",
            "expected_iterations": 6,  # 3600 / 600 = 6 итераций
            "iteration_duration": 600  # 10 минут на итерацию
        }
    )
    
    # Проверяем результаты итераций
    stats = result.get_stats("LongRunningWorkload")
    assert stats["total_iterations"] >= 5, "Недостаточно итераций"
    assert stats["successful_iterations"] >= 4, "Слишком много неуспешных итераций"
```

### **Отображение в Allure отчетах:**

```python
# В Allure отчете каждая итерация отображается отдельно:
# 
# Workload Iterations
# ┌─────┬────────┬───────┬─────────────┬─────────────┬─────────────┐
# │ Iter│ Status │ Dur(s)│   Нода1     │   Нода2     │   Нода3     │
# ├─────┼────────┼───────┼─────────────┼─────────────┼─────────────┤
# │  1  │   ok   │ 180.0 │     ok      │     ok      │     ok      │
# │  2  │ warning│ 180.0 │     ok      │   timeout   │     ok      │
# │  3  │   ok   │ 180.0 │     ok      │     ok      │     ok      │
# └─────┴────────┴───────┴─────────────┴─────────────┴─────────────┘
#
# Где:
# - Iter: номер итерации
# - Status: статус итерации (ok/warning/error)
# - Dur(s): продолжительность итерации
# - НодаX: статус выполнения на конкретной ноде
```

### **Статистика в YDB:**

```python
# При загрузке в YDB создается детальная статистика:
{
    "total_iterations": 10,           # Общее количество итераций
    "successful_iterations": 8,       # Успешные итерации
    "failed_iterations": 2,           # Неуспешные итерации
    "total_threads": 80,              # Общее количество потоков (8 нод × 10 итераций)
    "avg_threads_per_iteration": 8.0, # Среднее количество потоков на итерацию
    "use_iterations": true,           # Флаг использования итераций
    "actual_duration": 175.5,         # Фактическое время выполнения (среднее)
    "planned_duration": 180.0         # Плановое время выполнения
}
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

### Таймлайн работы с nemesis

```
Время    | Действие                                    | Описание
---------|---------------------------------------------|----------------------------------------
T-0s     | Setup class                                | Инициализация теста
T+0s     | Запуск workload теста                      | Начинается выполнение workload
T+0s     | ФАЗА 1: Подготовка                         | Остановка nemesis, деплой workload бинарных файлов на ноды
T+0s     | Остановка nemesis в preparation            | Nemesis останавливается на всех нодах для чистого старта
T+0s     | ФАЗА 2: Выполнение workload                | Workload начинает выполняться на нодах
T+15s    | Запуск nemesis                             | Nemesis стартует через 15 секунд после начала workload
T+15s    | Подготовка конфигурации кластера           | Удаление старого cluster.yaml, копирование нового
T+15s    | Деплой конфигурации кластера               | Файл конфигурации кластера копируется на все ноды
T+15s    | Начало chaos testing                       | Nemesis начинает выполнять операции согласно конфигурации
T+15s    | Workload продолжает работу                 | Основной workload работает параллельно с nemesis
T+15s    | Nemesis выполняет операции                 | Случайные операции: паузы, перезапуски, сетевые проблемы и т.д.
...      | Параллельная работа                        | Workload и nemesis работают одновременно
T+N-15s  | Завершение workload                        | Workload завершает выполнение
T+N-15s  | Остановка nemesis                          | Nemesis автоматически останавливается на всех нодах
T+N-15s  | Сбор результатов workload                  | Собираются результаты и логи workload
T+N-15s  | Сбор диагностики nemesis                   | Логи и диагностика nemesis с каждой ноды
T+N-15s  | Анализ результатов                         | Определение статуса теста на основе всех данных
T+N      | Завершение теста                           | Тест завершен, результаты готовы
```

### Как работает nemesis

1. **Preparation** - в ФАЗЕ 1 останавливается nemesis на всех нодах для чистого старта
2. **Деплой workload** - в ФАЗЕ 1 происходит деплой workload бинарных файлов на ноды
3. **Запуск nemesis** - через 15 секунд после начала выполнения workload
4. **Деплой конфигурации** - при запуске nemesis копируется конфигурация кластера на все ноды
5. **Параллельность** - операции выполняются параллельно на всех нодах
6. **Автоочистка** - nemesis автоматически останавливается в teardown_class

### Параметры nemesis

- `nemesis=True` - включает chaos testing
- `cluster_path` - путь к файлу конфигурации кластера (копируется как есть, без изменений)

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

## Примеры wokrloadов разной сложности

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

## Запуск тестов

### Структура файлов
```
ydb/tests/olap/load/
├── lib/
│ ├── workload_simple_queue.py # Реализация теста
│ ├── workload_olap.py # Реализация теста
│ ├── workload_executor.py # Базовый класс
│ └── ...
├── test_workload_simple_queue.py # Wrapper для pytest
├── test_workload_olap.py # Wrapper для pytest
├── ya.make # Основной ya.make
└── README.md # Это руководство
```

### **Примеры запуска теста**

#### **Вариант 1: Без выгрузки результатов в YDB**
```bash
./ya make -ttt \
  --test-param workload_duration=500 \
  --test-param ydb-db=/Root/db1 \
  --test-param ydb-endpoint=grpc://localhost:2135 \
  ydb/tests/olap/load \
  --test-filter test_workload_simple_queue.py::TestSimpleQueue::test_workload_simple_queue[nemesis_true*] \
  --allure=./allure_tmp \
  --test-tag ya:manual \
  --test-param cluster_path=cluster.yaml
```

#### **Вариант 2: С выгрузкой результатов в YDB**
```bash
# Установите переменную окружения для аутентификации
export RESULT_IAM_FILE_0=iam.json

./ya make -ttt \
  --test-param workload_duration=500 \
  --test-param ydb-db=/Root/db1 \
  --test-param ydb-endpoint=grpc://localhost:2135 \
  ydb/tests/olap/load \
  --test-filter test_workload_simple_queue.py::TestSimpleQueue::test_workload_simple_queue[nemesis_true*] \
  --allure=./allure_tmp \
  --test-tag ya:manual \
  --test-param send-results=true \
  --test-param results-endpoint=grpcs://lb.etnvsjbk7kh1jc6bbfi8.ydb.mdb.yandexcloud.net:2135 \
  --test-param results-db=/ru-central1/b1ggceeul2pkher8vhb6/etnvsjbk7kh1jc6bbfi8 \
  --test-param results-table=nemesis/tests_results \
  --test-param cluster_path=cluster.yaml
```

**Параметры запуска:**
- `workload_duration=500` - длительность теста в секундах
- `ydb-db=/Root/db1` - база данных для тестирования
- `ydb-endpoint=grpc://localhost:2135` - endpoint YDB кластера
- `--test-filter` - фильтр для запуска конкретного теста
- `--allure=./allure_tmp` - директория для Allure отчетов
- `cluster_path=$CROSS` - **конфигурация кластера для nemesis (требуется для chaos testing)**

**Параметры выгрузки в YDB (только для Варианта 2):**
- `send-results=true` - включить выгрузку результатов в YDB
- `results-endpoint=grpcs://lb.etnvsjbk7kh1jc6bbfi8.ydb.mdb.yandexcloud.net:2135` - endpoint для выгрузки результатов
- `results-db=/ru-central1/b1ggceeul2pkher8vhb6/etnvsjbk7kh1jc6bbfi8` - база данных для результатов
- `results-table=nemesis/tests_results` - таблица для результатов. Должна быть создана заранее

 
    Требуется `export RESULT_IAM_FILE_0=iam.json` - **переменная окружения с IAM файлом для аутентификации**

### **Параметры execute_workload_test()**

| Параметр | Тип | По умолчанию | Описание |
|----------|-----|--------------|----------|
| `workload_name` | str | - | Имя workload для отчетов и статистики |
| `command_args` | str | - | Аргументы командной строки (может быть шаблоном) |
| `duration_value` | float | `self.timeout` | Время выполнения в секундах |
| `additional_stats` | dict | `{}` | Дополнительная статистика для выгрузки в YDB |
| `use_chunks` | bool | `False` | **Использовать ли разбивку на итерации** |
| `duration_param` | str | `"--duration"` | Параметр для передачи времени выполнения |
| `nemesis` | bool | `False` | Запускать ли сервис nemesis через 15 секунд |
| `nodes_percentage` | int | `100` | Процент нод кластера для запуска workload (1-100) |

---