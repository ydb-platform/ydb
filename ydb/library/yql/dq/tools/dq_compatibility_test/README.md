# DQ Compatibility Test Tool

Утилита для тестирования совместимости бинарных запросов YDB между разными версиями с полноценным task runner.

## Сборка

```bash
ya make tools/dq_compatibility_test
```

Будут собраны две утилиты:
- `dq_compatibility_test/main/main` - основная утилита с полноценным task runner (prepare + run)
- `dq_compatibility_test/serializer/dq_task_serializer` - утилита для создания тестовых сериализованных задач

## Использование

### 1. Создание тестовой задачи

```bash
# Создать тестовую задачу
./tools/dq_compatibility_test/serializer/dq_task_serializer -o test_task.bin -v

# Создать задачу с кастомными параметрами
./tools/dq_compatibility_test/serializer/dq_task_serializer -o task_123.bin --task-id 123 --stage-id 5 -v
```

### 2. Проверка совместимости

```bash
# Базовая проверка (только парсинг и валидация)
./tools/dq_compatibility_test/main/main -i test_task.bin -v

# Полная проверка с выполнением task runner (prepare + run)
./tools/dq_compatibility_test/main/main -i test_task.bin -r -v
```

### 3. Логирование

```bash
# Записать логи в файл
./tools/dq_compatibility_test/main/main -i test_task.bin -r -v -l compatibility.log
```

## Флаги

### main/main (основная утилита)

- `-i, --input FILE` - входной файл с сериализованной задачей (обязательно)
- `-l, --log FILE` - файл для записи логов (опционально)
- `-v, --verbose` - подробный вывод
- `-r, --run` - выполнить задачу через task runner (prepare + run)

### serializer/dq_task_serializer

- `-o, --output FILE` - выходной файл для сериализованной задачи (обязательно)
- `-p, --program DATA` - данные программы (опционально, по умолчанию тестовые данные)
- `--task-id ID` - ID задачи (по умолчанию 1)
- `--stage-id ID` - ID стадии (по умолчанию 1)
- `-v, --verbose` - подробный вывод

## Примеры тестирования совместимости

### Сценарий 1: Тестирование между версиями

1. На версии YDB A:
```bash
./tools/dq_compatibility_test/serializer/dq_task_serializer -o task_version_A.bin --task-id 1001 -v
```

2. На версии YDB B:
```bash
./tools/dq_compatibility_test/main/main -i task_version_A.bin -r -v
```

## Возможные результаты

- **Код 0**: Задача совместима
- **Код 1**: Задача несовместима или произошла ошибка

## Уровни совместимости

1. **Структурная совместимость**: Задача может быть распарсена и имеет правильную структуру (без флага `-r`)
2. **Выполнительная совместимость**: Задача может быть подготовлена и выполнена через task runner (с флагом `-r`)

## Функциональность

Утилита выполняет следующие этапы:

1. **Парсинг**: Читает и парсит protobuf сообщение TDqTask
2. **Валидация**: Проверяет базовую структуру задачи
3. **Создание контекста**: Инициализирует TDqTaskRunnerContext и настройки (при -r)
4. **Prepare**: Вызывает taskRunner->Prepare() для подготовки задачи (при -r)
5. **Run**: Выполняет задачу через taskRunner->Run() в цикле до завершения (при -r)

## Технические детали

- Полная интеграция с TDqTaskRunner
- Использует TDqTaskRunnerContext, TDqTaskRunnerSettings, TDqTaskRunnerMemoryLimits
- Реализует TDqTaskRunnerExecutionContextDefault для контекста выполнения
- Создает TScopedAlloc с правильным source location
- Обрабатывает ERunStatus состояния (Finished, PendingInput, PendingOutput)
- Лимиты памяти: 1MB для буферов и чанков
- Режим базовой статистики включен
- LLVM оптимизации отключены для максимальной совместимости
- Защита от бесконечных циклов (максимум 1000 итераций) 