# DQ Compatibility Test Tool

Утилита для тестирования совместимости бинарных запросов YDB между разными версиями.

## Сборка

```bash
ya make tools/dq_compatibility_test
```

Будут собраны две утилиты:
- `dq_compatibility_test/dq_compatibility_test` - основная утилита с полноценным task runner
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
./tools/dq_compatibility_test/dq_compatibility_test -i test_task.bin -v

# Полная проверка с выполнением task runner
./tools/dq_compatibility_test/dq_compatibility_test -i test_task.bin -r -v
```

### 3. Логирование

```bash
# Записать логи в файл
./tools/dq_compatibility_test/dq_compatibility_test -i test_task.bin -r -v -l compatibility.log
```

## Флаги

### dq_compatibility_test

- `-i, --input FILE` - входной файл с сериализованной задачей (обязательно)
- `-l, --log FILE` - файл для записи логов (опционально)
- `-v, --verbose` - подробный вывод
- `-r, --run` - выполнить задачу через task runner (не только валидировать)

### dq_task_serializer

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
./tools/dq_compatibility_test/dq_compatibility_test -i task_version_A.bin -r -v
```

### Сценарий 2: Интеграция в существующий код

Используйте функции из `integration_example.cpp` для сохранения реальных задач из вашего кода YDB.

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
3. **Создание контекста**: Инициализирует TDqTaskRunnerContext и настройки
4. **Prepare**: Вызывает taskRunner->Prepare() для подготовки задачи
5. **Run**: Выполняет задачу через taskRunner->Run() в цикле до завершения

## Примечания

- Утилита отключает LLVM оптимизации для максимальной совместимости
- Используется базовый режим статистики
- Есть защита от бесконечных циклов (максимум 1000 итераций)
- При возникновении ошибок компиляции из-за проблем с override в MiniKQL, используйте флаги `-w` для подавления предупреждений 