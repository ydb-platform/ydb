# Модули анализа ошибок

Набор модулей для анализа ошибок тестов: нормализация и классификация.

## Актуальные модули

### Основные модули

1. **`error_normalizer.py`** - Нормализация текстов ошибок
   - Удаляет динамические данные (UUID, даты, IP, пути, номера строк и т.д.)
   - Заменяет их на плейсхолдеры для группировки похожих ошибок
   - Функция: `normalize_error_text(text)`

2. **`error_classifier.py`** - Классификация ошибок по типам
   - Классифицирует на: `test_error`, `infrastructure_error`, `timeout`, `setup_error`, `teardown_error`
   - Использует правила с приоритетами для разрешения конфликтов
   - Функция: `classify_error(status_description, normalized_pattern)`

### Скрипты тестирования

3. **`test_normalization_on_real_data.py`** - Тестирование нормализации
   - Загружает данные из YDB
   - Применяет нормализацию
   - Сохраняет результаты в JSON

4. **`test_error_classification.py`** - Тестирование классификации
   - Классифицирует ошибки по типам
   - Выводит статистику по типам

### Вспомогательные скрипты

5. **`check_classifier_overlaps.py`** - Проверка пересечений в классификаторе
   - Проверяет конфликты в правилах
   - Тестирует приоритеты

## Использование

### Нормализация

```python
from error_normalizer import normalize_error_text

normalized = normalize_error_text("Test crashed (return code: -6)")
# Результат: "Test crashed (return code: <RETURN_CODE>)"
```

### Классификация

```python
from error_classifier import classify_error

error_type, confidence = classify_error(
    "Test crashed (return code: -6)",
    normalized_pattern="Test crashed (return code: <RETURN_CODE>)"
)
# Результат: ('test_error', 0.7)
```

## Зависимости

- `ydb_wrapper.py` - для работы с YDB (из `../analytics/`, используется в тестовых скриптах)
