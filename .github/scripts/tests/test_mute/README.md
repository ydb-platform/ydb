# Тесты для системы мьютинга YDB

Эта папка содержит организованную структуру тестов для системы мьютинга тестов YDB.

> ⚠️ **ВАЖНО**: Тесты `test_mute_rules*.py` требуют моки и запускаются ТОЛЬКО через `python run_all_tests.py --mute-rules-only`!

## Структура

```
test_mute/
├── mock_setup.py                           # Общие моки для всех тестов
├── run_all_tests.py                        # Главный файл для запуска всех тестов
├── README.md                               # Этот файл
├── README_mute_rules.md                    # 📖 Подробная документация правил мьютинга
├── test_mute_rules.py                      # Основные тесты правил мьютинга
├── test_mute_rules_detailed.py             # Детальные тесты граничных условий
├── test_mute_rules_summary.py              # Комплексные тесты всех правил
├── test_module_transform_ya_junit/         # Тесты для transform_ya_junit.py
│   ├── __init__.py
│   └── test_ya_mute_check.py              # Тесты для класса YaMuteCheck
├── test_module_create_new_muted_ya/        # Тесты для create_new_muted_ya.py
│   ├── __init__.py
│   ├── test_update_muted_ya.py            # Тесты для опции update_muted_ya
│   └── test_create_issues.py              # Тесты для создания issues
├── test_module_update_mute_issues/         # Тесты для update_mute_issues.py
│   ├── __init__.py
│   └── test_issues_management.py          # Тесты для управления issues
└── test_module_formatting/                 # Тесты для форматирования
    ├── __init__.py
    └── test_output_formatting.py          # Тесты для форматирования вывода
```

## Модули тестирования

### test_mute_rules (НОВЫЕ ТЕСТЫ)

> 📖 **Подробная документация**: [README_mute_rules.md](README_mute_rules.md) - полное описание всех правил мьютинга

Тестирует **текущие правила мьютинга** в `create_new_muted_ya.py`:
- **Основные тесты** (`test_mute_rules.py`) - базовая функциональность всех правил
- **Детальные тесты** (`test_mute_rules_detailed.py`) - граничные условия и edge cases
- **Комплексные тесты** (`test_mute_rules_summary.py`) - валидация всех правил вместе

**Покрываемые правила:**
- ✅ **EXCLUSION RULES**: `deleted_today`, `muted_stable_n_days_today`
- ✅ **FLAKY RULES**: все 5 условий (flaky_today, days_in_state≥1, runs≥2, fails≥2, fail_rate>0.2)
- ✅ **MUTE CHECK**: `mute_check()` функция
- ✅ **REGEX TRANSFORMATION**: паттерн `\d+/(\d+)\]` → `*/*]`
- ✅ **BOUNDARY CONDITIONS**: точно 80% порог, минимальные требования
- ✅ **PRIORITY RULES**: приоритет exclusion > inclusion правил

**Цель**: Создать baseline для безопасного изменения правил мьютинга

### test_module_transform_ya_junit

Тестирует:
- `YaMuteCheck` класс для проверки мьют-паттернов
- Загрузку правил из файла
- Сопоставление тестов с паттернами
- Обработку некорректных регулярных выражений
- Пустые инициализации

### test_module_create_new_muted_ya

Тестирует:
- **update_muted_ya опцию** - создание файлов мьютинга
- Фильтрацию флаки тестов
- Создание различных категорий файлов (deleted, flaky, stable)
- Замену wildcards в именах тестов
- Отслеживание размьюченных тестов
- **create_issues функциональность** - создание GitHub issues
- Группировку тестов по командам
- Разбиение больших групп на chunks
- Обработку уже существующих issues
- **Исторические данные** - обработка данных за несколько дней (test_historical_data.py)
- **SQL запросы** - модификации запросов для исторических данных (test_historical_query.py, test_sql_modifications.py)
- **Тренд анализ** - выявление деградации/улучшения тестов во времени
- **Граничные условия** - детальное тестирование всех граничных значений (test_boundary_conditions.py)

### test_module_update_mute_issues

Тестирует:
- Получение GitHub репозитория
- Извлечение замьюченных тестов из issues
- Закрытие полностью размьюченных issues
- Обработку частично размьюченных issues
- Создание новых issues и добавление в проект
- Генерацию заголовков и тел issues
- Парсинг тестов из тел issues
- Обработку ошибок API

### test_module_formatting

Тестирует:
- Форматирование закрытых issues
- Форматирование частично размьюченных issues
- Форматирование созданных issues
- Все секции одновременно
- Группировку и сортировку по командам
- Отступы между группами команд

## Запуск тестов

### Все тесты сразу
```bash
cd .github/scripts/tests/test_mute
python run_all_tests.py
```

### Только тесты mute rules
```bash
cd .github/scripts/tests/test_mute
python run_all_tests.py --mute-rules-only
```

### Отдельные модули
```bash
cd .github/scripts/tests/test_mute

# Тесты правил мьютинга (НОВЫЕ) - ТОЛЬКО через run_all_tests.py (из-за моков)
python run_all_tests.py --mute-rules-only

# Тесты YaMuteCheck
python -m unittest test_module_transform_ya_junit.test_ya_mute_check

# Тесты update_muted_ya
python -m unittest test_module_create_new_muted_ya.test_update_muted_ya

# Тесты create_issues
python -m unittest test_module_create_new_muted_ya.test_create_issues

# Тесты исторических данных (НОВЫЕ)
python -m unittest test_module_create_new_muted_ya.test_historical_data
python -m unittest test_module_create_new_muted_ya.test_historical_query
python -m unittest test_module_create_new_muted_ya.test_sql_modifications

# Тесты граничных условий (НОВЫЕ)
python -m unittest test_module_create_new_muted_ya.test_boundary_conditions

# Тесты управления issues
python -m unittest test_module_update_mute_issues.test_issues_management

# Тесты форматирования
python -m unittest test_module_formatting.test_output_formatting
```

### Отдельные тесты
```bash
cd .github/scripts/tests/test_mute

# ❌ Конкретные тесты mute rules НЕ РАБОТАЮТ напрямую (нужны моки)
# python -m unittest test_mute_rules.TestMuteRules.test_flaky_test_rule_all_conditions_met  # НЕ РАБОТАЕТ!

# ✅ Для mute rules используйте только:
python run_all_tests.py --mute-rules-only

# ✅ Конкретные тесты из других модулей (работают напрямую):
python -m unittest test_module_transform_ya_junit.test_ya_mute_check.TestYaMuteCheck.test_basic_functionality
```

## Покрытие

### Функциональность покрыта тестами:

✅ **Mute Rules (НОВЫЕ)** - все текущие правила мьютинга, граничные условия, приоритеты
✅ **YaMuteCheck** - паттерн матчинг, загрузка файлов, обработка ошибок
✅ **update_muted_ya** - создание файлов мьютинга, фильтрация, категоризация
✅ **create_issues** - создание GitHub issues, группировка команд, chunking
✅ **update_mute_issues** - управление issues, парсинг, API взаимодействие
✅ **Форматирование** - все секции вывода, группировка, отступы
✅ **Исторические данные (НОВЫЕ)** - обработка данных за несколько дней, тренд анализ, SQL модификации
✅ **Граничные условия (НОВЫЕ)** - все граничные значения в SQL и фильтрации (>, >=, =, строгие равенства)

### Области требующие дополнительного тестирования:

⚠️ **Интеграционные тесты** - взаимодействие между модулями
⚠️ **Большие наборы данных** - производительность с тысячами тестов
⚠️ **Сетевые ошибки** - обработка нестабильности GitHub API
⚠️ **Конфигурация** - различные конфигурации YDB и GitHub
⚠️ **Реальные данные** - тесты mute rules пока используют только мок данные

## Моки

Все тесты используют общую систему моков из `mock_setup.py`:
- `ydb` - база данных
- `requests` - HTTP запросы  
- `github` - GitHub API
- `configparser` - конфигурационные файлы
- Переменные окружения

### ⚠️ Важно про mute rules тесты:

**Тесты `test_mute_rules*.py` ТРЕБУЮТ моки** и не могут запускаться напрямую через `python -m unittest`!

**Причина**: Они импортируют `create_new_muted_ya`, который требует модули `requests`, `ydb`, `github`, которых нет в окружении.

**Решение**: Используйте только `python run_all_tests.py --mute-rules-only`

**Моки настраиваются в `run_all_tests.py`**:
```python
from mock_setup import setup_mocks
setup_mocks()  # Должно быть ДО импорта любых модулей
```

**Альтернативный способ (если нужно запускать mute rules тесты напрямую)**:
```python
# Добавить в начало каждого test_mute_rules*.py файла:
import sys
import os
sys.path.insert(0, os.path.dirname(__file__))
from mock_setup import setup_mocks
setup_mocks()
```

## Модификация правил мьютинга

Новые тесты `test_mute_rules*.py` создают **baseline** для безопасного изменения правил мьютинга:

### Рекомендуемый процесс изменения правил:

1. **Запустите baseline тесты**:
   ```bash
   python run_all_tests.py --mute-rules-only
   ```

2. **Изучите документацию**: [README_mute_rules.md](README_mute_rules.md) содержит все текущие правила

3. **Модифицируйте тесты** под новые правила (сначала тесты, потом код!)

4. **Измените код** в `create_new_muted_ya.py`

5. **Проверьте что тесты проходят** после изменений

### Текущие правила (baseline):
- **EXCLUSION**: `deleted_today` > `muted_stable_n_days_today` > остальные
- **FLAKY**: 5 условий одновременно (flaky_today, days≥1, runs≥2, fails≥2, fail_rate>0.2)
- **THRESHOLD**: 80% порог (fail_rate > 0.2, НЕ ≥ 0.2)
- **REGEX**: `\d+/(\d+)\]` → `*/*]` для chunk тестов

## Примечания

1. Все тесты изолированы и не требуют внешних зависимостей
2. Используется подробное логирование для отладки
3. Тесты покрывают как нормальные, так и ошибочные сценарии
4. Структура позволяет легко добавлять новые тесты для новых модулей
5. **Новые тесты mute rules** обеспечивают безопасность изменения логики мьютинга

---

## Связанные документы

### 📖 Специализированная документация:
- **[README_mute_rules.md](README_mute_rules.md)** - полное описание всех правил мьютинга с примерами
- **[README_historical_data.md](README_historical_data.md)** - документация по работе с историческими данными для улучшения системы мьютинга
- **[BOUNDARY_CONDITIONS_ANALYSIS.md](BOUNDARY_CONDITIONS_ANALYSIS.md)** - детальный анализ граничных условий в SQL запросах и фильтрации

### 🔧 Ключевые файлы:
- **[mock_setup.py](mock_setup.py)** - настройка моков для всех тестов
- **[run_all_tests.py](run_all_tests.py)** - главный файл для запуска тестов

### 📝 Файлы тестов mute rules:
- **[test_mute_rules.py](test_mute_rules.py)** - основные тесты правил мьютинга
- **[test_mute_rules_detailed.py](test_mute_rules_detailed.py)** - детальные тесты граничных условий
- **[test_mute_rules_summary.py](test_mute_rules_summary.py)** - комплексные тесты всех правил 