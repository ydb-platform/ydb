# Анализ граничных условий в системе мьютинга

## Обзор граничных условий

Данный документ содержит анализ всех граничных условий в SQL запросах и логике фильтрации системы мьютинга YDB.

## SQL Запросы - Граничные условия

### 1. Условие `days_in_state = 1` (new_flaky)
**Расположение**: LEFT JOIN для new_flaky
```sql
WHERE state = 'Flaky'
AND days_in_state = 1  -- ТОЧНОЕ равенство
AND date_window = CurrentUtcDate()
```

**Граничные случаи**:
- ✅ `days_in_state = 0` → НЕ new_flaky
- ✅ `days_in_state = 1` → new_flaky  
- ✅ `days_in_state = 2` → НЕ new_flaky

**Покрытие тестами**: ✅ test_boundary_conditions.py

### 2. Условие `days_in_state >= 14` (muted_stable_n_days, deleted)
**Расположение**: LEFT JOIN для muted_stable_n_days и deleted
```sql
WHERE state = 'Muted Stable' -- или 'no_runs'
AND days_in_state >= 14  -- Больше или равно
AND date_window = CurrentUtcDate()
AND is_test_chunk = 0
```

**Граничные случаи**:
- ✅ `days_in_state = 13` → НЕ подходит
- ✅ `days_in_state = 14` → подходит
- ✅ `days_in_state = 15` → подходит

**Покрытие тестами**: ✅ test_boundary_conditions.py

### 3. Условие `is_test_chunk = 0`
**Расположение**: LEFT JOIN для muted_stable_n_days и deleted
```sql
AND is_test_chunk = 0  -- ТОЧНОЕ равенство
```

**Граничные случаи**:
- ✅ `is_test_chunk = 0` → подходит (не chunk тест)
- ✅ `is_test_chunk = 1` → НЕ подходит (chunk тест)
- ✅ `is_test_chunk = NULL` → НЕ подходит

**Покрытие тестами**: ✅ test_boundary_conditions.py

## Логика фильтрации флаки тестов - Граничные условия

### Комплексное условие для флаки тестов:
```python
if (test.get('days_in_state') >= 1
    and test.get('flaky_today')
    and (test.get('pass_count') + test.get('fail_count')) >= 2
    and test.get('fail_count') >= 2
    and test.get('fail_count')/(test.get('pass_count') + test.get('fail_count')) > 0.2):
```

### 1. Условие `days_in_state >= 1`
**Граничные случаи**:
- ✅ `days_in_state = 0` → НЕ флаки
- ✅ `days_in_state = 1` → может быть флаки
- ✅ `days_in_state > 1` → может быть флаки

**Покрытие тестами**: 
- ✅ test_mute_rules.py
- ✅ test_boundary_conditions.py

### 2. Условие `flaky_today = True`
**Граничные случаи**:
- ✅ `flaky_today = False` → НЕ флаки
- ✅ `flaky_today = True` → может быть флаки

**Покрытие тестами**: ✅ test_boundary_conditions.py

### 3. Условие `(pass_count + fail_count) >= 2`
**Граничные случаи**:
- ✅ `total_runs = 0` → НЕ флаки
- ✅ `total_runs = 1` → НЕ флаки
- ✅ `total_runs = 2` → может быть флаки
- ✅ `total_runs > 2` → может быть флаки

**Покрытие тестами**: 
- ✅ test_mute_rules.py
- ✅ test_boundary_conditions.py

### 4. Условие `fail_count >= 2`
**Граничные случаи**:
- ✅ `fail_count = 0` → НЕ флаки
- ✅ `fail_count = 1` → НЕ флаки
- ✅ `fail_count = 2` → может быть флаки
- ✅ `fail_count > 2` → может быть флаки

**Покрытие тестами**: 
- ✅ test_mute_rules.py
- ✅ test_boundary_conditions.py

### 5. Условие `fail_count/(pass_count + fail_count) > 0.2`
**КРИТИЧЕСКОЕ граничное условие** - строго больше 0.2 (НЕ больше или равно)

**Граничные случаи**:
- ✅ `fail_rate = 0.19` → НЕ флаки
- ✅ `fail_rate = 0.2` → НЕ флаки (ВАЖНО!)
- ✅ `fail_rate = 0.20001` → флаки
- ✅ `fail_rate = 0.21` → флаки

**Специальные случаи**:
- ✅ `fail_rate = 1/5 = 0.2` → НЕ флаки
- ✅ `fail_rate = 2/9 ≈ 0.222` → флаки
- ✅ `fail_rate = 21/100 = 0.21` → флаки

**Покрытие тестами**: 
- ✅ test_mute_rules.py (детальные тесты)
- ✅ test_mute_rules_detailed.py (граничные случаи)
- ✅ test_boundary_conditions.py

## Регулярное выражение для chunk тестов

### Паттерн замены: `r'\d+/(\d+)\]' → r'*/*]'`
**Граничные случаи**:
- ✅ `test[1/2]` → `test[*/*]`
- ✅ `test[0/1]` → `test[*/*]`
- ✅ `test[999/999]` → `test[*/*]`
- ✅ `test[a/b]` → остается `test[a/b]` (не меняется)

**Покрытие тестами**: ✅ test_boundary_conditions.py

## Исторические данные - Граничные условия

### Модифицированные SQL запросы
**Текущий подход**:
```sql
WHERE date_window = CurrentUtcDate()
```

**Исторический подход**:
```sql
WHERE date_window >= start_ordinal AND date_window <= end_ordinal
```

**Важно**: Граничные условия для `days_in_state` и `is_test_chunk` СОХРАНЯЮТСЯ в исторических запросах.

**Покрытие тестами**: ✅ test_boundary_conditions.py

## Потенциальные проблемы и рекомендации

### 1. ⚠️ Проблема точности вычислений
```python
# Потенциальная проблема с плавающей точкой
fail_rate = fail_count / (pass_count + fail_count)
if fail_rate > 0.2:  # Может быть неточно для некоторых дробей
```

**Рекомендация**: Использовать целочисленное сравнение:
```python
# Более точное сравнение
if fail_count * 5 > (pass_count + fail_count):  # Эквивалентно > 0.2
```

### 2. ✅ Корректная обработка граничных случаев
- Условие `> 0.2` (строго больше) правильно исключает точно 80% success rate
- Условие `>= 1` и `>= 2` корректно включают граничные значения
- Условие `= 1` и `= 0` правильно работает для точных значений

### 3. ⚠️ Отсутствие проверки на NULL/None значения
В некоторых местах может потребоваться дополнительная проверка на NULL значения.

## Статус покрытия тестами

| Граничное условие | Файл теста | Статус |
|------------------|------------|--------|
| `days_in_state = 1` | test_boundary_conditions.py | ✅ |
| `days_in_state >= 14` | test_boundary_conditions.py | ✅ |
| `is_test_chunk = 0` | test_boundary_conditions.py | ✅ |
| `days_in_state >= 1` | test_mute_rules.py + test_boundary_conditions.py | ✅ |
| `flaky_today = True` | test_boundary_conditions.py | ✅ |
| `total_runs >= 2` | test_mute_rules.py + test_boundary_conditions.py | ✅ |
| `fail_count >= 2` | test_mute_rules.py + test_boundary_conditions.py | ✅ |
| `fail_rate > 0.2` | test_mute_rules_detailed.py + test_boundary_conditions.py | ✅ |
| Regex chunks | test_boundary_conditions.py | ✅ |
| Исторические SQL | test_boundary_conditions.py | ✅ |

## Заключение

Все критические граничные условия покрыты тестами. Особое внимание уделено:

1. **Точному равенству** (`= 1`, `= 0`) для new_flaky и is_test_chunk
2. **Строгому неравенству** (`> 0.2`) для failure rate
3. **Нестрогому неравенству** (`>= 14`, `>= 1`, `>= 2`) для других условий
4. **Комбинированным условиям** для флаки тестов

Тесты проверяют как положительные, так и отрицательные граничные случаи, обеспечивая надежность логики мьютинга. 