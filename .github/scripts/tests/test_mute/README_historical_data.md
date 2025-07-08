# Тестирование исторических данных для системы мьютинга

## Обзор

Данный документ описывает тесты для работы с историческими данными в системе мьютинга YDB. Эти тесты демонстрируют как модифицировать существующую логику для анализа данных за несколько дней, а не только за сегодняшний день.

## Структура тестов

### 1. test_historical_data.py
**Назначение**: Тестирует логику работы с историческими данными

**Ключевые тесты**:
- `test_historical_data_structure` - проверка структуры данных за несколько дней
- `test_historical_flaky_detection` - выявление флаки тестов на основе истории
- `test_historical_trend_analysis` - анализ трендов деградации/улучшения тестов
- `test_aggregate_historical_data` - агрегация данных по тестам
- `test_historical_muted_stable_tracking` - отслеживание стабильно замьюченных тестов

### 2. test_historical_query.py
**Назначение**: Тестирует модификации SQL запросов и обработки данных

**Ключевые тесты**:
- `test_generate_historical_query_conditions` - генерация условий для исторических запросов
- `test_modified_sql_query_structure` - структура модифицированного SQL запроса
- `test_historical_data_aggregation` - агрегация исторических данных
- `test_historical_flaky_detection_logic` - логика выявления флаки тестов
- `test_historical_data_processing_pipeline` - полный пайплайн обработки

### 3. test_sql_modifications.py
**Назначение**: Демонстрирует конкретные изменения в SQL запросах

**Ключевые тесты**:
- `test_current_sql_query_structure` - текущая структура запроса
- `test_historical_sql_query_structure` - новая структура для исторических данных
- `test_sql_query_generation_function` - функция генерации SQL запроса
- `test_data_processing_modifications` - модификации обработки данных
- `test_migration_checklist` - чек-лист для миграции

## Основные изменения для работы с историческими данными

### 1. Изменения в SQL запросе

#### Текущий подход (только сегодняшние данные):
```sql
WHERE date_window = CurrentUtcDate()
```

#### Новый подход (исторические данные):
```sql
WHERE date_window >= start_ordinal AND date_window <= end_ordinal
```

### 2. Модификации в execute_query()

```python
def execute_query(driver, branch='main', build_type='relwithdebinfo', days_back=7):
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days_back)
    
    start_ordinal = start_date.toordinal()
    end_ordinal = end_date.toordinal()
    
    # Модифицированный SQL запрос
    query_string = f'''
    SELECT * from (
        SELECT data.*,
        -- Условия для JOIN'ов изменены на диапазон дат
        ...
        FROM
        (SELECT ... FROM test_results/analytics/tests_monitor
         WHERE date_window >= {start_ordinal} AND date_window <= {end_ordinal}) as data
        ...
    ) ORDER BY date_window DESC, full_name
    '''
```

### 3. Обновления в apply_and_add_mutes()

```python
def apply_and_add_mutes(all_tests, output_path, mute_check):
    # Группировка данных по тестам
    test_groups = {}
    for test in all_tests:
        test_name = test['full_name']
        if test_name not in test_groups:
            test_groups[test_name] = []
        test_groups[test_name].append(test)
    
    # Агрегация данных за несколько дней
    for test_name, test_history in test_groups.items():
        total_pass = sum(t.get('pass_count', 0) for t in test_history)
        total_fail = sum(t.get('fail_count', 0) for t in test_history)
        overall_failure_rate = total_fail / (total_pass + total_fail)
        
        # Анализ тренда
        # ...
```

## Преимущества исторического подхода

1. **Более точное выявление флаки тестов**: Анализ за несколько дней позволяет выявить реально нестабильные тесты
2. **Анализ трендов**: Определение тестов, которые становятся менее стабильными
3. **Уменьшение ложных срабатываний**: Единичные падения не приводят к мьютингу
4. **Лучшая статистика**: Агрегированные данные дают более точную картину

## Настройка периода анализа

```python
# Анализ за 7 дней (по умолчанию)
results = execute_query(driver, days_back=7)

# Анализ за 30 дней для более глубокого анализа
results = execute_query(driver, days_back=30)
```

## Запуск тестов

```bash
# Запуск всех тестов
python run_all_tests.py

# Запуск только тестов исторических данных
python -m unittest test_module_create_new_muted_ya.test_historical_data
python -m unittest test_module_create_new_muted_ya.test_historical_query
python -m unittest test_module_create_new_muted_ya.test_sql_modifications
```

## Миграционный чек-лист

1. ✅ **Изменить execute_query**: Добавить параметр `days_back` и диапазон дат
2. ✅ **Обновить apply_and_add_mutes**: Добавить агрегацию данных по тестам
3. ✅ **Модифицировать логику флаки тестов**: Учитывать тренды и историю
4. ✅ **Обновить отчеты**: Добавить информацию о трендах
5. ✅ **Добавить конфигурацию**: Параметр для настройки периода анализа

## Примеры использования

### Выявление трендов деградации
```python
# Тест который становится менее стабильным со временем
trend_data = analyze_test_trend(test_history)
if trend_data['trend'] == 'worsening':
    # Приоритетное мьютинг
    add_to_priority_mute_list(test_name)
```

### Анализ эффективности мьютинга
```python
# Проверка долгосрочных мьютов
long_term_mutes = get_tests_muted_for_days(days=30)
for muted_test in long_term_mutes:
    if should_unmute_based_on_history(muted_test):
        add_to_unmute_candidates(muted_test)
```

## Заключение

Исторические данные позволяют принимать более обоснованные решения о мьютинге тестов, основываясь на трендах и статистике, а не только на текущем состоянии. Это должно привести к более стабильной системе CI/CD и меньшему количеству ложных срабатываний. 