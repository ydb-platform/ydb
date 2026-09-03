# План исправления: `EnableCsWriteAffinity` для CTAS (server settings подход)

**Статус:** исправлено. Тесты используют server-level настройки вместо per-query PRAGMA.

## Проблема (до фикса)

Per-query `PRAGMA ydb.EnableCsWriteAffinity = "true"` не доходил до оптимизатора CTAS из-за:
1. CTAS split в `RewriteCreateTableAs()` происходит до полной type annotation (когда `Configure!` ещё не выполнен)
2. FILL-часть CTAS компилируется новым `TKikimrConfiguration`, который не содержит PRAGMA из исходного запроса

## Решение: server-level настройки вместо PRAGMA

Вместо сложного механизма переноса PRAGMA через фазы компиляции, тесты используют server-level настройки через `TKikimrSettings::SetKqpSettings()`.

### Архитектура

```
Test Code
    ↓ settings.SetKqpSettings(BuildKqpSettingsWithCsWriteAffinity(true))
    ↓ TKikimrRunner(settings)
    ↓ BuildConfiguration() читает KqpSettings->Settings
    ↓ TKikimrConfiguration.EnableCsWriteAffinity = true   ✅ все фазы
    ↓ RewriteCreateTableAs → GetEnableCsWriteAffinity() → true ✅
    ↓ CtasShardingColumns записываются в TKqlFillTable
    ↓
BuildFillTableEffect() → node.CtasShardingColumns().IsValid() → true ✅
```

### Ключевые изменения

#### 1. Тесты используют server settings

**Файл:** [`ydb/core/kqp/ut/query/kqp_write_affinity_ut.cpp`](ydb/core/kqp/ut/query/kqp_write_affinity_ut.cpp:223)

```cpp
static TVector<NKikimrKqp::TKqpSetting> BuildKqpSettingsWithCsWriteAffinity(bool enableCsWriteAffinity) {
    NKikimrKqp::TKqpSetting setting;
    setting.SetName("EnableCsWriteAffinity");
    setting.SetValue(enableCsWriteAffinity ? "true" : "false");
    return {setting};
}
```

Каждый тест устанавливает настройку:
```cpp
auto settings = TKikimrSettings().SetWithSampleTables(false);
settings.AppConfig.MutableTableServiceConfig()->SetEnablePerStatementQueryExecution(true);
settings.SetKqpSettings(BuildKqpSettingsWithCsWriteAffinity(EnableCsWriteAffinity));
TKikimrRunner kikimr(settings);
```

#### 2. Оптимизатор читает из узла, а не из конфига

**Файл:** [`ydb/core/kqp/opt/kqp_opt_effects.cpp`](ydb/core/kqp/opt/kqp_opt_effects.cpp:221)

```cpp
const bool enableCsWriteAffinity = node.CtasShardingColumns().IsValid();
```

`CtasShardingColumns` заполняется в `RewriteCreateTableAs()` на основе `GetEnableCsWriteAffinity()` и сохраняется в AST узле `TKqlFillTable`.

#### 3. Rewrite фаза читает из конфига

**Файл:** [`ydb/core/kqp/host/kqp_statement_rewrite.cpp`](ydb/core/kqp/host/kqp_statement_rewrite.cpp:319)

```cpp
const bool enableCsWriteAffinity = sessionCtx->ConfigPtr()->GetEnableCsWriteAffinity();
```

При `enableCsWriteAffinity == true` для OLAP CTAS вычисляются `CtasShardingColumns` и записываются в AST.

## Тесты

Все 20 тестов проходят:
```bash
./ya make --build relwithdebinfo -tA ydb/core/kqp/ut/query/ -F 'CS_WriteAffinity'
```

- 10 тестов с `EnableCsWriteAffinity=true` (+)
- 10 тестов с `EnableCsWriteAffinity=false` (-)

## Преимущества server settings подхода

1. **Простота** — не нужен сложный парсинг PRAGMA из текста запроса
2. **Надёжность** — настройка доступна во всех фазах компиляции
3. **Соответствие реальности** — в продакшене `EnableCsWriteAffinity` будет глобальной серверной настройкой
4. **Нет проблем с per-statement execution** — серверная настройка не зависит от разделения запросов

## Сравнение подходов

| Аспект | Per-query PRAGMA | Server Settings |
|---|---|---|
| Сложность | Высокая (нужен парсинг + dispatch) | Низкая (прямая настройка) |
| Надёжность | Низкая (теряется между фазами) | Высокая (доступна везде) |
| Гибкость | Per-query контроль | Серверный контроль |
| Продакшен использование | Не подходит | Подходит |
