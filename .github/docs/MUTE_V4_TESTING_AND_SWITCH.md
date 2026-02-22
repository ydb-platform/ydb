# Mute v4: параллельное тестирование и переход

## 1. Параллельное тестирование

После мержа в main обе системы работают параллельно при каждом `update_muted_ya`:

| Файл | Система | Используется CI |
|------|---------|-----------------|
| `.github/config/muted_ya.txt` | Legacy (старое) | **Да** |
| `.github/config/mute/muted_ya.txt` | v4 (новое) | Нет |

### Шаг 1.1. Проверка через workflow

1. Actions → **Update Muted tests** → Run workflow
2. Дождаться завершения
3. В PR (или артефакте) смотреть:
   - **Legacy (muted_ya.txt)** — to_mute, to_unmute, to_delete
   - **v4 (mute/muted_ya.txt)** — to_mute, to_unmute, to_delete
4. Сравнить расхождения между Legacy и v4

### Шаг 1.2. Проверка через compare_mute_systems (разовое сравнение)

1. Actions → **Compare Mute Systems (Legacy vs v4)** → Run workflow
2. Скачать артефакт `mute-comparison-{branch}`
3. Открыть `comparison_report.md` — там diff по to_mute, to_unmute, to_delete, new_muted_ya

### Шаг 1.3. Локальная проверка

```bash
# 1. Получить base-файлы
BRANCH=main
git fetch origin $BRANCH
git show origin/$BRANCH:.github/config/muted_ya.txt > base_muted_ya.txt
git show origin/$BRANCH:.github/config/mute/muted_ya.txt > base_mute_muted_ya.txt 2>/dev/null || cp base_muted_ya.txt base_mute_muted_ya.txt
git show origin/$BRANCH:.github/config/quarantine.txt > quarantine.txt 2>/dev/null || touch quarantine.txt

# 2. Заполнить данные (для legacy)
python3 .github/scripts/analytics/flaky_tests_history.py --branch=$BRANCH --build_type=relwithdebinfo
python3 .github/scripts/analytics/tests_monitor.py --branch=$BRANCH --build_type=relwithdebinfo

# 3. Запустить параллельно
python3 .github/scripts/tests/run_parallel_mute_update.py \
  --branch=$BRANCH \
  --muted_ya_file=base_muted_ya.txt \
  --mute_muted_ya_file=base_mute_muted_ya.txt \
  --quarantine_file=quarantine.txt

# 4. Сравнить
diff mute_update_legacy/mute_update/new_muted_ya.txt mute_update_v4/mute_update/new_muted_ya.txt
```

### Шаг 1.4. Анализ в YDB

- **Legacy:** `test_results/analytics/mute_decisions`
- **v4:** `test_results/mute/v4_decisions`

Можно строить отчёты и симулировать влияние v4 за период.

---

## 2. Переход на v4

Когда v4 проверена и готова к использованию в CI.

### Шаг 2.1. Переключить источник muted_ya в test_ya

Файл: `.github/actions/test_ya/action.yml`

Найти блок с `MUTED_YA_FILE` и заменить:

```yaml
# Было (legacy):
relwithdebinfo) MUTED_YA_FILE=".github/config/muted_ya.txt" ;;

# Стало (v4):
relwithdebinfo) MUTED_YA_FILE=".github/config/mute/muted_ya.txt" ;;
```

Для sanitizers (asan, tsan, msan) — аналогично, если используются отдельные mute-файлы.

### Шаг 2.2. Проверить после переключения

1. Смержить PR с изменением
2. Дождаться нескольких прогонов CI
3. Убедиться, что тесты используют `.github/config/mute/muted_ya.txt`

### Шаг 2.3. (Опционально) Убрать legacy

После стабилизации v4 можно:

1. Оставить только v4 в workflow (убрать запуск legacy)
2. Переименовать `mute/muted_ya.txt` → `muted_ya.txt` и вернуть старый путь
3. Или оставить два файла для истории и отката

---

## 3. Откат на legacy

Если v4 ведёт себя некорректно:

1. В `test_ya/action.yml` вернуть `MUTED_YA_FILE=".github/config/muted_ya.txt"`
2. Смержить PR
3. CI снова будет использовать legacy-файл
