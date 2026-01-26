# MCP GitHub — возможности и подсказки для LLM

Используй этот промпт, чтобы предлагать пользователю **актуальные** возможности MCP. Все инструменты описаны в `schema.json` (по одному на функцию).

---

## 1. Возможности MCP — когда что предлагать

### Репозиторий (ydb-platform/ydb)
- **Файл по пути:** `get_repo_file` (path, ref).
- **Поиск файлов по имени:** `search_repo_files` (query, extension).
- **Поиск по содержимому:** `search_repo_content` (query, path, extension, per_page).
- **Структура директории:** `get_repo_tree` (path, recursive, ref).
- **Метаданные файла:** `get_file_info` (path, ref).
- **История коммитов файла:** `get_file_commits` (path, ref, per_page).
- **Diff по коммиту:** `get_commit_diff` (sha, repo). Для **PR из форка** — `repo=head_repo` из `get_pull_request`.

### PR и issues
- **Инфо о PR:** `get_pull_request` (number). Возвращает `head_sha`, `head_repo`, `is_fork` — нужны для форков.
- **Файлы и diff по PR:** `get_pull_request_files` (number). **Работает и для PR из форка** — предпочтительнее `get_commit_diff`, если нужен diff по PR.
- **Поиск PR:** `search_pull_requests` (query, state, per_page).
- **Issue:** `get_issue` (number), `search_issues` (query, state, per_page).

### Workflow runs и отчёты
- **Список запусков:** `get_workflow_runs` (workflow, branch, status, per_page). У каждого run — `workflow_branch` (head_branch).
- **Jobs по run_id:** `get_workflow_run_jobs` (run_id, per_page). У каждого job — `id`, `name`, `html_url` (**прямые ссылки на jobs**).
- **Один вызов — отчёт по тестам:** `get_regression_report` (workflow, branch, test_branch, build_preset, max_failures). Сам находит run_id, качает report. Возвращает **run_url**, **job_links** (прямые ссылки на каждый job), failures/muted, report_url, html_url.
- **Отчёт по run_id + suite + platform:** `get_run_report` (run_id или job_id, suite, platform, …). Тоже возвращает **run_url**, **job_links**, failures_count, report_url, html_url.
- **Таблица «ветка × конфигурация»:** `get_run_errors_matrix` (run_id, suite или workflow, branches, build_presets). По каждому run_id — матрица с **failures_count** и **report_url** / **html_url** в каждой ячейке. Используй, когда просят «количество ошибок по конфигурации и ветке», «таблицу», «число — ссылка».
- **Сравнить с предыдущим:** `compare_with_previous_run` (workflow, branch, test_branch, build_preset, since, until). **Никогда** не вычисляй предыдущий run как `run_id - 1`.
- **Сравнить два конкретных run:** `compare_run_reports` (current_run_id, previous_run_id, suite, platform, …). ID бери из `get_workflow_runs(per_page=2)`.

**Предлагай пользователю:** таблицу по run (`get_run_errors_matrix`), отчёт по workflow (`get_regression_report`), ссылки на jobs (уже в `get_run_report` / `get_regression_report`), сравнение с предыдущим (`compare_with_previous_run`).

**Ссылки на отчёты:** только из полей `report_url`, `html_url`, `index_url` в ответе инструмента. Base всегда `ydb-gh-logs/ydb-platform/ydb`. **Не** `ydb-public`, **не** `reports` — не подставляй другой base и не собирай URL сам.

---

## 2. Отчёты и тесты — кратко

**Один вызов для отчёта по workflow:**

```json
{
  "tool_name": "get_regression_report",
  "arguments": {
    "workflow": "Regression-run_Small_and_Medium",
    "branch": "main",
    "build_preset": "relwithdebinfo"
  }
}
```

- **workflow:** `Regression-run_Small_and_Medium` | `Regression-run_Large` | `Regression-whitelist-run` | `Regression-run_compatibility` | `Regression-run_stress` | `nightly_small` | `whitelist` | `compatibility`.
- **branch:** ветка **запуска** workflow (фильтр API). Обычно `main`.
- **test_branch:** ветка **тестов** (platform в storage). Если не задана — `branch`. Нужна при «workflow из A, тесты по B».
- **build_preset:** `relwithdebinfo` | `asan` | `tsan` | `msan`.

**Синонимы:** «ночные» → Small_and_Medium / Large; «whitelist» → Regression-whitelist-run; «compatibility» → Regression-run_compatibility.

**Таблица по run (ветка × конфигурация, число ошибок — ссылка):**

```json
{
  "tool_name": "get_run_errors_matrix",
  "arguments": {
    "run_id": 21323451647,
    "suite": "Regression-run_Small_and_Medium"
  }
}
```

Возвращает `matrix`: для каждой (branch, build_preset) — `failures_count`, `report_url`, `html_url`. Строй таблицу, делай число ссылкой на `report_url` или `html_url`.

---

## 3. Ветка запуска vs ветка тестов

- **branch (workflow_branch):** по какой ветке запустили workflow (фильтр API, `head_branch`).
- **test_branch:** по какой ветке гоняют тесты; из неё **platform** в storage: `ya-{test_branch}-x86-64` или `ya-{test_branch}-x86-64-{suffix}`.

Часто совпадают. Если «workflow из main, тесты по stable-25-4» — используй `branch` и `test_branch` раздельно.

---

## 4. Сравнение запусков

**По умолчанию:** branch=**main**, build_preset=**relwithdebinfo**. Если указаны **since** / **until** — только запуски за эти даты.

**Один вызов:**

```json
{
  "tool_name": "compare_with_previous_run",
  "arguments": {
    "workflow": "Regression-run_Small_and_Medium",
    "branch": "main",
    "build_preset": "relwithdebinfo"
  }
}
```

С датами: добавь `"since": "2026-01-10"`, `"until": "2026-01-12"`.

**Никогда не используй `run_id - 1`** для «предыдущего» run — ID не последовательные.

При «сравни» без уточнения — уточни тип (ночные / whitelist / compatibility), затем вызывай `compare_with_previous_run`.

---

## 5. PR из форка

1. `get_pull_request` (number) → `head_repo`, `head_sha`, `is_fork`.
2. **Diff по PR:** `get_pull_request_files` (number) — работает и для форков, даёт файлы и патчи.
3. **Diff по коммиту из форка:** `get_commit_diff` (sha, **repo**=head_repo).

---

## 6. Storage и ссылки на отчёты

**Base всегда:** `https://storage.yandexcloud.net/ydb-gh-logs/ydb-platform/ydb`  
**Никогда не используй:** `ydb-public`, `reports` — это другой bucket/путь, наши отчёты там не лежат.

- Путь: `{base}/{suite}/{run_id}/{platform}/try_{1|2|3}/report.json`, `.../ya-test.html`, `.../{platform}/index.html`.
- В путях **run_id** (workflow run), не job_id. Tool при необходимости резолвит job_id → run_id.

**Важно:** Не придумывай и не собирай ссылки сам. Всегда бери **точные** `report_url`, `html_url`, `index_url` из ответа инструмента (`get_run_report`, `get_regression_report`, `get_run_errors_matrix`). Подставлять `ydb-public`, `reports` или другой base — ошибка, ссылка будет битой.

---

## 7. Схема

Актуальный список инструментов и их аргументов — в **schema.json** (поле `tools`, у каждого инструмента свой `inputSchema`). Используй его при выборе инструмента и формировании `arguments`.
