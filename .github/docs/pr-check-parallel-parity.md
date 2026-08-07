# PR-check-parallel: разбор расхождений по составу тестов

Цель: V2 (`pr_check_parallel.yml`) должен запускать **тот же набор тестов**, что и
прод в сопоставимом режиме (`increment` ↔ `increment`, full ↔ full). Ниже —
зафиксированные факты по запускам от 2026-06-13 и план разбора.

Сравнение делалось по множествам строк `type=test` из `try_1/report.json`
(ключ: `path` + `name` + `subtest_name`), preset relwithdebinfo.

## Зафиксированные расхождения

| Сценарий | Режим | Прод run | V2 run | Тестов прод | Тестов V2 | Missing в V2 | Extra в V2 | Статус |
|---|---|---|---|---:|---:|---:|---:|---|
| zero (#43126) | increment | [27285833429](https://github.com/ydb-platform/ydb/actions/runs/27285833429) | [27448787683](https://github.com/ydb-platform/ydb/actions/runs/27448787683) | 0 | 0 | 0 | 0 | OK |
| short docs (#43282) | increment | [27373849341](https://github.com/ydb-platform/ydb/actions/runs/27373849341) | [27448785772](https://github.com/ydb-platform/ydb/actions/runs/27448785772) | 1182 | 1182 | 0 | 0 | OK |
| medium kafka_proxy (#43285) | increment | [27375619130](https://github.com/ydb-platform/ydb/actions/runs/27375619130) | [27450301922](https://github.com/ydb-platform/ydb/actions/runs/27450301922) | 43554 | 0 | 43554 | 0 | **FAIL** |
| fat (#43284) | increment | [27398642720](https://github.com/ydb-platform/ydb/actions/runs/27398642720) | [27448783713](https://github.com/ydb-platform/ydb/actions/runs/27448783713) | 48487 | 9041 | 39454 | 8 | **FAIL** (V2 run = failure, покрытие частичное) |
| main `ydb/` | full | [27382570708](https://github.com/ydb-platform/ydb/actions/runs/27382570708) | [27448789885](https://github.com/ydb-platform/ydb/actions/runs/27448789885) | 52133 | 51957 | 177 | 1 | **MINOR** |
| `ydb/tests/olap/` | full | [27446799893](https://github.com/ydb-platform/ydb/actions/runs/27446799893) | [27448792212](https://github.com/ydb-platform/ydb/actions/runs/27448792212) | **4333** (summary) | 233 | — | 0 | **FAIL** (524 в старом diff — subset `try_1`) |

Итог: 2/6 строк совпадают полностью (zero, short). Остальные 4 имеют недобор;
наиболее критичны medium и olap.

## Разбор по строкам (категории missing)

- **medium increment** — ✅ **первопричина найдена и подтверждена**.
  - Симптом: `list_summary.json` после фильтра →
    `increment_filtered=true`, `increment_graph_suites=0`, `total_suites=0`,
    `shard_count=0`. Тесты не запускались (0 против 43554 в проде).
  - Путь: medium классифицировался как **sharded**, значит шёл через
    `prepare` job → `filter_suites_by_increment_graph.py` поверх `graph.json`
    из sharded-build. Лёгкий short PR прошёл как **single** (обычный
    prod-подобный путь `ya make -A` на cut-графе) и совпал 1:1 — поэтому
    баг проявился только на sharded+increment.
  - Корень: в `build` (no tests) job стоит `run_tests: false`, а в
    `test_ya_parallel/action.yml` флаг `-A` добавляется в команду построения
    графа **только при `run_tests=true`**:

    ```text
    if [ true = ${{ inputs.run_tests }} ]; then params+=(-A); fi
    GRAPH_YA_MAKE_COMMAND="./ya make ${GRAPH_SAVE_PARAMS[@]}"
    ./.github/scripts/graph_compare.py --ya-make-command="$GRAPH_YA_MAKE_COMMAND" ...
    ```

    Без `-A` `graph_compare` строит инкремент-граф **без тест-программных
    нод**. Проверка скачанного `graph.json` рана 27450301922: 33136 нод,
    `module_tag` ∈ {cpp_proto, py3_proto, py3, global, ...}, и **ни одной**
    `*test_program` ноды. Поэтому `extract_suite_paths`
    (ищет `py3test_program` / `unittest_program` / `gtest_program`)
    возвращает 0 сьют → инкремент-план пуст.
  - Фикс (направление): для sharded+increment граф для планирования должен
    строиться с `-A` независимо от `run_tests`. То есть отвязать флаг `-A`
    в `GRAPH_SAVE_PARAMS`/`GRAPH_YA_MAKE_COMMAND` от `run_tests` и включать
    его, когда `save_test_graph=true` (нужны тест-ноды для extract/​filter).
- **fat increment**: V2 run упал (`failure`), часть шардов не завершилась →
  9041 из 48487. Это не доказательство дыры в плане, а оборванный прогон;
  нужно перезапустить успешно и сравнить заново.
- **main full (missing 177)** — баг #2 (`--add-peerdirs-tests all`).
  - Симптом: missing 177 — почти всё `chunk` / `import_test`.
  - Корень: `list_tests_for_shard_plan.sh` (вселенная сьют для плана) **не
    передавал `--add-peerdirs-tests all`**, а монолит и реальный прогон шарда
    (`test_ya_parallel`, `params`) — передают. План строился по более узкому
    множеству, чем шарды реально исполняют.
  - Фикс применён: в `list_tests_for_shard_plan.sh` добавлены
    `--add-peerdirs-tests all` и `-DUSE_EAT_MY_DATA`.
  - Перепроверка v3: ожидаем missing → ~0; остаток `chunk` разбирать отдельно.
- **olap full (missing 291)** — ⚠️ **третья, отдельная первопричина (баг #3,
  фикс ещё не сделан).**
  - Проверка test-list рана 27448792212: тесты `compaction_config.py`,
    `data_read_correctness.py`, `test_replace.py`, … лежат в **корневой**
    py3test-сьюте `ydb/tests/olap` (заголовок `ydb/tests/olap <py3test>`),
    рядом есть вложенные `ydb/tests/olap/<child>`.
  - `extract_suites_from_ya_test_list.py` вызывается с
    `--target-prefix ydb/tests/olap`. `drop_redundant_scope_roots` видит,
    что корень `ydb/tests/olap` == prefix и есть вложенные `ydb/tests/olap/…`,
    и **выбрасывает корневую сьюту целиком** (с предупреждением про N прямых
    тестов). Эти ~291 прямых теста корня нигде не запускаются → недобор.
  - Замысел дропа: не гонять корень рекурсивно поверх детей (иначе дети
    выполнятся дважды). Но корень здесь — реальная сьюта с прямыми тестами.
  - Фикс (баг #3, варианты): (а) не дропать корень, а гонять его
    non-recursive/через `--test-filter` только прямыми тестами; (б) не дропать
    и полагаться на дедуп в merge (ценой дублей в детях); (в) переносить
    прямые тесты корня в отдельный «leaf»-таргет. Требует решения — вне
    объёма двух текущих фиксов.

## План разбора (начинаем с инкрементов)

### Этап 1 — инкременты (приоритет)

1. **medium increment (главный кейс).** ✅ диагностика завершена.
   - [x] Воспроизвести фильтр на `graph.json` из артефакта V2 рана
     27450301922 — граф без `*test_program` нод (см. раздел выше).
   - [x] Корневая причина: `-A` не передаётся в построение графа при
     `run_tests=false` (sharded build).
   - [x] Фикс применён: в `test_ya_parallel/action.yml` `-A` добавляется в
     `GRAPH_SAVE_PARAMS` при `GRAPH_ONLY_MODE + increment`.
   - [ ] Перепроверка: прогнать V2 sharded+increment над #43285,
     убедиться `increment_graph_suites > 0` и diff против прод-PR-check
     [27375619130](https://github.com/ydb-platform/ydb/actions/runs/27375619130)
     → missing ≈ 0.
2. **fat increment (валидность прогона).**
   - [ ] Перезапустить V2 над #43284 до зелёного/полного завершения
     (увеличить retry/таймаут шардов, дождаться всех шардов).
   - [ ] Повторить diff против прод-инкремента; ожидаем missing → ~0,
     иначе разбирать как medium.
3. **Регресс-проверка lightweight инкрементов.**
   - [ ] zero/short уже OK — закрепить как baseline-кейсы в этом документе,
     чтобы при правках инкремента они не сломались.

### Этап 2 — full-режимы

4. **main full (missing 177).**
   - [x] Первопричина: листинг без `--add-peerdirs-tests all` (баг #2).
   - [x] Фикс применён в `list_tests_for_shard_plan.sh`.
   - [ ] Перепроверка v3: missing → ~0; хвост `chunk`/`import_test` — отдельно.
5. **olap full (missing 291).**
   - [x] Первопричина: `drop_redundant_scope_roots` + dedup в merge.
   - [x] Фикс #3 применён; worst-wins dedup в `merge_reports`.
   - [ ] Перепроверка v4 olap: тестов ≈ 4333 vs монолит.

## Метод повторной сверки

Скрипт сравнения (используется при каждой итерации):
- берём `try_1/report.json` из корневого relwithdebinfo-префикса каждого рана;
- множество ключей `(path, name, subtest_name)` по `type=test`;
- считаем `missing_in_v2 = prod \ v2`, `extra_in_v2 = v2 \ prod`;
- для проблемных строк — топ missing по каталогам `ydb/<dir>`.

Готово к работе: начинаем с пункта 1 (medium increment) как с кейса с
максимальным расхождением и наиболее вероятной общей первопричиной для всех
инкрементальных прогонов.
