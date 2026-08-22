# Известные ловушки и открытые вопросы

Контекст из отладки per-query compartments / библиотек / host ABI.  
При фиксе — переносите пункт в «закрыто» или удаляйте и обновляйте `wasm-udf-runtime.md`.

## Закрытые / уже учтённые

### 1. SDK должен быть `"env"`, не `"sdk"`

Загрузка runtime-библиотеки через `CreateMinimalRuntimeImage` + имя `"sdk"` ломает линковку (типы import, GOT, table64).  
Правильный путь: `CreateImageFromSdk` / `AddSdk` → instance `"env"`.

### 2. Host ABI: `AllocateBytes` / `ThrowException`

Регистрируются на **standard** intrinsic module (`wasm/host.cpp`).  
`CreateMinimalRuntimeImage` использует **empty** intrinsics → `Missing: [ThrowException]`.  
Для пустого `required_libraries` используется `CreateEmptyImage()` + `EnsureUdfHostIntrinsicsRegistered()`.

### 3. Early CA failure маскировался stats ENSURE

Acquire падает до `SetTaskRunner` → `FillStats` без Tasks →  
`AFL_ENSURE(stats.GetTasks().size() == 1)` в `kqp_executer_stats.cpp` → клиент видел verification вместо WASM error.  
Сейчас при пустых Tasks и ненулевом taskId UpdateTaskStats выходит без ENSURE.

### 4. WAT + ObjectCode в AddPrecompiledModule / AddSdk

Раньше HumanReadable отвергался. Теперь IR для wat парсится через `ParseWast`, object code подставляется как у Binary. Нужно для CI-фикстур и `module_extension: "wat"`.

### 5. Пустой stub sdk без `malloc`

`IWebAssemblyCompartment::AllocateBytes` зовёт `malloc` на `RuntimeLibraryInstance_` (после AddSdk).  
Пустой `(module)` как sdk → SIGSEGV на AllocateBytes.  
Тестовый stub обязан экспортировать bump-`malloc`/`free` (`data/wasm/sdk_stub.wat`).  
Bump-heap base — **65536** (и в `DefaultRegistrySdkWast`): ниже — зона для data segments UDF; иначе первый `AllocateBytes` для result затирает `.rodata` (см. `udf_rodata_cookie` / `0x30000`).

### 6. `TSdkImageCache` static dtor

Process-wide кэш в `CreateImageFromSdk` при teardown unittest может падать (`recursive_mutex` / WAVM Module dtor).  
Unit-тесты линковки обходят кэш: `CreateEmptyImage` + `AddSdk` напрямую (см. `ut/with_helpers_ut.cpp`).

---

### 7. Objects / TypeConfig

Реестр — static `object_framework` внутри module image, не `required_libraries`.  
Host pin’ит TypeConfig blob через `AllocateBytes` (не Run-pool), хранит ui64 + generation.  
Stale handle после нового Acquire → recreate на следующем `Run`.

---

## Открытые / осторожно трогать

### A. `CreateEmptyImage` без sdk и wasm-`malloc`

При `required_libraries: []` RuntimeLibraryInstance_ может не иметь `malloc`.  
Host `AllocateBytes` (intrinsic) ≠ `compartment->AllocateBytes` (через wasm malloc).  
`TWasmUdfFunction::Run` использует `compartment->AllocateBytes` для result/args → модули без sdk уязвимы, если путь реально аллоцирует.  
Throw-only модули могут обойтись; string/result path — нет.

Варианты на будущее: MinimalRuntime+host на empty module; всегда требовать sdk; dual-path аллокации.

### B. Сбор `WasmUdfModules` в predictor

В `TKqpPhyStage.WasmUdfModules` попадает **любой** `TCoUdf` module name, не только WASM.  
На Acquire `FilterLoadedWasmUdfModules` оставляет только модули из каталога — native (`String`, `Knn`, …) отбрасываются.

### C. Порядок и дедуп библиотек при нескольких модулях в одном запросе

Acquire мержит libraries нескольких артефактов: первая встреченная «первая» библиотека становится sdk.  
Если у модулей разный «первый» runtime — поведение неоднозначно. Зафиксировать политику (общий sdk, ошибка при конфликте).

### D. Кэш `CreateImageFromSdk`

Ключ — `TModuleBytecode`; клон compartment’а. Следить за lifetime / teardown и за тем, что host intrinsics уже зарегистрированы **до** первого создания Empty/Standard image singleton.

### E. Emscripten multi-module imports

C++ UDF → отдельная библиотека: нужен `import_module("helpers")` / `import_name(...)`, не PEERDIR (иначе статический линк в один wasm).  
Сборка: `--target-platform=clang20-emscripten-wasm64`.

### F. Ошибки и UI

После фикса stats клиент должен видеть issue из CA.  
Обёртка `Internal error while executing transaction` всё ещё возможна на других ENSURE/verification путях — при доработке UX ошибок смотреть цепочку `ReportStateAndMaybeDie` → `ReplyErrorAndDie` vs `InternalError`.

### G. PreferWasm / резидентная память — оставшиеся ограничения

Каноническая таблица и контекст — в `wasm-udf-runtime.md` §8 «Ограничения PreferWasm (backlog)». Здесь — короткий список для планирования работ (false negative безопасен: host + copy).

1. **Same-stage only** — буфер не переживает канал; UDF после shuffle/join в другом stage не reuse.
2. **Fail-closed AST** — только известные формы в `kqp_wasm_string_columns`; join / computed / результат UDF — нет.
3. **`GROUP BY` / `DqPhyHashCombine`** — нет индексного маппинга wide-хендлеров.
4. **Не-колонки** (литерал, param, `$dict` / tx_result_binding) — только через `EnableWasmUdfResidentConstArgs` (opt-in, default false).
5. **ConstArgs узкий** — только прямые string-args `Apply(Udf)` без `Argument` в subtree; compile не отличает wasm от native; крупный частично читаемый blob держит всю linear memory на task.
6. **Embedded / short strings** (≤ `InternalBufferSize`) — `MakePreferWasm` не материализует в WASM.
7. **Нет compartment** → `FallbackNoCompartment` (планирование).
8. **Native UDF** — PreferWasm не применим.
9. **Returns** — нет resident path для string-результата UDF (только args).
10. **Blocks / lazy holder** — вне скоупа.

Кандидаты в ближайший план: (3) маппинг `DqPhyHashCombine`; (5) знание каталога на compile → безопасный default ConstArgs; (2) join/computed формы по мере появления реальных запросов.

---

## Чеклист при изменении линковки / compartment

- [ ] Unit: `ut/with_helpers_ut`, `ut/throw_exception_ut`, `ut/object_framework_ut`, `ut/objects_abi_ut`
- [ ] Functional: `test_using_wasm_udf`, `test_using_wasm_udf_with_sdk_and_library`
- [ ] Ручной сценарий: upload sdk → helpers → with_helpers / md5 / throw / prefix
- [ ] Ошибка Acquire видна в UI (не `stats.GetTasks().size() == 1`)
- [x] `ThrowException("…")` доходит как `fail(); ex: …` (+ wasm call stack)
