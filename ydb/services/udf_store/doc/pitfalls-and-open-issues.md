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

### 6. `TSdkImageCache` static dtor

Process-wide кэш в `CreateImageFromSdk` при teardown unittest может падать (`recursive_mutex` / WAVM Module dtor).  
Unit-тесты линковки обходят кэш: `CreateEmptyImage` + `AddSdk` напрямую (см. `ut/with_helpers_ut.cpp`).

---

## Открытые / осторожно трогать

### A. `CreateEmptyImage` без sdk и wasm-`malloc`

При `required_libraries: []` RuntimeLibraryInstance_ может не иметь `malloc`.  
Host `AllocateBytes` (intrinsic) ≠ `compartment->AllocateBytes` (через wasm malloc).  
`TWasmUdfFunction::Run` использует `compartment->AllocateBytes` для result/args → модули без sdk уязвимы, если путь реально аллоцирует.  
Throw-only модули могут обойтись; string/result path — нет.

Варианты на будущее: MinimalRuntime+host на empty module; всегда требовать sdk; dual-path аллокации.

### B. Сбор `WasmUdfModules` в predictor

Сейчас в `WasmUdfModules_` попадает **любой** `TCoUdf` module name, не только WASM.  
Лишние имена → `ResolveModules` / Acquire могут упасть или no-op некорректно.  
Имеет смысл фильтровать по типу регистрации (`wasm:` path / каталог).

### C. Порядок и дедуп библиотек при нескольких модулях в одном запросе

Acquire мержит libraries нескольких артефактов: первая встреченная «первая» библиотека становится sdk.  
Если у модулей разный «первый» runtime — поведение неоднозначно. Зафиксировать политику (общий sdk, ошибка при конфликте).

### D. Кэш `CreateImageFromSdk`

Ключ — `TModuleBytecode`; клон compartment’а. Следить за lifetime / teardown и за тем, что host intrinsics уже зарегистрированы **до** первого создания Empty/Standard image singleton.

### E. Emscripten multi-module imports

C++ UDF → отдельная библиотека: нужен `import_module("helpers")` / `import_name(...)`, не PEERDIR (иначе статический линк в один wasm).  
Сборка: `--target-platform=clang18-emscripten-wasm64`.

### F. Ошибки и UI

После фикса stats клиент должен видеть issue из CA.  
Обёртка `Internal error while executing transaction` всё ещё возможна на других ENSURE/verification путях — при доработке UX ошибок смотреть цепочку `ReportStateAndMaybeDie` → `ReplyErrorAndDie` vs `InternalError`.

---

## Чеклист при изменении линковки / compartment

- [ ] Unit: `ut/with_helpers_ut`, `ut/throw_exception_ut`
- [ ] Functional: `test_using_wasm_udf`, `test_using_wasm_udf_with_sdk_and_library`
- [ ] Ручной сценарий: upload sdk → helpers → with_helpers / md5 / throw
- [ ] Ошибка Acquire видна в UI (не `stats.GetTasks().size() == 1`)
- [ ] `ThrowException("…")` доходит как `fail(); ex: …`
