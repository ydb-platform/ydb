# WASM UDF Runtime — архитектура

## 1. Цель дизайна

WASM UDF живут в UDF Store и исполняются через WAVM:

1. **Исходники** (`.wasm` / `.wat`) и **манифест** хранятся в системных таблицах.
2. **AOT-компиляция** (WAVM object code) пишется в per-CPU artifact-таблицы.
3. **FunctionRegistry** видит модуль как обычный UDF (`ModuleName::Func`), без живого shared compartment.
4. На **каждый запрос / task** строится **отдельный compartment**: линкуются sdk + libraries + нужные UDF-модули; память и состояние не шарятся между запросами.

Ключевой сдвиг относительно «одного живого compartment на процесс»:  
**каталог артефактов — process-wide, compartment — per-query.**

---

## 2. Высокоуровневый поток

```mermaid
flowchart TD
  Upload["upload_udf<br/>WASM / library / native"] --> Modules["modules + module_chunks"]
  Modules --> Svc["TUdfStoreService<br/>metadata snapshot"]
  Svc --> LibAOT["TWasmLibraryCompileActor<br/>AOT library → artifact"]
  LibAOT --> ModAOT["TWasmCompileActor<br/>AOT module (после ready libs)"]
  ModAOT --> Load["TWasmArtifactLoadActor<br/>читать artifact + libs"]
  Load --> Catalog["TWasmModuleCatalog<br/>Register artifact"]
  Load --> FR["FunctionRegistry<br/>wasm:md5 → TWasmSoModule"]
  FR --> Compile["KQP compile<br/>WasmUdfModules → TKqpPhyStage"]
  Compile --> CA["ComputeActor / LiteralExecuter<br/>TQueryCompartmentScope::Acquire"]
  CA --> Run["TWasmUdfFunction::Run<br/>TLS query compartment"]
```

---

## 3. Хранение

Префикс: `.metadata/udf_store` (см. `metadata_subscription/storage_paths.*`).

| Таблица | Назначение |
|---|---|
| `modules` | Унифицированные записи: PK=`uid`, `md5`, `name`, `type`=`WASM`\|`LIBRARY`\|`NATIVE_UNSAFE`, manifest, version, size, chunk_count, compile_status/error, created_at, compile_started_at, compile_finished_at |
| `module_chunks` | Тело исходника (chunked blob), `owner_key`=`uid` |
| `artifact_<cpu_spec>` + chunks | AOT: wasm_data + object_code для module **или** library (без изменений) |

`cpu_spec` — нормализованный triple/cpu узла (`DetectLocalCpuSpec` / `WasmCpuSpecOverride`), чтобы object code был валиден на данной машине.

Artifact id остаётся `md5` для module и `name` для library. FunctionRegistry не переводится на uid.

Список текущих модулей (без доступа к `.metadata`):

```sql
SELECT Uid, Name, ModuleType, CompileStatus, Md5
FROM `.sys/udf_modules`
WHERE ModuleType = "WASM";
```

Колонки view: `Uid`, `Md5`, `Name`, `ModuleType`, `Version`, `Size`, `ChunkCount`, `CompileStatus`, `CompileError`, `CreatedAt`, `CompileStartedAt`, `CompileFinishedAt`, `Manifest`.

Upload helper: `ydb/tests/functional/udf_store/upload_udf`  
(`--action upload|delete`, `--kind udf|library`; для library нужен `--library-name`;
delete udf — `--md5` или `--udf-file`; delete чистит modules(+chunks) и best-effort AOT artifacts).

---

## 4. Манифест модуля

JSON, парсится `wasm/manifest.*` → `TWasmManifest`:

```json
{
  "module_name": "WithHelpers",
  "calling_convention": "unversioned_value",
  "module_extension": "wasm",
  "required_libraries": ["sdk", "helpers"],
  "functions": [
    {
      "name": "scale",
      "argument_types": [{"value": "int64", "tag": "concrete_type"}],
      "result_type": {"value": "int64", "tag": "concrete_type"}
    }
  ]
}
```

Важные поля:

- **`module_name`** — имя в YQL (`WithHelpers::scale`).
- **`module_extension`** — `wasm` | `wat` | `wast` → формат исходника / artifact.
- **`required_libraries`** — **упорядоченный** список имён библиотек (`modules.type=LIBRARY`).
  - **Первая** библиотека ставится как runtime via `AddSdk` / `CreateImageFromSdk` → модуль линкуется как **`"env"`**.
  - Остальные — `AddPrecompiledModule(..., name)` под своим именем (например `"helpers"`).
- **`functions`** — plain экспорты и типы ABI (`unversioned_value`).
- **`objects`** — stateful UDF с TypeConfig (ParseTskv-style). Разворачиваются в `functions` с `yql_binding=type_config_callable` (create/call/destroy exports). Реестр объектов — **static** `object_framework`, не отдельный wasm в `required_libraries`.

Пустой `required_libraries` → compartment из `CreateEmptyImage()` (standard host intrinsics: `AllocateBytes`, `ThrowException`).  
Если нужны `malloc`/`free` без пользовательского sdk — это отдельная тема (см. pitfalls).

### Objects / TypeConfigCallable

```json
{
  "module_name": "Prefix",
  "required_libraries": ["sdk"],
  "objects": [
    {
      "name": "Prefix",
      "create_export": "prefix_create",
      "destroy_export": "prefix_destroy",
      "methods": [{
        "name": "Apply",
        "export": "prefix_apply",
        "yql_binding": "type_config_callable",
        "argument_types": [{"value": "string", "tag": "concrete_type"}],
        "result_type": {"value": "string", "tag": "concrete_type"}
      }]
    }
  ]
}
```

YQL UX (opaque TypeConfig blob — host не парсит):

```sql
$fn = YQL::Udf(AsAtom("Prefix.Apply"), Void(), Void(), AsAtom("pre-"));
SELECT $fn("x");  -- "pre-x"
```

Host path (`TWasmConfiguredCallable`):

1. На первом `Run` (и после смены compartment generation): pin config blob через `compartment->AllocateBytes` + `memcpy`, вызвать `create_export(config)` → **ui64 handle**.
2. Каждый `Run`: `call_export(handle, args…)`.
3. Handle валиден только в текущем query compartment; при новом Acquire — recreate.

Статическая библиотека: `wasm/object_framework/` (`ObjectFrameworkCreate/Get/Destroy`). Модуль линкует через `PEERDIR`, не upload’ит framework.wasm.

Имена методов становятся YQL-именами функций (`Prefix::Apply`) и должны быть уникальны в манифесте.

При `create_export` хост также синтезирует **`New` → ui64`** (plain), если имя свободно. Methods с `yql_binding: "plain"` вызывают `export` напрямую (например `Snapshot(ctx) → string`). Optional `"export"` на `functions[]` — wasm-имя ≠ YQL `name`.

Shared context: один модуль линкует `object_framework`, передаёт `uint64` handle между `CountRow` / `CountPositive` и `Snapshot` (см. `examples/ctx/`, [adr-shared-wasm-context.md](./adr-shared-wasm-context.md)).

---

## 5. AOT-компиляция

### Библиотека — `TWasmLibraryCompileActor`

1. Читает строку `modules` (`type=LIBRARY`) + `module_chunks` по `uid`.
2. Ставит `compile_status=compiling` и `compile_started_at`.
3. `CompileModuleObjectCode(body, format)` (`wasm/compile.cpp`, WAVM `compileModule` → object code).
4. Пишет artifact (`kind=library`, id = library name): wasm_data + object_code chunks.
5. `compile_status = ready|failed` + `compile_finished_at` в `modules`.

### Модуль — `TWasmCompileActor`

1. Не стартует, пока `AreLibraryDependenciesReady(manifest)` (все `required_libraries` в snapshot со статусом `ready`).
2. Читает `modules` по md5 + chunks по `uid`, ставит `compiling` / `compile_started_at`.
3. Проверяет, что artifact’ы библиотек существуют.
4. Валидирует экспорты (`CollectWasmExports` vs plain `functions[].name` и create/call/destroy для `objects`).
5. Компилирует user module → artifact (`kind=module`, id = md5).
6. Обновляет `modules.compile_status` + `compile_finished_at`.

При смене/удалении библиотеки сервис может выгрузить зависящие WASM UDF и перезапустить compile (`UnloadWasmUdfsDependingOnLibrary`, `RetryPendingWasmCompilesForLibrary`).

---

## 6. Загрузка в процесс — `TWasmArtifactLoadActor`

После `compile_status=ready`:

1. Читает module artifact (wasm_data + object_code).
2. По порядку `required_libraries` читает library artifacts → `TVector<TNamedModuleBytecode>`.
3. `LoadWasmFromManifest` / `BuildModuleStateFromManifest`:
   - кладёт `TWasmModuleArtifact` в **`TWasmModuleCatalog`** (md5 + module_name → bytecode + libraries);
   - строит `TWasmSoModule` / `TWasmUdfFunction` и регистрирует в FunctionRegistry по пути `wasm:<md5>` с именем `module_name`.

На этом этапе **живой compartment для исполнения не создаётся** — только метаданные и object code в каталоге.

---

## 7. Module catalog vs per-query compartment

### `TWasmModuleCatalog` (process-wide)

- Хранит immutable-артефакты: манифест, module bytecode, список library bytecodes.
- Индексы: by md5, by module name.
- Потокобезопасный mutex.

### `TWasmCompartmentManager::Acquire(moduleNames)`

На каждый запрос (точнее — на scope в CA / literal executer):

1. `EnsureUdfHostIntrinsicsRegistered()` — host ABI в WAVM standard intrinsics.
2. `Catalog.ResolveModules(moduleNames)`.
3. Собирает уникальные libraries в порядке `RequiredLibraries` (первая — sdk/env).
4. `CreateRegistryCompartment(libraries)`:
   - `libraries.empty()` → `CreateEmptyImage()`;
   - иначе → `CreateImageFromSdk(first)` + `AddPrecompiledModule` для остальных.
5. Для каждого артефакта: `AddPrecompiledModule(moduleBytecode, moduleName)`.
6. Резолвит экспорты в map `"ModuleName::Export" → void*` (`MakeExportKey`) — для plain это YQL-имя, для objects — create/call/destroy.
7. Выставляет `Generation` (monotonic) на handle — TypeConfig callable пересоздаёт объекты при смене generation.

Результат — `TQueryCompartmentHandle` (compartment + Exports + Generation) под `std::shared_ptr`. Основной владелец — `TQueryCompartmentScope`, но резидентные строки держат свою ссылку через `TWasmAllocationRegistry` (см. «Время жизни резидентной строки»), поэтому compartment живёт до последнего значения в linear memory.

### TLS

Два уровня «текущего» указателя:

| TLS | Назначение |
|---|---|
| `TCurrentQueryCompartmentGuard` / `GetCurrentQueryCompartment()` | query handle с Exports (нужен `TWasmUdfFunction::Run`) |
| `TCurrentCompartmentGuard` / `GetCurrentCompartment()` (NYdb::NWasm) | WAVM compartment для PtrFromVM / host ThrowException |

`TQueryCompartmentScope::MakeTlsGuard()` ставит query TLS; внутри `Run` дополнительно ставится NYdb compartment guard.

---

## 8. Связка с KQP

1. **Compile / predictor** (`kqp_predictor`): обходит план, на `TCoUdf` ставит `HasUdf` и собирает имена модулей.
2. **Резолвер string-колонок** (`kqp_wasm_string_columns`) для каждого стейджа отдельно ведёт аргументы `Apply(Udf, …)` назад к физическим колонкам чтения этого же стейджа (`KqpRowsSourceSettings::Columns` у источника, `KqpWideRead*::Columns` у чтения внутри программы). Прослеживаются только шаги, сохраняющие сам буфер: `Member` физического row, аргументы `ExpandMap` / `WideMap` по индексу, AutoMap-развёртка (`Map` / `FlatMap` / `IfPresent`), `Just` / `Unwrap` / `Coalesce`; всё остальное — fail-closed.
3. Колонки попадают в `WasmUdfStringColumns` **только если стейдж содержит и чтение, и UDF**: буфер в линейной памяти WASM не переживает канал между стейджами, поэтому cross-stage пометки бессмысленны.
4. Модули пишутся в **KQP** `TKqpPhyStage.WasmUdfModules`; string columns остаются в `TProgram::TSettings.WasmUdfStringColumns` и копируются в scan/source settings.
5. При сериализации task (`SerializeTaskToProto`) список модулей кладётся в `TaskParams["_WasmUdfModules"]` (newline-separated).
6. **Compute actor** / **literal executer**:
   - CA читает `TaskParams`; literal — напрямую `stage.GetWasmUdfModules()`;
   - `TQueryCompartmentScope(modules)` → `FilterLoadedWasmUdfModules` (только каталог) → `Acquire`;
   - на init scan: `ApplyWasmUdfStringColumns` → `PreferWasm` только для имён из settings;
   - на обработке событий / DoExecute: `MakeTlsGuard()` → TLS guard;
   - при ошибке Acquire — `ErrorFromIssue` / failure state **до** `SetTaskRunner`.
7. Материализация scan: marked string → `MakePreferWasm` (1-copy cell→WASM); остальные large strings → host `MakeString`. Если колонка всё же уйдёт в WASM UDF при false negative — `FillAbiStringArg` сделает `CopyIntoCompartment`.
8. Исполнение UDF → `TWasmUdfFunction::Run` / `TWasmConfiguredCallable::Run` читает TLS query compartment.

Ошибка Acquire до появления task stats раньше маскировалась `AFL_ENSURE(stats.GetTasks().size() == 1)` в `kqp_executer_stats.cpp`; пустые Tasks при early failure теперь пропускаются, чтобы клиент видел исходный issue.

**Переключатель:** `TableServiceConfig.EnableWasmUdfResidentStringColumns` (default true, инвалидирует compile cache) — кластерный дефолт; на сессию переопределяется `PRAGMA ydb.EnableWasmUdfResidentStringColumns = "false"`. Выключение оставляет host-строку и `CopyIntoCompartment` на каждый вызов — это baseline для замеров и путь отката.

**Наблюдаемость:** `TPreferWasmStats` (`wasm/prefer_wasm_stats.h`) считает помеченные колонки, материализации в WASM, `CopyIntoCompartment` / reuse resident и fallback без compartment. `FallbackNoCompartment > 0` означает, что колонки помечены для стейджа без query compartment, то есть чтение и UDF всё-таки разъехались по разным task — планирование сломано.

**Что реально экономится.** Замеры (`wasm/benchmark`, циклы на строку-колонку):

| Операция | Стоимость |
|---|---|
| Гостевой вызов `malloc` + `free` через экспорты wasm | ~225 циклов на пару |
| Поиск экспорта по имени | ~70 циклов |
| Host-строка (`MakeString`, 4 KB) | ~112 циклов |
| Копия 4 KB в linear memory | ~320 циклов |
| `MakePreferWasm` (4 KB: гостевой malloc + копия + регистрация + освобождение) | ~478 циклов |
| Переиспользование резидентных байт в аргументе | ~38 циклов |

Полная стоимость значения (материализация плюс вызовы UDF), host против resident:

| Размер / вызовов | Host | Resident |
|---|---|---|
| 64 B × 1 | 389 | 486 |
| 4 KB × 1 | 528 | 550 |
| 8 KB × 1 | 599 | 709 |
| 16 KB × 1 | 865 | 852 |
| 32 KB × 1 | 3668 | 2084 |
| 64 KB × 1 | 6644 | 3673 |
| 256 KB × 1 | 25.0K | 13.4K |
| 4 KB × 2 | 930 | 597 |
| 64 KB × 4 | 17.4K | 4545 |

При одном вызове UDF на значение до 16 KB два пути равны в пределах шума (лишняя работа `MakePreferWasm` — регистрация в `TWasmAllocationRegistry` под локом — примерно равна сэкономленной копии), с 32 KB resident выигрывает в 1.8 раза. При двух вызовах resident впереди уже на 4 KB (1.6 раза), при четырёх на 64 KB — почти в 4 раза.

Цена материализации сильно зависела от диагностики реестра: `Register`/`TryFree` собирали `TStringBuilder` с сообщением на каждое значение, даже когда `YDB_WASM_STRING_DEBUG` выключен, и это стоило ~1000 циклов на строку (`MakePreferWasm` 4 KB: 1557 против 478 после того, как сообщения стали строиться лениво). Вторая часть — keep-alive: ссылку на handle берёт `TQueryCompartmentScope` один раз через `RetainOwner`, а не `Register` на каждое значение, поэтому на горячем пути не остаётся ни атомиков на `shared_ptr`, ни вставки/удаления узла в карте generation.

**Цена host→guest вызова.** Изначально пара `malloc`/`free` стоила 126K циклов, и это перекрывало любую экономию на копиях. Причина не в WAVM: страховка от переполнения стека (`CheckStackDepth` → `CheckFreeStackSpace`) запрашивала границы стека на входе в каждую гостевую функцию, а `TCurrentThreadLimits` вызывает `pthread_getattr_np`, который на главном потоке парсит `/proc/self/maps` (strace показывал ровно два `openat` на итерацию). Границы фиксированы на всё время жизни потока, поэтому теперь они кэшируются в `thread_local` (`engine/compartment.cpp`), и пара `malloc`/`free` стоит 233 цикла вместо 126K. Сам запрос границ: 49K циклов на главном потоке против 393 на pthread (`Part_StackBoundsQuery_*`) — то есть на actor-потоках сервера, где реально исполняются UDF, экономия составляет ~390 циклов на гостевой вызов (вызов подешевел примерно в 4 раза), а не 19 мкс.

**Что видно на живом кластере.** `bench_prefer_wasm.sh` (`ParseBlob::parse_blob` над колонкой, один вызов на строку, медиана 15 чередующихся прогонов) даёт разницу в пределах шума: 1024 × 64 KB — −2.2% wall / −0.7% cpu, 256 × 256 KB — −1.3% wall / −0.8% cpu; знак между повторами меняется. Профиль запроса объясняет, почему: ~60% сэмплов приходится на 200-байтовый JIT-цикл самого wasm-UDF (побайтовый xor 64 KB) и ещё ~25% на один горячий цикл хоста, так что подготовка аргумента теряется в фоне. Собрать в SQL профиль «несколько вызовов на значение», где выигрыш и появляется, повторением одного и того же вызова нельзя — YQL схлопывает идентичные `Apply(Udf, …)` в один (4 одинаковых вызова стоят +5% к одному, а не ×4); нужен стейдж, где колонка уходит в **разные** UDF.

Как перемерить:

```bash
./ya make -r ydb/services/udf_store/wasm/benchmark && ./ydb/services/udf_store/wasm/benchmark/benchmark
./ya make -tA ydb/services/udf_store/ut -F '*PreferWasmString*'   # инвариант «1 копия вместо 2» на счётчиках
# A/B на живом кластере через PRAGMA; режимы чередуются, чтобы прогрев не смещал результат
ROWS=1024 BLOB_SIZE=65536 RUNS=15 SKIP_SEED=1 \
    ydb/tests/functional/udf_store/examples/parse_blob/bench_prefer_wasm.sh
```

**Время жизни резидентной строки:** `Make` отдаёт pod с нулевым refcount (конвенция MiniKQL `MakeString`), поэтому владелец доводит счётчик до нуля на UnRef и буфер освобождается сразу после строки, а не копится в linear memory до конца запроса.

Опасность в том, что у резидентной строки в linear memory лежит и сам refcount-заголовок: если compartment уничтожить раньше значения, `TStringValue::TData::UnRef` читает `Refs_` из размапленной памяти и нода падает по SIGSEGV. Так и падал `COUNT(DISTINCT ParseBlob::blob_head(blob))` при `PreferWasm = true`: `TKqpComputeActor::Terminate` звал `DoTerminateImpl()` (снос task runner и графа, где `DISTINCT`-состояние держит значения) **после** `PassAway()`, то есть уже после деструктора актора вместе с его `TQueryCompartmentScope`. Исправление двойное:

- `dq_compute_actor_impl.h`: `DoTerminateImpl()` вызывается до `PassAway()` — граф сносится, пока актор и его scope живы;
- shared-владение: `TWasmAllocationRegistry::RetainOwner` получает keep-alive `shared_ptr` на handle от `TQueryCompartmentScope` (один раз на acquire, а не на значение) и держит его, пока у generation есть живые аллокации. `~TQueryCompartmentScope` вызывает `ReleaseOwner(Generation)`: если значений уже нет — compartment уничтожается сразу, если есть — generation помечается `OwnerReleased`, `FreeBytes` для оставшихся значений пропускается (возвращать байты гостевому аллокатору перед сносом памяти незачем), а последний `TryFree` роняет keep-alive и compartment. Keep-alive всегда отпускается **вне** локов реестра: деструктор handle сам заходит в `ForgetGeneration`, и под локом это давало дедлок.

Итог: любое значение может пережить свой scope, и порядок сноса больше не важен для корректности. Регрессии закрыты тестами `TPreferWasmStringTest::ResidentValueOutlivesQueryScope` и `AllocationRegistryOwnerOutlivesLiveAllocations`.

**Второй путь освобождения — LLVM codegen.** `TStringValue::TData::UnRef` спрашивает `UdfTryFreeExternalString` (наша сильная реализация ходит в реестр), но JIT-код MiniKQL этот метод не вызывает: `UnRefUnboxed` инкрементит счётчик инлайном и на нуле зовёт `DeleteString` (`mkql_computation_node_codegen.cpp`), который отдавал байты прямо в `UdfFreeWithSize`. То есть резидентная строка, последняя ссылка на которую умирала внутри скомпилированного графа (а это обычный случай: `TFromFlowWrapper::Fetch_`), уходила аллокатору MiniKQL, который эту память не выделял, — `VERIFY failed: Double free at: 0x...` и падение ноды. Теперь `DeleteString` повторяет порядок `UnRef`: сначала хук, потом `UdfFreeWithSize`. Тест `TPreferWasmStringTest::CodegenDeleteStringReachesRegistry` зовёт `DeleteString` напрямую и без исправления воспроизводит тот же abort.

Практический вывод для новых типов «внешней» памяти под значения MiniKQL: путей освобождения два (интерпретируемый `UnRef` и `DeleteString` из codegen), и хук нужен в обоих.

**Ограничения:** резолвер не различает wasm- и native-UDF (каталог модулей известен только в рантайме), поэтому в стейдже с обоими видами колонка native-UDF тоже может быть помечена — это лишняя запись в WASM, но не ошибка. Неотслеживаемые формы (literals / computed / join / результаты других UDF) дают false negative с корректным fallback через host + copy. Blocks path и lazy holder — вне скоупа.

## 9. Host ABI и calling convention

Файлы: `wasm/host.*`, `wasm/abi/udf_cpp_abi.h`.

Зарегистрированы на **standard** intrinsic module (`getIntrinsicModule_standard()`):

- `AllocateBytes(context, size)` — аллокация через `TWasmUdfInvocationContext::WebAssemblyPool` (не через wasm `malloc`, когда вызывается host intrinsic).
- `ThrowException(const char*)` — `yexception` с текстом из wasm memory (`PtrFromVM` + `GetCurrentCompartment()`) и WAVM call stack (`captureCallStack` / `describeCallStack`).

`compartment->AllocateBytes` (engine) идёт через **экспорт `malloc` у RuntimeLibraryInstance_** (sdk / AddSdk). Поэтому:

- с пользовательским `sdk` в `required_libraries[0]` нужны рабочие `malloc`/`free` в этом sdk;
- stub sdk для тестов обязан экспортировать bump-`malloc` (см. `data/wasm/sdk_stub.wat`).

Calling convention `unversioned_value`: указатели на `TUnversionedValue` в wasm memory; типы int64/uint64/double/bool/string/null.

Ошибки из wasm: `ThrowException` → C++ exception → `WasmError` → `UdfTerminate("name(); ex: …")` (+ call stack).

Unload WASM: при delete/replace вызывается `NKqp::IDynamicFunctionRegistry::RemoveModule(moduleName)` (через cast от `IMutableFunctionRegistry`); иначе reupload того же `module_name` падает с `UDF module duplication`, а в registry остаётся старый набор функций.
---

## 10. Линковка модулей (WAVM)

`ydb/library/wasm/engine/compartment.cpp`:

- `CreateEmptyImage` — standard intrinsics как `"env"` (+ host после `EnsureUdfHostIntrinsicsRegistered`).
- `CreateImageFromSdk` — Empty + `AddSdk` (кэш `TSdkImageCache`); sdk instance = RuntimeLibraryInstance.
- `AddPrecompiledModule(bytecode, name)`:
  - требует **ObjectCode**;
  - IR: Binary → `loadBinaryModule(Data)`, HumanReadable (wat) → `ParseWast(Data)`;
  - instantiate под именем `name` (`"env"` для sdk, `"helpers"` для библиотеки, `ModuleName` для UDF).
- Импорты UDF вида `(import "helpers" "helpers_scale" …)` резолвятся в предыдущие instance’ы.

Порядок в compartment при `required_libraries: ["sdk", "helpers"]`:

1. env = Empty intrinsics + sdk (AddSdk);
2. module `"helpers"`;
3. module `"WithHelpers"` (UDF).

---

## 11. Примеры конфигураций

| Сценарий | required_libraries | Комментарий |
|---|---|---|
| Минимальный WAT без libc | `[]` | Empty image + host; без wasm-malloc в RuntimeLibrary |
| throw (`Throw::fail`) | `["sdk"]` | host `ThrowException` + call stack |
| oob (`Oob::crash` / `bad_index` / `null_deref` / `bad_ref`) | `[]` | WAVM OOB / null+offset / poison-ref traps + call stack |
| md5 | `["sdk"]` | полный emscripten sdk как env |
| with_helpers | `["sdk", "helpers"]` | sdk + промежуточная библиотека + модуль |
| prefix (objects) | `["sdk"]` | TypeConfig + `object_framework` PEERDIR |

C++ examples (emscripten): `tests/functional/udf_store/examples/`.  
CI без emscripten: WAT в `tests/functional/udf_store/data/wasm/`.

---

## 12. Инварианты (не ломать без явной причины)

1. **Compartment на запрос**, не shared live compartment между запросами.
2. **Первая** `required_libraries` entry = runtime/`"env"` через `AddSdk`, не через `AddPrecompiledModule(..., "sdk")` с именем `"sdk"`.
3. Перед линковкой UDF в Acquire — `EnsureUdfHostIntrinsicsRegistered()`.
4. `TWasmUdfFunction::Run` требует активный query TLS (`GetCurrentQueryCompartment()`).
5. Object code обязателен для библиотек и модулей в runtime path.
6. Compile модуля ждёт `compile_status=ready` у всех библиотек из манифеста.
7. Object registry — static link в модуль (`object_framework`), не отдельная entry в `required_libraries`.
8. Host хранит только **ui64** handle (+ generation); семантика TypeConfig blob — у модуля.
