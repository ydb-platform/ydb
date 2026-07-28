# UDF Store / WASM — стартовый контекст

Документы в этой папке — опорный контекст для доработок загрузки и исполнения WASM UDF
в YDB (`ydb/services/udf_store`, `ydb/library/wasm`, точки входа в KQP).

| Документ | Содержание |
|---|---|
| [wasm-udf-runtime.md](./wasm-udf-runtime.md) | Архитектура: storage → AOT → catalog → per-query compartment → Run |
| [adr-wasm-udf-objects.md](./adr-wasm-udf-objects.md) | ADR: objects / TypeConfig / static object_framework / ui64 |
| [adr-shared-wasm-context.md](./adr-shared-wasm-context.md) | ADR: shared ctx handle + Snapshot for SELECT |
| [pitfalls-and-open-issues.md](./pitfalls-and-open-issues.md) | Известные ловушки, уже найденные баги, открытые вопросы |
| [examples-and-tests.md](./examples-and-tests.md) | Примеры, функциональные/unit тесты, как гонять |

## Быстрый ориентир по дереву кода

```
ydb/services/udf_store/
  service.{h,cpp}                 # оркестрация compile/load по metadata snapshot
  store_initializer.*             # создание таблиц modules / module_chunks
  artifact_table_initializer.*    # per-CPU artifact tables
  wasm_library_compile_actor.*    # AOT библиотек
  wasm_compile_actor.*            # AOT UDF-модулей (ждёт ready библиотек)
  wasm_artifact_load_actor.*      # чтение artifact + Register в FunctionRegistry/catalog
  wasm/
    module_catalog.*              # process-wide артефакты (bytecode + libraries)
    compartment_manager.*         # Acquire → per-query compartment + export map
    query_compartment_scope.h     # RAII scope для CA / literal executer
    registry_helpers.*            # CreateRegistryCompartment, InvokeUdfExport, …
    udf_function.*                # TWasmUdfFunction::Run (TLS query compartment)
    udf_configured_callable.*     # TypeConfig → create → ui64 → call
    object_framework/             # static registry (PEERDIR into UDF modules)
    host.*                        # AllocateBytes / ThrowException host ABI
    compile.*                     # CompileModuleObjectCode (WAVM AOT)
    manifest.*                    # JSON manifest parse (functions + objects)
  metadata_subscription/          # snapshot modules (udf + libraries)

ydb/library/wasm/
  engine/compartment.cpp          # WAVM compartment, AddSdk, AddPrecompiledModule
  api/compartment.h               # IWebAssemblyCompartment

ydb/core/kqp/
  query_data/kqp_predictor.*      # собирает WasmUdfModules из плана
  compute_actor/kqp_*_compute_actor.*  # Acquire scope + Activate TLS
  executer_actor/kqp_literal_executer.cpp
  executer_actor/kqp_executer_stats.cpp  # пустые Tasks при early CA fail

ydb/tests/functional/udf_store/
  examples/{sdk,helpers,with_helpers,md5,add,throw,prefix}/
  data/wasm/                      # WAT-фикстуры для CI
  test_udf_store.py
  upload_udf/
```

Обновляйте эти документы при существенных изменениях контракта
(manifest, порядок библиотек, TLS compartment, host ABI, таблицы).
