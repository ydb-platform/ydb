# Примеры и тесты

## Emscripten examples

Путь: `ydb/tests/functional/udf_store/examples/`.

| Каталог | Роль | required_libraries |
|---|---|---|
| `sdk/` | Runtime (libc/util), upload как library `"sdk"` | — |
| `helpers/` | Промежуточная библиотека, export `helpers_scale` | — |
| `with_helpers/` | UDF `WithHelpers::scale` | `["sdk", "helpers"]` |
| `md5/` | UDF с libc (MD5) | `["sdk"]` |
| `add/` | Минимальный UDF без libs | `[]` |
| `throw/` | Host `ThrowException` | `[]` |
| `prefix/` | Objects + TypeConfig (`Prefix::Apply`) | `["sdk"]` + PEERDIR `object_framework` |

Сборка:

```bash
ya make --target-platform=clang18-emscripten-wasm64 --build profile \
  ydb/tests/functional/udf_store/examples/sdk \
  ydb/tests/functional/udf_store/examples/helpers \
  ydb/tests/functional/udf_store/examples/with_helpers \
  ydb/tests/functional/udf_store/examples/md5 \
  ydb/tests/functional/udf_store/examples/add \
  ydb/tests/functional/udf_store/examples/throw \
  ydb/tests/functional/udf_store/examples/prefix
```

`webassembly_udf.inc` + `sdk/ld_plugin.py` вырезают sdk-архивы из линковки UDF (sdk подаётся отдельно через store).

Порядок upload для with_helpers:

1. `--kind library --library-name sdk` (бинарник examples/sdk)
2. `--kind library --library-name helpers`
3. UDF with_helpers + `manifest.json` (`required_libraries: ["sdk","helpers"]`)
4. Дождаться `compile_status=ready` у библиотек и модуля
5. `SELECT WithHelpers::scale(7);` → `21`

Prefix (objects):

1. upload library `sdk`
2. upload UDF `prefix` + `manifest.json` (`objects[]`)
3. `$fn = YQL::Udf(AsAtom("Prefix.Apply"), Void(), Void(), AsAtom("pre-")); SELECT $fn("x");` → `pre-x`

---

## WAT-фикстуры для CI

`ydb/tests/functional/udf_store/data/wasm/`:

| Файл | Назначение |
|---|---|
| `local_udf.wat` + `local_udf_manifest.json` | модуль без библиотек (`LocalUdf::udf_add`) |
| `sdk_stub.wat` | stub sdk с bump-malloc (library `"sdk"`) |
| `helpers.wat` | library `"helpers"` |
| `with_helpers.wat` + `with_helpers_manifest.json` | UDF с `["sdk","helpers"]` |

---

## Functional tests

`ydb/tests/functional/udf_store/test_udf_store.py`:

- `test_udf_store_feature_flag` — таблицы/KV при enable/disable
- `test_using_native_unsafe_udf` — native .so path
- `test_using_wasm_udf` — upload WAT, compile, `LocalUdf::udf_add(1,2)==3`
- `test_using_wasm_udf_with_sdk_and_library` — sdk + helpers + module, `WithHelpers::scale(7)==21`

Запуск (из корня ydbwork/ydb):

```bash
./ya make --build relwithdebinfo -tA ydb/tests/functional/udf_store -F '*wasm_udf_with_sdk*'
```

Upload helper: `ENV(YDB_UPLOAD_UDF_PATH=...)`, поддерживает `--kind library`.

---

## Unit tests

`ydb/services/udf_store/ut/`:

| Тест | Что проверяет |
|---|---|
| `manifest_ut` | parse `required_libraries`, functions, `objects` → TypeConfigCallable |
| `compartment_manager_ut` | catalog register/resolve, TLS guard |
| `object_framework_ut` | static registry create/get/destroy, 2 objects |
| `objects_abi_ut` | WAT create/call exports (ui64 handles) |
| `throw_exception_ut` | host ThrowException → reason `fail(); ex: … boom-from-wasm` |
| `with_helpers_ut` | Empty+AddSdk(sdk_stub)+helpers+module, `scale(7)==21` |
| `blob_chunks_ut` | chunk split/join |

```bash
./ya make --build relwithdebinfo -tA ydb/services/udf_store/ut -F '*WithHelpers*'
./ya make --build relwithdebinfo -tA ydb/services/udf_store/ut -F '*ThrowException*'
./ya make --build relwithdebinfo -tA ydb/services/udf_store/ut -F '*ObjectFramework*'
./ya make --build relwithdebinfo -tA ydb/services/udf_store/ut -F '*ObjectsAbi*'
```

---

## Минимальный ручной сценарий на локальном ydbd

1. Включить `udf_store_config.enabled` + `enable_wasm_udf`.
2. Upload library/module через `upload_udf` (или UI/SQL upsert в таблицы).
3. Дождаться `compile_status=ready` в `meta` / `library_source`.
4. Выполнить YQL с `Module::func`.
5. При ошибке смотреть CA log (`Failed to acquire WASM query compartment` / linkage Missing) и issues ответа — не verification stats.
