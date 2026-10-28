# pnpm bundle

Текущая версия закреплена в `package.json#version`. Корневой `ya.make` объявляет ресурс
`PNPM` из `pnpm.sbr.json`. Исторические версионные каталоги остаются для старых ссылок.

## Команды

Команды запускаются из `build/external_resources/pnpm`, без установки зависимостей.
Для `node --run` нужен Node.js 22 или новее. CLI находится в `scripts/resource.mjs`,
операции с ресурсом — в `scripts/resource-api.mjs`, механизм патчей — в
`scripts/apply-patches.mjs`. Нужны `curl` и `tar`, для загрузки — авторизованный `ya upload`.

```bash
node --run resource:help

# Каждый этап отдельно, с сохранением ресурса для проверки:
node --run resource:download -- --destination /tmp/pnpm-resource --version 10.33.4
node --run resource:patch -- --directory /tmp/pnpm-resource
node --run resource:upload -- --directory /tmp/pnpm-resource

# Полная цепочка и обновление локальных дескрипторов:
node --run resource:update -- --version 10.33.4
```

Можно запускать напрямую, например:
`node scripts/resource.mjs download --destination /tmp/pnpm-resource`.
Для `download` и `update` версия по умолчанию берётся из `package.json#version`.
`patch` и `upload` используют версию скачанного пакета в переданном каталоге.
Неизвестные команды и аргументы завершаются ошибкой; `--help` показывает API.

### Скачивание и патчи

`resource:download` скачивает архив из
`https://verdaccio.s3-staff.mds.yandex.net/pnpm/`, проверяет имя, версию и отсутствие
production зависимостей, распаковывает в `node_modules/pnpm` и создаёт ссылки `.bin`.
Каталог назначения не должен содержать `node_modules`.

`resource:patch` применяет все `patches/*.mjs` по порядку и проверяет запуск
`pnpm --version`. Повторно применять патчи к тому же bundle не следует:
строгие проверки фрагментов исходного кода завершаются ошибкой.

У 10.33.4 нет production зависимостей. Если они появятся, скачивание остановится:
их установку следует отдельно реализовать через pnpm, только с `--prod`.
Lifecycle scripts не запускаются.

### Загрузка и обновление

`resource:upload` загружает переданный ресурс в Sandbox с бесконечным TTL и
владельцем `FRONTEND_BUILD_PLATFORM`, выводит его URI. Локальные дескрипторы
эта команда не меняет.

`resource:update` выполняет download → patch → upload во временном каталоге,
после успеха обновляет `package.json` и `pnpm.sbr.json` (только `any`).
При ошибке дескрипторы остаются прежними; временный каталог удаляется.
Изменения отправляются обычным PR; команда не коммитит и не создаёт PR.

## Open source mapping

`devtools/ya/opensource/a.yaml` предлагает on-demand action `update_mapping` для
изменений `build/**`. Он строит граф `devtools/ya/opensource/mapping_targets`,
включающий `frontend_build_platform/open_source/perforator`, и извлекает Sandbox
ID из графа. Этот target использует корневой ресурс `PNPM`, поэтому обновление
`pnpm.sbr.json` попадает в граф. ERM для обнаружения pnpm не используется.

При новом Sandbox ID запустите Deploy mapping в Merge checks PR: tasklet
публикует ресурс в публичный devtools-registry S3 и создаёт обновление
`mapping.conf.json`. Сам pnpm updater mapping не публикует.
