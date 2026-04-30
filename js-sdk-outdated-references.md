# Устаревшие ссылки на JavaScript SDK (требуют обновления до v6 / @ydbjs/*)

Всё перечисленное использует старый пакет `ydb-sdk` или репозиторий `ydb-nodejs-sdk`.
Нужно обновить на новые пакеты `@ydbjs/*` и ссылку https://github.com/ydb-platform/ydb-js-sdk.

---

## _includes/nodejs/ — сниппеты для рецептов аутентификации

Все файлы используют `from 'ydb-sdk'` — нужно переписать под новый API:

- `ydb/docs/ru/core/_includes/nodejs/auth-access-token.md`
  — `import { Driver, TokenAuthService } from 'ydb-sdk'`

- `ydb/docs/ru/core/_includes/nodejs/auth-anonymous.md`
  — `import { Driver, AnonymousAuthService } from 'ydb-sdk'`

- `ydb/docs/ru/core/_includes/nodejs/auth-env.md`
  — `import { Driver, getCredentialsFromEnv } from 'ydb-sdk'`

- `ydb/docs/ru/core/_includes/nodejs/auth-metadata.md`
  — `import { Driver, MetadataAuthService } from 'ydb-sdk'`

- `ydb/docs/ru/core/_includes/nodejs/auth-sa-data.md`
  — `import { Driver, IamAuthService } from 'ydb-sdk'`
  — `import { IIamCredentials } from 'ydb-sdk/build/cjs/src/credentials'`

- `ydb/docs/ru/core/_includes/nodejs/auth-sa-file.md`
  — `import { Driver, getSACredentialsFromJson, IamAuthService } from 'ydb-sdk'`

- `ydb/docs/ru/core/_includes/nodejs/auth-static.md`
  — `import { Driver, StaticCredentialsAuthService } from 'ydb-sdk'`

---

## recipes/ydb-sdk/auth-env.md

- `ydb/docs/ru/core/recipes/ydb-sdk/auth-env.md` строка 101
  — `import { Driver, getCredentialsFromEnv } from 'ydb-sdk'`

---

## reference/ydb-sdk/_includes/auth.md — таблица методов JavaScript

- `ydb/docs/ru/core/reference/ydb-sdk/_includes/auth.md` строки 59–64
  — Все ссылки ведут на `ydb-nodejs-sdk/tree/main/examples/auth/...`
  — Классы `AnonymousAuthService`, `TokenAuthService`, `MetadataAuthService`,
    `getSACredentialsFromJson`, `StaticCredentialsAuthService`, `getCredentialsFromEnv`
    — всё старый API

---

## example-nodejs.md

- `ydb/docs/ru/core/dev/example-app/_includes/example-nodejs.md` строки 3–4
  — Ссылка на `github.com/ydb-platform/ydb-nodejs-sdk/tree/master/examples/basic-example-v2-with-query-service`
  — Ссылка на `github.com/ydb-platform/ydb-nodejs-sdk`
  — Требует полного переписывания примера под v6

---

## public-materials/videos/2020.md

- `ydb/docs/ru/core/public-materials/videos/2020.md` строка 40
  — `[JavaScript SDK](https://github.com/yandex-cloud/ydb-nodejs-sdk)` — старый форк на yandex-cloud

---

## Итого файлов: 11
## Из них требуют полного переписывания кода: 8 (_includes/nodejs/* + auth-env.md + example-nodejs.md)
## Требуют замены ссылок: 3 (auth.md таблица, videos/2020.md)
