# Drizzle ORM

[Drizzle ORM](https://orm.drizzle.team/) — это лёгкая TypeScript ORM для Node.js с типобезопасным построителем запросов и DSL для описания схемы.

{{ ydb-short-name }} поддерживает интеграцию с Drizzle ORM через адаптер [`@ydbjs/drizzle-adapter`](https://www.npmjs.com/package/@ydbjs/drizzle-adapter), который входит в [{{ ydb-short-name }} JavaScript/TypeScript SDK](https://github.com/ydb-platform/ydb-js-sdk). Адаптер предоставляет привычный для Drizzle API и учитывает особенности {{ ydb-short-name }}: первичные ключи, вторичные и векторные индексы, TTL, партиционирование и колоночные семейства.

Эта страница — краткий обзор интеграции. Подробное руководство со всеми возможностями (построители запросов, реляционный API, миграции, DDL, векторный поиск) доступно на [ydb.js.org](https://ydb.js.org/guide/drizzle/).

## Требования

- Node.js 20.19 или новее
- Пакеты `drizzle-orm` и `@ydbjs/drizzle-adapter`
- Доступный экземпляр {{ ydb-short-name }}

## Установка

```bash
npm install @ydbjs/drizzle-adapter drizzle-orm
```

## Подключение

{% list tabs %}

- Строка подключения

  Самый простой способ — передать строку подключения в `createDrizzle()`. Адаптер сам создаст и будет управлять драйвером {{ ydb-short-name }}.

  ```typescript
  import { createDrizzle } from '@ydbjs/drizzle-adapter'

  const db = createDrizzle({
    connectionString: process.env['YDB_CONNECTION_STRING']!,
  })
  ```

  Строка подключения имеет вид `grpc://localhost:2136/local` (или `grpcs://` для защищённого соединения), где указываются адрес сервера, порт gRPC и путь к базе данных.

- Существующий драйвер SDK

  Если приложение уже использует драйвер из `@ydbjs/core`, его можно обернуть в `YdbDriver` и переиспользовать общее соединение.

  ```typescript
  import { Driver } from '@ydbjs/core'
  import { YdbDriver, createDrizzle } from '@ydbjs/drizzle-adapter'

  const driver = new Driver('grpc://localhost:2136/local')
  await driver.ready()
  const db = createDrizzle({
    client: new YdbDriver(driver),
  })
  ```

{% endlist %}

После завершения работы освободите ресурсы, если драйвер был создан адаптером:

```typescript
db.$client.close?.()
```

## Описание схемы

Таблицы описываются с помощью `ydbTable()`. Для каждой таблицы обязателен первичный ключ.

```typescript
import { integer, text, timestamp, ydbTable } from '@ydbjs/drizzle-adapter/schema'

export const users = ydbTable('users', {
  id: integer('id').primaryKey(),
  email: text('email').notNull().unique(),
  name: text('name'),
  createdAt: timestamp('created_at')
    .notNull()
    .$defaultFn(() => new Date()),
})
```

Адаптер поддерживает все основные типы данных {{ ydb-short-name }} (`Bool`, целочисленные и вещественные типы, `Utf8`, `String`, `Uuid`, `Json`, типы даты и времени и др.), а также {{ ydb-short-name }}-специфичные возможности схемы: вторичные и векторные индексы, TTL, партиционирование и колоночные семейства. Полный перечень типов и опций приведён в [руководстве по схеме](https://ydb.js.org/guide/drizzle/schema).

## CRUD-операции

```typescript
import { and, eq, like } from 'drizzle-orm'

// Вставка
await db.insert(users).values({ id: 1, email: 'alice@example.com', name: 'Alice' }).execute()

// Выборка с фильтрацией
const filteredUsers = await db
  .select({ userId: users.id, userName: users.name })
  .from(users)
  .where(and(eq(users.id, 1), like(users.email, '%@example.com')))
  .execute()

// Обновление
await db.update(users).set({ name: 'Alice Cooper' }).where(eq(users.id, 1)).execute()

// Удаление
await db.delete(users).where(eq(users.id, 1)).execute()
```

Для операции «вставить или обновить» по первичному ключу используется `onDuplicateKeyUpdate()`:

```typescript
await db
  .insert(users)
  .values({ id: 1, email: 'alice_new@example.com', name: 'Alice Updated' })
  .onDuplicateKeyUpdate({ set: { name: 'Alice Updated' } })
  .execute()
```

## Транзакции

Транзакции запускаются через `db.transaction()`. Поддерживаются режимы доступа и уровни изоляции {{ ydb-short-name }}, а также флаг идемпотентности для автоматических повторов при сетевых ошибках.

```typescript
import { TransactionRollbackError } from 'drizzle-orm/errors'

try {
  await db.transaction(
    async (tx) => {
      await tx.insert(users).values({ id: 4, email: 'delta@example.com' }).execute()

      const user = await tx.select().from(users).where(eq(users.id, 4)).execute()
      if (user.length === 0) {
        tx.rollback()
      }
    },
    {
      accessMode: 'read write',
      isolationLevel: 'serializableReadWrite',
      idempotent: true,
    }
  )
} catch (error) {
  if (error instanceof TransactionRollbackError) {
    console.log('transaction was rolled back')
  }
}
```

## Ограничения

При использовании адаптера учитывайте особенности {{ ydb-short-name }} и адаптера:

- Адаптер распространяется только в формате ESM.
- Вложенные транзакции не поддерживаются.
- Допустимы только уровни изоляции {{ ydb-short-name }} (`serializableReadWrite`, `snapshotReadOnly`); эмуляции других уровней нет.
- `references()` хранится как метаданные для реляционного API — {{ ydb-short-name }} не контролирует внешние ключи на уровне СУБД.

## Полезные ссылки

- [Подробное руководство по адаптеру](https://ydb.js.org/guide/drizzle/)
- [{{ ydb-short-name }} JavaScript/TypeScript SDK на GitHub](https://github.com/ydb-platform/ydb-js-sdk)
- [npm-пакет `@ydbjs/drizzle-adapter`](https://www.npmjs.com/package/@ydbjs/drizzle-adapter)
