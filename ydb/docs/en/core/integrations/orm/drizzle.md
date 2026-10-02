# Drizzle ORM

[Drizzle ORM](https://orm.drizzle.team/) is a lightweight TypeScript ORM for Node.js with a type-safe query builder and a schema definition DSL.

{{ ydb-short-name }} supports integration with Drizzle ORM through the [`@ydbjs/drizzle-adapter`](https://www.npmjs.com/package/@ydbjs/drizzle-adapter) adapter, which is part of the [{{ ydb-short-name }} JavaScript/TypeScript SDK](https://github.com/ydb-platform/ydb-js-sdk). The adapter provides the familiar Drizzle API and accounts for {{ ydb-short-name }} specifics: primary keys, secondary and vector indexes, TTL, partitioning, and column families.

This page is a brief overview of the integration. The detailed guide covering all features (query builders, relational API, migrations, DDL, vector search) is available at [ydb.js.org](https://ydb.js.org/guide/drizzle/).

## Requirements

- Node.js 20.19 or newer
- The `drizzle-orm` and `@ydbjs/drizzle-adapter` packages
- A running {{ ydb-short-name }} instance

## Installation

```bash
npm install @ydbjs/drizzle-adapter drizzle-orm
```

## Connecting

{% list tabs %}

- Connection string

  The simplest way is to pass a connection string to `createDrizzle()`. The adapter creates and manages the {{ ydb-short-name }} driver itself.

  ```typescript
  import { createDrizzle } from '@ydbjs/drizzle-adapter'

  const db = createDrizzle({
    connectionString: process.env['YDB_CONNECTION_STRING']!,
  })
  ```

  The connection string has the form `grpc://localhost:2136/local` (or `grpcs://` for a secure connection), specifying the server address, gRPC port, and the database path.

- Existing SDK driver

  If your application already uses a driver from `@ydbjs/core`, you can wrap it in `YdbDriver` and reuse the shared connection.

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

When the work is done, release the resources if the driver was created by the adapter:

```typescript
db.$client.close?.()
```

## Schema definition

Tables are described with `ydbTable()`. A primary key is required for every table.

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

The adapter supports all the main {{ ydb-short-name }} data types (`Bool`, integer and floating-point types, `Utf8`, `String`, `Uuid`, `Json`, date and time types, and others), as well as {{ ydb-short-name }}-specific schema features: secondary and vector indexes, TTL, partitioning, and column families. The full list of types and options is provided in the [schema guide](https://ydb.js.org/guide/drizzle/schema).

## CRUD operations

```typescript
import { and, eq, like } from 'drizzle-orm'

// Insert
await db.insert(users).values({ id: 1, email: 'alice@example.com', name: 'Alice' }).execute()

// Select with filtering
const filteredUsers = await db
  .select({ userId: users.id, userName: users.name })
  .from(users)
  .where(and(eq(users.id, 1), like(users.email, '%@example.com')))
  .execute()

// Update
await db.update(users).set({ name: 'Alice Cooper' }).where(eq(users.id, 1)).execute()

// Delete
await db.delete(users).where(eq(users.id, 1)).execute()
```

The "insert or update" by primary key operation uses `onDuplicateKeyUpdate()`:

```typescript
await db
  .insert(users)
  .values({ id: 1, email: 'alice_new@example.com', name: 'Alice Updated' })
  .onDuplicateKeyUpdate({ set: { name: 'Alice Updated' } })
  .execute()
```

## Transactions

Transactions are started via `db.transaction()`. {{ ydb-short-name }} access modes and isolation levels are supported, along with an idempotency flag for automatic retries on network errors.

```typescript
import { eq } from 'drizzle-orm'
import { TransactionRollbackError } from 'drizzle-orm/errors'

try {
  await db.transaction(
    async (tx) => {
      // Check a precondition before modifying data
      const inviter = await tx.select().from(users).where(eq(users.id, 1)).execute()
      if (inviter.length === 0) {
        tx.rollback()
      }

      await tx.insert(users).values({ id: 4, email: 'delta@example.com' }).execute()
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

## Limitations

Keep in mind the following {{ ydb-short-name }} and adapter specifics:

- The adapter is distributed in ESM format only.
- Nested transactions are not supported.
- Only {{ ydb-short-name }} isolation levels are allowed (`serializableReadWrite`, `snapshotReadOnly`); other levels are not emulated.
- `references()` is stored as metadata for the relational API — {{ ydb-short-name }} does not enforce foreign keys at the database level.

## See also

- [Detailed adapter guide](https://ydb.js.org/guide/drizzle/)
- [{{ ydb-short-name }} JavaScript/TypeScript SDK on GitHub](https://github.com/ydb-platform/ydb-js-sdk)
- [The `@ydbjs/drizzle-adapter` npm package](https://www.npmjs.com/package/@ydbjs/drizzle-adapter)
