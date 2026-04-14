# Распределённая блокировка

Рассмотрим сценарий, где необходимо обеспечить, чтобы с разделяемым ресурсом в один момент времени работал только один экземпляр клиентского приложения. Для его реализации можно воспользоваться механизмом семафоров в [узлах координации {{ ydb-short-name }}](../../reference/ydb-sdk/coordination.md).

## Механизм аренды семафоров

В отличие от локального многопоточного программирования, клиенты в распределённых системах не захватывают блокировки или семафоры напрямую. Вместо этого они арендуют их на определённое время, которое можно периодически продлевать. Поскольку физическое время может различаться на разных машинах, клиенты и сервер могут оказаться в ситуации, когда несколько клиентов считают, что их сессии захватили один и тот же семафор одновременно, даже если с точки зрения сервера это не так. Чтобы уменьшить вероятность таких ситуаций, важно заранее настроить автоматическую синхронизацию времени как на серверах с клиентскими приложениями, так и на стороне {{ ydb-short-name }}, желательно с использованием единого источника времени.

Таким образом, реализация распределённой блокировки через такие механизмы не может гарантировать отсутствие одновременного доступа к ресурсу, но может значительно снизить вероятность такого события. Это может быть использовано как оптимизация, чтобы клиенты не конкурировали за общий ресурс, когда это не имеет смысла. Гарантии отсутствия конкурентных запросов к ресурсу могут быть реализованы на стороне самого ресурса.

## Пример кода

{% list tabs %}

- C++

  ```cpp
  #include <ydb-cpp-sdk/client/coordination/coordination.h>

  void CoordinationExclusiveWork(
      const NYdb::TDriver& driver,
      const std::string& nodePath,
      const std::string& semaphoreName)
  {
      using NYdb::NStatusHelpers;

      NYdb::NCoordination::TClient client(driver);

      ThrowOnError(client.CreateNode(nodePath).ExtractValueSync());

      auto sessionResult = client.StartSession(nodePath).ExtractValueSync();
      ThrowOnError(sessionResult);

      auto session = sessionResult.ExtractResult();

      auto acquireSettings = NYdb::NCoordination::TAcquireSemaphoreSettings();
          .Ephemeral(true)
          .Exclusive()
          .Timeout(TDuration::Minutes(5));

      auto acquireResult = session.AcquireSemaphore(semaphoreName, acquireSettings).ExtractValueSync();
      ThrowOnError(acquireResult);

      if (!acquireResult.GetResult()) {
          // semaphore was not acquired
          return;
      }

      // lock acquired, start processing

      auto releaseStatus = session.ReleaseSemaphore(semaphoreName).ExtractValueSync();
      ThrowOnError(releaseStatus);

      if (!releaseStatus.GetResult()) {
          // semaphore was not released
          return;
      }

      // lock released, end processing
  }
  ```

- Go

   ```go
   for {
     if session, err := db.Coordination().CreateSession(ctx, path); err != nil {
       return fmt.Errorf("cannot create session: %v", err);
     }

     if lease, err := session.AcquireSemaphore(ctx,
       semaphore,
       coordination.Exclusive,
       options.WithEphemeral(true),
     ); err != nil {
       // the session is likely lost, try to create a new one and get the lock in it
       session.Close(ctx);
       continue;
     }

     // lock acquired, start processing
     select {
        case <-lease.Context().Done():
     }

     // lock released, end processing
   }
   ```

- Python

  {% list tabs %}

  - Native SDK

    ```python
    import ydb

    def coordination_service_workflow(driver: ydb.Driver, node_path: str, semaphore_name: str):
        client = driver.coordination_client

        client.create_node(node_path)

        with client.session(node_path) as session:
            with session.semaphore(semaphore_name) as semaphore:
                print("Some exclusive work")

    ```

  - Native SDK (Asyncio)

    ```python
    import os
    import ydb

    async def coordination_service_workflow(driver: ydb.aio.Driver, node_path: str, semaphore_name: str):
        client = driver.coordination_client
        await client.create_node(node_path)
        async with client.session(node_path) as session:
            async with session.semaphore(semaphore_name) as semaphore:
                print("Some exclusive work")
    ```

  {% endlist %}

- C#

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- JavaScript

  ```javascript
  import { Driver } from '@ydbjs/core'
  import { CoordinationClient } from '@ydbjs/coordination'

  const driver = new Driver('grpc://localhost:2136/local')
  const client = new CoordinationClient(driver)

  await using session = await client.createSession('/local/my-app')
  await using lock = await session.mutex('job-lock').lock()
  await doWork(lock.signal)

  // For long lived applications

  for await (let session of client.openSession('/local/my-app')) {
    let mutex = session.mutex('job-lock')

    try {
      // Blocks until the lock is acquired.
      await using lock = await mutex.lock()

      await doWork(lock.signal)
    } catch {
      if (session.signal.aborted) continue // session expired, retry
      throw error
    }

    break
  }
  ```

- Java

  ```java
  CoordinationClient client = CoordinationClient.newClient(transport);
  client.createNode(nodePath).join().expectSuccess();

  try (CoordinationSession session = client.createSession(nodePath)) {
      session.connect().join().expectSuccess();
      SemaphoreLease lease = session.acquireEphemeralSemaphore(semaphoreName, true, Duration.ofMinutes(5))
              .join().getValue();
      try {
          // монопольная работа с ресурсом
      } finally {
          lease.release().join();
      }
  }
  ```

- Rust

  ```rust
  use ydb::{ClientBuilder, NodeConfigBuilder, SessionOptionsBuilder, YdbResult};

  #[tokio::main]
  async fn main() -> YdbResult<()> {
      let client = ClientBuilder::new_from_connection_string("grpc://localhost:2136?database=local")?
          .client()?;
      client.wait().await?;

      let mut coordination_client = client.coordination_client();
      coordination_client
          .create_node(
              "/local/my_lock_node".into(),
              NodeConfigBuilder::default().build()?,
          )
          .await?;

      let session = coordination_client
          .create_session(
              "/local/my_lock_node".into(),
              SessionOptionsBuilder::default().build()?,
          )
          .await?;

      session.create_semaphore("resource", 1, vec![]).await?;
      let _lease = session.acquire_semaphore("resource".into(), 1).await?;
      // критическая секция
      Ok(())
  }
  ```

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}
