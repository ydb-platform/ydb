## Параметризованные запросы

{{ ydb-short-name }} поддерживает [параметризованные запросы](https://en.wikipedia.org/wiki/Prepared_statement). В таких запросах данные передаются отдельно от самого тела запроса, а в SQL-запросе используются специальные параметры для обозначения местоположения данных.

Запрос с данными в теле запроса:

```yql
SELECT sa.title AS season_title, sr.title AS series_title
FROM seasons AS sa INNER JOIN series AS sr ON sa.series_id = sr.series_id
WHERE sa.series_id = 15 AND sa.season_id = 3
```

Соответствующий ему параметризованный запрос:

```yql
DECLARE $seriesId AS Uint64;
DECLARE $seasonId AS Uint64;

SELECT sa.title AS season_title, sr.title AS series_title
FROM seasons AS sa INNER JOIN series AS sr ON sa.series_id = sr.series_id
WHERE sa.series_id = $seriesId AND sa.season_id = $seasonId
```

Параметризованные запросы записываются в форме шаблона, в котором определенного вида имена заменяются конкретными параметрами при каждом выполнении запроса. Лексемы начинающиеся со знака `$` такие, как `$seriesId` и `$seasonId` в запросе выше, используются для обозначения параметров.

Параметризованные запросы обеспечивают следующие преимущества:

* При повторяющихся запросах сервер базы данных имеет возможность кэшировать план запроса для параметризованных запросов. Это радикально снижает потребление CPU и повышает пропускную способность системы.
* Использование параметризованных запросов спасает от уязвимостей вида [SQL Injection](https://ru.wikipedia.org/wiki/%D0%92%D0%BD%D0%B5%D0%B4%D1%80%D0%B5%D0%BD%D0%B8%D0%B5_SQL-%D0%BA%D0%BE%D0%B4%D0%B0).

Однако у использования параметризованных запросов в {{ ydb-short-name }} есть недостатки. 

- Параметризованные запросы существуют, пока сессия {{ ydb-short-name }} "жива". В {{ ydb-short-name }} сессия — однопоточный [актор](../../concepts/glossary.md#актор-actor) в [распределенной системе](../../concepts/glossary.md#распредёленная-конфигурация-distributed-configuration). Сессия может быть инвалидирована в результате серверной балансировки или работ по обслуживанию на стороне {{ ydb-short-name }}. Если сессия исчезает, подготовленные на ней запросы тоже исчезают, и пользователю приходится разбираться в кодах ошибок {{ ydb-short-name }}, чтобы выяснить, когда нужно инвалидировать запросы, а когда нет. Например, если в {{ ydb-short-name }} приходит запрос с неизвестным `query_id`, то возвращается ошибка `NotFound`.

- Если по каким-то причинам [нода](../../concepts/glossary.md#узел-node) {{ ydb-short-name }} или сессия на ней недоступна, то и заранее подготовленный параметризованный запрос тоже недоступен. Пользователю приходится готовить запрос на другой сессии, а возможно, и на другой ноде. 

- При подготовке параметризованного запроса все последующие `Execute` попадают на один и тот же узел {{ ydb-short-name }}. Это значит, что, скорее всего, возникнет перекос в нагрузке: некоторые узлы будут перегружены, некоторые будут простаивать. 

- В случае параметризованных запросов [клиентская балансировка](../../recipes/ydb-sdk/balancing.md) не работает, потому что запрос жестко привязан к сессии и узлу {{ ydb-short-name }}

Опасно использовать параметризованные запросы, когда `statement` является внешней переменной относительно ретраера: 

``` go
func queryRetry(stmt, args) (res, err) {
   for i := 0; i < 10; i++ {
       // stmt может быть инвалидирован на одной из прошлых попыток, 
       // а мы продолжаем на нем ретраить
       res, err = stmt.Execute(args)
       if err == nil {
           return res, nil
       }
   }
   return nil, err
}
...
// здесь кроется опасность: stmt сохранен для последующего использования,
// и его статус никак не отслеживается
stmt := sessionPool.Get().Prepare(query)
...
err := queryRetry(stmt, args)
``` 

Если `Prepare` вызывается внутри ретраера, то риск сохранить `statement` для последующего использования устраняется, но требуется лишний запрос на сервер в каждой итерации ретраера.

``` go 
// Какой-то ретраер (из sdk или свой)
func queryRetry(sessionPool, query, args) (res, err) {
   for i := 0; i < 10; i++ {
       session := sessionPool.Get()
       // если на сессии уже был подготовлен такой запрос, то это будет быстрый хоп.
       // в противном случае будет честная компиляция запроса
       stmt := session.Prepare(query)  
       res, err = stmt.Execute(args)
       if err == nil {
           return res, nil
       }
   }
   return nil, err
}
```

{% note tip %}

В {{ ydb-short-name }} есть более эффективный способ заставить запрос скомпилироваться один раз и выполнить несколько последующих запросов с результатом компиляции.

Выполнить параметризованный вызов `Execute` в сеансе с установленным флагом `KeepInCache`. 

В этом случае первый запрос к узлу {{ ydb-short-name }} приводит к компиляции запроса, а результат компиляции кэшируется на узле {{ ydb-short-name }}.

{% endnote %}

Замена двух вызовов `<Prepare + Execute>` одним `Execute` с флагом `KeepInCache`.

#|
|| **С использованием `Prepare`** | **С флагом `KeepInCache`** ||
|| `session := sessionPool.Get()`
   `stmt := session.Prepare(query) stmt.Execute(`
   `  query, args, WithKeepInCache(),`
   `)`
| `session := sessionPool.Get()`
  `res, err = session.Execute(`
  `  query, args, WithKeepInCache(),`
  `)` ||
|#              

С помощью `<Execute+KeepInCache>` обеспечиваются преимущества клиентской балансировки: запросы равномерно распределяются по узлам {{ ydb-short-name }} и компилируются по первому `Execute` запросу на каждом новом узле, поэтому кэш запросов также дублируется на узлах {{ ydb-short-name }}.

{{ ydb-short-name }} SDK автоматически кэшируют планы параметризованных запросов по умолчанию, для этого обычно используется настройка `KeepInCache = true`.

В SDK обеспечивается комфортное для пользователя и максимально ожидаемое поведение по умолчанию, в том числе касательно серверного кэша. Примеры явного управления серверным кэшем скомпилированных запросов из SDK:

{% list tabs %}

- YDB-GO-SDK

  Флаг `KeepInCache` устанавливается автоматически (если передан хотя бы один аргумент запроса), чтобы явно отключить серверное кэширование запроса, можно передать опцию с `Keepincache(false)`

  ``` go
  err = db.Table().Do(ctx, func(ctx context.Context, s table.Session) (err error) {
    _, res, err = s.Execute(ctx, table.DefaultTxControl(), query,
        table.NewQueryParameters(params...),
        // uncomment if need to disable query caching
        // options.WithKeepInCache(false), 
    )
    return err
  })
  ```

- YDB-JAVA-SDK

  Флаг `KeepInCache` по умолчанию установлен в значение `true` в объекте класса ExecuteDataQuerySettings. При необходимости флаг `KeepInCache` можно отключить:

  ``` java
  CompletableFuture<Result<DataQueryResult>> result = session.executeDataQuery(
    query,
    txControl,
    params,
    new ExecuteDataQuerySettings()
        // uncomment if need to disable query caching
        // .disableQueryCache()
  );
  ```

- YDB-JDBC-DRIVER

  Cерверное кеширование задается через параметр строки подключения `&keepInQueryCache=true&alwaysPrepareDataQuery=false` параметрами строки подключения JDBC.

- YDB-CPP-SDK

  Чтобы поместить запрос в кэш, используйте метод `KeepInQueryCache` объекта класса `TExecDataQuerySettings`.

  ``` cpp
  NYdb::NTable::TExecDataQuerySettings execSettings;
  // uncomment if need to enable query caching
  // execSettings.KeepInQueryCache(true);
  auto result = session.ExecuteDataQuery(
  query, TTxControl::BeginTx().CommitTx(), execSettings
  )
  ```

- YDB-PYTHON-SDK

  Флаг `KeepInCache` устанавливается автоматически, если передается хотя бы один аргумент запроса. Это поведение нельзя переопределить. Если кэш запросов на стороне сервера не требуется, рекомендуется использовать `Prepare` явно.

- YDB-DOTNET-SDK

  Флаг `KeepInQueryCache` по умолчанию установлен в значение `true` в объекте класса `ExecuteDataQuerySettings`. При необходимости флаг `KeepInQueryCache` можно отменить:

  ```
  var response = await tableClient.SessionExec(async session =>
    await session.ExecuteDataQuery(
        query: query,
        txControl: txControl,
        settings: new ExecuteDataQuerySettings { KeepInQueryCache = false }
    ));
  ``` 

- YDB-RS-SDK

  На данный момент в Rust нет отдельного метода для компиляции запроса, серверный кэш также пока не используется.

- YDB-PHP-SDK

  Флаг `keepInCache` по умолчанию установлен в значение `true` в объекте класса `query`. При необходимости флаг `keepInCache` можно отменить:

  ``` php
  $table->retryTransaction(function(Session $session){
    $query = $session->newQuery($yql);
    $query->parameters($params);
    $query->keepInCache(false);
    return $query->execute();
  }, $idempotent);
  ```

- YDB-NODEJS-SDK

  4-й аргумент метода `session.ExecuteQuery()` является необязательным объектом настроек типа `ExecuteQuerySettings`, он имеет флаг `keepInCache`, по умолчанию он имеет значение `false`. Можно установить для него значение `true` следующим образом:

  ``` javascript
  const settings = new ExecuteQuerySettings();
  settings.withKeepInCache(true);
  await session.executeQuery(..., settings);
  ```

{% endlist %}  