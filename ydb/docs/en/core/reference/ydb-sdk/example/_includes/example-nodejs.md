# App in Node.js

This page contains a detailed description of the code of a [test app](https://github.com/ydb-platform/ydb-nodejs-sdk/tree/master/examples/basic-example-v1) that is available as part of the {{ ydb-short-name }} [Node.js SDK](https://github.com/yandex-cloud/ydb-nodejs-sdk).

{% include [addition.md](auxilary/addition.md) %}

{% include [scan_query.md](steps/08_scan_query.md) %}

```js
let count = 0;

const consumer = (result: ExecuteScanQueryPartialResult) => {
  count += result.resultSet?.rows?.length || 0;
};

await session.streamExecuteScanQuery(`
  DECLARE $value as Utf8;
  SELECT * FROM table WHERE value=$value;
`, consumer, {'$value': Primitive.utf8('ttt')});
```

{% include [error_handling.md](steps/50_error_handling.md) %}

