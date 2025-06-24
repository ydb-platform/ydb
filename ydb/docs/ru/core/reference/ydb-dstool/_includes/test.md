<!-- markdownlint-disable blanks-around-fences -->

```bash
ydb-dstool -e <bs_endpoint> cluster list
```

* `bs_endpoint` — URI HTTP-эндпоинта кластера — это тот же самый эндпоинт, который обслуживает [Embedded UI](../../embedded-ui/index.md). Пример: `http://localhost:8765`.

Результат:

```text
┌───────┬───────┬───────┬────────┬────────┬───────┬────────┐
│ Hosts │ Nodes │ Pools │ Groups │ VDisks │ Boxes │ PDisks │
├───────┼───────┼───────┼────────┼────────┼───────┼────────┤
│ 8     │ 16    │ 1     │ 5      │ 40     │ 1     │ 32     │
└───────┴───────┴───────┴────────┴────────┴───────┴────────┘
```
