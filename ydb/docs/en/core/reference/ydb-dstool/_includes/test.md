<!-- markdownlint-disable blanks-around-fences -->

```bash
ydb-dstool -e <bs_endpoint> cluster list
```

* `bs_endpoint`: URI of the cluster's HTTP endpoint, the same endpoint that serves the [Embedded UI](../../embedded-ui/index.md). Example: `http://localhost:8765`.

Result:

```text
┌───────┬───────┬───────┬────────┬────────┬───────┬────────┐
│ Hosts │ Nodes │ Pools │ Groups │ VDisks │ Boxes │ PDisks │
├───────┼───────┼───────┼────────┼────────┼───────┼────────┤
│ 8     │ 16    │ 1     │ 5      │ 40     │ 1     │ 32     │
└───────┴───────┴───────┴────────┴────────┴───────┴────────┘
```
