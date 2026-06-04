The `operation get` output format is also set by the `--format` option.

Although the operation ID has URL format, there is no guarantee that it will be preserved in the future. It should be interpreted only as a string.

You can track export completion by changes in the `progress` attribute:

- In the default `pretty` mode, a successfully completed operation is shown as `Done` in the semigraphics-formatted `progress` field:

  ```text
  ┌───── ... ──┬───────┬─────────┬──────────┬─...
  | id         | ready | status  | progress | ...
  ├──────... ──┼───────┼─────────┼──────────┼─...
  | ydb:/...   | true  | SUCCESS | Done     | ...
  ├╴╴╴╴╴ ... ╴╴┴╴╴╴╴╴╴╴┴╴╴╴╴╴╴╴╴╴┴╴╴╴╴╴╴╴╴╴╴┴╴...
  ...
  ```

- In the `proto-json-base64` mode, a completed operation is indicated by the `PROGRESS_DONE` value of the `progress` attribute:

  ```json
  {"id":"ydb://...", ...,"progress":"PROGRESS_DONE",... }
  ```
