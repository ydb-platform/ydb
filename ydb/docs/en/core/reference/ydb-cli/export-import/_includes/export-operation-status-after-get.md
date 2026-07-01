The output format `operation get` is also set by the option `--format`.

Although the operation ID is in URL format, it is not guaranteed to be preserved in the future. It should be interpreted only as a string.

The completion of the export is tracked by the change of the "progress" attribute:

- In the `pretty` output mode (default), a successfully completed operation is reflected by the value "Done" in the `progress` field highlighted with pseudographics:


  ```text
  ┌───── ... ──┬───────┬─────────┬──────────┬─...
  | id         | ready | status  | progress | ...
  ├──────... ──┼───────┼─────────┼──────────┼─...
  | ydb://...   | true  | SUCCESS | Done     | ...
  ├╴╴╴╴╴ ... ╴╴┴╴╴╴╴╴╴╴┴╴╴╴╴╴╴╴╴╴┴╴╴╴╴╴╴╴╴╴╴┴╴...
  ...
  ```

- In the `proto-json-base64` output mode, a completed operation is reflected by the value `PROGRESS_DONE` of the `progress` attribute:


  ```json
  {"id":"ydb://...", ...,"progress":"PROGRESS_DONE",... }
  ```
