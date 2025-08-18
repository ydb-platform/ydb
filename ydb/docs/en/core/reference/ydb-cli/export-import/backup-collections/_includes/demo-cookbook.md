Demo: multi-database restore using a backup collection

Scenario
- Source DB: test1 creates collection `shop_backups` (SQL) with STORAGE='cluster'.
- Take backups: full (Checkpoint 1), incremental #1 (Checkpoint 2), incremental #2 (Checkpoint 3).
- Simulate failure (optional), then export backups to filesystem with `ydb tools dump`.

Restore patterns (destination DBs)
- Create/select a new target collection per destination (e.g., `restore_checkpoint1`).
- Import only the backups you need, in chronological order:
  - Full only (Checkpoint 1): import the full backup.
  - Full + first incremental (Checkpoint 2): import the full backup, then the first incremental.
  - Full + all incrementals (Checkpoint 3): import the full backup, then all incrementals.
- Missing tables/collection are created automatically during import.

Validation ideas
- Count rows by timestamp periods; verify expected IDs and prices.
- Compute totals to match the original database state for that checkpoint.
