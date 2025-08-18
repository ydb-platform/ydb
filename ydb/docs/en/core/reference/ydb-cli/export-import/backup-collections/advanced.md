# Advanced topics

Encryption

- Use KMS-managed keys if supported; plan key rotation and restore with rotated keys.

Performance

- Increase parallelism judiciously; batch operations and tune network/storage.

Partial backups/restores

- Use explicit table lists; document mapping semantics and limitations.

Scheduling patterns

- Daily full + hourly incremental + weekly synthetic full (when available).
