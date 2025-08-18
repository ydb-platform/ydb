# Compatibility and migration

Format versions (per RFC 0115)

- metadata.json and mapping.json schema and versioning.

Cross-version restore rules

- Newer restores should accept older format versions if documented.

Migration from legacy backups

- From tools dump/restore to collections; verify outcomes and integrity.

Notes

- Keep chains valid when applying retention; do not drop a full with dependent incrementals.
