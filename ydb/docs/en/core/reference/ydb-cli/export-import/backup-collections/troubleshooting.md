# Troubleshooting

Missing base / chain gap
- Symptom: restore fails or skips changes.
- Fix: ensure full backup is present and apply incrementals in order.

Checksum/integrity mismatch
- Fix: re-export backups and verify storage/media integrity.

Encryption/key errors
- Fix: verify KMS permissions and key IDs; rotate or reconfigure keys as needed.

Partial restore conflicts
- Fix: decide replace/fail policy; map paths carefully.

S3 auth or throttling
- Fix: check credentials, endpoints, retry settings; reduce parallelism.
