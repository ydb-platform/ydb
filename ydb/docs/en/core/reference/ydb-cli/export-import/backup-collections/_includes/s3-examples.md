S3 examples

Overview
- Use object storage for long-term backup storage by exporting backups and storing artifacts in S3-compatible backends.
- Authentication and connection details are documented in the S3 export/import pages.

References
- Export to S3: see `../export-s3.md`
- Import from S3: see `../import-s3.md`
- S3 auth: see `../auth-s3.md`

Notes
- Prefer lifecycle rules and versioning on buckets.
- Tune multipart uploads and concurrency according to provider limits.
