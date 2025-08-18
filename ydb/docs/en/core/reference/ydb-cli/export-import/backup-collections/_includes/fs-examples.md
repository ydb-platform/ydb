Filesystem examples

Export
```bash
ydb tools dump -p .backups/collections/<collection> -o <output_dir>
```

Import (concept)
- Create/select a new collection in the target DB.
- Import the required backups in chronological order: full, then incrementals.
- The tool auto-creates missing tables/collection.
