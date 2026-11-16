- `--description STRING`: Text description of the operation, saved in the operation history.
- `--retries NUM`: Number of retry attempts the server will make. Default value: `10`.
- `--skip-checksum-validation`: Skip the validation stage of [checksums](../file-structure.md#checksums) of imported objects.
- `--no-acl`: Do not import ACLs and object owners. Created objects get an empty ACL, and their owner is the user who started the import.
- `--index-population-mode STRING`: Index population mode during import. Allowed values:

  - `Build` — build indexes from scratch (default).
  - `Import` — load index table data (if the export was created with `--include-index-data`).
  - `Auto` — try to load index table data, and build indexes if the data is absent.
- `--encryption-key-file PATH`: Path to the file containing the encryption key (only for encrypted exports). This file is binary and must contain the exact number of bytes corresponding to the key length in the selected encryption algorithm (16 bytes for `AES-128-GCM`, 32 bytes for `AES-256-GCM` and `ChaCha20-Poly1305`). The key can also be passed via the `YDB_ENCRYPTION_KEY` environment variable, in hexadecimal string representation.
- `--format STRING`: Output result format. Valid values:

  - `pretty` — human-readable format (default).
  - `proto-json-base64` — [Protocol Buffers](https://en.wikipedia.org/wiki/Protocol_Buffers) in [JSON](https://en.wikipedia.org/wiki/JSON) format, binary strings encoded in [Base64](https://en.wikipedia.org/wiki/Base64).
