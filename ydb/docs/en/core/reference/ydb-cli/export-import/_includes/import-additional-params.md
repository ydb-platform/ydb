- 
- `--retries NUM`: Number of upload retries the server will attempt. Default value: `10`.
- `--skip-checksum-validation`: Skip the validation stage of [checksums](../file-structure.md#checksums) of the uploaded objects.
- `--encryption-key-file PATH`: Path to the file containing the encryption key (only for encrypted exports). This file is binary and must contain the exact number of bytes corresponding to the key length in the selected encryption algorithm (16 bytes for `AES-128-GCM`, 32 bytes for `AES-256-GCM` and `ChaCha20-Poly1305`). The key can also be passed via the `YDB_ENCRYPTION_KEY` environment variable, in hexadecimal string representation.
- `--format STRING`: Output result format. Valid values:

  - `pretty` — human-readable format (default).
  - `proto-json-base64` — [Protocol Buffers](https://en.wikipedia.org/wiki/Protocol_Buffers) in [JSON](https://en.wikipedia.org/wiki/JSON) format, binary strings encoded in [Base64](https://en.wikipedia.org/wiki/Base64).
