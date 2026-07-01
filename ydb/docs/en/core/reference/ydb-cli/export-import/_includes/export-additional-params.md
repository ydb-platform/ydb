- 
- `--retries NUM`: Number of retry attempts for export that the server will make. Default value: `10`.
- `--compression STRING`: Compress exported data. With the default compression level for the [Zstandard](https://en.wikipedia.org/wiki/Zstd) algorithm, data can be compressed by 5-10 times. Data compression uses CPU resources and may affect the speed of other database operations. Valid values:

  - `zstd` — compression using the Zstandard algorithm with the default compression level (`3`).
  - `zstd-N` — compression using the Zstandard algorithm, `N` — compression level (`1` — `22`).
- `--encryption-algorithm ALGORITHM`: Encrypt exported data using the specified algorithm. Supported values: `AES-128-GCM`, `AES-256-GCM`, `ChaCha20-Poly1305`.
- `--encryption-key-file PATH`: Path to the file containing the encryption key (only for encrypted exports). This file is binary and must contain the exact number of bytes corresponding to the key length in the selected encryption algorithm (16 bytes for `AES-128-GCM`, 32 bytes for `AES-256-GCM` and `ChaCha20-Poly1305`). The key can also be passed via the `YDB_ENCRYPTION_KEY` environment variable, in hexadecimal string representation.
- `--format STRING`: Output result format. Valid values:

  - `pretty` — human-readable format (default).
  - `proto-json-base64` — [Protocol Buffers](https://en.wikipedia.org/wiki/Protocol_Buffers) in [JSON](https://en.wikipedia.org/wiki/JSON) format, binary strings encoded in [Base64](https://en.wikipedia.org/wiki/Base64).
