### Additional parameters {#aux}

| Parameter | Description |
| --- | --- |
| `--description STRING` | Operation text description saved to the history of operations. |
| `--retries NUM` | Number of export retries to be made by the server.<br/>Defaults to `10`. |
| `--compression STRING` | Compress exported data.<br/>If the default compression level is used for the [Zstandard](https://en.wikipedia.org/wiki/Zstandard) algorithm, data can be compressed by 5–10 times. Compressing data uses CPU resources and may affect the speed of performing other DB operations.<br/>Possible values:<br/><ul><li>`zstd`: Compression using the Zstandard algorithm with the default compression level (`3`).</li><li>`zstd-N`: Compression using the Zstandard algorithm, where `N` is the compression level (`1` — `22`).</li></ul> |
| `--encryption-algorithm ALGORITHM` | Encrypt exported data using the specified algorithm. Supported values: `AES-128-GCM`, `AES-256-GCM`, `ChaCha20-Poly1305`. |
| `--encryption-key-file PATH` | File path containing the encryption key (only for encrypted exports). The file is binary and must contain exactly the number of bytes matching the key length for the chosen encryption algorithm (16 bytes for `AES-128-GCM`, 32 bytes for `AES-256-GCM` and `ChaCha20-Poly1305`). The key can also be provided using the `YDB_ENCRYPTION_KEY` environment variable, in hexadecimal string representation. |
| `--format STRING` | Result format.<br/>Possible values:<br/><ul><li>`pretty`: Human-readable format (default).</li><li>`proto-json-base64`: [Protocol Buffers](https://en.wikipedia.org/wiki/Protocol_Buffers) in [JSON](https://en.wikipedia.org/wiki/JSON) format; binary strings are [Base64](https://en.wikipedia.org/wiki/Base64)-encoded.</li></ul> |
