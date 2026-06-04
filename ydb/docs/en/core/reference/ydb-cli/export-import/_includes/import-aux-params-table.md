### Additional parameters {#aux}

| Parameter | Description |
| --- | --- |
| `--description STRING` | Operation text description saved to the history of operations. |
| `--retries NUM` | Number of import retries to be made by the server.<br/>Defaults to `10`. |
| `--encryption-key-file PATH` | File path containing the encryption key (only for encrypted exports). The file is binary and must contain exactly the number of bytes matching the key length for the chosen encryption algorithm (16 bytes for `AES-128-GCM`, 32 bytes for `AES-256-GCM` and `ChaCha20-Poly1305`). The key can also be provided using the `YDB_ENCRYPTION_KEY` environment variable, in hexadecimal string representation. |
| `--format STRING` | Result format.<br/>Possible values:<br/><ul><li>`pretty`: Human-readable format (default).</li><li>`proto-json-base64`: [Protocol Buffers](https://en.wikipedia.org/wiki/Protocol_Buffers) in [JSON](https://en.wikipedia.org/wiki/JSON) format; binary strings are [Base64](https://en.wikipedia.org/wiki/Base64)-encoded.</li></ul> |
