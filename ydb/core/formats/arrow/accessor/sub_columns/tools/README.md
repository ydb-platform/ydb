# Sub-columns tools

`builder` converts newline-delimited JSON into a serialized sub-columns blob (with metadata for reader) and outputs compression statistics (with `--json-stats` option).
Pass accessor options with `--settings`, like the contents of `ALTER OBJECT ... SET (...)` statement (use singular `'` to avoid bash substitutions).
Use `--bench` mode to measure compression timings.

Non-map top-level nodes are unsupported, records with UTF-8 decoding errors are dropped.

`reader` converts the blob created by `builder` back to newline-delimited JSON. It is self-sufficient - all metadata for decoding is stored in the blob.

Example:
```
builder --input input --output output.bin --settings '`DICTIONARY_UNIQUE_FRACTION` = `0.75`, `ENABLE_NATIVE_COLUMNS` = `true`' --zstd-level 4
reader --input output.bin --output input.restored
```
