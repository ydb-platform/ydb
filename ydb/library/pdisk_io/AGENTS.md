# PDisk I/O Library Development

These instructions apply to `ydb/library/pdisk_io/`.

For backend, router, buffer, completion, or shutdown work, read the [I/O library skill](.agents/skills/ydb-pdisk-io-development/SKILL.md). Use [README.md](README.md) for the maintained source map and library contracts.

The skill links to caller guidance for changes that cross the library boundary. Follow the root build and test instructions, and distinguish tests that exercise io_uring from fallback or skipped tests.
