# BlobStorage Development

These instructions apply to `ydb/core/blobstorage/`. Use the [source map](README.md) to identify the component, then read the matching guidance:

| Task | Guidance |
|---|---|
| Shared contracts, DSProxy, group layout, BSC allocation, or NodeWarden lifecycle | [Shared BlobStorage skill](.agents/skills/ydb-blobstorage-development/SKILL.md) |
| DDisk and PersistentBuffer implementation | [DDisk guidance](ddisk/AGENTS.md) |
| PDisk implementation | [PDisk guidance](pdisk/AGENTS.md) |
| VDisk implementation | [VDisk guidance](vdisk/AGENTS.md) |
| Reusable I/O library or its caller boundary | [I/O library guidance](../../library/pdisk_io/AGENTS.md) |

Load only the entries involved in the task. For integration tests, select guidance by the components the scenario exercises. Work on BSC or shared storage schemas outside this subtree can use the shared skill without extending these directory instructions to unrelated code.

Keep shared contracts in the [contributor guide](../../docs/en/core/contributor/distributed-storage.md) and source navigation in component READMEs. Follow the root build and test instructions.
