# Transfer (topic → table)

Writes from **YDB Topics** into tables (row / column) via the replication
transfer pipeline (`TTransferWriterFactory`).
Core: [`ydb/core/persqueue/AGENTS.md`](../persqueue/AGENTS.md).
Shared rules: [`RULES.md`](../persqueue/RULES.md).

## Layout

* Root — transfer writer, scheme mapping, Purecalc I/O, row/column table paths.
* `ut/` — unit and functional transfer tests.

Tests: `./ya make --build relwithdebinfo -tA ydb/core/transfer`
