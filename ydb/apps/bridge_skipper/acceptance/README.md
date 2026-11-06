# Executable acceptance

`tests` is a MEDIUM repository PY3TEST target. Its dependency supplies the real
`bridge_skipper` executable; the slim harness starts mock gRPC and peer HTTP
listeners with the existing `LiveCluster` testkit. All skipper requests use the
real C++ SDK. No external virtualenv or YDB CLI is required.

`FORK_SUBTESTS` and `SPLIT_FACTOR(10)` partition subtests into ten parallel
chunks; a chunk can contain multiple scenarios. Configuration uses
positive whole seconds and preserves the production validator's warmup minimum
and timing inequalities.
All mock and controller listeners use `PortManager` reservations held until
cleanup, avoiding ephemeral ports and collisions with parallel test workers.

`test_e2e.py` keeps only the cases that need the real binary, SDK and wire:

- failover: a confirmed synchronized-pile loss (with a `/metrics` scrape),
  primary loss in fallback, stage-2 loss, and an A|B partition with BSC on the
  primary side;
- no mutation: a lost SUSPENDED pile, and manual plans with their CLI commands;
- guarded sends: a lost commit reply, an accepted mutation racing an external
  update, an identical retry after UNAVAILABLE, and staggered failures replacing
  the pending update with a same-generation superset (e18);
- slow observations: delayed SelfCheck while connectivity reports remain
  binding (e17);
- shutdown: SIGTERM during a hung SelfCheck within the default stop bound;
- startup: a locked state directory reports `startup_failed` and exits;
  HTTP and Skipper logs retain their distinct component names at debug level.

Every skipper mutation assertion parses the mock request log and requires the
exact overlay, empty `quorum_piles`, and `expected_generation` equal to G captured
from the decision/view **before sending**. Post-commit views cannot establish
that guard. Test output retains `requestlog.json`, the last `snapshot.json`,
`skipper.json`, `skipper.log` and the state directory for diagnosis.

Configuration, state-file, restart, HTTP-routing and endpoint-selection rules are
covered by the unit tests in `config/ut`, `runtime/ut`, `http/ut` and `domain/ut`.
Direct public-wire contracts belong to `ydbd_mock/tests/test_contracts.py`.
Qualification on recorded real YDB builds is part of the release checklist in
the [skipper README](../README.md).
