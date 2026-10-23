# Mirror-3-dc recovery model

This bounded model checks confirmed-data guarantees and recovery transitions for
one immutable blob. It keeps physical realm identities fixed when logical roles
change.

## Source and scope

The model is adapted from [the mirror-3-dc audit gist](https://gist.github.com/kruall-tachikoma/5b2387f9cd33722e0c33403edaa0bd57),
revision `86910d127f0d97ccd181a9247c6a788ce69b4d83`. The gist's logs are historical
results, not results for this checkout.

`graph.json` contains the manually maintained states, CW layouts and transitions;
its `source_sha` records the original source baseline. It is not generated from
the C++ implementation. `model_common.py` defines layout and replication helpers;
`realms.py` explores physical placements and generates the Promela model.
Changes to C++ transitions or policies require a separate check of these inputs.
A successful model run does not prove the C++ implementation correct.

The group-state contract contains `Mode` and explicit `FailRealmStates`, with
exactly one entry for each physical realm index `0..2`. Parsing checks that the
entries are complete, unique and match the mode's role pattern. For example:

```textproto
Mode: SOLO_DC_RESURECTION_PENDING
FailRealmStates {
    FailRealm { FailRealmIdx: 0 State: Included }
    FailRealm { FailRealmIdx: 1 State: Leaving }
    FailRealm { FailRealmIdx: 2 State: Excluded }
}
```

Transitions preserve the maximum possible number of physical realm roles. Thus
`IIJ -> IJE` preserves the existing Joining realm and excludes an Included realm.
An `IEE -> ILE` or `IEE -> IJE` transition may select either Excluded realm;
neither the contract nor the model records a historical returning-realm choice.
The graph also includes the direct `DC_RECOVERY` transitions to
`DC_FAILURE_PENDING_INV_1` and `DC_FAILURE_INV_1`.

The following `IIL -> ILE` transitions preserve the physical Leaving realm and
exclude one of the Included realms:

- `DC_FAILURE_PENDING -> SOLO_DC_RESURECTION_PENDING`;
- `DC_FAILURE_PENDING_INV_1 -> SOLO_DC_INV_1_PENDING`;
- `DC_FAILURE_PENDING_INV_2 -> SOLO_DC_INV_2_PENDING`.

`DC_FAILURE -> SOLO_DC_RESURECTION_PENDING` is a direct `IIE -> ILE` shortcut
without an intermediate `SOLO_DC_DISASTER`. The existing Excluded realm remains
Excluded; one Included realm is selected as
the survivor and the other becomes Leaving. The target has CW 0.

The reverse `SOLO_DC_RESURECTION_PENDING -> SOLO_DC_DISASTER` transition changes
`ILE -> IEE`, preserves the Included realm and retains CW 0. A subsequent
`SOLO_DC_DISASTER -> SOLO_DC_RESURECTION_PENDING` retry can select either
Excluded realm under the contract. For this transition only, the one-blob model
assumes that, if any Excluded realm holds a copy, a realm holding a copy is
selected. If neither does, either choice is allowed and CW 0 still permits
unrecoverability. This is a model assumption about selecting a recovery source,
not a BSC constraint or a guarantee for a collection of blobs.

## Run

The `PY3_PROGRAM(mirror3dc-verify)` target bundles the Python runtime, graph and
generator sources. Build and test it remotely using the workspace's configured
development host pool:

```sh
ssh_ya_pool test ydb/core/blobstorage/groupinfo/models/mirror3dc
```

Run the resulting executable on a machine with a C compiler (default `gcc`) and
SPIN installed:

```sh
/path/to/mirror3dc-verify --output /absolute/path/out
```

The executable needs neither a checkout nor a separate Python installation.
Alternatively, with Python 3 available, run directly from this directory:

```sh
python3 verify.py --output /absolute/path/out
```

Set `CC` or `SPIN` to override the compiler or SPIN executable. To generate only
the model and Python audit summary:

```sh
python3 realms.py --output /absolute/path/out
```

The generator writes `model.pml` and `summary.json`. The verifier runs separate
full safety and recovery searches and saves the run manifest, logs and any
counterexample trail in the output directory. Inspect those artifacts for the
result of a particular checkout; no new verification result is recorded here.

## Properties and assumptions

- Safety checks that each modeled transition preserves its target CW guarantee,
  including the modeled loss of old data when a realm enters Joining.
- Recovery selects one outgoing transition and applies the source state's
  durability policy until the target CW holds. For a source state with positive
  CW, it rejects a terminal placement where that guarantee is still unmet and
  no replication step is available. A source state with CW 0 makes no
  recoverability guarantee, so such terminal placements are allowed and reported
  separately in `summary.json` as `unrecoverable_cw0_count` and
  `unrecoverable_cw0_examples`. This exception does not weaken the transition
  guard: entering the target still requires its CW to hold.
  This is a check for terminal inability to complete the selected transition,
  not unrestricted liveness under arbitrary scheduling or failures.
- Copy counts are bounded to `0..2` per physical realm. The model tracks one
  immutable blob and assumes eventual completion of enabled I/O. It excludes
  concurrent writes, network races, group-generation races, garbage collection
  and the actual DSProxy/VDisk execution protocols.
- Leaving realms may supply old data but receive no writes and do not count
  toward CW. Joining may participate in acknowledged writes only together with
  every Included realm. When a CW layout fits entirely in Included realms while
  another realm is Joining, that layout must be held by Included realms alone.
