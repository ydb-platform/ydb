---
name: ydb-spin-promela-verifier
description: "Model concurrent YDB code and protocols in Promela, verify safety and liveness with Spin, and investigate counterexample trails."
---

# Spin Promela Verification

Paths in code spans are relative to the repository root. Keep models beside the
component they describe. Existing examples are under `ydb/library/actors/queues/spin`,
`ydb/library/actors/metrics/spin`, and `ydb/core/blobstorage/nodewarden/spin`.
Read each component's README and instructions before changing its model.

## Model the contract

1. Name the invariant or progress requirement, source revision, and implementation
   transitions it depends on. Choose finite process, queue, retry, and value bounds.
2. Record omitted behavior and environment assumptions in the model. Spin checks
   the modeled transition system; an SC model does not prove C++ weak-memory
   behavior or correctness beyond its bounds.
3. Read [modeling patterns](references/promela-modeling-patterns.md) and apply
   [Promela style](../ydb-promela-code-style/SKILL.md). Represent component state
   separately from the environment that injects events and failures. Do not gate
   component progress on scenario markers absent from the implementation.
4. Leave supported interleavings nondeterministic. Derive lost messages from
   lifecycle, channel, or epoch rules rather than scripting a desired failure.
5. Add local assertions and named LTL claims using
   [property patterns](references/property-patterns.md). State fairness assumptions:
   weak process fairness does not guarantee fairness of each branch or message.

## Run and preserve evidence

1. Require Spin, Python 3.9 or newer, and a C compiler on the execution host.
   Follow active execution instructions. Remote runs must use the exact model
   and include files; compare hashes before reusing a remote snapshot.
2. Use the helper for exhaustive safety or one named inline LTL claim:

   ```bash
   python3 .agents/skills/ydb-spin-promela-verifier/scripts/run_spin_verify.py \
     --model path/to/model.pml --safety
   python3 .agents/skills/ydb-spin-promela-verifier/scripts/run_spin_verify.py \
     --model path/to/model.pml --ltl live_progress --depth 200000 --mem 4096
   ```

   These paths and claim names are placeholders. Use `--help` for options.
   The helper snapshots the model directory into a new output directory, retains
   generated files/trails, writes commands and input hashes to `manifest.json`,
   and saves full logs. For parent-relative includes, select the smallest enclosing
   `--source-root`. Only quoted relative includes inside that tree are supported;
   symlinks and escaping or macro-based includes are rejected. Do not choose the entire repository.
3. Use `--fair` only when weak process fairness is part of the contract. If the
   verifier reports insufficient fairness bookkeeping, increase `--nfair` and
   rerun; do not interpret the failed run as a property result.
4. The helper supports a fixed exhaustive search configuration. For model defines,
   special reduction, non-progress cycles, or embedded C, follow the component's
   runner or a documented manual run. Preserve exact generation, compilation,
   and runtime flags, tool versions, input hashes, bounds, logs, and trails.
5. A printed artifact directory means setup began, not that state exploration
   started. Inspect `pan.log`. Compare one experimental change at a time with
   baseline files preserved; explain differences in search counts through input
   hashes and flags before drawing conclusions.

## Interpret the result

1. Report `holds` only for completed exhaustive search with zero errors for the
   selected property and bounds. Depth/memory/time limits or interruption mean
   `unknown`; a setup or verifier failure means `error`. Neither proves safety.
2. For `violated`, preserve and replay the trail in the copied model tree. Use
   the same preprocessing options and selected claim as the original run.
   Inspect [Spin options](https://spinroot.com/spin/Man/Spin.html) for replay flags.
3. Map the trail to implementation transitions. Distinguish a supported failure
   from a modeling error, impossible environment, or overstrong property before
   proposing a code fix. Use a negative control to check that a weakened model
   has not removed the behavior being investigated.
4. Return model paths, assumptions, bounds, per-property status, decisive trail
   steps, and artifact paths. Across sessions, retain observations and hypotheses
   separately and recheck conclusions when the source or input hashes change.

Search and fairness options are documented in
[Pan options](https://spinroot.com/spin/Man/Pan.html).
