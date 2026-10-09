# Modeling patterns

1. Translate independently scheduled participants to `proctype`, messages to
   bounded channels, and shared state to bounded variables. Model the atomic
   implementation operations separately from multi-step publication protocols.
2. Give the environment explicit choices for permitted faults: drop, duplicate,
   reorder, restart, or delay. Document reliability and eventual recovery bounds.
   A channel send can block when full; do not accidentally model loss as blocking.
3. Keep environment driver state out of component guards. A restart may change
   component state, epoch, or mailbox lifetime; a driver flag alone cannot authorize
   internal progress. The component should remain meaningful with another driver.
4. Model timers as explicit state/transitions. Promela `timeout` means no process
   has an executable transition, not elapsed real time.
5. Separate invariants from progress properties. Explicitly model an epoch or
   request identity when a late completion could otherwise satisfy a newer request.
6. Start with small bounds. Remove payload detail only when it cannot affect the
   property. Symmetry and collapsed transitions need a property-specific argument.
7. Preserve a baseline and a deliberately broken negative control where useful.
   Check that the negative control produces the expected mechanism, not an
   unrelated deadlock or environment artifact.
8. State memory-model limits. Ordinary shared-variable interleavings model SC;
   release/acquire visibility and relaxed atomics require a justified abstraction
   or a different analysis in addition to Spin.
