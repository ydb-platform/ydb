### Changelog entry <!-- a user-readable short description of the changes that goes to CHANGELOG.md and Release Notes -->

Fixed internal error when executing `UPSERT INTO ... SELECT` without LIMIT into a table with Serial column: the sequencer stage now gets the same task count as its input (1-1 like Map). Fixes #30562.

### Changelog category <!-- remove all except one -->

* Bugfix

### Description for reviewers <!-- (optional) description for those who read this PR -->

**Problem (issue #30562):**  
Query `UPSERT INTO folder SELECT ... FROM dashboard` (table with BigSerial) failed with:
`BuildSequencerChannels(): requirement stageInfo.Tasks.size() == inputStageInfo.Tasks.size() failed`

Without LIMIT, the scan stage had many tasks while the write stage (with Sequencer connection) was created with one task. Sequencer is 1-1 like Map, so the requirement in `BuildSequencerChannels` was correct; the bug was that the sequencer stage was not given the same number of tasks as its input.

**Solution:**  
- **Correct place:** In `BuildComputeTasks()` (kqp_tasks_graph.cpp), treat `kSequencer` like `kMap`: set `partitionsCount = originStageInfo.Tasks.size()` and `forceMapTasks = true` when the stage has a Sequencer input. Then the sequencer stage gets as many tasks as the input stage, and 1-1 channel building in `BuildSequencerChannels` succeeds.
- **BuildSequencerChannels:** Kept strict 1-1: `YQL_ENSURE(stageInfo.Tasks.size() == inputStageInfo.Tasks.size())` and single `BuildTransformChannels` call (no round-robin).
- **Tests:** Added `SequencerStageTaskCountMatchesInput` in kqp_executer_ut.cpp: UPSERT INTO table with BigSerial SELECT from TwoShard without LIMIT; asserts success.
