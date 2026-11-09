# SelfCheck classifier fixtures

These are synthetic, source-derived protobuf text fixtures, not recordings from
a running cluster. Pile names and issue IDs are shortened for clarity. Their
shape and messages are pinned to `ydb/core/health_check` as of `7596b0fcb34`,
which includes the server changes `6d0f8ee7a75` (stage-2 bridge part failures)
and `47c54ff9176` (missing whiteboard data kept distinct from failure):

| Fixture | Source scenario |
| --- | --- |
| `bsc_red.pbtxt` | `health_check_ut.cpp`: `BridgeGroupDeadInOnePile` |
| `stage2.pbtxt` | `health_check_ut.cpp`: `BridgeGroupDeadInStage2Pile` |
| `fallback_grey.pbtxt` | `health_check_ut.cpp`: `BridgeWhiteboardMissingRepliesDoNotBlamePile` |
| `fallback_red.pbtxt` | `health_check_ut.cpp`: `WhiteboardDiskAbsentFromReplyIsGroupFailure`, with synthetic bridge attribution |
| `blue_only.pbtxt` | `health_check_ut.cpp`: `BlueGroupIssueWhenPartialGroupStatusAndReplicationDisks`; RED threshold and bridge wrapper are synthetic |
| `blue_with_orange.pbtxt` | `health_check.cpp`: block-4-2 part with three replicating VDisks and one throttled VDisk (`VDisk is being throttled`, ORANGE) |
| `bsc_clean_with_pile_issues.pbtxt` | `health_check.cpp::FillStateStorage`: RED `PILE_STATE_STORAGE` with no group issues |
| `truncated.pbtxt` | `health_check.cpp::TruncateIssuesIfBeyondLimit`: BLUE VDisk issues dropped first, reason IDs left dangling, RED `HEALTHCHECK_STATUS` appended last |
| `unattributed.pbtxt` | `health_check_ut.cpp`: `BridgeOrdinaryGroupHasNoPileAttribution`, plus synthetic numeric unknown pile |

The classifier intentionally pins `System tablet BSC didn't provide information`
and `Replication in progress` exactly. The test suite adds near-match negatives.
Real-cluster fixtures for primary/PROMOTED loss, partitions and stage-2 loss
remain an external release gate; these synthetic fixtures do not qualify
production automatic failover.

Known limit: YELLOW and ORANGE VDisk reasons are neutral, so a throttled VDisk
on a RED PDisk is read as neutral. The issue graph does not link a VDisk issue
to its PDisk.
