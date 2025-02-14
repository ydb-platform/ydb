# Traceability Matrix

## Non-functional Requirements

### Performance
#### REQ-PERF-001
Description: Ensure system operations meet expected performance benchmarks.

| Case ID | Name | Description | Issues | Test Case Status |
|---------|------|-------------|--------|------------------|
| REQ-PERF-001-7.1 | Bulk Upsert Performance | Measure and analyze the efficiency of bulk upsert operations. |  | Pending |
| REQ-PERF-001-7.2 | DELETE Operation Speed | Validate performance expectations during DELETE operations. |  | Pending |
| REQ-PERF-001-7.3 | TTL Deletion Speed | Measure and confirm deletion speed exceeds insertion speed. |  | Pending |

### Disk Space Management
#### REQ-DISK-001
Description: Effectively manage disk space to avoid system failures.

| Case ID | Name | Description | Issues | Test Case Status |
|---------|------|-------------|--------|------------------|
| REQ-DISK-001-8.1 | Disk Space Recovery | Test system behavior and recovery strategies for low disk space conditions. |  | Pending |
| REQ-DISK-001-8.2 | Resilience to Disk Saturation | Ensure database resilience and behavior expectations under full disk conditions. |  | Pending |

### Documentation
#### REQ-DOC-001
Description: Maintain and update comprehensive documentation.

| Case ID | Name | Description | Issues | Test Case Status |
|---------|------|-------------|--------|------------------|
| REQ-DOC-001-9.1 | Documentation Completeness | Review documentation for completeness and clarity. |  | Pending |

### Load Testing
#### REQ-LOAD-001
Description: Validate system performance under workload log scenarios.

Issues:
- 14493:  Nodes crush under write+select: https

| Case ID | Name | Description | Issues | Test Case Status |
|---------|------|-------------|--------|------------------|
| REQ-LOAD-001-1.1 | Bulk Upsert - 25MB/s | Measure bulk upsert performance at 25MB/s. | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/issue_id)](https://github.com/ydb-platform/ydb/issues/issue_id) | Pending |
| REQ-LOAD-001-1.2 | Bulk Upsert - 1GB/s | Measure bulk upsert performance at 1GB/s. | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/issue_id)](https://github.com/ydb-platform/ydb/issues/issue_id) | Pending |
| REQ-LOAD-001-1.3 | SELECT | Measure bulk upsert performance at 1GB/s. | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/issue_id)](https://github.com/ydb-platform/ydb/issues/issue_id) | Pending |

#### REQ-LOAD-002
Description: Evaluate system performance under simple queue scenarios.

| Case ID | Name | Description | Issues | Test Case Status |
|---------|------|-------------|--------|------------------|
| REQ-LOAD-002-2.1 | Simple Queue Load | Validate performance under simple queue load conditions. |  | Pending |

#### REQ-LOAD-003
Description: Assess system capabilities with an OLAP workload.

| Case ID | Name | Description | Issues | Test Case Status |
|---------|------|-------------|--------|------------------|
| REQ-LOAD-003-3.1 | OLAP Workload Performance | Test OLAP workload performance metrics. |  | Pending |

### Stability
#### REQ-STAB-001
Description: Ensure system stability under load scenarios with Nemesis.

| Case ID | Name | Description | Issues | Test Case Status |
|---------|------|-------------|--------|------------------|
| REQ-STAB-001-4.1 | Nemesis Stability Test | Measure stability performance during Nemesis events. |  | Pending |

### Compatibility
#### REQ-COMP-001
Description: Validate compatibility during system upgrades.

| Case ID | Name | Description | Issues | Test Case Status |
|---------|------|-------------|--------|------------------|
| REQ-COMP-001-5.1 | Upgrade 24-3 to 24-4 | Test upgrade compatibility from version 24-3 to 24-4. |  | Pending |
| REQ-COMP-001-5.2 | Upgrade 24-4 to 25-1 | Test upgrade compatibility from version 24-4 to 25-1. |  | Pending |

