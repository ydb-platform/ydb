# Traceability Matrix

## Non-functional Requirements

### Performance
#### REQ-PERF-001: Ensure system operations meet expected performance benchmarks.
**Description**: Maintain performance standards across operations, ensuring response times and throughput align with requirements.

| Case ID | Name | Description | Issues |  Status |
|---------|------|-------------|--------|:--------|
| REQ-PERF-001-7.1 | Bulk Upsert Performance | Measure and analyze the efficiency of bulk upsert operations. |  | Pending |
| REQ-PERF-001-7.2 | DELETE Operation Speed | Validate performance expectations during DELETE operations. |  | Pending |
| REQ-PERF-001-7.3 | TTL Deletion Speed | Measure and confirm deletion speed exceeds insertion speed. |  | Pending |

### Disk Space Management
#### REQ-DISK-001: Effectively manage disk space to avoid system failures.
**Description**: Utilize efficient disk space management strategies, particularly under scenarios with constrained disk resources.

| Case ID | Name | Description | Issues |  Status |
|---------|------|-------------|--------|:--------|
| REQ-DISK-001-8.1 | Disk Space Recovery | Test system behavior and recovery strategies for low disk space conditions. |  | Pending |
| REQ-DISK-001-8.2 | Resilience to Disk Saturation | Ensure database resilience and behavior expectations under full disk conditions. |  | Pending |

### Documentation
#### REQ-DOC-001: Maintain and update comprehensive documentation.
**Description**: Ensure all functionalities are clearly documented, aiding user orientation and system understanding.

| Case ID | Name | Description | Issues |  Status |
|---------|------|-------------|--------|:--------|
| REQ-DOC-001-9.1 | Documentation Completeness | Review documentation for completeness and clarity. |  | Pending |

### Load Testing
#### REQ-LOAD-001: Validate system performance under workload log scenarios.
**Description**: Ensure the system can handle select queries and bulk upsert operations efficiently, with specified data rates.

Issues:
- 14493:  Nodes crush under write+select: https

| Case ID | Name | Description | Issues |  Status |
|---------|------|-------------|--------|:--------|
| REQ-LOAD-001-1.1 | Bulk Upsert - 25MB/s | Measure bulk upsert performance at 25MB/s. | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/14493)](https://github.com/ydb-platform/ydb/issues/14493) | Pending |
| REQ-LOAD-001-1.2 | Bulk Upsert - 1GB/s | Measure bulk upsert performance at 1GB/s. | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/14493)](https://github.com/ydb-platform/ydb/issues/14493) | Pending |
| REQ-LOAD-001-1.3 | SELECT | Measure bulk upsert performance at 1GB/s. | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/14493)](https://github.com/ydb-platform/ydb/issues/14493) | Pending |

#### REQ-LOAD-002: Evaluate system performance under simple queue scenarios.
**Description**: Test the ability of the system to efficiently process tasks in simple queue scenarios.

| Case ID | Name | Description | Issues |  Status |
|---------|------|-------------|--------|:--------|
| REQ-LOAD-002-2.1 | Simple Queue Load | Validate performance under simple queue load conditions. |  | Pending |

#### REQ-LOAD-003: Assess system capabilities with an OLAP workload.
**Description**: Verify OLAP queries perform efficiently under varied load.

| Case ID | Name | Description | Issues |  Status |
|---------|------|-------------|--------|:--------|
| REQ-LOAD-003-3.1 | OLAP Workload Performance | Test OLAP workload performance metrics. |  | Pending |

### Stability
#### REQ-STAB-001: Ensure system stability under load scenarios with Nemesis.
**Description**: Validate stability by running load tests with a one-minute reload interval using Nemesis.

| Case ID | Name | Description | Issues |  Status |
|---------|------|-------------|--------|:--------|
| REQ-STAB-001-4.1 | Nemesis Stability Test | Measure stability performance during Nemesis events. |  | Pending |

### Compatibility
#### REQ-COMP-001: Validate compatibility during system upgrades.
**Description**: Ensure seamless transition and compatibility when upgrading the system from one version to another.

| Case ID | Name | Description | Issues |  Status |
|---------|------|-------------|--------|:--------|
| REQ-COMP-001-5.1 | Upgrade 24-3 to 24-4 | Test upgrade compatibility from version 24-3 to 24-4. |  | Pending |
| REQ-COMP-001-5.2 | Upgrade 24-4 to 25-1 | Test upgrade compatibility from version 24-4 to 25-1. |  | Pending |

