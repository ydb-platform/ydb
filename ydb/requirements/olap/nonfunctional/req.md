# Requirements for YDB Analytics System

## Introduction
This document outlines the detailed functional and non-functional requirements for the YDB analytics system, including associated test cases for verification.

## Non-functional Requirements

### Performance

- **REQ-PERF-001**: Ensure system operations meet expected performance benchmarks.
  - **Description**: Maintain performance standards across operations, ensuring response times and throughput align with requirements.
  - **Cases**:
    - Case 7.1: [Bulk Upsert Performance](path/to/test/34) - Measure and analyze the efficiency of bulk upsert operations.
    - Case 7.2: [DELETE Operation Speed](path/to/test/35) - Validate performance expectations during DELETE operations.
    - Case 7.3: [TTL Deletion Speed](path/to/test/36) - Measure and confirm deletion speed exceeds insertion speed.

### Disk Space Management
- **REQ-DISK-001**: Effectively manage disk space to avoid system failures.
  - **Description**: Utilize efficient disk space management strategies, particularly under scenarios with constrained disk resources.
  - **Cases**:
    - Case 8.1: [Disk Space Recovery](path/to/test/37) - Test system behavior and recovery strategies for low disk space conditions.
    - Case 8.2: [Resilience to Disk Saturation](path/to/test/38) - Ensure database resilience and behavior expectations under full disk conditions.

### Documentation
- **REQ-DOC-001**: Maintain and update comprehensive documentation.
  - **Description**: Ensure all functionalities are clearly documented, aiding user orientation and system understanding.
  - **Cases**:
    - Case 9.1: [Documentation Completeness](path/to/test/39) - Review documentation for completeness and clarity.

### Load Testing

- **REQ-LOAD-001**: Validate system performance under workload log scenarios.
  - **Description**: Ensure the system can handle select queries and bulk upsert operations efficiently, with specified data rates.
   **Issues**: 
    - ISSUE: Nodes crush under write+select: https://github.com/ydb-platform/ydb/issues/14493

  - **Cases**:
    - Case 1.1: [Bulk Upsert - 25MB/s](path/to/test/bulk_upsert_25mbs) - Measure bulk upsert performance at 25MB/s.
    - Case 1.2: [Bulk Upsert - 1GB/s](path/to/test/bulk_upsert_1gbs) - Measure bulk upsert performance at 1GB/s.
    - Case 1.3: [SELECT](path/to/test/bulk_upsert_1gbs) - Measure bulk upsert performance at 1GB/s.

- **REQ-LOAD-002**: Evaluate system performance under simple queue scenarios.
  - **Description**: Test the ability of the system to efficiently process tasks in simple queue scenarios.
  - **Cases**:
    - Case 2.1: [Simple Queue Load](path/to/test/simple_queue_load) - Validate performance under simple queue load conditions.

- **REQ-LOAD-003**: Assess system capabilities with an OLAP workload.
  - **Description**: Verify OLAP queries perform efficiently under varied load.
  - **Cases**:
    - Case 3.1: [OLAP Workload Performance](path/to/test/olap_load) - Test OLAP workload performance metrics.

### Stability

- **REQ-STAB-001**: Ensure system stability under load scenarios with Nemesis.
  - **Description**: Validate stability by running load tests with a one-minute reload interval using Nemesis.
  - **Cases**:
    - Case 4.1: [Nemesis Stability Test](path/to/test/nemesis_stability) - Measure stability performance during Nemesis events.

### Compatibility

- **REQ-COMP-001**: Validate compatibility during system upgrades.
  - **Description**: Ensure seamless transition and compatibility when upgrading the system from one version to another.
  - **Cases**:
    - Case 5.1: [Upgrade 24-3 to 24-4](path/to/test/upgrade_24_3_to_24_4) - Test upgrade compatibility from version 24-3 to 24-4.
    - Case 5.2: [Upgrade 24-4 to 25-1](path/to/test/upgrade_24_4_to_25_1) - Test upgrade compatibility from version 24-4 to 25-1.
