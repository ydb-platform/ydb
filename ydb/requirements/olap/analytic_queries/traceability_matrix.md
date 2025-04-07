# Traceability Matrix

## Non-functional Requirements

### Performance
#### REQ-PERF-001: Ensure the system handles aggregate functions efficiently across various data sizes.
**Description**: Verify that aggregate functions maintain performance standards at increasing data volumes, ensuring response times are within acceptable limits.

| Case ID | Name | Description | Issues |  Status |
|---------|------|-------------|--------|:--------|
| REQ-PERF-001-1.1 | COUNT Function Performance - 1GB | Validate performance with a dataset of 1GB. |  | Pending |
| REQ-PERF-001-1.2 | COUNT Function Performance - 10GB | Validate performance with a dataset of 10GB. |  | Pending |
| REQ-PERF-001-1.3 | COUNT Function Performance - 100GB | Validate performance with a dataset of 100GB. |  | Pending |
| REQ-PERF-001-1.4 | COUNT Function Performance - 1TB | Validate performance with a dataset of 1TB. |  | Pending |
| REQ-PERF-001-1.5 | COUNT Function Performance - 10TB | Validate performance with a dataset of 10TB. |  | Pending |

#### REQ-PERF-002: Ensure system can efficiently compute distinct counts at scale.
**Description**: Evaluate the ability to perform COUNT(DISTINCT) operations with acceptable overhead across increasing data volumes.

| Case ID | Name | Description | Issues |  Status |
|---------|------|-------------|--------|:--------|
| REQ-PERF-002-2.1 | COUNT DISTINCT Performance - 1GB | Measure distinct count efficiency at 1GB. |  | Pending |
| REQ-PERF-002-2.2 | COUNT DISTINCT Performance - 10GB | Measure distinct count efficiency at 10GB. |  | Pending |

#### REQ-PERF-003: Validate efficiency of SUM operations over large datasets.
**Description**: Ensure SUM functions execute with optimal performance metrics at different data scales.

| Case ID | Name | Description | Issues |  Status |
|---------|------|-------------|--------|:--------|
| REQ-PERF-003-3.1 | SUM Function Performance - 1GB | Validate SUM operation efficiency with 1GB of data. |  | Pending |
| REQ-PERF-003-3.2 | SUM Function Performance - 10GB | Validate SUM operation efficiency with 10GB of data. |  | Pending |

#### REQ-PERF-004: Ensure system maintains average calculation efficiency.
**Description**: Verify AVG functions sustain performance as data sizes increase.

| Case ID | Name | Description | Issues |  Status |
|---------|------|-------------|--------|:--------|
| REQ-PERF-004-4.1 | AVG Function Performance - 1GB | Performance metrics for AVG operation on 1GB of data. |  | Pending |

#### REQ-PERF-005: Efficient computation of MIN/MAX operations.
**Description**: Confirm that minimum and maximum functions perform within the expected time frames across various datasets.

| Case ID | Name | Description | Issues |  Status |
|---------|------|-------------|--------|:--------|
| REQ-PERF-005-5.1 | MIN/MAX Performance - 1GB | Validate performance of MIN/MAX operations with 1GB. |  | Pending |

#### REQ-PERF-006: TPC-H benchmark testing on scalability.
**Description**: Evaluate system performance using TPC-H benchmark tests at different dataset volumes.

| Case ID | Name | Description | Issues |  Status |
|---------|------|-------------|--------|:--------|
| REQ-PERF-006-6.1 | TPC-H Performance - 10GB | Validate TPC-H benchmark performance with 10GB. |  | Pending |

#### REQ-PERF-007: ClickBench benchmark to test efficiency under different conditions.
**Description**: Assess system capabilities using ClickBench, targeting different data sizes.

| Case ID | Name | Description | Issues |  Status |
|---------|------|-------------|--------|:--------|
| REQ-PERF-007-7.1 | ClickBench Performance - 1GB | Evaluate with ClickBench on 1GB of data. |  | Pending |

