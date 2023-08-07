# Changelog

## Unreleased

Full list of differences found in [this compare](https://github.com/open-telemetry/opentelemetry-proto/compare/v0.20.0...main).

## 0.20.0 - 2023-06-06

Full list of differences found in [this compare](https://github.com/open-telemetry/opentelemetry-proto/compare/v0.19.0...v0.20.0).

### Maturity

* Declare OTLP/JSON Stable.
  [#436](https://github.com/open-telemetry/opentelemetry-proto/pull/436)
  [#435](https://github.com/open-telemetry/opentelemetry-proto/pull/435)
* Provide stronger symbolic stability guarantees.
  [#432](https://github.com/open-telemetry/opentelemetry-proto/pull/432)
* Clarify how additive changes are handled.
  [#455](https://github.com/open-telemetry/opentelemetry-proto/pull/455)

### Changed

* Change the exponential histogram boundary condition.
  [#409](https://github.com/open-telemetry/opentelemetry-proto/pull/409)
* Clarify behavior for empty/not present/invalid trace_id and span_id fields.
  [#442](https://github.com/open-telemetry/opentelemetry-proto/pull/442)
* Change the collector trace endpoint to /v1/traces.
  [#449](https://github.com/open-telemetry/opentelemetry-proto/pull/449)

### Added

* Introduce `zero_threshold` field to `ExponentialHistogramDataPoint`.
  [#441](https://github.com/open-telemetry/opentelemetry-proto/pull/441)
  [#453](https://github.com/open-telemetry/opentelemetry-proto/pull/453)

### Removed

* Delete requirement to generate new trace/span id if an invalid id is received.
  [#444](https://github.com/open-telemetry/opentelemetry-proto/pull/444)

## 0.19.0 - 2022-08-03

Full list of differences found in [this compare](https://github.com/open-telemetry/opentelemetry-proto/compare/v0.18.0...v0.19.0).

### Changed

* Add `csharp_namespace` option to protos.
  ([#399](https://github.com/open-telemetry/opentelemetry-proto/pull/399))
* Fix some out-of-date urls which link to [specification](https://github.com/open-telemetry/opentelemetry-specification). ([#402](https://github.com/open-telemetry/opentelemetry-proto/pull/402))
* :stop_sign: [BREAKING] Delete deprecated InstrumentationLibrary, 
  InstrumentationLibraryLogs, InstrumentationLibrarySpans and
  InstrumentationLibraryMetrics messages. Delete deprecated
  instrumentation_library_logs, instrumentation_library_spans and
  instrumentation_library_metrics fields.

### Added

* Introduce Scope Attributes. [#395](https://github.com/open-telemetry/opentelemetry-proto/pull/395)
* Introduce partial success fields in `Export<signal>ServiceResponse`.
 [#414](https://github.com/open-telemetry/opentelemetry-proto/pull/414)

## 0.18.0 - 2022-05-17

Full list of differences found in [this compare](https://github.com/open-telemetry/opentelemetry-proto/compare/v0.17.0...v0.18.0).

### Changed

* Declare logs Stable.
  [(#376)](https://github.com/open-telemetry/opentelemetry-proto/pull/376)
* Metrics ExponentialHistogramDataPoint makes the `sum` optional
  (follows the same change in HistogramDataPOint in 0.15). [#392](https://github.com/open-telemetry/opentelemetry-proto/pull/392)

## 0.17.0 - 2022-05-06

Full list of differences found in [this compare](https://github.com/open-telemetry/opentelemetry-proto/compare/v0.16.0...v0.17.0).

### Changed

* Introduce optional `min` and `max` fields to the Histogram and ExponentialHistogram data points.
  [(#279)](https://github.com/open-telemetry/opentelemetry-proto/pull/279)

## 0.16.0 - 2022-03-31

Full list of differences found in [this compare](https://github.com/open-telemetry/opentelemetry-proto/compare/v0.15.0...v0.16.0).

### Removed

* Remove deprecated LogRecord.Name field (#373).

## 0.15.0 - 2022-03-19

Full list of differences found in [this compare](https://github.com/open-telemetry/opentelemetry-proto/compare/v0.14.0...v0.15.0).

### Changed

* Rename InstrumentationLibrary to InstrumentationScope (#362)

### Added

* Use optional for `sum` field to mark presence (#366)

## 0.14.0 - 2022-03-08

Full list of differences found in [this compare](https://github.com/open-telemetry/opentelemetry-proto/compare/v0.13.0...v0.14.0).

### Removed

* Deprecate LogRecord.Name field (#357)

## 0.13.0 - 2022-02-10

Full list of differences found in [this compare](https://github.com/open-telemetry/opentelemetry-proto/compare/v0.12.0...v0.13.0).

### Changed

* `Swagger` generation updated to `openapiv2` due to the use of an updated version of protoc in `otel/build-protobuf`
* Clarify attribute key uniqueness requirement (#350)
* Fix path to Go packages (#360)

### Added

* Add ObservedTimestamp to LogRecord. (#351)
* Add native kotlin support (#337)

### Removed

* Remove unused deprecated message StringKeyValue (#358)
* Remove experimental metrics config service (#359)
 
## 0.12.0 - 2022-01-19

Full list of differences found in [this compare](https://github.com/open-telemetry/opentelemetry-proto/compare/v0.11.0...v0.12.0).

### Changed

* Rename logs to log_records in InstrumentationLibraryLogs. (#352)

### Removed

* Remove deprecated messages and fields from traces. (#341)
* Remove deprecated messages and fields from metrics. (#342)

## 0.11.0 - 2021-10-07

Full list of differences found in [this compare](https://github.com/open-telemetry/opentelemetry-proto/compare/v0.10.0...v0.11.0).

### Added

* ExponentialHistogram is a base-2 exponential histogram described in [OTEP 149](https://github.com/open-telemetry/oteps/pull/149). (#322)
* Adds `TracesData`, `MetricsData`, and `LogsData` container types for common use
  in transporting multiple `ResourceSpans`, `ResourceMetrics`, and `ResourceLogs`. (#332)

## 0.10.0 - 2021-09-07

Full list of differences found in [this compare.](https://github.com/open-telemetry/opentelemetry-proto/compare/v0.9.0...v0.10.0)

### Maturity

* `collector/logs/*` is now considered `Beta`. (#311)
* `logs/*` is now considered `Beta`. (#311)

### Added

* Metrics data points add a `flags` field with one bit to represent explicitly missing data. (#316)

## 0.9.0 - 2021-04-12

Full list of differences found in [this compare.](https://github.com/open-telemetry/opentelemetry-proto/compare/v0.8.0...v0.9.0)

### Maturity

* `collector/metrics/*` is now considered `stable`. (#305)

### Changed: Metrics

* :stop_sign: [DATA MODEL CHANGE] Histogram/Summary sums must be monotonic counters of events (#302)
* :stop_sign: [DATA MODEL CHANGE] Clarify requirements and semantics for start time (#295)
* :stop_sign: [BREAKING] Deprecate `labels` field from NumberDataPoint, HistogramDataPoint, SummaryDataPoint and add equivalent `attributes` field (#283)
* :stop_sign: [BREAKING] Deprecate `filtered_labels` field from Exemplars and add equivalent `filtered_attributes` field (#283)

### Added

- Common - Add bytes (binary) as data type to AnyValue (#297)
- Common - Add schema_url fields as described in OTEP 0152 (#298)

### Removed

* Remove if no changes for this section before release.

## 0.8.0 - 2021-03-23

Full list of differences found in [this compare.](https://github.com/open-telemetry/opentelemetry-proto/compare/v0.7.0...v0.8.0)

### Historical breaking change notice

Release 0.8 was the last in the line of releases marked as "unstable".
This release broke compatibility in more ways than were recognized and
documented at the time of its release.  In particular, #278 created
the `NumberDataPoint` type and used it in several locations in place
of the former `DoubleDataPoint`.  The new `oneof` in `NumberDataPoint`
re-used the former `DoubleDataPoint` tag number, which means that
pre-0.8 `DoubleSum` and `DoubleGauge` points would parse correctly as
a 0.8 `Sum` and `Gauge` points containing double-valued numbers.

However, by virtue of a `syntax = "proto3"` declaration, the protocol
compiler for all versions of OTLP have not included field presence,
which means 0 values are not serialized.  **The result is that valid
OTLP 0.7 `DoubleSum` and `DoubleGauge` points would not parse
correctly as OTLP 0.8 data.**  Instead, they parse as
`NumberDataPoint` with a missing value in the `oneof` field.

### Changed: Metrics

* :stop_sign: [DEPRECATION] Deprecate IntSum, IntGauge, and IntDataPoint (#278)
* :stop_sign: [DEPRECATION] Deprecate IntExemplar (#281)
* :stop_sign: [DEPRECATION] Deprecate IntHistogram (#270)
* :stop_sign: [BREAKING] Rename DoubleGauge to Gauge (#278)
* :stop_sign: [BREAKING] Rename DoubleSum to Sum (#278)
* :stop_sign: [BREAKING] Rename DoubleDataPoint to NumberDataPoint (#278)
* :stop_sign: [BREAKING] Rename DoubleSummary to Summary (#269)
* :stop_sign: [BREAKING] Rename DoubleExemplar to Exemplar (#281)
* :stop_sign: [BREAKING] Rename DoubleHistogram to Histogram (#270)
* :stop_sign: [DATA MODEL CHANGE] Make explicit bounds compatible with OM/Prometheus (#262)

## 0.7.0 - 2021-01-28

Full list of differences found in [this compare.](https://github.com/open-telemetry/opentelemetry-proto/compare/v0.6.0...v0.7.0)

### Maturity

$$$Protobuf Encodings:**

* `collector/metrics/*` is now considered `Beta`. (#223)
* `collector/logs/*` is now considered `Alpha`. (#228)
* `logs/*` is now considered `Alpha`. (#228)
* `metrics/*` is now considered `Beta`. (#223)

### Changed

* Common/Logs/Metrics/Traces - Clarify empty instrumentation (#245)

### Added

* Metrics - Add SummaryDataPoint support to Metrics proto (#227)

## 0.6.0 - 2020-10-28

Full list of differences found in [this compare.](https://github.com/open-telemetry/opentelemetry-proto/compare/v0.5.0...v0.6.0)

### Maturity

* Clarify maturity guarantees (#225)

### Changed

* Traces - Deprecated old Span status code and added a new status code according to specification (#224)
** Marked for removal `2021-10-22` given Stability Guarantees.
* Rename ProbabilitySampler to TraceIdRatioBased (#221)

## 0.5.0 - 2020-08-31

Full list of differences found in [this compare.](https://github.com/open-telemetry/opentelemetry-proto/compare/v0.4.0...v0.5.0)

### Maturity Changes

**Protobuf Encodings:**

* `collector/trace/*` is now `Stable`.
* `common/*` is now `Stable`.
* `resource/*` is now `Stable`.
* `trace/trace.proto` is now `Stable`. (#160)

**JSON Encodings:**

* All messages are now `Alpha`.

### Changed

* :stop_sign: [BREAKING] Metrics - protocol was refactored, and lots of breaking changes.
** Removed MetricDescriptor and embedded into Metric and the new data types.
** Add new data types Gauge/Sum/Histogram.
** Make use of the "AggregationTemporality" into the data types that allow that support.
* Rename enum values to follow the proto3 style guide.

### Added

* Enable build to use docker image otel/build-protobuf to be used in CI.
** Can also be used by the languages to generate protos.

### Removed

* :stop_sign: [BREAKING] Remove generated golang structs from the repository

### Errata

The following was announced in the release, but has not yet been considered stable. Please see the latest
README.md for actual status.

> This is a Release Candidate to declare Metrics part of the protocol Stable.

## 0.4.0 - 2020-06-23

Full list of differences found in [this compare.](https://github.com/open-telemetry/opentelemetry-proto/compare/v0.3.0...v0.4.0)

### Changed

* Metrics - Add temporality to MetricDescriptor (#140).

### Added

* Metrics - Add Monotonic Types (#145)
* Common/Traces - Added support for arrays and maps for attribute values (AnyValue) (#157).

### Removed

* :stop_sign: [BREAKING] Metrics - Removed common labels from MetricDescriptor (#144).

### Errata

The following was announced in the release, but this was not considered Stable until `v0.5.0`

> This is a Release Candidate to declare Traces part of the protocol Stable.

## 0.3.0 - 2020-03-23

* Initial protos for trace, metrics, resource and OTLP.
