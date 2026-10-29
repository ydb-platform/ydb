//! Payload/memo size-limit validation infrastructure.
//!
//! Mirrors the size checks the Temporal server enforces. The per-(message, field) policy is
//! generated from the proto descriptors against the hand-authored `*_FIELDS` tables in
//! `build.rs`; adding or removing a payload-bearing field fails the build until they're updated.
//!
//! Size is the serialized proto size (`encoded_len()`), except for the map-aggregate helpers that
//! mirror the server's `len(key) + …` accounting (`map_payloads_sum` / `map_payload_data_sum`).
//!
//! Generated `PayloadLimitsValidatable` impls report each field's size and class to a
//! `PayloadLimitSink`. `validate_payload_limits` is a sink that logs warnings and returns the first
//! error-level violation.

use crate::protos::temporal::api::common::v1::{Memo, Payload, Payloads};
use prost::Message;

/// Which server-enforced size limit a payload field is subject to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[doc(hidden)]
pub enum LimitClass {
    /// Subject to the blob (payload) size limit.
    Blob,
    /// Subject to the memo size limit.
    Memo,
}

/// How a nested field is addressed within its parent.
#[derive(Debug, Clone, Copy)]
pub(crate) enum FieldIndexer<'a> {
    /// A singular field.
    None,
    /// The `n`-th element of a repeated field.
    Index(usize),
    /// A map entry with the given key.
    Key(&'a str),
}

/// Path of proto field names to the field being validated; proto names make it language-agnostic.
/// A helper sinks can embed to track location across `enter`/`exit`.
#[derive(Debug, Clone, Default)]
struct PayloadPath {
    segments: Vec<String>,
}

impl PayloadPath {
    fn push(&mut self, name: &str, indexer: FieldIndexer) {
        self.segments.push(match indexer {
            FieldIndexer::None => name.to_string(),
            FieldIndexer::Index(index) => format!("{name}[{index}]"),
            FieldIndexer::Key(key) => format!("{name}[{key}]"),
        });
    }
    fn pop(&mut self) {
        self.segments.pop();
    }
    /// The full path to a leaf field with proto name `field_name`.
    fn leaf(&self, field_name: &str) -> String {
        if self.segments.is_empty() {
            field_name.to_string()
        } else {
            format!("{}.{}", self.segments.join("."), field_name)
        }
    }
}

/// Receives one callback per validated payload field, with the field's size as the server measures
/// it. Implementors decide how to handle warnings and errors.
///
/// The generated traversal calls `enter`/`exit` around each nested message so the sink can track a
/// field's location for `check`.
pub(crate) trait PayloadLimitSink {
    /// Called for each validated payload field. `field_name` is the leaf field's proto name; `size`
    /// is the field's size in bytes for the given [`LimitClass`]. When `enforce_error` is `false`,
    /// the field may warn but must not produce an error-level violation.
    fn check(
        &mut self,
        field_name: &'static str,
        class: LimitClass,
        size: usize,
        enforce_error: bool,
    );

    /// Enter a nested-message field `name`, addressed within its parent by `indexer`.
    fn enter(&mut self, name: &'static str, indexer: FieldIndexer);

    /// Leave the most recently entered nested field.
    fn exit(&mut self);
}

/// Implemented via codegen for every outbound request message that transitively contains validated
/// payload fields.
pub(crate) trait PayloadLimitsValidatable {
    /// Reports each validated payload field's size and class to `sink`.
    fn validate_payload_limits(&self, sink: &mut dyn PayloadLimitSink);
}

/// Serialized size of a [`Payloads`] message, as the server measures it.
pub(crate) fn payloads_size(payloads: &Payloads) -> usize {
    payloads.encoded_len()
}

/// Serialized size of a single [`Payload`], as the server measures it.
pub(crate) fn payload_size(payload: &Payload) -> usize {
    payload.encoded_len()
}

/// Serialized size of a [`Memo`] message, as the server measures it.
pub(crate) fn memo_size(memo: &Memo) -> usize {
    memo.encoded_len()
}

/// Serialized size of an arbitrary proto message, as the server measures it. Used for messages the
/// server checks as a whole rather than per-payload-field (e.g. `Failure`).
pub(crate) fn message_size<M: Message>(message: &M) -> usize {
    message.encoded_len()
}

/// Aggregate size of a marker-style `map<string, Payloads>`, mirroring the server's
/// `sum(len(key) + payloads.Size())` accounting (e.g. `RecordMarkerCommandAttributes.details`).
pub(crate) fn map_payloads_sum<'a, K>(
    entries: impl IntoIterator<Item = (&'a K, &'a Payloads)>,
) -> usize
where
    K: AsRef<str> + 'a,
{
    entries
        .into_iter()
        .map(|(k, v)| k.as_ref().len() + v.encoded_len())
        .sum()
}

/// Aggregate size of a search-attribute-style `map<string, Payload>`, mirroring the server's
/// `sum(len(key) + len(payload.data))` accounting — note the server counts the **raw data** length
/// here, not the serialized payload size (e.g. `UpsertWorkflowSearchAttributes.indexed_fields`).
pub(crate) fn map_payload_data_sum<'a, K>(
    entries: impl IntoIterator<Item = (&'a K, &'a Payload)>,
) -> usize
where
    K: AsRef<str> + 'a,
{
    entries
        .into_iter()
        .map(|(k, v)| k.as_ref().len() + v.data.len())
        .sum()
}

/// Warn/error thresholds (bytes) for both limit classes. A `0` threshold disables that check for
/// that class: `0` warn = no warnings, `0` error = no error enforcement (warnings only).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[doc(hidden)]
pub struct PayloadLimits {
    /// Blob warning threshold (bytes); `0` disables blob warnings.
    pub blob_warn: usize,
    /// Blob error threshold (bytes); `0` disables blob error enforcement.
    pub blob_error: usize,
    /// Memo warning threshold (bytes); `0` disables memo warnings.
    pub memo_warn: usize,
    /// Memo error threshold (bytes); `0` disables memo error enforcement.
    pub memo_error: usize,
}

impl PayloadLimits {
    /// Returns the `(warn, error)` thresholds for a given class; `0` means disabled.
    fn thresholds(&self, class: LimitClass) -> (usize, usize) {
        match class {
            LimitClass::Blob => (self.blob_warn, self.blob_error),
            LimitClass::Memo => (self.memo_warn, self.memo_error),
        }
    }
}

/// Whether a violation exceeded the warning threshold or the error threshold.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[doc(hidden)]
pub enum LimitSeverity {
    /// Exceeded the warning threshold.
    Warning,
    /// Exceeded the error threshold.
    Error,
}

/// A payload field whose size exceeded one of its configured thresholds (warning or error).
#[derive(Debug, Clone, PartialEq, Eq)]
#[doc(hidden)]
pub struct PayloadLimitViolation {
    /// Path of proto field names from the root message
    /// (e.g. `commands[2].schedule_activity_task_command_attributes.input`).
    pub path: String,
    /// Which limit class the threshold belongs to.
    pub class: LimitClass,
    /// Which threshold was exceeded.
    pub severity: LimitSeverity,
    /// The field's measured size in bytes.
    pub size: usize,
    /// The threshold that was exceeded (warning threshold for warnings, error threshold for errors).
    pub limit: usize,
}

impl std::fmt::Display for PayloadLimitViolation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let limit_class = match self.class {
            LimitClass::Blob => "payloads",
            LimitClass::Memo => "memo",
        };
        let limit_kind = match self.severity {
            LimitSeverity::Warning => "warning",
            LimitSeverity::Error => "error",
        };
        write!(
            f,
            "[TMPRL1103] Attempted to upload {limit_class} with size that exceeded the {limit_kind} limit."
        )
    }
}

impl std::error::Error for PayloadLimitViolation {}

/// A [`PayloadLimitSink`] that collects violations without logging or policy decisions.
///
/// Each checked field is sorted into [`warnings`](Self::warnings) or [`errors`](Self::errors): a
/// field over its error threshold (when `enforce_error` and an error threshold are set) is an error;
/// otherwise a field over its warning threshold is a warning.
#[derive(Debug, Clone, Default)]
struct CollectingSink {
    limits: PayloadLimits,
    path: PayloadPath,
    /// Fields that exceeded their warning threshold (but not an enforced error threshold).
    warnings: Vec<PayloadLimitViolation>,
    /// Fields that exceeded their enforced error threshold.
    errors: Vec<PayloadLimitViolation>,
}

impl CollectingSink {
    /// A new collector that classifies fields against `limits`.
    fn new(limits: PayloadLimits) -> Self {
        Self {
            limits,
            ..Default::default()
        }
    }
}

impl PayloadLimitSink for CollectingSink {
    fn check(
        &mut self,
        field_name: &'static str,
        class: LimitClass,
        size: usize,
        enforce_error: bool,
    ) {
        let (warn, error) = self.limits.thresholds(class);
        if enforce_error && error > 0 && size > error {
            self.errors.push(PayloadLimitViolation {
                path: self.path.leaf(field_name),
                class,
                severity: LimitSeverity::Error,
                size,
                limit: error,
            });
        } else if warn > 0 && size > warn {
            self.warnings.push(PayloadLimitViolation {
                path: self.path.leaf(field_name),
                class,
                severity: LimitSeverity::Warning,
                size,
                limit: warn,
            });
        }
    }

    fn enter(&mut self, name: &'static str, indexer: FieldIndexer) {
        self.path.push(name, indexer);
    }
    fn exit(&mut self) {
        self.path.pop();
    }
}

/// Validate a message's payload fields against `limits`.
///
/// If any field exceeded its error threshold, logs the error(s) and returns the first one without
/// logging warnings; otherwise logs each warning and returns `None`. With no error thresholds set,
/// there are never errors, so this only warns and always returns `None`.
pub(crate) fn validate_payload_limits<M: PayloadLimitsValidatable + ?Sized>(
    msg: &M,
    limits: &PayloadLimits,
) -> Option<PayloadLimitViolation> {
    let mut sink = CollectingSink::new(*limits);
    msg.validate_payload_limits(&mut sink);

    if !sink.errors.is_empty() {
        for error in &sink.errors {
            error!(size = error.size, limit = error.limit, "{error}",);
        }
        return sink.errors.into_iter().next();
    }

    for warning in &sink.warnings {
        warn!(size = warning.size, limit = warning.limit, "{warning}",);
    }
    None
}

// Include the generated PayloadLimitsValidatable implementations.
include!(concat!(env!("OUT_DIR"), "/payload_limits_impl.rs"));

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    use crate::protos::temporal::api::{
        command::v1::{
            Command, CompleteWorkflowExecutionCommandAttributes,
            FailWorkflowExecutionCommandAttributes, ModifyWorkflowPropertiesCommandAttributes,
            RecordMarkerCommandAttributes, ScheduleActivityTaskCommandAttributes,
            ScheduleNexusOperationCommandAttributes, command::Attributes,
        },
        failure::v1::Failure,
        protocol::v1::Message,
        sdk::v1::UserMetadata,
        workflowservice::v1::{
            RespondActivityTaskFailedRequest, RespondWorkflowTaskCompletedRequest,
            StartWorkflowExecutionRequest,
        },
    };

    fn payload(data: &[u8]) -> Payload {
        Payload {
            metadata: HashMap::new(),
            data: data.to_vec(),
            external_payloads: vec![],
        }
    }

    #[test]
    fn map_payload_data_sum_counts_key_and_raw_data() {
        let mut m: HashMap<String, Payload> = HashMap::new();
        m.insert("ab".to_string(), payload(&[0u8; 10]));
        m.insert("cde".to_string(), payload(&[0u8; 20]));
        // (2 + 10) + (3 + 20) = 35
        assert_eq!(map_payload_data_sum(m.iter()), 35);
    }

    /// A sink that records the path of each visited field (order is not significant).
    #[derive(Default)]
    struct RecordingSink {
        path: PayloadPath,
        visited: Vec<String>,
    }
    impl PayloadLimitSink for RecordingSink {
        fn check(&mut self, field_name: &'static str, _: LimitClass, _: usize, _: bool) {
            self.visited.push(self.path.leaf(field_name));
        }
        fn enter(&mut self, name: &'static str, indexer: FieldIndexer) {
            self.path.push(name, indexer);
        }
        fn exit(&mut self) {
            self.path.pop();
        }
    }
    impl RecordingSink {
        /// The visited paths, sorted for order-independent comparison. Not deduped, so a field
        /// visited more than once would still show up as a duplicate.
        fn sorted(&self) -> Vec<String> {
            let mut v = self.visited.clone();
            v.sort();
            v
        }
    }

    fn memo_with_key_value(key: &str, data_len: usize) -> Memo {
        let mut fields = HashMap::new();
        fields.insert(key.to_string(), payload(&vec![0u8; data_len]));
        Memo { fields }
    }

    fn payloads(total_data: usize) -> Payloads {
        Payloads {
            payloads: vec![payload(&vec![0u8; total_data])],
        }
    }

    fn worker_limits(blob_error: usize, memo_error: usize) -> PayloadLimits {
        PayloadLimits {
            blob_warn: 10,
            blob_error,
            memo_warn: 10,
            memo_error,
        }
    }

    #[test]
    fn blob_field_over_error_limit_is_reported() {
        let req = StartWorkflowExecutionRequest {
            input: Some(payloads(1000)),
            ..Default::default()
        };
        let violation =
            validate_payload_limits(&req, &worker_limits(100, 100)).expect("should error");
        assert_eq!(violation.class, LimitClass::Blob);
        assert_eq!(violation.path, "input");
        assert!(violation.size > 100);
    }

    #[test]
    fn memo_field_uses_memo_limit() {
        // A memo larger than the memo error limit but under the blob error limit must still error,
        // proving the memo class routes to the memo threshold.
        let req = StartWorkflowExecutionRequest {
            memo: Some(memo_with_key_value("k", 50)),
            ..Default::default()
        };
        let limits = PayloadLimits {
            blob_warn: 10,
            blob_error: 1_000_000,
            memo_warn: 10,
            memo_error: 20,
        };
        let violation = validate_payload_limits(&req, &limits).expect("memo should error");
        assert_eq!(violation.class, LimitClass::Memo);
        assert_eq!(violation.path, "memo");
    }

    #[test]
    fn warn_only_classified_field_never_errors() {
        // RespondActivityTaskFailed.failure is classified warn-only (enforce_error = false), so even
        // with worker error limits a huge failure does not produce an error-level violation.
        let req = RespondActivityTaskFailedRequest {
            failure: Some(Failure {
                message: "x".repeat(10_000),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(validate_payload_limits(&req, &worker_limits(100, 100)).is_none());
    }

    #[test]
    fn under_limit_is_ok() {
        let req = StartWorkflowExecutionRequest {
            input: Some(payloads(5)),
            ..Default::default()
        };
        assert!(validate_payload_limits(&req, &worker_limits(100_000, 100_000)).is_none());
    }

    #[test]
    fn zero_warn_threshold_disables_warnings() {
        let req = StartWorkflowExecutionRequest {
            input: Some(payloads(1000)),
            ..Default::default()
        };
        // A nonzero warn threshold below the field size produces a (non-error) warning.
        let mut sink = CollectingSink::new(PayloadLimits {
            blob_warn: 100,
            blob_error: 0,
            memo_warn: 0,
            memo_error: 0,
        });
        req.validate_payload_limits(&mut sink);
        assert_eq!(sink.warnings.len(), 1);
        assert!(sink.errors.is_empty());

        // A `0` warn threshold disables the warning entirely.
        let mut sink = CollectingSink::new(PayloadLimits::default());
        req.validate_payload_limits(&mut sink);
        assert!(sink.warnings.is_empty());
    }

    #[test]
    fn blob_classed_memo_is_measured_as_fields_data_sum() {
        // upserted_memo is classified `blob`, so it is measured as the data-sum of its fields
        // (the server's line-1386 check), NOT whole-Memo proto size.
        let mut fields = HashMap::new();
        fields.insert("ab".to_string(), payload(&[0u8; 10]));
        fields.insert("cde".to_string(), payload(&[0u8; 20]));
        let attr = ModifyWorkflowPropertiesCommandAttributes {
            upserted_memo: Some(Memo { fields }),
        };
        // data-sum = (2 + 10) + (3 + 20) = 35. blob_error = 30 -> errors; classified as Blob.
        let violation = validate_payload_limits(&attr, &worker_limits(30, 1_000_000))
            .expect("blob fields-data-sum should error");
        assert_eq!(violation.class, LimitClass::Blob);
        assert_eq!(violation.path, "upserted_memo");
        assert_eq!(violation.size, 35);
    }

    #[test]
    fn marker_details_map_is_measured_as_payloads_sum() {
        // details is a map<string, Payloads>, measured as sum(len(key) + payloads.Size()).
        let mut details = HashMap::new();
        details.insert("marker".to_string(), payloads(1000));
        let attr = RecordMarkerCommandAttributes {
            details,
            ..Default::default()
        };
        let violation =
            validate_payload_limits(&attr, &worker_limits(100, 100)).expect("map-sum should error");
        assert_eq!(violation.class, LimitClass::Blob);
        assert_eq!(violation.path, "details");
    }

    #[test]
    fn single_payload_field_is_measured_as_payload_size() {
        // ScheduleNexusOperation.input is a single Payload (not Payloads).
        let attr = ScheduleNexusOperationCommandAttributes {
            input: Some(payload(&[0u8; 1000])),
            ..Default::default()
        };
        let violation = validate_payload_limits(&attr, &worker_limits(100, 100))
            .expect("single payload should error");
        assert_eq!(violation.class, LimitClass::Blob);
        assert_eq!(violation.path, "input");
    }

    #[test]
    fn whole_failure_is_measured_as_message_size() {
        // FailWorkflowExecution.failure is blob-classed and measured as the whole Failure proto size.
        let attr = FailWorkflowExecutionCommandAttributes {
            failure: Some(Failure {
                message: "x".repeat(1000),
                ..Default::default()
            }),
        };
        let violation = validate_payload_limits(&attr, &worker_limits(100, 100))
            .expect("whole-failure should error");
        assert_eq!(violation.class, LimitClass::Blob);
        assert_eq!(violation.path, "failure");
    }

    // --- CollectingSink: classification, independent of logging/early-return ---------------------

    #[test]
    fn collecting_sink_classifies_error_vs_warning() {
        // worker_limits(100, 100): blob_warn = 10, blob_error = Some(100).
        let mut sink = CollectingSink::new(worker_limits(100, 100));
        sink.check("over_error", LimitClass::Blob, 200, true);
        sink.check("over_warn", LimitClass::Blob, 50, true);
        sink.check("under_warn", LimitClass::Blob, 5, true);

        assert_eq!(sink.errors.len(), 1);
        assert_eq!(sink.errors[0].path, "over_error");
        assert_eq!(sink.errors[0].limit, 100);
        assert_eq!(sink.warnings.len(), 1);
        assert_eq!(sink.warnings[0].path, "over_warn");
        assert_eq!(sink.warnings[0].limit, 10);
    }

    #[test]
    fn collecting_sink_warn_only_field_never_errors() {
        let mut sink = CollectingSink::new(worker_limits(100, 100));
        // enforce_error = false: even way over the error threshold, this is a warning.
        sink.check("warn_only", LimitClass::Blob, 5000, false);
        assert!(sink.errors.is_empty());
        assert_eq!(sink.warnings.len(), 1);
        assert_eq!(sink.warnings[0].path, "warn_only");
    }

    #[test]
    fn collecting_sink_no_error_limit_only_warns() {
        // Warn threshold set, no error threshold: an over-threshold field warns (never errors),
        // even with enforce_error = true.
        let mut sink = CollectingSink::new(PayloadLimits {
            blob_warn: 100,
            blob_error: 0,
            memo_warn: 0,
            memo_error: 0,
        });
        sink.check("big", LimitClass::Blob, 101, true);
        assert!(sink.errors.is_empty());
        assert_eq!(sink.warnings.len(), 1);
        assert_eq!(sink.warnings[0].path, "big");
    }

    #[test]
    fn collecting_sink_routes_memo_to_memo_limit() {
        let mut sink = CollectingSink::new(worker_limits(1_000_000, 20));
        // Same size: blob is fine (huge blob limit) but memo errors (tiny memo limit).
        sink.check("blob_field", LimitClass::Blob, 100, true);
        sink.check("memo_field", LimitClass::Memo, 100, true);
        assert_eq!(sink.errors.len(), 1);
        assert_eq!(sink.errors[0].class, LimitClass::Memo);
        assert_eq!(sink.errors[0].path, "memo_field");
    }

    // --- Which fields get visited (visit order is not significant) -------------------------------

    #[test]
    fn visits_validated_fields_and_skips_not_validated() {
        let req = StartWorkflowExecutionRequest {
            input: Some(payloads(1)),
            memo: Some(memo_with_key_value("k", 1)),
            // `header` is classified not_validated, so it is never visited...
            header: Some(crate::protos::temporal::api::common::v1::Header {
                fields: {
                    let mut fields = HashMap::new();
                    fields.insert("h".to_string(), payload(&[1]));
                    fields
                },
            }),
            // ...and `user_metadata` is recursed into, but its summary/details are not_validated
            // (the server enforces dedicated limits only in the nexus path), so nothing is visited.
            user_metadata: Some(UserMetadata {
                summary: Some(payload(&[1])),
                details: Some(payload(&[1])),
            }),
            ..Default::default()
        };
        let mut sink = RecordingSink::default();
        req.validate_payload_limits(&mut sink);
        assert_eq!(sink.sorted(), vec!["input", "memo"]);
    }

    #[test]
    fn visits_only_present_fields() {
        // Only `input` is set; memo / user_metadata / search_attributes etc. are absent and must
        // not be visited.
        let req = StartWorkflowExecutionRequest {
            input: Some(payloads(1)),
            ..Default::default()
        };
        let mut sink = RecordingSink::default();
        req.validate_payload_limits(&mut sink);
        assert_eq!(sink.sorted(), vec!["input"]);
    }

    #[test]
    fn visits_payload_fields_of_each_command() {
        let req = RespondWorkflowTaskCompletedRequest {
            commands: vec![
                Command {
                    attributes: Some(Attributes::ScheduleActivityTaskCommandAttributes(
                        ScheduleActivityTaskCommandAttributes {
                            input: Some(payloads(1)),
                            ..Default::default()
                        },
                    )),
                    ..Default::default()
                },
                Command {
                    attributes: Some(Attributes::CompleteWorkflowExecutionCommandAttributes(
                        CompleteWorkflowExecutionCommandAttributes {
                            result: Some(payloads(1)),
                        },
                    )),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let mut sink = RecordingSink::default();
        req.validate_payload_limits(&mut sink);
        assert_eq!(
            sink.sorted(),
            vec![
                "commands[0].schedule_activity_task_command_attributes.input",
                "commands[1].complete_workflow_execution_command_attributes.result",
            ]
        );
    }

    #[test]
    fn visits_protocol_message_body() {
        // Message.body is a google.protobuf.Any (not payload-bearing), reached only because Message
        // is a forced whole-message leaf and the parent recurses into `messages`.
        let req = RespondWorkflowTaskCompletedRequest {
            messages: vec![Message {
                body: Some(Default::default()),
                ..Default::default()
            }],
            ..Default::default()
        };
        let mut sink = RecordingSink::default();
        req.validate_payload_limits(&mut sink);
        assert_eq!(sink.sorted(), vec!["messages[0].body"]);
    }
}
