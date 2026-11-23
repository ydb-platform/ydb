//! # Tracer
//!
//! The OpenTelemetry library achieves in-process context propagation of
//! `Span`s by way of the `Tracer`.
//!
//! The `Tracer` is responsible for tracking the currently active `Span`,
//! and exposes methods for creating and activating new `Spans`.
//!
//! Docs: <https://github.com/open-telemetry/opentelemetry-specification/blob/v1.3.0/specification/trace/api.md#tracer>
use crate::trace::{
    provider::SdkTracerProvider,
    span::{Span, SpanData},
    SamplingDecision, SpanEvents, SpanLimits, SpanLinks,
};
use opentelemetry::{
    trace::{Span as _, SpanBuilder, SpanContext, SpanKind, Status, TraceContextExt, TraceFlags},
    Context, InstrumentationScope, KeyValue,
};
use std::fmt;
use std::sync::Arc;

/// `Tracer` implementation to create and manage spans
#[derive(Clone)]
pub struct SdkTracer {
    scope: Arc<InstrumentationScope>,
    provider: SdkTracerProvider,
}

impl fmt::Debug for SdkTracer {
    /// Formats the `Tracer` using the given formatter.
    /// Omitting `provider` here is necessary to avoid cycles.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Tracer")
            .field("name", &self.scope.name())
            .field("version", &self.scope.version())
            .finish()
    }
}

impl SdkTracer {
    /// Create a new tracer (used internally by `TracerProvider`s).
    pub(crate) fn new(scope: InstrumentationScope, provider: SdkTracerProvider) -> Self {
        SdkTracer {
            scope: Arc::new(scope),
            provider,
        }
    }

    /// TracerProvider associated with this tracer.
    pub(crate) fn provider(&self) -> &SdkTracerProvider {
        &self.provider
    }

    /// Instrumentation scope of this tracer.
    pub(crate) fn instrumentation_scope(&self) -> &InstrumentationScope {
        &self.scope
    }

    fn build_recording_span(
        &self,
        psc: &SpanContext,
        sc: SpanContext,
        mut builder: SpanBuilder,
        attrs: Vec<KeyValue>,
        span_limits: SpanLimits,
    ) -> Span {
        let mut attribute_options = builder.attributes.take().unwrap_or_default();
        for extra_attr in attrs {
            attribute_options.push(extra_attr);
        }
        let span_attributes_limit = span_limits.max_attributes_per_span as usize;
        let dropped_attributes_count = attribute_options
            .len()
            .saturating_sub(span_attributes_limit);
        attribute_options.truncate(span_attributes_limit);
        let dropped_attributes_count = dropped_attributes_count as u32;

        // Links are available as Option<Vec<Link>> in the builder
        // If it is None, then there are no links to process.
        // In that case Span.Links will be default (empty Vec<Link>, 0 drop count)
        // Otherwise, truncate Vec<Link> to keep until limits and use that in Span.Links.
        // Store the count of excess links into Span.Links.dropped_count.
        // There is no ability today to add Links after Span creation,
        // but such a capability will be needed in the future
        // once the spec for that stabilizes.

        let spans_links_limit = span_limits.max_links_per_span as usize;
        let span_links: SpanLinks = if let Some(mut links) = builder.links.take() {
            let dropped_count = links.len().saturating_sub(spans_links_limit);
            links.truncate(spans_links_limit);
            let link_attributes_limit = span_limits.max_attributes_per_link as usize;
            for link in links.iter_mut() {
                let dropped_attributes_count =
                    link.attributes.len().saturating_sub(link_attributes_limit);
                link.attributes.truncate(link_attributes_limit);
                link.dropped_attributes_count = dropped_attributes_count as u32;
            }
            SpanLinks {
                links,
                dropped_count: dropped_count as u32,
            }
        } else {
            SpanLinks::default()
        };

        let SpanBuilder {
            name,
            start_time,
            events,
            ..
        } = builder;

        let start_time = start_time.unwrap_or_else(opentelemetry::time::now);
        let spans_events_limit = span_limits.max_events_per_span as usize;
        let span_events: SpanEvents = if let Some(mut events) = events {
            let dropped_count = events.len().saturating_sub(spans_events_limit);
            events.truncate(spans_events_limit);
            let event_attributes_limit = span_limits.max_attributes_per_event as usize;
            for event in events.iter_mut() {
                let dropped_attributes_count = event
                    .attributes
                    .len()
                    .saturating_sub(event_attributes_limit);
                event.attributes.truncate(event_attributes_limit);
                event.dropped_attributes_count = dropped_attributes_count as u32;
            }
            SpanEvents {
                events,
                dropped_count: dropped_count as u32,
            }
        } else {
            SpanEvents::default()
        };
        Span::new(
            sc,
            Some(SpanData {
                parent_span_id: psc.span_id(),
                parent_span_is_remote: psc.is_valid() && psc.is_remote(),
                span_kind: builder.span_kind.take().unwrap_or(SpanKind::Internal),
                name,
                start_time,
                end_time: start_time,
                attributes: attribute_options,
                dropped_attributes_count,
                events: span_events,
                links: span_links,
                status: Status::default(),
            }),
            self.clone(),
            span_limits,
        )
    }
}

impl opentelemetry::trace::Tracer for SdkTracer {
    /// This implementation of `Tracer` produces `sdk::Span` instances.
    type Span = Span;

    /// Starts a span from a `SpanBuilder`.
    ///
    /// Each span has zero or one parent spans and zero or more child spans, which
    /// represent causally related operations. A tree of related spans comprises a
    /// trace. A span is said to be a _root span_ if it does not have a parent. Each
    /// trace includes a single root span, which is the shared ancestor of all other
    /// spans in the trace.
    fn build_with_context(&self, builder: SpanBuilder, parent_cx: &Context) -> Self::Span {
        if parent_cx.is_telemetry_suppressed() {
            return Span::new(
                SpanContext::empty_context(),
                None,
                self.clone(),
                SpanLimits::default(),
            );
        }

        let provider = self.provider();
        // no point start a span if the tracer provider has already being shutdown
        if provider.is_shutdown() {
            return Span::new(
                SpanContext::empty_context(),
                None,
                self.clone(),
                SpanLimits::default(),
            );
        }

        let config = provider.config();
        let span_id = config.id_generator.new_span_id();
        let trace_id;
        let mut psc = &SpanContext::empty_context();

        let parent_span = if parent_cx.has_active_span() {
            Some(parent_cx.span())
        } else {
            None
        };

        // Build context for sampling decision
        if let Some(sc) = parent_span.as_ref().map(|parent| parent.span_context()) {
            trace_id = sc.trace_id();
            psc = sc;
        } else {
            trace_id = config.id_generator.new_trace_id();
        };

        let samplings_result = config.sampler.should_sample(
            Some(parent_cx),
            trace_id,
            &builder.name,
            builder.span_kind.as_ref().unwrap_or(&SpanKind::Internal),
            builder.attributes.as_ref().unwrap_or(&Vec::new()),
            builder.links.as_deref().unwrap_or(&[]),
        );

        let trace_flags = parent_cx.span().span_context().trace_flags();
        let trace_state = samplings_result.trace_state;
        let span_limits = config.span_limits;
        // Build optional inner context, `None` if not recording.
        let mut span = match samplings_result.decision {
            SamplingDecision::RecordAndSample => {
                let sc = SpanContext::new(
                    trace_id,
                    span_id,
                    trace_flags.with_sampled(true),
                    false,
                    trace_state,
                );
                self.build_recording_span(
                    psc,
                    sc,
                    builder,
                    samplings_result.attributes,
                    span_limits,
                )
            }
            SamplingDecision::RecordOnly => {
                let sc = SpanContext::new(
                    trace_id,
                    span_id,
                    trace_flags.with_sampled(false),
                    false,
                    trace_state,
                );
                self.build_recording_span(
                    psc,
                    sc,
                    builder,
                    samplings_result.attributes,
                    span_limits,
                )
            }
            SamplingDecision::Drop => {
                let span_context =
                    SpanContext::new(trace_id, span_id, TraceFlags::default(), false, trace_state);
                Span::new(span_context, None, self.clone(), span_limits)
            }
        };

        if span.is_recording() {
            // Call `on_start` for all processors
            for processor in provider.span_processors() {
                processor.on_start(&mut span, parent_cx)
            }
        }

        span
    }
}

#[cfg(all(test, feature = "testing", feature = "trace"))]
mod tests {
    use crate::{
        testing::trace::TestSpan,
        trace::{Sampler, SamplingDecision, SamplingResult, ShouldSample},
    };
    use opentelemetry::{
        trace::{
            Link, Span, SpanContext, SpanId, SpanKind, TraceContextExt, TraceFlags, TraceId,
            TraceState, Tracer, TracerProvider,
        },
        Context, KeyValue,
    };

    #[derive(Clone, Debug)]
    struct TestSampler {}

    impl ShouldSample for TestSampler {
        fn should_sample(
            &self,
            parent_context: Option<&Context>,
            _trace_id: TraceId,
            _name: &str,
            _span_kind: &SpanKind,
            _attributes: &[KeyValue],
            _links: &[Link],
        ) -> SamplingResult {
            let trace_state = parent_context
                .unwrap()
                .span()
                .span_context()
                .trace_state()
                .clone();
            SamplingResult {
                decision: SamplingDecision::RecordAndSample,
                attributes: Vec::new(),
                trace_state: trace_state.insert("foo", "notbar").unwrap(),
            }
        }
    }

    #[test]
    fn allow_sampler_to_change_trace_state() {
        // Setup
        let sampler = TestSampler {};
        let tracer_provider = crate::trace::SdkTracerProvider::builder()
            .with_sampler(sampler)
            .build();
        let tracer = tracer_provider.tracer("test");
        let trace_state = TraceState::from_key_value(vec![("foo", "bar")]).unwrap();

        let parent_context = Context::new().with_span(TestSpan(SpanContext::new(
            TraceId::from(128),
            SpanId::from(64),
            TraceFlags::SAMPLED,
            true,
            trace_state,
        )));

        // Test sampler should change trace state
        let span = tracer.start_with_context("foo", &parent_context);
        let span_context = span.span_context();
        let expected = span_context.trace_state();
        assert_eq!(expected.get("foo"), Some("notbar"))
    }

    #[test]
    fn allow_sampler_to_change_trace_state_boxed() {
        // Setup
        let sampler: Box<dyn ShouldSample> = Box::new(TestSampler {});
        let tracer_provider = crate::trace::SdkTracerProvider::builder()
            .with_sampler(sampler)
            .build();
        let tracer = tracer_provider.tracer("test");
        let trace_state = TraceState::from_key_value(vec![("foo", "bar")]).unwrap();

        let parent_context = Context::new().with_span(TestSpan(SpanContext::new(
            TraceId::from(128),
            SpanId::from(64),
            TraceFlags::SAMPLED,
            true,
            trace_state,
        )));

        // Test sampler should change trace state
        let span = tracer.start_with_context("foo", &parent_context);
        let span_context = span.span_context();
        let expected = span_context.trace_state();
        assert_eq!(expected.get("foo"), Some("notbar"))
    }

    #[test]
    fn drop_parent_based_children() {
        let sampler = Sampler::ParentBased(Box::new(Sampler::AlwaysOn));
        let tracer_provider = crate::trace::SdkTracerProvider::builder()
            .with_sampler(sampler)
            .build();

        let context = Context::current_with_span(TestSpan(SpanContext::empty_context()));
        let tracer = tracer_provider.tracer("test");
        let span = tracer.start_with_context("must_not_be_sampled", &context);

        assert!(!span.span_context().is_sampled());
    }

    #[test]
    fn uses_current_context_for_builders_if_unset() {
        let sampler = Sampler::ParentBased(Box::new(Sampler::AlwaysOn));
        let tracer_provider = crate::trace::SdkTracerProvider::builder()
            .with_sampler(sampler)
            .build();
        let tracer = tracer_provider.tracer("test");

        let _attached = Context::current_with_span(TestSpan(SpanContext::empty_context())).attach();
        let span = tracer.span_builder("must_not_be_sampled").start(&tracer);
        assert!(!span.span_context().is_sampled());

        let context = Context::map_current(|cx| {
            cx.with_remote_span_context(SpanContext::new(
                TraceId::from(1),
                SpanId::from(1),
                TraceFlags::default(),
                true,
                Default::default(),
            ))
        });
        let _attached = context.attach();
        let span = tracer.span_builder("must_not_be_sampled").start(&tracer);

        assert!(!span.span_context().is_sampled());
    }

    #[test]
    fn in_span_with_context_uses_provided_context() {
        use crate::trace::{InMemorySpanExporter, SimpleSpanProcessor};

        let exporter = InMemorySpanExporter::default();
        let tracer_provider = crate::trace::SdkTracerProvider::builder()
            .with_sampler(Sampler::AlwaysOn)
            .with_span_processor(SimpleSpanProcessor::new(exporter.clone()))
            .build();
        let tracer = tracer_provider.tracer("test");

        // Create a parent context explicitly
        let parent_span = tracer.start("parent");
        let parent_trace_id = parent_span.span_context().trace_id();
        let parent_span_id = parent_span.span_context().span_id();
        let parent_cx = Context::current_with_span(parent_span);

        // Use in_span_with_context with explicit parent context
        let mut child_trace_id = None;
        let mut child_span_id = None;
        let mut executed = false;

        let returned_value = tracer.in_span_with_context("child-span", &parent_cx, |cx| {
            let span = cx.span();
            child_trace_id = Some(span.span_context().trace_id());
            child_span_id = Some(span.span_context().span_id());
            executed = true;
            "test_result"
        });

        // Verify child span inherited parent's trace_id
        assert_eq!(child_trace_id, Some(parent_trace_id));
        // Verify child has a different span_id than parent
        assert_ne!(child_span_id, Some(parent_span_id));
        // Verify the closure was executed
        assert!(executed);
        // Verify return value is passed through
        assert_eq!(returned_value, "test_result");

        // End the parent span to export it
        drop(parent_cx);

        // Verify parent-child relationship through exporter
        let spans = exporter.get_finished_spans().unwrap();
        assert_eq!(spans.len(), 2);
        let parent = spans.iter().find(|s| s.name == "parent").unwrap();
        let child = spans.iter().find(|s| s.name == "child-span").unwrap();
        assert_eq!(child.parent_span_id, parent.span_context.span_id());
        assert_eq!(
            child.span_context.trace_id(),
            parent.span_context.trace_id()
        );
    }

    #[test]
    fn in_span_with_builder_uses_current_context() {
        use crate::trace::{InMemorySpanExporter, SimpleSpanProcessor};

        let exporter = InMemorySpanExporter::default();
        let tracer_provider = crate::trace::SdkTracerProvider::builder()
            .with_sampler(Sampler::AlwaysOn)
            .with_span_processor(SimpleSpanProcessor::new(exporter.clone()))
            .build();
        let tracer = tracer_provider.tracer("test");

        // Create a parent span and attach it to the current context
        let parent_span = tracer.start("parent");
        let parent_trace_id = parent_span.span_context().trace_id();
        let parent_span_id = parent_span.span_context().span_id();
        let _attached = Context::current_with_span(parent_span).attach();

        // Use in_span_with_builder with configured span
        let mut child_trace_id = None;

        tracer.in_span_with_builder(
            tracer
                .span_builder("child")
                .with_kind(SpanKind::Client)
                .with_attributes(vec![KeyValue::new("test_key", "test_value")]),
            |cx| {
                let span = cx.span();
                child_trace_id = Some(span.span_context().trace_id());
            },
        );

        // Verify child span inherited parent's trace_id
        assert_eq!(child_trace_id, Some(parent_trace_id));

        // End the attached context to export the parent span
        drop(_attached);

        // Verify parent-child relationship through exporter
        let spans = exporter.get_finished_spans().unwrap();
        assert_eq!(spans.len(), 2);
        let child = spans.iter().find(|s| s.name == "child").unwrap();
        assert_eq!(child.parent_span_id, parent_span_id);
        assert_eq!(child.span_context.trace_id(), parent_trace_id);
    }

    #[test]
    fn in_span_with_builder_and_context_uses_provided_context() {
        use crate::trace::{InMemorySpanExporter, SimpleSpanProcessor};

        let exporter = InMemorySpanExporter::default();
        let tracer_provider = crate::trace::SdkTracerProvider::builder()
            .with_sampler(Sampler::AlwaysOn)
            .with_span_processor(SimpleSpanProcessor::new(exporter.clone()))
            .build();
        let tracer = tracer_provider.tracer("test");

        // Create a parent context explicitly
        let parent_span = tracer.start("parent");
        let parent_trace_id = parent_span.span_context().trace_id();
        let parent_span_id = parent_span.span_context().span_id();
        let parent_cx = Context::current_with_span(parent_span);

        // Use in_span_with_builder_and_context with explicit parent context
        let mut child_trace_id = None;
        let mut result = 0;

        let returned_value = tracer.in_span_with_builder_and_context(
            tracer
                .span_builder("child")
                .with_kind(SpanKind::Server)
                .with_attributes(vec![
                    KeyValue::new("http.method", "GET"),
                    KeyValue::new("http.url", "/api/test"),
                ]),
            &parent_cx,
            |cx| {
                let span = cx.span();
                child_trace_id = Some(span.span_context().trace_id());
                result = 42;
                result
            },
        );

        // Verify child span inherited parent's trace_id
        assert_eq!(child_trace_id, Some(parent_trace_id));
        // Verify return value is passed through
        assert_eq!(returned_value, 42);
        assert_eq!(result, 42);

        // End the parent span to export it
        drop(parent_cx);

        // Verify parent-child relationship through exporter
        let spans = exporter.get_finished_spans().unwrap();
        assert_eq!(spans.len(), 2);
        let child = spans.iter().find(|s| s.name == "child").unwrap();
        assert_eq!(child.parent_span_id, parent_span_id);
        assert_eq!(child.span_context.trace_id(), parent_trace_id);
    }

    #[test]
    fn in_span_with_builder_and_context_ignores_active_context() {
        use crate::trace::{InMemorySpanExporter, SimpleSpanProcessor};

        let exporter = InMemorySpanExporter::default();
        let tracer_provider = crate::trace::SdkTracerProvider::builder()
            .with_sampler(Sampler::AlwaysOn)
            .with_span_processor(SimpleSpanProcessor::new(exporter.clone()))
            .build();
        let tracer = tracer_provider.tracer("test");

        // Create an active context with a specific trace context
        let active_span_context = SpanContext::new(
            TraceId::from(1u128),
            SpanId::from(1u64),
            TraceFlags::SAMPLED,
            true,
            Default::default(),
        );
        let active_trace_id = active_span_context.trace_id();
        let active_span_id = active_span_context.span_id();
        let _attached = Context::current_with_span(TestSpan(active_span_context)).attach();

        // Create a different parent context with a different trace ID to explicitly provide
        let provided_span_context = SpanContext::new(
            TraceId::from(2u128),
            SpanId::from(2u64),
            TraceFlags::SAMPLED,
            true,
            Default::default(),
        );
        let provided_trace_id = provided_span_context.trace_id();
        let provided_span_id = provided_span_context.span_id();
        let provided_cx = Context::current_with_span(TestSpan(provided_span_context));

        // Ensure the two parents have different trace IDs
        assert_ne!(active_trace_id, provided_trace_id);

        // Use in_span_with_builder_and_context with explicit parent context
        let mut child_trace_id = None;

        tracer.in_span_with_builder_and_context(
            tracer.span_builder("child").with_kind(SpanKind::Internal),
            &provided_cx,
            |cx| {
                let span = cx.span();
                child_trace_id = Some(span.span_context().trace_id());
            },
        );

        // Verify child uses the provided context, NOT the active context
        assert_eq!(child_trace_id, Some(provided_trace_id));
        assert_ne!(child_trace_id, Some(active_trace_id));

        // Verify parent-child relationship through exporter
        let spans = exporter.get_finished_spans().unwrap();
        assert_eq!(spans.len(), 1);
        let child = &spans[0];
        assert_eq!(child.name, "child");
        assert_eq!(child.parent_span_id, provided_span_id);
        assert_eq!(child.span_context.trace_id(), provided_trace_id);
        assert_ne!(child.parent_span_id, active_span_id);
        assert_ne!(child.span_context.trace_id(), active_trace_id);
    }
}
