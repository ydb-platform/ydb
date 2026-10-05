// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#include <chrono>
#include <cstdint>
#include <mutex>
#include <new>
#include <utility>

#include "opentelemetry/common/key_value_iterable.h"
#include "opentelemetry/context/context.h"
#include "opentelemetry/context/runtime_context.h"
#include "opentelemetry/nostd/shared_ptr.h"
#include "opentelemetry/nostd/string_view.h"
#include "opentelemetry/nostd/variant.h"
#include "opentelemetry/sdk/instrumentationscope/instrumentation_scope.h"
#include "opentelemetry/sdk/instrumentationscope/scope_configurator.h"
#include "opentelemetry/sdk/trace/id_generator.h"
#include "opentelemetry/sdk/trace/sampler.h"
#include "opentelemetry/sdk/trace/tracer.h"
#include "opentelemetry/sdk/trace/tracer_config.h"
#include "opentelemetry/sdk/trace/tracer_context.h"
#include "opentelemetry/trace/context.h"
#include "opentelemetry/trace/default_span.h"
#include "opentelemetry/trace/span.h"
#include "opentelemetry/trace/span_context.h"
#include "opentelemetry/trace/span_context_kv_iterable.h"
#include "opentelemetry/trace/span_id.h"
#include "opentelemetry/trace/span_startoptions.h"
#include "opentelemetry/trace/trace_flags.h"
#include "opentelemetry/trace/trace_id.h"
#include "opentelemetry/trace/trace_state.h"
#include "opentelemetry/version.h"

#include "src/trace/span.h"

OPENTELEMETRY_BEGIN_NAMESPACE namespace sdk
{
  namespace trace
  {

  namespace
  {

  nostd::shared_ptr<opentelemetry::trace::Span> MakeNonRecordingSpan(
      opentelemetry::trace::SpanContext &&span_context) noexcept
  {
#if OPENTELEMETRY_HAVE_EXCEPTIONS
    try
    {
#endif
      return {std::make_shared<opentelemetry::trace::DefaultSpan>(std::move(span_context))};
#if OPENTELEMETRY_HAVE_EXCEPTIONS
    }
    catch (const std::bad_alloc &)
    {
      return {};
    }
#else
    return nostd::shared_ptr<opentelemetry::trace::Span>{
        new (std::nothrow) opentelemetry::trace::DefaultSpan(std::move(span_context))};
#endif
  }

  nostd::shared_ptr<opentelemetry::trace::Span> MakeSpan(
      std::shared_ptr<Tracer> &&tracer,
      nostd::string_view name,
      const opentelemetry::common::KeyValueIterable &attributes,
      const opentelemetry::trace::SpanContextKeyValueIterable &links,
      const opentelemetry::trace::StartSpanOptions &options,
      const opentelemetry::sdk::trace::SamplingResult &sampling_result,
      const opentelemetry::trace::SpanContext &parent_context,
      opentelemetry::trace::SpanContext &&span_context) noexcept
  {
#if OPENTELEMETRY_HAVE_EXCEPTIONS
    try
    {
#endif
      return {std::make_shared<Span>(std::move(tracer), name, attributes, links, options,
                                     sampling_result, parent_context, std::move(span_context))};
#if OPENTELEMETRY_HAVE_EXCEPTIONS
    }
    catch (const std::bad_alloc &)
    {
      return {};
    }
#else
    return nostd::shared_ptr<opentelemetry::trace::Span>{
        new (std::nothrow) Span{std::move(tracer), name, attributes, links, options,
                                sampling_result, parent_context, std::move(span_context)}};
#endif
  }

  }  // namespace

  Tracer::Tracer(std::shared_ptr<TracerContext> context,
                 std::unique_ptr<InstrumentationScope> instrumentation_scope) noexcept
      : instrumentation_scope_{std::move(instrumentation_scope)},
        context_{std::move(context)},
        tracer_config_(context_->GetTracerConfigurator().ComputeConfig(*instrumentation_scope_)),
        noop_span_{std::make_shared<opentelemetry::trace::DefaultSpan>(
            opentelemetry::trace::SpanContext::GetInvalid())}
  {
    UpdateEnabled(tracer_config_.IsEnabled());
  }

  nostd::shared_ptr<opentelemetry::trace::Span> Tracer::StartSpan(
      nostd::string_view name,
      const opentelemetry::common::KeyValueIterable &attributes,
      const opentelemetry::trace::SpanContextKeyValueIterable &links,
      const opentelemetry::trace::StartSpanOptions &options) noexcept
  {
    // Check if the tracer is enabled using the API Tracer::Enabled() accessor if available.
    if (!Enabled())
    {
      return noop_span_;
    }

    // Resolve parent span context from options or fall back to the current runtime context.
    const auto parent_context = [&options]() noexcept -> opentelemetry::trace::SpanContext {
      // 1. If the parent is a valid SpanContext, use it directly.
      // 2. If the parent is a Context with a span, extract the SpanContext.
      // 3. If the parent is a Context with the `is_root_span` flag set, return an invalid
      // SpanContext.
      // 4. If the parent is not provided, use the current runtime context to get the SpanContext.
      if (const auto *span_context =
              nostd::get_if<opentelemetry::trace::SpanContext>(&options.parent))
      {
        if (span_context->IsValid())
        {
          return *span_context;
        }
      }
      else if (const auto *context = nostd::get_if<context::Context>(&options.parent))
      {
        auto ctx_span_context = opentelemetry::trace::GetSpanContext(*context);
        if (ctx_span_context.IsValid())
        {
          return ctx_span_context;
        }
        else if (opentelemetry::trace::IsRootSpan(*context))
        {
          return opentelemetry::trace::SpanContext::GetInvalid();
        }
      }

      return opentelemetry::trace::GetSpanContext(
          opentelemetry::context::RuntimeContext::GetCurrent());
    }();

    IdGenerator &generator                     = GetIdGenerator();
    const opentelemetry::trace::SpanId span_id = generator.GenerateSpanId();
    const opentelemetry::trace::TraceId trace_id =
        parent_context.IsValid() ? parent_context.trace_id() : generator.GenerateTraceId();

    const auto sampling_result = context_->GetSampler().ShouldSample(
        parent_context, trace_id, name, options.kind, attributes, links);

    const opentelemetry::trace::TraceFlags trace_flags =
        [&]() noexcept -> opentelemetry::trace::TraceFlags {
      std::uint8_t flags = 0;
      if (parent_context.IsValid())
      {
        flags = parent_context.trace_flags().flags();
      }
      else if (generator.IsRandom())
      {
        flags = opentelemetry::trace::TraceFlags::kIsRandom;
      }

      if (sampling_result.IsSampled())
      {
        flags |= opentelemetry::trace::TraceFlags::kIsSampled;
      }
      else
      {
        flags &= ~opentelemetry::trace::TraceFlags::kIsSampled;
      }

      /* Support W3C Trace Context version 2. */
      flags &= opentelemetry::trace::TraceFlags::kAllW3CTraceContext2Flags;

      return opentelemetry::trace::TraceFlags(flags);
    }();

    const auto get_trace_state =
        [&sampling_result,
         &parent_context]() -> nostd::shared_ptr<opentelemetry::trace::TraceState> {
      if (sampling_result.trace_state)
      {
        return sampling_result.trace_state;
      }
      if (parent_context.IsValid())
      {
        return parent_context.trace_state();
      }
      return opentelemetry::trace::TraceState::GetDefault();
    };

    opentelemetry::trace::SpanContext span_context(trace_id, span_id, trace_flags, false,
                                                   get_trace_state());

    if (!sampling_result.IsRecording())
    {
      auto non_recording_span = MakeNonRecordingSpan(std::move(span_context));

      if (!non_recording_span)
      {
        return noop_span_;
      }
      return non_recording_span;
    }

    auto span = MakeSpan(shared_from_this(), name, attributes, links, options, sampling_result,
                         parent_context, std::move(span_context));
    if (!span)
    {
      return noop_span_;
    }
    return span;
  }

  void Tracer::ForceFlushWithMicroseconds(uint64_t timeout) noexcept
  {
    if (context_)
    {
      context_->ForceFlush(
          std::chrono::microseconds{static_cast<std::chrono::microseconds::rep>(timeout)});
    }
  }

  void Tracer::CloseWithMicroseconds(uint64_t timeout) noexcept
  {
    // Trace context is shared by many tracers.So we just call ForceFlush to flush all pending spans
    // and do not shutdown it.
    if (context_)
    {
      context_->ForceFlush(
          std::chrono::microseconds{static_cast<std::chrono::microseconds::rep>(timeout)});
    }
  }

  void Tracer::UpdateTracerConfig(TracerConfig config) noexcept
  {
    const bool enabled = config.IsEnabled();
    {
      std::lock_guard<std::mutex> lock(tracer_config_mutex_);
      tracer_config_ = config;
    }
    UpdateEnabled(enabled);
  }

  }  // namespace trace
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
