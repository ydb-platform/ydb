// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <map>
#include <memory>
#include <string>

#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace configuration
{

// Forward declarations
class AlwaysOffSamplerBuilder;
class AlwaysOnSamplerBuilder;
class BatchLogRecordProcessorBuilder;
class BatchSpanProcessorBuilder;
class ComposableAlwaysOffSamplerBuilder;
class ComposableAlwaysOnSamplerBuilder;
class ComposableParentThresholdSamplerBuilder;
class ComposableProbabilitySamplerBuilder;
class ComposableRuleBasedSamplerBuilder;
class CompositeSamplerBuilder;
class ConsoleLogRecordExporterBuilder;
class ConsolePushMetricExporterBuilder;
class ConsoleSpanExporterBuilder;
class ContainerResourceDetectorBuilder;
class ExtensionComposableSamplerBuilder;
class ExtensionLogRecordExporterBuilder;
class ExtensionLogRecordProcessorBuilder;
class ExtensionMetricProducerBuilder;
class ExtensionPullMetricExporterBuilder;
class ExtensionPushMetricExporterBuilder;
class ExtensionResourceDetectorBuilder;
class ExtensionSamplerBuilder;
class ExtensionSpanExporterBuilder;
class ExtensionSpanProcessorBuilder;
class HostResourceDetectorBuilder;
class JaegerRemoteSamplerBuilder;
class LoggerConfiguratorBuilder;
class LoggerProviderBuilder;
class MeterConfiguratorBuilder;
class MeterProviderBuilder;
class OpenCensusMetricProducerBuilder;
class OtlpFileLogRecordExporterBuilder;
class OtlpFilePushMetricExporterBuilder;
class OtlpFileSpanExporterBuilder;
class OtlpGrpcLogRecordExporterBuilder;
class OtlpGrpcPushMetricExporterBuilder;
class OtlpGrpcSpanExporterBuilder;
class OtlpHttpLogRecordExporterBuilder;
class OtlpHttpPushMetricExporterBuilder;
class OtlpHttpSpanExporterBuilder;
class ParentBasedSamplerBuilder;
class PeriodicMetricReaderBuilder;
class ProbabilitySamplerBuilder;
class ProcessResourceDetectorBuilder;
class PrometheusPullMetricExporterBuilder;
class PullMetricReaderBuilder;
class ServiceResourceDetectorBuilder;
class SimpleLogRecordProcessorBuilder;
class SimpleSpanProcessorBuilder;
class TextMapPropagatorBuilder;
class TraceIdRatioBasedSamplerBuilder;
class TracerConfiguratorBuilder;
class TracerProviderBuilder;

class OPENTELEMETRY_EXPORT Registry
{
public:
  Registry();
  Registry(Registry &&)                      = delete;
  Registry(const Registry &)                 = delete;
  Registry &operator=(Registry &&)           = delete;
  Registry &operator=(const Registry &other) = delete;

  ~Registry();

  /* Core optional components. */

  const OtlpHttpSpanExporterBuilder *GetOtlpHttpSpanBuilder() const
  {
    return otlp_http_span_builder_.get();
  }

  void SetOtlpHttpSpanBuilder(std::unique_ptr<OtlpHttpSpanExporterBuilder> &&builder);

  const OtlpGrpcSpanExporterBuilder *GetOtlpGrpcSpanBuilder() const
  {
    return otlp_grpc_span_builder_.get();
  }

  void SetOtlpGrpcSpanBuilder(std::unique_ptr<OtlpGrpcSpanExporterBuilder> &&builder);

  const OtlpFileSpanExporterBuilder *GetOtlpFileSpanBuilder() const
  {
    return otlp_file_span_builder_.get();
  }

  void SetOtlpFileSpanBuilder(std::unique_ptr<OtlpFileSpanExporterBuilder> &&builder);

  const ConsoleSpanExporterBuilder *GetConsoleSpanBuilder() const
  {
    return console_span_builder_.get();
  }

  void SetConsoleSpanBuilder(std::unique_ptr<ConsoleSpanExporterBuilder> &&builder);

  const OtlpHttpPushMetricExporterBuilder *GetOtlpHttpPushMetricExporterBuilder() const
  {
    return otlp_http_push_metric_builder_.get();
  }

  void SetOtlpHttpPushMetricExporterBuilder(
      std::unique_ptr<OtlpHttpPushMetricExporterBuilder> &&builder);

  const OtlpGrpcPushMetricExporterBuilder *GetOtlpGrpcPushMetricExporterBuilder() const
  {
    return otlp_grpc_push_metric_builder_.get();
  }

  void SetOtlpGrpcPushMetricExporterBuilder(
      std::unique_ptr<OtlpGrpcPushMetricExporterBuilder> &&builder);

  const OtlpFilePushMetricExporterBuilder *GetOtlpFilePushMetricExporterBuilder() const
  {
    return otlp_file_push_metric_builder_.get();
  }

  void SetOtlpFilePushMetricExporterBuilder(
      std::unique_ptr<OtlpFilePushMetricExporterBuilder> &&builder);

  const ConsolePushMetricExporterBuilder *GetConsolePushMetricExporterBuilder() const
  {
    return console_metric_builder_.get();
  }

  void SetConsolePushMetricExporterBuilder(
      std::unique_ptr<ConsolePushMetricExporterBuilder> &&builder);

  const PrometheusPullMetricExporterBuilder *GetPrometheusPullMetricExporterBuilder() const
  {
    return prometheus_metric_builder_.get();
  }

  void SetPrometheusPullMetricExporterBuilder(
      std::unique_ptr<PrometheusPullMetricExporterBuilder> &&builder);

  const OpenCensusMetricProducerBuilder *GetOpenCensusMetricProducerBuilder() const
  {
    return open_census_metric_producer_builder_.get();
  }

  void SetOpenCensusMetricProducerBuilder(
      std::unique_ptr<OpenCensusMetricProducerBuilder> &&builder);

  const PeriodicMetricReaderBuilder *GetPeriodicMetricReaderBuilder() const
  {
    return periodic_metric_reader_builder_.get();
  }

  void SetPeriodicMetricReaderBuilder(std::unique_ptr<PeriodicMetricReaderBuilder> &&builder);

  const PullMetricReaderBuilder *GetPullMetricReaderBuilder() const
  {
    return pull_metric_reader_builder_.get();
  }

  void SetPullMetricReaderBuilder(std::unique_ptr<PullMetricReaderBuilder> &&builder);

  const OtlpHttpLogRecordExporterBuilder *GetOtlpHttpLogRecordBuilder() const
  {
    return otlp_http_log_record_builder_.get();
  }

  void SetOtlpHttpLogRecordBuilder(std::unique_ptr<OtlpHttpLogRecordExporterBuilder> &&builder);

  const OtlpGrpcLogRecordExporterBuilder *GetOtlpGrpcLogRecordBuilder() const
  {
    return otlp_grpc_log_record_builder_.get();
  }

  void SetOtlpGrpcLogRecordBuilder(std::unique_ptr<OtlpGrpcLogRecordExporterBuilder> &&builder);

  const OtlpFileLogRecordExporterBuilder *GetOtlpFileLogRecordBuilder() const
  {
    return otlp_file_log_record_builder_.get();
  }

  void SetOtlpFileLogRecordBuilder(std::unique_ptr<OtlpFileLogRecordExporterBuilder> &&builder);

  const ConsoleLogRecordExporterBuilder *GetConsoleLogRecordBuilder() const
  {
    return console_log_record_builder_.get();
  }

  void SetConsoleLogRecordBuilder(std::unique_ptr<ConsoleLogRecordExporterBuilder> &&builder);

  /* Samplers. */

  const AlwaysOnSamplerBuilder *GetAlwaysOnSamplerBuilder() const
  {
    return always_on_sampler_builder_.get();
  }

  void SetAlwaysOnSamplerBuilder(std::unique_ptr<AlwaysOnSamplerBuilder> &&builder);

  const AlwaysOffSamplerBuilder *GetAlwaysOffSamplerBuilder() const
  {
    return always_off_sampler_builder_.get();
  }

  void SetAlwaysOffSamplerBuilder(std::unique_ptr<AlwaysOffSamplerBuilder> &&builder);

  const TraceIdRatioBasedSamplerBuilder *GetTraceIdRatioBasedSamplerBuilder() const
  {
    return trace_id_ratio_based_sampler_builder_.get();
  }

  void SetTraceIdRatioBasedSamplerBuilder(
      std::unique_ptr<TraceIdRatioBasedSamplerBuilder> &&builder);

  const ProbabilitySamplerBuilder *GetProbabilitySamplerBuilder() const
  {
    return probability_sampler_builder_.get();
  }

  void SetProbabilitySamplerBuilder(std::unique_ptr<ProbabilitySamplerBuilder> &&builder);

  const ParentBasedSamplerBuilder *GetParentBasedSamplerBuilder() const
  {
    return parent_based_sampler_builder_.get();
  }

  void SetParentBasedSamplerBuilder(std::unique_ptr<ParentBasedSamplerBuilder> &&builder);

  const JaegerRemoteSamplerBuilder *GetJaegerRemoteSamplerBuilder() const
  {
    return jaeger_remote_sampler_builder_.get();
  }

  void SetJaegerRemoteSamplerBuilder(std::unique_ptr<JaegerRemoteSamplerBuilder> &&builder);

  /* Composable samplers. */

  const ComposableAlwaysOnSamplerBuilder *GetComposableAlwaysOnSamplerBuilder() const
  {
    return composable_always_on_sampler_builder_.get();
  }

  void SetComposableAlwaysOnSamplerBuilder(
      std::unique_ptr<ComposableAlwaysOnSamplerBuilder> &&builder);

  const ComposableAlwaysOffSamplerBuilder *GetComposableAlwaysOffSamplerBuilder() const
  {
    return composable_always_off_sampler_builder_.get();
  }

  void SetComposableAlwaysOffSamplerBuilder(
      std::unique_ptr<ComposableAlwaysOffSamplerBuilder> &&builder);

  const ComposableProbabilitySamplerBuilder *GetComposableProbabilitySamplerBuilder() const
  {
    return composable_probability_sampler_builder_.get();
  }

  void SetComposableProbabilitySamplerBuilder(
      std::unique_ptr<ComposableProbabilitySamplerBuilder> &&builder);

  const ComposableParentThresholdSamplerBuilder *GetComposableParentThresholdSamplerBuilder() const
  {
    return composable_parent_threshold_sampler_builder_.get();
  }

  void SetComposableParentThresholdSamplerBuilder(
      std::unique_ptr<ComposableParentThresholdSamplerBuilder> &&builder);

  const ComposableRuleBasedSamplerBuilder *GetComposableRuleBasedSamplerBuilder() const
  {
    return composable_rule_based_sampler_builder_.get();
  }

  void SetComposableRuleBasedSamplerBuilder(
      std::unique_ptr<ComposableRuleBasedSamplerBuilder> &&builder);

  const CompositeSamplerBuilder *GetCompositeSamplerBuilder() const
  {
    return composite_sampler_builder_.get();
  }

  void SetCompositeSamplerBuilder(std::unique_ptr<CompositeSamplerBuilder> &&builder);

  /* Processors. */

  const BatchSpanProcessorBuilder *GetBatchSpanProcessorBuilder() const
  {
    return batch_span_processor_builder_.get();
  }

  void SetBatchSpanProcessorBuilder(std::unique_ptr<BatchSpanProcessorBuilder> &&builder);

  const SimpleSpanProcessorBuilder *GetSimpleSpanProcessorBuilder() const
  {
    return simple_span_processor_builder_.get();
  }

  void SetSimpleSpanProcessorBuilder(std::unique_ptr<SimpleSpanProcessorBuilder> &&builder);

  const BatchLogRecordProcessorBuilder *GetBatchLogRecordProcessorBuilder() const
  {
    return batch_log_record_processor_builder_.get();
  }

  void SetBatchLogRecordProcessorBuilder(std::unique_ptr<BatchLogRecordProcessorBuilder> &&builder);

  const SimpleLogRecordProcessorBuilder *GetSimpleLogRecordProcessorBuilder() const
  {
    return simple_log_record_processor_builder_.get();
  }

  void SetSimpleLogRecordProcessorBuilder(
      std::unique_ptr<SimpleLogRecordProcessorBuilder> &&builder);

  /* Configurators. */

  const TracerConfiguratorBuilder *GetTracerConfiguratorBuilder() const
  {
    return tracer_configurator_builder_.get();
  }

  void SetTracerConfiguratorBuilder(std::unique_ptr<TracerConfiguratorBuilder> &&builder);

  const MeterConfiguratorBuilder *GetMeterConfiguratorBuilder() const
  {
    return meter_configurator_builder_.get();
  }

  void SetMeterConfiguratorBuilder(std::unique_ptr<MeterConfiguratorBuilder> &&builder);

  const LoggerConfiguratorBuilder *GetLoggerConfiguratorBuilder() const
  {
    return logger_configurator_builder_.get();
  }

  void SetLoggerConfiguratorBuilder(std::unique_ptr<LoggerConfiguratorBuilder> &&builder);

  /* Provider builders. */

  const TracerProviderBuilder *GetTracerProviderBuilder() const
  {
    return tracer_provider_builder_.get();
  }

  void SetTracerProviderBuilder(std::unique_ptr<TracerProviderBuilder> &&builder);

  const MeterProviderBuilder *GetMeterProviderBuilder() const
  {
    return meter_provider_builder_.get();
  }

  void SetMeterProviderBuilder(std::unique_ptr<MeterProviderBuilder> &&builder);

  const LoggerProviderBuilder *GetLoggerProviderBuilder() const
  {
    return logger_provider_builder_.get();
  }

  void SetLoggerProviderBuilder(std::unique_ptr<LoggerProviderBuilder> &&builder);

  /* Resource detectors. */

  const ContainerResourceDetectorBuilder *GetContainerResourceDetectorBuilder() const
  {
    return container_resource_detector_builder_.get();
  }

  void SetContainerResourceDetectorBuilder(
      std::unique_ptr<ContainerResourceDetectorBuilder> &&builder);

  const HostResourceDetectorBuilder *GetHostResourceDetectorBuilder() const
  {
    return host_resource_detector_builder_.get();
  }

  void SetHostResourceDetectorBuilder(std::unique_ptr<HostResourceDetectorBuilder> &&builder);

  const ProcessResourceDetectorBuilder *GetProcessResourceDetectorBuilder() const
  {
    return process_resource_detector_builder_.get();
  }

  void SetProcessResourceDetectorBuilder(std::unique_ptr<ProcessResourceDetectorBuilder> &&builder);

  const ServiceResourceDetectorBuilder *GetServiceResourceDetectorBuilder() const
  {
    return service_resource_detector_builder_.get();
  }

  void SetServiceResourceDetectorBuilder(std::unique_ptr<ServiceResourceDetectorBuilder> &&builder);

  /* Extension points */

  const TextMapPropagatorBuilder *GetTextMapPropagatorBuilder(const std::string &name) const;

  void SetTextMapPropagatorBuilder(const std::string &name,
                                   std::unique_ptr<TextMapPropagatorBuilder> &&builder);

  const ExtensionSamplerBuilder *GetExtensionSamplerBuilder(const std::string &name) const;

  void SetExtensionSamplerBuilder(const std::string &name,
                                  std::unique_ptr<ExtensionSamplerBuilder> &&builder);

  const ExtensionComposableSamplerBuilder *GetExtensionComposableSamplerBuilder(
      const std::string &name) const;

  void SetExtensionComposableSamplerBuilder(
      const std::string &name,
      std::unique_ptr<ExtensionComposableSamplerBuilder> &&builder);

  const ExtensionSpanExporterBuilder *GetExtensionSpanExporterBuilder(
      const std::string &name) const;

  void SetExtensionSpanExporterBuilder(const std::string &name,
                                       std::unique_ptr<ExtensionSpanExporterBuilder> &&builder);

  const ExtensionSpanProcessorBuilder *GetExtensionSpanProcessorBuilder(
      const std::string &name) const;

  void SetExtensionSpanProcessorBuilder(const std::string &name,
                                        std::unique_ptr<ExtensionSpanProcessorBuilder> &&builder);

  const ExtensionPushMetricExporterBuilder *GetExtensionPushMetricExporterBuilder(
      const std::string &name) const;

  void SetExtensionPushMetricExporterBuilder(
      const std::string &name,
      std::unique_ptr<ExtensionPushMetricExporterBuilder> &&builder);

  const ExtensionPullMetricExporterBuilder *GetExtensionPullMetricExporterBuilder(
      const std::string &name) const;

  void SetExtensionPullMetricExporterBuilder(
      const std::string &name,
      std::unique_ptr<ExtensionPullMetricExporterBuilder> &&builder);

  const ExtensionLogRecordExporterBuilder *GetExtensionLogRecordExporterBuilder(
      const std::string &name) const;

  void SetExtensionLogRecordExporterBuilder(
      const std::string &name,
      std::unique_ptr<ExtensionLogRecordExporterBuilder> &&builder);

  const ExtensionLogRecordProcessorBuilder *GetExtensionLogRecordProcessorBuilder(
      const std::string &name) const;

  void SetExtensionLogRecordProcessorBuilder(
      const std::string &name,
      std::unique_ptr<ExtensionLogRecordProcessorBuilder> &&builder);

  const ExtensionMetricProducerBuilder *GetExtensionMetricProducerBuilder(
      const std::string &name) const;

  void SetExtensionMetricProducerBuilder(const std::string &name,
                                         std::unique_ptr<ExtensionMetricProducerBuilder> &&builder);

  const ExtensionResourceDetectorBuilder *GetExtensionResourceDetectorBuilder(
      const std::string &name) const;

  void SetExtensionResourceDetectorBuilder(
      const std::string &name,
      std::unique_ptr<ExtensionResourceDetectorBuilder> &&builder);

private:
  std::unique_ptr<OtlpHttpSpanExporterBuilder> otlp_http_span_builder_;
  std::unique_ptr<OtlpGrpcSpanExporterBuilder> otlp_grpc_span_builder_;
  std::unique_ptr<OtlpFileSpanExporterBuilder> otlp_file_span_builder_;
  std::unique_ptr<ConsoleSpanExporterBuilder> console_span_builder_;

  std::unique_ptr<OtlpHttpPushMetricExporterBuilder> otlp_http_push_metric_builder_;
  std::unique_ptr<OtlpGrpcPushMetricExporterBuilder> otlp_grpc_push_metric_builder_;
  std::unique_ptr<OtlpFilePushMetricExporterBuilder> otlp_file_push_metric_builder_;
  std::unique_ptr<ConsolePushMetricExporterBuilder> console_metric_builder_;
  std::unique_ptr<PrometheusPullMetricExporterBuilder> prometheus_metric_builder_;
  std::unique_ptr<PeriodicMetricReaderBuilder> periodic_metric_reader_builder_;
  std::unique_ptr<PullMetricReaderBuilder> pull_metric_reader_builder_;

  std::unique_ptr<OtlpHttpLogRecordExporterBuilder> otlp_http_log_record_builder_;
  std::unique_ptr<OtlpGrpcLogRecordExporterBuilder> otlp_grpc_log_record_builder_;
  std::unique_ptr<OtlpFileLogRecordExporterBuilder> otlp_file_log_record_builder_;
  std::unique_ptr<ConsoleLogRecordExporterBuilder> console_log_record_builder_;

  std::unique_ptr<AlwaysOnSamplerBuilder> always_on_sampler_builder_;
  std::unique_ptr<AlwaysOffSamplerBuilder> always_off_sampler_builder_;
  std::unique_ptr<TraceIdRatioBasedSamplerBuilder> trace_id_ratio_based_sampler_builder_;
  std::unique_ptr<ProbabilitySamplerBuilder> probability_sampler_builder_;
  std::unique_ptr<ParentBasedSamplerBuilder> parent_based_sampler_builder_;
  std::unique_ptr<JaegerRemoteSamplerBuilder> jaeger_remote_sampler_builder_;

  std::unique_ptr<ComposableAlwaysOnSamplerBuilder> composable_always_on_sampler_builder_;
  std::unique_ptr<ComposableAlwaysOffSamplerBuilder> composable_always_off_sampler_builder_;
  std::unique_ptr<ComposableProbabilitySamplerBuilder> composable_probability_sampler_builder_;
  std::unique_ptr<ComposableParentThresholdSamplerBuilder>
      composable_parent_threshold_sampler_builder_;
  std::unique_ptr<ComposableRuleBasedSamplerBuilder> composable_rule_based_sampler_builder_;
  std::unique_ptr<CompositeSamplerBuilder> composite_sampler_builder_;

  std::unique_ptr<BatchSpanProcessorBuilder> batch_span_processor_builder_;
  std::unique_ptr<SimpleSpanProcessorBuilder> simple_span_processor_builder_;
  std::unique_ptr<BatchLogRecordProcessorBuilder> batch_log_record_processor_builder_;
  std::unique_ptr<SimpleLogRecordProcessorBuilder> simple_log_record_processor_builder_;

  std::unique_ptr<TracerConfiguratorBuilder> tracer_configurator_builder_;
  std::unique_ptr<MeterConfiguratorBuilder> meter_configurator_builder_;
  std::unique_ptr<LoggerConfiguratorBuilder> logger_configurator_builder_;

  std::unique_ptr<TracerProviderBuilder> tracer_provider_builder_;
  std::unique_ptr<MeterProviderBuilder> meter_provider_builder_;
  std::unique_ptr<LoggerProviderBuilder> logger_provider_builder_;

  std::unique_ptr<OpenCensusMetricProducerBuilder> open_census_metric_producer_builder_;

  std::unique_ptr<ContainerResourceDetectorBuilder> container_resource_detector_builder_;
  std::unique_ptr<HostResourceDetectorBuilder> host_resource_detector_builder_;
  std::unique_ptr<ProcessResourceDetectorBuilder> process_resource_detector_builder_;
  std::unique_ptr<ServiceResourceDetectorBuilder> service_resource_detector_builder_;

  std::map<std::string, std::unique_ptr<TextMapPropagatorBuilder>> propagator_builders_;
  std::map<std::string, std::unique_ptr<ExtensionSamplerBuilder>> sampler_builders_;
  std::map<std::string, std::unique_ptr<ExtensionComposableSamplerBuilder>>
      composable_sampler_builders_;
  std::map<std::string, std::unique_ptr<ExtensionSpanExporterBuilder>> span_exporter_builders_;
  std::map<std::string, std::unique_ptr<ExtensionSpanProcessorBuilder>> span_processor_builders_;
  std::map<std::string, std::unique_ptr<ExtensionPushMetricExporterBuilder>>
      push_metric_exporter_builders_;
  std::map<std::string, std::unique_ptr<ExtensionPullMetricExporterBuilder>>
      pull_metric_exporter_builders_;
  std::map<std::string, std::unique_ptr<ExtensionLogRecordExporterBuilder>>
      log_record_exporter_builders_;
  std::map<std::string, std::unique_ptr<ExtensionLogRecordProcessorBuilder>>
      log_record_processor_builders_;
  std::map<std::string, std::unique_ptr<ExtensionMetricProducerBuilder>> metric_producer_builders_;
  std::map<std::string, std::unique_ptr<ExtensionResourceDetectorBuilder>>
      resource_detector_builders_;
};

}  // namespace configuration
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
