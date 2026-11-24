// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#include <chrono>
#include <exception>
#include <mutex>
#include <ostream>
#include <utility>
#include <vector>

#include "opentelemetry/logs/logger.h"
#include "opentelemetry/logs/noop.h"
#include "opentelemetry/nostd/shared_ptr.h"
#include "opentelemetry/nostd/string_view.h"
#include "opentelemetry/sdk/common/global_log_handler.h"
#include "opentelemetry/sdk/instrumentationscope/instrumentation_scope.h"
#include "opentelemetry/sdk/instrumentationscope/scope_configurator.h"
#include "opentelemetry/sdk/logs/logger.h"
#include "opentelemetry/sdk/logs/logger_config.h"
#include "opentelemetry/sdk/logs/logger_context.h"
#include "opentelemetry/sdk/logs/logger_provider.h"
#include "opentelemetry/sdk/logs/processor.h"
#include "opentelemetry/sdk/resource/resource.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace logs
{

namespace
{

nostd::shared_ptr<opentelemetry::logs::Logger> CreateNoopLoggerFallback()
{
  return nostd::shared_ptr<opentelemetry::logs::Logger>(new opentelemetry::logs::NoopLogger());
}

void LogGetLoggerConstructionFailure(const char *detail) noexcept
{
#if OPENTELEMETRY_HAVE_EXCEPTIONS
  try
  {
#endif
    OTEL_INTERNAL_LOG_ERROR("[LoggerProvider::GetLogger] Failed to construct logger: "
                            << detail << "; returning noop logger.");
#if OPENTELEMETRY_HAVE_EXCEPTIONS
  }
  catch (const std::exception &)  // NOLINT(bugprone-empty-catch)
  {
    // Logging can throw (typically std::bad_alloc from the string stream).
    // Swallow it so the noexcept GetLogger path cannot throw.
  }
#endif
}

}  // namespace

LoggerProvider::LoggerProvider(
    std::unique_ptr<LogRecordProcessor> &&processor,
    const opentelemetry::sdk::resource::Resource &resource,
    std::unique_ptr<instrumentationscope::ScopeConfigurator<LoggerConfig>> logger_configurator)
    : noop_logger_(CreateNoopLoggerFallback())
{
  std::vector<std::unique_ptr<LogRecordProcessor>> processors;
  processors.emplace_back(std::move(processor));
  context_ = std::make_shared<LoggerContext>(std::move(processors), resource,
                                             std::move(logger_configurator));
  OTEL_INTERNAL_LOG_DEBUG("[LoggerProvider] LoggerProvider created.");
  initialized_ = true;
}

LoggerProvider::LoggerProvider(
    std::vector<std::unique_ptr<LogRecordProcessor>> &&processors,
    const opentelemetry::sdk::resource::Resource &resource,
    std::unique_ptr<instrumentationscope::ScopeConfigurator<LoggerConfig>> logger_configurator)
    : context_{std::make_shared<LoggerContext>(std::move(processors),
                                               resource,
                                               std::move(logger_configurator))},
      noop_logger_(CreateNoopLoggerFallback()),
      initialized_(true)
{}

LoggerProvider::LoggerProvider()
    : context_{std::make_shared<LoggerContext>(std::vector<std::unique_ptr<LogRecordProcessor>>{})},
      noop_logger_(CreateNoopLoggerFallback()),
      initialized_(true)
{}

LoggerProvider::LoggerProvider(std::unique_ptr<LoggerContext> context)
    : context_(std::move(context)), noop_logger_(CreateNoopLoggerFallback()), initialized_(true)
{}

LoggerProvider::~LoggerProvider()
{
  // Logger hold the shared pointer to the context. So we can not use destructor of LoggerContext to
  // Shutdown and flush all pending recordables when we have more than one loggers. These
  // recordables may use the raw pointer of instrumentation_scope_ in Logger
  if (context_)
  {
    context_->Shutdown();
  }
}

opentelemetry::nostd::shared_ptr<opentelemetry::logs::Logger> LoggerProvider::GetLogger(
    opentelemetry::nostd::string_view logger_name,
    opentelemetry::nostd::string_view name,
    opentelemetry::nostd::string_view version,
    opentelemetry::nostd::string_view schema_url,
    const opentelemetry::common::KeyValueIterable &attributes) noexcept
{
  // https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/logs/data-model.md#field-instrumentationscope
  if (name.empty())
  {
    name = logger_name;
  }

#if OPENTELEMETRY_HAVE_EXCEPTIONS
  try
  {
#endif
    // Release the lock before construction-failure diagnostics can re-enter the provider.
    // Ensure only one thread can read/write from the map of loggers
    std::lock_guard<std::mutex> lock_guard{lock_};

    // If a logger with a name "logger_name" already exists, return it
    for (auto &logger : loggers_)
    {
      auto &logger_lib = logger->GetInstrumentationScope();
      if (logger->GetName() == logger_name &&
          logger_lib.equal(name, version, schema_url, &attributes))
      {
        return opentelemetry::nostd::shared_ptr<opentelemetry::logs::Logger>{logger};
      }
    }

    // GetLogger is noexcept and does not surface an error, so callers cannot
    // recover after a construction failure. Retrying would re-throw, catch, and
    // log on later GetLogger calls. After the first failure, treat the provider
    // as non-functional for new loggers.
    if (!initialized_)
    {
      return noop_logger_;
    }

    initialized_ = false;
    std::unique_ptr<instrumentationscope::InstrumentationScope> lib =
        instrumentationscope::InstrumentationScope::Create(name, version, schema_url, attributes);

    auto logger = std::make_shared<Logger>(logger_name, context_, std::move(lib));
    loggers_.push_back(logger);
    opentelemetry::nostd::shared_ptr<opentelemetry::logs::Logger> result{logger};
    initialized_ = true;
    return result;
#if OPENTELEMETRY_HAVE_EXCEPTIONS
  }
  catch (const std::exception &ex)
  {
    LogGetLoggerConstructionFailure(ex.what());
    return noop_logger_;
  }
  // User-provided scope configurators can throw any exception type, not just
  // std::exception. Catch everything so GetLogger stays noexcept.
  catch (...)
  {
    LogGetLoggerConstructionFailure("unknown exception");
    return noop_logger_;
  }
#endif
}

void LoggerProvider::AddProcessor(std::unique_ptr<LogRecordProcessor> processor) noexcept
{
  context_->AddProcessor(std::move(processor));
}

void LoggerProvider::UpdateLoggerConfigurator(
    std::unique_ptr<instrumentationscope::ScopeConfigurator<LoggerConfig>>
        logger_configurator) noexcept
{
  if (!logger_configurator)
  {
    OTEL_INTERNAL_LOG_ERROR(
        "[LoggerProvider::UpdateLoggerConfigurator] logger_configurator is null, "
        "ignoring.");
    return;
  }

  // Lock the provider mutex to ensure that calls to GetLogger are exclusive with respect to the
  // LoggerConfigurator update and corresponding LoggerConfig updates. This ensures that a Logger
  // will never be returned from GetLogger with a LoggerConfig that is out of date with respect to
  // the provider-level LoggerConfigurator.
  const std::lock_guard<std::mutex> guard(lock_);
  context_->SetLoggerConfigurator(std::move(logger_configurator));

  // The only way to set the LoggerConfig of a logger is on Logger construction in
  // LoggerProvider::GetLogger or through Logger::UpdateLoggerConfig (which is private and only
  // accessed by LoggerProvider).
  for (auto &logger : loggers_)
  {
    LoggerConfig new_config =
        context_->GetLoggerConfigurator().ComputeConfig(logger->GetInstrumentationScope());
    logger->UpdateLoggerConfig(new_config);
  }
}

const opentelemetry::sdk::resource::Resource &LoggerProvider::GetResource() const noexcept
{
  return context_->GetResource();
}

bool LoggerProvider::Shutdown(std::chrono::microseconds timeout) noexcept
{
  return context_->Shutdown(timeout);
}

bool LoggerProvider::ForceFlush(std::chrono::microseconds timeout) noexcept
{
  return context_->ForceFlush(timeout);
}

}  // namespace logs
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
