// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#include <stdint.h>
#include <limits>
#include <ostream>
#include <string>
#include <utility>

#include "opentelemetry/context/context.h"
#include "opentelemetry/version.h"

#ifdef OPENTELEMETRY_HAVE_METRICS_BOUND_INSTRUMENTS_PREVIEW
#  include "opentelemetry/metrics/sync_instruments.h"
#endif

#include "opentelemetry/nostd/unique_ptr.h"
#include "opentelemetry/sdk/common/global_log_handler.h"
#include "opentelemetry/sdk/metrics/instruments.h"
#include "opentelemetry/sdk/metrics/state/metric_storage.h"
#include "opentelemetry/sdk/metrics/sync_instruments.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace metrics
{
namespace
{

bool ToInt64Value(uint64_t value, const char *operation, int64_t &converted) noexcept
{
  if (value > static_cast<uint64_t>(std::numeric_limits<int64_t>::max()))
  {
    OTEL_INTERNAL_LOG_WARN(operation << " Value not recorded - value exceeds int64_t max");
    return false;
  }
  converted = static_cast<int64_t>(value);
  return true;
}

}  // namespace

LongCounter::LongCounter(const InstrumentDescriptor &instrument_descriptor,
                         std::unique_ptr<SyncWritableMetricStorage> storage)
    : Synchronous(instrument_descriptor, std::move(storage))
{
  if (!storage_)
  {
    OTEL_INTERNAL_LOG_ERROR("[LongCounter::LongCounter] - Error constructing LongCounter."
                            << "The metric storage is invalid for " << instrument_descriptor.name_);
  }
}

void LongCounter::Add(uint64_t value,
                      const opentelemetry::common::KeyValueIterable &attributes) noexcept
{
  auto context = opentelemetry::context::Context{};
  if (!storage_)
  {
    OTEL_INTERNAL_LOG_WARN("[LongCounter::Add(V,A)] Value not recorded - invalid storage for: "
                           << instrument_descriptor_.name_);
    return;
  }
  int64_t converted = 0;
  if (ToInt64Value(value, "[LongCounter::Add(V,A)]", converted))
  {
    return storage_->RecordLong(converted, attributes, context);
  }
}

void LongCounter::Add(uint64_t value,
                      const opentelemetry::common::KeyValueIterable &attributes,
                      const opentelemetry::context::Context &context) noexcept
{
  if (!storage_)
  {
    OTEL_INTERNAL_LOG_WARN("[LongCounter::Add(V,A,C)] Value not recorded - invalid storage for: "
                           << instrument_descriptor_.name_);
    return;
  }
  int64_t converted = 0;
  if (ToInt64Value(value, "[LongCounter::Add(V,A,C)]", converted))
  {
    return storage_->RecordLong(converted, attributes, context);
  }
}

void LongCounter::Add(uint64_t value) noexcept
{
  auto context = opentelemetry::context::Context{};
  if (!storage_)
  {
    OTEL_INTERNAL_LOG_WARN("[LongCounter::Add(V)] Value not recorded - invalid storage for: "
                           << instrument_descriptor_.name_);
    return;
  }
  int64_t converted = 0;
  if (ToInt64Value(value, "[LongCounter::Add(V)]", converted))
  {
    return storage_->RecordLong(converted, context);
  }
}

void LongCounter::Add(uint64_t value, const opentelemetry::context::Context &context) noexcept
{
  if (!storage_)
  {
    OTEL_INTERNAL_LOG_WARN("[LongCounter::Add(V,C)] Value not recorded - invalid storage for: "
                           << instrument_descriptor_.name_);
    return;
  }
  int64_t converted = 0;
  if (ToInt64Value(value, "[LongCounter::Add(V,C)]", converted))
  {
    return storage_->RecordLong(converted, context);
  }
}

DoubleCounter::DoubleCounter(const InstrumentDescriptor &instrument_descriptor,
                             std::unique_ptr<SyncWritableMetricStorage> storage)
    : Synchronous(instrument_descriptor, std::move(storage))
{
  if (!storage_)
  {
    OTEL_INTERNAL_LOG_ERROR("[DoubleCounter::DoubleCounter] - Error constructing DoubleCounter."
                            << "The metric storage is invalid for " << instrument_descriptor.name_);
  }
}

void DoubleCounter::Add(double value,
                        const opentelemetry::common::KeyValueIterable &attributes) noexcept
{
  if (value < 0)
  {
    OTEL_INTERNAL_LOG_WARN("[DoubleCounter::Add(V,A)] Value not recorded - negative value for: "
                           << instrument_descriptor_.name_);
    return;
  }
  if (!storage_)
  {
    OTEL_INTERNAL_LOG_WARN("[DoubleCounter::Add(V,A)] Value not recorded - invalid storage for: "
                           << instrument_descriptor_.name_);
    return;
  }
  auto context = opentelemetry::context::Context{};
  return storage_->RecordDouble(value, attributes, context);
}

void DoubleCounter::Add(double value,
                        const opentelemetry::common::KeyValueIterable &attributes,
                        const opentelemetry::context::Context &context) noexcept
{
  if (value < 0)
  {
    OTEL_INTERNAL_LOG_WARN("[DoubleCounter::Add(V,A,C)] Value not recorded - negative value for: "
                           << instrument_descriptor_.name_);
    return;
  }
  if (!storage_)
  {
    OTEL_INTERNAL_LOG_WARN("[DoubleCounter::Add(V,A,C)] Value not recorded - invalid storage for: "
                           << instrument_descriptor_.name_);
    return;
  }
  return storage_->RecordDouble(value, attributes, context);
}

void DoubleCounter::Add(double value) noexcept
{
  if (value < 0)
  {
    OTEL_INTERNAL_LOG_WARN("[DoubleCounter::Add(V)] Value not recorded - negative value for: "
                           << instrument_descriptor_.name_);
    return;
  }
  if (!storage_)
  {
    OTEL_INTERNAL_LOG_WARN("[DoubleCounter::Add(V)] Value not recorded - invalid storage for: "
                           << instrument_descriptor_.name_);
    return;
  }
  auto context = opentelemetry::context::Context{};
  return storage_->RecordDouble(value, context);
}

void DoubleCounter::Add(double value, const opentelemetry::context::Context &context) noexcept
{
  if (value < 0)
  {
    OTEL_INTERNAL_LOG_WARN("[DoubleCounter::Add(V)] Value not recorded - negative value for: "
                           << instrument_descriptor_.name_);
    return;
  }
  if (!storage_)
  {
    OTEL_INTERNAL_LOG_WARN("[DoubleCounter::Add(V,C)] Value not recorded - invalid storage for: "
                           << instrument_descriptor_.name_);
    return;
  }
  return storage_->RecordDouble(value, context);
}

LongUpDownCounter::LongUpDownCounter(const InstrumentDescriptor &instrument_descriptor,
                                     std::unique_ptr<SyncWritableMetricStorage> storage)
    : Synchronous(instrument_descriptor, std::move(storage))
{
  if (!storage_)
  {
    OTEL_INTERNAL_LOG_ERROR(
        "[LongUpDownCounter::LongUpDownCounter] - Error constructing LongUpDownCounter."
        << "The metric storage is invalid for " << instrument_descriptor.name_);
  }
}

void LongUpDownCounter::Add(int64_t value,
                            const opentelemetry::common::KeyValueIterable &attributes) noexcept
{
  auto context = opentelemetry::context::Context{};
  if (!storage_)
  {
    OTEL_INTERNAL_LOG_WARN(
        "[LongUpDownCounter::Add(V,A)] Value not recorded - invalid storage for: "
        << instrument_descriptor_.name_);
    return;
  }
  return storage_->RecordLong(value, attributes, context);
}

void LongUpDownCounter::Add(int64_t value,
                            const opentelemetry::common::KeyValueIterable &attributes,
                            const opentelemetry::context::Context &context) noexcept
{
  if (!storage_)
  {
    OTEL_INTERNAL_LOG_WARN(
        "[LongUpDownCounter::Add(V,A,C)] Value not recorded - invalid storage for: "
        << instrument_descriptor_.name_);
    return;
  }
  return storage_->RecordLong(value, attributes, context);
}

void LongUpDownCounter::Add(int64_t value) noexcept
{
  auto context = opentelemetry::context::Context{};
  if (!storage_)
  {
    OTEL_INTERNAL_LOG_WARN("[LongUpDownCounter::Add(V)] Value not recorded - invalid storage for: "
                           << instrument_descriptor_.name_);
    return;
  }
  return storage_->RecordLong(value, context);
}

void LongUpDownCounter::Add(int64_t value, const opentelemetry::context::Context &context) noexcept
{
  if (!storage_)
  {
    OTEL_INTERNAL_LOG_WARN(
        "[LongUpDownCounter::Add(V,C)] Value not recorded - invalid storage for: "
        << instrument_descriptor_.name_);
    return;
  }
  return storage_->RecordLong(value, context);
}

DoubleUpDownCounter::DoubleUpDownCounter(const InstrumentDescriptor &instrument_descriptor,
                                         std::unique_ptr<SyncWritableMetricStorage> storage)
    : Synchronous(instrument_descriptor, std::move(storage))
{
  if (!storage_)
  {
    OTEL_INTERNAL_LOG_ERROR(
        "[DoubleUpDownCounter::DoubleUpDownCounter] - Error constructing DoubleUpDownCounter."
        << "The metric storage is invalid for " << instrument_descriptor.name_);
  }
}

void DoubleUpDownCounter::Add(double value,
                              const opentelemetry::common::KeyValueIterable &attributes) noexcept
{
  if (!storage_)
  {
    OTEL_INTERNAL_LOG_WARN(
        "[DoubleUpDownCounter::Add(V,A)] Value not recorded - invalid storage for: "
        << instrument_descriptor_.name_);
  }
  auto context = opentelemetry::context::Context{};
  return storage_->RecordDouble(value, attributes, context);
}

void DoubleUpDownCounter::Add(double value,
                              const opentelemetry::common::KeyValueIterable &attributes,
                              const opentelemetry::context::Context &context) noexcept
{
  if (!storage_)
  {
    OTEL_INTERNAL_LOG_WARN(
        "[DoubleUpDownCounter::Add(V,A,C)] Value not recorded - invalid storage for: "
        << instrument_descriptor_.name_);
    return;
  }
  return storage_->RecordDouble(value, attributes, context);
}

void DoubleUpDownCounter::Add(double value) noexcept
{
  if (!storage_)
  {
    OTEL_INTERNAL_LOG_WARN(
        "[DoubleUpDownCounter::Add(V)] Value not recorded - invalid storage for: "
        << instrument_descriptor_.name_);
    return;
  }
  auto context = opentelemetry::context::Context{};
  return storage_->RecordDouble(value, context);
}

void DoubleUpDownCounter::Add(double value, const opentelemetry::context::Context &context) noexcept
{
  if (!storage_)
  {
    OTEL_INTERNAL_LOG_WARN(
        "[DoubleUpDownCounter::Add(V,C)] Value not recorded - invalid storage for: "
        << instrument_descriptor_.name_);
    return;
  }
  return storage_->RecordDouble(value, context);
}

#if OPENTELEMETRY_ABI_VERSION_NO >= 2
LongGauge::LongGauge(const InstrumentDescriptor &instrument_descriptor,
                     std::unique_ptr<SyncWritableMetricStorage> storage)
    : Synchronous(instrument_descriptor, std::move(storage))
{
  if (!storage_)
  {
    OTEL_INTERNAL_LOG_ERROR("[LongGauge::LongGauge] - Error constructing LongGauge."
                            << "The metric storage is invalid for " << instrument_descriptor.name_);
  }
}

void LongGauge::Record(int64_t value,
                       const opentelemetry::common::KeyValueIterable &attributes) noexcept
{
  auto context = opentelemetry::context::Context{};
  if (!storage_)
  {
    OTEL_INTERNAL_LOG_WARN("[LongGauge::Record(V,A)] Value not recorded - invalid storage for: "
                           << instrument_descriptor_.name_);
    return;
  }
  return storage_->RecordLong(value, attributes, context);
}

void LongGauge::Record(int64_t value,
                       const opentelemetry::common::KeyValueIterable &attributes,
                       const opentelemetry::context::Context &context) noexcept
{
  if (!storage_)
  {
    OTEL_INTERNAL_LOG_WARN("[LongGauge::Record(V,A,C)] Value not recorded - invalid storage for: "
                           << instrument_descriptor_.name_);
    return;
  }
  return storage_->RecordLong(value, attributes, context);
}

void LongGauge::Record(int64_t value) noexcept
{
  auto context = opentelemetry::context::Context{};
  if (!storage_)
  {
    OTEL_INTERNAL_LOG_WARN("[LongGauge::Record(V)] Value not recorded - invalid storage for: "
                           << instrument_descriptor_.name_);
    return;
  }
  return storage_->RecordLong(value, context);
}

void LongGauge::Record(int64_t value, const opentelemetry::context::Context &context) noexcept
{
  if (!storage_)
  {
    OTEL_INTERNAL_LOG_WARN("[LongGauge::Record(V,C)] Value not recorded - invalid storage for: "
                           << instrument_descriptor_.name_);
    return;
  }
  return storage_->RecordLong(value, context);
}

DoubleGauge::DoubleGauge(const InstrumentDescriptor &instrument_descriptor,
                         std::unique_ptr<SyncWritableMetricStorage> storage)
    : Synchronous(instrument_descriptor, std::move(storage))
{
  if (!storage_)
  {
    OTEL_INTERNAL_LOG_ERROR("[DoubleGauge::DoubleGauge] - Error constructing DoubleUpDownCounter."
                            << "The metric storage is invalid for " << instrument_descriptor.name_);
  }
}

void DoubleGauge::Record(double value,
                         const opentelemetry::common::KeyValueIterable &attributes) noexcept
{
  if (!storage_)
  {
    OTEL_INTERNAL_LOG_WARN("[DoubleGauge::Record(V,A)] Value not recorded - invalid storage for: "
                           << instrument_descriptor_.name_);
  }
  auto context = opentelemetry::context::Context{};
  return storage_->RecordDouble(value, attributes, context);
}

void DoubleGauge::Record(double value,
                         const opentelemetry::common::KeyValueIterable &attributes,
                         const opentelemetry::context::Context &context) noexcept
{
  if (!storage_)
  {
    OTEL_INTERNAL_LOG_WARN("[DoubleGauge::Record(V,A,C)] Value not recorded - invalid storage for: "
                           << instrument_descriptor_.name_);
    return;
  }
  return storage_->RecordDouble(value, attributes, context);
}

void DoubleGauge::Record(double value) noexcept
{
  if (!storage_)
  {
    OTEL_INTERNAL_LOG_WARN("[DoubleGauge::Record(V)] Value not recorded - invalid storage for: "
                           << instrument_descriptor_.name_);
    return;
  }
  auto context = opentelemetry::context::Context{};
  return storage_->RecordDouble(value, context);
}

void DoubleGauge::Record(double value, const opentelemetry::context::Context &context) noexcept
{
  if (!storage_)
  {
    OTEL_INTERNAL_LOG_WARN("[DoubleGauge::Record(V,C)] Value not recorded - invalid storage for: "
                           << instrument_descriptor_.name_);
    return;
  }
  return storage_->RecordDouble(value, context);
}
#endif

LongHistogram::LongHistogram(const InstrumentDescriptor &instrument_descriptor,
                             std::unique_ptr<SyncWritableMetricStorage> storage)
    : Synchronous(instrument_descriptor, std::move(storage))
{
  if (!storage_)
  {
    OTEL_INTERNAL_LOG_ERROR("[LongHistogram::LongHistogram] - Error constructing LongHistogram."
                            << "The metric storage is invalid for " << instrument_descriptor.name_);
  }
}

void LongHistogram::Record(uint64_t value,
                           const opentelemetry::common::KeyValueIterable &attributes,
                           const opentelemetry::context::Context &context) noexcept
{
  if (!storage_)
  {
    OTEL_INTERNAL_LOG_WARN(
        "[LongHistogram::Record(V,A,C)] Value not recorded - invalid storage for: "
        << instrument_descriptor_.name_);
    return;
  }
  int64_t converted = 0;
  if (ToInt64Value(value, "[LongHistogram::Record(V,A,C)]", converted))
  {
    return storage_->RecordLong(converted, attributes, context);
  }
}

void LongHistogram::Record(uint64_t value, const opentelemetry::context::Context &context) noexcept
{
  if (!storage_)
  {
    OTEL_INTERNAL_LOG_WARN("[LongHistogram::Record(V,C)] Value not recorded - invalid storage for: "
                           << instrument_descriptor_.name_);
    return;
  }
  int64_t converted = 0;
  if (ToInt64Value(value, "[LongHistogram::Record(V,C)]", converted))
  {
    return storage_->RecordLong(converted, context);
  }
}

#if OPENTELEMETRY_ABI_VERSION_NO >= 2
void LongHistogram::Record(uint64_t value,
                           const opentelemetry::common::KeyValueIterable &attributes) noexcept
{
  if (!storage_)
  {
    OTEL_INTERNAL_LOG_WARN("[LongHistogram::Record(V,A)] Value not recorded - invalid storage for: "
                           << instrument_descriptor_.name_);
    return;
  }
  auto context      = opentelemetry::context::Context{};
  int64_t converted = 0;
  if (ToInt64Value(value, "[LongHistogram::Record(V,A)]", converted))
  {
    return storage_->RecordLong(converted, attributes, context);
  }
}

void LongHistogram::Record(uint64_t value) noexcept
{
  if (!storage_)
  {
    OTEL_INTERNAL_LOG_WARN("[LongHistogram::Record(V)] Value not recorded - invalid storage for: "
                           << instrument_descriptor_.name_);
    return;
  }
  auto context      = opentelemetry::context::Context{};
  int64_t converted = 0;
  if (ToInt64Value(value, "[LongHistogram::Record(V)]", converted))
  {
    return storage_->RecordLong(converted, context);
  }
}
#endif

DoubleHistogram::DoubleHistogram(const InstrumentDescriptor &instrument_descriptor,
                                 std::unique_ptr<SyncWritableMetricStorage> storage)
    : Synchronous(instrument_descriptor, std::move(storage))
{
  if (!storage_)
  {
    OTEL_INTERNAL_LOG_ERROR(
        "[DoubleHistogram::DoubleHistogram] - Error constructing DoubleHistogram."
        << "The metric storage is invalid for " << instrument_descriptor.name_);
  }
}

void DoubleHistogram::Record(double value,
                             const opentelemetry::common::KeyValueIterable &attributes,
                             const opentelemetry::context::Context &context) noexcept
{
  if (value < 0)
  {
    OTEL_INTERNAL_LOG_WARN(
        "[DoubleHistogram::Record(V,A,C)] Value not recorded - negative value for: "
        << instrument_descriptor_.name_);
    return;
  }
  if (!storage_)
  {
    OTEL_INTERNAL_LOG_WARN(
        "[DoubleHistogram::Record(V,A,C)] Value not recorded - invalid storage for: "
        << instrument_descriptor_.name_);
    return;
  }
  return storage_->RecordDouble(value, attributes, context);
}

void DoubleHistogram::Record(double value, const opentelemetry::context::Context &context) noexcept
{
  if (value < 0)
  {
    OTEL_INTERNAL_LOG_WARN(
        "[DoubleHistogram::Record(V,C)] Value not recorded - negative value for: "
        << instrument_descriptor_.name_);
    return;
  }
  if (!storage_)
  {
    OTEL_INTERNAL_LOG_WARN(
        "[DoubleHistogram::Record(V,C)] Value not recorded - invalid storage for: "
        << instrument_descriptor_.name_);
    return;
  }
  return storage_->RecordDouble(value, context);
}

#if OPENTELEMETRY_ABI_VERSION_NO >= 2
void DoubleHistogram::Record(double value,
                             const opentelemetry::common::KeyValueIterable &attributes) noexcept
{
  if (value < 0)
  {
    OTEL_INTERNAL_LOG_WARN(
        "[DoubleHistogram::Record(V,A)] Value not recorded - negative value for: "
        << instrument_descriptor_.name_);
    return;
  }
  if (!storage_)
  {
    OTEL_INTERNAL_LOG_WARN(
        "[DoubleHistogram::Record(V,A)] Value not recorded - invalid storage for: "
        << instrument_descriptor_.name_);
    return;
  }
  auto context = opentelemetry::context::Context{};
  return storage_->RecordDouble(value, attributes, context);
}

void DoubleHistogram::Record(double value) noexcept
{
  if (value < 0)
  {
    OTEL_INTERNAL_LOG_WARN("[DoubleHistogram::Record(V)] Value not recorded - negative value for: "
                           << instrument_descriptor_.name_);
    return;
  }
  if (!storage_)
  {
    OTEL_INTERNAL_LOG_WARN("[DoubleHistogram::Record(V)] Value not recorded - invalid storage for: "
                           << instrument_descriptor_.name_);
    return;
  }
  auto context = opentelemetry::context::Context{};
  return storage_->RecordDouble(value, context);
}
#endif

#ifdef OPENTELEMETRY_HAVE_METRICS_BOUND_INSTRUMENTS_PREVIEW
namespace
{

class BoundLongCounterImpl : public opentelemetry::metrics::BoundCounter<uint64_t>
{
public:
  explicit BoundLongCounterImpl(std::shared_ptr<BoundSyncWritableMetricStorage> storage) noexcept
      : storage_(std::move(storage))
  {}
  void Add(uint64_t value) noexcept override
  {
    if (storage_)
    {
      int64_t converted = 0;
      if (ToInt64Value(value, "[BoundLongCounter::Add(V)]", converted))
      {
        storage_->RecordLong(converted);
      }
    }
  }

private:
  std::shared_ptr<BoundSyncWritableMetricStorage> storage_;
};

class BoundDoubleCounterImpl : public opentelemetry::metrics::BoundCounter<double>
{
public:
  explicit BoundDoubleCounterImpl(std::shared_ptr<BoundSyncWritableMetricStorage> storage) noexcept
      : storage_(std::move(storage))
  {}
  void Add(double value) noexcept override
  {
    if (value < 0)
    {
      OTEL_INTERNAL_LOG_WARN("[BoundDoubleCounter::Add(V)] Value not recorded - negative value");
      return;
    }
    if (storage_)
    {
      storage_->RecordDouble(value);
    }
  }

private:
  std::shared_ptr<BoundSyncWritableMetricStorage> storage_;
};

class BoundLongHistogramImpl : public opentelemetry::metrics::BoundHistogram<uint64_t>
{
public:
  explicit BoundLongHistogramImpl(std::shared_ptr<BoundSyncWritableMetricStorage> storage) noexcept
      : storage_(std::move(storage))
  {}
  void Record(uint64_t value) noexcept override
  {
    if (storage_)
    {
      int64_t converted = 0;
      if (ToInt64Value(value, "[BoundLongHistogram::Record(V)]", converted))
      {
        storage_->RecordLong(converted);
      }
    }
  }

private:
  std::shared_ptr<BoundSyncWritableMetricStorage> storage_;
};

class BoundDoubleHistogramImpl : public opentelemetry::metrics::BoundHistogram<double>
{
public:
  explicit BoundDoubleHistogramImpl(
      std::shared_ptr<BoundSyncWritableMetricStorage> storage) noexcept
      : storage_(std::move(storage))
  {}
  void Record(double value) noexcept override
  {
    if (value < 0)
    {
      OTEL_INTERNAL_LOG_WARN(
          "[BoundDoubleHistogram::Record(V)] Value not recorded - negative value");
      return;
    }
    if (storage_)
    {
      storage_->RecordDouble(value);
    }
  }

private:
  std::shared_ptr<BoundSyncWritableMetricStorage> storage_;
};

}  // namespace

opentelemetry::nostd::unique_ptr<opentelemetry::metrics::BoundCounter<uint64_t>> LongCounter::Bind(
    const opentelemetry::common::KeyValueIterable &attributes) noexcept
{
  std::shared_ptr<BoundSyncWritableMetricStorage> bound;
  if (storage_)
  {
    bound = storage_->Bind(attributes);
  }
  return opentelemetry::nostd::unique_ptr<opentelemetry::metrics::BoundCounter<uint64_t>>{
      new BoundLongCounterImpl(std::move(bound))};
}

opentelemetry::nostd::unique_ptr<opentelemetry::metrics::BoundCounter<double>> DoubleCounter::Bind(
    const opentelemetry::common::KeyValueIterable &attributes) noexcept
{
  std::shared_ptr<BoundSyncWritableMetricStorage> bound;
  if (storage_)
  {
    bound = storage_->Bind(attributes);
  }
  return opentelemetry::nostd::unique_ptr<opentelemetry::metrics::BoundCounter<double>>{
      new BoundDoubleCounterImpl(std::move(bound))};
}

opentelemetry::nostd::unique_ptr<opentelemetry::metrics::BoundHistogram<uint64_t>>
LongHistogram::Bind(const opentelemetry::common::KeyValueIterable &attributes) noexcept
{
  std::shared_ptr<BoundSyncWritableMetricStorage> bound;
  if (storage_)
  {
    bound = storage_->Bind(attributes);
  }
  return opentelemetry::nostd::unique_ptr<opentelemetry::metrics::BoundHistogram<uint64_t>>{
      new BoundLongHistogramImpl(std::move(bound))};
}

opentelemetry::nostd::unique_ptr<opentelemetry::metrics::BoundHistogram<double>>
DoubleHistogram::Bind(const opentelemetry::common::KeyValueIterable &attributes) noexcept
{
  std::shared_ptr<BoundSyncWritableMetricStorage> bound;
  if (storage_)
  {
    bound = storage_->Bind(attributes);
  }
  return opentelemetry::nostd::unique_ptr<opentelemetry::metrics::BoundHistogram<double>>{
      new BoundDoubleHistogramImpl(std::move(bound))};
}
#endif

}  // namespace metrics
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
