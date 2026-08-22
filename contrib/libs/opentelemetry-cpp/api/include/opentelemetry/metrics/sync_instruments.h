// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include "opentelemetry/common/attribute_value.h"
#include "opentelemetry/common/key_value_iterable_view.h"
#include "opentelemetry/context/context.h"
#include "opentelemetry/nostd/span.h"
#include "opentelemetry/nostd/string_view.h"
#include "opentelemetry/nostd/type_traits.h"
#include "opentelemetry/version.h"

#ifdef OPENTELEMETRY_HAVE_METRICS_BOUND_INSTRUMENTS_PREVIEW
#  include "opentelemetry/nostd/unique_ptr.h"
#endif

OPENTELEMETRY_BEGIN_NAMESPACE
namespace metrics
{

class SynchronousInstrument
{
public:
  SynchronousInstrument()                                             = default;
  SynchronousInstrument(const SynchronousInstrument &)                = default;
  SynchronousInstrument(SynchronousInstrument &&) noexcept            = default;
  SynchronousInstrument &operator=(const SynchronousInstrument &)     = default;
  SynchronousInstrument &operator=(SynchronousInstrument &&) noexcept = default;
  virtual ~SynchronousInstrument()                                    = default;
};

#ifdef OPENTELEMETRY_HAVE_METRICS_BOUND_INSTRUMENTS_PREVIEW
// Bound synchronous instrument support intentionally covers Counter and
// Histogram only. UpDownCounter, Gauge, exemplar parity, and context-bearing
// bound operations are follow-ups. This API is experimental and is gated
// behind both ABI v2 and ENABLE_METRICS_BOUND_INSTRUMENTS_PREVIEW
// (see OPENTELEMETRY_HAVE_METRICS_BOUND_INSTRUMENTS_PREVIEW in version.h).
/**
 * @since ABI_VERSION 2
 * A bound counter handle obtained via Counter<T>::Bind(...). The associated
 * attribute set is captured at Bind time so the hot path avoids per-call
 * attribute processing and hashmap lookup. Calls made after the originating
 * Counter or SDK storage is destroyed or shut down may be dropped and are not
 * guaranteed to be exported.
 */
template <class T>
class BoundCounter
{
public:
  BoundCounter()                                    = default;
  BoundCounter(const BoundCounter &)                = delete;
  BoundCounter(BoundCounter &&) noexcept            = delete;
  BoundCounter &operator=(const BoundCounter &)     = delete;
  BoundCounter &operator=(BoundCounter &&) noexcept = delete;
  virtual ~BoundCounter()                           = default;

  /**
   * Record a value against the bound attribute set.
   *
   * @param value The increment amount. MUST be non-negative.
   */
  virtual void Add(T value) noexcept = 0;
};

/**
 * @since ABI_VERSION 2
 * A bound histogram handle obtained via Histogram<T>::Bind(...).
 */
template <class T>
class BoundHistogram
{
public:
  BoundHistogram()                                      = default;
  BoundHistogram(const BoundHistogram &)                = delete;
  BoundHistogram(BoundHistogram &&) noexcept            = delete;
  BoundHistogram &operator=(const BoundHistogram &)     = delete;
  BoundHistogram &operator=(BoundHistogram &&) noexcept = delete;
  virtual ~BoundHistogram()                             = default;

  /**
   * Record a value against the bound attribute set.
   *
   * @param value The measurement value. MUST be non-negative.
   */
  virtual void Record(T value) noexcept = 0;
};
#endif

/* A Counter instrument that adds values. */
template <class T>
class Counter : public SynchronousInstrument
{

public:
  /**
   * Record a value
   *
   * @param value The increment amount. MUST be non-negative.
   */
  virtual void Add(T value) noexcept = 0;

  /**
   * Record a value
   *
   * @param value The increment amount. MUST be non-negative.
   * @param context The explicit context to associate with this measurement.
   */
  virtual void Add(T value, const context::Context &context) noexcept = 0;

  /**
   * Record a value with a set of attributes.
   *
   * @param value The increment amount. MUST be non-negative.
   * @param attributes A set of attributes to associate with the value.
   */

  virtual void Add(T value, const common::KeyValueIterable &attributes) noexcept = 0;

  /**
   * Record a value with a set of attributes.
   *
   * @param value The increment amount. MUST be non-negative.
   * @param attributes A set of attributes to associate with the value.
   * @param context The explicit context to associate with this measurement.
   */
  virtual void Add(T value,
                   const common::KeyValueIterable &attributes,
                   const context::Context &context) noexcept = 0;

  template <class U,
            nostd::enable_if_t<common::detail::is_key_value_iterable<U>::value> * = nullptr>
  void Add(T value, const U &attributes) noexcept
  {
    this->Add(value, common::KeyValueIterableView<U>{attributes});
  }

  template <class U,
            nostd::enable_if_t<common::detail::is_key_value_iterable<U>::value> * = nullptr>
  void Add(T value, const U &attributes, const context::Context &context) noexcept
  {
    this->Add(value, common::KeyValueIterableView<U>{attributes}, context);
  }

  void Add(T value,
           std::initializer_list<std::pair<nostd::string_view, common::AttributeValue>>
               attributes) noexcept
  {
    this->Add(value, nostd::span<const std::pair<nostd::string_view, common::AttributeValue>>{
                         attributes.begin(), attributes.end()});
  }

  void Add(T value,
           std::initializer_list<std::pair<nostd::string_view, common::AttributeValue>> attributes,
           const context::Context &context) noexcept
  {
    this->Add(value,
              nostd::span<const std::pair<nostd::string_view, common::AttributeValue>>{
                  attributes.begin(), attributes.end()},
              context);
  }

#ifdef OPENTELEMETRY_HAVE_METRICS_BOUND_INSTRUMENTS_PREVIEW
  /**
   * @since ABI_VERSION 2
   * Returns a bound counter handle for the given attribute set. Repeated calls
   * to BoundCounter<T>::Add(value) avoid per-call attribute processing and
   * hashmap lookup. Calls made after the Counter or SDK storage is destroyed or
   * shut down may be dropped and are not guaranteed to be exported.
   */
  virtual nostd::unique_ptr<BoundCounter<T>> Bind(
      const common::KeyValueIterable &attributes) noexcept = 0;

  template <class U,
            nostd::enable_if_t<common::detail::is_key_value_iterable<U>::value> * = nullptr>
  nostd::unique_ptr<BoundCounter<T>> Bind(const U &attributes) noexcept
  {
    return this->Bind(common::KeyValueIterableView<U>{attributes});
  }

  nostd::unique_ptr<BoundCounter<T>> Bind(
      std::initializer_list<std::pair<nostd::string_view, common::AttributeValue>>
          attributes) noexcept
  {
    return this->Bind(nostd::span<const std::pair<nostd::string_view, common::AttributeValue>>{
        attributes.begin(), attributes.end()});
  }
#endif
};

/** A histogram instrument that records values. */

template <class T>
class Histogram : public SynchronousInstrument
{
public:
#if OPENTELEMETRY_ABI_VERSION_NO >= 2
  /**
   * @since ABI_VERSION 2
   * Records a value.
   *
   * @param value The measurement value. MUST be non-negative.
   */
  virtual void Record(T value) noexcept = 0;

  /**
   * @since ABI_VERSION 2
   * Records a value with a set of attributes.
   *
   * @param value The measurement value. MUST be non-negative.
   * @param attribute A set of attributes to associate with the value.
   */
  virtual void Record(T value, const common::KeyValueIterable &attribute) noexcept = 0;

  template <class U,
            nostd::enable_if_t<common::detail::is_key_value_iterable<U>::value> * = nullptr>
  void Record(T value, const U &attributes) noexcept
  {
    this->Record(value, common::KeyValueIterableView<U>{attributes});
  }

  void Record(T value,
              std::initializer_list<std::pair<nostd::string_view, common::AttributeValue>>
                  attributes) noexcept
  {
    this->Record(value, nostd::span<const std::pair<nostd::string_view, common::AttributeValue>>{
                            attributes.begin(), attributes.end()});
  }
#endif

#ifdef OPENTELEMETRY_HAVE_METRICS_BOUND_INSTRUMENTS_PREVIEW
  /**
   * @since ABI_VERSION 2
   * Returns a bound histogram handle for the given attribute set. Repeated
   * calls to BoundHistogram<T>::Record(value) avoid per-call attribute
   * processing and hashmap lookup. Calls made after the Histogram or SDK
   * storage is destroyed or shut down may be dropped and are not guaranteed to
   * be exported.
   */
  virtual nostd::unique_ptr<BoundHistogram<T>> Bind(
      const common::KeyValueIterable &attributes) noexcept = 0;

  template <class U,
            nostd::enable_if_t<common::detail::is_key_value_iterable<U>::value> * = nullptr>
  nostd::unique_ptr<BoundHistogram<T>> Bind(const U &attributes) noexcept
  {
    return this->Bind(common::KeyValueIterableView<U>{attributes});
  }

  nostd::unique_ptr<BoundHistogram<T>> Bind(
      std::initializer_list<std::pair<nostd::string_view, common::AttributeValue>>
          attributes) noexcept
  {
    return this->Bind(nostd::span<const std::pair<nostd::string_view, common::AttributeValue>>{
        attributes.begin(), attributes.end()});
  }
#endif

  /**
   * Records a value.
   *
   * @param value The measurement value. MUST be non-negative.
   * @param context The explicit context to associate with this measurement.
   */
  virtual void Record(T value, const context::Context &context) noexcept = 0;

  /**
   * Records a value with a set of attributes.
   *
   * @param value The measurement value. MUST be non-negative.
   * @param attributes A set of attributes to associate with the value..
   * @param context The explicit context to associate with this measurement.
   */
  virtual void Record(T value,
                      const common::KeyValueIterable &attributes,
                      const context::Context &context) noexcept = 0;

  template <class U,
            nostd::enable_if_t<common::detail::is_key_value_iterable<U>::value> * = nullptr>
  void Record(T value, const U &attributes, const context::Context &context) noexcept
  {
    this->Record(value, common::KeyValueIterableView<U>{attributes}, context);
  }

  void Record(
      T value,
      std::initializer_list<std::pair<nostd::string_view, common::AttributeValue>> attributes,
      const context::Context &context) noexcept
  {
    this->Record(value,
                 nostd::span<const std::pair<nostd::string_view, common::AttributeValue>>{
                     attributes.begin(), attributes.end()},
                 context);
  }
};

/** An up-down-counter instrument that adds or reduce values. */

template <class T>
class UpDownCounter : public SynchronousInstrument
{
public:
  /**
   * Record a value.
   *
   * @param value The increment amount. May be positive, negative or zero.
   */
  virtual void Add(T value) noexcept = 0;

  /**
   * Record a value.
   *
   * @param value The increment amount. May be positive, negative or zero.
   * @param context The explicit context to associate with this measurement.
   */
  virtual void Add(T value, const context::Context &context) noexcept = 0;

  /**
   * Record a value with a set of attributes.
   *
   * @param value The increment amount. May be positive, negative or zero.
   * @param attributes A set of attributes to associate with the count.
   */
  virtual void Add(T value, const common::KeyValueIterable &attributes) noexcept = 0;

  /**
   * Record a value with a set of attributes.
   *
   * @param value The increment amount. May be positive, negative or zero.
   * @param attributes A set of attributes to associate with the count.
   * @param context The explicit context to associate with this measurement.
   */
  virtual void Add(T value,
                   const common::KeyValueIterable &attributes,
                   const context::Context &context) noexcept = 0;

  template <class U,
            nostd::enable_if_t<common::detail::is_key_value_iterable<U>::value> * = nullptr>
  void Add(T value, const U &attributes) noexcept
  {
    this->Add(value, common::KeyValueIterableView<U>{attributes});
  }

  template <class U,
            nostd::enable_if_t<common::detail::is_key_value_iterable<U>::value> * = nullptr>
  void Add(T value, const U &attributes, const context::Context &context) noexcept
  {
    this->Add(value, common::KeyValueIterableView<U>{attributes}, context);
  }

  void Add(T value,
           std::initializer_list<std::pair<nostd::string_view, common::AttributeValue>>
               attributes) noexcept
  {
    this->Add(value, nostd::span<const std::pair<nostd::string_view, common::AttributeValue>>{
                         attributes.begin(), attributes.end()});
  }

  void Add(T value,
           std::initializer_list<std::pair<nostd::string_view, common::AttributeValue>> attributes,
           const context::Context &context) noexcept
  {
    this->Add(value,
              nostd::span<const std::pair<nostd::string_view, common::AttributeValue>>{
                  attributes.begin(), attributes.end()},
              context);
  }
};

#if OPENTELEMETRY_ABI_VERSION_NO >= 2
/* A Gauge instrument that records values. */
template <class T>
class Gauge : public SynchronousInstrument
{

public:
  /**
   * Record a value
   *
   * @param value The measurement value. May be positive, negative or zero.
   */
  virtual void Record(T value) noexcept = 0;

  /**
   * Record a value
   *
   * @param value The measurement value. May be positive, negative or zero.
   * @param context The explicit context to associate with this measurement.
   */
  virtual void Record(T value, const context::Context &context) noexcept = 0;

  /**
   * Record a value with a set of attributes.
   *
   * @param value The measurement value. May be positive, negative or zero.
   * @param attributes A set of attributes to associate with the value.
   */

  virtual void Record(T value, const common::KeyValueIterable &attributes) noexcept = 0;

  /**
   * Record a value with a set of attributes.
   *
   * @param value The measurement value. May be positive, negative or zero.
   * @param attributes A set of attributes to associate with the value.
   * @param context The explicit context to associate with this measurement.
   */
  virtual void Record(T value,
                      const common::KeyValueIterable &attributes,
                      const context::Context &context) noexcept = 0;

  template <class U,
            nostd::enable_if_t<common::detail::is_key_value_iterable<U>::value> * = nullptr>
  void Record(T value, const U &attributes) noexcept
  {
    this->Record(value, common::KeyValueIterableView<U>{attributes});
  }

  template <class U,
            nostd::enable_if_t<common::detail::is_key_value_iterable<U>::value> * = nullptr>
  void Record(T value, const U &attributes, const context::Context &context) noexcept
  {
    this->Record(value, common::KeyValueIterableView<U>{attributes}, context);
  }

  void Record(T value,
              std::initializer_list<std::pair<nostd::string_view, common::AttributeValue>>
                  attributes) noexcept
  {
    this->Record(value, nostd::span<const std::pair<nostd::string_view, common::AttributeValue>>{
                            attributes.begin(), attributes.end()});
  }

  void Record(
      T value,
      std::initializer_list<std::pair<nostd::string_view, common::AttributeValue>> attributes,
      const context::Context &context) noexcept
  {
    this->Record(value,
                 nostd::span<const std::pair<nostd::string_view, common::AttributeValue>>{
                     attributes.begin(), attributes.end()},
                 context);
  }
};
#endif

}  // namespace metrics
OPENTELEMETRY_END_NAMESPACE
