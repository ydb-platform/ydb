// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <stddef.h>
#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>

#include "opentelemetry/nostd/function_ref.h"
#include "opentelemetry/sdk/common/attribute_utils.h"
#include "opentelemetry/sdk/common/attributemap_hash.h"
#include "opentelemetry/sdk/metrics/aggregation/aggregation.h"
#include "opentelemetry/sdk/metrics/state/filtered_ordered_attribute_map.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace metrics
{

using opentelemetry::sdk::common::OrderedAttributeMap;

constexpr size_t kAggregationCardinalityLimit = 2000;
const bool kAttributesLimitOverflowValue      = true;

constexpr char kAttributesLimitOverflowKey[] = "otel.metric.overflow";

inline const MetricAttributes &GetOverflowAttributes()
{
  static const MetricAttributes value = {
      {kAttributesLimitOverflowKey,
       kAttributesLimitOverflowValue}};  // precalculated for optimization
  return value;
}

class AttributeHashGenerator
{
public:
  size_t operator()(const MetricAttributes &attributes) const
  {
    return opentelemetry::sdk::common::GetHashForAttributeMap(attributes);
  }
};

template <typename CustomHash = MetricAttributesHash>
class AttributesHashMapWithCustomHash
{
public:
  AttributesHashMapWithCustomHash(size_t attributes_limit = kAggregationCardinalityLimit)
      : attributes_limit_(attributes_limit)
  {
    if (attributes_limit_ > kAggregationCardinalityLimit)
    {
      hash_map_.reserve(attributes_limit_);
    }
  }

  Aggregation *Get(const MetricAttributes &attributes) const
  {
    auto it = hash_map_.find(attributes);
    if (it != hash_map_.end())
    {
      return it->second.get();
    }
    return nullptr;
  }

  /**
   * @return check if key is present in hash
   *
   */
  bool Has(const MetricAttributes &attributes) const
  {
    return hash_map_.find(attributes) != hash_map_.end();
  }

  /**
   * @return the pointer to value for given key if present.
   * If not present, it uses the provided callback to generate
   * value and store in the hash
   */
  Aggregation *GetOrSetDefault(
      const MetricAttributes &attributes,
      nostd::function_ref<std::unique_ptr<Aggregation>()> aggregation_callback)
  {
    auto it = hash_map_.find(attributes);
    if (it != hash_map_.end())
    {
      return it->second.get();
    }

    if (IsOverflowAttributes(attributes))
    {
      return GetOrSetOveflowAttributes(aggregation_callback);
    }

    hash_map_[attributes] = aggregation_callback();
    return hash_map_[attributes].get();
  }

  Aggregation *GetOrSetDefault(
      MetricAttributes &&attributes,
      nostd::function_ref<std::unique_ptr<Aggregation>()> aggregation_callback)
  {
    auto it = hash_map_.find(attributes);
    if (it != hash_map_.end())
    {
      return it->second.get();
    }

    if (IsOverflowAttributes(attributes))
    {
      return GetOrSetOveflowAttributes(aggregation_callback);
    }

    auto result = hash_map_.emplace(std::move(attributes), aggregation_callback());
    return result.first->second.get();
  }
  /**
   * Set the value for given key, overwriting the value if already present
   */
  void Set(const MetricAttributes &attributes, std::unique_ptr<Aggregation> aggr)
  {
    auto it = hash_map_.find(attributes);
    if (it != hash_map_.end())
    {
      it->second = std::move(aggr);
    }
    else if (IsOverflowAttributes(attributes))
    {
      hash_map_[GetOverflowAttributes()] = std::move(aggr);
    }
    else
    {
      hash_map_[attributes] = std::move(aggr);
    }
  }

  void Set(MetricAttributes &&attributes, std::unique_ptr<Aggregation> aggr)
  {
    auto it = hash_map_.find(attributes);
    if (it != hash_map_.end())
    {
      it->second = std::move(aggr);
    }
    else if (IsOverflowAttributes(attributes))
    {
      hash_map_[GetOverflowAttributes()] = std::move(aggr);
    }
    else
    {
      hash_map_[std::move(attributes)] = std::move(aggr);
    }
  }

  /**
   * Iterate the hash to yield key and value stored in hash.
   */
  bool GetAllEntries(
      nostd::function_ref<bool(const MetricAttributes &, Aggregation &)> callback) const
  {
    for (auto &kv : hash_map_)
    {
      if (!callback(kv.first, *(kv.second.get())))
      {
        return false;  // callback is not prepared to consume data
      }
    }
    return true;
  }

  /**
   * Return the size of hash.
   */
  size_t Size() { return hash_map_.size(); }

#ifdef UNIT_TESTING
  size_t BucketCount() { return hash_map_.bucket_count(); }
  size_t BucketSize(size_t n) { return hash_map_.bucket_size(n); }
#endif

private:
  std::unordered_map<MetricAttributes, std::unique_ptr<Aggregation>, CustomHash> hash_map_;
  size_t attributes_limit_;

  Aggregation *GetOrSetOveflowAttributes(
      nostd::function_ref<std::unique_ptr<Aggregation>()> aggregation_callback)
  {
    auto agg = aggregation_callback();
    return GetOrSetOveflowAttributes(std::move(agg));
  }

  Aggregation *GetOrSetOveflowAttributes(std::unique_ptr<Aggregation> agg)
  {
    auto it = hash_map_.find(GetOverflowAttributes());
    if (it != hash_map_.end())
    {
      return it->second.get();
    }

    auto result = hash_map_.emplace(GetOverflowAttributes(), std::move(agg));
    return result.first->second.get();
  }

  bool IsOverflowAttributes(const MetricAttributes &attributes) const
  {
    // If the incoming attributes are exactly the overflow sentinel, route
    // directly to the overflow entry.
    if (attributes == GetOverflowAttributes())
    {
      return true;
    }
    // The configured limit applies to distinct non-overflow attribute sets.
    // The overflow point is an additional reserved entry.
    const bool has_overflow        = (hash_map_.find(GetOverflowAttributes()) != hash_map_.end());
    const size_t non_overflow_size = hash_map_.size() - (has_overflow ? 1 : 0);
    return non_overflow_size >= attributes_limit_;
  }
};

using AttributesHashMap = AttributesHashMapWithCustomHash<>;

}  // namespace metrics

}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
