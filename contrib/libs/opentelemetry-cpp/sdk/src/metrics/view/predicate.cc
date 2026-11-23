// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#include "opentelemetry/sdk/metrics/view/predicate.h"

#include <string>

#if OPENTELEMETRY_HAVE_WORKING_REGEX
#  include <regex>
#endif

#if !OPENTELEMETRY_HAVE_WORKING_REGEX
#  include "opentelemetry/sdk/common/global_log_handler.h"
#endif

#include "src/common/wildcard_match.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace metrics
{

class PatternPredicate::Impl
{
public:
  explicit Impl(opentelemetry::nostd::string_view pattern)
#if OPENTELEMETRY_HAVE_WORKING_REGEX
      : reg_key_{std::string{pattern.data(), pattern.size()}}
#else
      : reg_key_{pattern.data(), pattern.size()}
#endif
  {}

#if OPENTELEMETRY_HAVE_WORKING_REGEX
  std::regex reg_key_;
#else
  std::string reg_key_;
#endif
};

PatternPredicate::PatternPredicate(opentelemetry::nostd::string_view pattern)
    : impl_{new Impl(pattern)}
{}

PatternPredicate::~PatternPredicate() = default;

bool PatternPredicate::Match(opentelemetry::nostd::string_view str) const noexcept
{
#if OPENTELEMETRY_HAVE_WORKING_REGEX
  return std::regex_match(str.data(), str.data() + str.size(), impl_->reg_key_);
#else
  OTEL_INTERNAL_LOG_ERROR(
      "PatternPredicate::Match - failed. std::regex not fully supported for this compiler.");
  return false;
#endif
}

WildcardPredicate::WildcardPredicate(opentelemetry::nostd::string_view pattern)
    : pattern_{pattern.data(), pattern.size()}
{}

bool WildcardPredicate::Match(opentelemetry::nostd::string_view str) const noexcept
{
  return opentelemetry::sdk::common::WildcardMatch(pattern_, str);
}

ExactPredicate::ExactPredicate(opentelemetry::nostd::string_view pattern)
    : pattern_{pattern.data(), pattern.size()}
{}

bool ExactPredicate::Match(opentelemetry::nostd::string_view str) const noexcept
{
  return pattern_ == str;
}

bool MatchEverythingPattern::Match(opentelemetry::nostd::string_view /* str */) const noexcept
{
  return true;
}

bool MatchNothingPattern::Match(opentelemetry::nostd::string_view /* str */) const noexcept
{
  return false;
}

}  // namespace metrics
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
