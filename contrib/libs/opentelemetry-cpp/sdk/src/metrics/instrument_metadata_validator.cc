// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#include <cstddef>

#include "opentelemetry/common/detail/ascii.h"
#include "opentelemetry/nostd/string_view.h"
#include "opentelemetry/sdk/metrics/instrument_metadata_validator.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace metrics
{
namespace
{
// instrument-name = ALPHA 0*254 ("_" / "." / "-" / "/" / ALPHA / DIGIT)
constexpr std::size_t kMaxNameSize = 255;
// instrument-unit = 0*63 ASCII characters, excluding NUL
constexpr std::size_t kMaxUnitSize = 63;

bool IsNameChar(char c) noexcept
{
  return opentelemetry::common::detail::IsAsciiAlnum(c) || c == '_' || c == '.' || c == '-' ||
         c == '/';
}

bool IsUnitChar(unsigned char c) noexcept
{
  return c >= 0x01 && c <= 0x7F;
}
}  // namespace

InstrumentMetaDataValidator::InstrumentMetaDataValidator() = default;

bool InstrumentMetaDataValidator::ValidateName(nostd::string_view name) const
{
  if (name.empty() || name.size() > kMaxNameSize ||
      !opentelemetry::common::detail::IsAsciiAlpha(name[0]))
  {
    return false;
  }
  for (std::size_t i = 1; i < name.size(); ++i)
  {
    if (!IsNameChar(name[i]))
    {
      return false;
    }
  }
  return true;
}

bool InstrumentMetaDataValidator::ValidateUnit(nostd::string_view unit) const
{
  if (unit.size() > kMaxUnitSize)
  {
    return false;
  }
  for (const char c : unit)
  {
    if (!IsUnitChar(static_cast<unsigned char>(c)))
    {
      return false;
    }
  }
  return true;
}

bool InstrumentMetaDataValidator::ValidateDescription(nostd::string_view /*description*/) const
{
  return true;
}

}  // namespace metrics
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
