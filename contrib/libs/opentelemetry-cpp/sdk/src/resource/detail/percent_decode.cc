// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#include "percent_decode.h"

#include <cctype>
#include <cstddef>

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace resource
{
namespace detail
{
namespace
{

unsigned char HexDigitValue(unsigned char c) noexcept
{
  return static_cast<unsigned char>(std::isdigit(c) ? c - '0' : std::toupper(c) - 'A' + 10);
}

}  // namespace

std::string PercentDecode(const std::string &value)
{
  std::string decoded;
  decoded.reserve(value.size());

  for (std::size_t i = 0; i < value.size(); ++i)
  {
    if (value[i] == '%' && i + 2 < value.size())
    {
      unsigned char high = static_cast<unsigned char>(value[i + 1]);
      unsigned char low  = static_cast<unsigned char>(value[i + 2]);

      if (std::isxdigit(high) && std::isxdigit(low))
      {
        unsigned char unescaped_value =
            static_cast<unsigned char>((HexDigitValue(high) << 4) | HexDigitValue(low));

        decoded.push_back(static_cast<char>(unescaped_value));
        i += 2;
        continue;
      }
    }

    decoded.push_back(value[i]);
  }

  return decoded;
}

}  // namespace detail
}  // namespace resource
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
