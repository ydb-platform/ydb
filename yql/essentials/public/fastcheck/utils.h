#pragma once

#include "linter.h"

#include <library/cpp/json/json_reader.h>

namespace NYql {
namespace NFastCheck {

TUdfFilter ParseUdfFilter(const NJson::TJsonValue& json);

} // namespace NFastCheck
} // namespace NYql
