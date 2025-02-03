#pragma once

#include <ydb/public/sdk/cpp/client/impl/ydb_internal/common/type_switcher.h>

namespace NYdb::inline V2 {

TStringType GetStrFromEnv(const char* envVarName, const TStringType& defaultValue = "");

} // namespace NYdb

