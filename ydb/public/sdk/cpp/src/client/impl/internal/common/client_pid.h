#pragma once

#include <ydb/public/sdk/cpp/src/client/impl/internal/internal_header.h>

#include <string>

namespace NYdb::inline Dev {

std::string GetClientPIDHeaderValue();

} // namespace NYdb
