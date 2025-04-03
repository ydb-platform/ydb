#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/value/value.h>

#include <map>
#include <optional>

namespace NYdb {
namespace NConsoleClient {

class TYqlParamParser {
public:
    static std::optional<std::map<std::string, TType>> GetParamTypes(const TString& queryText);
};

} // namespace NConsoleClient
} // namespace NYdb
