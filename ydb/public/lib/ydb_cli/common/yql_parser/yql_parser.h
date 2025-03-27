#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/value/value.h>

#include <map>

namespace NYdb {
namespace NConsoleClient {

class TYqlParser {
public:
    static std::optional<std::map<std::string, TType>> GetParamTypes(const TString& queryText);

private:
    static bool ProcessType(const TString& typeStr, TTypeBuilder& builder);

    static TString ToLower(const TString& s);
};

} // namespace NConsoleClient
} // namespace NYdb
