#include <ydb-cpp-sdk/library/yql/public/issue/yql_issue_id.h>

#include <ydb-cpp-sdk/library/string_utils/helpers/helpers.h>

namespace NYql {

std::string SeverityToString(ESeverity severity) {
    auto ret = NYql::TSeverityIds::ESeverityId_Name(severity);
    return ret.empty() ? "Unknown" : NUtils::ToTitle(ret.substr(2)); //remove prefix "S_"
}

};
