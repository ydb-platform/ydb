#include <yql/essentials/public/issue/yql_issue.h>

namespace NYdb::NDump {

bool RewriteCreateQuery(TString& query, std::string_view pattern, const std::string& dbPath, NYql::TIssues& issues);

}
