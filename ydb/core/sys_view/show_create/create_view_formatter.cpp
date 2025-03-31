#include "create_view_formatter.h"

#include <ydb/public/lib/ydb_cli/dump/util/view_utils.h>
#include <yql/essentials/public/issue/yql_issue.h>

namespace NKikimr::NSysView {

TFormatResult TCreateViewFormatter::Format(const TString& viewPath, const NKikimrSchemeOp::TViewDescription& viewDesc) {
    NYdb::NDump::TViewQuerySplit split;
    NYql::TIssues issues;
    if (!NYdb::NDump::SplitViewQuery(viewDesc.GetQueryText(), split, issues)) {
        return TFormatResult(Ydb::StatusIds::SCHEME_ERROR, issues.ToString());
    }

    const TString creationQuery = std::format(
        "{}"
        "CREATE VIEW `{}` WITH (security_invoker = TRUE) AS\n"
        "{};\n",
        split.ContextRecreation.c_str(),
        viewPath.c_str(),
        split.Select.c_str()
    );

    return TFormatResult(creationQuery);
}

}
