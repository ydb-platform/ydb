#include "create_view_formatter.h"

#include <ydb/public/lib/ydb_cli/dump/util/view_utils.h>

namespace NKikimr::NSysView {

TFormatResult TCreateViewFormatter::Format(const TString& viewPath, const NKikimrSchemeOp::TViewDescription& viewDesc) {
    const auto [contextRecreation, select] = NYdb::NDump::SplitViewQuery(viewDesc.GetQueryText());

    const TString creationQuery = std::format(
        "{}"
        "CREATE VIEW `{}` WITH (security_invoker = TRUE) AS\n"
        "{};\n",
        contextRecreation.c_str(),
        viewPath.c_str(),
        select.c_str()
    );

    return TFormatResult(creationQuery);
}

}
