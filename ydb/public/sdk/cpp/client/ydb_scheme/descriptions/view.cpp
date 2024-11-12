#include "view.h"

#include <ydb/public/api/protos/draft/ydb_view.pb.h>

namespace NYdb::NScheme {

TViewDescription::TViewDescription(const Ydb::View::ViewDescription& desc)
    : QueryText(desc.query_text())
{
}

const TString& TViewDescription::GetQueryText() const {
    return QueryText;
}

}
