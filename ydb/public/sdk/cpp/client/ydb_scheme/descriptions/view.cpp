#include "view.h"

#include <ydb/public/api/protos/ydb_scheme.pb.h>

namespace NYdb::NScheme {

TViewDescription::TViewDescription(const Ydb::Scheme::ViewDescription& desc)
    : QueryText(desc.query_text())
{
}

const TString& TViewDescription::GetQueryText() const {
    return QueryText;
}

}
