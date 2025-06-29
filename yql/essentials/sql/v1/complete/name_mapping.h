#pragma once

#include "sql_complete.h"

#include <yql/essentials/sql/v1/complete/analysis/local/local.h>
#include <yql/essentials/sql/v1/complete/name/service/name_service.h>

namespace NSQLComplete {

    TCandidate ToCandidate(TGenericName name, TLocalSyntaxContext& context);

    TVector<TCandidate> ToCandidate(TVector<TGenericName> names, TLocalSyntaxContext context);

} // namespace NSQLComplete
