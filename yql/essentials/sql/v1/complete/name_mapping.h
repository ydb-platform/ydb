#pragma once

#include "sql_complete.h"

#include <yql/essentials/sql/v1/complete/analysis/local/local.h>
#include <yql/essentials/sql/v1/complete/name/service/name_service.h>

namespace NSQLComplete {

    TCandidate ToCandidate(TGenericName name, TLocalSyntaxContext& local);

    TVector<TCandidate> ToCandidate(TVector<TGenericName> names, TLocalSyntaxContext local);

} // namespace NSQLComplete
