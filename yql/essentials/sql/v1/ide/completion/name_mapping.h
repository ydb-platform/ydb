#pragma once

#include "sql_complete.h"

#include <yql/essentials/sql/v1/ide/completion/analysis/local/local.h>
#include <yql/essentials/sql/v1/ide/completion/name/service/name_service.h>

namespace NSQLComplete {

TCandidate ToCandidate(TGenericName generic, TLocalSyntaxContext& local);

TVector<TCandidate> ToCandidate(TVector<TGenericName> names, TLocalSyntaxContext local);

} // namespace NSQLComplete
