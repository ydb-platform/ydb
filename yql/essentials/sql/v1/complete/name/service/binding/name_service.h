#pragma once

#pragma once

#include <yql/essentials/sql/v1/complete/name/service/name_service.h>
#include <yql/essentials/sql/v1/complete/name/service/ranking/ranking.h>

namespace NSQLComplete {

    INameService::TPtr MakeBindingNameService(TVector<TString> names);

} // namespace NSQLComplete
