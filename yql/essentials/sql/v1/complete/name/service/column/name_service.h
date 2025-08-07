#pragma once

#include <yql/essentials/sql/v1/complete/analysis/global/global.h>
#include <yql/essentials/sql/v1/complete/name/service/name_service.h>

namespace NSQLComplete {

    INameService::TPtr MakeColumnNameService(TVector<TColumnId> columns);

} // namespace NSQLComplete
