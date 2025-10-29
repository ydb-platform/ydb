#pragma once

#include <yql/essentials/sql/v1/complete/name/service/ranking/ranking.h>
#include <yql/essentials/sql/v1/complete/name/service/name_service.h>

namespace NSQLComplete {

INameService::TPtr MakeUnionNameService(
    TVector<INameService::TPtr> children,
    IRanking::TPtr ranking);

} // namespace NSQLComplete
