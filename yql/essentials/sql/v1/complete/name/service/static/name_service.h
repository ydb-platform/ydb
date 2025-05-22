#pragma once

#include "name_set.h"

#include <yql/essentials/sql/v1/complete/name/service/ranking/frequency.h>
#include <yql/essentials/sql/v1/complete/name/service/ranking/ranking.h>
#include <yql/essentials/sql/v1/complete/name/service/name_service.h>

namespace NSQLComplete {

    INameService::TPtr MakeStaticNameService(TNameSet names, TFrequencyData frequency);

    // TODO(YQL-19747): Migrate YDB CLI to method from pure TNameSet and TFrequencyData
    INameService::TPtr MakeStaticNameService(TNameSet names, IRanking::TPtr ranking);

} // namespace NSQLComplete
