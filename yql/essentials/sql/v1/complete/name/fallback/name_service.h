#pragma once

#include <yql/essentials/sql/v1/complete/name/name_service.h>

namespace NSQLComplete {

    INameService::TPtr MakeDeadlinedNameService(
        INameService::TPtr origin, TDuration timeout);

    INameService::TPtr MakeFallbackNameService(
        INameService::TPtr primary, INameService::TPtr standby);

} // namespace NSQLComplete
