#pragma once

#include "ranking.h"

#include <yql/essentials/sql/v1/complete/name/name_service.h>

namespace NSQLComplete {

    struct NameSet {
        TVector<TString> Types;
        TVector<TString> Functions;
    };

    NameSet MakeDefaultNameSet();

    INameService::TPtr MakeStaticNameService();

    INameService::TPtr MakeStaticNameService(NameSet names, IRanking::TPtr ranking);

} // namespace NSQLComplete
