#pragma once

#include <yql/essentials/sql/v1/complete/name/name_service.h>

namespace NSQLComplete {

    struct NameSet {
        TVector<TString> Types;
    };

    NameSet MakeDefaultNameSet();

    INameService::TPtr MakeStaticNameService(NameSet names);

} // namespace NSQLComplete
