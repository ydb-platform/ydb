#pragma once

#include <yql/essentials/sql/v1/complete/name/object/simple/schema.h>

namespace NSQLComplete {

    ISimpleSchema::TPtr MakeStaticSimpleSchema(
        THashMap<TString, THashMap<TString, TVector<TFolderEntry>>> fs);

} // namespace NSQLComplete
