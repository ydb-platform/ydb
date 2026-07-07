#pragma once

#include <yql/essentials/sql/v1/ide/completion/name/object/simple/schema.h>

#include <util/generic/hash.h>

namespace NSQLComplete {

struct TSchemaData {
    THashMap<TString, THashMap<TString, TVector<TFolderEntry>>> Folders;
    THashMap<TString, THashMap<TString, TTableDetails>> Tables;
};

ISimpleSchema::TPtr MakeStaticSimpleSchema(TSchemaData data);

} // namespace NSQLComplete
