#include "schema.h"

#include <yql/essentials/utils/meta/out.h>

namespace NSQLComplete {

THashSet<TString> TFolderEntry::KnownTypes = {
    TFolderEntry::Folder,
    TFolderEntry::Table,
};

} // namespace NSQLComplete

YQL_DERIVE_OUT_SPEC(NSQLComplete::TFolderEntry);
