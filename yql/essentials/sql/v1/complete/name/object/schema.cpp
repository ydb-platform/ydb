#include "schema.h"

namespace NSQLComplete {

    THashSet<TString> TFolderEntry::KnownTypes = {
        TFolderEntry::Folder,
        TFolderEntry::Table,
    };

} // namespace NSQLComplete

template <>
void Out<NSQLComplete::TFolderEntry>(IOutputStream& out, const NSQLComplete::TFolderEntry& value) {
    out << "{" << value.Type << ", " << value.Name << "}";
}
