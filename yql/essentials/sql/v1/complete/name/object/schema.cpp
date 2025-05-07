#include "schema.h"

template <>
void Out<NSQLComplete::TFolderEntry>(IOutputStream& out, const NSQLComplete::TFolderEntry& entry) {
    out << "{" << entry.Type << ", " << entry.Name << "}";
}
