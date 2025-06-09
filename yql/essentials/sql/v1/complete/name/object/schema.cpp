#include "schema.h"

template <>
void Out<NSQLComplete::TFolderEntry>(IOutputStream& out, const NSQLComplete::TFolderEntry& value) {
    out << "{" << value.Type << ", " << value.Name << "}";
}
