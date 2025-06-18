#pragma once

#include <util/generic/string.h>

namespace NSQLComplete {

    enum class EObjectKind {
        Folder,
        Table,
    };

    struct TTableId {
        TString Cluster;
        TString Path;

        friend bool operator==(const TTableId& lhs, const TTableId& rhs) = default;
    };

} // namespace NSQLComplete
