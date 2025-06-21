#pragma once

#include <util/generic/string.h>
#include <util/generic/hash.h>

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

template <>
struct THash<NSQLComplete::TTableId> {
    inline size_t operator()(const NSQLComplete::TTableId& x) const {
        return THash<std::tuple<TString, TString>>()(std::tie(x.Cluster, x.Path));
    }
};
