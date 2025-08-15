#pragma once

#include <util/generic/string.h>
#include <util/generic/hash.h>

namespace NSQLComplete {

    enum class EObjectKind {
        Folder,
        Table,
        Unknown,
    };

    enum class ENodeKind {
        Any,
        Table,
    };

    struct TTableId {
        TString Cluster;
        TString Path;

        friend bool operator<(const TTableId& lhs, const TTableId& rhs);
        friend bool operator==(const TTableId& lhs, const TTableId& rhs) = default;
    };

    template <class T>
        requires std::regular<T> &&
                 requires(T x) { {x < x} -> std::convertible_to<bool>; }
    struct TAliased: T {
        TString Alias;

        TAliased(TString alias, T value)
            : T(std::move(value))
            , Alias(std::move(alias))
        {
        }

        TAliased(T value)
            : T(std::move(value))
        {
        }

        friend bool operator<(const TAliased& lhs, const TAliased& rhs) {
            return std::tie(lhs.Alias, static_cast<const T&>(lhs)) < std::tie(rhs.Alias, static_cast<const T&>(rhs));
        }

        friend bool operator==(const TAliased& lhs, const TAliased& rhs) = default;
    };

    struct TColumnId {
        TString TableAlias;
        TString Name;

        friend bool operator<(const TColumnId& lhs, const TColumnId& rhs);
        friend bool operator==(const TColumnId& lhs, const TColumnId& rhs) = default;
    };

} // namespace NSQLComplete

template <>
struct THash<NSQLComplete::TTableId> {
    inline size_t operator()(const NSQLComplete::TTableId& x) const {
        return THash<std::tuple<TString, TString>>()(std::tie(x.Cluster, x.Path));
    }
};
