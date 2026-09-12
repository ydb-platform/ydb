#pragma once

#include <yql/essentials/utils/meta/hash.h>

#include <util/generic/string.h>

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

    TAliased(T value) // NOLINT(google-explicit-constructor)
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

TString NormalizeName(TStringBuf name);

} // namespace NSQLComplete

namespace NYql::NReflection {

YQL_DEFINE_REFLECTING(NSQLComplete::TTableId, (Cluster)(Path));

} // namespace NYql::NReflection

YQL_DERIVE_HASH(NSQLComplete::TTableId);
