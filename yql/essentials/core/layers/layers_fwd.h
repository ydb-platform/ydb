#pragma once

#include <yql/essentials/utils/yql_panic.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/generic/maybe.h>
#include <util/generic/hash.h>

namespace NYql::NLayers {
struct TLocation {
    TString System;
    TString Cluster;
    TString Path;
    TMaybe<TString> Format;
};

using TLocations = TVector<TLocation>;

class TKey {
public:
    TKey(TMaybe<TString>&& name, TMaybe<TString>&& url = {})
        : Name(name)
        , Url(url)
    {
        YQL_ENSURE(Name || Url);
    }
    TKey(const TKey&) = default;
    TKey(TKey&&) = default;
    TKey& operator=(const TKey& rhs) = default;
    TKey& operator=(TKey&&) = default;
    bool operator==(const TKey& rhs) const {
        return Name == rhs.Name && Url == rhs.Url;
    }
    TString ToString() const {
        TStringBuilder sb;
        sb << "(";
        if (Name) {
            sb << "Name=" << Name->Quote();
        }
        if (Url) {
            if (Name) {
                sb << ", ";
            }
            sb << "Url=" << Url->Quote();
        }
        sb << ")";
        return sb;
    }
    TMaybe<TString> Name;
    TMaybe<TString> Url;
};

struct TLayerInfo {
    TLocations Locations;
    TString Url;
    TMaybe<TKey> Parent;
};
}

template <>
struct THash<NYql::NLayers::TKey> {
    size_t operator()(const NYql::NLayers::TKey& v) const noexcept {
        return CombineHashes(THash<decltype(v.Name)>()(v.Name), THash<decltype(v.Url)>()(v.Url));
    }
};
