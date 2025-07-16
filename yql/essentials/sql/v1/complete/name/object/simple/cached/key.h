#pragma once

#include <yql/essentials/sql/v1/complete/name/object/simple/schema.h>
#include <yql/essentials/sql/v1/complete/name/cache/byte_size.h>

#include <util/generic/string.h>
#include <util/generic/hash.h>

namespace NSQLComplete {

    struct TSchemaDescribeCacheKey {
        TString Zone;
        TString Cluster;
        TString Path;

        friend bool operator==(
            const TSchemaDescribeCacheKey& lhs,
            const TSchemaDescribeCacheKey& rhs) = default;
    };

    template <>
    struct TByteSize<TSchemaDescribeCacheKey> {
        size_t operator()(const TSchemaDescribeCacheKey& x) const noexcept {
            return sizeof(x) +
                   TByteSize<TString>()(x.Zone) +
                   TByteSize<TString>()(x.Cluster) +
                   TByteSize<TString>()(x.Path);
        }
    };

    template <>
    struct TByteSize<TFolderEntry> {
        size_t operator()(const TFolderEntry& x) const noexcept {
            return sizeof(x) +
                   TByteSize<TString>()(x.Type) +
                   TByteSize<TString>()(x.Name);
        }
    };

    template <>
    struct TByteSize<TTableDetails> {
        size_t operator()(const TTableDetails& x) const noexcept {
            return TByteSize<TVector<TString>>()(x.Columns);
        }
    };

} // namespace NSQLComplete

template <>
struct THash<NSQLComplete::TSchemaDescribeCacheKey> {
    inline size_t operator()(const NSQLComplete::TSchemaDescribeCacheKey& key) const {
        return THash<std::tuple<TString, TString, TString>>()(
            std::tie(key.Zone, key.Cluster, key.Path));
    }
};
