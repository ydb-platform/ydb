#pragma once

#include <yql/essentials/sql/v1/complete/name/object/simple/schema.h>
#include <yql/essentials/sql/v1/complete/name/cache/byte_size.h>

#include <util/generic/string.h>
#include <util/generic/hash.h>

namespace NSQLComplete {

    struct TSchemaListCacheKey {
        TString Zone;
        TString Cluster;
        TString Folder;

        friend bool operator==(
            const TSchemaListCacheKey& lhs,
            const TSchemaListCacheKey& rhs) = default;
    };

    template <>
    struct TByteSize<TSchemaListCacheKey> {
        size_t operator()(const TSchemaListCacheKey& x) const noexcept {
            return sizeof(x) +
                   TByteSize<TString>()(x.Zone) +
                   TByteSize<TString>()(x.Cluster) +
                   TByteSize<TString>()(x.Folder);
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

} // namespace NSQLComplete

template <>
struct THash<NSQLComplete::TSchemaListCacheKey> {
    inline size_t operator()(const NSQLComplete::TSchemaListCacheKey& key) const {
        return THash<std::tuple<TString, TString, TString>>()(
            std::tie(key.Zone, key.Cluster, key.Folder));
    }
};
