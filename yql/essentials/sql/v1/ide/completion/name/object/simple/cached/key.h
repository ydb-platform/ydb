#pragma once

#include <yql/essentials/sql/v1/ide/completion/name/object/simple/schema.h>
#include <yql/essentials/sql/v1/ide/completion/name/cache/byte_size.h>
#include <yql/essentials/utils/meta/hash.h>

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

namespace NYql::NReflection {

YQL_DEFINE_REFLECTING(NSQLComplete::TSchemaDescribeCacheKey, (Zone)(Cluster)(Path));

} // namespace NYql::NReflection

YQL_DERIVE_HASH(NSQLComplete::TSchemaDescribeCacheKey);
