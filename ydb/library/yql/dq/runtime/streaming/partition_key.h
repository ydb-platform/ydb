#pragma once

#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/stream/input.h>
#include <util/stream/output.h>
#include <util/string/split.h>

namespace NYql::NDq {

struct TPartitionKey {
    TString Cluster;
    ui64 PartitionId;

    bool operator==(const TPartitionKey&) const = default;
};

} // namespace NYql::NDq

template<>
struct THash<NYql::NDq::TPartitionKey> {
    ui64 operator() (const NYql::NDq::TPartitionKey& pk) const {
        return CombineHashes<ui64>(
            std::hash<TString>{} (pk.Cluster),
            std::hash<ui64>{} (pk.PartitionId)
        );
    }
};

template <>
[[maybe_unused]] inline void In<NYql::NDq::TPartitionKey>(IInputStream& in, NYql::NDq::TPartitionKey& value) {
    TString source;
    in >> source;

    TStringBuf cluster = source;
    ui64 partitionId;
    GetNext(cluster, '@', partitionId);

    value = NYql::NDq::TPartitionKey {
        .Cluster = TString{cluster},
        .PartitionId = partitionId,
    };
}

template <>
[[maybe_unused]] inline void Out<NYql::NDq::TPartitionKey>(IOutputStream& stream, TTypeTraits<NYql::NDq::TPartitionKey>::TFuncParam& pk) {
    stream << pk.PartitionId << '@' << pk.Cluster;
}
