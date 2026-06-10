#pragma once

#include <util/generic/hash.h>
#include <util/stream/output.h>

namespace NYql::NDq {

struct TPartitionKey {
    std::string Cluster;
    ui64 PartitionId;

    bool operator==(const TPartitionKey&) const = default;
};

} // namespace NYql::NDq

template<>
struct THash<NYql::NDq::TPartitionKey> {
    ui64 operator() (const NYql::NDq::TPartitionKey& pk) const {
        return CombineHashes<ui64>(
            std::hash<std::string>{} (pk.Cluster),
            std::hash<ui64>{} (pk.PartitionId)
        );
    }
};

template <>
[[maybe_unused]] inline void Out<NYql::NDq::TPartitionKey>(IOutputStream& stream, TTypeTraits<NYql::NDq::TPartitionKey>::TFuncParam& pk) {
    stream << pk.PartitionId << '@' << pk.Cluster;
}
