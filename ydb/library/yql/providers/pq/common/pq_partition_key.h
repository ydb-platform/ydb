#pragma once
#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/stream/output.h>
namespace NPq {
    struct TPartitionKey {
        TString Cluster;
        ui64 PartitionId;
        bool operator==(const TPartitionKey& other) const = default;
    };
    struct TPartitionKeyHash {
        ui64 operator()(const TPartitionKey& x) const {
            return CombineHashes<ui64>(
                    std::hash<TString>{} (x.Cluster),
                    std::hash<ui64>{} (x.PartitionId)
                    );
        }
    };
}

template <>
inline void Out<NPq::TPartitionKey>(IOutputStream& stream, TTypeTraits<NPq::TPartitionKey>::TFuncParam& t) {
    stream << t.PartitionId << '@' << t.Cluster;
}
