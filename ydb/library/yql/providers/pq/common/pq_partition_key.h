#pragma once
#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/stream/output.h>
namespace NPq {
    struct TPartitionKey {
        TString Cluster;
        ui64 PartitionId;
        bool operator==(const TPartitionKey& other) const = default;
        ui64 Hash() const {
            return CombineHashes<ui64>(
                    std::hash<TString>{} (Cluster),
                    std::hash<ui64>{} (PartitionId)
                    );
        }
    };
}

template<>
struct THash<NPq::TPartitionKey> {
    ui64 operator() (const NPq::TPartitionKey& x) const {
        return x.Hash();
    }
};

template <>
inline void Out<NPq::TPartitionKey>(IOutputStream& stream, TTypeTraits<NPq::TPartitionKey>::TFuncParam& t) {
    stream << t.PartitionId << '@' << t.Cluster;
}
