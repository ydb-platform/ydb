#pragma once

#include <ydb/core/persqueue/public/write_id.h>

#include <util/generic/maybe.h>
#include <util/stream/fwd.h>
#include <util/system/types.h>
#include <util/str_stl.h>

#include <compare>
#include <functional>
#include <tuple>

namespace NKikimr::NPQ {

class TPartitionId {
public:
    TPartitionId() = default;

    explicit TPartitionId(ui32 partition) :
        OriginalPartitionId(partition),
        InternalPartitionId(partition)
    {
    }

    TPartitionId(ui32 originalPartitionId, const TMaybe<TWriteId>& writeId, ui32 internalPartitionId) :
        OriginalPartitionId(originalPartitionId),
        WriteId(writeId),
        InternalPartitionId(internalPartitionId)
    {
    }

    size_t GetHash() const;

    friend auto operator<=>(const TPartitionId& lhs, const TPartitionId& rhs) {
        auto makeTuple = [](const TPartitionId& v) {
            return std::tie(v.OriginalPartitionId, v.WriteId, v.InternalPartitionId);
        };
        return makeTuple(lhs) <=> makeTuple(rhs);
    }

    friend bool operator==(const TPartitionId& lhs, const TPartitionId& rhs) = default;

    void ToStream(IOutputStream& s) const;

    TString ToString() const;

    bool IsSupportivePartition() const
    {
        return WriteId.Defined();
    }

    ui32 OriginalPartitionId = 0;
    TMaybe<TWriteId> WriteId;
    ui32 InternalPartitionId = 0;
};

}

template <>
struct THash<NKikimr::NPQ::TPartitionId> {
    inline size_t operator()(const NKikimr::NPQ::TPartitionId& v) const
    {
        return v.GetHash();
    }
};

namespace std {

template <>
struct hash<NKikimr::NPQ::TPartitionId> {
    inline size_t operator()(const NKikimr::NPQ::TPartitionId& v) const
    {
        return THash<NKikimr::NPQ::TPartitionId>()(v);
    }
};

}
