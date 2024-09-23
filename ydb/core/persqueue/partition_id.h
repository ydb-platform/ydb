#pragma once

#include <util/generic/maybe.h>
#include <util/stream/output.h>
#include <util/system/types.h>
#include <util/digest/multi.h>
#include <util/str_stl.h>

#include <functional>

namespace NKikimr::NPQ {

class TPartitionId {
public:
    TPartitionId() = default;

    explicit TPartitionId(ui32 partition) :
        OriginalPartitionId(partition),
        InternalPartitionId(partition)
    {
    }

    TPartitionId(ui32 originalPartitionId, TMaybe<ui64> writeId, ui32 internalPartitionId) :
        OriginalPartitionId(originalPartitionId),
        WriteId(writeId),
        InternalPartitionId(internalPartitionId)
    {
    }

    size_t GetHash() const
    {
        return MultiHash(MultiHash(OriginalPartitionId, WriteId), InternalPartitionId);
    }

    bool IsEqual(const TPartitionId& rhs) const
    {
        return
            (OriginalPartitionId == rhs.OriginalPartitionId) &&
            (WriteId == rhs.WriteId) &&
            (InternalPartitionId == rhs.InternalPartitionId);
    }

    void ToStream(IOutputStream& s) const
    {
        if (WriteId.Defined()) {
            s << '{' << OriginalPartitionId << ", " << *WriteId << ", " << InternalPartitionId << '}';
        } else {
            s << OriginalPartitionId;
        }
    }

    bool IsSupportivePartition() const
    {
        return WriteId.Defined();
    }

    ui32 OriginalPartitionId = 0;
    TMaybe<ui64> WriteId;
    ui32 InternalPartitionId = 0;
};

inline
bool operator==(const TPartitionId& lhs, const TPartitionId& rhs)
{
    return lhs.IsEqual(rhs);
}

inline
IOutputStream& operator<<(IOutputStream& s, const TPartitionId& v)
{
    v.ToStream(s);
    return s;
}

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
struct less<NKikimr::NPQ::TPartitionId> {
    inline bool operator()(const NKikimr::NPQ::TPartitionId& lhs, const NKikimr::NPQ::TPartitionId& rhs) const
    {
        if (lhs.OriginalPartitionId < rhs.OriginalPartitionId) {
            return true;
        } else if (rhs.OriginalPartitionId < lhs.OriginalPartitionId) {
            return false;
        } else {
            return lhs.WriteId < rhs.WriteId;
        }
    }
};

template <>
struct hash<NKikimr::NPQ::TPartitionId> {
    inline size_t operator()(const NKikimr::NPQ::TPartitionId& v) const
    {
        return THash<NKikimr::NPQ::TPartitionId>()(v);
    }
};

}
