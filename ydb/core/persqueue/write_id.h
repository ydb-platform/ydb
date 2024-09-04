#pragma once

#include <ydb/core/protos/kqp.pb.h>
#include <ydb/core/protos/msgbus_pq.pb.h>
#include <ydb/core/protos/pqconfig.pb.h>

#include <util/system/types.h>
#include <util/digest/multi.h>
#include <util/stream/output.h>

namespace NKikimr::NPQ {

struct TWriteId {
    TWriteId() = default;
    TWriteId(ui64 nodeId, ui64 keyId);

    bool operator==(const TWriteId& rhs) const;
    bool operator<(const TWriteId& rhs) const;

    void ToStream(IOutputStream& s) const;

    size_t GetHash() const
    {
        return MultiHash(NodeId, KeyId);
    }

    ui64 NodeId = 0;
    ui64 KeyId = 0;
};

inline
IOutputStream& operator<<(IOutputStream& s, const TWriteId& v)
{
    v.ToStream(s);
    return s;
}

TWriteId GetWriteId(const NKikimrPQ::TTransaction& m);
void SetWriteId(NKikimrPQ::TTransaction& m, const TWriteId& writeId);

TWriteId GetWriteId(const NKikimrPQ::TDataTransaction& m);
void SetWriteId(NKikimrPQ::TDataTransaction& m, const TWriteId& writeId);

TWriteId GetWriteId(const NKikimrPQ::TTabletTxInfo::TTxWriteInfo& m);
void SetWriteId(NKikimrPQ::TTabletTxInfo::TTxWriteInfo& m, const TWriteId& writeId);

TWriteId GetWriteId(const NKikimrClient::TPersQueuePartitionRequest& m);
void SetWriteId(NKikimrClient::TPersQueuePartitionRequest& m, const TWriteId& writeId);

TWriteId GetWriteId(const NKikimrKqp::TTopicOperationsResponse& m);
void SetWriteId(NKikimrKqp::TTopicOperationsResponse& m, const TWriteId& writeId);

}

template <>
struct THash<NKikimr::NPQ::TWriteId> {
    inline size_t operator()(const NKikimr::NPQ::TWriteId& v) const
    {
        return v.GetHash();
    }
};
