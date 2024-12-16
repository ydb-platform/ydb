#include "write_id.h"

namespace NKikimr::NPQ {

TWriteId::TWriteId(ui64 nodeId, ui64 keyId) :
    NodeId(nodeId),
    KeyId(keyId)
{
}

bool TWriteId::operator==(const TWriteId& rhs) const
{
    return std::make_tuple(NodeId, KeyId) == std::make_tuple(rhs.NodeId, rhs.KeyId);
}

bool TWriteId::operator<(const TWriteId& rhs) const
{
    return std::make_tuple(NodeId, KeyId) < std::make_tuple(rhs.NodeId, rhs.KeyId);
}

void TWriteId::ToStream(IOutputStream& s) const
{
    s << '{' << NodeId << ", " << KeyId << '}';
}

template <class T>
TWriteId GetWriteIdImpl(const T& m)
{
    const auto& writeId = m.GetWriteId();
    return {writeId.GetNodeId(), writeId.GetKeyId()};
}

template <class T>
void SetWriteIdImpl(T& m, const TWriteId& writeId)
{
    auto* w = m.MutableWriteId();
    w->SetNodeId(writeId.NodeId);
    w->SetKeyId(writeId.KeyId);
}

TWriteId GetWriteId(const NKikimrPQ::TTransaction& m)
{
    return GetWriteIdImpl(m);
}

void SetWriteId(NKikimrPQ::TTransaction& m, const TWriteId& writeId)
{
    SetWriteIdImpl(m, writeId);
}

TWriteId GetWriteId(const NKikimrPQ::TDataTransaction& m)
{
    return GetWriteIdImpl(m);
}

void SetWriteId(NKikimrPQ::TDataTransaction& m, const TWriteId& writeId)
{
    SetWriteIdImpl(m, writeId);
}

TWriteId GetWriteId(const NKikimrPQ::TTabletTxInfo::TTxWriteInfo& m)
{
    return GetWriteIdImpl(m);
}

void SetWriteId(NKikimrPQ::TTabletTxInfo::TTxWriteInfo& m, const TWriteId& writeId)
{
    SetWriteIdImpl(m, writeId);
}

TWriteId GetWriteId(const NKikimrClient::TPersQueuePartitionRequest& m)
{
    return GetWriteIdImpl(m);
}

void SetWriteId(NKikimrClient::TPersQueuePartitionRequest& m, const NKikimr::NPQ::TWriteId& writeId)
{
    SetWriteIdImpl(m, writeId);
}

TWriteId GetWriteId(const NKikimrKqp::TTopicOperationsResponse& m)
{
    return GetWriteIdImpl(m);
}

void SetWriteId(NKikimrKqp::TTopicOperationsResponse& m, const TWriteId& writeId)
{
    SetWriteIdImpl(m, writeId);
}

}
