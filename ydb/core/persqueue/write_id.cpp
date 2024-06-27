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

TWriteId GetWriteId(const NKikimrPQ::TTransaction& m)
{
    const auto& writeId = m.GetWriteId();
    return {writeId.GetNodeId(), writeId.GetKeyId()};
}

void SetWriteId(NKikimrPQ::TTransaction& m, const TWriteId& writeId)
{
    auto* w = m.MutableWriteId();
    w->SetNodeId(writeId.NodeId);
    w->SetKeyId(writeId.KeyId);
}

TWriteId GetWriteId(const NKikimrPQ::TDataTransaction& m)
{
    const auto& writeId = m.GetWriteId();
    return {writeId.GetNodeId(), writeId.GetKeyId()};
}

void SetWriteId(NKikimrPQ::TDataTransaction& m, const TWriteId& writeId)
{
    auto* w = m.MutableWriteId();
    w->SetNodeId(writeId.NodeId);
    w->SetKeyId(writeId.KeyId);
}

TWriteId GetWriteId(const NKikimrPQ::TTabletTxInfo::TTxWriteInfo& m)
{
    const auto& writeId = m.GetWriteId();
    return {writeId.GetNodeId(), writeId.GetKeyId()};
}

void SetWriteId(NKikimrPQ::TTabletTxInfo::TTxWriteInfo& m, const TWriteId& writeId)
{
    auto* w = m.MutableWriteId();
    w->SetNodeId(writeId.NodeId);
    w->SetKeyId(writeId.KeyId);
}

TWriteId GetWriteId(const NKikimrClient::TPersQueuePartitionRequest& m)
{
    const auto& writeId = m.GetWriteId();
    return {writeId.GetNodeId(), writeId.GetKeyId()};
}

void SetWriteId(NKikimrClient::TPersQueuePartitionRequest& m, const NKikimr::NPQ::TWriteId& writeId)
{
    auto* w = m.MutableWriteId();
    w->SetNodeId(writeId.NodeId);
    w->SetKeyId(writeId.KeyId);
}

TWriteId GetWriteId(const NKikimrKqp::TTopicOperationsResponse& m)
{
    return {m.GetNodeId(), m.GetKeyId()};
}

void SetWriteId(NKikimrKqp::TTopicOperationsResponse& m, const TWriteId& writeId)
{
    m.SetNodeId(writeId.NodeId);
    m.SetKeyId(writeId.KeyId);
}

}
