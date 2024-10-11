#include "offsets_collector.h"

namespace NYdb::NTopic {

void TOffsetsCollector::CollectOffsets(const TTransactionId& txId,
                                       const TVector<TReadSessionEvent::TEvent>& events)
{
    TTransactionInfoPtr txInfo = GetOrCreateTransactionInfo(txId);
    for (auto& event : events) {
        CollectOffsets(txInfo, event);
    }
}

void TOffsetsCollector::CollectOffsets(const TTransactionId& txId,
                                       const TReadSessionEvent::TEvent& event)
{
    TTransactionInfoPtr txInfo = GetOrCreateTransactionInfo(txId);
    CollectOffsets(txInfo, event);
}

TVector<TTopicOffsets> TOffsetsCollector::ExtractOffsets(const TTransactionId& txId)
{
    auto p = Txs.find(txId);
    Y_ABORT_UNLESS(p != Txs.end(), "txId=%s", GetTxId(txId).data());

    const TTransactionInfoPtr txInfo = p->second;
    Y_ABORT_UNLESS(txInfo);

    TVector<TTopicOffsets> topics;

    for (auto& [path, partitions] : txInfo->Ranges) {
        TTopicOffsets topic;
        topic.Path = path;

        topics.push_back(std::move(topic));

        for (auto& [id, ranges] : partitions) {
            TPartitionOffsets partition;
            partition.PartitionId = id;

            TTopicOffsets& t = topics.back();
            t.Partitions.push_back(std::move(partition));

            for (auto& range : ranges) {
                TPartitionOffsets& p = t.Partitions.back();

                TOffsetsRange r;
                r.Start = range.first;
                r.End = range.second;

                p.Offsets.push_back(r);
            }
        }
    }

    Txs.erase(txId);

    return topics;
}

auto TOffsetsCollector::GetOrCreateTransactionInfo(const TTransactionId& txId) -> TTransactionInfoPtr
{
    auto p = Txs.find(txId);
    if (p == Txs.end()) {
        TTransactionInfoPtr& txInfo = Txs[txId];
        txInfo = std::make_shared<TTransactionInfo>();
        txInfo->Subscribed = false;
        p = Txs.find(txId);
    }
    return p->second;
}

void TOffsetsCollector::CollectOffsets(TTransactionInfoPtr txInfo,
                                       const TReadSessionEvent::TEvent& event)
{
    if (auto* e = std::get_if<TReadSessionEvent::TDataReceivedEvent>(&event)) {
        CollectOffsets(txInfo, *e);
    }
}

void TOffsetsCollector::CollectOffsets(TTransactionInfoPtr txInfo,
                                       const TReadSessionEvent::TDataReceivedEvent& event)
{
    const auto& session = *event.GetPartitionSession();
    const TString& topicPath = session.GetTopicPath();
    ui32 partitionId = session.GetPartitionId();

    with_lock (txInfo->Lock) {
        if (event.HasCompressedMessages()) {
            for (auto& message : event.GetCompressedMessages()) {
                ui64 offset = message.GetOffset();
                txInfo->Ranges[topicPath][partitionId].InsertInterval(offset, offset + 1);
            }
        } else {
            for (auto& message : event.GetMessages()) {
                ui64 offset = message.GetOffset();
                txInfo->Ranges[topicPath][partitionId].InsertInterval(offset, offset + 1);
            }
        }
    }
}

}
