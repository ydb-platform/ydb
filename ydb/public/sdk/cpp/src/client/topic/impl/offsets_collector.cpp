#include "offsets_collector.h"

namespace NYdb::inline Dev::NTopic {

std::vector<TTopicOffsets> TOffsetsCollector::GetOffsets() const
{
    std::vector<TTopicOffsets> topics;

    for (auto& [path, partitions] : Ranges) {
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

    return topics;
}

void TOffsetsCollector::CollectOffsets(const std::vector<TReadSessionEvent::TEvent>& events)
{
    for (auto& event : events) {
        if (auto* e = std::get_if<TReadSessionEvent::TDataReceivedEvent>(&event)) {
            CollectOffsets(*e);
        }
    }
}

void TOffsetsCollector::CollectOffsets(const TReadSessionEvent::TEvent& event)
{
    if (auto* e = std::get_if<TReadSessionEvent::TDataReceivedEvent>(&event)) {
        CollectOffsets(*e);
    }
}

void TOffsetsCollector::CollectOffsets(const TReadSessionEvent::TDataReceivedEvent& event)
{
    const auto& session = *event.GetPartitionSession();
    const std::string& topicPath = session.GetTopicPath();
    uint32_t partitionId = session.GetPartitionId();

    if (event.HasCompressedMessages()) {
        for (auto& message : event.GetCompressedMessages()) {
            uint64_t offset = message.GetOffset();
            Ranges[topicPath][partitionId].InsertInterval(offset, offset + 1);
        }
    } else {
        for (auto& message : event.GetMessages()) {
            uint64_t offset = message.GetOffset();
            Ranges[topicPath][partitionId].InsertInterval(offset, offset + 1);
        }
    }
}

}