#pragma once

#include <ydb/library/persqueue/topic_parser/topic_parser.h>

#include <util/stream/output.h>
#include <util/system/types.h>

#include <tuple>

namespace NKikimr::NGRpcProxy::V1 {

struct TPartitionId {
    //NPersQueue::TTopicConverterPtr TopicConverter;
    NPersQueue::TDiscoveryConverterPtr DiscoveryConverter;
    ui64 Partition;
    ui64 AssignId;

    bool operator < (const TPartitionId& rhs) const {
        return std::make_tuple(AssignId, Partition, DiscoveryConverter->GetOriginalPath()) <
               std::make_tuple(rhs.AssignId, rhs.Partition, rhs.DiscoveryConverter->GetOriginalPath());
    }
};


inline IOutputStream& operator <<(IOutputStream& out, const TPartitionId& partId) {
    out << "TopicId: " << partId.DiscoveryConverter->GetPrintableString() << ", partition " << partId.Partition
        << "(assignId:" << partId.AssignId << ")";
    return out;
}

}
