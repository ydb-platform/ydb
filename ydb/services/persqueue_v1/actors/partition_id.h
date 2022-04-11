#pragma once

#include <ydb/library/persqueue/topic_parser/topic_parser.h>

#include <util/stream/output.h>
#include <util/system/types.h>

#include <tuple>

namespace NKikimr::NGRpcProxy::V1 {

struct TPartitionId {
    NPersQueue::TConverterPtr TopicConverter;
    ui64 Partition;
    ui64 AssignId;

    inline bool operator<(const TPartitionId& rhs) const {
        return std::make_tuple(AssignId, Partition, TopicConverter->GetClientsideName()) <
               std::make_tuple(rhs.AssignId, rhs.Partition, rhs.TopicConverter->GetClientsideName());
    }
};

inline IOutputStream& operator<<(IOutputStream& out, const TPartitionId& partId) {
    out << "TopicId: " << partId.TopicConverter->GetClientsideName() << ":" << partId.Partition << "(assignId:" << partId.AssignId << ")";
    return out;
}

}
