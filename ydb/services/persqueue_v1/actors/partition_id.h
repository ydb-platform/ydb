#pragma once

#include <ydb/core/persqueue/public/nameresolver/nameresolver.h>

#include <util/stream/output.h>
#include <util/system/types.h>

#include <tuple>

namespace NKikimr::NGRpcProxy::V1 {

struct TPartitionId {
    NPQ::NNameResolver::TTopicNamesPtr TopicNames;
    ui64 Partition;
    ui64 AssignId;

    bool operator < (const TPartitionId& rhs) const {
        const TString& leftPath = TopicNames ? TopicNames->Path : TString();
        const TString& rightPath = rhs.TopicNames ? rhs.TopicNames->Path : TString();
        return std::make_tuple(AssignId, Partition, leftPath) <
               std::make_tuple(rhs.AssignId, rhs.Partition, rightPath);
    }
};


inline IOutputStream& operator <<(IOutputStream& out, const TPartitionId& partId) {
    out << "TopicId: " << (partId.TopicNames ? partId.TopicNames->GetPrintableString() : TString())
        << ", partition " << partId.Partition
        << "(assignId:" << partId.AssignId << ")";
    return out;
}

}
