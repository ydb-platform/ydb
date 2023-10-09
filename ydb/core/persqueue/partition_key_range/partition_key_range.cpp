#include "partition_key_range.h"

#include <ydb/core/protos/pqconfig.pb.h>

namespace NKikimr {
namespace NPQ {

TPartitionKeyRange TPartitionKeyRange::Parse(const NKikimrPQ::TPartitionKeyRange& proto) {
    TPartitionKeyRange result;

    if (proto.HasFromBound()) {
        ParseBound(proto.GetFromBound(), result.FromBound);
    }

    if (proto.HasToBound()) {
        ParseBound(proto.GetToBound(), result.ToBound);
    }

    return result;
}

void TPartitionKeyRange::Serialize(NKikimrPQ::TPartitionKeyRange& proto) const {
    if (FromBound) {
        proto.SetFromBound(FromBound->GetBuffer());
    }

    if (ToBound) {
        proto.SetToBound(ToBound->GetBuffer());
    }
}

void TPartitionKeyRange::ParseBound(const TString& data, TMaybe<TSerializedCellVec>& bound) {
    TSerializedCellVec cells;
    Y_ABORT_UNLESS(TSerializedCellVec::TryParse(data, cells));
    bound.ConstructInPlace(std::move(cells));
}

} // NPQ
} // NKikimr
