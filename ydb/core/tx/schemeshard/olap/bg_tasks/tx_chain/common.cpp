#include "common.h"

namespace NKikimr::NSchemeShard::NOlap::NBackground {

TConclusionStatus TTxChainData::DeserializeFromProto(const TProtoStorage& proto) {
    TablePath = proto.GetTablePath();
    for (auto&& i : proto.GetModification()) {
        Transactions.emplace_back(i.GetTransaction());
    }
    return TConclusionStatus::Success();
}

TTxChainData::TProtoStorage TTxChainData::SerializeToProto() const {
    TProtoStorage result;
    result.SetTablePath(TablePath);
    for (auto&& i : Transactions) {
        *result.AddModification()->MutableTransaction() = i;
    }
    return result;
}

}