#include "simple.h"

namespace NKikimr::NOlap::NIndexes {

bool TSimpleIndexChecker::DoDeserializeFromProto(const NKikimrSSA::TProgram::TOlapIndexChecker& proto) {
    if (proto.HasIndexCategory()) {
        IndexId = TIndexDataAddress(proto.GetIndexId(), proto.GetIndexCategory());
    } else {
        IndexId = TIndexDataAddress(proto.GetIndexId());
    }
    return DoDeserializeFromProtoImpl(proto);
}

bool TSimpleIndexChecker::DoCheck(const THashMap<TIndexDataAddress, std::vector<TString>>& blobs) const {
    auto it = blobs.find(IndexId);
    AFL_VERIFY(it != blobs.end());
    return DoCheckImpl(it->second);
}

void TSimpleIndexChecker::DoSerializeToProto(NKikimrSSA::TProgram::TOlapIndexChecker& proto) const {
    AFL_VERIFY(IndexId.GetIndexId());
    proto.SetIndexId(IndexId.GetIndexId());
    if (IndexId.GetCategory()) {
        proto.SetIndexCategory(*IndexId.GetCategory());
    }
    return DoSerializeToProtoImpl(proto);
}

}   // namespace NKikimr::NOlap::NIndexes