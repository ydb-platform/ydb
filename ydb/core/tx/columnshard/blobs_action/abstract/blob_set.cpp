#include "blob_set.h"

#include <ydb/core/tx/columnshard/blobs_action/protos/blobs.pb.h>

#include <util/generic/refcount.h>
#include <util/string/join.h>

namespace NKikimr::NOlap {

NKikimrColumnShardBlobOperationsProto::TTabletByBlob TTabletByBlob::SerializeToProto() const {
    NKikimrColumnShardBlobOperationsProto::TTabletByBlob result;
    for (auto&& i : Data) {
        auto* blobsProto = result.AddBlobs();
        blobsProto->SetBlobId(i.first.ToStringNew());
        blobsProto->SetTabletId((ui64)i.second);
    }
    return result;
}

NKikimr::TConclusionStatus TTabletByBlob::DeserializeFromProto(const NKikimrColumnShardBlobOperationsProto::TTabletByBlob& proto) {
    for (auto&& i : proto.GetBlobs()) {
        auto parse = TUnifiedBlobId::BuildFromString(i.GetBlobId(), nullptr);
        if (!parse) {
            return parse;
        }
        AFL_VERIFY(Data.emplace(*parse, (TTabletId)i.GetTabletId()).second);
    }
    return TConclusionStatus::Success();
}

NKikimrColumnShardBlobOperationsProto::TTabletsByBlob TTabletsByBlob::SerializeToProto() const {
    NKikimrColumnShardBlobOperationsProto::TTabletsByBlob result;
    for (auto&& i : Data) {
        auto* blobsProto = result.AddBlobs();
        blobsProto->SetBlobId(i.first.ToStringNew());
        for (auto&& t : i.second) {
            blobsProto->AddTabletIds((ui64)t);
        }
    }
    return result;
}

NKikimr::TConclusionStatus TTabletsByBlob::DeserializeFromProto(const NKikimrColumnShardBlobOperationsProto::TTabletsByBlob& proto) {
    for (auto&& i : proto.GetBlobs()) {
        auto parse = TUnifiedBlobId::BuildFromString(i.GetBlobId(), nullptr);
        if (!parse) {
            return parse;
        }
        auto it = Data.emplace(*parse, THashSet<TTabletId>()).first;
        for (auto&& t : i.GetTabletIds()) {
            AFL_VERIFY(it->second.emplace((TTabletId)t).second);
            ++Size;
        }
    }
    return TConclusionStatus::Success();
}

TString TTabletsByBlob::DebugString() const {
    TStringBuilder sb;
    for (auto&& i : Data) {
        sb << "[";
        sb << i.first.ToStringNew() << ":" << JoinSeq(",", i.second);
        sb << "];";
    }
    return sb;
}

}
