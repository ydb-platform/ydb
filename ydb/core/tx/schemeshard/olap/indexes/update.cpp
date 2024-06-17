#include "update.h"

namespace NKikimr::NSchemeShard {

void TOlapIndexUpsert::SerializeToProto(NKikimrSchemeOp::TOlapIndexRequested& requestedProto) const {
    requestedProto.SetName(Name);
    if (StorageId && !!*StorageId) {
        requestedProto.SetStorageId(*StorageId);
    }
    IndexConstructor.SerializeToProto(requestedProto);
}

bool TOlapIndexUpsert::DeserializeFromProto(const NKikimrSchemeOp::TOlapIndexRequested& indexSchema) {
    Name = indexSchema.GetName();
    if (!!indexSchema.GetStorageId()) {
        StorageId = indexSchema.GetStorageId();
    }
    AFL_VERIFY(IndexConstructor.DeserializeFromProto(indexSchema))("incorrect_proto", indexSchema.DebugString());
    return true;
}

bool TOlapIndexesUpdate::Parse(const NKikimrSchemeOp::TAlterColumnTableSchema& alterRequest, IErrorCollector& errors) {
    for (const auto& indexName : alterRequest.GetDropIndexes()) {
        if (!DropIndexes.emplace(indexName).second) {
            errors.AddError(NKikimrScheme::StatusInvalidParameter, "Duplicated index for drop");
            return false;
        }
    }
    TSet<TString> upsertIndexNames;
    for (auto& indexSchema : alterRequest.GetUpsertIndexes()) {
        TOlapIndexUpsert index;
        AFL_VERIFY(index.DeserializeFromProto(indexSchema));
        if (!upsertIndexNames.emplace(index.GetName()).second) {
            errors.AddError(NKikimrScheme::StatusAlreadyExists, TStringBuilder() << "index '" << index.GetName() << "' duplication for add");
            return false;
        }
        UpsertIndexes.emplace_back(std::move(index));
    }
    return true;
}
}
