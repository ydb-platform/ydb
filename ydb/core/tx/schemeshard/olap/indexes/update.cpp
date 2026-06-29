#include "update.h"

namespace NKikimr::NSchemeShard {

namespace {

bool ValidateMoveIndexOperation(
    const NKikimrSchemeOp::TOlapMoveIndex& rename,
    TSet<TString>& renameSources,
    TSet<TString>& renameDestinations,
    IErrorCollector& errors
) {
    if (!rename.GetSourceName()) {
        errors.AddError(NKikimrScheme::StatusInvalidParameter, "Empty source index name for move");
        return false;
    }

    if (!rename.GetDestinationName()) {
        errors.AddError(NKikimrScheme::StatusInvalidParameter, "Empty destination index name for move");
        return false;
    }

    if (rename.GetSourceName() == rename.GetDestinationName()) {
        errors.AddError(NKikimrScheme::StatusInvalidParameter, "Source and destination index names must differ");
        return false;
    }

    if (!renameSources.emplace(rename.GetSourceName()).second) {
        errors.AddError(
            NKikimrScheme::StatusInvalidParameter,
            TStringBuilder() << "Duplicated index for move source: " << rename.GetSourceName()
        );

        return false;
    }

    if (!renameDestinations.emplace(rename.GetDestinationName()).second) {
        errors.AddError(
            NKikimrScheme::StatusAlreadyExists,
            TStringBuilder() << "Duplicated index for move destination: " << rename.GetDestinationName()
        );

        return false;
    }

    return true;
}

}

void TOlapIndexUpsert::SerializeToProto(NKikimrSchemeOp::TOlapIndexRequested& requestedProto) const {
    requestedProto.SetName(Name);
    if (StorageId && !!*StorageId) {
        requestedProto.SetStorageId(*StorageId);
    }
    if (!!InheritPortionStorage) {
        requestedProto.SetInheritPortionStorage(*InheritPortionStorage);
    }
    IndexConstructor.SerializeToProto(requestedProto);
}

bool TOlapIndexUpsert::DeserializeFromProto(const NKikimrSchemeOp::TOlapIndexRequested& indexSchema) {
    Name = indexSchema.GetName();
    if (!!indexSchema.GetStorageId()) {
        StorageId = indexSchema.GetStorageId();
    }
    if (indexSchema.HasInheritPortionStorage()) {
        InheritPortionStorage = indexSchema.GetInheritPortionStorage();
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

    TSet<TString> moveSources;
    TSet<TString> moveDestinations;
    for (auto&& rename : alterRequest.GetMoveIndex()) {
        if (!ValidateMoveIndexOperation(rename, moveSources, moveDestinations, errors)) {
            return false;
        }

        MoveIndexes.emplace_back(rename.GetSourceName(), rename.GetDestinationName(), rename.GetReplaceDestination());
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
