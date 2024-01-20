#include "update.h"

namespace NKikimr::NSchemeShard {

void TOlapIndexUpsert::Serialize(NKikimrSchemeOp::TOlapIndexDescription& columnSchema) const {
    columnSchema.SetName(Name);
    IndexConstructor.SerializeToProto(columnSchema);
}

bool TOlapIndexUpsert::ParseFromRequest(const NKikimrSchemeOp::TOlapIndexDescription& indexSchema, IErrorCollector& errors) {
    if (!indexSchema.GetName()) {
        errors.AddError("Indexes cannot have an empty name");
        return false;
    }
    Name = indexSchema.GetName();
    if (!IndexConstructor.DeserializeFromProto(indexSchema)) {
        errors.AddError("Cannot restore index data from proto: \"" + indexSchema.DebugString() + "\"");
        return false;
    }
    return true;
}

void TOlapIndexUpsert::ParseFromLocalDB(const NKikimrSchemeOp::TOlapIndexDescription& indexSchema) {
    Name = indexSchema.GetName();
    AFL_VERIFY(IndexConstructor.DeserializeFromProto(indexSchema))("incorrect_proto", indexSchema.DebugString());
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
        if (!index.ParseFromRequest(indexSchema, errors)) {
            return false;
        }
        if (!upsertIndexNames.emplace(index.GetName()).second) {
            errors.AddError(NKikimrScheme::StatusAlreadyExists, TStringBuilder() << "index '" << index.GetName() << "' duplication for add");
            return false;
        }
        UpsertIndexes.emplace_back(std::move(index));
    }
    return true;
}

bool TOlapIndexesUpdate::Parse(const NKikimrSchemeOp::TColumnTableSchema& tableSchema, IErrorCollector& errors) {
    TSet<TString> indexNames;
    for (auto& indexSchema : tableSchema.GetIndexes()) {
        TOlapIndexUpsert index;
        if (!index.ParseFromRequest(indexSchema, errors)) {
            return false;
        }
        if (!indexNames.emplace(index.GetName()).second) {
            errors.AddError(NKikimrScheme::StatusMultipleModifications, TStringBuilder() << "Duplicate index '" << index.GetName() << "'");
            return false;
        }
        indexNames.emplace(index.GetName());
        UpsertIndexes.emplace_back(std::move(index));
    }

    return true;
}
}
