#include "schema.h"
#include <ydb/library/accessor/validator.h>

namespace NKikimr::NSchemeShard {

void TOlapIndexSchema::Serialize(NKikimrSchemeOp::TOlapIndexDescription& columnSchema) const {
    TBase::Serialize(columnSchema);
    columnSchema.SetId(Id);
}

void TOlapIndexSchema::ParseFromLocalDB(const NKikimrSchemeOp::TOlapIndexDescription& columnSchema) {
    TBase::ParseFromLocalDB(columnSchema);
    Id = columnSchema.GetId();
}

bool TOlapIndexesDescription::ApplyUpdate(const TOlapIndexesUpdate& schemaUpdate, IErrorCollector& errors, ui32& nextEntityId) {
    for (auto&& index : schemaUpdate.GetUpsertIndexes()) {
        auto* currentIndex = MutableByName(index.GetName());
        if (currentIndex) {
            if (!currentIndex->ApplyUpdate(index, errors)) {
                return false;
            }
        } else {
            const ui32 id = nextEntityId++;
            TOlapIndexSchema newIndex(index, id);
            Y_ABORT_UNLESS(IndexesByName.emplace(index.GetName(), id).second);
            Y_ABORT_UNLESS(Indexes.emplace(id, std::move(newIndex)).second);
        }
    }

    for (const auto& name : schemaUpdate.GetDropIndexes()) {
        auto info = GetByName(name);
        if (!info) {
            errors.AddError(NKikimrScheme::StatusSchemeError, TStringBuilder() << "Unknown index for drop: " << name);
            return false;
        }
        AFL_VERIFY(IndexesByName.erase(name));
        AFL_VERIFY(Indexes.erase(info->GetId()));
    }

    return true;
}

void TOlapIndexesDescription::Parse(const NKikimrSchemeOp::TColumnTableSchema& tableSchema) {
    for (const auto& indexProto : tableSchema.GetIndexes()) {
        TOlapIndexSchema index;
        index.ParseFromLocalDB(indexProto);
        Y_ABORT_UNLESS(IndexesByName.emplace(indexProto.GetName(), indexProto.GetId()).second);
        Y_ABORT_UNLESS(Indexes.emplace(indexProto.GetId(), std::move(index)).second);
    }
}

void TOlapIndexesDescription::Serialize(NKikimrSchemeOp::TColumnTableSchema& tableSchema) const {
    for (const auto& index : Indexes) {
        index.second.Serialize(*tableSchema.AddIndexes());
    }
}

bool TOlapIndexesDescription::Validate(const NKikimrSchemeOp::TColumnTableSchema& opSchema, IErrorCollector& errors) const {
    THashSet<ui32> usedIndexes;
    ui32 lastIdx = 0;
    for (const auto& proto : opSchema.GetIndexes()) {
        if (proto.GetName().empty()) {
            errors.AddError("Index cannot have an empty name");
            return false;
        }
        const TString& name = proto.GetName();
        auto* index = GetByName(name);
        if (!index) {
            errors.AddError("Index '" + name + "' does not match schema preset");
            return false;
        }
        if (proto.HasId() && proto.GetId() != index->GetId()) {
            errors.AddError("Index '" + name + "' has id " + proto.GetId() + " that does not match schema preset");
            return false;
        }

        if (!usedIndexes.insert(index->GetId()).second) {
            errors.AddError("Column '" + name + "' is specified multiple times");
            return false;
        }
        if (index->GetId() < lastIdx) {
            errors.AddError("Index order does not match schema preset");
            return false;
        }
        lastIdx = index->GetId();

    }

    for (auto& pr : Indexes) {
        if (!usedIndexes.contains(pr.second.GetId())) {
            errors.AddError("Specified schema is missing some schema preset indexes");
            return false;
        }
    }
    return true;
}

const NKikimr::NSchemeShard::TOlapIndexSchema* TOlapIndexesDescription::GetByIdVerified(const ui32 id) const noexcept {
    return TValidator::CheckNotNull(GetById(id));
}

NKikimr::NSchemeShard::TOlapIndexSchema* TOlapIndexesDescription::MutableByIdVerified(const ui32 id) noexcept {
    return TValidator::CheckNotNull(MutableById(id));
}

}
