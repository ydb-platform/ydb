#include "schema.h"
#include <ydb/library/accessor/validator.h>
#include <ydb/core/tx/columnshard/blobs_action/common/const.h>

namespace NKikimr::NSchemeShard {

void TOlapIndexSchema::SerializeToProto(NKikimrSchemeOp::TOlapIndexDescription& indexSchema) const {
    indexSchema.SetId(Id);
    indexSchema.SetName(Name);
    IndexMeta.SerializeToProto(indexSchema);
}

void TOlapIndexSchema::DeserializeFromProto(const NKikimrSchemeOp::TOlapIndexDescription& indexSchema) {
    Id = indexSchema.GetId();
    Name = indexSchema.GetName();
    AFL_VERIFY(IndexMeta.DeserializeFromProto(indexSchema))("incorrect_proto", indexSchema.DebugString());
}

bool TOlapIndexSchema::ApplyUpdate(const TOlapSchema& currentSchema, const TOlapIndexUpsert& upsert, IErrorCollector& errors) {
    AFL_VERIFY(upsert.GetName() == GetName());
    AFL_VERIFY(!!upsert.GetIndexConstructor());
    if (upsert.GetIndexConstructor().GetClassName() != IndexMeta.GetClassName()) {
        errors.AddError("different index classes: " + upsert.GetIndexConstructor().GetClassName() + " vs " + IndexMeta.GetClassName());
        return false;
    }
    auto object = upsert.GetIndexConstructor()->CreateIndexMeta(GetId(), GetName(), currentSchema, errors);
    if (!object) {
        return false;
    }
    auto conclusion = IndexMeta->CheckModificationCompatibility(object);
    if (conclusion.IsFail()) {
        errors.AddError("cannot modify index: " + conclusion.GetErrorMessage());
        return false;
    }
    IndexMeta = NBackgroundTasks::TInterfaceProtoContainer<NOlap::NIndexes::IIndexMeta>(object);
    return true;
}

bool TOlapIndexesDescription::ApplyUpdate(const TOlapSchema& currentSchema, const TOlapIndexesUpdate& schemaUpdate, IErrorCollector& errors, ui32& nextEntityId) {
    for (auto&& index : schemaUpdate.GetUpsertIndexes()) {
        auto* currentIndex = MutableByName(index.GetName());
        if (currentIndex) {
            if (!currentIndex->ApplyUpdate(currentSchema, index, errors)) {
                return false;
            }
        } else {
            const ui32 id = nextEntityId++;
            auto meta = index.GetIndexConstructor()->CreateIndexMeta(id, index.GetName(), currentSchema, errors);
            if (!meta) {
                return false;
            }
            TOlapIndexSchema newIndex(id, index.GetName(), meta);
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
        index.DeserializeFromProto(indexProto);
        Y_ABORT_UNLESS(IndexesByName.emplace(indexProto.GetName(), indexProto.GetId()).second);
        Y_ABORT_UNLESS(Indexes.emplace(indexProto.GetId(), std::move(index)).second);
    }
}

void TOlapIndexesDescription::Serialize(NKikimrSchemeOp::TColumnTableSchema& tableSchema) const {
    for (const auto& index : Indexes) {
        index.second.SerializeToProto(*tableSchema.AddIndexes());
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
