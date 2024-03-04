#include "schema.h"
#include <ydb/library/accessor/validator.h>

namespace NKikimr::NSchemeShard {

void TOlapStatisticsSchema::SerializeToProto(NKikimrColumnShardStatisticsProto::TOperatorContainer& proto) const {
    proto.SetName(Name);
    Operator.SerializeToProto(proto);
}

bool TOlapStatisticsSchema::DeserializeFromProto(const NKikimrColumnShardStatisticsProto::TOperatorContainer& proto) {
    Name = proto.GetName();
    AFL_VERIFY(Operator.DeserializeFromProto(proto))("incorrect_proto", proto.DebugString());
    return true;
}

bool TOlapStatisticsSchema::ApplyUpdate(const TOlapSchema& /*currentSchema*/, const TOlapStatisticsUpsert& upsert, IErrorCollector& errors) {
    AFL_VERIFY(upsert.GetName() == GetName());
    AFL_VERIFY(!!upsert.GetConstructor());
    if (upsert.GetConstructor().GetClassName() != Operator.GetClassName()) {
        errors.AddError("different index classes: " + upsert.GetConstructor().GetClassName() + " vs " + Operator.GetClassName());
        return false;
    }
    errors.AddError("cannot modify statistics calculation for " + GetName() + ". not implemented currently.");
    return false;
}

bool TOlapStatisticsDescription::ApplyUpdate(const TOlapSchema& currentSchema, const TOlapStatisticsModification& schemaUpdate, IErrorCollector& errors) {
    for (auto&& stat : schemaUpdate.GetUpsert()) {
        auto* current = MutableByNameOptional(stat.GetName());
        if (current) {
            if (!current->ApplyUpdate(currentSchema, stat, errors)) {
                return false;
            }
        } else {
            auto meta = stat.GetConstructor()->CreateOperator(currentSchema);
            if (!meta) {
                errors.AddError(meta.GetErrorMessage());
                return false;
            }
            TOlapStatisticsSchema object(stat.GetName(), meta.DetachResult());
            Y_ABORT_UNLESS(ObjectsByName.emplace(stat.GetName(), std::move(object)).second);
        }
    }

    for (const auto& name : schemaUpdate.GetDrop()) {
        auto info = GetByNameOptional(name);
        if (!info) {
            errors.AddError(NKikimrScheme::StatusSchemeError, TStringBuilder() << "Unknown stat for drop: " << name);
            return false;
        }
        AFL_VERIFY(ObjectsByName.erase(name));
    }

    return true;
}

void TOlapStatisticsDescription::Parse(const NKikimrSchemeOp::TColumnTableSchema& tableSchema) {
    for (const auto& proto : tableSchema.GetStatistics()) {
        TOlapStatisticsSchema object;
        AFL_VERIFY(object.DeserializeFromProto(proto));
        AFL_VERIFY(ObjectsByName.emplace(proto.GetName(), std::move(object)).second);
    }
}

void TOlapStatisticsDescription::Serialize(NKikimrSchemeOp::TColumnTableSchema& tableSchema) const {
    for (const auto& object : ObjectsByName) {
        object.second.SerializeToProto(*tableSchema.AddStatistics());
    }
}

bool TOlapStatisticsDescription::Validate(const NKikimrSchemeOp::TColumnTableSchema& opSchema, IErrorCollector& errors) const {
    THashSet<TString> usedObjects;
    for (const auto& proto : opSchema.GetStatistics()) {
        if (proto.GetName().empty()) {
            errors.AddError("Statistic cannot have an empty name");
            return false;
        }

        const TString& name = proto.GetName();
        if (!GetByNameOptional(name)) {
            errors.AddError("Stat '" + name + "' does not match schema preset");
            return false;
        }

        if (!usedObjects.emplace(proto.GetName()).second) {
            errors.AddError("Column '" + name + "' is specified multiple times");
            return false;
        }
    }
    return true;
}

}
