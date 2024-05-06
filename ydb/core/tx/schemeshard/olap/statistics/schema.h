#pragma once
#include "update.h"

namespace NKikimr::NSchemeShard {

class TOlapSchema;

class TOlapStatisticsSchema {
private:
    YDB_READONLY_DEF(NOlap::NStatistics::TOperatorContainer, Operator);
public:
    TOlapStatisticsSchema() = default;

    TOlapStatisticsSchema(const NOlap::NStatistics::TOperatorContainer& container)
        : Operator(container)
    {
        AFL_VERIFY(container.GetName());
    }

    bool ApplyUpdate(const TOlapSchema& currentSchema, const TOlapStatisticsUpsert& upsert, IErrorCollector& errors);

    void SerializeToProto(NKikimrColumnShardStatisticsProto::TOperatorContainer& proto) const;
    bool DeserializeFromProto(const NKikimrColumnShardStatisticsProto::TOperatorContainer& proto);
};

class TOlapStatisticsDescription {
public:
    using TObjectsByName = THashMap<TString, TOlapStatisticsSchema>;

private:
    YDB_READONLY_DEF(TObjectsByName, ObjectsByName);
public:
    const TOlapStatisticsSchema* GetByIdOptional(const NOlap::NStatistics::EType type, const std::vector<ui32>& entityIds) const noexcept {
        for (auto&& i : ObjectsByName) {
            if (!i.second.GetOperator()) {
                continue;
            }
            if (i.second.GetOperator()->GetIdentifier() != NOlap::NStatistics::TIdentifier(type, entityIds)) {
                continue;
            }
            return &i.second;
        }
        return nullptr;
    }

    const TOlapStatisticsSchema* GetByNameOptional(const TString& name) const noexcept {
        auto it = ObjectsByName.find(name);
        if (it != ObjectsByName.end()) {
            return &it->second;
        }
        return nullptr;
    }

    const TOlapStatisticsSchema& GetByNameVerified(const TString& name) const noexcept {
        auto object = GetByNameOptional(name);
        AFL_VERIFY(object);
        return *object;
    }

    TOlapStatisticsSchema* MutableByNameOptional(const TString& name) noexcept {
        auto it = ObjectsByName.find(name);
        if (it != ObjectsByName.end()) {
            return &it->second;
        }
        return nullptr;
    }

    TOlapStatisticsSchema& MutableByNameVerified(const TString& name) noexcept {
        auto* object = MutableByNameOptional(name);
        AFL_VERIFY(object);
        return *object;
    }

    bool ApplyUpdate(const TOlapSchema& currentSchema, const TOlapStatisticsModification& schemaUpdate, IErrorCollector& errors);

    void Parse(const NKikimrSchemeOp::TColumnTableSchema& tableSchema);
    void Serialize(NKikimrSchemeOp::TColumnTableSchema& tableSchema) const;
    bool Validate(const NKikimrSchemeOp::TColumnTableSchema& opSchema, IErrorCollector& errors) const;
};
}
