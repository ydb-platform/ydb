#pragma once
#include "update.h"

#include <util/generic/hash.h>

namespace NKikimr::NSchemeShard {

class TOlapSchema;

class TOlapMultiColumnStatisticsSchema {
private:
    YDB_READONLY_DEF(TString, Name);
    YDB_READONLY_DEF(TVector<TString>, ColumnNames);
    YDB_READONLY_DEF(TVector<ui32>, ColumnIds);
    YDB_READONLY_DEF(TVector<NKikimrSchemeOp::EMultiColumnStatisticsType>, Types);
public:
    TOlapMultiColumnStatisticsSchema() = default;

    void SerializeToProto(NKikimrSchemeOp::TMultiColumnStatisticsDescription& proto) const;
    void DeserializeFromProto(const NKikimrSchemeOp::TMultiColumnStatisticsDescription& proto);
    bool ApplyUpsert(const TOlapSchema& currentSchema, const TOlapMultiColumnStatisticsUpsert& upsert, IErrorCollector& errors);
};

class TOlapMultiColumnStatisticsDescription {
public:
    using TMultiColumnStatistics = THashMap<TString, TOlapMultiColumnStatisticsSchema>;

private:
    YDB_READONLY_DEF(TMultiColumnStatistics, MultiColumnStatisticsByName);
public:
    const TOlapMultiColumnStatisticsSchema* GetByName(const TString& name) const noexcept {
        auto it = MultiColumnStatisticsByName.find(name);
        if (it != MultiColumnStatisticsByName.end()) {
            return &it->second;
        }
        return nullptr;
    }

    bool ApplyUpdate(const TOlapSchema& currentSchema, const TOlapMultiColumnStatisticsUpdate& schemaUpdate, IErrorCollector& errors);

    void Parse(const NKikimrSchemeOp::TColumnTableSchema& tableSchema);
    void Serialize(NKikimrSchemeOp::TColumnTableSchema& tableSchema) const;
    bool ValidateForStore(const NKikimrSchemeOp::TColumnTableSchema& opSchema, IErrorCollector& errors) const;
};
}
