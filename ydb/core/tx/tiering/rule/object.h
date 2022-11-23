#pragma once
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/tx/columnshard/engines/column_engine.h>
#include <ydb/core/tx/tiering/common/global_tier_id.h>
#include <ydb/services/metadata/abstract/decoder.h>
#include <ydb/services/metadata/service.h>
#include <ydb/services/metadata/manager/object.h>
#include <ydb/services/metadata/manager/preparation_controller.h>

#include <library/cpp/json/writer/json_value.h>

namespace NKikimr::NColumnShard::NTiers {

class TTieringRule: public NMetadataManager::TObject<TTieringRule> {
private:
    YDB_ACCESSOR_DEF(TString, TieringRuleId);
    YDB_ACCESSOR_DEF(TString, OwnerPath);
    YDB_ACCESSOR_DEF(TString, TierName);
    YDB_ACCESSOR_DEF(TString, TablePath);
    YDB_ACCESSOR(ui64, TablePathId, 0);
    YDB_ACCESSOR_DEF(TString, Column);
    YDB_ACCESSOR_DEF(TDuration, DurationForEvict);
public:
    static TString GetTypeId() {
        return "TIERING_RULE";
    }
    TGlobalTierId GetGlobalTierId() const {
        return TGlobalTierId(OwnerPath, TierName);
    }

    static TString GetStorageTablePath();
    static void AlteringPreparation(std::vector<TTieringRule>&& objects,
        NMetadataManager::IAlterPreparationController<TTieringRule>::TPtr controller,
        const NMetadata::IOperationsManager::TModificationContext& context);

    bool operator<(const TTieringRule& item) const {
        return std::tie(TablePath, TierName, Column, DurationForEvict, TieringRuleId)
            < std::tie(item.TablePath, item.TierName, item.Column, item.DurationForEvict, item.TieringRuleId);
    }

    NJson::TJsonValue GetDebugJson() const;

    class TDecoder: public NInternal::TDecoderBase {
    private:
        YDB_READONLY(i32, TieringRuleIdIdx, -1);
        YDB_READONLY(i32, OwnerPathIdx, -1);
        YDB_READONLY(i32, TablePathIdx, -1);
        YDB_READONLY(i32, DurationForEvictIdx, -1);
        YDB_READONLY(i32, TierNameIdx, -1);
        YDB_READONLY(i32, ColumnIdx, -1);
    public:
        static inline const TString TieringRuleId = "tieringRuleId";
        static inline const TString OwnerPath = "ownerPath";
        static inline const TString TierName = "tierName";
        static inline const TString TablePath = "tablePath";
        static inline const TString DurationForEvict = "durationForEvict";
        static inline const TString Column = "column";

        static std::vector<Ydb::Column> GetPKColumns();
        static std::vector<Ydb::Column> GetColumns();
        static std::vector<TString> GetPKColumnIds();

        TDecoder(const Ydb::ResultSet& rawData) {
            TieringRuleIdIdx = GetFieldIndex(rawData, TieringRuleId);
            OwnerPathIdx = GetFieldIndex(rawData, OwnerPath);
            TierNameIdx = GetFieldIndex(rawData, TierName);
            TablePathIdx = GetFieldIndex(rawData, TablePath);
            DurationForEvictIdx = GetFieldIndex(rawData, DurationForEvict);
            ColumnIdx = GetFieldIndex(rawData, Column);
        }
    };
    NKikimr::NMetadataManager::TTableRecord SerializeToRecord() const;
    bool DeserializeFromRecord(const TDecoder& decoder, const Ydb::Value& r);
    static NMetadata::TOperationParsingResult BuildPatchFromSettings(const NYql::TObjectSettingsImpl& settings,
        const NMetadata::IOperationsManager::TModificationContext& context);
};

class TTableTiering {
private:
    YDB_READONLY_DEF(TString, TablePath);
    YDB_READONLY(ui64, TablePathId, 0);
    YDB_READONLY_DEF(TString, Column);
    YDB_READONLY_DEF(TVector<TTieringRule>, Rules);
public:
    void SetTablePathId(const ui64 pathId) {
        Y_VERIFY(!TablePathId);
        TablePathId = pathId;
        for (auto&& r : Rules) {
            r.SetTablePathId(pathId);
        }
    }
    NJson::TJsonValue GetDebugJson() const;
    void AddRule(TTieringRule&& tr);

    NOlap::TTiersInfo BuildTiersInfo() const;
};

}
