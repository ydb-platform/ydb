#pragma once
#include "decoder.h"

#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/tx/columnshard/engines/column_engine.h>
#include <ydb/services/metadata/service.h>

#include <library/cpp/json/writer/json_value.h>

namespace NKikimr::NColumnShard::NTiers {

class TTieringRule {
private:
    YDB_ACCESSOR_DEF(TString, OwnerPath);
    YDB_ACCESSOR_DEF(TString, TierName);
    YDB_ACCESSOR_DEF(TString, TablePath);
    YDB_ACCESSOR(ui64, TablePathId, 0);
    YDB_ACCESSOR_DEF(TString, Column);
    YDB_ACCESSOR_DEF(TDuration, DurationForEvict);
public:
    bool operator<(const TTieringRule& item) const {
        return std::tie(OwnerPath, TierName, TablePath, Column, DurationForEvict)
            < std::tie(item.OwnerPath, item.TierName, item.TablePath, item.Column, item.DurationForEvict);
    }

    NJson::TJsonValue GetDebugJson() const;
    TString GetTierId() const {
        return OwnerPath + "." + TierName;
    }
    class TDecoder: public NInternal::TDecoderBase {
    private:
        YDB_READONLY(i32, OwnerPathIdx, -1);
        YDB_READONLY(i32, TablePathIdx, -1);
        YDB_READONLY(i32, DurationForEvictIdx, -1);
        YDB_READONLY(i32, TierNameIdx, -1);
        YDB_READONLY(i32, ColumnIdx, -1);
    public:
        static inline const TString OwnerPath = "ownerPath";
        static inline const TString TierName = "tierName";
        static inline const TString TablePath = "tablePath";
        static inline const TString DurationForEvict = "durationForEvict";
        static inline const TString Column = "column";

        TDecoder(const Ydb::ResultSet& rawData) {
            OwnerPathIdx = GetFieldIndex(rawData, OwnerPath);
            TierNameIdx = GetFieldIndex(rawData, TierName);
            TablePathIdx = GetFieldIndex(rawData, TablePath);
            DurationForEvictIdx = GetFieldIndex(rawData, DurationForEvict);
            ColumnIdx = GetFieldIndex(rawData, Column);
        }
    };
    bool DeserializeFromRecord(const TDecoder& decoder, const Ydb::Value& r) {
        if (!decoder.Read(decoder.GetOwnerPathIdx(), OwnerPath, r)) {
            return false;
        }
        if (!decoder.Read(decoder.GetTierNameIdx(), TierName, r)) {
            return false;
        }
        if (!decoder.Read(decoder.GetTablePathIdx(), TablePath, r)) {
            return false;
        }
        TablePath = TFsPath(TablePath).Fix().GetPath();
        if (!decoder.Read(decoder.GetDurationForEvictIdx(), DurationForEvict, r)) {
            return false;
        }
        if (!decoder.Read(decoder.GetColumnIdx(), Column, r)) {
            return false;
        }
        return true;
    }
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
    void AddRule(TTieringRule&& tr) {
        if (Rules.size()) {
            Y_VERIFY(Rules.back().GetDurationForEvict() <= tr.GetDurationForEvict());
            if (Column != tr.GetColumn()) {
                ALS_ERROR(NKikimrServices::TX_COLUMNSHARD) << "inconsistency rule column: " <<
                    TablePath << "/" << Column << " != " << tr.GetColumn();
                return;
            }
        } else {
            Column = tr.GetColumn();
            TablePath = tr.GetTablePath();
            TablePathId = tr.GetTablePathId();
        }
        Rules.emplace_back(std::move(tr));
    }

    NOlap::TTiersInfo BuildTiersInfo() const {
        NOlap::TTiersInfo result(GetColumn());
        for (auto&& r : Rules) {
            result.AddTier(r.GetTierName(), Now() - r.GetDurationForEvict());
        }
        return result;
    }
};

}
