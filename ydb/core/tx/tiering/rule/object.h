#pragma once
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/tx/columnshard/engines/scheme/tier_info.h>
#include <ydb/core/tx/schemeshard/operations/metadata/properties.h>

#include <ydb/services/metadata/abstract/decoder.h>
#include <ydb/services/metadata/service.h>
#include <ydb/services/metadata/manager/object.h>
#include <ydb/services/metadata/manager/preparation_controller.h>

#include <library/cpp/json/writer/json_value.h>

namespace NKikimr::NColumnShard::NTiers {

class TTieringInterval {
private:
    YDB_ACCESSOR_DEF(TString, TierName);
    YDB_ACCESSOR_DEF(TDuration, DurationForEvict);

public:
    using TProto = NKikimrSchemeOp::TTieringIntervals::TTieringInterval;

    TTieringInterval() = default;
    TTieringInterval(const TString& name, const TDuration d)
        : TierName(name)
        , DurationForEvict(d) {
    }

    bool operator<(const TTieringInterval& item) const {
        return DurationForEvict < item.DurationForEvict;
    }

    NJson::TJsonValue SerializeToJson() const {
        NJson::TJsonValue result;
        result.InsertValue("tierName", TierName);
        result.InsertValue("durationForEvict", DurationForEvict.ToString());
        return result;
    }

    bool DeserializeFromJson(const NJson::TJsonValue& jsonInfo) {
        if (!jsonInfo["tierName"].GetString(&TierName)) {
            return false;
        }
        const TString dStr = jsonInfo["durationForEvict"].GetStringRobust();
        if (!TDuration::TryParse(dStr, DurationForEvict)) {
            return false;
        }
        return true;
    }

    bool DeserializeFromProto(const NKikimrSchemeOp::TTieringIntervals::TTieringInterval& proto) {
        TierName = proto.GetTierName();
        DurationForEvict = TDuration::MilliSeconds(proto.GetEvictionDelayMs());
        return true;
    }

    TProto SerializeToProto() const {
        TProto serialized;
        serialized.SetTierName(TierName);
        serialized.SetEvictionDelayMs(DurationForEvict.MilliSeconds());
        return serialized;
    }
};

class TTieringRule: public NSchemeShard::TMetadataObjectPropertiesBase<TTieringRule>, public NMetadata::NModifications::TObject<TTieringRule> {
private:
    YDB_ACCESSOR_DEF(TString, TieringRuleId);
    YDB_ACCESSOR_DEF(TString, DefaultColumn);
    YDB_ACCESSOR_DEF(std::vector<TTieringInterval>, Intervals);
protected:
    NJson::TJsonValue SerializeDescriptionToJson() const;
    bool DeserializeDescriptionFromJson(const NJson::TJsonValue& jsonInfo);

    static TFactory::TRegistrator<TTieringRule> Registrator;

private:
    static const NKikimrSchemeOp::TTieringRuleProperties& GetProperties(const TProto& proto) {
        return proto.GetTieringRule();
    }
    static NKikimrSchemeOp::TTieringRuleProperties* MutableProperties(TProto& proto) {
        return proto.MutableTieringRule();
    }

public:
    static NMetadata::IClassBehaviour::TPtr GetBehaviour();

    bool ContainsTier(const TString& tierName) const;

    void AddInterval(const TString& name, const TDuration evDuration) {
        Intervals.emplace_back(TTieringInterval(name, evDuration));
    }

    bool DeserializeFromProto(const TProto& proto) override {
        const auto& tieringRule = GetProperties(proto);

        DefaultColumn = tieringRule.GetDefaultColumn();

        Intervals.clear();
        for (const auto& interval : tieringRule.GetTiers().GetIntervals()) {
            Intervals.emplace_back();
            if (!Intervals.back().DeserializeFromProto(interval)) {
                return false;
            }
        }
        return true;
    }

    bool ApplyPatch(const TProto& proto) override {
        const auto& tieringRule = GetProperties(proto);

        if (tieringRule.HasDefaultColumn()) {
            DefaultColumn = tieringRule.GetDefaultColumn();
        }

        if (tieringRule.HasTiers()) {
            Intervals.clear();
            for (const auto& interval : tieringRule.GetTiers().GetIntervals()) {
                Intervals.emplace_back();
                if (!Intervals.back().DeserializeFromProto(interval)) {
                    return false;
                }
            }
        }

        return true;
    }

    TProto SerializeToProto() const override {
        TProto serialized;
        auto* tieringRule = MutableProperties(serialized);

        tieringRule->SetDefaultColumn(DefaultColumn);
        auto* intervalsProto = tieringRule->MutableTiers()->MutableIntervals();
        for (const auto& interval : Intervals) {
            *intervalsProto->Add() = interval.SerializeToProto();
        }

        return serialized;
    }

    static TString GetTypeId() {
        return "TIERING_RULE";
    }

    NJson::TJsonValue GetDebugJson() const;

    class TDecoder: public NMetadata::NInternal::TDecoderBase {
    private:
        YDB_READONLY(i32, TieringRuleIdIdx, -1);
        YDB_READONLY(i32, DefaultColumnIdx, -1);
        YDB_READONLY(i32, DescriptionIdx, -1);
    public:
        static inline const TString TieringRuleId = "tieringRuleId";
        static inline const TString DefaultColumn = "defaultColumn";
        static inline const TString Description = "description";

        TDecoder(const Ydb::ResultSet& rawData) {
            TieringRuleIdIdx = GetFieldIndex(rawData, TieringRuleId);
            DefaultColumnIdx = GetFieldIndex(rawData, DefaultColumn);
            DescriptionIdx = GetFieldIndex(rawData, Description);
        }
    };
    NMetadata::NInternal::TTableRecord SerializeToRecord() const;
    bool DeserializeFromRecord(const TDecoder& decoder, const Ydb::Value& r);
    NKikimr::NOlap::TTiering BuildOlapTiers() const;
};
}
