#pragma once
#include <ydb/core/tx/columnshard/engines/scheme/tier_info.h>
#include <ydb/core/tx/schemeshard/operations/metadata/properties.h>

#include <ydb/services/metadata/service.h>

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

class TTieringRule: public NSchemeShard::IMetadataObjectProperties {
private:
    YDB_ACCESSOR_DEF(TString, DefaultColumn);
    YDB_ACCESSOR_DEF(std::vector<TTieringInterval>, Intervals);

    static TFactory::TRegistrator<TTieringRule> Registrator;

private:
    static const NKikimrSchemeOp::TTieringRuleProperties& GetProperties(const TProto& proto) {
        return proto.GetTieringRule();
    }
    static NKikimrSchemeOp::TTieringRuleProperties* MutableProperties(TProto& proto) {
        return proto.MutableTieringRule();
    }

public:
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

    static NMetadata::IClassBehaviour::TPtr GetBehaviour();

    bool ContainsTier(const TString& tierName) const;
    NKikimr::NOlap::TTiering BuildOlapTiers() const;
};
}
