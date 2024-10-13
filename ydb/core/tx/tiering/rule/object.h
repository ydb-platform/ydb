#pragma once
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/tx/columnshard/engines/scheme/tier_info.h>

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
    TTieringInterval() = default;
    TTieringInterval(const TString& name, const TDuration d)
        : TierName(name)
        , DurationForEvict(d)
    {

    }

    bool operator<(const TTieringInterval& item) const {
        return DurationForEvict < item.DurationForEvict;
    }

    bool DeserializeFromProto(const NKikimrSchemeOp::TTieringIntervals::TTieringInterval& proto) {
        TierName = proto.GetTierName();
        DurationForEvict = TDuration::MilliSeconds(proto.GetEvictionDelayMs());
        return true;
    }

};

class TTieringRule {
private:
    YDB_ACCESSOR_DEF(TString, DefaultColumn);
    YDB_ACCESSOR_DEF(std::vector<TTieringInterval>, Intervals);

public:
    static NMetadata::IClassBehaviour::TPtr GetBehaviour();

    bool DeserializeFromProto(const NKikimrSchemeOp::TTieringRuleProperties& proto) {
        DefaultColumn = proto.GetDefaultColumn();

        Intervals.clear();
        for (const auto& interval : proto.GetTiers().GetIntervals()) {
            Intervals.emplace_back();
            if (!Intervals.back().DeserializeFromProto(interval)) {
                return false;
            }
        }
        return true;
    }

    static TString GetTypeId() {
        return "TIERING_RULE";
    }

    bool ContainsTier(const TString& tierName) const;
    NKikimr::NOlap::TTiering BuildOlapTiers() const;
};

}
