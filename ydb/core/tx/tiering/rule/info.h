#include <ydb/core/protos/flat_scheme_op.pb.h>

#include <ydb/library/accessor/accessor.h>

#include <util/datetime/base.h>
#include <util/generic/fwd.h>

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

class TTieringRuleInfo {
private:
    YDB_ACCESSOR_DEF(TString, DefaultColumn);
    YDB_ACCESSOR_DEF(std::vector<TTieringInterval>, Intervals);

public:
    using TProto = NKikimrSchemeOp::TTieringRuleProperties;

    bool DeserializeFromProto(const TProto& proto) {
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

    bool ApplyPatch(const TProto& proto) {
        if (proto.HasDefaultColumn()) {
            DefaultColumn = proto.GetDefaultColumn();
        }

        if (proto.HasTiers()) {
            Intervals.clear();
            for (const auto& interval : proto.GetTiers().GetIntervals()) {
                Intervals.emplace_back();
                if (!Intervals.back().DeserializeFromProto(interval)) {
                    return false;
                }
            }
        }

        return true;
    }

    TProto SerializeToProto() const {
        TProto serialized;

        serialized.SetDefaultColumn(DefaultColumn);
        auto* intervalsProto = serialized.MutableTiers()->MutableIntervals();
        for (const auto& interval : Intervals) {
            *intervalsProto->Add() = interval.SerializeToProto();
        }

        return serialized;
    }

    static TString GetTypeId() {
        return "TIERING_RULE";
    }
};

}   // namespace NKikimr::NColumnShard::NTiers
