#pragma once
#include "defs.h"

namespace NKikimr::NColumnShard {

class TTtl {
public:
    static constexpr const ui64 DEFAULT_TTL_TIMEOUT_SEC = 60 * 60;
    static constexpr const ui64 DEFAULT_REPEAT_TTL_TIMEOUT_SEC = 10;

    struct TEviction {
        TString TierName;
        TDuration EvictAfter;
    };

    struct TDescription {
        TString ColumnName;
        std::vector<TEviction> Evictions;

        TDescription() = default;

        TDescription(const NKikimrSchemeOp::TColumnDataLifeCycle& ttl) {
            if (ttl.HasEnabled()) {
                auto& enabled = ttl.GetEnabled();
                ColumnName = enabled.GetColumnName();
                auto expireSec = TDuration::Seconds(enabled.GetExpireAfterSeconds());

                Evictions.reserve(1);
                Evictions.emplace_back(TEviction{{}, expireSec});
            }

            if (Enabled()) {
                Y_VERIFY(!ColumnName.empty());
            }
        }

        bool Enabled() const {
            return !Evictions.empty();
        }
    };

    ui64 PathsCount() const {
        return PathTtls.size();
    }

    void SetPathTtl(ui64 pathId, TDescription&& descr) {
        if (descr.Enabled()) {
            auto it = Columns.find(descr.ColumnName);
            if (it != Columns.end()) {
                descr.ColumnName = *it; // replace string dups (memory efficiency)
            } else {
                Columns.insert(descr.ColumnName);
            }
            PathTtls[pathId] = descr;
        } else {
            PathTtls.erase(pathId);
        }
    }

    void DropPathTtl(ui64 pathId) {
        PathTtls.erase(pathId);
    }

    THashMap<ui64, NOlap::TTiersInfo> MakeIndexTtlMap(TInstant now, bool force = false) {
        if ((now < LastRegularTtl + TtlTimeout) && !force) {
            return {};
        }

        THashMap<ui64, NOlap::TTiersInfo> out;
        for (auto& [pathId, descr] : PathTtls) {
            out.emplace(pathId, Convert(descr, now));
        }

        LastRegularTtl = now;
        return out;
    }

    void Repeat() {
        LastRegularTtl -= TtlTimeout;
        LastRegularTtl += RepeatTtlTimeout;
    }

    const THashSet<TString>& TtlColumns() const { return Columns; }

private:
    THashMap<ui64, TDescription> PathTtls; // pathId -> ttl
    THashSet<TString> Columns;
    TDuration TtlTimeout{TDuration::Seconds(DEFAULT_TTL_TIMEOUT_SEC)};
    TDuration RepeatTtlTimeout{TDuration::Seconds(DEFAULT_REPEAT_TTL_TIMEOUT_SEC)};
    TInstant LastRegularTtl;

    NOlap::TTiersInfo Convert(const TDescription& descr, TInstant timePoint) const {
        Y_VERIFY(descr.Enabled());
        NOlap::TTiersInfo out(descr.ColumnName);

        for (auto& tier : descr.Evictions) {
            auto border = timePoint - tier.EvictAfter;
            out.AddTier(tier.TierName, border);
        }

        return out;
    }
};

}
