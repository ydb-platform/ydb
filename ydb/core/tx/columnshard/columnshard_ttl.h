#pragma once
#include "defs.h"

namespace NKikimr::NColumnShard {

class TTtl {
public:
    static constexpr const ui64 DEFAULT_TTL_TIMEOUT_SEC = 60 * 60;
    static constexpr const ui64 DEFAULT_REPEAT_TTL_TIMEOUT_SEC = 10;

    struct TEviction {
        TDuration EvictAfter;
        TString ColumnName;
        ui32 UnitsInSecond = 0; // 0 means auto (data type specific)
    };

    struct TDescription {
        std::optional<TEviction> Eviction;

        TDescription() = default;

        TDescription(const NKikimrSchemeOp::TColumnDataLifeCycle::TTtl& ttl) {
            auto expireSec = TDuration::Seconds(ttl.GetExpireAfterSeconds());

            Eviction = TEviction{expireSec, ttl.GetColumnName()};
            Y_VERIFY(!Eviction->ColumnName.empty());

            switch (ttl.GetColumnUnit()) {
                case NKikimrSchemeOp::TTTLSettings::UNIT_SECONDS:
                    Eviction->UnitsInSecond = 1;
                    break;
                case NKikimrSchemeOp::TTTLSettings::UNIT_MILLISECONDS:
                    Eviction->UnitsInSecond = 1000;
                    break;
                case NKikimrSchemeOp::TTTLSettings::UNIT_MICROSECONDS:
                    Eviction->UnitsInSecond = 1000 * 1000;
                    break;
                case NKikimrSchemeOp::TTTLSettings::UNIT_NANOSECONDS:
                    Eviction->UnitsInSecond = 1000 * 1000 * 1000;
                    break;
                case NKikimrSchemeOp::TTTLSettings::UNIT_AUTO:
                default:
                    break;
            }
        }
    };

    ui64 PathsCount() const {
        return PathTtls.size();
    }

    void SetPathTtl(ui64 pathId, TDescription&& descr) {
        if (descr.Eviction) {
            auto& evict = descr.Eviction;
            auto it = Columns.find(evict->ColumnName);
            if (it != Columns.end()) {
                evict->ColumnName = *it; // replace string dups (memory efficiency)
            } else {
                Columns.insert(evict->ColumnName);
            }
            PathTtls[pathId] = descr;
        } else {
            PathTtls.erase(pathId);
        }
    }

    void DropPathTtl(ui64 pathId) {
        PathTtls.erase(pathId);
    }

    void AddTtls(THashMap<ui64, NOlap::TTiering>& eviction, TInstant now, bool force = false) {
        if ((now < LastRegularTtl + TtlTimeout) && !force) {
            return;
        }

        for (auto& [pathId, descr] : PathTtls) {
            eviction[pathId].Ttl = Convert(descr, now);
        }

        LastRegularTtl = now;
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

    std::shared_ptr<NOlap::TTierInfo> Convert(const TDescription& descr, TInstant timePoint) const
    {
        if (descr.Eviction) {
            auto& evict = descr.Eviction;
            TInstant border = timePoint - evict->EvictAfter;
            return NOlap::TTierInfo::MakeTtl(border, evict->ColumnName, evict->UnitsInSecond);
        }
        return {};
    }
};

}
