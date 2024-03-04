#pragma once
#include "defs.h"

namespace NKikimr::NColumnShard {

class TTtl {
public:
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
            Y_ABORT_UNLESS(!Eviction->ColumnName.empty());

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

    bool AddTtls(THashMap<ui64, NOlap::TTiering>& eviction) const {
        for (auto& [pathId, descr] : PathTtls) {
            if (!eviction[pathId].Add(Convert(descr))) {
                return false;
            }
        }
        return true;
    }

    const THashSet<TString>& TtlColumns() const { return Columns; }

private:
    THashMap<ui64, TDescription> PathTtls; // pathId -> ttl
    THashSet<TString> Columns;

    std::shared_ptr<NOlap::TTierInfo> Convert(const TDescription& descr) const
    {
        if (descr.Eviction) {
            auto& evict = descr.Eviction;
            return NOlap::TTierInfo::MakeTtl(evict->EvictAfter, evict->ColumnName, evict->UnitsInSecond);
        }
        return {};
    }
};

}
