#pragma once 
#include "defs.h" 
 
namespace NKikimr::NColumnShard { 
 
class TTtl { 
public: 
    static constexpr const ui64 DEFAULT_TTL_TIMEOUT_SEC = 60 * 60; 
 
    struct TEviction { 
        TString TierName; 
        TString ColumnName; 
        TDuration ExpireAfter; 
    }; 
 
    struct TDescription { 
        std::vector<TEviction> Evictions; 
 
        TDescription() = default; 
 
        TDescription(const NKikimrSchemeOp::TColumnDataLifeCycle& ttl) { 
            if (ttl.HasEnabled()) { 
                auto& enabled = ttl.GetEnabled(); 
                Evictions.emplace_back( 
                    TEviction{{}, enabled.GetColumnName(), TDuration::Seconds(enabled.GetExpireAfterSeconds())}); 
            } else if (ttl.HasTiering()) { 
                for (auto& tier : ttl.GetTiering().GetTiers()) { 
                    auto& eviction = tier.GetEviction(); 
                    Evictions.emplace_back(TEviction{tier.GetName(), 
                        eviction.GetColumnName(), TDuration::Seconds(eviction.GetExpireAfterSeconds())}); 
                } 
            } 
        } 
 
        bool Enabled() const { 
            return !Evictions.empty(); 
        } 
 
        const TEviction& LastTier() const { 
            Y_VERIFY(!Evictions.empty()); 
            return Evictions.back(); 
        } 
    }; 
 
    ui64 PathsCount() const { 
        return PathTtls.size(); 
    } 
 
    void SetPathTtl(ui64 pathId, TDescription&& descr) { 
        if (descr.Enabled()) { 
            for (auto& evict : descr.Evictions) { 
                if (!evict.ColumnName.empty()) { 
                    Columns.insert(evict.ColumnName); 
                } 
            } 
            PathTtls[pathId] = descr; 
        } else { 
            PathTtls.erase(pathId); 
        } 
    } 
 
    void DropPathTtl(ui64 pathId) { 
        PathTtls.erase(pathId); 
    } 
 
    THashMap<ui64, NOlap::TTtlInfo> MakeIndexTtlMap(bool force = false) { 
        if ((TInstant::Now() < LastRegularTtl + RegularTtlTimeout) && !force) { 
            return {}; 
        } 
 
        THashMap<ui64, NOlap::TTtlInfo> out; 
        for (auto& [pathId, descr] : PathTtls) { 
            out.emplace(pathId, Convert(descr)); 
        } 
 
        LastRegularTtl = TInstant::Now(); 
        return out; 
    } 
 
    const THashSet<TString>& TtlColumns() const { return Columns; } 
 
private: 
    THashMap<ui64, TDescription> PathTtls; // pathId -> ttl 
    THashSet<TString> Columns; 
    TDuration RegularTtlTimeout{TDuration::Seconds(DEFAULT_TTL_TIMEOUT_SEC)}; 
    TInstant LastRegularTtl; 
 
    NOlap::TTtlInfo Convert(const TDescription& descr) const { 
        auto& lastTier = descr.LastTier(); 
        ui64 border = (TInstant::Now() - lastTier.ExpireAfter).MicroSeconds(); 
        return NOlap::TTtlInfo{ 
            .Column = lastTier.ColumnName, 
            .Border = std::make_shared<arrow::TimestampScalar>(border, arrow::timestamp(arrow::TimeUnit::MICRO)) 
        }; 
    } 
}; 
 
} 
