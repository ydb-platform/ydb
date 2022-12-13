#pragma once
#include "defs.h"
#include "scalars.h"

namespace NKikimr::NOlap {

struct TCompression {
    arrow::Compression::type Codec{arrow::Compression::LZ4_FRAME};
    std::optional<int> Level;
};

struct TTierInfo {
    TString Name;
    TInstant EvictBorder;
    TString Column;
    std::optional<TCompression> Compression;

    TTierInfo(const TString& tierName, TInstant evictBorder, const TString& column)
        : Name(tierName)
        , EvictBorder(evictBorder)
        , Column(column)
    {
        Y_VERIFY(!Name.empty());
        Y_VERIFY(!Column.empty());
    }

    std::shared_ptr<arrow::Scalar> EvictTimestamp() const {
        if (Scalar) {
            return Scalar;
        }

        Scalar = std::make_shared<arrow::TimestampScalar>(
            EvictBorder.MicroSeconds(), arrow::timestamp(arrow::TimeUnit::MICRO));
        return Scalar;
    }

    static std::shared_ptr<TTierInfo> MakeTtl(TInstant ttlBorder, const TString& ttlColumn) {
        return std::make_shared<TTierInfo>("TTL", ttlBorder, ttlColumn);
    }

    TString GetDebugString() const {
        TStringBuilder sb;
        sb << "tier name '" << Name << "' border '" << EvictBorder << "' column '" << Column << "' "
            << arrow::util::Codec::GetCodecAsString(Compression ? Compression->Codec : TCompression().Codec)
            << ":" << ((Compression && Compression->Level) ?
                *Compression->Level : arrow::util::kUseDefaultCompressionLevel);
        return sb;
    }

private:
    mutable std::shared_ptr<arrow::Scalar> Scalar;
};

struct TTierRef {
    TTierRef(const std::shared_ptr<TTierInfo>& tierInfo)
        : Info(tierInfo)
    {
        Y_VERIFY(tierInfo);
    }

    bool operator < (const TTierRef& b) const {
        if (Info->EvictBorder < b.Info->EvictBorder) {
            return true;
        } else if (Info->EvictBorder == b.Info->EvictBorder) {
            return Info->Name > b.Info->Name; // add stability: smaller name is hotter
        }
        return false;
    }

    bool operator == (const TTierRef& b) const {
        return Info->EvictBorder == b.Info->EvictBorder
            && Info->Name == b.Info->Name;
    }

    const TTierInfo& Get() const {
        return *Info;
    }

private:
    std::shared_ptr<TTierInfo> Info;
};

struct TTiering {
    THashMap<TString, std::shared_ptr<TTierInfo>> TierByName;
    TSet<TTierRef> OrderedTiers; // Tiers ordered by border
    std::shared_ptr<TTierInfo> Ttl;

    static TTiering MakeTtl(TInstant ttlBorder, const TString& ttlColumn) {
        TTiering out;
        out.Ttl = TTierInfo::MakeTtl(ttlBorder, ttlColumn);
        return out;
    }

    bool Empty() const {
        return OrderedTiers.empty();
    }

    void Add(const std::shared_ptr<TTierInfo>& tier) {
        if (!Empty()) {
            Y_VERIFY(tier->Column == OrderedTiers.begin()->Get().Column); // TODO: support different ttl columns
        }

        TierByName.emplace(tier->Name, tier);
        OrderedTiers.emplace(tier);
    }

    TString GetHottestTierName() const {
        if (OrderedTiers.size()) {
            return OrderedTiers.rbegin()->Get().Name; // hottest one
        }
        return {};
    }

    std::shared_ptr<arrow::Scalar> ExpireTimestamp() const {
        auto ttlTs = Ttl ? Ttl->EvictTimestamp() : nullptr;
        auto tierTs = OrderedTiers.empty() ? nullptr : OrderedTiers.begin()->Get().EvictTimestamp();
        if (!ttlTs) {
            return tierTs;
        } else if (!tierTs) {
            return ttlTs;
        }
        return NArrow::ScalarLess(ttlTs, tierTs) ? tierTs : ttlTs; // either TTL or tier border appear
    }

    std::optional<TCompression> GetCompression(const TString& name) const {
        auto it = TierByName.find(name);
        if (it != TierByName.end()) {
            Y_VERIFY(!name.empty());
            return it->second->Compression;
        }
        return {};
    }

    THashSet<TString> GetTtlColumns() const {
        THashSet<TString> out;
        if (Ttl) {
            out.insert(Ttl->Column);
        }
        for (auto& [tierName, tier] : TierByName) {
            out.insert(tier->Column);
        }
        return out;
    }

    TString GetDebugString() const {
        TStringBuilder sb;
        if (Ttl) {
            sb << "ttl border '" << Ttl->EvictBorder << "' column '" << Ttl->Column << "'; ";
        }
        for (auto&& i : OrderedTiers) {
            sb << i.Get().GetDebugString() << "; ";
        }
        return sb;
    }
};

}
