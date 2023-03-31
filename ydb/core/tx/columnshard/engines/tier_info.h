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
    TString EvictColumnName;
    std::shared_ptr<arrow::Field> EvictColumn;
    std::optional<TCompression> Compression;
    ui32 TtlUnitsInSecond;
    bool NeedExport = false;

    TTierInfo(const TString& tierName, TInstant evictBorder, const TString& column, ui32 unitsInSecond = 0)
        : Name(tierName)
        , EvictBorder(evictBorder)
        , EvictColumnName(column)
        , TtlUnitsInSecond(unitsInSecond)
    {
        Y_VERIFY(!Name.empty());
        Y_VERIFY(!EvictColumnName.empty());
    }

    std::shared_ptr<arrow::Scalar> EvictScalar() const {
        if (Scalar) {
            return Scalar;
        }

        ui32 multiplier = TtlUnitsInSecond ? TtlUnitsInSecond : 1;

        Y_VERIFY(EvictColumn);
        switch (EvictColumn->type()->id()) {
            case arrow::Type::TIMESTAMP:
                Scalar = std::make_shared<arrow::TimestampScalar>(
                    EvictBorder.MicroSeconds(), arrow::timestamp(arrow::TimeUnit::MICRO));
                break;
            case arrow::Type::UINT16: // YQL Date
                Scalar = std::make_shared<arrow::UInt16Scalar>(EvictBorder.Days());
                break;
            case arrow::Type::UINT32: // YQL Datetime or Uint32
                Scalar = std::make_shared<arrow::UInt32Scalar>(EvictBorder.Seconds() * multiplier);
                break;
            case arrow::Type::UINT64:
                Scalar = std::make_shared<arrow::UInt64Scalar>(EvictBorder.Seconds() * multiplier);
                break;
            default:
                break;
        }

        return Scalar;
    }

    static std::shared_ptr<TTierInfo> MakeTtl(TInstant ttlBorder, const TString& ttlColumn, ui32 unitsInSecond = 0) {
        return std::make_shared<TTierInfo>("TTL", ttlBorder, ttlColumn, unitsInSecond);
    }

    TString GetDebugString() const {
        TStringBuilder sb;
        sb << "tier name '" << Name << "' border '" << EvictBorder << "' column '" << EvictColumnName << "' "
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

    bool HasTiers() const {
        return !OrderedTiers.empty();
    }

    void Add(const std::shared_ptr<TTierInfo>& tier) {
        if (HasTiers()) {
            // TODO: support different ttl columns
            Y_VERIFY(tier->EvictColumnName == OrderedTiers.begin()->Get().EvictColumnName);
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

    std::shared_ptr<arrow::Scalar> EvictScalar() const {
        auto ttlTs = Ttl ? Ttl->EvictScalar() : nullptr;
        auto tierTs = OrderedTiers.empty() ? nullptr : OrderedTiers.begin()->Get().EvictScalar();
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

    bool NeedExport(const TString& name) const {
        auto it = TierByName.find(name);
        if (it != TierByName.end()) {
            Y_VERIFY(!name.empty());
            return it->second->NeedExport;
        }
        return false;
    }

    THashSet<TString> GetTtlColumns() const {
        THashSet<TString> out;
        if (Ttl) {
            out.insert(Ttl->EvictColumnName);
        }
        for (auto& [tierName, tier] : TierByName) {
            out.insert(tier->EvictColumnName);
        }
        return out;
    }

    TString GetDebugString() const {
        TStringBuilder sb;
        if (Ttl) {
            sb << Ttl->GetDebugString() << "; ";
        }
        for (auto&& i : OrderedTiers) {
            sb << i.Get().GetDebugString() << "; ";
        }
        return sb;
    }
};

}
