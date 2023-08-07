#pragma once

#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/formats/arrow/common/validation.h>
#include <ydb/core/formats/arrow/serializer/abstract.h>
#include <ydb/core/formats/arrow/compression/object.h>
#include <ydb/core/tx/columnshard/common/scalars.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/util/compression.h>
#include <util/generic/set.h>
#include <util/generic/hash_set.h>

namespace NKikimr::NOlap {

class TTierInfo {
private:
    TString Name;
    TString EvictColumnName;
    TDuration EvictDuration;
    bool NeedExport = false;

    ui32 TtlUnitsInSecond;
    std::optional<NArrow::TCompression> Compression;
public:
    TTierInfo(const TString& tierName, TDuration evictDuration, const TString& column, ui32 unitsInSecond = 0)
        : Name(tierName)
        , EvictColumnName(column)
        , EvictDuration(evictDuration)
        , TtlUnitsInSecond(unitsInSecond)
    {
        Y_VERIFY(!!Name);
        Y_VERIFY(!!EvictColumnName);
    }

    const TString& GetName() const {
        return Name;
    }

    const TString& GetEvictColumnName() const {
        return EvictColumnName;
    }

    TInstant GetEvictInstant(const TInstant now) const {
        return now - EvictDuration;
    }

    TDuration GetEvictDuration() const {
        return EvictDuration;
    }

    bool GetNeedExport() const {
        return NeedExport;
    }

    TTierInfo& SetNeedExport(const bool value) {
        NeedExport = value;
        return *this;
    }

    TTierInfo& SetCompression(const NArrow::TCompression& value) {
        Compression = value;
        return *this;
    }

    const std::optional<NArrow::TCompression> GetCompression() const {
        if (NeedExport) {
            return {};
        }
        return Compression;
    }

    std::shared_ptr<arrow::Field> GetEvictColumn(const std::shared_ptr<arrow::Schema>& schema) const {
        return schema->GetFieldByName(EvictColumnName);
    }

    std::optional<TInstant> ScalarToInstant(const std::shared_ptr<arrow::Scalar>& scalar) const;

    static std::shared_ptr<TTierInfo> MakeTtl(const TDuration evictDuration, const TString& ttlColumn, ui32 unitsInSecond = 0) {
        return std::make_shared<TTierInfo>("TTL", evictDuration, ttlColumn, unitsInSecond);
    }

    TString GetDebugString() const {
        TStringBuilder sb;
        sb << "name=" << Name << ";duration=" << EvictDuration << ";column=" << EvictColumnName << ";compression=";
        if (Compression) {
            sb << Compression->DebugString();
        } else {
            sb << "NOT_SPECIFIED(Default)";
        }
        sb << ";";
        return sb;
    }
};

class TTierRef {
public:
    TTierRef(const std::shared_ptr<TTierInfo>& tierInfo)
        : Info(tierInfo)
    {
        Y_VERIFY(tierInfo);
    }

    bool operator < (const TTierRef& b) const {
        if (Info->GetEvictDuration() > b.Info->GetEvictDuration()) {
            return true;
        } else if (Info->GetEvictDuration() == b.Info->GetEvictDuration()) {
            return Info->GetName() > b.Info->GetName(); // add stability: smaller name is hotter
        }
        return false;
    }

    bool operator == (const TTierRef& b) const {
        return Info->GetEvictDuration() == b.Info->GetEvictDuration()
            && Info->GetName() == b.Info->GetName();
    }

    const TTierInfo& Get() const {
        return *Info;
    }

    std::shared_ptr<TTierInfo> GetPtr() const {
        return Info;
    }

private:
    std::shared_ptr<TTierInfo> Info;
};

class TTiering {
    using TTiersMap = THashMap<TString, std::shared_ptr<TTierInfo>>;
    TTiersMap TierByName;
    TSet<TTierRef> OrderedTiers;
public:

    std::shared_ptr<TTierInfo> GetMainTierInfo() const;

    std::shared_ptr<TTierInfo> Ttl;

    const TTiersMap& GetTierByName() const {
        return TierByName;
    }

    const TSet<TTierRef>& GetOrderedTiers() const {
        return OrderedTiers;
    }

    bool HasTiers() const {
        return !OrderedTiers.empty();
    }

    void Add(const std::shared_ptr<TTierInfo>& tier) {
        if (HasTiers()) {
            // TODO: support different ttl columns
            Y_VERIFY(tier->GetEvictColumnName() == OrderedTiers.begin()->Get().GetEvictColumnName());
        }

        TierByName.emplace(tier->GetName(), tier);
        OrderedTiers.emplace(tier);
    }

    TString GetHottestTierName() const {
        if (OrderedTiers.size()) {
            return OrderedTiers.rbegin()->Get().GetName(); // hottest one
        }
        return {};
    }

    std::optional<TInstant> ScalarToInstant(const std::shared_ptr<arrow::Scalar>& scalar) const {
        auto mainTier = GetMainTierInfo();
        if (!mainTier) {
            return {};
        } else {
            return mainTier->ScalarToInstant(scalar);
        }
    }

    std::optional<TInstant> GetEvictInstant(const TInstant now) const {
        auto mainTier = GetMainTierInfo();
        if (!mainTier) {
            return {};
        } else {
            return mainTier->GetEvictInstant(now);
        }
    }

    std::optional<NArrow::TCompression> GetCompression(const TString& name) const {
        auto it = TierByName.find(name);
        if (it != TierByName.end()) {
            Y_VERIFY(!name.empty());
            return it->second->GetCompression();
        }
        return {};
    }

    bool NeedExport(const TString& name) const {
        auto it = TierByName.find(name);
        if (it != TierByName.end()) {
            Y_VERIFY(!name.empty());
            return it->second->GetNeedExport();
        }
        return false;
    }

    THashSet<TString> GetTtlColumns() const {
        THashSet<TString> out;
        if (Ttl) {
            out.insert(Ttl->GetEvictColumnName());
        }
        for (auto& [tierName, tier] : TierByName) {
            out.insert(tier->GetEvictColumnName());
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
