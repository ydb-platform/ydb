#pragma once

#include "defs.h"
#include "scalars.h"

#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/formats/arrow/common/validation.h>
#include <ydb/core/formats/arrow/serializer/abstract.h>
#include <ydb/core/formats/arrow/compression/object.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/util/compression.h>

namespace NKikimr::NOlap {

class TTierInfo {
private:
    TString Name;
    TString EvictColumnName;
    TInstant EvictBorder;
    bool NeedExport = false;

    ui32 TtlUnitsInSecond;
    std::optional<NArrow::TCompression> Compression;
    mutable std::shared_ptr<arrow::Scalar> Scalar;

public:
    TTierInfo(const TString& tierName, TInstant evictBorder, const TString& column, ui32 unitsInSecond = 0)
        : Name(tierName)
        , EvictColumnName(column)
        , EvictBorder(evictBorder)
        , TtlUnitsInSecond(unitsInSecond)
    {
        Y_VERIFY(!Name.empty());
        Y_VERIFY(!EvictColumnName.empty());
    }

    const TString& GetName() const {
        return Name;
    }

    const TString& GetEvictColumnName() const {
        return EvictColumnName;
    }

    const TInstant GetEvictBorder() const {
        return EvictBorder;
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

    std::shared_ptr<arrow::Scalar> EvictScalar(const std::shared_ptr<arrow::Schema>& schema) const {
        if (Scalar) {
            return Scalar;
        }
        auto evictColumn = GetEvictColumn(schema);
        Y_VERIFY(evictColumn);

        ui32 multiplier = TtlUnitsInSecond ? TtlUnitsInSecond : 1;
        switch (evictColumn->type()->id()) {
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
        sb << "tier name '" << Name << "' border '" << EvictBorder << "' column '" << EvictColumnName << "' ";
        if (Compression) {
            sb << Compression->DebugString();
        } else {
            sb << "NOT_SPECIFIED(Default)";
        }
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
        if (Info->GetEvictBorder() < b.Info->GetEvictBorder()) {
            return true;
        } else if (Info->GetEvictBorder() == b.Info->GetEvictBorder()) {
            return Info->GetName() > b.Info->GetName(); // add stability: smaller name is hotter
        }
        return false;
    }

    bool operator == (const TTierRef& b) const {
        return Info->GetEvictBorder() == b.Info->GetEvictBorder()
            && Info->GetName() == b.Info->GetName();
    }

    const TTierInfo& Get() const {
        return *Info;
    }

private:
    std::shared_ptr<TTierInfo> Info;
};

class TTiering {
    using TTiersMap = THashMap<TString, std::shared_ptr<TTierInfo>>;
    TTiersMap TierByName;
    TSet<TTierRef> OrderedTiers;
public:
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

    std::shared_ptr<arrow::Scalar> EvictScalar(const std::shared_ptr<arrow::Schema>& schema) const {
        auto ttlTs = Ttl ? Ttl->EvictScalar(schema) : nullptr;
        auto tierTs = OrderedTiers.empty() ? nullptr : OrderedTiers.begin()->Get().EvictScalar(schema);
        if (!ttlTs) {
            return tierTs;
        } else if (!tierTs) {
            return ttlTs;
        }
        return NArrow::ScalarLess(ttlTs, tierTs) ? tierTs : ttlTs; // either TTL or tier border appear
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
