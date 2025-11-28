#pragma once

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/generic/map.h>
#include <util/generic/set.h>

#include <ydb/core/persqueue/common/metering.h>

namespace NKikimr::NPQ {


class TMeteringSink {
public:
    struct TParameters {
        TDuration FlushLimit{TDuration::Hours(1)};
        TDuration FlushInterval;
        TString TabletId;
        TString YcCloudId;
        TString YcFolderId;
        TString YdbDatabaseId;
        TString StreamName;
        TString ResourceId;
        ui64 PartitionsSize;
        ui64 WriteQuota;
        ui64 ReservedSpace;
        ui64 ConsumersCount;
    };

    bool Create(TInstant now, const TParameters& p, const TSet<EMeteringJson>& whichToFlush,
                std::function<void(TString)> howToFlush);
    void MayFlush(TInstant now);
    void MayFlushForcibly(TInstant now);
    void Close();
    ui64 IncreaseQuantity(EMeteringJson meteringJson, ui64 inc);

    TParameters GetParameters() const;
    bool IsCreated() const;

    ui64 GetMeteringCounter() const;

private:
    TParameters Parameters_{};
    bool Created_{false};
    ui64 CurrentPutUnitsQuantity_{0};
    ui64 CurrentUsedStorage_{0};
    TSet<EMeteringJson> WhichToFlush_{};
    TMap<EMeteringJson, TInstant> LastFlush_;
    std::function<void(TString)> FlushFunction_;

    static std::atomic<ui64> MeteringCounter_;

private:
    void Flush(TInstant now, bool force);

    struct FlushParameters {
        FlushParameters(TString&& name,
                        TString&& schema,
                        TString&& units,
                        ui64 quantity = 1)
            : Name(std::move(name))
            , Schema(std::move(schema))
            , Units(std::move(units))
            , Quantity(quantity) {
        }

        FlushParameters& withTags(THashMap<TString, ui64>&& tags) {
            Tags = std::move(tags);
            return *this;
        }

        FlushParameters& withVersion(TString&& version) {
            Version = std::move(version);
            return *this;
        }

        FlushParameters& withOneFlush() {
            OneFlush = true;
            return *this;
        }

        // skip flush, if quantity is zero
        FlushParameters& withImplicitZero() {
            ImplicitZero = true;
            return *this;
        }

        TString Name;
        TString Schema;
        TString Units;
        ui64 Quantity;
        THashMap<TString, ui64> Tags;
        bool OneFlush = false;
        bool ImplicitZero = false;
        TString Version = "v1";
    };

    const FlushParameters GetFlushParameters(const EMeteringJson type, const TInstant& now, const TInstant& lastFlush);
    TString GetMeteringJson(const TMeteringSink::FlushParameters& parameters, ui64 quantity, TInstant start, TInstant end, TInstant now);

    bool IsTimeToFlush(TInstant now, TInstant last) const;
};

} // namespace NKikimr::NPQ
