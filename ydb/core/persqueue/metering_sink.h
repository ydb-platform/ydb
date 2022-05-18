#pragma once

#include <util/generic/set.h>


namespace NKikimr::NPQ {

enum class EMeteringJson {
    PutEventsV1,
    ResourcesReservedV1,
    ThroughputV1,
    StorageV1,
};

class TMeteringSink {
public:
    struct TParameters {
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
        ui64 ConsumersThroughput;
    };
    bool Create(TInstant now, const TParameters& p, const TSet<EMeteringJson>& whichToFlush,
                std::function<void(TString)> howToFlush);
    void MayFlush(TInstant now);
    void MayFlushForcibly(TInstant now);
    void Close();
    ui64 IncreaseQuantity(EMeteringJson meteringJson, ui64 inc);
    TParameters GetParameters() const;
    bool IsCreated() const;

    TString GetMeteringJson(const TString& metricBillingId, const TString& schemeName,
                            const THashMap<TString, ui64>& tags, const TString& quantityUnit, ui64 quantity,
                            TInstant start, TInstant end, TInstant now);
    ui32 GetMeteringCounter() const;

private:
    TParameters Parameters_{};
    bool Created_{false};
    ui64 CurrentPutUnitsQuantity_{0};
    TSet<EMeteringJson> WhichToFlush_{};
    TMap<EMeteringJson, TInstant> LastFlush_;
    std::function<void(TString)> FlushFunction_;

    static ui32 MeteringCounter_;

private:
    void Flush(TInstant now, bool force);

    bool IsTimeToFlush(TInstant now, TInstant last) const;
};

} // namespace NKikimr::NPQ
