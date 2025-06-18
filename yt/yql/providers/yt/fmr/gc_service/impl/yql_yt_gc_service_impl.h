#pragma once

#include <yt/yql/providers/yt/fmr/gc_service/interface/yql_yt_gc_service_interface.h>

namespace NYql::NFmr {

struct TGcServiceSettings {
    TDuration TimeToSleepBetweenGroupDeletionRequests = TDuration::Seconds(1);
    ui64 GroupDeletionRequestMaxBatchSize = 1000;
    ui64 MaxInflightGroupDeletionRequests = 1000000;
    ui64 WorkersNum = 3;
};

IFmrGcService::TPtr MakeGcService(
    ITableDataService::TPtr tableDataService,
    const TGcServiceSettings& settings = TGcServiceSettings()
);

} // namespace NYql::NFmr
