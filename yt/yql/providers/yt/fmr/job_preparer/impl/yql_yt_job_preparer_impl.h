#pragma once

#include <yt/yql/providers/yt/fmr/job_preparer/interface/yql_yt_job_preparer_interface.h>
#include <yt/yql/providers/yt/fmr/table_data_service/discovery/interface/yql_yt_service_discovery.h>

#include <yql/essentials/core/file_storage/file_storage.h>

#include <library/cpp/threading/future/future.h>

#include <util/thread/pool.h>

namespace NYql::NFmr {

struct TFmrJobPreparerSettings {
    ui64 NumThreads = 3; // TODO - add to settings file.
};

IFmrJobPreparer::TPtr MakeFmrJobPreparer(
    TFileStoragePtr fileStorage,
    ITableDataServiceDiscovery::TPtr tableDataServiceDiscovery,
    const TFmrJobPreparerSettings& settings = TFmrJobPreparerSettings(),
    IFmrTvmClient::TPtr tvmClient = nullptr,
    TTvmId destinationTvmId = 0
);

} // namespace NYql::NFmr
