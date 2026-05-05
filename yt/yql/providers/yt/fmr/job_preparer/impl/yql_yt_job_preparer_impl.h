#pragma once

#include <library/cpp/threading/future/future.h>
#include <util/thread/pool.h>
#include <yql/essentials/core/file_storage/file_storage.h>
#include <yt/yql/providers/yt/fmr/job_preparer/interface/yql_yt_job_preparer_interface.h>

namespace NYql::NFmr {

struct TFmrJobPreparerSettings {
    ui64 NumThreads = 3; // TODO - add to settings file.
};

IFmrJobPreparer::TPtr MakeFmrJobPreparer(
    TFileStoragePtr fileStorage,
    const TString& tableDataServiceDiscoveryFilePath,
    const TFmrJobPreparerSettings& settings = TFmrJobPreparerSettings(),
    IFmrTvmClient::TPtr tvmClient = nullptr,
    TTvmId destinationTvmId = 0
);

} // namespace NYql::NFmr
