#pragma once

#include <yt/yql/providers/yt/gateway/fmr/yql_yt_fmr.h>
#include <yt/yql/providers/yt/fmr/worker/impl/yql_yt_worker_impl.h>
#include <yt/yql/providers/yt/fmr/coordinator/client/yql_yt_coordinator_client.h>
#include <yt/yql/providers/yt/fmr/coordinator/impl/yql_yt_coordinator_impl.h>
#include <yt/yql/providers/yt/fmr/job/impl/yql_yt_job_impl.h>
#include <yt/yql/providers/yt/fmr/job_factory/impl/yql_yt_job_factory_impl.h>
#include <yt/yql/providers/yt/fmr/table_data_service/local/yql_yt_table_data_service_local.h>
#include <yt/yql/providers/yt/fmr/yt_service/file/yql_yt_file_yt_service.h>
#include <yt/yql/providers/yt/fmr/yt_service/impl/yql_yt_yt_service_impl.h>

namespace NYql::NFmr {

constexpr TStringBuf FastMapReduceGatewayName = "fmr";

std::pair<IYtGateway::TPtr, IFmrWorker::TPtr> InitializeFmrGateway(
    IYtGateway::TPtr slave,
    bool disableLocalFmrWorker = false,
    const TString& coordinatorServerUrl = TString(),
    bool isFileGateway = false,
    const TString& fmrOperationSpecFilePath = TString()
);

} // namespace NYql::NFmr
