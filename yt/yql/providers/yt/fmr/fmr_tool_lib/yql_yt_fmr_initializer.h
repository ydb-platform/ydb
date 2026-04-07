#pragma once

#include <yql/essentials/providers/common/proto/gateways_config.pb.h>

#include <yt/yql/providers/yt/gateway/fmr/yql_yt_fmr.h>
#include <yt/yql/providers/yt/fmr/worker/impl/yql_yt_worker_impl.h>
#include <yt/yql/providers/yt/fmr/coordinator/client/yql_yt_coordinator_client.h>
#include <yt/yql/providers/yt/fmr/coordinator/impl/yql_yt_coordinator_impl.h>
#include <yt/yql/providers/yt/fmr/coordinator/yt_coordinator_service/file/yql_yt_file_coordinator_service.h>
#include <yt/yql/providers/yt/fmr/coordinator/yt_coordinator_service/impl/yql_yt_coordinator_service_impl.h>
#include <yt/yql/providers/yt/fmr/file/upload/impl/yql_yt_file_upload_impl.h>
#include <yt/yql/providers/yt/fmr/file/metadata/impl/yql_yt_file_metadata_impl.h>
#include <yt/yql/providers/yt/fmr/job/impl/yql_yt_job_impl.h>
#include <yt/yql/providers/yt/fmr/job_factory/impl/yql_yt_job_factory_impl.h>
#include <yt/yql/providers/yt/fmr/job_preparer/impl/yql_yt_job_preparer_impl.h>
#include <yt/yql/providers/yt/fmr/gc_service/impl/yql_yt_gc_service_impl.h>
#include <yt/yql/providers/yt/fmr/table_data_service/local/impl/yql_yt_table_data_service_local.h>
#include <yt/yql/providers/yt/fmr/tvm/impl/yql_yt_fmr_tvm_impl.h>
#include <yt/yql/providers/yt/fmr/yt_job_service/file/yql_yt_file_yt_job_service.h>
#include <yt/yql/providers/yt/fmr/yt_job_service/impl/yql_yt_job_service_impl.h>
#include <yt/yql/providers/yt/gateway/lib/exec_ctx.h>


namespace NYql::NFmr {

constexpr TStringBuf FastMapReduceGatewayName = "fmr";

struct TFmrDistributedCacheSettings {
    TString Path;
    TString YtServerName;
    TString YtToken;
};

struct TFmrInitializationOptions {
    TMaybe<TString> FmrCoordinatorUrl;
    NFmr::IFileMetadataService::TPtr FmrFileMetadataService;
    NFmr::IFileUploadService::TPtr FmrFileUploadService;
    TFmrDistributedCacheSettings FmrDistributedCacheSettings = TFmrDistributedCacheSettings(); // return fmr cache settings explicitly for jobPrerarer initialization.
    TMaybe<TFmrTvmGatewaySettings> FmrTvmSettings;
};

TFmrInitializationOptions GetFmrInitializationInfoFromConfig(
    const TFmrInstance& fmrConfiguration,
    const google::protobuf::RepeatedPtrField<TFmrFileRemoteCache>& fileCacheConfigurations
);

std::pair<IYtGateway::TPtr, IFmrWorker::TPtr> InitializeFmrGateway(IYtGateway::TPtr slave, const TFmrServices::TPtr fmrServices);

} // namespace NYql::NFmr
