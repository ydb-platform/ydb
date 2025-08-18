#pragma once

#include <yt/yql/providers/yt/gateway/fmr/yql_yt_fmr.h>
#include <yt/yql/providers/yt/fmr/worker/impl/yql_yt_worker_impl.h>
#include <yt/yql/providers/yt/fmr/coordinator/client/yql_yt_coordinator_client.h>
#include <yt/yql/providers/yt/fmr/coordinator/impl/yql_yt_coordinator_impl.h>
#include <yt/yql/providers/yt/fmr/coordinator/yt_coordinator_service/file/yql_yt_file_coordinator_service.h>
#include <yt/yql/providers/yt/fmr/coordinator/yt_coordinator_service/impl/yql_yt_coordinator_service_impl.h>
#include <yt/yql/providers/yt/fmr/job/impl/yql_yt_job_impl.h>
#include <yt/yql/providers/yt/fmr/job_factory/impl/yql_yt_job_factory_impl.h>
#include <yt/yql/providers/yt/fmr/gc_service/impl/yql_yt_gc_service_impl.h>
#include <yt/yql/providers/yt/fmr/table_data_service/local/impl/yql_yt_table_data_service_local.h>
#include <yt/yql/providers/yt/fmr/yt_job_service/file/yql_yt_file_yt_job_service.h>
#include <yt/yql/providers/yt/fmr/yt_job_service/impl/yql_yt_job_service_impl.h>
#include <yt/yql/providers/yt/gateway/lib/exec_ctx.h>


namespace NYql::NFmr {

constexpr TStringBuf FastMapReduceGatewayName = "fmr";

std::pair<IYtGateway::TPtr, IFmrWorker::TPtr> InitializeFmrGateway(IYtGateway::TPtr slave, const TFmrServices::TPtr fmrServices);

} // namespace NYql::NFmr
