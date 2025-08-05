#pragma once

#include <yt/yql/providers/yt/fmr/request_options/yql_yt_request_options.h>
#include <yt/yql/providers/yt/fmr/table_data_service/interface/yql_yt_table_data_service.h>
#include <yt/yql/providers/yt/fmr/yt_job_service/interface/yql_yt_job_service.h>

namespace NYql::NFmr {

std::vector<NYT::TRawTableReaderPtr> GetYtTableReaders(
    IYtJobService::TPtr jobService,
    const TYtTableTaskRef& ytTableTaskRef,
    const std::unordered_map<TFmrTableId, TClusterConnection>& clusterConnections
);

std::vector<NYT::TRawTableReaderPtr> GetTableInputStreams(
    IYtJobService::TPtr jobService,
    ITableDataService::TPtr tableDataService,
    const TTaskTableRef& tableRef,
    const std::unordered_map<TFmrTableId, TClusterConnection>& clusterConnections
);

} // namespace NYql::NFmr
