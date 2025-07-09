#include "yql_yt_table_input_streams.h"
#include <yt/yql/providers/yt/fmr/job/impl/yql_yt_table_data_service_reader.h>

namespace NYql::NFmr {

std::vector<NYT::TRawTableReaderPtr> GetYtTableReaders(
    IYtJobService::TPtr jobService,
    const TYtTableTaskRef& ytTableTaskRef,
    const std::unordered_map<TFmrTableId, TClusterConnection>& clusterConnections
) {
    std::vector<NYT::TRawTableReaderPtr> ytTableReaders;
    if (!ytTableTaskRef.FilePaths.empty()) {
        // underlying gateway is file, so create readers from filepaths.
        for (auto& filePath: ytTableTaskRef.FilePaths) {
            ytTableReaders.emplace_back(jobService->MakeReader(filePath));
        }
    } else {
        for (auto& richPath: ytTableTaskRef.RichPaths) {
            YQL_ENSURE(richPath.Cluster_);

            // TODO - вместо этого написать нормальные хелперы из RichPath в структуры и назад
            TStringBuf choppedPath;
            YQL_ENSURE(TStringBuf(richPath.Path_).AfterPrefix("//", choppedPath));
            auto fmrTableId = TFmrTableId(*richPath.Cluster_, TString(choppedPath));
            auto clusterConnection = clusterConnections.at(fmrTableId);
            ytTableReaders.emplace_back(jobService->MakeReader(richPath, clusterConnection)); // TODO - reader Settings
        }
    }
    return ytTableReaders;
}

std::vector<NYT::TRawTableReaderPtr> GetTableInputStreams(
    IYtJobService::TPtr jobService,
    ITableDataService::TPtr tableDataService,
    const TTaskTableRef& tableRef,
    const std::unordered_map<TFmrTableId, TClusterConnection>& clusterConnections
) {
    auto ytTableTaskRef = std::get_if<TYtTableTaskRef>(&tableRef);
    auto fmrTable = std::get_if<TFmrTableInputRef>(&tableRef);
    if (ytTableTaskRef) {
        return GetYtTableReaders(jobService, *ytTableTaskRef, clusterConnections);
    } else if (fmrTable) {
        return {MakeIntrusive<TFmrTableDataServiceReader>(fmrTable->TableId, fmrTable->TableRanges, tableDataService)}; // TODO - fmr reader settings
    } else {
        ythrow yexception() << "Unsupported table type";
    }
}

} // namespace NYql::NFmr
