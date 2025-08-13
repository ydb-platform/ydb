#include "yql_yt_table_input_streams.h"
#include <yt/yql/providers/yt/fmr/job/impl/yql_yt_table_data_service_reader.h>

namespace NYql::NFmr {

std::vector<NYT::TRawTableReaderPtr> GetYtTableReaders(
    IYtJobService::TPtr jobService,
    const TYtTableTaskRef& ytTableTaskRef,
    const std::unordered_map<TFmrTableId, TClusterConnection>& clusterConnections
) {
    std::vector<NYT::TRawTableReaderPtr> ytTableReaders;

    std::vector<NYT::TRichYPath> richPaths = ytTableTaskRef.RichPaths;
    std::vector<TString> filePaths = ytTableTaskRef.FilePaths;
    bool hasFilePaths = false;

    if (!filePaths.empty()) {
        YQL_ENSURE(filePaths.size() == richPaths.size());
        hasFilePaths = true;
    }

    for (ui64 i = 0; i < richPaths.size(); ++i) {
        auto& richPath = richPaths[i];

        auto fmrTableId = TFmrTableId(richPath);
        auto clusterConnection = TClusterConnection();

        TYtTableRef ytTablePart{.RichPath = richPath};
        if (hasFilePaths) {
            ytTablePart.FilePath = filePaths[i];
        } else {
            clusterConnection = clusterConnections.at(fmrTableId);
        }

        ytTableReaders.emplace_back(jobService->MakeReader(ytTablePart, clusterConnection)); // TODO - reader Settings
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
    } else {
        return {MakeIntrusive<TFmrTableDataServiceReader>(fmrTable->TableId, fmrTable->TableRanges, tableDataService, fmrTable->Columns, fmrTable->SerializedColumnGroups)}; // TODO - fmr reader settings
    }
}

} // namespace NYql::NFmr
