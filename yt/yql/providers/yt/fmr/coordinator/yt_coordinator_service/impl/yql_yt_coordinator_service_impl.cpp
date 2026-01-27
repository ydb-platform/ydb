#include "yql_yt_coordinator_service_impl.h"

#include <library/cpp/yt/error/error.h>
#include <yt/cpp/mapreduce/common/helpers.h>
#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/utils/yql_panic.h>

namespace NYql::NFmr {

namespace {

class TYtCoordinatorService: public IYtCoordinatorService {
public:

    std::pair<std::vector<TYtTableTaskRef>, bool> PartitionYtTables(
        const std::vector<TYtTableRef>& ytTables,
        const std::unordered_map<TFmrTableId, TClusterConnection>& clusterConnections,
        const TYtPartitionerSettings& settings
    ) override {
        auto getTablePartitionsOptions = NYT::TGetTablePartitionsOptions()
            .PartitionMode(settings.PartitionMode)
            .DataWeightPerPartition(settings.MaxDataWeightPerPart)
            .MaxPartitionCount(settings.MaxParts)
            .AdjustDataWeightPerPartition(false);

        std::vector<TYtTableTaskRef> ytPartitions;
        auto groupedYtTables = GroupYtTables(ytTables, clusterConnections);
        for (auto& [ytTables, clusterConnection]: groupedYtTables) {
            auto client = CreateClient(clusterConnection);
            auto transaction = client->AttachTransaction(GetGuid(clusterConnection.TransactionId));
            TVector<NYT::TRichYPath> richPaths;
            for (auto& ytTable: ytTables ) {
                auto richPath = ytTable.RichPath;
                NormalizeRichPath(richPath);
                richPaths.emplace_back(richPath);
            }
            try {
                YQL_CLOG(TRACE, FastMapReduce) << "Calling YT API GetTablePartitions with DataWeightPerPartition="
                << settings.MaxDataWeightPerPart << ", MaxParts=" << settings.MaxParts
                << ", AdjustDataWeightPerPartition=true";
                NYT::TMultiTablePartitions partitions = transaction->GetTablePartitions(richPaths, getTablePartitionsOptions);

                YQL_CLOG(TRACE, FastMapReduce) << "YT API returned " << partitions.Partitions.size()
                << " partitions for DataWeightPerPartition=" << settings.MaxDataWeightPerPart;


                for (const auto& partition : partitions.Partitions) {
                    TYtTableTaskRef ytTableTaskRef{};
                    for (const auto& richPath : partition.TableRanges) {
                        ytTableTaskRef.RichPaths.emplace_back(richPath);
                    }
                    ytPartitions.emplace_back(ytTableTaskRef);
                }
            } catch (NYT::TErrorException& ex) {
                YQL_CLOG(ERROR, FastMapReduce) << "Failed to partition yt tables with message: " << CurrentExceptionMessage();
                return {{}, false};
            }
        }
        YQL_CLOG(INFO, FastMapReduce) << "partitioned " << ytTables.size() << " input yt tables into " << ytPartitions.size() << " tasks";
        for (auto& task: ytPartitions) {
            YQL_CLOG(DEBUG, FastMapReduce) << task;
        }
        return {ytPartitions, true};
    }

private:
    struct TGroupedYtTablesByCluster {
        std::vector<TYtTableRef> YtTables;
        TClusterConnection ClusterConnection;
    };

    std::vector<TGroupedYtTablesByCluster> GroupYtTables(
        const std::vector<TYtTableRef>& ytTables,
        const std::unordered_map<TFmrTableId, TClusterConnection>& clusterConnections
    ) {
        std::vector<TGroupedYtTablesByCluster> tableGroups;
        std::unordered_map<TString, ui64> ytServerToGroups;
        for (auto& ytTable: ytTables) {
            auto fmrTableId = TFmrTableId(ytTable.GetCluster(), ytTable.GetPath());
            auto clusterConnection = clusterConnections.at(fmrTableId);
            auto ytServerName = clusterConnection.YtServerName;
            if (!ytServerToGroups.contains(ytServerName)) {
                tableGroups.emplace_back(TGroupedYtTablesByCluster{
                    .YtTables = {ytTable},
                    .ClusterConnection = clusterConnection
                });
                ytServerToGroups[ytServerName] = tableGroups.size() - 1;
            } else {
                auto index = ytServerToGroups[ytServerName];
                tableGroups[index].YtTables.emplace_back(ytTable);
            }
        }
        return tableGroups;
    }
};

} // namespace

IYtCoordinatorService::TPtr MakeYtCoordinatorService() {
    return MakeIntrusive<TYtCoordinatorService>();
}

} // namespace NYql::NFmr
