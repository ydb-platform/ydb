#include "yql_yt_coordinator_service_impl.h"

#include <library/cpp/yt/error/error.h>
#include <yt/cpp/mapreduce/common/helpers.h>
#include <yt/yql/providers/yt/fmr/utils/yql_yt_client.h>
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
            .PartitionMode(NYT::ETablePartitionMode::Unordered)
            .DataWeightPerPartition(settings.MaxDataWeightPerPart)
            .MaxPartitionCount(settings.MaxParts)
            .AdjustDataWeightPerPartition(false); // TODO - add adjust data weight into partitioner settings

        std::vector<TYtTableTaskRef> ytPartitions;
        auto groupedYtTables = GroupYtTables(ytTables, clusterConnections);
        for (auto& [ytTables, clusterConnection]: groupedYtTables) {
            auto client = CreateClient(clusterConnection);
            auto transaction = client->AttachTransaction(GetGuid(clusterConnection.TransactionId));
            TVector<NYT::TRichYPath> richPaths;
            for (auto& ytTable: ytTables ) {
                TString ytPath = NYT::AddPathPrefix(ytTable.Path, "//");
                richPaths.emplace_back(NYT::TRichYPath(ytPath).Cluster(ytTable.Cluster));
            }
            try {
                NYT::TMultiTablePartitions partitions = transaction->GetTablePartitions(richPaths, getTablePartitionsOptions);

                for (const auto& partition : partitions.Partitions) {
                    TYtTableTaskRef ytTableTaskRef{};
                    for (const auto& richPath : partition.TableRanges) {
                        ytTableTaskRef.RichPaths.emplace_back(richPath);
                    }
                    ytPartitions.emplace_back(ytTableTaskRef);
                }
            } catch (NYT::TErrorException& ex) {
                YQL_CLOG(INFO, FastMapReduce) << "Failed to partition yt tables with message: " << CurrentExceptionMessage();
                return {{}, false};
            }
        }
        YQL_CLOG(INFO, FastMapReduce) << "partitioned input yt tables into " << ytPartitions.size() << " tasks";
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
            auto fmrTableId = TFmrTableId(ytTable.Cluster, ytTable.Path);
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
