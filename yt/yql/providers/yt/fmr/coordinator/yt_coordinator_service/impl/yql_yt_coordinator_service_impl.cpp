#include "yql_yt_coordinator_service_impl.h"

#include <library/cpp/yt/error/error.h>
#include <yt/cpp/mapreduce/common/helpers.h>
#include <yql/essentials/utils/log/log.h>

#include <util/generic/hash.h>

namespace NYql::NFmr {

namespace {

// Key used to match a GetTablePartitions result back to the input table it came from. Only the
// (prefix-normalized) bare path is used: a returned partition's path carries a concrete
// server-computed sub-range (e.g. a row_index slice) and possibly other request-independent
// attributes that the original request path never had, so anything beyond the path itself would
// never match between the request side and the response side.
TString GetTablePathKey(NYT::TRichYPath richPath) {
    NormalizeRichPath(richPath);
    return richPath.Path_;
}

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

            // GetTablePartitions returns bare TRichYPaths with no back-reference to which input index
            // produced them, so SectionIndex has to be recovered by matching on table path — which is
            // safe only while every path passed to one call is unique. Split inputs into batches with
            // no duplicate path each (usually just one batch, when no table repeats across sections or
            // via Concat); a repeat spills into a later batch instead of aliasing within the same call.
            // This keeps GetTablePartitions's own cross-table partition packing for the common
            // no-duplicates case.
            //
            // Matching uses only the bare (prefix-normalized) Path_, not the full path incl. ranges:
            // a returned partition's path carries a concrete server-computed sub-range (e.g. a
            // row_index slice) that the original request path never had, so comparing full
            // (YSON-serialized) paths would never match.
            // The k-th occurrence of a given path always belongs to batch k (first occurrence to
            // batch 0, second to batch 1, ...), so a per-path occurrence counter places each table
            // directly without scanning existing batches (which would be O(N^2) if many inputs share
            // the same path).
            std::vector<std::vector<TYtTableRef>> batches;
            THashMap<TString, size_t> pathOccurrenceCount;
            for (auto& ytTable: ytTables) {
                auto pathKey = GetTablePathKey(ytTable.RichPath);
                size_t batchIndex = pathOccurrenceCount[pathKey]++;
                if (batchIndex == batches.size()) {
                    batches.emplace_back();
                }
                batches[batchIndex].emplace_back(ytTable);
            }

            struct TWeightedPartition {
                TYtTableTaskRef TaskRef;
                i64 DataWeight = 0;
            };
            std::vector<TWeightedPartition> weightedPartitions;

            for (auto& batch: batches) {
                TVector<NYT::TRichYPath> richPaths;
                THashMap<TString, ui32> sectionIndexByPath;
                for (auto& ytTable: batch) {
                    auto richPath = ytTable.RichPath;
                    NormalizeRichPath(richPath);
                    richPaths.emplace_back(richPath);
                    sectionIndexByPath[GetTablePathKey(ytTable.RichPath)] = ytTable.SectionIndex;
                }
                try {
                    for (size_t i = 0; i < richPaths.size(); ++i) {
                        YQL_CLOG(TRACE, FastMapReduce) << "GetTablePartitions richPath[" << i << "]: "
                            << NYT::NodeToYsonString(NYT::PathToNode(richPaths[i]));
                    }
                    YQL_CLOG(TRACE, FastMapReduce) << "Calling YT API GetTablePartitions with DataWeightPerPartition="
                    << settings.MaxDataWeightPerPart << ", MaxParts=" << settings.MaxParts
                    << ", AdjustDataWeightPerPartition=false";
                    NYT::TMultiTablePartitions partitions = transaction->GetTablePartitions(richPaths, getTablePartitionsOptions);

                    YQL_CLOG(TRACE, FastMapReduce) << "YT API returned " << partitions.Partitions.size()
                    << " partitions for DataWeightPerPartition=" << settings.MaxDataWeightPerPart;

                    for (const auto& partition: partitions.Partitions) {
                        TYtTableTaskRef ytTableTaskRef{};
                        for (const auto& richPath: partition.TableRanges) {
                            ytTableTaskRef.RichPaths.emplace_back(richPath);
                            ytTableTaskRef.SectionIndices.emplace_back(
                                sectionIndexByPath.at(GetTablePathKey(richPath)));
                        }
                        weightedPartitions.emplace_back(TWeightedPartition{
                            .TaskRef = std::move(ytTableTaskRef),
                            .DataWeight = partition.AggregateStatistics.DataWeight
                        });
                    }
                } catch (NYT::TErrorException& ex) {
                    YQL_CLOG(ERROR, FastMapReduce) << "Failed to partition yt tables with message: " << CurrentExceptionMessage();
                    return {{}, false};
                }
            }

            // Bin-pack partitions (possibly from different tables/batches) sharing this cluster into
            // combined tasks up to MaxDataWeightPerPart, mirroring
            // TFmrPartitioner::HandleFmrLeftoverRanges. GetTablePartitions already does this within a
            // single batch; this additionally packs across batches split apart above.
            TYtTableTaskRef currentTask;
            ui64 currentWeight = 0;
            for (auto& weighted: weightedPartitions) {
                if (!currentTask.RichPaths.empty() &&
                    currentWeight + static_cast<ui64>(weighted.DataWeight) > settings.MaxDataWeightPerPart)
                {
                    ytPartitions.emplace_back(std::move(currentTask));
                    currentTask = TYtTableTaskRef{};
                    currentWeight = 0;
                }
                currentTask.RichPaths.insert(currentTask.RichPaths.end(),
                    std::make_move_iterator(weighted.TaskRef.RichPaths.begin()),
                    std::make_move_iterator(weighted.TaskRef.RichPaths.end()));
                currentTask.SectionIndices.insert(currentTask.SectionIndices.end(),
                    weighted.TaskRef.SectionIndices.begin(), weighted.TaskRef.SectionIndices.end());
                currentWeight += weighted.DataWeight;
            }
            if (!currentTask.RichPaths.empty()) {
                ytPartitions.emplace_back(std::move(currentTask));
            }

            if (settings.MaxParts > 0 && ytPartitions.size() > settings.MaxParts) {
                YQL_CLOG(ERROR, FastMapReduce) << "Failed to partition yt tables: got " << ytPartitions.size()
                    << " tasks, exceeding MaxParts=" << settings.MaxParts;
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
        THashMap<TString, ui64> ytServerToGroups;
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
