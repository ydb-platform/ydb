#include <library/cpp/testing/unittest/registar.h>
#include <yt/yql/providers/yt/fmr/coordinator/impl/yql_yt_partitioner.h>

namespace NYql::NFmr {

const TString FirstPartId = "test_part_id_0", SecondPartId = "test_part_id_1";

std::unordered_map<TFmrTableId, std::vector<TString>> GetTestPartIdsForTable(const TFmrTableId& fmrId) {
    return std::unordered_map<TFmrTableId, std::vector<TString>>{{fmrId, std::vector<TString>{FirstPartId, SecondPartId}}};
}

std::unordered_map<TString, std::vector<TChunkStats>> GetTestPartIdStats() {
    const std::vector<TChunkStats> firstPartitionChunkStats{
        TChunkStats{.DataWeight = 30},
        TChunkStats{.DataWeight = 30},
        TChunkStats{.DataWeight = 10},
        TChunkStats{.DataWeight = 20},
    };
    const std::vector<TChunkStats> secondPartitionChunkStats{
        TChunkStats{.DataWeight = 40},
        TChunkStats{.DataWeight = 15},
    };

    return std::unordered_map<TString, std::vector<TChunkStats>>{
        {FirstPartId, firstPartitionChunkStats},
        {SecondPartId, secondPartitionChunkStats}
    };
}

const TString FirstTablePartId = "first_table_part_id", SecondTablePartId = "sec_table_part_id", ThirdTablePartId = "third_table_part_id";

std::unordered_map<TFmrTableId, std::vector<TString>> GetTestPartIdsForMultipleTables(std::vector<TFmrTableId>& fmrTableIds) {
    UNIT_ASSERT_VALUES_EQUAL(fmrTableIds.size(), 3);
    return std::unordered_map<TFmrTableId, std::vector<TString>>{
        {fmrTableIds[0], std::vector<TString>{FirstTablePartId}},
        {fmrTableIds[1], std::vector<TString>{SecondTablePartId}},
        {fmrTableIds[2], std::vector<TString>{ThirdTablePartId}}
    };
}

std::unordered_map<TString, std::vector<TChunkStats>> GetTestPartIdStatsForMultipleTables() {
    const std::vector<TChunkStats> firstPartitionChunkStats{
        TChunkStats{.DataWeight = 40},
        TChunkStats{.DataWeight = 20},
    };
    const std::vector<TChunkStats> secondPartitionChunkStats{
        TChunkStats{.DataWeight = 40},
        TChunkStats{.DataWeight = 15},
        TChunkStats{.DataWeight = 5},
    };

    const std::vector<TChunkStats> thirdPartitionChunkStats{
        TChunkStats{.DataWeight = 20},
        TChunkStats{.DataWeight = 30},
        TChunkStats{.DataWeight = 60},
    };

    return std::unordered_map<TString, std::vector<TChunkStats>>{
        {FirstTablePartId, firstPartitionChunkStats},
        {SecondTablePartId, secondPartitionChunkStats},
        {ThirdTablePartId, thirdPartitionChunkStats}
    };
}

std::vector<std::vector<TFmrTableInputRef>> ChangeGottenTasksFormat(const std::vector<TTaskTableInputRef>& inputTasks) {
    // needed for testing so we can check resulting vectors for equality.
    std::vector<std::vector<TFmrTableInputRef>> resultTasks;
    for (auto& task: inputTasks) {
        std::vector<TFmrTableInputRef> curTask;
        std::transform(task.Inputs.begin(),task.Inputs.end(), std::back_inserter(curTask), [](const TTaskTableRef& tablePart){
            return std::get<TFmrTableInputRef>(tablePart);
        });
        resultTasks.emplace_back(curTask);
    }
    return resultTasks;
}

Y_UNIT_TEST_SUITE(PartitionerTests) {
    Y_UNIT_TEST(PartitionFmrTable) {
        auto fmrTableId = TFmrTableId("test_cluster", "test_path");
        TFmrTableRef fmrTable = TFmrTableRef{fmrTableId};

        auto partIdsForTables = GetTestPartIdsForTable(fmrTableId);
        auto partIdStats = GetTestPartIdStats();
        TFmrPartitionerSettings settings{.MaxDataWeightPerPart = 50, .MaxParts = 100};
        TFmrPartitioner partitioner(partIdsForTables, partIdStats, settings);

        auto [gottenTasks, status] = partitioner.PartitionFmrTablesIntoTasks({fmrTable});
        UNIT_ASSERT_VALUES_EQUAL(status, true);

        std::vector<std::vector<TFmrTableInputRef>> expectedTasks = {
            {TFmrTableInputRef{
                .TableId = fmrTableId.Id,
                .TableRanges = {
                    TTableRange{.PartId = FirstPartId, .MinChunk = 0, .MaxChunk = 1}
                }
            }},
            {TFmrTableInputRef{
                .TableId = fmrTableId.Id,
                .TableRanges = {
                    TTableRange{.PartId = FirstPartId, .MinChunk = 1, .MaxChunk = 3}
                }
            }},
            {TFmrTableInputRef{
                .TableId = fmrTableId.Id,
                .TableRanges = {
                    TTableRange{.PartId = SecondPartId, .MinChunk = 0, .MaxChunk = 1}
                }
            }},
            {TFmrTableInputRef{
                .TableId = fmrTableId.Id,
                .TableRanges = {
                    TTableRange{.PartId = FirstPartId, .MinChunk = 3, .MaxChunk = 4},
                    TTableRange{.PartId = SecondPartId, .MinChunk = 1, .MaxChunk = 2},
                }
            }},
        };
        UNIT_ASSERT_VALUES_EQUAL(ChangeGottenTasksFormat(gottenTasks), expectedTasks);
    }
    Y_UNIT_TEST(MaxPartsNumExceeded) {
        auto fmrTableId = TFmrTableId("test_cluster", "test_path");
        TFmrTableRef fmrTable = TFmrTableRef{fmrTableId};

        auto partIdsForTables = GetTestPartIdsForTable(fmrTableId);
        auto partIdStats = GetTestPartIdStats();
        TFmrPartitionerSettings settings{.MaxDataWeightPerPart = 50, .MaxParts = 2};
        TFmrPartitioner partitioner(partIdsForTables, partIdStats, settings);

        auto [gottenTasks, status] = partitioner.PartitionFmrTablesIntoTasks({fmrTable});
        UNIT_ASSERT_VALUES_EQUAL(status, false);
    }
    Y_UNIT_TEST(SeveralFullPartitionsInTask) {
        auto fmrTableId = TFmrTableId("test_cluster", "test_path");
        TFmrTableRef fmrTable = TFmrTableRef{fmrTableId};

        auto partIdsForTables = GetTestPartIdsForTable(fmrTableId);
        auto partIdStats = GetTestPartIdStats();
        TFmrPartitionerSettings settings{.MaxDataWeightPerPart = 1000000, .MaxParts = 1};
        TFmrPartitioner partitioner(partIdsForTables, partIdStats, settings);

        auto [gottenTasks, status] = partitioner.PartitionFmrTablesIntoTasks({fmrTable});
        UNIT_ASSERT_VALUES_EQUAL(status, true);

        std::vector<std::vector<TFmrTableInputRef>> expectedTasks = {
            {
                TFmrTableInputRef{
                    .TableId = fmrTableId.Id,
                    .TableRanges = {
                        TTableRange{.PartId = FirstPartId, .MinChunk = 0, .MaxChunk = 4},
                        TTableRange{.PartId = SecondPartId, .MinChunk = 0, .MaxChunk = 2},
                    }
                }
            }
        };
        UNIT_ASSERT_VALUES_EQUAL(ChangeGottenTasksFormat(gottenTasks), expectedTasks);
    }
    Y_UNIT_TEST(SeveralInputTables) {
        std::vector<TFmrTableId> inputFmrTableIds{
            TFmrTableId("test_cluster_1", "test_path_1"),
            TFmrTableId("test_cluster_2", "test_path_2"),
            TFmrTableId("test_cluster_3", "test_path_3"),
        };
        std::vector<TFmrTableRef> inputTables;
        for (auto& id: inputFmrTableIds) {
            inputTables.emplace_back(TFmrTableRef{.FmrTableId = id});
        }

        auto partIdsForTables = GetTestPartIdsForMultipleTables(inputFmrTableIds);
        auto partIdStats = GetTestPartIdStatsForMultipleTables();
        TFmrPartitionerSettings settings{.MaxDataWeightPerPart = 50, .MaxParts = 1000};
        TFmrPartitioner partitioner(partIdsForTables, partIdStats, settings);
        auto [gottenTasks, status] = partitioner.PartitionFmrTablesIntoTasks(inputTables);
        UNIT_ASSERT_VALUES_EQUAL(status, true);

        std::vector<std::vector<TFmrTableInputRef>> expectedTasks = {
            {TFmrTableInputRef{
                .TableId = TFmrTableId("test_cluster_1", "test_path_1").Id,
                .TableRanges = {
                    TTableRange{.PartId = FirstTablePartId, .MinChunk = 0, .MaxChunk = 1}
                }
            }},
            {TFmrTableInputRef{
                .TableId = TFmrTableId("test_cluster_2", "test_path_2").Id,
                .TableRanges = {
                    TTableRange{.PartId = SecondTablePartId, .MinChunk = 0, .MaxChunk = 1}
                }
            }},

            {TFmrTableInputRef{
                .TableId = TFmrTableId("test_cluster_3", "test_path_3").Id,
                .TableRanges = {
                    TTableRange{.PartId = ThirdTablePartId, .MinChunk = 0, .MaxChunk = 2}
                }
            }},
            {TFmrTableInputRef{
                .TableId = TFmrTableId("test_cluster_3", "test_path_3").Id,
                .TableRanges = {
                    TTableRange{.PartId = ThirdTablePartId, .MinChunk = 2, .MaxChunk = 3}
                }
            }},
            {
                TFmrTableInputRef{
                    .TableId = TFmrTableId("test_cluster_1", "test_path_1").Id,
                    .TableRanges = {
                        TTableRange{.PartId = FirstTablePartId, .MinChunk = 1, .MaxChunk = 2}
                    }
                },
                TFmrTableInputRef{
                    .TableId = TFmrTableId("test_cluster_2", "test_path_2").Id,
                    .TableRanges = {
                        TTableRange{.PartId = SecondTablePartId, .MinChunk = 1, .MaxChunk = 3}
                    }
                }
            }
        };
        UNIT_ASSERT_VALUES_EQUAL(ChangeGottenTasksFormat(gottenTasks), expectedTasks);
    }
}

} // namespace NYql::NFmr
