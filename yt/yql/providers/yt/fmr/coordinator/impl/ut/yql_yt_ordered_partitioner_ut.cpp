#include <library/cpp/testing/unittest/registar.h>
#include <yt/yql/providers/yt/fmr/coordinator/impl/yql_yt_ordered_partitioner.h>
#include <yt/yql/providers/yt/fmr/test_tools/fmr_coordinator_service_helper/yql_yt_mock_coordinator_service.h>
#include <algorithm>

namespace NYql::NFmr {

namespace {

const TString FirstPartId = "ordered_part_id_0", SecondPartId = "ordered_part_id_1", ThirdPartId = "ordered_part_id_2";

const TString FirstTablePartId = "first_ordered_part_id";
const TString SecondTablePartId = "sec_ordered_part_id";
const TString ThirdTablePartId = "third_ordered_part_id";

std::unordered_map<TFmrTableId, std::vector<TString>> GetTestPartIdsForSingleTable(const TFmrTableId& tableId) {
    return {{tableId, {FirstPartId}}};
}

std::unordered_map<TFmrTableId, std::vector<TString>> GetTestPartIdsForMultipleTables(const std::vector<TFmrTableId>& tableIds) {
    std::unordered_map<TFmrTableId, std::vector<TString>> result;
    if (tableIds.size() >= 1) {
        result[tableIds[0]] = {FirstTablePartId};
    }
    if (tableIds.size() >= 2) {
        result[tableIds[1]] = {SecondTablePartId};
    }
    if (tableIds.size() >= 3) {
        result[tableIds[2]] = {ThirdTablePartId};
    }
    return result;
}

std::unordered_map<TString, std::vector<TChunkStats>> GeneratePartIdStats(const TString& partId, const std::vector<ui64>& dataWeights) {
    std::vector<TChunkStats> chunkStats;
    for (auto weight : dataWeights) {
        chunkStats.push_back(TChunkStats{
            .Rows = weight * 100,
            .DataWeight = weight
        });
    }
    return {{partId, chunkStats}};
}

std::unordered_map<TString, std::vector<TChunkStats>> MergePartIdStats(
    const std::vector<std::unordered_map<TString, std::vector<TChunkStats>>>& statsVec
) {
    std::unordered_map<TString, std::vector<TChunkStats>> result;
    for (const auto& stats : statsVec) {
        result.insert(stats.begin(), stats.end());
    }
    return result;
}

std::vector<std::vector<TFmrTableInputRef>> ChangeGottenTasksFormat(const std::vector<TTaskTableInputRef>& inputTasks) {
    std::vector<std::vector<TFmrTableInputRef>> resultTasks;
    for (auto& task: inputTasks) {
        std::vector<TFmrTableInputRef> curTask;
        std::transform(task.Inputs.begin(), task.Inputs.end(), std::back_inserter(curTask), [](const TTaskTableRef& tablePart){
            if (std::holds_alternative<TFmrTableInputRef>(tablePart)) {
                return std::get<TFmrTableInputRef>(tablePart);
            }
            return TFmrTableInputRef();
        });
        resultTasks.emplace_back(curTask);
    }
    return resultTasks;
}

struct TComparableYtTableTaskRef {
    std::vector<TString> SerializedRichPaths;
    std::vector<TString> FilePaths;

    bool operator==(const TComparableYtTableTaskRef&) const = default;
};

using TComparableTaskTableRef = std::variant<TComparableYtTableTaskRef, TFmrTableInputRef>;

TComparableTaskTableRef MakeComparableTaskRef(const TTaskTableRef& taskRef) {
    if (std::holds_alternative<TYtTableTaskRef>(taskRef)) {
        const auto& ytTask = std::get<TYtTableTaskRef>(taskRef);
        TComparableYtTableTaskRef comparable;
        comparable.SerializedRichPaths.reserve(ytTask.RichPaths.size());
        for (const auto& richPath : ytTask.RichPaths) {
            comparable.SerializedRichPaths.emplace_back(SerializeRichPath(richPath));
        }
        comparable.FilePaths = ytTask.FilePaths;
        return comparable;
    }
    return std::get<TFmrTableInputRef>(taskRef);
}

std::vector<std::vector<TComparableTaskTableRef>> MakeComparableTasks(const std::vector<TTaskTableInputRef>& inputTasks) {
    std::vector<std::vector<TComparableTaskTableRef>> result;
    result.reserve(inputTasks.size());
    for (const auto& task : inputTasks) {
        std::vector<TComparableTaskTableRef> cur;
        cur.reserve(task.Inputs.size());
        for (const auto& ref : task.Inputs) {
            cur.emplace_back(MakeComparableTaskRef(ref));
        }
        result.emplace_back(std::move(cur));
    }
    return result;
}

TYtTableTaskRef MakeYtTaskRef(const TString& cluster, const TString& path, const TString& suffix) {
    TYtTableTaskRef task;
    task.RichPaths.emplace_back(NYT::TRichYPath().Path(path + suffix).Cluster(cluster));
    return task;
}

} // namespace

Y_UNIT_TEST_SUITE(OrderedPartitionerTests) {
    Y_UNIT_TEST(SingleTableSplitIntoMultipleTasks) {
        TFmrTableId tableId("test_cluster_1", "test_path_1");
        TFmrTableRef table = TFmrTableRef{.FmrTableId = tableId};

        auto partIdsForTables = GetTestPartIdsForSingleTable(tableId);
        auto partIdStats = GeneratePartIdStats(FirstPartId, {10, 20, 30, 40});

        TOrderedPartitionSettings settings{
            .YtPartitionSettings = {},
            .FmrPartitionSettings = {.MaxDataWeightPerPart = 50, .MaxParts = 100}
        };

        TOrderedPartitioner partitioner(partIdsForTables, partIdStats, settings);
        auto [gottenTasks, status] = partitioner.PartitionTablesIntoTasksOrdered(
            {table}, nullptr, {}
        );

        UNIT_ASSERT_EQUAL(status, true);

        // Chunks {10, 20, 30, 40}: takes [0,3) weight 60 isFull → task1
        // Then takes [3,4) weight 40 isLast → task2
        std::vector<std::vector<TFmrTableInputRef>> expectedTasks = {
            {
                TFmrTableInputRef{
                    .TableId = table.FmrTableId.Id,
                    .TableRanges = {
                        TTableRange{.PartId = FirstPartId, .MinChunk = 0, .MaxChunk = 3}
                    }
                }
            },
            {
                TFmrTableInputRef{
                    .TableId = table.FmrTableId.Id,
                    .TableRanges = {
                        TTableRange{.PartId = FirstPartId, .MinChunk = 3, .MaxChunk = 4}
                    }
                }
            }
        };

        UNIT_ASSERT_EQUAL(ChangeGottenTasksFormat(gottenTasks), expectedTasks);
    }

    Y_UNIT_TEST(MultipleTablesEachInSeparateTask) {
        std::vector<TFmrTableId> tableIds = {
            TFmrTableId("test_cluster_1", "test_path_1"),
            TFmrTableId("test_cluster_2", "test_path_2")
        };
        std::vector<TOperationTableRef> tables = {
            TFmrTableRef{.FmrTableId = tableIds[0]},
            TFmrTableRef{.FmrTableId = tableIds[1]}
        };

        auto partIdsForTables = GetTestPartIdsForMultipleTables(tableIds);
        auto partIdStats = MergePartIdStats({
            GeneratePartIdStats(FirstTablePartId, {40, 20}),
            GeneratePartIdStats(SecondTablePartId, {30, 40})
        });

        TOrderedPartitionSettings settings{
            .YtPartitionSettings = {},
            .FmrPartitionSettings = {.MaxDataWeightPerPart = 50, .MaxParts = 100}
        };

        TOrderedPartitioner partitioner(partIdsForTables, partIdStats, settings);
        auto [gottenTasks, status] = partitioner.PartitionTablesIntoTasksOrdered(
            tables, nullptr, {}
        );

        UNIT_ASSERT_EQUAL(status, true);

        // Table1 {40, 20}: takes both [0,2) weight 60 isLast+isFull → task1, weight=0
        // Table2 {30, 40}: takes both [0,2) weight 70 isLast+isFull → task2, weight=0
        std::vector<std::vector<TFmrTableInputRef>> expectedTasks = {
            {
                TFmrTableInputRef{
                    .TableId = tableIds[0].Id,
                    .TableRanges = {
                        TTableRange{.PartId = FirstTablePartId, .MinChunk = 0, .MaxChunk = 2}
                    }
                }
            },
            {
                TFmrTableInputRef{
                    .TableId = tableIds[1].Id,
                    .TableRanges = {
                        TTableRange{.PartId = SecondTablePartId, .MinChunk = 0, .MaxChunk = 2}
                    }
                }
            }
        };

        UNIT_ASSERT_EQUAL(ChangeGottenTasksFormat(gottenTasks), expectedTasks);
    }

    Y_UNIT_TEST(ThreeTablesWithWeightCarryover) {
        std::vector<TFmrTableId> tableIds = {
            TFmrTableId("test_cluster_1", "test_path_1"),
            TFmrTableId("test_cluster_2", "test_path_2"),
            TFmrTableId("test_cluster_3", "test_path_3")
        };
        std::vector<TOperationTableRef> tables = {
            TFmrTableRef{.FmrTableId = tableIds[0]},
            TFmrTableRef{.FmrTableId = tableIds[1]},
            TFmrTableRef{.FmrTableId = tableIds[2]}
        };

        auto partIdsForTables = GetTestPartIdsForMultipleTables(tableIds);
        auto partIdStats = MergePartIdStats({
            GeneratePartIdStats(FirstTablePartId, {40, 20}),
            GeneratePartIdStats(SecondTablePartId, {40, 15, 5}),
            GeneratePartIdStats(ThirdTablePartId, {20, 30, 60})
        });

        TOrderedPartitionSettings settings{
            .YtPartitionSettings = {},
            .FmrPartitionSettings = {.MaxDataWeightPerPart = 50, .MaxParts = 100}
        };

        TOrderedPartitioner partitioner(partIdsForTables, partIdStats, settings);
        auto [gottenTasks, status] = partitioner.PartitionTablesIntoTasksOrdered(
            tables, nullptr, {}
        );

        UNIT_ASSERT_EQUAL(status, true);

        // Table1 {40, 20}: [0,2) weight 60 isLast+isFull → task1, weight=0
        // Table2 {40, 15, 5}: [0,2) weight 55 isFull → task2, weight=0
        //                     [2,3) weight 5 isLast → adds to task_input, weight=5
        // Table3 {20, 30, 60}: [0,2) weight 55 isFull → task3, weight=0
        //                      [2,3) weight 60 isLast+isFull → task4
        std::vector<std::vector<TFmrTableInputRef>> expectedTasks = {
            {
                TFmrTableInputRef{
                    .TableId = tableIds[0].Id,
                    .TableRanges = {
                        TTableRange{.PartId = FirstTablePartId, .MinChunk = 0, .MaxChunk = 2}
                    }
                }
            },
            {
                TFmrTableInputRef{
                    .TableId = tableIds[1].Id,
                    .TableRanges = {
                        TTableRange{.PartId = SecondTablePartId, .MinChunk = 0, .MaxChunk = 2}
                    }
                }
            },
            {
                TFmrTableInputRef{
                    .TableId = tableIds[1].Id,
                    .TableRanges = {
                        TTableRange{.PartId = SecondTablePartId, .MinChunk = 2, .MaxChunk = 3}
                    }
                },
                TFmrTableInputRef{
                    .TableId = tableIds[2].Id,
                    .TableRanges = {
                        TTableRange{.PartId = ThirdTablePartId, .MinChunk = 0, .MaxChunk = 2}
                    }
                }
            },
            {
                TFmrTableInputRef{
                    .TableId = tableIds[2].Id,
                    .TableRanges = {
                        TTableRange{.PartId = ThirdTablePartId, .MinChunk = 2, .MaxChunk = 3}
                    }
                }
            }
        };

        UNIT_ASSERT_EQUAL(ChangeGottenTasksFormat(gottenTasks), expectedTasks);
    }

    Y_UNIT_TEST(EmptyTable) {
        TFmrTableId tableId("test_cluster_1", "test_path_1");
        TFmrTableRef table = TFmrTableRef{.FmrTableId = tableId};

        auto partIdsForTables = GetTestPartIdsForSingleTable(tableId);
        auto partIdStats = GeneratePartIdStats(FirstPartId, {});

        TOrderedPartitionSettings settings{
            .YtPartitionSettings = {},
            .FmrPartitionSettings = {.MaxDataWeightPerPart = 50, .MaxParts = 100}
        };

        TOrderedPartitioner partitioner(partIdsForTables, partIdStats, settings);
        auto [gottenTasks, status] = partitioner.PartitionTablesIntoTasksOrdered(
            {table}, nullptr, {}
        );

        UNIT_ASSERT_EQUAL(status, true);
        UNIT_ASSERT_EQUAL(gottenTasks.size(), 0);
    }

    Y_UNIT_TEST(MultiplePartitionsEachInSeparateTask) {
        TFmrTableId tableId("test_cluster_1", "test_path_1");
        TFmrTableRef table = TFmrTableRef{.FmrTableId = tableId};

        std::unordered_map<TFmrTableId, std::vector<TString>> partIdsForTables = {
            {tableId, {FirstPartId, SecondPartId}}
        };
        auto partIdStats = MergePartIdStats({
            GeneratePartIdStats(FirstPartId, {30, 40}),
            GeneratePartIdStats(SecondPartId, {25, 35})
        });

        TOrderedPartitionSettings settings{
            .YtPartitionSettings = {},
            .FmrPartitionSettings = {.MaxDataWeightPerPart = 50, .MaxParts = 100}
        };

        TOrderedPartitioner partitioner(partIdsForTables, partIdStats, settings);
        auto [gottenTasks, status] = partitioner.PartitionTablesIntoTasksOrdered(
            {table}, nullptr, {}
        );

        UNIT_ASSERT_EQUAL(status, true);

        // FirstPartId {30, 40}: takes both [0,2) weight 70 isLast+isFull → task1
        // SecondPartId {25, 35}: takes both [0,2) weight 60 isLast+isFull → task2
        std::vector<std::vector<TFmrTableInputRef>> expectedTasks = {
            {
                TFmrTableInputRef{
                    .TableId = table.FmrTableId.Id,
                    .TableRanges = {
                        TTableRange{.PartId = FirstPartId, .MinChunk = 0, .MaxChunk = 2}
                    }
                }
            },
            {
                TFmrTableInputRef{
                    .TableId = table.FmrTableId.Id,
                    .TableRanges = {
                        TTableRange{.PartId = SecondPartId, .MinChunk = 0, .MaxChunk = 2}
                    }
                }
            }
        };

        UNIT_ASSERT_EQUAL(ChangeGottenTasksFormat(gottenTasks), expectedTasks);
    }

    Y_UNIT_TEST(SingleChunkExceedsLimit) {
        // Edge case: single chunk weight > limit
        TFmrTableId tableId("test_cluster_1", "test_path_1");
        TFmrTableRef table = TFmrTableRef{.FmrTableId = tableId};

        auto partIdsForTables = GetTestPartIdsForSingleTable(tableId);
        auto partIdStats = GeneratePartIdStats(FirstPartId, {100});

        TOrderedPartitionSettings settings{
            .YtPartitionSettings = {},
            .FmrPartitionSettings = {.MaxDataWeightPerPart = 50, .MaxParts = 100}
        };

        TOrderedPartitioner partitioner(partIdsForTables, partIdStats, settings);
        auto [gottenTasks, status] = partitioner.PartitionTablesIntoTasksOrdered(
            {table}, nullptr, {}
        );

        UNIT_ASSERT_EQUAL(status, true);

        // Single chunk must be taken even if exceeds limit
        std::vector<std::vector<TFmrTableInputRef>> expectedTasks = {
            {
                TFmrTableInputRef{
                    .TableId = table.FmrTableId.Id,
                    .TableRanges = {
                        TTableRange{.PartId = FirstPartId, .MinChunk = 0, .MaxChunk = 1}
                    }
                }
            }
        };

        UNIT_ASSERT_EQUAL(ChangeGottenTasksFormat(gottenTasks), expectedTasks);
    }

    Y_UNIT_TEST(EachChunkEqualsLimit) {
        // Edge case: each chunk weight == limit
        TFmrTableId tableId("test_cluster_1", "test_path_1");
        TFmrTableRef table = TFmrTableRef{.FmrTableId = tableId};

        auto partIdsForTables = GetTestPartIdsForSingleTable(tableId);
        auto partIdStats = GeneratePartIdStats(FirstPartId, {50, 50, 50});

        TOrderedPartitionSettings settings{
            .YtPartitionSettings = {},
            .FmrPartitionSettings = {.MaxDataWeightPerPart = 50, .MaxParts = 100}
        };

        TOrderedPartitioner partitioner(partIdsForTables, partIdStats, settings);
        auto [gottenTasks, status] = partitioner.PartitionTablesIntoTasksOrdered(
            {table}, nullptr, {}
        );

        UNIT_ASSERT_EQUAL(status, true);

        // Each chunk at limit should be separate task
        std::vector<std::vector<TFmrTableInputRef>> expectedTasks = {
            {
                TFmrTableInputRef{
                    .TableId = table.FmrTableId.Id,
                    .TableRanges = {
                        TTableRange{.PartId = FirstPartId, .MinChunk = 0, .MaxChunk = 1}
                    }
                }
            },
            {
                TFmrTableInputRef{
                    .TableId = table.FmrTableId.Id,
                    .TableRanges = {
                        TTableRange{.PartId = FirstPartId, .MinChunk = 1, .MaxChunk = 2}
                    }
                }
            },
            {
                TFmrTableInputRef{
                    .TableId = table.FmrTableId.Id,
                    .TableRanges = {
                        TTableRange{.PartId = FirstPartId, .MinChunk = 2, .MaxChunk = 3}
                    }
                }
            }
        };

        UNIT_ASSERT_EQUAL(ChangeGottenTasksFormat(gottenTasks), expectedTasks);
    }

    Y_UNIT_TEST(WeightCarriesBetweenTables) {
        // Edge case: weight from first table should carry to second table
        std::vector<TFmrTableId> tableIds = {
            TFmrTableId("test_cluster_1", "test_path_1"),
            TFmrTableId("test_cluster_2", "test_path_2")
        };
        std::vector<TOperationTableRef> tables = {
            TFmrTableRef{.FmrTableId = tableIds[0]},
            TFmrTableRef{.FmrTableId = tableIds[1]}
        };

        auto partIdsForTables = GetTestPartIdsForMultipleTables(tableIds);
        auto partIdStats = MergePartIdStats({
            GeneratePartIdStats(FirstTablePartId, {25}),
            GeneratePartIdStats(SecondTablePartId, {25})
        });

        TOrderedPartitionSettings settings{
            .YtPartitionSettings = {},
            .FmrPartitionSettings = {.MaxDataWeightPerPart = 50, .MaxParts = 100}
        };

        TOrderedPartitioner partitioner(partIdsForTables, partIdStats, settings);
        auto [gottenTasks, status] = partitioner.PartitionTablesIntoTasksOrdered(
            tables, nullptr, {}
        );

        UNIT_ASSERT_EQUAL(status, true);

        // Both tables should be in same task (25 + 25 = 50)
        std::vector<std::vector<TFmrTableInputRef>> expectedTasks = {
            {
                TFmrTableInputRef{
                    .TableId = tableIds[0].Id,
                    .TableRanges = {
                        TTableRange{.PartId = FirstTablePartId, .MinChunk = 0, .MaxChunk = 1}
                    }
                },
                TFmrTableInputRef{
                    .TableId = tableIds[1].Id,
                    .TableRanges = {
                        TTableRange{.PartId = SecondTablePartId, .MinChunk = 0, .MaxChunk = 1}
                    }
                }
            }
        };

        UNIT_ASSERT_EQUAL(ChangeGottenTasksFormat(gottenTasks), expectedTasks);
    }

    Y_UNIT_TEST(TableEndsAtLimitStartsNewTask) {
        // Edge case: first table ends with weight == limit, next table starts fresh
        std::vector<TFmrTableId> tableIds = {
            TFmrTableId("test_cluster_1", "test_path_1"),
            TFmrTableId("test_cluster_2", "test_path_2")
        };
        std::vector<TOperationTableRef> tables = {
            TFmrTableRef{.FmrTableId = tableIds[0]},
            TFmrTableRef{.FmrTableId = tableIds[1]}
        };

        auto partIdsForTables = GetTestPartIdsForMultipleTables(tableIds);
        auto partIdStats = MergePartIdStats({
            GeneratePartIdStats(FirstTablePartId, {50}),
            GeneratePartIdStats(SecondTablePartId, {30})
        });

        TOrderedPartitionSettings settings{
            .YtPartitionSettings = {},
            .FmrPartitionSettings = {.MaxDataWeightPerPart = 50, .MaxParts = 100}
        };

        TOrderedPartitioner partitioner(partIdsForTables, partIdStats, settings);
        auto [gottenTasks, status] = partitioner.PartitionTablesIntoTasksOrdered(
            tables, nullptr, {}
        );

        UNIT_ASSERT_EQUAL(status, true);

        // First table hits limit exactly, second table starts new task
        std::vector<std::vector<TFmrTableInputRef>> expectedTasks = {
            {
                TFmrTableInputRef{
                    .TableId = tableIds[0].Id,
                    .TableRanges = {
                        TTableRange{.PartId = FirstTablePartId, .MinChunk = 0, .MaxChunk = 1}
                    }
                }
            },
            {
                TFmrTableInputRef{
                    .TableId = tableIds[1].Id,
                    .TableRanges = {
                        TTableRange{.PartId = SecondTablePartId, .MinChunk = 0, .MaxChunk = 1}
                    }
                }
            }
        };

        UNIT_ASSERT_EQUAL(ChangeGottenTasksFormat(gottenTasks), expectedTasks);
    }

    Y_UNIT_TEST(AllChunksInOneTaskWhenLastExceedsLimit) {
        // Edge case: last chunk is much larger than limit
        TFmrTableId tableId("test_cluster_1", "test_path_1");
        TFmrTableRef table = TFmrTableRef{.FmrTableId = tableId};

        auto partIdsForTables = GetTestPartIdsForSingleTable(tableId);
        auto partIdStats = GeneratePartIdStats(FirstPartId, {10, 20, 200});

        TOrderedPartitionSettings settings{
            .YtPartitionSettings = {},
            .FmrPartitionSettings = {.MaxDataWeightPerPart = 50, .MaxParts = 100}
        };

        TOrderedPartitioner partitioner(partIdsForTables, partIdStats, settings);
        auto [gottenTasks, status] = partitioner.PartitionTablesIntoTasksOrdered(
            {table}, nullptr, {}
        );

        UNIT_ASSERT_EQUAL(status, true);

        // Takes all chunks [0,3) weight 230, isLast+isFull → single task
        std::vector<std::vector<TFmrTableInputRef>> expectedTasks = {
            {
                TFmrTableInputRef{
                    .TableId = table.FmrTableId.Id,
                    .TableRanges = {
                        TTableRange{.PartId = FirstPartId, .MinChunk = 0, .MaxChunk = 3}
                    }
                }
            }
        };

        UNIT_ASSERT_EQUAL(ChangeGottenTasksFormat(gottenTasks), expectedTasks);
    }

    Y_UNIT_TEST(MultiplePartitionsInOneTask) {
        // Edge case: weight carries across partition boundaries
        TFmrTableId tableId("test_cluster_1", "test_path_1");
        TFmrTableRef table = TFmrTableRef{.FmrTableId = tableId};

        std::unordered_map<TFmrTableId, std::vector<TString>> partIdsForTables = {
            {tableId, {FirstPartId, SecondPartId}}
        };
        auto partIdStats = MergePartIdStats({
            GeneratePartIdStats(FirstPartId, {20}),
            GeneratePartIdStats(SecondPartId, {30})
        });

        TOrderedPartitionSettings settings{
            .YtPartitionSettings = {},
            .FmrPartitionSettings = {.MaxDataWeightPerPart = 50, .MaxParts = 100}
        };

        TOrderedPartitioner partitioner(partIdsForTables, partIdStats, settings);
        auto [gottenTasks, status] = partitioner.PartitionTablesIntoTasksOrdered(
            {table}, nullptr, {}
        );

        UNIT_ASSERT_EQUAL(status, true);

        // Both partitions fit in one task (20 + 30 = 50)
        std::vector<std::vector<TFmrTableInputRef>> expectedTasks = {
            {
                TFmrTableInputRef{
                    .TableId = table.FmrTableId.Id,
                    .TableRanges = {
                        TTableRange{.PartId = FirstPartId, .MinChunk = 0, .MaxChunk = 1},
                        TTableRange{.PartId = SecondPartId, .MinChunk = 0, .MaxChunk = 1}
                    }
                }
            }
        };

        UNIT_ASSERT_EQUAL(ChangeGottenTasksFormat(gottenTasks), expectedTasks);
    }

    Y_UNIT_TEST(PartitionMixedYtAndFmrTablesOrdered) {
        // YT and FMR tables are mixed; ordered partitioner should flush FMR scope around YT tables,
        // while YT partitions are always separate tasks.
        TYtTableRef yt1(TString("yt_cluster_1"), TString("yt_path_1"));
        TYtTableRef yt2(TString("yt_cluster_2"), TString("yt_path_2"));

        TFmrTableId fmrId1("fmr_cluster_1", "fmr_path_1");
        TFmrTableId fmrId2("fmr_cluster_2", "fmr_path_2");

        std::vector<TOperationTableRef> inputTables = {
            yt1,
            TFmrTableRef{.FmrTableId = fmrId1},
            yt2,
            TFmrTableRef{.FmrTableId = fmrId2}
        };

        auto partIdsForTables = GetTestPartIdsForMultipleTables({fmrId1, fmrId2});
        auto partIdStats = MergePartIdStats({
            GeneratePartIdStats(FirstTablePartId, {40, 20}),    // -> two ranges due to max=50: [0,2) (60) then task split at "full"
            GeneratePartIdStats(SecondTablePartId, {10, 10, 10}) // -> [0,3)
        });

        TOrderedPartitionSettings settings{
            .YtPartitionSettings = {.MaxDataWeightPerPart = 1, .MaxParts = 100, .PartitionMode = NYT::ETablePartitionMode::Ordered},
            .FmrPartitionSettings = {.MaxDataWeightPerPart = 50, .MaxParts = 100}
        };

        TOrderedPartitioner partitioner(partIdsForTables, partIdStats, settings);

        auto ytService = MakeIntrusive<TMockYtCoordinatorService>();
        ytService->SetPartitionsForTable(
            TFmrTableId(yt1.GetCluster(), yt1.GetPath()),
            {MakeYtTaskRef(yt1.GetCluster(), yt1.GetPath(), "#p0"), MakeYtTaskRef(yt1.GetCluster(), yt1.GetPath(), "#p1")}
        );
        ytService->SetPartitionsForTable(
            TFmrTableId(yt2.GetCluster(), yt2.GetPath()),
            {MakeYtTaskRef(yt2.GetCluster(), yt2.GetPath(), "#p0")}
        );

        auto partitionResult = PartitionInputTablesIntoTasksOrdered(inputTables, partitioner, ytService, {});
        UNIT_ASSERT_EQUAL(partitionResult.PartitionStatus, true);

        std::vector<TTaskTableInputRef> expected;
        // yt1 partitions
        expected.push_back(TTaskTableInputRef{.Inputs = {MakeYtTaskRef(yt1.GetCluster(), yt1.GetPath(), "#p0")}});
        expected.push_back(TTaskTableInputRef{.Inputs = {MakeYtTaskRef(yt1.GetCluster(), yt1.GetPath(), "#p1")}});
        // fmr1 scope (single table)
        expected.push_back(TTaskTableInputRef{.Inputs = {TFmrTableInputRef{
            .TableId = fmrId1.Id,
            .TableRanges = {TTableRange{.PartId = FirstTablePartId, .MinChunk = 0, .MaxChunk = 2}}
        }}});
        // yt2 partitions
        expected.push_back(TTaskTableInputRef{.Inputs = {MakeYtTaskRef(yt2.GetCluster(), yt2.GetPath(), "#p0")}});
        // fmr2 scope (single table)
        expected.push_back(TTaskTableInputRef{.Inputs = {TFmrTableInputRef{
            .TableId = fmrId2.Id,
            .TableRanges = {TTableRange{.PartId = SecondTablePartId, .MinChunk = 0, .MaxChunk = 3}}
        }}});

        UNIT_ASSERT_EQUAL(MakeComparableTasks(partitionResult.TaskInputs), MakeComparableTasks(expected));
    }

    Y_UNIT_TEST(PartitionMixedFmrScopeCarriesWeightUntilYt) {
        // FMR tables before the first YT should be partitioned as one scope,
        // so weight can carry across table boundaries inside that scope.
        TYtTableRef yt1(TString("yt_cluster_1"), TString("yt_path_1"));

        TFmrTableId fmrId1("fmr_cluster_1", "fmr_path_1");
        TFmrTableId fmrId2("fmr_cluster_2", "fmr_path_2");
        TFmrTableId fmrId3("fmr_cluster_3", "fmr_path_3");

        std::vector<TOperationTableRef> inputTables = {
            TFmrTableRef{.FmrTableId = fmrId1},
            TFmrTableRef{.FmrTableId = fmrId2},
            yt1,
            TFmrTableRef{.FmrTableId = fmrId3}
        };

        auto partIdsForTables = GetTestPartIdsForMultipleTables({fmrId1, fmrId2, fmrId3});
        auto partIdStats = MergePartIdStats({
            GeneratePartIdStats(FirstTablePartId, {25}),   // table1: 25
            GeneratePartIdStats(SecondTablePartId, {25}),  // table2: 25 -> carried, total 50 => same task
            GeneratePartIdStats(ThirdTablePartId, {30})    // table3 after YT, new scope => separate task
        });

        TOrderedPartitionSettings settings{
            .YtPartitionSettings = {.MaxDataWeightPerPart = 1, .MaxParts = 100, .PartitionMode = NYT::ETablePartitionMode::Ordered},
            .FmrPartitionSettings = {.MaxDataWeightPerPart = 50, .MaxParts = 100}
        };

        TOrderedPartitioner partitioner(partIdsForTables, partIdStats, settings);

        auto ytService = MakeIntrusive<TMockYtCoordinatorService>();
        ytService->SetPartitionsForTable(
            TFmrTableId(yt1.GetCluster(), yt1.GetPath()),
            {MakeYtTaskRef(yt1.GetCluster(), yt1.GetPath(), "#p0"), MakeYtTaskRef(yt1.GetCluster(), yt1.GetPath(), "#p1")}
        );

        auto partitionResult = PartitionInputTablesIntoTasksOrdered(inputTables, partitioner, ytService, {});
        UNIT_ASSERT_EQUAL(partitionResult.PartitionStatus, true);

        std::vector<TTaskTableInputRef> expected;
        // FMR scope before YT: both tables in one task due to carry (25 + 25 = 50)
        expected.push_back(TTaskTableInputRef{.Inputs = {
            TFmrTableInputRef{
                .TableId = fmrId1.Id,
                .TableRanges = {TTableRange{.PartId = FirstTablePartId, .MinChunk = 0, .MaxChunk = 1}}
            },
            TFmrTableInputRef{
                .TableId = fmrId2.Id,
                .TableRanges = {TTableRange{.PartId = SecondTablePartId, .MinChunk = 0, .MaxChunk = 1}}
            }
        }});
        // YT partitions
        expected.push_back(TTaskTableInputRef{.Inputs = {MakeYtTaskRef(yt1.GetCluster(), yt1.GetPath(), "#p0")}});
        expected.push_back(TTaskTableInputRef{.Inputs = {MakeYtTaskRef(yt1.GetCluster(), yt1.GetPath(), "#p1")}});
        // FMR after YT: new scope
        expected.push_back(TTaskTableInputRef{.Inputs = {TFmrTableInputRef{
            .TableId = fmrId3.Id,
            .TableRanges = {TTableRange{.PartId = ThirdTablePartId, .MinChunk = 0, .MaxChunk = 1}}
        }}});

        UNIT_ASSERT_EQUAL(MakeComparableTasks(partitionResult.TaskInputs), MakeComparableTasks(expected));
    }

    Y_UNIT_TEST(PartitionMixedYtBreaksFmrScopeNoCarryAcrossYt) {
        // Without YT between FMR tables, weights could carry and pack into one task,
        // but YT must flush the FMR scope, so carry must NOT happen across the YT boundary.
        TYtTableRef yt1(TString("yt_cluster_1"), TString("yt_path_1"));

        TFmrTableId fmrId1("fmr_cluster_1", "fmr_path_1");
        TFmrTableId fmrId2("fmr_cluster_2", "fmr_path_2");

        std::vector<TOperationTableRef> inputTables = {
            TFmrTableRef{.FmrTableId = fmrId1},
            yt1,
            TFmrTableRef{.FmrTableId = fmrId2}
        };

        auto partIdsForTables = GetTestPartIdsForMultipleTables({fmrId1, fmrId2});
        auto partIdStats = MergePartIdStats({
            GeneratePartIdStats(FirstTablePartId, {25}),
            GeneratePartIdStats(SecondTablePartId, {25})
        });

        TOrderedPartitionSettings settings{
            .YtPartitionSettings = {.MaxDataWeightPerPart = 1, .MaxParts = 100, .PartitionMode = NYT::ETablePartitionMode::Ordered},
            .FmrPartitionSettings = {.MaxDataWeightPerPart = 50, .MaxParts = 100}
        };

        TOrderedPartitioner partitioner(partIdsForTables, partIdStats, settings);

        auto ytService = MakeIntrusive<TMockYtCoordinatorService>();
        ytService->SetPartitionsForTable(
            TFmrTableId(yt1.GetCluster(), yt1.GetPath()),
            {MakeYtTaskRef(yt1.GetCluster(), yt1.GetPath(), "#p0")}
        );

        auto partitionResult = PartitionInputTablesIntoTasksOrdered(inputTables, partitioner, ytService, {});
        UNIT_ASSERT_EQUAL(partitionResult.PartitionStatus, true);

        std::vector<TTaskTableInputRef> expected;
        // FMR1 scope (flushed by YT): must be its own task
        expected.push_back(TTaskTableInputRef{.Inputs = {TFmrTableInputRef{
            .TableId = fmrId1.Id,
            .TableRanges = {TTableRange{.PartId = FirstTablePartId, .MinChunk = 0, .MaxChunk = 1}}
        }}});
        // YT task
        expected.push_back(TTaskTableInputRef{.Inputs = {MakeYtTaskRef(yt1.GetCluster(), yt1.GetPath(), "#p0")}});
        // FMR2 scope: separate task, even though 25+25 would fit without YT
        expected.push_back(TTaskTableInputRef{.Inputs = {TFmrTableInputRef{
            .TableId = fmrId2.Id,
            .TableRanges = {TTableRange{.PartId = SecondTablePartId, .MinChunk = 0, .MaxChunk = 1}}
        }}});

        UNIT_ASSERT_EQUAL(MakeComparableTasks(partitionResult.TaskInputs), MakeComparableTasks(expected));
    }
}

} // namespace NYql::NFmr
