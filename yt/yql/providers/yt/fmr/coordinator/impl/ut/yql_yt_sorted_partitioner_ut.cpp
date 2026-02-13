#include <library/cpp/testing/unittest/registar.h>

#include <yt/yql/providers/yt/fmr/coordinator/impl/yql_yt_sorted_partitioner.h>
#include <yt/yql/providers/yt/fmr/test_tools/sorted_partitioner/yql_yt_sorted_partitioner_test_tools.h>

#include <util/string/builder.h>

namespace NYql::NFmr {

namespace {

NYT::TNode Key(ui64 v) {
    NYT::TNode m = NYT::TNode::CreateMap();
    m["k"] = static_cast<i64>(v);
    return m;
}

TChunkStats MakeSortedChunk(ui64 weight, ui64 firstK, ui64 lastK) {
    TSortedChunkStats s;
    s.IsSorted = true;
    s.FirstRowKeys = Key(firstK);
    s.LastRowKeys = Key(lastK);
    return TChunkStats{.Rows = 1, .DataWeight = weight, .SortedChunkStats = s};
}

NYT::TNode Key(const TString& v) {
    NYT::TNode m = NYT::TNode::CreateMap();
    m["k"] = v;
    return m;
}

TChunkStats MakeSortedChunk(ui64 weight, const TString& firstK, const TString& lastK) {
    TSortedChunkStats s;
    s.IsSorted = true;
    s.FirstRowKeys = Key(firstK);
    s.LastRowKeys = Key(lastK);
    return TChunkStats{.Rows = 1, .DataWeight = weight, .SortedChunkStats = s};
}

void AssertTaskHasRangesForTable(
    const TTaskTableInputRef& task,
    const TString& tableId,
    size_t expectedRangesCount,
    bool expectLowerKey,
    bool expectUpperKey
) {
    for (const auto& input : task.Inputs) {
        if (auto* fmrInput = std::get_if<TFmrTableInputRef>(&input)) {
            if (fmrInput->TableId == tableId) {
                UNIT_ASSERT_VALUES_EQUAL(fmrInput->TableRanges.size(), expectedRangesCount);
                UNIT_ASSERT_VALUES_EQUAL(fmrInput->FirstRowKeys.Defined(), expectLowerKey);
                UNIT_ASSERT_VALUES_EQUAL(fmrInput->LastRowKeys.Defined(), expectUpperKey);
                if (expectLowerKey) {
                    UNIT_ASSERT_C(fmrInput->IsFirstRowInclusive.Defined(), "IsFirstRowInclusive must be set when FirstRowKeys is set");
                }
                return;
            }
        }
    }
    UNIT_FAIL("Table not found in task inputs: " + tableId);
}

TString ExtractKeyValueK(const TString& ysonRow) {
    const NYT::TNode n = NYT::NodeFromYsonString(ysonRow);
    return n["k"].AsString();
}

TVector<TVector<TString>> NormalizeTasksAsProtoStrings(const std::vector<TTaskTableInputRef>& tasks, size_t tablesCount) {
    TVector<TVector<TString>> out;
    out.reserve(tasks.size());

    for (const auto& task : tasks) {
        TVector<TString> parts;
        parts.reserve(tablesCount);

        for (size_t tableIdx = 0; tableIdx < tablesCount; ++tableIdx) {
            const TString tableId = TStringBuilder() << "c.t" << tableIdx;

            const TFmrTableInputRef* ref = nullptr;
            for (const auto& input : task.Inputs) {
                const auto* fmr = std::get_if<TFmrTableInputRef>(&input);
                if (!fmr) {
                    continue;
                }
                if (fmr->TableId == tableId) {
                    ref = fmr;
                    break;
                }
            }
            if (!ref) {
                continue;
            }

            const bool inclusive = ref->IsFirstRowInclusive.Defined() ? *ref->IsFirstRowInclusive : true;
            const TString left = ref->FirstRowKeys ? ExtractKeyValueK(*ref->FirstRowKeys) : TString();
            const TString right = ref->LastRowKeys ? ExtractKeyValueK(*ref->LastRowKeys) : TString();

            TStringBuilder s;
            s << "TAB" << tableIdx << "-" << (inclusive ? "[" : "(") << left << ":" << right << "]";
            parts.push_back(s);
        }

        out.push_back(std::move(parts));
    }

    return out;
}

TString DumpTasks(const TVector<TVector<TString>>& tasks) {
    TStringBuilder b;
    b << "[";
    for (size_t i = 0; i < tasks.size(); ++i) {
        if (i) {
            b << ", ";
        }
        b << "[";
        for (size_t j = 0; j < tasks[i].size(); ++j) {
            if (j) {
                b << ", ";
            }
            b << tasks[i][j];
        }
        b << "]";
    }
    b << "]";
    return b;
}

} // namespace

Y_UNIT_TEST_SUITE(SortedPartitionerTests) {
    Y_UNIT_TEST(SplitsIntoKeyAlignedTasksAndSetsBounds) {
        TFmrTableId t1("c", "t1");
        TFmrTableId t2("c", "t2");

        TFmrTableRef r1{.FmrTableId = t1};
        TFmrTableRef r2{.FmrTableId = t2};

        std::unordered_map<TFmrTableId, std::vector<TString>> partIdsForTables{
            {t1, {"p1"}},
            {t2, {"p2"}},
        };

        std::unordered_map<TString, std::vector<TChunkStats>> partIdStats{
            {"p1", {MakeSortedChunk(10, 1, 5), MakeSortedChunk(10, 6, 10)}},
            {"p2", {MakeSortedChunk(10, 3, 7), MakeSortedChunk(10, 8, 12)}},
        };

        TSortedPartitionSettings settings;
        settings.FmrPartitionSettings = {.MaxDataWeightPerPart = 15, .MaxParts = 1000};
        TSortingColumns keyColumns{.Columns = {"k"}, .SortOrders = {ESortOrder::Ascending}};

        TSortedPartitioner partitioner(partIdsForTables, partIdStats, keyColumns, settings);
        auto [tasks, ok] = partitioner.PartitionTablesIntoTasksSorted({r1, r2});
        UNIT_ASSERT(ok);
        UNIT_ASSERT(!tasks.empty());

        const auto& task0 = tasks[0];
        UNIT_ASSERT_VALUES_EQUAL(task0.Inputs.size(), 2);

        AssertTaskHasRangesForTable(task0, t1.Id, 1, true, true);
        AssertTaskHasRangesForTable(task0, t2.Id, 1, true, true);
    }

    Y_UNIT_TEST(FailsOnEmptyPartitionsForInputTable) {
        TFmrTableId t1("c", "t1");
        TFmrTableRef r1{.FmrTableId = t1};

        std::unordered_map<TFmrTableId, std::vector<TString>> partIdsForTables{
            {t1, {}},
        };
        std::unordered_map<TString, std::vector<TChunkStats>> partIdStats;

        TSortedPartitionSettings settings;
        settings.FmrPartitionSettings = {.MaxDataWeightPerPart = 10, .MaxParts = 1000};
        TSortingColumns keyColumns{.Columns = {"k"}, .SortOrders = {ESortOrder::Ascending}};

        TSortedPartitioner partitioner(partIdsForTables, partIdStats, keyColumns, settings);
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            partitioner.PartitionTablesIntoTasksSorted({r1}),
            yexception,
            "at least one partition");
    }

    Y_UNIT_TEST(FailsOnEmptyChunksForInputTable) {
        TFmrTableId t1("c", "t1");
        TFmrTableRef r1{.FmrTableId = t1};

        std::unordered_map<TFmrTableId, std::vector<TString>> partIdsForTables{
            {t1, {"p1"}},
        };
        std::unordered_map<TString, std::vector<TChunkStats>> partIdStats{
            {"p1", {}},
        };

        TSortedPartitionSettings settings;
        settings.FmrPartitionSettings = {.MaxDataWeightPerPart = 10, .MaxParts = 1000};
        TSortingColumns keyColumns{.Columns = {"k"}, .SortOrders = {ESortOrder::Ascending}};

        TSortedPartitioner partitioner(partIdsForTables, partIdStats, keyColumns, settings);
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            partitioner.PartitionTablesIntoTasksSorted({r1}),
            yexception,
            "at least one chunk");
    }

    Y_UNIT_TEST(MergeMatchesSourceMultipleChunks) {
        const TVector<TString> keyColumns = {"k"};

        NTestTools::TBoundaryKeyTableSpec specTable1{.Path = "t1", .PartId = "p1"};
        NTestTools::TBoundaryKeyTableSpec specTable2{.Path = "t2", .PartId = "p2"};

        TVector<NTestTools::TGeneratedSortedTable> tables;
        tables.push_back(NTestTools::GenerateBoundaryKeySpanningMultipleChunksTable(specTable1, /*idBase*/ 1));
        tables.push_back(NTestTools::GenerateBoundaryKeySpanningMultipleChunksTable(specTable2, /*idBase*/ 10'000'000));

        std::unordered_map<TFmrTableId, std::vector<TString>> partIdsForTables;
        std::unordered_map<TString, std::vector<TChunkStats>> partIdStats;
        NTestTools::BuildPartitionerInputs(tables, partIdsForTables, partIdStats);

        TFmrTableRef refTable1{.FmrTableId = tables[0].TableId};
        TFmrTableRef refTable2{.FmrTableId = tables[1].TableId};

        const auto srcFingerprints = NTestTools::CountSourceFingerprintsFromFiles(tables, keyColumns);

        const TVector<ui64> weightsToTest = {
            1,
            15,
            100
        };

        for (ui64 maxWeight : weightsToTest) {
            TSortedPartitionSettings settings;
            settings.FmrPartitionSettings = {.MaxDataWeightPerPart = maxWeight, .MaxParts = 1000};
            TSortingColumns sortingColumns{.Columns = keyColumns, .SortOrders = {ESortOrder::Ascending}};

            TSortedPartitioner partitioner(partIdsForTables, partIdStats, sortingColumns, settings);
            auto [tasks, ok] = partitioner.PartitionTablesIntoTasksSorted({refTable1, refTable2});
            UNIT_ASSERT_C(ok, "Partitioner returned non-ok");
            UNIT_ASSERT_C(!tasks.empty(), "Partitioner returned no tasks");

            const auto dstFingerprints = NTestTools::CountTaskFingerprintsFromFiles(tasks, tables, keyColumns);

            auto [onlyInSrc, onlyInDst] = NTestTools::DiffFingerprintMultisets(srcFingerprints, dstFingerprints);
            const ui64 missingRows = onlyInSrc.size();
            const ui64 extraRows = onlyInDst.size();

            UNIT_ASSERT_VALUES_EQUAL_C(missingRows, 0, "Missing rows after partitioning (lost data)");
            UNIT_ASSERT_VALUES_EQUAL_C(extraRows, 0, "Extra/duplicate rows after partitioning");
        }
    }

    Y_UNIT_TEST(PrototypeLikePartitioningCases) {
        struct TCase {
            TVector<TVector<std::pair<TString, TString>>> Input;
            TVector<TVector<TString>> Expected;
            ui64 MaxWeight = 1;
        };

        const TVector<TCase> cases = {
            {
                .Input = {
                    {{"A","B"}, {"B","C"}, {"C","D"}},
                    {{"A","D"}},
                },
                .Expected = {
                    {"TAB0-[A:B]", "TAB1-[A:B]"},
                    {"TAB0-[B:C]", "TAB1-(B:C]"},
                    {"TAB0-[C:D]", "TAB1-(C:D]"},
                },
                .MaxWeight = 1
            },
            {
                .Input = {
                    {{"A","B"}, {"B","B"}, {"B","B"}, {"B","C"}},
                    {{"A","D"}},
                },
                .Expected = {
                    {"TAB0-[A:B]", "TAB1-[A:B]"},
                    {"TAB0-[B:B]"},
                    {"TAB0-[B:B]"},
                    {"TAB0-[B:C]", "TAB1-(B:C]"},
                    {"TAB1-(C:D]"},
                },
                .MaxWeight = 1
            },
            {
                .Input = {
                    {{"A","B"}, {"B","B"}, {"B","B"}, {"B","C"}},
                    {{"A","D"}},
                },
                .Expected = {
                    {"TAB0-[A:C]", "TAB1-[A:D]"},
                },
                .MaxWeight = 1000
            },
            {
                .Input = {
                    {{"A","B"}, {"B","C"}},
                    {{"X","Y"}},
                },
                .Expected = {
                    {"TAB0-[A:C]", "TAB1-[X:Y]"},
                },
                .MaxWeight = 1000
            },
            {
                .Input = {
                    {{"A","B"}},
                    {{"X","Y"}},
                },
                .Expected = {
                    {"TAB0-[A:B]"},
                    {"TAB1-[X:Y]"},
                },
                .MaxWeight = 1
            },
            {
                .Input = {
                    {{"A","B"}},
                    {{"A","B"}},
                },
                .Expected = {
                    {"TAB0-[A:B]", "TAB1-[A:B]"},
                },
                .MaxWeight = 1
            },
            {
                .Input = {
                    {{"B","B"}, {"B","B"}},
                    {{"A","D"}},
                },
                .Expected = {
                    {"TAB0-[B:B]", "TAB1-[A:D]"},
                },
                .MaxWeight = 1000
            },
            {
                .Input = {
                    {{"B","B"}, {"B","B"}},
                    {{"A","D"}},
                },
                .Expected = {
                    {"TAB0-[B:B]", "TAB1-[A:B]"},
                    {"TAB0-[B:B]"},
                    {"TAB1-(B:D]"},
                },
                .MaxWeight = 1
            },
            {
                .Input = {
                    {{"A","B"}, {"B","C"}},
                    {{"A","D"}},
                },
                .Expected = {
                    {"TAB0-[A:B]", "TAB1-[A:B]"},
                    {"TAB0-[B:C]", "TAB1-(B:C]"},
                    {"TAB1-(C:D]"},
                },
                .MaxWeight = 1
            },
            {
                .Input = {
                    {{"A","B"}, {"B","C"}},
                    {{"A","C"}, {"C","D"}},
                    {{"A","D"}, {"D","E"}},
                    {{"A","E"}, {"E","F"}, {"F","G"}},
                    {{"A","F"}, {"F","G"}, {"G","H"}},
                },
                .Expected = {
                    {"TAB0-[A:B]", "TAB1-[A:B]", "TAB2-[A:B]", "TAB3-[A:B]", "TAB4-[A:B]"},
                    {"TAB0-[B:C]", "TAB1-(B:C]", "TAB2-(B:C]", "TAB3-(B:C]", "TAB4-(B:C]"},
                    {"TAB1-[C:D]", "TAB2-(C:D]", "TAB3-(C:D]", "TAB4-(C:D]"},
                    {"TAB2-[D:E]", "TAB3-(D:E]", "TAB4-(D:E]"},
                    {"TAB3-[E:F]", "TAB4-(E:F]"},
                    {"TAB3-[F:G]", "TAB4-[F:G]"},
                    {"TAB4-[G:H]"},
                },
                .MaxWeight = 1
            },
        };

        for (size_t caseIdx = 0; caseIdx < cases.size(); ++caseIdx) {
            const auto& c = cases[caseIdx];

            std::unordered_map<TFmrTableId, std::vector<TString>> partIdsForTables;
            std::unordered_map<TString, std::vector<TChunkStats>> partIdStats;
            TVector<TOperationTableRef> inputTables;
            inputTables.reserve(c.Input.size());

            for (size_t tableIdx = 0; tableIdx < c.Input.size(); ++tableIdx) {
                TFmrTableId tableId("c", TStringBuilder() << "t" << tableIdx);
                TFmrTableRef ref{.FmrTableId = tableId};
                inputTables.push_back(ref);

                const TString partId = TStringBuilder() << "p" << tableIdx;
                partIdsForTables[tableId] = {partId};

                auto& stats = partIdStats[partId];
                stats.reserve(c.Input[tableIdx].size());
                for (const auto& interval : c.Input[tableIdx]) {
                    stats.push_back(MakeSortedChunk(1, interval.first, interval.second));
                }
            }

            TSortedPartitionSettings settings;
            settings.FmrPartitionSettings = {.MaxDataWeightPerPart = c.MaxWeight, .MaxParts = 1000};
            TSortingColumns keyColumns{.Columns = {"k"}, .SortOrders = {ESortOrder::Ascending}};

            TSortedPartitioner partitioner(partIdsForTables, partIdStats, keyColumns, settings);
            auto [tasks, ok] = partitioner.PartitionTablesIntoTasksSorted(inputTables);
            UNIT_ASSERT_C(ok, "Partitioner returned non-ok");

            const auto actual = NormalizeTasksAsProtoStrings(tasks, c.Input.size());
            UNIT_ASSERT_VALUES_EQUAL_C(
                DumpTasks(actual),
                DumpTasks(c.Expected),
                TStringBuilder() << "caseIdx=" << caseIdx
            );
        }
    }
}

} // namespace NYql::NFmr
