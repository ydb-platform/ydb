#include "yql_yt_sorted_partitioner_base_ut.h"

namespace NYql::NFmr::NPartitionerTest {

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

            const bool leftInclusive = ref->IsFirstRowInclusive.Defined() ? *ref->IsFirstRowInclusive : true;
            const bool rightInclusive = ref->IsLastRowInclusive.Defined() ? *ref->IsLastRowInclusive : true;
            const TString left = ref->FirstRowKeys ? ExtractKeyValueK(*ref->FirstRowKeys) : TString();
            const TString right = ref->LastRowKeys ? ExtractKeyValueK(*ref->LastRowKeys) : TString();

            TStringBuilder s;
            s << "TAB" << tableIdx << "-" << (leftInclusive ? "[" : "(") << left << ":" << right << (rightInclusive ? "]" : ")");
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

void CheckPartitionCorrectness(const std::vector<TCase>& cases) {
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

        TFmrPartitionerSettings fmrPartitionSettings {.MaxDataWeightPerPart = c.MaxWeight, .MaxParts = 1000};
        TSortingColumns keyColumns{.Columns = {"k"}, .SortOrders = {ESortOrder::Ascending}};

        TSortedPartitionerBase::TPtr partitioner;
        if (c.PartitionerType == EPartitionerType::Sort) {
            TSortedPartitionSettings settings;
            settings.FmrPartitionSettings = fmrPartitionSettings;
            partitioner = MakeIntrusive<TSortedPartitioner>(partIdsForTables, partIdStats, keyColumns, settings);
        } else {
            TReducePartitionSettings settings;
            settings.FmrPartitionSettings = fmrPartitionSettings;
            settings.MaxKeySizePerPart = c.MaxKeySizePerPart;
            partitioner = MakeIntrusive<TReducePartitioner>(partIdsForTables, partIdStats, keyColumns, settings);
        }

        auto [tasks, error] = partitioner->PartitionTablesIntoTasks(inputTables);
        if (error) {
            YQL_ENSURE(!c.ExpectedError.empty(), "Partitioner fell with unexpected error " << error->ErrorMessage);
            UNIT_ASSERT(error->ErrorMessage.Contains(c.ExpectedError));
        } else {
            const auto actual = NormalizeTasksAsProtoStrings(tasks, c.Input.size());
            UNIT_ASSERT_VALUES_EQUAL_C(
                DumpTasks(actual),
                DumpTasks(c.Expected),
                TStringBuilder() << "caseIdx=" << caseIdx
            );
        }
    }
}

} // namespace NYql::NFmr::NPartitionerTest
