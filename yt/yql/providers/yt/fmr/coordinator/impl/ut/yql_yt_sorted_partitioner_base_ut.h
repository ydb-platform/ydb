#pragma once

#include <library/cpp/testing/unittest/registar.h>

#include <yt/yql/providers/yt/fmr/coordinator/partitioner/yql_yt_sorted_partitioner.h>
#include <yt/yql/providers/yt/fmr/coordinator/partitioner/yql_yt_reduce_partitioner.h>
#include <yt/yql/providers/yt/fmr/test_tools/sorted_partitioner/yql_yt_sorted_partitioner_test_tools.h>

#include <util/string/builder.h>

namespace NYql::NFmr::NPartitionerTest {

NYT::TNode Key(ui64 v);

TChunkStats MakeSortedChunk(ui64 weight, ui64 firstK, ui64 lastK);

NYT::TNode Key(const TString& v);

TChunkStats MakeSortedChunk(ui64 weight, const TString& firstK, const TString& lastK);

void AssertTaskHasRangesForTable(
    const TTaskTableInputRef& task,
    const TString& tableId,
    size_t expectedRangesCount,
    bool expectLowerKey,
    bool expectUpperKey
);

TString ExtractKeyValueK(const TString& ysonRow);

TVector<TVector<TString>> NormalizeTasksAsProtoStrings(const std::vector<TTaskTableInputRef>& tasks, size_t tablesCount);

TString DumpTasks(const TVector<TVector<TString>>& tasks);

enum EPartitionerType {
    Sort,
    Reduce
};

struct TCase {
    TVector<TVector<std::pair<TString, TString>>> Input;
    TVector<TVector<TString>> Expected;
    ui64 MaxWeight = 1;
    EPartitionerType PartitionerType = EPartitionerType::Sort;
    ui64 MaxKeySizePerPart = 1;
    TString ExpectedError;
};

void CheckPartitionCorrectness(const std::vector<TCase>& cases);

} // namespace NYql::NFmr
