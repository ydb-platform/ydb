#include "source.h"
#include "interval.h"
#include "fetched_data.h"
#include "constructor.h"
#include "plain_read_data.h"
#include <ydb/core/formats/arrow/serializer/full.h>

namespace NKikimr::NOlap::NPlainReader {

void IDataSource::InitFF(const std::shared_ptr<arrow::RecordBatch>& batch) {
    NActors::TLogContextGuard logGuard(NActors::TLogContextBuilder::Build()("source", SourceIdx)("method", "InitFF"));
    Y_VERIFY(!FFData);
    FFData = std::make_shared<TFullData>(batch);
    auto intervals = Intervals;
    for (auto&& i : intervals) {
        i->OnSourceFFReady(GetSourceIdx());
    }
}

void IDataSource::InitEF(const std::shared_ptr<NArrow::TColumnFilter>& appliedFilter, const std::shared_ptr<NArrow::TColumnFilter>& earlyFilter, const std::shared_ptr<arrow::RecordBatch>& batch) {
    NActors::TLogContextGuard logGuard(NActors::TLogContextBuilder::Build()("source", SourceIdx)("method", "InitEF"));
    Y_VERIFY(!EFData);
    EFData = std::make_shared<TEarlyFilterData>(appliedFilter, earlyFilter, batch);
    auto intervals = Intervals;
    for (auto&& i : intervals) {
        i->OnSourceEFReady(GetSourceIdx());
    }
    NeedPK();
}

void IDataSource::InitPK(const std::shared_ptr<arrow::RecordBatch>& batch) {
    NActors::TLogContextGuard logGuard(NActors::TLogContextBuilder::Build()("source", SourceIdx)("method", "InitPK"));
    Y_VERIFY(!PKData);
    PKData = std::make_shared<TPrimaryKeyData>(batch);
    auto intervals = Intervals;
    for (auto&& i : intervals) {
        i->OnSourcePKReady(GetSourceIdx());
    }
    NeedFF();
}

void IDataSource::NeedEF() {
    NActors::TLogContextGuard logGuard(NActors::TLogContextBuilder::Build()("source", SourceIdx)("method", "NeedEF"));
    if (!NeedEFFlag) {
        NeedEFFlag = true;
        DoFetchEF();
    }
}

void IDataSource::NeedPK() {
    NActors::TLogContextGuard logGuard(NActors::TLogContextBuilder::Build()("source", SourceIdx)("method", "NeedPK"));
    if (!NeedPKFlag) {
        NeedPKFlag = true;
        DoFetchPK();
    }
}

void IDataSource::NeedFF() {
    NActors::TLogContextGuard logGuard(NActors::TLogContextBuilder::Build()("source", SourceIdx)("method", "NeedFF"));
    if (!NeedFFFlag) {
        NeedFFFlag = true;
        DoFetchFF();
    }
}

bool IDataSource::OnIntervalFinished(const ui32 intervalIdx) {
    Y_VERIFY(Intervals.size());
    Y_VERIFY(Intervals.front()->GetIntervalIdx() == intervalIdx);
    Intervals.pop_front();
    return Intervals.empty();
}

void TPortionDataSource::NeedFetchColumns(const std::set<ui32>& columnIds, std::shared_ptr<IFetchTaskConstructor> constructor) {
    for (auto&& i : columnIds) {
        auto columnChunks = Portion->GetColumnChunksPointers(i);
        for (auto&& c : columnChunks) {
            constructor->AddChunk(*c);
            Y_VERIFY(BlobsWaiting.emplace(c->BlobRange, constructor).second);
            ReadData.AddBlobForFetch(GetSourceIdx(), c->BlobRange);
        }
    }
    constructor->StartDataWaiting();
}

void TPortionDataSource::DoFetchEF() {
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "DoFetchEF");
    if (ReadData.GetEFColumnIds().size()) {
        NeedFetchColumns(ReadData.GetEFColumnIds(), std::make_shared<TEFTaskConstructor>(ReadData.GetEFColumnIds(), *this, ReadData));
    } else {
        InitEF(nullptr, nullptr, nullptr);
    }
}

void TPortionDataSource::DoFetchPK() {
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "DoFetchPK");
    if (ReadData.GetPKColumnIds().size()) {
        NeedFetchColumns(ReadData.GetPKColumnIds(), std::make_shared<TPKColumnsTaskConstructor>(ReadData.GetPKColumnIds(), *this, ReadData));
    } else {
        InitPK(nullptr);
    }
}

void TPortionDataSource::DoFetchFF() {
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "DoFetchFF");
    if (ReadData.GetFFColumnIds().size()) {
        NeedFetchColumns(ReadData.GetFFColumnIds(), std::make_shared<TFFColumnsTaskConstructor>(ReadData.GetFFColumnIds(), *this, ReadData));
    } else {
        InitFF(nullptr);
    }
}

void TPortionDataSource::AddData(const TBlobRange& range, TString&& data) {
    auto it = BlobsWaiting.find(range);
    Y_VERIFY(it != BlobsWaiting.end());
    it->second->AddData(range, std::move(data));
    BlobsWaiting.erase(it);
}

void TPortionDataSource::DoAbort() {
    for (auto&& i : BlobsWaiting) {
        i.second->Abort();
    }
}

void TCommittedDataSource::DoFetch() {
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "DoFetch");
    if (!ReadStarted) {
        Y_VERIFY(!ResultReady);
        ReadStarted = true;
        ReadData.AddBlobForFetch(GetSourceIdx(), TBlobRange(CommittedBlob.GetBlobId(), 0, CommittedBlob.GetBlobId().BlobSize()));
    }
}

void TCommittedDataSource::AddData(const TBlobRange& /*range*/, TString&& data) {
    Y_VERIFY(!ResultReady);
    ResultReady = true;
    auto resultBatch = NArrow::DeserializeBatch(data, ReadData.GetReadMetadata()->GetBlobSchema(CommittedBlob.GetSchemaSnapshot()));
    Y_VERIFY(resultBatch);
    resultBatch = ReadData.GetReadMetadata()->GetIndexInfo().AddSpecialColumns(resultBatch, CommittedBlob.GetSnapshot());
    Y_VERIFY(resultBatch);
    ReadData.GetReadMetadata()->GetPKRangesFilter().BuildFilter(resultBatch).Apply(resultBatch);
    InitEF(nullptr, ReadData.GetReadMetadata()->GetProgram().BuildEarlyFilter(resultBatch), NArrow::ExtractColumnsValidate(resultBatch, ReadData.GetEFColumnNames()));
    InitPK(NArrow::ExtractColumnsValidate(resultBatch, ReadData.GetPKColumnNames()));
    InitFF(NArrow::ExtractColumnsValidate(resultBatch, ReadData.GetFFColumnNames()));
}

}
