#pragma once
#include <ydb/core/base/appdata.h>
#include <ydb/core/formats/arrow/arrow_filter.h>
#include <ydb/core/formats/arrow/common/container.h>
#include <ydb/core/formats/arrow/program/collection.h>
#include <ydb/core/formats/arrow/size_calcer.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/core/tx/columnshard/blob.h>
#include <ydb/core/tx/columnshard/blobs_reader/task.h>
#include <ydb/core/tx/columnshard/engines/portions/data_accessor.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <ydb/core/tx/columnshard/engines/scheme/indexes/abstract/collection.h>
#include <ydb/core/tx/limiter/grouped_memory/usage/abstract.h>

#include <ydb/library/accessor/accessor.h>
#include <ydb/library/actors/core/log.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_base.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/table.h>

namespace NKikimr::NOlap::NReader::NCommon {

class IKernelFetchLogic;

class TFetchedData {
private:
    using TBlobs = THashMap<TChunkAddress, TPortionDataAccessor::TAssembleBlobInfo>;
    using TFetchers = THashMap<ui32, std::shared_ptr<IKernelFetchLogic>>;
    TFetchers Fetchers;
    YDB_ACCESSOR_DEF(TBlobs, Blobs);
    YDB_READONLY_DEF(std::shared_ptr<NArrow::NAccessor::TAccessorsCollection>, Table);
    YDB_READONLY_DEF(std::shared_ptr<NIndexes::TIndexesCollection>, Indexes);
    YDB_READONLY(bool, Aborted, false);

    std::shared_ptr<NGroupedMemoryManager::TAllocationGuard> AccessorsGuard;
    std::optional<TPortionDataAccessor> PortionAccessor;
    THashMap<NArrow::NSSA::IDataSource::TCheckIndexContext, std::shared_ptr<NIndexes::IIndexMeta>> DataAddrToIndex;

public:
    void AddRemapDataToIndex(const NArrow::NSSA::IDataSource::TCheckIndexContext& addr, const std::shared_ptr<NIndexes::IIndexMeta>& index) {
        AFL_VERIFY(DataAddrToIndex.emplace(addr, index).second);
    }

    std::shared_ptr<NIndexes::IIndexMeta> GetRemapDataToIndex(const NArrow::NSSA::IDataSource::TCheckIndexContext& addr) const {
        auto it = DataAddrToIndex.find(addr);
        AFL_VERIFY(it != DataAddrToIndex.end());
        return it->second;
    }

    void AddFetchers(const std::vector<std::shared_ptr<IKernelFetchLogic>>& fetchers);
    void AddFetcher(const std::shared_ptr<IKernelFetchLogic>& fetcher);

    std::shared_ptr<IKernelFetchLogic> ExtractFetcherOptional(const ui32 entityId) {
        auto it = Fetchers.find(entityId);
        if (it == Fetchers.end()) {
            return nullptr;
        } else {
            auto result = it->second;
            Fetchers.erase(it);
            return result;
        }
    }

    std::shared_ptr<IKernelFetchLogic> ExtractFetcherVerified(const ui32 entityId) {
        auto result = ExtractFetcherOptional(entityId);
        AFL_VERIFY(!!result)("column_id", entityId);
        return result;
    }

    void Abort() {
        Aborted = true;
    }

    bool GetUseFilter() const {
        return Table->GetFilterUsage();
    }

    TString DebugString() const {
        return TStringBuilder() << "OK";
    }

    TFetchedData(const bool useFilter, const ui32 recordsCount) {
        Table = std::make_shared<NArrow::NAccessor::TAccessorsCollection>(recordsCount);
        Table->SetFilterUsage(useFilter);
        Indexes = std::make_shared<NIndexes::TIndexesCollection>();
    }

    void SetAccessorsGuard(std::shared_ptr<NGroupedMemoryManager::TAllocationGuard>&& guard) {
        AFL_VERIFY(!AccessorsGuard);
        AFL_VERIFY(!!guard);
        AccessorsGuard = std::move(guard);
    }

    void SetUseFilter(const bool value) {
        Table->SetFilterUsage(value);
    }

    bool HasPortionAccessor() const {
        return !!PortionAccessor;
    }

    void SetPortionAccessor(TPortionDataAccessor&& accessor) {
        AFL_VERIFY(!PortionAccessor);
        PortionAccessor = std::move(accessor);
    }

    const TPortionDataAccessor& GetPortionAccessor() const {
        AFL_VERIFY(!!PortionAccessor);
        return *PortionAccessor;
    }

    ui32 GetFilteredCount(const ui32 recordsCount, const ui32 defLimit) const {
        return Table->GetFilteredCount(recordsCount, defLimit);
    }

    void SyncTableColumns(const std::vector<std::shared_ptr<arrow::Field>>& fields, const ISnapshotSchema& schema, const ui32 recordsCount);

    std::shared_ptr<NArrow::TColumnFilter> GetAppliedFilter() const {
        return Table->GetAppliedFilter();
    }

    std::shared_ptr<NArrow::TColumnFilter> GetNotAppliedFilter() const {
        return Table->GetNotAppliedFilter();
    }

    TString ExtractBlob(const TChunkAddress& address) {
        auto it = Blobs.find(address);
        AFL_VERIFY(it != Blobs.end());
        AFL_VERIFY(it->second.IsBlob());
        auto result = it->second.GetData();
        Blobs.erase(it);
        return result;
    }

    void AddBatch(
        const std::shared_ptr<NArrow::TGeneralContainer>& container, const NArrow::NSSA::IColumnResolver& resolver, const bool withFilter) {
        Table->AddBatch(container, resolver, withFilter);
    }

    void AddBlobs(THashMap<TChunkAddress, TString>&& blobData) {
        for (auto&& i : blobData) {
            AFL_VERIFY(Blobs.emplace(i.first, std::move(i.second)).second);
        }
    }

    void AddDefaults(THashMap<TChunkAddress, TPortionDataAccessor::TAssembleBlobInfo>&& blobs) {
        for (auto&& i : blobs) {
            AFL_VERIFY(Blobs.emplace(i.first, std::move(i.second)).second);
        }
    }

    bool IsEmptyFiltered() const {
        return Table->IsEmptyFiltered();
    }

    void Clear() {
        Table->Clear();
    }

    void AddFilter(const std::shared_ptr<NArrow::TColumnFilter>& filter) {
        if (!filter) {
            return;
        }
        return Table->AddFilter(*filter);
    }

    std::shared_ptr<NArrow::TGeneralContainer> ToGeneralContainer() const {
        return Table->ToGeneralContainer();
    }

    void CutFilter(const ui32 recordsCount, const ui32 limit, const bool reverse) {
        Table->CutFilter(recordsCount, limit, reverse);
    }

    void AddFilter(const NArrow::TColumnFilter& filter) {
        Table->AddFilter(filter);
    }
};

class TFetchedResult {
private:
    YDB_READONLY_DEF(std::shared_ptr<NArrow::TGeneralContainer>, Batch);
    YDB_READONLY_DEF(std::shared_ptr<NArrow::TColumnFilter>, NotAppliedFilter);
    std::optional<std::deque<TPortionDataAccessor::TReadPage>> PagesToResult;
    std::optional<std::shared_ptr<arrow::Table>> ChunkToReply;

    TFetchedResult() = default;

public:
    static std::unique_ptr<TFetchedResult> BuildEmpty() {
        return std::unique_ptr<TFetchedResult>(new TFetchedResult);
    }

    TFetchedResult(
        std::unique_ptr<TFetchedData>&& data, const std::optional<std::set<ui32>>& columnIds, const NArrow::NSSA::IColumnResolver& resolver)
        : Batch(data->GetAborted() ? nullptr : data->GetTable()->ToGeneralContainer(&resolver, columnIds, false))
        , NotAppliedFilter(data->GetAborted() ? nullptr : data->GetNotAppliedFilter()) {
    }

    TFetchedResult(std::unique_ptr<TFetchedData>&& data, const NArrow::NSSA::IColumnResolver& resolver)
        : Batch(data->GetAborted() ? nullptr : data->GetTable()->ToGeneralContainer(&resolver, {}, false))
        , NotAppliedFilter(data->GetAborted() ? nullptr : data->GetNotAppliedFilter()) {
    }

    TPortionDataAccessor::TReadPage ExtractPageForResult() {
        AFL_VERIFY(PagesToResult);
        AFL_VERIFY(PagesToResult->size());
        auto result = PagesToResult->front();
        PagesToResult->pop_front();
        return result;
    }

    const std::deque<TPortionDataAccessor::TReadPage>& GetPagesToResultVerified() const {
        AFL_VERIFY(PagesToResult);
        return *PagesToResult;
    }

    void SetPages(std::vector<TPortionDataAccessor::TReadPage>&& pages) {
        AFL_VERIFY(!PagesToResult);
        PagesToResult = std::deque<TPortionDataAccessor::TReadPage>(pages.begin(), pages.end());
    }

    void SetResultChunk(std::shared_ptr<arrow::Table>&& table, const ui32 indexStart, const ui32 recordsCount) {
        auto page = ExtractPageForResult();
        AFL_VERIFY(page.GetIndexStart() == indexStart)("real", page.GetIndexStart())("expected", indexStart);
        AFL_VERIFY(page.GetRecordsCount() == recordsCount)("real", page.GetRecordsCount())("expected", recordsCount);
        AFL_VERIFY(!ChunkToReply);
        ChunkToReply = std::move(table);
    }

    bool IsFinished() const {
        return GetPagesToResultVerified().empty();
    }

    bool HasResultChunk() const {
        return !!ChunkToReply;
    }

    std::shared_ptr<arrow::Table> ExtractResultChunk() {
        AFL_VERIFY(!!ChunkToReply);
        auto result = std::move(*ChunkToReply);
        ChunkToReply.reset();
        return result;
    }

    bool IsEmpty() const {
        return !Batch || Batch->num_rows() == 0 || (NotAppliedFilter && NotAppliedFilter->IsTotalDenyFilter());
    }
};

}   // namespace NKikimr::NOlap::NReader::NCommon
