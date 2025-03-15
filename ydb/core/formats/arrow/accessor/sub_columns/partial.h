#pragma once
#include "header.h"
#include "others_storage.h"

#include <ydb/core/formats/arrow/arrow_filter.h>

namespace NKikimr::NArrow::NAccessor {

class TPartialColumnsData {
private:
    THashMap<ui32, std::shared_ptr<IChunkedArray>> ColumnsData;

public:
    void AddColumn(const ui32 columnId, const std::shared_ptr<IChunkedArray>& arr) {
        AFL_VERIFY(ColumnsData.emplace(columnId, arr).second);
    }

    bool HasColumn(const ui32 columnId) const {
        return ColumnsData.contains(columnId);
    }

    const std::shared_ptr<IChunkedArray>& GetAccessorVerified(const ui32 columnId) const {
        auto it = ColumnsData.find(columnId);
        AFL_VERIFY(it != ColumnsData.end())("id", columnId);
        return it->second;
    }

    TPartialColumnsData ApplyFilter(const TColumnFilter& filter) const {
        TPartialColumnsData result;
        for (auto&& i : ColumnsData) {
            AFL_VERIFY(result.ColumnsData.emplace(i.first, filter.Apply(i.second)).second);
        }
        return result;
    }
    TPartialColumnsData Slice(const ui32 offset, const ui32 count) const {
        TPartialColumnsData result;
        for (auto&& i : ColumnsData) {
            AFL_VERIFY(result.ColumnsData.emplace(i.first, i.second->ISlice(offset, count)).second);
        }
        return result;
    }
};

class TSubColumnsPartialArray: public IChunkedArray {
private:
    using TBase = IChunkedArray;
    using TSubColumnsHeader = NSubColumns::TSubColumnsHeader;
    const TSubColumnsHeader Header;
    TPartialColumnsData PartialColumnsData;
    std::optional<NSubColumns::TOthersData> OthersData;
    NSubColumns::TSettings Settings;
    TString StoreOthersString;

    virtual void DoVisitValues(const TValuesSimpleVisitor& /*visitor*/) const override {
        AFL_VERIFY(false);
    }

protected:
    virtual ui32 DoGetNullsCount() const override {
        AFL_VERIFY(false);
        return 0;
    }
    virtual ui32 DoGetValueRawBytes() const override {
        AFL_VERIFY(false);
        return 0;
    }

    virtual std::shared_ptr<arrow::Scalar> DoGetMaxScalar() const override {
        return nullptr;
    }

    virtual TLocalDataAddress DoGetLocalData(
        const std::optional<TCommonChunkAddress>& /*chunkCurrent*/, const ui64 /*position*/) const override {
        AFL_VERIFY(false);
        return TLocalDataAddress(nullptr, 0, 0);
    }
    virtual std::optional<ui64> DoGetRawSize() const override {
        return std::nullopt;
    }
    virtual std::shared_ptr<IChunkedArray> DoApplyFilter(const TColumnFilter& filter) const override {
        std::optional<NSubColumns::TOthersData> others;
        if (OthersData) {
            others = OthersData->ApplyFilter(filter, Settings);
        }
        return std::make_shared<TSubColumnsPartialArray>(Header,
            PartialColumnsData.ApplyFilter(filter), std::move(others), GetDataType(), filter.GetFilteredCountVerified());
    }

    virtual std::shared_ptr<IChunkedArray> DoISlice(const ui32 offset, const ui32 count) const override {
        std::optional<NSubColumns::TOthersData> others;
        if (OthersData) {
            others = OthersData->Slice(offset, count, Settings);
        }
        return std::make_shared<TSubColumnsPartialArray>(
            Header, PartialColumnsData.Slice(offset, count), std::move(others), GetDataType(), count);
    }

public:
    TSubColumnsPartialArray(TSubColumnsHeader&& header, const ui32 recordsCount, const std::shared_ptr<arrow::DataType>& dataType)
        : TBase(recordsCount, EType::SubColumnsPartialArray, dataType)
        , Header(std::move(header)) {
    }

    virtual bool HasWholeDataVolume() const override {
        return false;
    }

    virtual bool HasSubColumnData(const TString& subColumnName) const override {
        return !NeedFetch(std::string_view(subColumnName.data(), subColumnName.size()));
    }

    static std::shared_ptr<TSubColumnsPartialArray> BuildEmpty(const std::shared_ptr<arrow::DataType>& dataType, const ui32 recordsCount) {
        return std::make_shared<TSubColumnsPartialArray>(TSubColumnsHeader::BuildEmpty(), recordsCount, dataType);
    }

    const TSubColumnsHeader& GetHeader() const {
        return Header;
    }
    
    std::shared_ptr<IChunkedArray> GetPathAccessor(const std::string_view svPath, const ui32 recordsCount) const;

    bool NeedFetch(const std::string_view colName) const {
        if (auto idx = Header.GetColumnStats().GetKeyIndexOptional(colName)) {
            return !PartialColumnsData.HasColumn(*idx);
        } else if (auto idx = Header.GetOtherStats().GetKeyIndexOptional(colName)) {
            return !OthersData;
        }
        return false;
    }

    void AddColumn(const TString& columnName, const std::shared_ptr<IChunkedArray>& arr) {
        PartialColumnsData.AddColumn(Header.GetColumnStats().GetKeyIndexVerified(std::string_view(columnName.data(), columnName.size())), arr);
    }

    bool HasOthers() const {
        return !!OthersData;
    }

    void InitOthers(const TString& blob, const TChunkConstructionData& externalInfo,
        const std::shared_ptr<NArrow::TColumnFilter>& applyFilter, const bool deserialize);

    bool IsOtherColumn(const TString& colName) const {
        return !!Header.GetOtherStats().GetKeyIndexOptional(std::string_view(colName.data(), colName.size()));
    }

    NSubColumns::TReadRange GetColumnReadRange(const ui32 colIndex) const {
        return Header.GetColumnReadRange(colIndex);
    }

    TSubColumnsPartialArray(const TSubColumnsHeader& header, TPartialColumnsData&& columnsData,
        std::optional<NSubColumns::TOthersData>&& othersData,
        const std::shared_ptr<arrow::DataType>& dataType, const ui32 recordsCount)
        : TBase(recordsCount, EType::SubColumnsPartialArray, dataType)
        , Header(std::move(header))
        , PartialColumnsData(std::move(columnsData))
        , OthersData(std::move(othersData)) {
    }

    virtual std::shared_ptr<arrow::Scalar> DoGetScalar(const ui32 /*index*/) const override {
        return nullptr;
    }
};

}   // namespace NKikimr::NArrow::NAccessor
