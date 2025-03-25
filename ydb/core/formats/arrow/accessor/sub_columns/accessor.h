#pragma once
#include "columns_storage.h"
#include "data_extractor.h"
#include "iterators.h"
#include "others_storage.h"
#include "settings.h"

#include <ydb/core/formats/arrow/accessor/abstract/accessor.h>
#include <ydb/core/formats/arrow/accessor/common/chunk_data.h>
#include <ydb/core/formats/arrow/arrow_filter.h>
#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/formats/arrow/common/container.h>

#include <ydb/library/accessor/accessor.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_base.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/type_fwd.h>

namespace NKikimr::NArrow::NAccessor {

class TSubColumnsArray: public IChunkedArray {
private:
    using TBase = IChunkedArray;
    NSubColumns::TColumnsData ColumnsData;
    NSubColumns::TOthersData OthersData;
    const NSubColumns::TSettings Settings;
    TString SourceDeserializationString;

protected:
    virtual NJson::TJsonValue DoDebugJson() const override {
        NJson::TJsonValue result = NJson::JSON_MAP;
        result.InsertValue("columns_data", ColumnsData.DebugJson());
        result.InsertValue("others_data", OthersData.DebugJson());
        result.InsertValue("settings", Settings.DebugJson());
        return result;
    }
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

    virtual TLocalDataAddress DoGetLocalData(const std::optional<TCommonChunkAddress>& chunkCurrent, const ui64 position) const override;
    virtual std::optional<ui64> DoGetRawSize() const override {
        return ColumnsData.GetRawSize() + OthersData.GetRawSize();
    }
    virtual std::shared_ptr<IChunkedArray> DoApplyFilter(const TColumnFilter& filter) const override {
        return std::make_shared<TSubColumnsArray>(ColumnsData.ApplyFilter(filter), OthersData.ApplyFilter(filter, Settings), GetDataType(),
            filter.GetFilteredCountVerified(), Settings);
    }

    virtual std::shared_ptr<IChunkedArray> DoISlice(const ui32 offset, const ui32 count) const override {
        return std::make_shared<TSubColumnsArray>(
            ColumnsData.Slice(offset, count), OthersData.Slice(offset, count, Settings), GetDataType(), count, Settings);
    }

public:
    virtual void DoVisitValues(const std::function<void(std::shared_ptr<arrow::Array>)>& /*visitor*/) const override {
        AFL_VERIFY(false);
    }

    bool HasSubColumn(const TString& subColumnName) const {
        return ColumnsData.GetStats().GetKeyIndexOptional(std::string_view(subColumnName.data(), subColumnName.size())) ||
               OthersData.GetStats().GetKeyIndexOptional(std::string_view(subColumnName.data(), subColumnName.size()));
    }

    void StoreSourceString(const TString& sourceDeserializationString) {
        AFL_VERIFY(!SourceDeserializationString);
        SourceDeserializationString = sourceDeserializationString;
    }

    std::shared_ptr<NSubColumns::TReadIteratorOrderedKeys> BuildOrderedIterator() const {
        return std::make_shared<NSubColumns::TReadIteratorOrderedKeys>(ColumnsData, OthersData);
    }

    NSubColumns::TReadIteratorUnorderedKeys BuildUnorderedIterator() const {
        return NSubColumns::TReadIteratorUnorderedKeys(ColumnsData, OthersData);
    }

    const NSubColumns::TColumnsData& GetColumnsData() const {
        return ColumnsData;
    }
    const NSubColumns::TOthersData& GetOthersData() const {
        return OthersData;
    }

    TString SerializeToString(const TChunkConstructionData& externalInfo) const;

    TSubColumnsArray(NSubColumns::TColumnsData&& columns, NSubColumns::TOthersData&& others, const std::shared_ptr<arrow::DataType>& type,
        const ui32 recordsCount, const NSubColumns::TSettings& settings);

    static TConclusion<std::shared_ptr<TSubColumnsArray>> Make(const std::shared_ptr<IChunkedArray>& sourceArray,
        const std::shared_ptr<NSubColumns::IDataAdapter>& adapter, const NSubColumns::TSettings& settings);

    TSubColumnsArray(const std::shared_ptr<arrow::DataType>& type, const ui32 recordsCount, const NSubColumns::TSettings& settings);

    virtual std::shared_ptr<arrow::Scalar> DoGetScalar(const ui32 /*index*/) const override {
        return nullptr;
    }

    std::shared_ptr<IChunkedArray> GetPathAccessor(const std::string_view svPath, const ui32 recordsCount) const {
        auto accResult = ColumnsData.GetPathAccessor(svPath);
        if (accResult) {
            return accResult;
        }
        return OthersData.GetPathAccessor(svPath, recordsCount);
    }
};

}   // namespace NKikimr::NArrow::NAccessor
