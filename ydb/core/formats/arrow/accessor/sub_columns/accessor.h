#pragma once
#include "columns_storage.h"
#include "data_extractor.h"
#include "iterators.h"
#include "others_storage.h"

#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/formats/arrow/common/container.h>

#include <ydb/library/accessor/accessor.h>
#include <ydb/library/formats/arrow/accessor/abstract/accessor.h>
#include <ydb/library/formats/arrow/accessor/common/chunk_data.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_base.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/type_fwd.h>

namespace NKikimr::NArrow::NAccessor {

class TSubColumnsArray: public IChunkedArray {
private:
    using TBase = IChunkedArray;
    NSubColumns::TColumnsData ColumnsData;
    NSubColumns::TOthersData OthersData;

protected:
    virtual std::shared_ptr<arrow::Scalar> DoGetMaxScalar() const override {
        return nullptr;
    }

    virtual std::vector<TChunkedArraySerialized> DoSplitBySizes(
        const TColumnLoader& loader, const TString& fullSerializedData, const std::vector<ui64>& splitSizes) override;

    virtual TLocalDataAddress DoGetLocalData(const std::optional<TCommonChunkAddress>& chunkCurrent, const ui64 position) const override;
    virtual std::optional<ui64> DoGetRawSize() const override {
        return ColumnsData.GetRawSize() + OthersData.GetRawSize();
    }

public:
    NSubColumns::TReadIteratorOrderedKeys BuildOrderedIterator() const {
        return NSubColumns::TReadIteratorOrderedKeys(ColumnsData, OthersData);
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
        const ui32 recordsCount);

    static TConclusion<std::shared_ptr<TSubColumnsArray>> Make(
        const std::shared_ptr<IChunkedArray>& sourceArray, const std::shared_ptr<NSubColumns::IDataAdapter>& adapter);

    TSubColumnsArray(const std::shared_ptr<arrow::DataType>& type, const ui32 recordsCount);

    virtual std::shared_ptr<arrow::Scalar> DoGetScalar(const ui32 /*index*/) const override {
        return nullptr;
    }
};

}   // namespace NKikimr::NArrow::NAccessor
