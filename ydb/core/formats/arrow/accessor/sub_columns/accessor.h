#pragma once
#include "data_extractor.h"

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
    std::shared_ptr<arrow::Schema> Schema;
    std::shared_ptr<TGeneralContainer> Records;

protected:
    virtual std::shared_ptr<arrow::Scalar> DoGetMaxScalar() const override {
        return nullptr;
    }

    virtual std::vector<TChunkedArraySerialized> DoSplitBySizes(
        const TColumnLoader& loader, const TString& fullSerializedData, const std::vector<ui64>& splitSizes) override;

    virtual TLocalDataAddress DoGetLocalData(const std::optional<TCommonChunkAddress>& chunkCurrent, const ui64 position) const override {
        return Records->GetAccessorByNameVerified("__ORIGINAL")->GetLocalData(chunkCurrent, position);
    }
    virtual std::optional<ui64> DoGetRawSize() const override {
        return Records->GetRawSizeVerified();
    }

public:
    TString SerializeToString(const TChunkConstructionData& externalInfo) const;

    TSubColumnsArray(const std::shared_ptr<arrow::RecordBatch>& batch);

    TSubColumnsArray(const std::shared_ptr<IChunkedArray>& sourceArray, const std::shared_ptr<IDataAdapter>& adapter);

    TSubColumnsArray(
        const std::shared_ptr<arrow::Schema>& schema, const std::vector<TString>& columns, const TChunkConstructionData& externalInfo);

    TSubColumnsArray(const std::shared_ptr<arrow::DataType>& type, const ui32 recordsCount);

    virtual std::shared_ptr<arrow::Scalar> DoGetScalar(const ui32 /*index*/) const override {
        return nullptr;
    }
};

}   // namespace NKikimr::NArrow::NAccessor
