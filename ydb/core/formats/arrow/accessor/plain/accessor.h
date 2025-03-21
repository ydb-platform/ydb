#pragma once
#include <ydb/core/formats/arrow/accessor/abstract/accessor.h>

#include <ydb/library/formats/arrow/arrow_helpers.h>
#include <ydb/library/formats/arrow/switch/switch_type.h>
#include <ydb/library/formats/arrow/validation/validation.h>

namespace NKikimr::NArrow::NAccessor {

class TTrivialArray: public IChunkedArray {
private:
    using TBase = IChunkedArray;
    std::shared_ptr<arrow::Array> Array;

    virtual void DoVisitValues(const TValuesSimpleVisitor& visitor) const override {
        visitor(Array);
    }

protected:
    virtual std::optional<ui64> DoGetRawSize() const override;

    virtual TLocalDataAddress DoGetLocalData(
        const std::optional<TCommonChunkAddress>& /*chunkCurrent*/, const ui64 /*position*/) const override {
        return TLocalDataAddress(Array, 0, 0);
    }
    virtual std::shared_ptr<arrow::Scalar> DoGetScalar(const ui32 index) const override {
        return NArrow::TStatusValidator::GetValid(Array->GetScalar(index));
    }
    virtual std::shared_ptr<arrow::Scalar> DoGetMaxScalar() const override;
    virtual std::shared_ptr<IChunkedArray> DoISlice(const ui32 offset, const ui32 count) const override {
        return std::make_shared<TTrivialArray>(Array->Slice(offset, count));
    }
    virtual ui32 DoGetNullsCount() const override {
        return Array->null_count();
    }
    virtual ui32 DoGetValueRawBytes() const override;

    virtual std::optional<bool> DoCheckOneValueAccessor(std::shared_ptr<arrow::Scalar>& value) const override;

public:

    virtual void Reallocate() override;

    virtual std::shared_ptr<arrow::ChunkedArray> GetChunkedArray() const override {
        return std::make_shared<arrow::ChunkedArray>(Array);
    }

    const std::shared_ptr<arrow::Array>& GetArray() const {
        return Array;
    }

    static std::shared_ptr<TTrivialArray> BuildEmpty(const std::shared_ptr<arrow::DataType>& type);

    TTrivialArray(const std::shared_ptr<arrow::Array>& data)
        : TBase(data->length(), EType::Array, data->type())
        , Array(data) {
    }

    static std::shared_ptr<arrow::Array> BuildArrayFromScalar(const std::shared_ptr<arrow::Scalar>& scalar) {
        AFL_VERIFY(scalar);
        auto builder = NArrow::MakeBuilder(scalar->type, 1);
        TStatusValidator::Validate(builder->AppendScalar(*scalar));
        return NArrow::FinishBuilder(std::move(builder));
    }

    static std::shared_ptr<arrow::Array> BuildArrayFromOptionalScalar(
        const std::shared_ptr<arrow::Scalar>& scalar, const std::shared_ptr<arrow::DataType>& type);

    TTrivialArray(const std::shared_ptr<arrow::Scalar>& scalar)
        : TBase(1, EType::Array, TValidator::CheckNotNull(scalar)->type)
        , Array(BuildArrayFromScalar(scalar)) {
    }

    template <class TArrowDataType = arrow::StringType>
    class TPlainBuilder {
    private:
        std::unique_ptr<arrow::ArrayBuilder> Builder;
        std::optional<ui32> LastRecordIndex;

    public:
        TPlainBuilder(const ui32 reserveItems = 0, const ui32 reserveSize = 0) {
            Builder = NArrow::MakeBuilder(arrow::TypeTraits<TArrowDataType>::type_singleton(), reserveItems, reserveSize);
        }

        void AddRecord(const ui32 recordIndex, const std::string_view value) {
            if (LastRecordIndex) {
                AFL_VERIFY(*LastRecordIndex < recordIndex)("last", LastRecordIndex)("index", recordIndex);
                TStatusValidator::Validate(Builder->AppendNulls(recordIndex - *LastRecordIndex - 1));
            } else {
                TStatusValidator::Validate(Builder->AppendNulls(recordIndex));
            }
            LastRecordIndex = recordIndex;
            AFL_VERIFY(NArrow::Append<TArrowDataType>(*Builder, arrow::util::string_view(value.data(), value.size())));
        }

        std::shared_ptr<IChunkedArray> Finish(const ui32 recordsCount) {
            if (LastRecordIndex) {
                AFL_VERIFY(*LastRecordIndex < recordsCount)("last", LastRecordIndex)("count", recordsCount);
                TStatusValidator::Validate(Builder->AppendNulls(recordsCount - *LastRecordIndex - 1));
            } else {
                TStatusValidator::Validate(Builder->AppendNulls(recordsCount));
            }
            return std::make_shared<TTrivialArray>(NArrow::FinishBuilder(std::move(Builder)));
        }
    };

    static TPlainBuilder<arrow::StringType> MakeBuilderUtf8(const ui32 reserveItems = 0, const ui32 reserveSize = 0) {
        return TPlainBuilder<arrow::StringType>(reserveItems, reserveSize);
    }
};

class TTrivialChunkedArray: public IChunkedArray {
private:
    using TBase = IChunkedArray;
    const std::shared_ptr<arrow::ChunkedArray> Array;

    virtual void DoVisitValues(const TValuesSimpleVisitor& visitor) const override {
        for (auto&& i : Array->chunks()) {
            visitor(i);
        }
    }

protected:
    virtual ui32 DoGetValueRawBytes() const override;
    virtual ui32 DoGetNullsCount() const override {
        return Array->null_count();
    }
    virtual TLocalDataAddress DoGetLocalData(const std::optional<TCommonChunkAddress>& chunkCurrent, const ui64 position) const override;
    virtual std::optional<ui64> DoGetRawSize() const override;

    virtual std::shared_ptr<arrow::Scalar> DoGetScalar(const ui32 index) const override {
        auto chunk = GetChunkSlow(index);
        return NArrow::TStatusValidator::GetValid(chunk.GetArray()->GetScalar(chunk.GetAddress().GetLocalIndex(index)));
    }

    virtual std::shared_ptr<IChunkedArray> DoISlice(const ui32 offset, const ui32 count) const override {
        return std::make_shared<TTrivialChunkedArray>(Array->Slice(offset, count));
    }
    virtual std::shared_ptr<arrow::Scalar> DoGetMaxScalar() const override;

public:
    virtual std::shared_ptr<arrow::ChunkedArray> GetChunkedArray() const override {
        return Array;
    }

    TTrivialChunkedArray(const std::shared_ptr<arrow::ChunkedArray>& data)
        : TBase(data->length(), EType::ChunkedArray, data->type())
        , Array(data) {
    }
};

}   // namespace NKikimr::NArrow::NAccessor
