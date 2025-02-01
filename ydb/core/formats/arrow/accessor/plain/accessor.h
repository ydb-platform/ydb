#pragma once
#include <ydb/library/formats/arrow/accessor/abstract/accessor.h>
#include <ydb/library/formats/arrow/arrow_helpers.h>
#include <ydb/library/formats/arrow/validation/validation.h>

namespace NKikimr::NArrow::NAccessor {

class TTrivialArray: public IChunkedArray {
private:
    using TBase = IChunkedArray;
    const std::shared_ptr<arrow::Array> Array;

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
    virtual std::vector<TChunkedArraySerialized> DoSplitBySizes(
        const TColumnLoader& saver, const TString& fullSerializedData, const std::vector<ui64>& splitSizes) override;

public:
    const std::shared_ptr<arrow::Array>& GetArray() const {
        return Array;
    }

    TTrivialArray(const std::shared_ptr<arrow::Array>& data)
        : TBase(data->length(), EType::Array, data->type())
        , Array(data) {
    }

    class TPlainBuilder {
    private:
        std::unique_ptr<arrow::ArrayBuilder> Builder;
        arrow::StringBuilder* ValueBuilder;
        std::optional<ui32> LastRecordIndex;

    public:
        TPlainBuilder() {
            Builder = NArrow::MakeBuilder(arrow::utf8());
            ValueBuilder = static_cast<arrow::StringBuilder*>(Builder.get());
        }

        void AddRecord(const ui32 recordIndex, const std::string_view value) {
            TStatusValidator::Validate(ValueBuilder->AppendNulls(recordIndex - LastRecordIndex.value_or(0)));
            LastRecordIndex = recordIndex;
            TStatusValidator::Validate(ValueBuilder->Append(value.data(), value.size()));
        }

        std::shared_ptr<IChunkedArray> Finish(const ui32 recordsCount) {
            if (LastRecordIndex) {
                TStatusValidator::Validate(ValueBuilder->AppendNulls(recordsCount - *LastRecordIndex - 1));
            } else {
                TStatusValidator::Validate(ValueBuilder->AppendNulls(recordsCount));
            }
            return std::make_shared<TTrivialArray>(NArrow::FinishBuilder(std::move(Builder)));
        }
    };

    static TPlainBuilder MakeBuilderUtf8() {
        return TPlainBuilder();
    }
};

class TTrivialChunkedArray: public IChunkedArray {
private:
    using TBase = IChunkedArray;
    const std::shared_ptr<arrow::ChunkedArray> Array;

protected:
    virtual TLocalDataAddress DoGetLocalData(const std::optional<TCommonChunkAddress>& chunkCurrent, const ui64 position) const override;
    virtual std::optional<ui64> DoGetRawSize() const override;
    virtual std::shared_ptr<arrow::Scalar> DoGetScalar(const ui32 index) const override {
        auto chunk = GetChunkSlow(index);
        return NArrow::TStatusValidator::GetValid(chunk.GetArray()->GetScalar(chunk.GetAddress().GetLocalIndex(index)));
    }
    virtual std::vector<TChunkedArraySerialized> DoSplitBySizes(
        const TColumnLoader& /*saver*/, const TString& /*fullSerializedData*/, const std::vector<ui64>& /*splitSizes*/) override {
        AFL_VERIFY(false);
        return {};
    }

    virtual std::shared_ptr<arrow::Scalar> DoGetMaxScalar() const override;

public:
    TTrivialChunkedArray(const std::shared_ptr<arrow::ChunkedArray>& data)
        : TBase(data->length(), EType::ChunkedArray, data->type())
        , Array(data) {
    }
};

}   // namespace NKikimr::NArrow::NAccessor
