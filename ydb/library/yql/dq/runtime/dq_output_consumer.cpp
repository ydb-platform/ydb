#include "dq_output_consumer.h"

#include <ydb/library/yql/dq/actors/protos/dq_events.pb.h>
#include <ydb/library/yql/minikql/computation/mkql_block_builder.h>
#include <ydb/library/yql/minikql/computation/mkql_block_reader.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/mkql_node.h>
#include <ydb/library/yql/minikql/mkql_type_builder.h>

#include <ydb/library/yql/public/udf/arrow/args_dechunker.h>
#include <ydb/library/yql/public/udf/arrow/memory_pool.h>
#include <ydb/library/yql/public/udf/udf_value.h>

#include <ydb/library/yql/utils/yql_panic.h>

namespace NYql::NDq {

namespace {

using namespace NKikimr;
using namespace NMiniKQL;
using namespace NUdf;

inline ui64 SpreadHash(ui64 hash) {
    // https://probablydance.com/2018/06/16/fibonacci-hashing-the-optimization-that-the-world-forgot-or-a-better-alternative-to-integer-modulo/
    return ((unsigned __int128)hash * 11400714819323198485llu) >> 64;
}


class TDqOutputMultiConsumer : public IDqOutputConsumer {
public:
    explicit TDqOutputMultiConsumer(TVector<IDqOutputConsumer::TPtr>&& consumers)
        : Consumers(std::move(consumers))
    {
        YQL_ENSURE(!Consumers.empty());
    }

    bool IsFull() const override {
        return AnyOf(Consumers, [](const auto& consumer) { return consumer->IsFull(); });
    }

    void WideConsume(TUnboxedValue* values, ui32 count) override {
        Y_UNUSED(values);
        Y_UNUSED(count);
        YQL_ENSURE(false, "WideConsume is not supported");
    }

    void Consume(TUnboxedValue&& value) override {
        if (Consumers.size() == 1) {
            Consumers[0]->Consume(std::move(value));
            return;
        }

        auto index = value.GetVariantIndex();
        YQL_ENSURE(index < Consumers.size());
        auto variantItem = value.GetVariantItem();
        Consumers[index]->Consume(std::move(variantItem));
    }

    void Consume(NDqProto::TCheckpoint&& checkpoint) override {
        for (auto& consumer : Consumers) {
            consumer->Consume(NDqProto::TCheckpoint(checkpoint));
        }
    }

    void Finish() override {
        for (auto& consumer : Consumers) {
            consumer->Finish();
        }
    }

private:
    TVector<IDqOutputConsumer::TPtr> Consumers;
};

class TDqOutputMapConsumer : public IDqOutputConsumer {
public:
    TDqOutputMapConsumer(IDqOutput::TPtr output)
        : Output(output) {}

    bool IsFull() const override {
        return Output->IsFull();
    }

    void Consume(TUnboxedValue&& value) override {
        Output->Push(std::move(value));
    }

    void WideConsume(TUnboxedValue* values, ui32 count) override {
        Output->WidePush(values, count);
    }

    void Consume(NDqProto::TCheckpoint&& checkpoint) override {
        Output->Push(std::move(checkpoint));
    }

    void Finish() override {
        Output->Finish();
    }

private:
    IDqOutput::TPtr Output;
};

class TDqOutputHashPartitionConsumer : public IDqOutputConsumer {
private:
    mutable bool IsWaitingFlag = false;
    mutable TUnboxedValue WaitingValue;
    mutable TUnboxedValueVector WideWaitingValues;
    mutable IDqOutput::TPtr OutputWaiting;
protected:
    void DrainWaiting() const {
        if (Y_UNLIKELY(IsWaitingFlag)) {
            if (OutputWaiting->IsFull()) {
                return;
            }
            if (OutputWidth.Defined()) {
                YQL_ENSURE(OutputWidth == WideWaitingValues.size());
                OutputWaiting->WidePush(WideWaitingValues.data(), *OutputWidth);
            } else {
                OutputWaiting->Push(std::move(WaitingValue));
            }
            IsWaitingFlag = false;
        }
    }

    virtual bool DoTryFinish() override {
        DrainWaiting();
        return !IsWaitingFlag;
    }
public:
    TDqOutputHashPartitionConsumer(TVector<IDqOutput::TPtr>&& outputs, TVector<TColumnInfo>&& keyColumns, TMaybe<ui32> outputWidth)
        : Outputs(std::move(outputs))
        , KeyColumns(std::move(keyColumns))
        , OutputWidth(outputWidth)
    {
        for (auto& column : KeyColumns) {
            ValueHashers.emplace_back(MakeHashImpl(column.Type));
        }

        if (outputWidth.Defined()) {
            WideWaitingValues.resize(*outputWidth);
        }
    }

    bool IsFull() const override {
        DrainWaiting();
        return IsWaitingFlag;
    }

    void Consume(TUnboxedValue&& value) final {
        YQL_ENSURE(!OutputWidth.Defined());
        ui32 partitionIndex = GetHashPartitionIndex(value);
        if (Outputs[partitionIndex]->IsFull()) {
            YQL_ENSURE(!IsWaitingFlag);
            IsWaitingFlag = true;
            OutputWaiting = Outputs[partitionIndex];
            WaitingValue = std::move(value);
        } else {
            Outputs[partitionIndex]->Push(std::move(value));
        }
    }

    void WideConsume(TUnboxedValue* values, ui32 count) final {
        YQL_ENSURE(OutputWidth.Defined() && count == OutputWidth);
        ui32 partitionIndex = GetHashPartitionIndex(values);
        if (Outputs[partitionIndex]->IsFull()) {
            YQL_ENSURE(!IsWaitingFlag);
            IsWaitingFlag = true;
            OutputWaiting = Outputs[partitionIndex];
            std::move(values, values + count, WideWaitingValues.data());
        } else {
            Outputs[partitionIndex]->WidePush(values, count);
        }
    }

    void Consume(NDqProto::TCheckpoint&& checkpoint) override {
        for (auto& output : Outputs) {
            output->Push(NDqProto::TCheckpoint(checkpoint));
        }
    }

    void Finish() final {
        for (auto& output : Outputs) {
            output->Finish();
        }
    }

private:
    size_t GetHashPartitionIndex(const TUnboxedValue& value) {
        ui64 hash = 0;

        for (size_t keyId = 0; keyId < KeyColumns.size(); keyId++) {
            auto columnValue = value.GetElement(KeyColumns[keyId].Index);
            hash = CombineHashes(hash, HashColumn(keyId, columnValue));
        }


        hash = SpreadHash(hash);

        return hash % Outputs.size();
    }

    size_t GetHashPartitionIndex(const TUnboxedValue* values) {
        ui64 hash = 0;

        for (size_t keyId = 0; keyId < KeyColumns.size(); keyId++) {
            MKQL_ENSURE_S(KeyColumns[keyId].Index < OutputWidth);
            hash = CombineHashes(hash, HashColumn(keyId, values[KeyColumns[keyId].Index]));
        }

        hash = SpreadHash(hash);

        return hash % Outputs.size();
    }

    ui64 HashColumn(size_t keyId, const TUnboxedValue& value) const {
        if (!value.HasValue()) {
            return 0;
        }
        return ValueHashers[keyId]->Hash(value);
    }

private:
    const TVector<IDqOutput::TPtr> Outputs;
    const TVector<TColumnInfo> KeyColumns;
    TVector<NUdf::IHash::TPtr> ValueHashers;
    const TMaybe<ui32> OutputWidth;
};

class TDqOutputHashPartitionConsumerScalar : public IDqOutputConsumer {
public:
    TDqOutputHashPartitionConsumerScalar(TVector<IDqOutput::TPtr>&& outputs, TVector<TColumnInfo>&& keyColumns, const  NKikimr::NMiniKQL::TType* outputType)
        : Outputs_(std::move(outputs))
        , KeyColumns_(std::move(keyColumns))
        , OutputWidth_(static_cast<const NMiniKQL::TMultiType*>(outputType)->GetElementsCount())
        , WaitingValues_(OutputWidth_)
    {
        auto multiType = static_cast<const NMiniKQL::TMultiType*>(outputType);
        TBlockTypeHelper helper;
        for (auto& column : KeyColumns_) {
            auto columnType = multiType->GetElementType(column.Index);
            YQL_ENSURE(columnType->IsBlock());
            auto blockType = static_cast<const NMiniKQL::TBlockType*>(columnType);
            YQL_ENSURE(blockType->GetShape() == NMiniKQL::TBlockType::EShape::Scalar);
            Readers_.emplace_back(MakeBlockReader(TTypeInfoHelper(), blockType->GetItemType()));
            Hashers_.emplace_back(helper.MakeHasher(blockType->GetItemType()));
        }
    }
private:
    bool IsFull() const final {
        DrainWaiting();
        return IsWaitingFlag_;
    }

    void Consume(TUnboxedValue&& value) final {
        Y_UNUSED(value);
        YQL_ENSURE(false, "Consume() called on wide block stream");
    }

    void WideConsume(TUnboxedValue* values, ui32 count) final {
        YQL_ENSURE(count == OutputWidth_);
        const ui64 inputBlockLen = TArrowBlock::From(values[count - 1]).GetDatum().scalar_as<arrow::UInt64Scalar>().value;
        if (!inputBlockLen) {
            return;
        }

        if (!Output_) {
            Output_ = Outputs_[GetHashPartitionIndex(values)];
        }
        if (Output_->IsFull()) {
            YQL_ENSURE(!IsWaitingFlag_);
            IsWaitingFlag_ = true;
            std::move(values, values + count, WaitingValues_.data());
        } else {
            Output_->WidePush(values, count);
        }
    }

    void Consume(NDqProto::TCheckpoint&& checkpoint) override {
        for (auto& output : Outputs_) {
            output->Push(NDqProto::TCheckpoint(checkpoint));
        }
    }

    void Finish() final {
        for (auto& output : Outputs_) {
            output->Finish();
        }
    }

    void DrainWaiting() const {
        if (Y_UNLIKELY(IsWaitingFlag_)) {
            YQL_ENSURE(Output_);
            if (Output_->IsFull()) {
                return;
            }
            YQL_ENSURE(OutputWidth_ == WaitingValues_.size());
            Output_->WidePush(WaitingValues_.data(), OutputWidth_);
            IsWaitingFlag_ = false;
        }
    }

    bool DoTryFinish() final {
        DrainWaiting();
        return !IsWaitingFlag_;
    }

    size_t GetHashPartitionIndex(const TUnboxedValue* values) {
        ui64 hash = 0;

        for (size_t keyId = 0; keyId < KeyColumns_.size(); keyId++) {
            YQL_ENSURE(KeyColumns_[keyId].Index < OutputWidth_);
            hash = CombineHashes(hash, HashColumn(keyId, values[KeyColumns_[keyId].Index]));
        }

        hash = SpreadHash(hash);

        return hash % Outputs_.size();
    }

    ui64 HashColumn(size_t keyId, const TUnboxedValue& value) const {
        TBlockItem item = Readers_[keyId]->GetScalarItem(*TArrowBlock::From(value).GetDatum().scalar());
        return Hashers_[keyId]->Hash(item);
    }

private:
    const TVector<IDqOutput::TPtr> Outputs_;
    const TVector<TColumnInfo> KeyColumns_;
    const ui32 OutputWidth_;
    TVector<NUdf::IBlockItemHasher::TPtr> Hashers_;
    TVector<std::unique_ptr<IBlockReader>> Readers_;
    IDqOutput::TPtr Output_;
    mutable bool IsWaitingFlag_ = false;
    mutable TUnboxedValueVector WaitingValues_;
};

class TDqOutputHashPartitionConsumerBlock : public IDqOutputConsumer {
public:
    TDqOutputHashPartitionConsumerBlock(TVector<IDqOutput::TPtr>&& outputs, TVector<TColumnInfo>&& keyColumns,
        const  NKikimr::NMiniKQL::TType* outputType,
        const NKikimr::NMiniKQL::THolderFactory& holderFactory)
        : OutputType_(static_cast<const NMiniKQL::TMultiType*>(outputType))
        , HolderFactory_(holderFactory)
        , Outputs_(std::move(outputs))
        , KeyColumns_(std::move(keyColumns))
        , ScalarColumnHashes_(KeyColumns_.size())
        , OutputWidth_(OutputType_->GetElementsCount())
    {
        TTypeInfoHelper helper;
        YQL_ENSURE(OutputWidth_ > KeyColumns_.size());

        TVector<const NMiniKQL::TType*> blockTypes;
        for (auto& columnType : OutputType_->GetElements()) {
            YQL_ENSURE(columnType->IsBlock());
            auto blockType = static_cast<const NMiniKQL::TBlockType*>(columnType);
            if (blockType->GetShape() == NMiniKQL::TBlockType::EShape::Many) {
                blockTypes.emplace_back(blockType->GetItemType());
            }
        }
        ui64 maxBlockLen = CalcMaxBlockLength(blockTypes.begin(), blockTypes.end(), helper);
        YQL_ENSURE(maxBlockLen > 0);
        MakeBuilders(maxBlockLen);

        TBlockTypeHelper blockHelper;
        for (auto& column : KeyColumns_) {
            auto columnType = OutputType_->GetElementType(column.Index);
            YQL_ENSURE(columnType->IsBlock());
            auto blockType = static_cast<const NMiniKQL::TBlockType*>(columnType);
            Readers_.emplace_back(MakeBlockReader(helper, blockType->GetItemType()));
            Hashers_.emplace_back(blockHelper.MakeHasher(blockType->GetItemType()));
        }
    }

private:
    bool IsFull() const final {
        DrainWaiting();
        return IsWaitingFlag_;
    }

    void Consume(TUnboxedValue&& value) final {
        Y_UNUSED(value);
        YQL_ENSURE(false, "Consume() called on wide block stream");
    }

    void WideConsume(TUnboxedValue* values, ui32 count) final {
        YQL_ENSURE(!IsWaitingFlag_);
        YQL_ENSURE(count == OutputWidth_);

        const ui64 inputBlockLen = TArrowBlock::From(values[count - 1]).GetDatum().scalar_as<arrow::UInt64Scalar>().value;
        if (!inputBlockLen) {
            return;
        }

        TVector<const arrow::Datum*> datums;
        datums.reserve(count - 1);
        for (ui32 i = 0; i + 1 < count; ++i) {
            datums.push_back(&TArrowBlock::From(values[i]).GetDatum());
        }

        TVector<TVector<ui64>> outputBlockIndexes(Outputs_.size());
        for (ui64 i = 0; i < inputBlockLen; ++i) {
            outputBlockIndexes[GetHashPartitionIndex(datums.data(), i)].push_back(i);
        }

        ui64 maxLen = 0;
        for (auto& indexes : outputBlockIndexes) {
            maxLen = std::max(maxLen, indexes.size());
        }

        if (maxLen > MaxOutputBlockLen_) {
            MakeBuilders(maxLen);
        }

        TVector<std::unique_ptr<TArgsDechunker>> outputData;
        for (size_t i = 0; i < Outputs_.size(); ++i) {
            ui64 outputBlockLen = outputBlockIndexes[i].size();
            if (!outputBlockLen) {
                outputData.emplace_back();
                continue;
            }
            const ui64* indexes = outputBlockIndexes[i].data();

            std::vector<arrow::Datum> output;
            for (size_t j = 0; j < datums.size(); ++j) {
                const arrow::Datum* src = datums[j];
                if (src->is_scalar()) {
                    output.emplace_back(*src);
                } else {
                    IArrayBuilder::TArrayDataItem dataItem;
                    dataItem.Data = src->array().get();
                    dataItem.StartOffset = 0;
                    Builders_[j]->AddMany(&dataItem, 1, indexes, outputBlockLen);
                    output.emplace_back(Builders_[j]->Build(false));
                }
            }
            outputData.emplace_back(std::make_unique<TArgsDechunker>(std::move(output)));
        }

        DoConsume(std::move(outputData));
    }

    void DoConsume(TVector<std::unique_ptr<TArgsDechunker>>&& outputData) const {
        while (!outputData.empty()) {
            bool hasData = false;
            for (size_t i = 0; i < Outputs_.size(); ++i) {
                if (Outputs_[i]->IsFull()) {
                    IsWaitingFlag_ = true;
                    OutputData_ = std::move(outputData);
                    return;
                }

                std::vector<arrow::Datum> chunk;
                ui64 blockLen = 0;
                if (outputData[i] && outputData[i]->Next(chunk, blockLen)) {
                    YQL_ENSURE(blockLen > 0);
                    hasData = true;
                    TUnboxedValueVector outputValues;
                    for (auto& datum : chunk) {
                        outputValues.emplace_back(HolderFactory_.CreateArrowBlock(std::move(datum)));
                    }
                    outputValues.emplace_back(HolderFactory_.CreateArrowBlock(arrow::Datum(std::make_shared<arrow::UInt64Scalar>(blockLen))));
                    Outputs_[i]->WidePush(outputValues.data(), outputValues.size());
                }
            }
            if (!hasData) {
                outputData.clear();
            }
        }
    }

    void Consume(NDqProto::TCheckpoint&& checkpoint) override {
        for (auto& output : Outputs_) {
            output->Push(NDqProto::TCheckpoint(checkpoint));
        }
    }

    void Finish() final {
        for (auto& output : Outputs_) {
            output->Finish();
        }
    }

    void DrainWaiting() const {
        if (Y_UNLIKELY(IsWaitingFlag_)) {
            TVector<std::unique_ptr<TArgsDechunker>> outputData;
            outputData.swap(OutputData_);
            DoConsume(std::move(outputData));
            if (OutputData_.empty()) {
                IsWaitingFlag_ = false;
            }
        }
    }

    bool DoTryFinish() final {
        DrainWaiting();
        return !IsWaitingFlag_;
    }

    size_t GetHashPartitionIndex(const arrow::Datum** values, ui64 blockIndex) {
        ui64 hash = 0;
        for (size_t keyId = 0; keyId < KeyColumns_.size(); keyId++) {
            const ui32 columnIndex = KeyColumns_[keyId].Index;
            Y_DEBUG_ABORT_UNLESS(columnIndex < OutputWidth_);
            ui64 keyHash;
            if (*KeyColumns_[keyId].IsScalar) {
                if (!ScalarColumnHashes_[keyId].Defined()) {
                    ScalarColumnHashes_[keyId] = HashScalarColumn(keyId, *values[columnIndex]->scalar());
                }
                keyHash = *ScalarColumnHashes_[keyId];
            } else {
                keyHash = HashBlockColumn(keyId, *values[columnIndex]->array(), blockIndex);
            }
            hash = CombineHashes(hash, keyHash);
        }

        hash = SpreadHash(hash);

        return hash % Outputs_.size();
    }

    inline ui64 HashScalarColumn(size_t keyId, const arrow::Scalar& value) const {
        TBlockItem item = Readers_[keyId]->GetScalarItem(value);
        return Hashers_[keyId]->Hash(item);
    }

    inline ui64 HashBlockColumn(size_t keyId, const arrow::ArrayData& value, ui64 index) const {
        TBlockItem item = Readers_[keyId]->GetItem(value, index);
        return Hashers_[keyId]->Hash(item);
    }

    void MakeBuilders(ui64 maxBlockLen) {
        Builders_.clear();
        TTypeInfoHelper helper;
        for (auto& columnType : OutputType_->GetElements()) {
            YQL_ENSURE(columnType->IsBlock());
            auto blockType = static_cast<const NMiniKQL::TBlockType*>(columnType);
            if (blockType->GetShape() == NMiniKQL::TBlockType::EShape::Many) {
                auto itemType = blockType->GetItemType();
                YQL_ENSURE(!itemType->IsPg(), "pg types are not supported yet");
                Builders_.emplace_back(MakeArrayBuilder(helper, itemType, *NYql::NUdf::GetYqlMemoryPool(), maxBlockLen, nullptr));
            } else {
                Builders_.emplace_back();
            }
        }
        MaxOutputBlockLen_ = maxBlockLen;
    }

private:
    const NKikimr::NMiniKQL::TMultiType* const OutputType_;
    const NKikimr::NMiniKQL::THolderFactory& HolderFactory_;

    const TVector<IDqOutput::TPtr> Outputs_;
    mutable TVector<std::unique_ptr<TArgsDechunker>> OutputData_;

    const TVector<TColumnInfo> KeyColumns_;
    TVector<TMaybe<ui64>> ScalarColumnHashes_;
    const ui32 OutputWidth_;

    TVector<NUdf::IBlockItemHasher::TPtr> Hashers_;
    TVector<std::unique_ptr<IBlockReader>> Readers_;
    TVector<std::unique_ptr<IArrayBuilder>> Builders_;

    ui64 MaxOutputBlockLen_ = 0;
    mutable bool IsWaitingFlag_ = false;
};

class TDqOutputBroadcastConsumer : public IDqOutputConsumer {
public:
    TDqOutputBroadcastConsumer(TVector<IDqOutput::TPtr>&& outputs, TMaybe<ui32> outputWidth)
        : Outputs(std::move(outputs))
        , OutputWidth(outputWidth)
        , Tmp(outputWidth.Defined() ? *outputWidth : 0u)
    {
    }

    bool IsFull() const override {
        return AnyOf(Outputs, [](const auto& output) { return output->IsFull(); });
    }

    void Consume(TUnboxedValue&& value) final {
        YQL_ENSURE(!OutputWidth.Defined());
        for (auto& output : Outputs) {
            TUnboxedValue copy{ value };
            output->Push(std::move(copy));
        }
    }

    void WideConsume(TUnboxedValue* values, ui32 count) final {
        YQL_ENSURE(OutputWidth.Defined() && OutputWidth == count);
        for (auto& output : Outputs) {
            std::copy(values, values + count, Tmp.begin());
            output->WidePush(Tmp.data(), count);
        }
    }

    void Consume(NDqProto::TCheckpoint&& checkpoint) override {
        for (auto& output : Outputs) {
            output->Push(NDqProto::TCheckpoint(checkpoint));
        }
    }

    void Finish() override {
        for (auto& output : Outputs) {
            output->Finish();
        }
    }

private:
    TVector<IDqOutput::TPtr> Outputs;
    const TMaybe<ui32> OutputWidth;
    TUnboxedValueVector Tmp;
};

} // namespace

IDqOutputConsumer::TPtr CreateOutputMultiConsumer(TVector<IDqOutputConsumer::TPtr>&& consumers) {
    return MakeIntrusive<TDqOutputMultiConsumer>(std::move(consumers));
}

IDqOutputConsumer::TPtr CreateOutputMapConsumer(IDqOutput::TPtr output) {
    return MakeIntrusive<TDqOutputMapConsumer>(output);
}

IDqOutputConsumer::TPtr CreateOutputHashPartitionConsumer(
    TVector<IDqOutput::TPtr>&& outputs,
    TVector<TColumnInfo>&& keyColumns, const  NKikimr::NMiniKQL::TType* outputType,
    const NKikimr::NMiniKQL::THolderFactory& holderFactory)
{
    YQL_ENSURE(!outputs.empty());
    YQL_ENSURE(!keyColumns.empty());
    TMaybe<ui32> outputWidth;
    if (outputType->IsMulti()) {
        outputWidth = static_cast<const NMiniKQL::TMultiType*>(outputType)->GetElementsCount();
    }

    if (AnyOf(keyColumns, [](const auto& info) { return !info.IsBlockOrScalar(); })) {
        return MakeIntrusive<TDqOutputHashPartitionConsumer>(std::move(outputs), std::move(keyColumns), outputWidth);
    }

    YQL_ENSURE(outputWidth.Defined(), "Expecting wide stream for block data");
    if (AllOf(keyColumns, [](const auto& info) { return *info.IsScalar; })) {
        // all key columns are scalars - all data will go to single output
        return MakeIntrusive<TDqOutputHashPartitionConsumerScalar>(std::move(outputs), std::move(keyColumns), outputType);
    }

    return MakeIntrusive<TDqOutputHashPartitionConsumerBlock>(std::move(outputs), std::move(keyColumns), outputType, holderFactory);
}

IDqOutputConsumer::TPtr CreateOutputBroadcastConsumer(TVector<IDqOutput::TPtr>&& outputs, TMaybe<ui32> outputWidth) {
    return MakeIntrusive<TDqOutputBroadcastConsumer>(std::move(outputs), outputWidth);
}

} // namespace NYql::NDq
