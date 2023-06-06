#include "dq_output_consumer.h"

#include <ydb/library/yql/dq/actors/protos/dq_events.pb.h>
#include <ydb/library/yql/public/udf/udf_value.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/mkql_node.h>
#include <ydb/library/yql/utils/yql_panic.h>

namespace NYql::NDq {

namespace {

using namespace NKikimr;
using namespace NMiniKQL;
using namespace NUdf;


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
            OutputWaiting->Push(std::move(WaitingValue));
            IsWaitingFlag = false;
        }
    }

    virtual bool DoTryFinish() override {
        DrainWaiting();
        return !IsWaitingFlag;
    }
public:
    TDqOutputHashPartitionConsumer(TVector<IDqOutput::TPtr>&& outputs,
        TVector<NKikimr::NMiniKQL::TType*>&& keyColumnTypes, TVector<ui32>&& keyColumnIndices,
        TMaybe<ui32> outputWidth)
        : Outputs(std::move(outputs))
        , KeyColumnIndices(std::move(keyColumnIndices))
        , ValueHashers(KeyColumnIndices.size(), NUdf::IHash::TPtr{})
        , OutputWidth(outputWidth)
    {
        MKQL_ENSURE_S(keyColumnTypes.size() == KeyColumnIndices.size());

        for (auto i = 0U; i < keyColumnTypes.size(); i++) {
            ValueHashers[i] = MakeHashImpl(keyColumnTypes[i]);
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
            for (ui32 i = 0; i < count; ++i) {
                WideWaitingValues[i] = std::move(values[i]);
            }
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

        for (size_t keyId = 0; keyId < KeyColumnIndices.size(); keyId++) {
            auto columnValue = value.GetElement(KeyColumnIndices[keyId]);
            hash = CombineHashes(hash, HashColumn(keyId, columnValue));
        }

        return hash % Outputs.size();
    }

    size_t GetHashPartitionIndex(const TUnboxedValue* values) {
        ui64 hash = 0;

        for (size_t keyId = 0; keyId < KeyColumnIndices.size(); keyId++) {
            MKQL_ENSURE_S(KeyColumnIndices[keyId] < OutputWidth);
            hash = CombineHashes(hash, HashColumn(keyId, values[KeyColumnIndices[keyId]]));
        }

        return hash % Outputs.size();
    }

    ui64 HashColumn(size_t keyId, const TUnboxedValue& value) const {
        if (!value.HasValue()) {
            return 0;
        }
        return ValueHashers[keyId]->Hash(value);
    }

private:
    TVector<IDqOutput::TPtr> Outputs;
    TVector<ui32> KeyColumnIndices;
    TVector<NUdf::IHash::TPtr> ValueHashers;
    const TMaybe<ui32> OutputWidth;
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
    TVector<NKikimr::NMiniKQL::TType*>&& keyColumnTypes, TVector<ui32>&& keyColumnIndices, TMaybe<ui32> outputWidth)
{
    return MakeIntrusive<TDqOutputHashPartitionConsumer>(std::move(outputs), std::move(keyColumnTypes),
        std::move(keyColumnIndices), outputWidth);
}

IDqOutputConsumer::TPtr CreateOutputBroadcastConsumer(TVector<IDqOutput::TPtr>&& outputs, TMaybe<ui32> outputWidth) {
    return MakeIntrusive<TDqOutputBroadcastConsumer>(std::move(outputs), outputWidth);
}

} // namespace NYql::NDq
