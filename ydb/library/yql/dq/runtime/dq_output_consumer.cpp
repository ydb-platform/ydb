#include "dq_output_consumer.h"

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

    void Finish() override {
        Output->Finish();
    }

private:
    IDqOutput::TPtr Output;
};

class TDqOutputHashPartitionConsumer : public IDqOutputConsumer {
public:
    TDqOutputHashPartitionConsumer(const TTypeEnvironment& typeEnv, TVector<IDqOutput::TPtr>&& outputs,
        TVector<TDataTypeId>&& keyColumnTypes, TVector<ui32>&& keyColumnIndices)
        : TypeEnv(typeEnv)
        , Outputs(std::move(outputs))
        , KeyColumnTypes(std::move(keyColumnTypes))
        , KeyColumnIndices(std::move(keyColumnIndices))
    {
        MKQL_ENSURE_S(KeyColumnTypes.size() == KeyColumnIndices.size());

        for (size_t keyId = 0; keyId < KeyColumnTypes.size(); keyId++) {
            NMiniKQL::TType* type;
            if (KeyColumnTypes[keyId] == NUdf::TDataType<NUdf::TDecimal>::Id) {
                type = NMiniKQL::TDataDecimalType::Create(22, 9, TypeEnv);
            } else {
                type = NMiniKQL::TDataType::Create(KeyColumnTypes[keyId], TypeEnv);
            }
            bool isTuple;
            bool encoded;
            GetDictionaryKeyTypes(type, KeyTypes, isTuple, encoded);

            ValueHashers.emplace_back(KeyTypes, isTuple);
        }
    }

    bool IsFull() const override {
        return AnyOf(Outputs, [](const auto& output) { return output->IsFull(); });
    }

    void Consume(TUnboxedValue&& value) final {
        ui32 partitionIndex = GetHashPartitionIndex(value);
        Outputs[partitionIndex]->Push(std::move(value));
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

    ui64 HashColumn(size_t keyId, const TUnboxedValue& value) const {
        if (!value.HasValue()) {
            return 0;
        }

        #define APPLY_HASHER(type, layout) \
            case TDataType<type>::Id: return hasher(value);

        auto& hasher = ValueHashers[keyId];
        switch (KeyColumnTypes[keyId]) {
            KNOWN_FIXED_VALUE_TYPES(APPLY_HASHER)
            case TDataType<TDecimal>::Id:
                return GetValueHash<EDataSlot::Decimal>(value);
        }

        #undef APPLY_HASHER

        return GetValueHash<EDataSlot::String>(value);
    }

private:
    const TTypeEnvironment& TypeEnv;
    TVector<IDqOutput::TPtr> Outputs;
    TVector<TDataTypeId> KeyColumnTypes;
    TVector<ui32> KeyColumnIndices;
    TVector<TValueHasher> ValueHashers;
    TKeyTypes KeyTypes;
};

class TDqOutputBroadcastConsumer : public IDqOutputConsumer {
public:
    TDqOutputBroadcastConsumer(TVector<IDqOutput::TPtr>&& outputs)
        : Outputs(std::move(outputs)) {}

    bool IsFull() const override {
        return AnyOf(Outputs, [](const auto& output) { return output->IsFull(); });
    }

    void Consume(TUnboxedValue&& value) final {
        for (auto& output : Outputs) {
            TUnboxedValue copy{ value };
            output->Push(std::move(copy));
        }
    }

    void Finish() override {
        for (auto& output : Outputs) {
            output->Finish();
        }
    }

private:
    TVector<IDqOutput::TPtr> Outputs;
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
    TVector<NUdf::TDataTypeId>&& keyColumnTypes, TVector<ui32>&& keyColumnIndices,
    const NKikimr::NMiniKQL::TTypeEnvironment& typeEnv)
{
    return MakeIntrusive<TDqOutputHashPartitionConsumer>(typeEnv, std::move(outputs), std::move(keyColumnTypes),
        std::move(keyColumnIndices));
}

IDqOutputConsumer::TPtr CreateOutputBroadcastConsumer(TVector<IDqOutput::TPtr>&& outputs) {
    return MakeIntrusive<TDqOutputBroadcastConsumer>(std::move(outputs));
}

} // namespace NYql::NDq
