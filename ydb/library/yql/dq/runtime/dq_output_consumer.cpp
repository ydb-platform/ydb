#include "dq_output_consumer.h"

#include <yql/essentials/utils/log/log.h>

#include <ydb/library/yql/dq/actors/protos/dq_events.pb.h>
#include <yql/essentials/minikql/computation/mkql_block_builder.h>
#include <yql/essentials/minikql/computation/mkql_block_reader.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/minikql/mkql_type_builder.h>

#include <yql/essentials/public/udf/arrow/args_dechunker.h>
#include <yql/essentials/public/udf/arrow/memory_pool.h>
#include <yql/essentials/public/udf/udf_value.h>

#include <yql/essentials/utils/yql_panic.h>

#include <atomic>
#include <memory>
#include <type_traits>
#include <ydb/library/formats/arrow/hash/xx_hash.h>

#include <util/string/builder.h>
#include <util/string/join.h>

namespace NYql::NDq {

TString FillLevelToString(EDqFillLevel level) {
    switch(level) {
        case NoLimit : return "No";
        case SoftLimit : return "Soft";
        case HardLimit : return "Hard";
        default: return "-";
    }
}

namespace {

using namespace NKikimr;
using namespace NMiniKQL;
using namespace NUdf;

///////////////////////////////////
// Hash Function implementations //
///////////////////////////////////

// TODO: maybe use common interface without templates?

struct THashBase {
    void Start() {
        Hash = 0;
    }

    template <class TValue>
    void Update(const TValue& value, size_t keyIdx) {
        Hash = CombineHashes(Hash, value.HasValue() ? Hashers.at(keyIdx)->Hash(value) : 0);
    }
protected:
    THashBase() = default;

    TVector<NUdf::IHash::TPtr> Hashers;
    ui64 Hash;
};

struct THashV1 : public THashBase {
    explicit THashV1(const TVector<TColumnInfo>& keyColumns) {
        for (const auto& column : keyColumns) {
            Hashers.emplace_back(MakeHashImpl(column.DataType));
        }
    }

    ui64 Finish(ui64 outputsSize) {
        return Hash % outputsSize;
    }
};

struct THashV2 : public THashV1 {
    explicit THashV2(const TVector<TColumnInfo>& keyColumns)
        : THashV1(keyColumns)
    {}

    static inline ui64 SpreadHash(ui64 hash) {
        // https://probablydance.com/2018/06/16/fibonacci-hashing-the-optimization-that-the-world-forgot-or-a-better-alternative-to-integer-modulo/
        return ((unsigned __int128)hash * 11400714819323198485llu) >> 64;
    }

    ui64 Finish(ui64 outputsSize) {
        return SpreadHash(Hash) % outputsSize;
    }
};

struct TBlockHashBase {
    void Start() {
        Hash = 0;
    }

protected:
    TBlockHashBase() = default;

    TVector<NUdf::IBlockItemHasher::TPtr> Hashers;
    ui64 Hash;
};

struct TBlockHashV1 : public TBlockHashBase {
    explicit TBlockHashV1(const TVector<TColumnInfo>& keyColumns) {
        TBlockTypeHelper helper;
        for (const auto& column : keyColumns) {
            Hashers.emplace_back(helper.MakeHasher(column.OriginalType));
        }
    }

    template <class TValue>
    void Update(const TValue& item, size_t keyIdx) {
        Hash = CombineHashes(Hash, Hashers.at(keyIdx)->Hash(item));
    }

    ui64 Finish(ui64 outputsSize) {
        return Hash % outputsSize;
    }
};

struct TBlockHashV2 : public TBlockHashBase {
    explicit TBlockHashV2(const TVector<TColumnInfo>& keyColumns) {
        TBlockTypeHelper helper;
        for (const auto& column : keyColumns) {
            Hashers.emplace_back(helper.MakeHasher(column.DataType));
        }
    }

    template <class TValue>
    void Update(const TValue& item, size_t keyIdx) {
        Hash = CombineHashes(Hash, item.HasValue() ? Hashers.at(keyIdx)->Hash(item) : 0);
    }

    ui64 Finish(ui64 outputsSize) {
        return THashV2::SpreadHash(Hash) % outputsSize;
    }
};

struct TColumnShardHashV1 {
    TColumnShardHashV1(
        std::size_t shardCount,
        TVector<ui64> taskIndexByHash,
        TVector<NYql::NProto::TypeIds> keyColumnTypes
    )
        : ShardCount(shardCount)
        , TaskIndexByHash(std::move(taskIndexByHash))
        , KeyColumnTypes(std::move(keyColumnTypes))
        , HashCalcer(0)
    {}

    void Start() {
        HashCalcer.Start();
    }

    template <typename TValue>
    void Update(const TValue& uv, size_t keyIdx) {
        if (!uv.HasValue()) {
            return;
        }

        if (uv.IsBoxed()) {
            if (auto list = uv.GetElements()) {
                UpdateImpl(*list, keyIdx);
                return;
            }
        }

        UpdateImpl(uv, keyIdx);
    }

    ui64 Finish(ui64 outputsSize) {
        ui64 hash = HashCalcer.Finish();
        hash = std::min<ui32>(hash / (Max<ui64>() / ShardCount), ShardCount - 1);
        auto result = TaskIndexByHash.at(hash);
        Y_ENSURE(result < outputsSize);
        return result;
    }

private:
    template <typename TValue>
    void UpdateImpl(const TValue& uv, size_t keyIdx) {
        switch (KeyColumnTypes.at(keyIdx)) {
            case NYql::NProto::Bool: {
                auto value = uv.template Get<bool>();
                HashCalcer.Update(reinterpret_cast<const ui8*>(&value), sizeof(value));
                break;
            }
            case NYql::NProto::Int8: {
                auto value = uv.template Get<uint8_t>();
                HashCalcer.Update(reinterpret_cast<const ui8*>(&value), sizeof(value));
                break;
            }
            case NYql::NProto::Uint8: {
                auto value = uv.template Get<uint8_t>();
                HashCalcer.Update(reinterpret_cast<const ui8*>(&value), sizeof(value));
                break;
            }
            case NYql::NProto::Int16: {
                auto value = uv.template Get<int16_t>();
                HashCalcer.Update(reinterpret_cast<const ui8*>(&value), sizeof(value));
                break;
            }
            case NYql::NProto::Date:
            case NYql::NProto::TzDate:
            case NYql::NProto::Uint16: {
                auto value = uv.template Get<uint16_t>();
                HashCalcer.Update(reinterpret_cast<const ui8*>(&value), sizeof(value));
                break;
            }
            case NYql::NProto::Int32:
            case NYql::NProto::Date32:
            case NYql::NProto::TzDate32: {
                auto value = uv.template Get<int32_t>();
                HashCalcer.Update(reinterpret_cast<const ui8*>(&value), sizeof(value));
                break;
            }
            case NYql::NProto::Uint32:
            case NYql::NProto::Datetime:
            case NYql::NProto::TzDatetime: {
                auto value = uv.template Get<uint32_t>();
                HashCalcer.Update(reinterpret_cast<const ui8*>(&value), sizeof(value));
                break;
            }
            case NYql::NProto::Int64:
            case NYql::NProto::Interval:
            case NYql::NProto::Interval64:
            case NYql::NProto::Datetime64:
            case NYql::NProto::TzDatetime64:
            case NYql::NProto::Timestamp64:
            case NYql::NProto::TzTimestamp64: {
                auto value = uv.template Get<i64>();
                HashCalcer.Update(reinterpret_cast<const ui8*>(&value), sizeof(value));
                break;
            }
            case NYql::NProto::Uint64:
            case NYql::NProto::Timestamp:
            case NYql::NProto::TzTimestamp: {
                auto value = uv.template Get<ui64>();
                HashCalcer.Update(reinterpret_cast<const ui8*>(&value), sizeof(value));
                break;
            }
            case NYql::NProto::Double: {
                auto value = uv.template Get<double>();
                HashCalcer.Update(reinterpret_cast<const ui8*>(&value), sizeof(value));
                break;
            }
            case NYql::NProto::Float: {
                auto value = uv.template Get<float>();
                HashCalcer.Update(reinterpret_cast<const ui8*>(&value), sizeof(value));
                break;
            }
            case NYql::NProto::Uuid:
            case NYql::NProto::String:
            case NYql::NProto::Utf8: {
                auto value = uv.AsStringRef();
                HashCalcer.Update(reinterpret_cast<const ui8*>(value.Data()), value.Size());
                break;
            }
            case NYql::NProto::Decimal: {
                auto value = uv.GetInt128();
                HashCalcer.Update(reinterpret_cast<const ui8*>(&value), sizeof(value));
                break;
            }
            case NYql::NProto::JsonDocument:
            case NYql::NProto::DyNumber:
            case NYql::NProto::Yson:
            case NYql::NProto::Json: {
                auto typeStr = TypeIds_Name(KeyColumnTypes[keyIdx]);;
                Y_ENSURE(false, TStringBuilder{} << "HashFunc for HashShuffle isn't supported with such type: " << typeStr);
                break;
            }
            // we prefer not to use default to incentivize developers write implementations for hashfunc (otherwise code won't be compiled)
            case NYql::NProto::TypeIds_INT_MAX_SENTINEL_DO_NOT_USE_:
            case NYql::NProto::TypeIds_INT_MIN_SENTINEL_DO_NOT_USE_:
            case NYql::NProto::UNUSED: {
                Y_ENSURE(false, "Attempted to use internal sentinel/special type markers. These types are for system use only and not valid for actual values.");
            }
        }
    }

private:
    const std::size_t ShardCount;
    const TVector<ui64> TaskIndexByHash;
    const TVector<NYql::NProto::TypeIds> KeyColumnTypes;
    NArrow::NHash::NXX64::TStreamStringHashCalcer HashCalcer;
};

//////////////////////////////////

class TDqOutputMultiConsumer : public IDqOutputConsumer {
public:
    explicit TDqOutputMultiConsumer(TVector<IDqOutputConsumer::TPtr>&& consumers)
        : Consumers(std::move(consumers))
    {
        YQL_ENSURE(!Consumers.empty());
    }

    EDqFillLevel GetFillLevel() const override {
        EDqFillLevel result = SoftLimit;
        for (auto consumer : Consumers) {
            switch(consumer->GetFillLevel()) {
                case HardLimit:
                    return HardLimit;
                case SoftLimit:
                    break;
                case NoLimit:
                    result = NoLimit;
                    break;
            }
        }
        return result;
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

    void Consume(NDqProto::TWatermark&& watermark) override {
        for (auto& consumer : Consumers) {
            consumer->Consume(NDqProto::TWatermark(watermark));
        }
    }

    void Finish() override {
        for (auto& consumer : Consumers) {
            consumer->Finish();
        }
    }

    void Flush() override {
        for (auto& consumer : Consumers) {
            consumer->Flush();
        }
    }

    bool IsFinished() const override {
        for (auto consumer : Consumers) {
            if (!consumer->IsFinished()) {
                return false;
            }
        }
        return true;
    }

    bool IsEarlyFinished() const override {
        for (auto consumer : Consumers) {
            if (!consumer->IsEarlyFinished()) {
                return false;
            }
        }
        return true;
    }

    TString DebugString() override {
        TStringBuilder builder;
        builder << "TDqOutputMultiConsumer [";
        for (auto consumer : Consumers) {
            builder << consumer->DebugString();
        }
        builder << ']';
        return builder;
    }

private:
    TVector<IDqOutputConsumer::TPtr> Consumers;
};

class TDqOutputMapConsumer : public IDqOutputConsumer {
public:
    TDqOutputMapConsumer(IDqOutput::TPtr output)
        : Output(output) {}

    EDqFillLevel GetFillLevel() const override {
        return Output->UpdateFillLevel();
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

    void Consume(NDqProto::TWatermark&& watermark) override {
        Output->Push(std::move(watermark));
    }

    void Finish() override {
        Output->Finish();
    }

    void Flush() override {
        Output->Flush();
    }

    bool IsFinished() const override {
        return Output->IsFinished();
    }

    bool IsEarlyFinished() const override {
        return Output->IsEarlyFinished();
    }

    TString DebugString() override {
        return "TDqOutputMapConsumer";
    }

private:
    IDqOutput::TPtr Output;
};

template <typename THashFunc>
class TDqOutputHashPartitionConsumer : public IDqOutputConsumer {
private:
    mutable TUnboxedValue WaitingValue;
    mutable TUnboxedValueVector WideWaitingValues;
    mutable IDqOutput::TPtr OutputWaiting;
public:
    TDqOutputHashPartitionConsumer(
        TVector<IDqOutput::TPtr>&& outputs,
        TVector<TColumnInfo>&& keyColumns,
        TMaybe<ui32> outputWidth,
        THashFunc hashFunc
    )
        : Outputs(std::move(outputs))
        , KeyColumns(std::move(keyColumns))
        , OutputWidth(outputWidth)
        , HashFunc(std::move(hashFunc))
    {
        if (outputWidth.Defined()) {
            WideWaitingValues.resize(*outputWidth);
        }

        Aggregator = std::make_shared<TDqFillAggregator>();
        for (auto output : Outputs) {
            output->SetFillAggregator(Aggregator);
        }
    }

    EDqFillLevel GetFillLevel() const override {
        auto result = Aggregator->GetFillLevel();
        if (result == HardLimit) {
            for (auto output : Outputs) {
                output->UpdateFillLevel();
            }
            result = Aggregator->GetFillLevel();
        }
        return result;
    }

    TString DebugString() override {
        TStringBuilder builder;
        builder << "TDqOutputHashPartitionConsumer " << Aggregator->DebugString() << " Channels {";
        ui32 i = 0;
        for (auto output : Outputs) {
            builder << " C" << i++ << ":" << FillLevelToString(output->UpdateFillLevel());
            if (i >= 20) {
                builder << "...";
                break;
            }
        }
        builder << " }";
        return builder;
    }

    void Consume(TUnboxedValue&& value) final {
        YQL_ENSURE(!OutputWidth.Defined());
        ui32 partitionIndex = GetHashPartitionIndex(value);
        Outputs[partitionIndex]->Push(std::move(value));
    }

    void WideConsume(TUnboxedValue* values, ui32 count) final {
        YQL_ENSURE(OutputWidth.Defined() && count == OutputWidth);
        ui32 partitionIndex = GetHashPartitionIndex(values);
        Outputs[partitionIndex]->WidePush(values, count);
    }

    void Consume(NDqProto::TCheckpoint&& checkpoint) override {
        for (auto& output : Outputs) {
            output->Push(NDqProto::TCheckpoint(checkpoint));
        }
    }

    void Consume(NDqProto::TWatermark&& watermark) override {
        for (auto& output : Outputs) {
            output->Push(NDqProto::TWatermark(watermark));
        }
    }

    void Finish() final {
        for (auto& output : Outputs) {
            output->Finish();
        }
    }

    void Flush() final {
        for (auto& output : Outputs) {
            output->Flush();
        }
    }

    bool IsFinished() const override {
        return Aggregator->IsFinished();
    }

    bool IsEarlyFinished() const override {
        return Aggregator->IsEarlyFinished();
    }

private:
    size_t GetHashPartitionIndex(const TUnboxedValue& value) {
        HashFunc.Start();
        for (std::size_t keyIdx = 0; keyIdx < KeyColumns.size(); ++keyIdx) {
            const auto& keyColumn = KeyColumns[keyIdx];
            HashFunc.Update(value.GetElement(keyColumn.Index), keyIdx);
        }
        return HashFunc.Finish(Outputs.size());
    }

    size_t GetHashPartitionIndex(const TUnboxedValue* values) {
        HashFunc.Start();
        for (std::size_t keyIdx = 0; keyIdx < KeyColumns.size(); ++keyIdx) {
            const auto& keyColumn = KeyColumns[keyIdx];
            MKQL_ENSURE_S(keyColumn.Index < OutputWidth);
            HashFunc.Update(values[keyColumn.Index], keyIdx);
        }
        return HashFunc.Finish(Outputs.size());
    }

private:
    const TVector<IDqOutput::TPtr> Outputs;
    const TVector<TColumnInfo> KeyColumns;
    const TMaybe<ui32> OutputWidth;
    THashFunc HashFunc;
    std::shared_ptr<TDqFillAggregator> Aggregator;
};

template <typename THashFunc>
class TDqOutputHashPartitionConsumerScalar : public IDqOutputConsumer {
public:
    TDqOutputHashPartitionConsumerScalar(
        TVector<IDqOutput::TPtr>&& outputs,
        TVector<TColumnInfo>&& keyColumns,
        const NKikimr::NMiniKQL::TType* outputType,
        THashFunc hashFunc
    )
        : Outputs_(std::move(outputs))
        , KeyColumns_(std::move(keyColumns))
        , OutputWidth_(static_cast<const NMiniKQL::TMultiType*>(outputType)->GetElementsCount())
        , WaitingValues_(OutputWidth_)
        , HashFunc(std::move(hashFunc))
    {
        auto multiType = static_cast<const NMiniKQL::TMultiType*>(outputType);
        for (auto& column : KeyColumns_) {
            auto columnType = multiType->GetElementType(column.Index);
            YQL_ENSURE(columnType->IsBlock());
            auto blockType = static_cast<const NMiniKQL::TBlockType*>(columnType);
            YQL_ENSURE(blockType->GetShape() == NMiniKQL::TBlockType::EShape::Scalar);
            Readers_.emplace_back(MakeBlockReader(TTypeInfoHelper(), blockType->GetItemType()));
        }

        Aggregator = std::make_shared<TDqFillAggregator>();
        for (auto output : Outputs_) {
            output->SetFillAggregator(Aggregator);
        }
    }
private:
    EDqFillLevel GetFillLevel() const override {
        auto result = Aggregator->GetFillLevel();
        if (result == HardLimit) {
            for (auto output : Outputs_) {
                output->UpdateFillLevel();
            }
            result = Aggregator->GetFillLevel();
        }
        return result;
    }

    TString DebugString() override {
        TStringBuilder builder;
        builder << "TDqOutputHashPartitionConsumerScalar " << Aggregator->DebugString() << " Channels {";
        ui32 i = 0;
        for (auto output : Outputs_) {
            builder << " C" << i++ << ":" << FillLevelToString(output->UpdateFillLevel());
            if (i >= 20) {
                builder << "...";
                break;
            }
        }
        builder << " }";
        return builder;
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
        Outputs_[GetHashPartitionIndex(values)]->WidePush(values, count);
    }

    void Consume(NDqProto::TCheckpoint&& checkpoint) override {
        for (auto& output : Outputs_) {
            output->Push(NDqProto::TCheckpoint(checkpoint));
        }
    }

    void Consume(NDqProto::TWatermark&& watermark) override {
        for (auto& output : Outputs_) {
            output->Push(NDqProto::TWatermark(watermark));
        }
    }

    void Finish() final {
        for (auto& output : Outputs_) {
            output->Finish();
        }
    }

    void Flush() final {
        for (auto& output : Outputs_) {
            output->Flush();
        }
    }

    bool IsFinished() const override {
        return Aggregator->IsFinished();
    }

    bool IsEarlyFinished() const override {
        return Aggregator->IsEarlyFinished();
    }

    size_t GetHashPartitionIndex(const TUnboxedValue* values) {
        HashFunc.Start();
        for (size_t keyId = 0; keyId < KeyColumns_.size(); keyId++) {
            YQL_ENSURE(KeyColumns_[keyId].Index < OutputWidth_);
            const TUnboxedValue& value = values[KeyColumns_[keyId].Index];
            TBlockItem item = Readers_[keyId]->GetScalarItem(*TArrowBlock::From(value).GetDatum().scalar());
            HashFunc.Update(item, keyId);
        }
        return HashFunc.Finish(Outputs_.size());
    }

private:
    const TVector<IDqOutput::TPtr> Outputs_;
    const TVector<TColumnInfo> KeyColumns_;
    const ui32 OutputWidth_;
    TVector<std::unique_ptr<IBlockReader>> Readers_;
    mutable TUnboxedValueVector WaitingValues_;
    THashFunc HashFunc;
    std::shared_ptr<TDqFillAggregator> Aggregator;
};

template <typename THashFunc>
class TDqOutputHashPartitionConsumerBlock : public IDqOutputConsumer {
public:
    TDqOutputHashPartitionConsumerBlock(TVector<IDqOutput::TPtr>&& outputs, TVector<TColumnInfo>&& keyColumns,
        const  NKikimr::NMiniKQL::TType* outputType,
        const NKikimr::NMiniKQL::THolderFactory& holderFactory,
        TMaybe<ui8> minFillPercentage,
        THashFunc hashFunc,
        NUdf::IPgBuilder* pgBuilder
    )
        : OutputType_(static_cast<const NMiniKQL::TMultiType*>(outputType))
        , HolderFactory_(holderFactory)
        , Outputs_(std::move(outputs))
        , KeyColumns_(std::move(keyColumns))
        , OutputWidth_(OutputType_->GetElementsCount())
        , MinFillPercentage_(minFillPercentage)
        , PgBuilder_(pgBuilder)
        , HashFunc(std::move(hashFunc))
    {
        TTypeInfoHelper helper;

        TVector<const NMiniKQL::TType*> blockTypes;
        for (auto& columnType : OutputType_->GetElements()) {
            YQL_ENSURE(columnType->IsBlock());
            auto blockType = static_cast<const NMiniKQL::TBlockType*>(columnType);
            if (blockType->GetShape() == NMiniKQL::TBlockType::EShape::Many) {
                blockTypes.emplace_back(blockType->GetItemType());
            }
        }

        for (auto& column : KeyColumns_) {
            auto columnType = OutputType_->GetElementType(column.Index);
            YQL_ENSURE(columnType->IsBlock());
            auto blockType = static_cast<const NMiniKQL::TBlockType*>(columnType);
            Readers_.emplace_back(MakeBlockReader(helper, blockType->GetItemType()));
        }

        Aggregator = std::make_shared<TDqFillAggregator>();
        for (auto output : Outputs_) {
            output->SetFillAggregator(Aggregator);
        }
    }

private:
    EDqFillLevel GetFillLevel() const override {
        auto result = Aggregator->GetFillLevel();
        if (result == HardLimit) {
            for (auto output : Outputs_) {
                output->UpdateFillLevel();
            }
            result = Aggregator->GetFillLevel();
        }
        return result;
    }

    TString DebugString() override {
        TStringBuilder builder;
        builder << "TDqOutputHashPartitionConsumerBlock " << Aggregator->DebugString() << " Channels {";
        ui32 i = 0;
        for (auto output : Outputs_) {
            builder << " C" << i++ << ":" << FillLevelToString(output->UpdateFillLevel());
            if (i >= 20) {
                builder << "...";
                break;
            }
        }
        builder << " }";
        return builder;
    }

    void Consume(TUnboxedValue&& value) final {
        Y_UNUSED(value);
        YQL_ENSURE(false, "Consume() called on wide block stream");
    }

    void WideConsume(TUnboxedValue values[], ui32 count) final {
        YQL_ENSURE(count == OutputWidth_);

        const ui64 inputBlockLen = TArrowBlock::From(values[count - 1]).GetDatum().scalar_as<arrow::UInt64Scalar>().value;
        if (!inputBlockLen) {
            return;
        }

        TVector<const arrow::Datum*> datums;
        datums.reserve(count - 1);
        for (ui32 i = 0; i < count - 1; ++i) {
            datums.push_back(&TArrowBlock::From(values[i]).GetDatum());
        }

        TVector<TVector<ui64>> outputBlockIndexes(Outputs_.size());
        for (ui64 i = 0; i < inputBlockLen; ++i) {
            std::size_t idx = GetHashPartitionIndex(datums.data(), i);
            outputBlockIndexes[idx].push_back(i);
        }

        TVector<std::unique_ptr<TArgsDechunker>> outputData;
        for (size_t i = 0; i < Outputs_.size(); ++i) {
            ui64 outputBlockLen = outputBlockIndexes[i].size();
            if (!outputBlockLen) {
                outputData.emplace_back();
                continue;
            }
            MakeBuilders(outputBlockLen);
            const ui64* indexes = outputBlockIndexes[i].data();

            std::vector<arrow::Datum> output;
            for (size_t j = 0; j < datums.size(); ++j) {
                const arrow::Datum* src = datums[j];
                if (src->is_scalar()) {
                    output.emplace_back(*src);
                } else {
                    IArrayBuilder::TArrayDataItem dataItem {
                        .Data = src->array().get(),
                        .StartOffset = 0,
                    };
                    Builders_[j]->AddMany(&dataItem, 1, indexes, outputBlockLen);
                    output.emplace_back(Builders_[j]->Build(true));
                }
            }
            outputData.emplace_back(std::make_unique<TArgsDechunker>(std::move(output)));
        }

        DoConsume(std::move(outputData));
    }

    void DoConsume(TVector<std::unique_ptr<TArgsDechunker>>&& outputData) const {
        Y_ENSURE(outputData.size() == Outputs_.size());

        while (!outputData.empty()) {
            bool hasData = false;
            for (size_t i = 0; i < Outputs_.size(); ++i) {
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

    void Consume(NDqProto::TWatermark&& watermark) override {
        for (auto& output : Outputs_) {
            output->Push(NDqProto::TWatermark(watermark));
        }
    }

    void Finish() final {
        for (auto& output : Outputs_) {
            output->Finish();
        }
    }

    void Flush() final {
        for (auto& output : Outputs_) {
            output->Flush();
        }
    }

    bool IsFinished() const override {
        return Aggregator->IsFinished();
    }

    bool IsEarlyFinished() const override {
        return Aggregator->IsEarlyFinished();
    }

    size_t GetHashPartitionIndex(const arrow::Datum* values[], ui64 blockIndex) {
        HashFunc.Start();

        for (size_t keyId = 0; keyId < KeyColumns_.size(); keyId++) {
            const ui32 columnIndex = KeyColumns_[keyId].Index;
            Y_DEBUG_ABORT_UNLESS(columnIndex < OutputWidth_);

            if (*KeyColumns_[keyId].IsScalar) {
                TBlockItem item = Readers_[keyId]->GetScalarItem(*values[columnIndex]->scalar());
                HashFunc.Update(item, keyId);
            } else {
                TBlockItem item = Readers_[keyId]->GetItem(*values[columnIndex]->array(), blockIndex);
                HashFunc.Update(item, keyId);
            }
        }

        return HashFunc.Finish(Outputs_.size());
    }

    void MakeBuilders(ui64 maxBlockLen) {
        Builders_.clear();
        TTypeInfoHelper helper;
        for (auto& columnType : OutputType_->GetElements()) {
            YQL_ENSURE(columnType->IsBlock());
            auto blockType = static_cast<const NMiniKQL::TBlockType*>(columnType);
            if (blockType->GetShape() == NMiniKQL::TBlockType::EShape::Many) {
                auto itemType = blockType->GetItemType();
                Builders_.emplace_back(MakeArrayBuilder(helper, itemType, *NYql::NUdf::GetYqlMemoryPool(), maxBlockLen, PgBuilder_, {.MinFillPercentage=MinFillPercentage_}));
            } else {
                Builders_.emplace_back();
            }
        }
    }

private:
    const NKikimr::NMiniKQL::TMultiType* const OutputType_;
    const NKikimr::NMiniKQL::THolderFactory& HolderFactory_;

    const TVector<IDqOutput::TPtr> Outputs_;
    mutable TVector<std::unique_ptr<TArgsDechunker>> OutputData_;

    const TVector<TColumnInfo> KeyColumns_;
    const ui32 OutputWidth_;
    const TMaybe<ui8> MinFillPercentage_;

    TVector<std::unique_ptr<IBlockReader>> Readers_;
    TVector<std::unique_ptr<IArrayBuilder>> Builders_;

    NUdf::IPgBuilder* PgBuilder_;
    THashFunc HashFunc;
    std::shared_ptr<TDqFillAggregator> Aggregator;
};

class TDqOutputBroadcastConsumer : public IDqOutputConsumer {
public:
    TDqOutputBroadcastConsumer(TVector<IDqOutput::TPtr>&& outputs, TMaybe<ui32> outputWidth)
        : Outputs(std::move(outputs))
        , OutputWidth(outputWidth)
        , Tmp(outputWidth.Defined() ? *outputWidth : 0u)
    {
        Aggregator = std::make_shared<TDqFillAggregator>();
        for (auto output : Outputs) {
            output->SetFillAggregator(Aggregator);
        }
    }

    EDqFillLevel GetFillLevel() const override {
        auto result = Aggregator->GetFillLevel();
        if (result == HardLimit) {
            for (auto output : Outputs) {
                output->UpdateFillLevel();
            }
            result = Aggregator->GetFillLevel();
        }
        return result;
    }

    TString DebugString() override {
        TStringBuilder builder;
        builder << "TDqOutputBroadcastConsumer " << Aggregator->DebugString() << " Channels {";
        ui32 i = 0;
        for (auto output : Outputs) {
            builder << " C" << i++ << ":" << FillLevelToString(output->UpdateFillLevel());
            if (i >= 20) {
                builder << "...";
                break;
            }
        }
        builder << " }";
        return builder;
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

    void Consume(NDqProto::TWatermark&& watermark) override {
        for (auto& output : Outputs) {
            output->Push(NDqProto::TWatermark(watermark));
        }
    }

    void Finish() override {
        for (auto& output : Outputs) {
            output->Finish();
        }
    }

    void Flush() override {
        for (auto& output : Outputs) {
            output->Flush();
        }
    }

    bool IsFinished() const override {
        return Aggregator->IsFinished();
    }

    bool IsEarlyFinished() const override {
        return Aggregator->IsEarlyFinished();
    }

private:
    TVector<IDqOutput::TPtr> Outputs;
    const TMaybe<ui32> OutputWidth;
    TUnboxedValueVector Tmp;
    std::shared_ptr<TDqFillAggregator> Aggregator;
};

// Adaptive scatter router with lazy channel activation.
//
// Routing: producer thread calls PickBest() to choose a destination channel,
// preferring the least-loaded one. Loads are read from ChannelLevels_ atomics,
// updated by consumer threads via LevelChangeCallback (release/acquire).
//
// Lazy activation: only primaryIdx is active at start; a new channel is activated
// when all active channels leave NoLimit. Never deactivates. Keeps gRPC streams
// and buffers from being spun up for small data volumes.
//
// Thread-safety: ActiveChannels_, InactiveCursor_, RoundRobinPos_ are touched
// only from the producer thread (single-threaded DqTaskRunner contract).
// ChannelLevels_ is the sole cross-thread surface.
class TScatterRouter {
public:
    static constexpr ui32 kLevelCount = 3u;

    explicit TScatterRouter(ui32 channelCount, ui32 primaryIdx = 0)
        : ChannelCount_(channelCount)
    {
        Y_ENSURE(channelCount > 0, "TScatterRouter requires at least one channel");
        Y_ENSURE(primaryIdx < channelCount, "primaryIdx out of range");

        ChannelLevels_ = std::shared_ptr<std::atomic<EDqFillLevel>[]>(
            new std::atomic<EDqFillLevel>[channelCount]
        );
        for (ui32 j = 0; j < channelCount; ++j) {
            ChannelLevels_[j].store(NoLimit, std::memory_order_relaxed);
        }

        ActiveChannels_.reserve(channelCount);
        ActiveChannels_.push_back(primaryIdx);

        InactiveChannels_.reserve(channelCount - 1);
        for (ui32 i = 1; i < channelCount; ++i) {
            InactiveChannels_.push_back((primaryIdx + i) % channelCount);
        }
    }

    std::weak_ptr<std::atomic<EDqFillLevel>[]> WeakLevels() const {
        return ChannelLevels_;
    }

    ui32 ChannelCount() const { 
        return ChannelCount_; 
    }
    ui32 ActiveCount() const { 
        return static_cast<ui32>(ActiveChannels_.size()); 
    }

    EDqFillLevel GetFillLevel() const {
        bool anySoft = false;
        for (const ui32 idx : ActiveChannels_) {
            const auto l = ChannelLevels_[idx].load(std::memory_order_acquire);
            if (l == NoLimit) return NoLimit;
            if (l == SoftLimit) anySoft = true;
        }
        for (size_t i = InactiveCursor_; i < InactiveChannels_.size(); ++i) {
            const auto l = ChannelLevels_[InactiveChannels_[i]].load(std::memory_order_acquire);
            if (l == NoLimit) return NoLimit;
            if (l == SoftLimit) anySoft = true;
        }
        return anySoft ? SoftLimit : HardLimit;
    }

    std::pair<ui32, EDqFillLevel> PickBest() {
        const ui32 n = static_cast<ui32>(ActiveChannels_.size());

        if (n == 1) {
            const ui32 idx = ActiveChannels_[0];
            const auto lvl = ChannelLevels_[idx].load(std::memory_order_acquire);
            if (lvl != NoLimit && HasPending()) {
                // Scan inactive channels, skipping stubs (HardLimit), until we
                // find one that is actually available. Stubs get activated into
                // ActiveChannels_ so their callback can update the level later.
                while (HasPending()) {
                    const ui32 nextIdx = InactiveChannels_[InactiveCursor_];
                    const auto nextLvl = ChannelLevels_[nextIdx].load(std::memory_order_acquire);
                    const ui32 activated = ActivateNext();
                    if (nextLvl < HardLimit) {
                        return {activated, nextLvl};
                    }
                }
                // All inactive were stubs; fall through to the current channel.
            }
            return {idx, lvl};
        }

        const ui32 start = RoundRobinPos_++ % n;
        ui32 bestIdx = 0;
        EDqFillLevel bestLevel = HardLimit;
        bool initialized = false;

        for (ui32 i = 0; i < n; ++i) {
            const ui32 idx = ActiveChannels_[(start + i) % n];
            const auto lvl = ChannelLevels_[idx].load(std::memory_order_acquire);
            if (lvl == NoLimit) {
                return {idx, NoLimit};
            }
            if (!initialized || lvl < bestLevel) {
                bestLevel = lvl;
                bestIdx = idx;
                initialized = true;
            }
        }

        if (HasPending()) {
            while (HasPending()) {
                const ui32 nextIdx = InactiveChannels_[InactiveCursor_];
                const auto nextLvl = ChannelLevels_[nextIdx].load(std::memory_order_acquire);
                const ui32 activated = ActivateNext();
                if (nextLvl < HardLimit) {
                    return {activated, nextLvl};
                }
            }
            // All inactive were stubs; fall through to best active channel.
        }

        return {bestIdx, bestLevel};
    }

private:
    bool HasPending() const {
        return InactiveCursor_ < InactiveChannels_.size();
    }

    ui32 ActivateNext() {
        Y_ENSURE(HasPending());
        const ui32 idx = InactiveChannels_[InactiveCursor_++];
        ActiveChannels_.push_back(idx);
        return idx;
    }

    const ui32 ChannelCount_;
    std::shared_ptr<std::atomic<EDqFillLevel>[]> ChannelLevels_;
    TVector<ui32> ActiveChannels_;
    TVector<ui32> InactiveChannels_;
    size_t InactiveCursor_ = 0;
    ui32 RoundRobinPos_ = 0;
};

class TDqOutputScatterConsumer : public IDqOutputConsumer {
public:
    TDqOutputScatterConsumer(TVector<IDqOutput::TPtr>&& outputs, TMaybe<ui32> outputWidth,
                             ui32 primaryChannelIdx = 0)
        : Outputs(std::move(outputs))
        , OutputWidth(outputWidth)
        , Tmp(outputWidth.Defined() ? *outputWidth : 0u)
        , Router_(static_cast<ui32>(Outputs.size()), primaryChannelIdx)
    {
        Aggregator = std::make_shared<TDqFillAggregator>();
        auto weakLevels = Router_.WeakLevels();
        for (ui32 i = 0; i < Outputs.size(); ++i) {
            YQL_ENSURE(Outputs[i]->SupportsLevelChangeCallback(),
                "TDqOutputScatterConsumer: output " << i << " does not support LevelChangeCallback. "
                "Only implementations that override SupportsLevelChangeCallback() (e.g. TDqOutputChannel) "
                "may be used with Scatter.");
            Outputs[i]->SetFillAggregator(Aggregator);
            Outputs[i]->SetLevelChangeCallback([weakLevels, i](EDqFillLevel /*from*/, EDqFillLevel to) {
                if (auto levels = weakLevels.lock()) {
                    levels[i].store(to, std::memory_order_release);
                }
            });
            // Sync router's view from the actual fill level. Unbound remote channels
            // (TChannelStub) report HardLimit; initializing to NoLimit would cause the
            // router to route to a stub and crash.
            if (auto levels = weakLevels.lock()) {
                levels[i].store(Outputs[i]->GetFillLevel(), std::memory_order_relaxed);
            }
        }
    }

    EDqFillLevel GetFillLevel() const override {
        return Router_.GetFillLevel();
    }

    void Consume(TUnboxedValue&& value) final {
        YQL_ENSURE(!OutputWidth.Defined());
        auto [idx, level] = Router_.PickBest();
        ++PicksByLevel[static_cast<ui32>(level)];
        Outputs[idx]->Push(std::move(value));
    }

    void WideConsume(TUnboxedValue* values, ui32 count) final {
        YQL_ENSURE(OutputWidth.Defined() && OutputWidth == count);
        auto [idx, level] = Router_.PickBest();
        ++PicksByLevel[static_cast<ui32>(level)];
        std::copy(values, values + count, Tmp.begin());
        Outputs[idx]->WidePush(Tmp.data(), count);
    }

    void Consume(NDqProto::TCheckpoint&& checkpoint) override {
        for (auto& output : Outputs) {
            output->Push(NDqProto::TCheckpoint(checkpoint));
        }
    }

    void Consume(NDqProto::TWatermark&& watermark) override {
        for (auto& output : Outputs) {
            output->Push(NDqProto::TWatermark(watermark));
        }
    }

    void Finish() override {
        const ui64 total = PicksByLevel[0] + PicksByLevel[1] + PicksByLevel[2];
        // Per-stage summary. TRACE in the common case to keep prod logs quiet;
        // WARN when >10% of picks hit HardLimit — that means scatter was
        // consistently back-pressured and upstream/downstream capacity is
        // likely under-provisioned.
        const bool backpressured = total > 0 && PicksByLevel[2] * 10 > total;
        // TODO(anely-d): switch quiet case back to TRACE once ProviderDq log level is raised to 8 on cluster
        const auto level = backpressured ? NLog::ELevel::WARN : NLog::ELevel::DEBUG;
        YQL_CVLOG(level, NLog::EComponent::ProviderDq) << "[Scatter] outputs=" << Outputs.size()
            << " active=" << Router_.ActiveCount()
            << " picks total=" << total
            << " noLimit=" << PicksByLevel[0]
            << " softLimit=" << PicksByLevel[1]
            << " hardLimit=" << PicksByLevel[2];
        for (auto& output : Outputs) {
            output->Finish();
        }
    }

    void Flush() override {
        for (auto& output : Outputs) {
            output->Flush();
        }
    }

    bool IsFinished() const override {
        return Aggregator->IsFinished();
    }

    bool IsEarlyFinished() const override {
        return Aggregator->IsEarlyFinished();
    }

private:
    TVector<IDqOutput::TPtr> Outputs;
    const TMaybe<ui32> OutputWidth;
    TUnboxedValueVector Tmp;
    std::shared_ptr<TDqFillAggregator> Aggregator;
    TScatterRouter Router_;
    std::array<ui64, TScatterRouter::kLevelCount> PicksByLevel = {};
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
    TVector<TColumnInfo>&& keyColumns, const NKikimr::NMiniKQL::TType* outputType,
    const NKikimr::NMiniKQL::THolderFactory& holderFactory,
    TMaybe<ui8> minFillPercentage,
    const NDqProto::TTaskOutputHashPartition& hashPartition,
    NUdf::IPgBuilder* pgBuilder
)
{
    YQL_ENSURE(!outputs.empty());
    YQL_ENSURE(!keyColumns.empty());

    TMaybe<ui32> outputWidth;
    if (outputType->IsMulti()) {
        outputWidth = static_cast<const NMiniKQL::TMultiType*>(outputType)->GetElementsCount();
    }

    switch (hashPartition.GetHashKindCase()) {
        case NDqProto::TTaskOutputHashPartition::kColumnShardHashV1: {
            auto& columnShardHashV1Proto = hashPartition.GetColumnShardHashV1();

            std::size_t shardCount = columnShardHashV1Proto.GetShardCount();
            TVector<ui64> taskIndexByHash{columnShardHashV1Proto.GetTaskIndexByHash().begin(), columnShardHashV1Proto.GetTaskIndexByHash().end()};

            TVector<NYql::NProto::TypeIds> keyColumnTypes;
            keyColumnTypes.reserve(columnShardHashV1Proto.GetKeyColumnTypes().size());
            for (const auto& keyColumnType: columnShardHashV1Proto.GetKeyColumnTypes()) {
                keyColumnTypes.push_back(static_cast<NYql::NProto::TypeIds>(keyColumnType));
            }

            const auto keyColumnsToString = [](const TVector<TColumnInfo>& keyColumns) -> TString {
                TVector<TString> stringNames;
                stringNames.reserve(keyColumns.size());
                for (const auto& keyColumn: keyColumns) {
                    stringNames.push_back(keyColumn.Name);
                }
                return "[" + JoinSeq(",", stringNames) + "]";
            };

            const auto keyTypesToString = [](const TVector<NYql::NProto::TypeIds>& keyColumnTypes) -> TString {
                TVector<TString> stringNames;
                stringNames.reserve(keyColumnTypes.size());
                for (const auto& keyColumnType: keyColumnTypes) {
                    stringNames.push_back(NYql::NProto::TypeIds_Name(keyColumnType));
                }
                return "[" + JoinSeq(",", stringNames) + "]";
            };

            Y_ENSURE(
                keyColumnTypes.size() == keyColumns.size(),
                TStringBuilder{} << "Hashshuffle keycolumns and keytypes args count mismatch, types: "
                << keyTypesToString(keyColumnTypes) << " for the columns: " << keyColumnsToString(keyColumns)
            );

            TColumnShardHashV1 hashFunc(shardCount, std::move(taskIndexByHash), std::move(keyColumnTypes));

            if (AnyOf(keyColumns, [](const auto& info) { return !info.IsBlockOrScalar(); })) {
                return MakeIntrusive<TDqOutputHashPartitionConsumer<TColumnShardHashV1>>(std::move(outputs), std::move(keyColumns), outputWidth, std::move(hashFunc));
            }

            YQL_ENSURE(outputWidth.Defined(), "Expecting wide stream for block data");
            if (AllOf(keyColumns, [](const auto& info) { return *info.IsScalar; })) {
                // all key columns are scalars - all data will go to single output
                return MakeIntrusive<TDqOutputHashPartitionConsumerScalar<TColumnShardHashV1>>(std::move(outputs), std::move(keyColumns), outputType, std::move(hashFunc));
            }

            return MakeIntrusive<TDqOutputHashPartitionConsumerBlock<TColumnShardHashV1>>(std::move(outputs), std::move(keyColumns), outputType, holderFactory, minFillPercentage, std::move(hashFunc), pgBuilder);
        }
        case NDqProto::TTaskOutputHashPartition::kHashV2: {
            if (AnyOf(keyColumns, [](const auto& info) { return !info.IsBlockOrScalar(); })) {
                THashV2 hashFunc(keyColumns);
                return MakeIntrusive<TDqOutputHashPartitionConsumer<THashV2>>(std::move(outputs), std::move(keyColumns), outputWidth, std::move(hashFunc));
            }

            TBlockHashV2 hashFunc(keyColumns);
            YQL_ENSURE(outputWidth.Defined(), "Expecting wide stream for block data");
            if (AllOf(keyColumns, [](const auto& info) { return *info.IsScalar; })) {
                // all key columns are scalars - all data will go to single output
                return MakeIntrusive<TDqOutputHashPartitionConsumerScalar<TBlockHashV2>>(std::move(outputs), std::move(keyColumns), outputType, std::move(hashFunc));
            }

            return MakeIntrusive<TDqOutputHashPartitionConsumerBlock<TBlockHashV2>>(std::move(outputs), std::move(keyColumns), outputType, holderFactory, minFillPercentage, std::move(hashFunc), pgBuilder);
        }
        case NDqProto::TTaskOutputHashPartition::kHashV1:
        default: {
            // TODO: `NUdf::IHash` and `NUdf::IBlockItemHasher` have different interfaces, we need 2 different classes! Refactor this.
            if (AnyOf(keyColumns, [](const auto& info) { return !info.IsBlockOrScalar(); })) {
                THashV1 hashFunc(keyColumns);
                return MakeIntrusive<TDqOutputHashPartitionConsumer<THashV1>>(std::move(outputs), std::move(keyColumns), outputWidth, std::move(hashFunc));
            }

            TBlockHashV1 hashFunc(keyColumns);
            YQL_ENSURE(outputWidth.Defined(), "Expecting wide stream for block data");
            if (AllOf(keyColumns, [](const auto& info) { return *info.IsScalar; })) {
                // all key columns are scalars - all data will go to single output
                return MakeIntrusive<TDqOutputHashPartitionConsumerScalar<TBlockHashV1>>(std::move(outputs), std::move(keyColumns), outputType, std::move(hashFunc));
            }

            return MakeIntrusive<TDqOutputHashPartitionConsumerBlock<TBlockHashV1>>(std::move(outputs), std::move(keyColumns), outputType, holderFactory, minFillPercentage, std::move(hashFunc), pgBuilder);
        }
    }
}

IDqOutputConsumer::TPtr CreateOutputBroadcastConsumer(TVector<IDqOutput::TPtr>&& outputs, TMaybe<ui32> outputWidth) {
    return MakeIntrusive<TDqOutputBroadcastConsumer>(std::move(outputs), outputWidth);
}

IDqOutputConsumer::TPtr CreateOutputScatterConsumer(TVector<IDqOutput::TPtr>&& outputs, TMaybe<ui32> outputWidth, ui32 primaryChannelIdx) {
    return MakeIntrusive<TDqOutputScatterConsumer>(std::move(outputs), outputWidth, primaryChannelIdx);
}

} // namespace NYql::NDq
