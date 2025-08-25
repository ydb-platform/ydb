#include "dq_async_output.h"
#include "dq_transport.h"

#include <yql/essentials/utils/yql_panic.h>

#include <deque>
#include <variant>

namespace NYql::NDq {
namespace {

class TDqAsyncOutputBuffer : public IDqAsyncOutputBuffer {
    struct TValueDesc {
        std::variant<NUdf::TUnboxedValue, NKikimr::NMiniKQL::TUnboxedValueVector, NDqProto::TWatermark, NDqProto::TCheckpoint> Value;
        ui64 EstimatedSize;

        TValueDesc(NUdf::TUnboxedValue&& value, ui64 size)
            : Value(std::move(value))
            , EstimatedSize(size)
        {
        }

        TValueDesc(NUdf::TUnboxedValue* values, ui32 count, ui64 size)
            : Value(NKikimr::NMiniKQL::TUnboxedValueVector(values, values + count))
            , EstimatedSize(size)
        {
        }

        TValueDesc(NDqProto::TWatermark&& watermark, ui64 size)
            : Value(std::move(watermark))
            , EstimatedSize(size)
        {
        }

        TValueDesc(NDqProto::TCheckpoint&& checkpoint, ui64 size)
            : Value(std::move(checkpoint))
            , EstimatedSize(size)
        {
        }

        bool HasValue() const {
            return std::holds_alternative<NUdf::TUnboxedValue>(Value) || std::holds_alternative<NKikimr::NMiniKQL::TUnboxedValueVector>(Value);
        }

        TValueDesc(const TValueDesc&) = default;
        TValueDesc(TValueDesc&&) = default;
    };

    static constexpr ui64 REESTIMATE_ROW_SIZE_PERIOD = 1024;

public:
    TDqOutputStats PushStats;
    TDqAsyncOutputBufferStats PopStats;

    TDqAsyncOutputBuffer(ui64 outputIndex, const TString& type, NKikimr::NMiniKQL::TType* outputType, ui64 maxStoredBytes, TCollectStatsLevel level)
        : MaxStoredBytes(maxStoredBytes)
        , OutputType(outputType)
        , IsBlock(IsBlockType(OutputType))
    {
        PushStats.Level = level;
        PopStats.Level = level;
        PopStats.OutputIndex = outputIndex;
        PopStats.Type = type;
    }

    ui64 GetOutputIndex() const override {
        return PopStats.OutputIndex;
    }

    const TDqOutputStats& GetPushStats() const override {
        return PushStats;
    }

    const TDqAsyncOutputBufferStats& GetPopStats() const override {
        return PopStats;
    }

    EDqFillLevel GetFillLevel() const override {
        return FillLevel;
    }

    EDqFillLevel UpdateFillLevel() override {
        auto result = EstimatedStoredBytes >= MaxStoredBytes ? HardLimit : NoLimit;
        if (FillLevel != result) {
            if (Aggregator) {
                Aggregator->UpdateCount(FillLevel, result);
            }
            FillLevel = result;
        }
        return result;
    }

    void SetFillAggregator(std::shared_ptr<TDqFillAggregator> aggregator) override {
        Aggregator = aggregator;
        Aggregator->AddCount(FillLevel);
    }

    void Push(NUdf::TUnboxedValue&& value) override {
        DoPush(std::move(value));
    }

    void WidePush(NUdf::TUnboxedValue* values, ui32 count) override {
        DoPush(values, count);
    }

    void Push(NDqProto::TWatermark&& watermark) override {
        const ui64 bytesSize = watermark.ByteSize();
        Values.emplace_back(std::move(watermark), bytesSize);
        EstimatedStoredBytes += bytesSize;

        ReportChunkIn(1, bytesSize);
    }

    void Push(NDqProto::TCheckpoint&& checkpoint) override {
        const ui64 bytesSize = checkpoint.ByteSize();
        Values.emplace_back(std::move(checkpoint), bytesSize);
        EstimatedStoredBytes += bytesSize;

        ReportChunkIn(1, bytesSize);
    }

    void Finish() override {
        Finished = true;
    }

    ui64 Pop(NKikimr::NMiniKQL::TUnboxedValueBatch& batch, ui64 bytes) override {
        batch.clear();
        ui64 valuesCount = 0;
        ui64 usedBytes = 0;

        if (Values.empty()) {
            PushStats.TryPause();
            return 0;
        }

        // Calc values count.
        for (auto iter = Values.cbegin(), end = Values.cend();
            usedBytes < bytes && iter != end && iter->HasValue();
            ++iter)
        {
            ++valuesCount;
            usedBytes += iter->EstimatedSize;
        }

        // Reserve size and return data.
        while (valuesCount--) {
            auto& value = Values.front().Value;
            if (std::holds_alternative<NUdf::TUnboxedValue>(value)) {
                batch.emplace_back(std::move(std::get<NUdf::TUnboxedValue>(value)));
            } else if (std::holds_alternative<NKikimr::NMiniKQL::TUnboxedValueVector>(value)) {
                auto& multiValue = std::get<NKikimr::NMiniKQL::TUnboxedValueVector>(value);
                batch.PushRow(multiValue.data(), multiValue.size());
            } else {
                YQL_ENSURE(false, "Unsupported output value");
            }
            Values.pop_front();
        }
        Y_ABORT_UNLESS(EstimatedStoredBytes >= usedBytes);
        EstimatedStoredBytes -= usedBytes;

        ReportChunkOut(batch.RowCount(), usedBytes);

        return usedBytes;
    }

    virtual ui64 Pop(TDqSerializedBatch&, ui64) override {
        YQL_ENSURE(!"Unimplemented");
        return 0;
    }

    bool Pop(NDqProto::TWatermark& watermark) override {
        if (!Values.empty() && std::holds_alternative<NDqProto::TWatermark>(Values.front().Value)) {
            watermark = std::move(std::get<NDqProto::TWatermark>(Values.front().Value));
            const auto size = Values.front().EstimatedSize;
            Y_ABORT_UNLESS(EstimatedStoredBytes >= size);
            EstimatedStoredBytes -= size;
            Values.pop_front();

            ReportChunkOut(1, size);

            return true;
        }
        PushStats.TryPause();
        return false;
    }

    bool Pop(NDqProto::TCheckpoint& checkpoint) override {
        if (!Values.empty() && std::holds_alternative<NDqProto::TCheckpoint>(Values.front().Value)) {
            checkpoint = std::move(std::get<NDqProto::TCheckpoint>(Values.front().Value));
            const auto size = Values.front().EstimatedSize;
            Y_ABORT_UNLESS(EstimatedStoredBytes >= size);
            EstimatedStoredBytes -= size;
            Values.pop_front();

            ReportChunkOut(1, size);

            return true;
        }
        PushStats.TryPause();
        return false;
    }

    bool HasData() const override {
        return EstimatedRowBytes > 0;
    }

    bool IsFinished() const override {
        if (!Finished) {
            return false;
        }
        for (const TValueDesc& v : Values) {
            if (v.HasValue()) {
                return false;
            }
        }
        // Finished and no data values.
        return true;
    }

    NKikimr::NMiniKQL::TType* GetOutputType() const override {
        return OutputType;
    }

private:
    template <typename... TArgs>
    void DoPush(TArgs&&... args) {
        if (ValuesPushed++ % REESTIMATE_ROW_SIZE_PERIOD == 0) {
            ReestimateRowBytes(args...);
        }

        Y_ABORT_UNLESS(EstimatedRowBytes > 0);
        EstimatedStoredBytes += EstimatedRowBytes;
        ReportChunkIn(GetRowsCount(args...), EstimatedRowBytes);

        Values.emplace_back(std::forward<TArgs>(args)..., EstimatedRowBytes);
    }

    ui64 GetRowsCount(const NUdf::TUnboxedValue& value) const {
        Y_UNUSED(value);
        return 1;
    }

    ui64 GetRowsCount(const NUdf::TUnboxedValue* values, ui32 count) const {
        if (!IsBlock) {
            return 1;
        }
        return NKikimr::NMiniKQL::TArrowBlock::From(values[count - 1]).GetDatum().scalar_as<arrow::UInt64Scalar>().value;
    }

    void ReestimateRowBytes(const NUdf::TUnboxedValue& value) {
        DoReestimateRowBytes(TDqDataSerializer::EstimateSize(value, OutputType));
    }

    void ReestimateRowBytes(const NUdf::TUnboxedValue* values, ui32 count) {
        const auto* multiType = static_cast<NKikimr::NMiniKQL::TMultiType* const>(OutputType);
        YQL_ENSURE(multiType, "Expected multi type for wide output");

        ui64 valueSize = 0;
        for (ui32 i = 0; i < count; ++i) {
            valueSize += TDqDataSerializer::EstimateSize(values[i], multiType->GetElementType(i));
        }
        DoReestimateRowBytes(valueSize);
    }

    void DoReestimateRowBytes(ui64 valueSize) {
        if (EstimatedRowBytes) {
            EstimatedRowBytes = static_cast<ui64>(0.6 * valueSize + 0.4 * EstimatedRowBytes);
        } else {
            EstimatedRowBytes = valueSize;
        }
        if (!EstimatedRowBytes) {
            EstimatedRowBytes = 1;
        }
    }

    void ReportChunkIn(ui64 rows, ui64 bytes) {
        if (PushStats.CollectBasic()) {
            PushStats.Bytes += bytes;
            PushStats.Rows += rows;
            PushStats.Chunks++;
            PushStats.Resume();
        }

        auto fillLevel = UpdateFillLevel();

        if (PopStats.CollectFull()) {
            if (fillLevel != NoLimit) {
                PopStats.TryPause();
            }
            PopStats.MaxMemoryUsage = std::max(PopStats.MaxMemoryUsage, EstimatedStoredBytes);
            PopStats.MaxRowsInMemory = std::max(PopStats.MaxRowsInMemory, Values.size());
        }
    }

    void ReportChunkOut(ui64 rows, ui64 bytes) {
        auto fillLevel = UpdateFillLevel();

        if (PopStats.CollectBasic()) {
            PopStats.Bytes += bytes;
            PopStats.Rows += rows;
            PopStats.Chunks++;
            if (fillLevel == NoLimit) {
                PopStats.Resume();
            }
        }
    }

    static bool IsBlockType(const NKikimr::NMiniKQL::TType* type) {
        if (!type->IsMulti()) {
            return false;
        }

        const NKikimr::NMiniKQL::TMultiType* multiType = static_cast<const NKikimr::NMiniKQL::TMultiType*>(type);
        const ui32 width = multiType->GetElementsCount();
        if (!width) {
            return false;
        }

        for (ui32 i = 0; i < width; i++) {
            if (!multiType->GetElementType(i)->IsBlock()) {
                return false;
            }
        }

        const auto lengthType = static_cast<const NKikimr::NMiniKQL::TBlockType*>(multiType->GetElementType(width - 1));
        return lengthType->GetShape() == NKikimr::NMiniKQL::TBlockType::EShape::Scalar
            && lengthType->GetItemType()->IsData()
            && static_cast<const NKikimr::NMiniKQL::TDataType*>(lengthType->GetItemType())->GetDataSlot() == NUdf::EDataSlot::Uint64;
    }

private:
    const ui64 MaxStoredBytes;
    NKikimr::NMiniKQL::TType* const OutputType;
    const bool IsBlock = false;
    ui64 EstimatedStoredBytes = 0;
    ui64 ValuesPushed = 0;
    bool Finished = false;
    std::deque<TValueDesc> Values;
    ui64 EstimatedRowBytes = 0;
    std::shared_ptr<TDqFillAggregator> Aggregator;
    EDqFillLevel FillLevel = NoLimit;
};

} // namespace

IDqAsyncOutputBuffer::TPtr CreateDqAsyncOutputBuffer(ui64 outputIndex, const TString& type, NKikimr::NMiniKQL::TType* outputType, ui64 maxStoredBytes,
    TCollectStatsLevel level)
{
    return MakeIntrusive<TDqAsyncOutputBuffer>(outputIndex, type, outputType, maxStoredBytes, level);
}

} // namespace NYql::NDq
