#include "dq_sink.h"
#include "dq_transport.h"

#include <ydb/library/yql/utils/yql_panic.h> 

#include <deque>
#include <variant>

namespace NYql::NDq {
namespace {

class TDqSink : public IDqSink {
    struct TValueDesc {
        std::variant<NUdf::TUnboxedValue, NDqProto::TCheckpoint> Value;
        ui64 EstimatedSize;

        TValueDesc(NUdf::TUnboxedValue&& value, ui64 size)
            : Value(std::move(value))
            , EstimatedSize(size)
        {
        }

        TValueDesc(NDqProto::TCheckpoint&& checkpoint, ui64 size)
            : Value(std::move(checkpoint))
            , EstimatedSize(size)
        {
        }

        TValueDesc(const TValueDesc&) = default;
        TValueDesc(TValueDesc&&) = default;
    };

public:
    TDqSink(ui64 outputIndex, NKikimr::NMiniKQL::TType* outputType, ui64 maxStoredBytes, bool collectProfileStats)
        : OutputIndex(outputIndex)
        , MaxStoredBytes(maxStoredBytes)
        , OutputType(outputType)
        , BasicStats(OutputIndex)
        , ProfileStats(collectProfileStats ? &BasicStats : nullptr) {}

    ui64 GetOutputIndex() const override {
        return OutputIndex;
    }

    bool IsFull() const override {
        return EstimatedStoredBytes >= MaxStoredBytes;
    }

    void Push(NUdf::TUnboxedValue&& value) override {
        if (!BasicStats.FirstRowIn) {
            BasicStats.FirstRowIn = TInstant::Now();
        }

        if (ValuesPushed++ % 1000 == 0) {
            ReestimateRowBytes(value);
        }
        Y_VERIFY(EstimatedRowBytes > 0);
        Values.emplace_back(std::move(value), EstimatedRowBytes);
        EstimatedStoredBytes += EstimatedRowBytes;

        ReportChunkIn();
    }

    void Push(NDqProto::TCheckpoint&& checkpoint) override {
        const ui64 bytesSize = checkpoint.ByteSize();
        Values.emplace_back(std::move(checkpoint), bytesSize);
        EstimatedStoredBytes += bytesSize;

        ReportChunkIn();
    }

    void Finish() override {
        Finished = true;

        if (!BasicStats.FirstRowIn) {
            BasicStats.FirstRowIn = TInstant::Now();
        }
    }

    ui64 Pop(NKikimr::NMiniKQL::TUnboxedValueVector& batch, ui64 bytes) override {
        batch.clear();
        ui64 valuesCount = 0;
        ui64 usedBytes = 0;

        // Calc values count.
        for (auto iter = Values.cbegin(), end = Values.cend();
            usedBytes < bytes && iter != end && std::holds_alternative<NUdf::TUnboxedValue>(iter->Value);
            ++iter)
        {
            ++valuesCount;
            usedBytes += iter->EstimatedSize;
        }

        // Reserve size and return data.
        batch.reserve(valuesCount);
        while (valuesCount--) {
            batch.emplace_back(std::move(std::get<NUdf::TUnboxedValue>(Values.front().Value)));
            Values.pop_front();
        }
        Y_VERIFY(EstimatedStoredBytes >= usedBytes);
        EstimatedStoredBytes -= usedBytes;

        ReportChunkOut(batch.size(), usedBytes);

        return usedBytes;
    }

    bool Pop(NDqProto::TCheckpoint& checkpoint) override {
        if (!Values.empty() && std::holds_alternative<NDqProto::TCheckpoint>(Values.front().Value)) {
            checkpoint = std::move(std::get<NDqProto::TCheckpoint>(Values.front().Value));
            const auto size = Values.front().EstimatedSize;
            Y_VERIFY(EstimatedStoredBytes >= size);
            EstimatedStoredBytes -= size;
            Values.pop_front();

            ReportChunkOut(1, size);

            return true;
        }
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
            if (std::holds_alternative<NUdf::TUnboxedValue>(v.Value)) {
                return false;
            }
        }
        // Finished and no data values.
        return true;
    }

    NKikimr::NMiniKQL::TType* GetOutputType() const override {
        return OutputType;
    }

    const TDqSinkStats* GetStats() const override {
        return &BasicStats;
    }

private:
    void ReestimateRowBytes(const NUdf::TUnboxedValue& value) {
        const ui64 valueSize = TDqDataSerializer::EstimateSize(value, OutputType);
        if (EstimatedRowBytes) {
            EstimatedRowBytes = static_cast<ui64>(0.6 * valueSize + 0.4 * EstimatedRowBytes);
        } else {
            EstimatedRowBytes = valueSize;
        }
        if (!EstimatedRowBytes) {
            EstimatedRowBytes = 1;
        }
    }

    void ReportChunkIn() {
        BasicStats.Bytes += EstimatedRowBytes;
        BasicStats.RowsIn++;
        if (ProfileStats) {
            ProfileStats->MaxMemoryUsage = std::max(ProfileStats->MaxMemoryUsage, EstimatedStoredBytes);
            ProfileStats->MaxRowsInMemory = std::max(ProfileStats->MaxRowsInMemory, Values.size());
        }
    }

    void ReportChunkOut(ui64 rowsCount, ui64 /* usedBytes */) {
        BasicStats.Chunks++;
        BasicStats.RowsOut += rowsCount;
    }

private:
    const ui64 OutputIndex;
    const ui64 MaxStoredBytes;
    NKikimr::NMiniKQL::TType* const OutputType;
    ui64 EstimatedStoredBytes = 0;
    ui64 ValuesPushed = 0;
    bool Finished = false;
    std::deque<TValueDesc> Values;
    ui64 EstimatedRowBytes = 0;
    TDqSinkStats BasicStats;
    TDqSinkStats* ProfileStats = nullptr;
};

} // namespace

IDqSink::TPtr CreateDqSink(ui64 outputIndex, NKikimr::NMiniKQL::TType* outputType, ui64 maxStoredBytes,
    bool collectProfileStats)
{
    return MakeIntrusive<TDqSink>(outputIndex, outputType, maxStoredBytes, collectProfileStats);
}

} // namespace NYql::NDq
