#include "dq_output_channel.h"
#include "dq_transport.h"

#include <ydb/library/yql/utils/yql_panic.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_pack.h>

#include <util/generic/buffer.h>
#include <util/generic/size_literals.h>
#include <util/stream/buffer.h>


namespace NYql::NDq {

#define LOG(s) do { if (Y_UNLIKELY(LogFunc)) { LogFunc(TStringBuilder() << "channelId: " << ChannelId << ". " << s); } } while (0)

#ifndef NDEBUG
#define DLOG(s) LOG(s)
#else
#define DLOG(s)
#endif

namespace {

class TProfileGuard {
public:
    TProfileGuard(TDuration* duration)
        : Duration(duration)
    {
        if (Y_UNLIKELY(duration)) {
            Start = TInstant::Now();
        }
    }

    ~TProfileGuard() {
        if (Y_UNLIKELY(Duration)) {
            *Duration += TInstant::Now() - Start;
        }
    }
private:
    TInstant Start;
    TDuration* Duration;
};

using namespace NKikimr;

using NKikimr::NMiniKQL::TPagedBuffer;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

template<bool FastPack>
class TDqOutputChannel : public IDqOutputChannel {
public:
    TDqOutputChannel(ui64 channelId, NMiniKQL::TType* outputType,
        const NMiniKQL::THolderFactory& holderFactory, const TDqOutputChannelSettings& settings, const TLogFunc& logFunc,
        NDqProto::EDataTransportVersion transportVersion)
        : ChannelId(channelId)
        , OutputType(outputType)
        , BasicStats(ChannelId)
        , ProfileStats(settings.CollectProfileStats ? &BasicStats : nullptr)
        , Packer(OutputType)
        , Width(OutputType->IsMulti() ? static_cast<NMiniKQL::TMultiType*>(OutputType)->GetElementsCount() : 1u)
        , Storage(settings.ChannelStorage)
        , HolderFactory(holderFactory)
        , TransportVersion(transportVersion)
        , MaxStoredBytes(settings.MaxStoredBytes)
        , MaxChunkBytes(settings.MaxChunkBytes)
        , ChunkSizeLimit(settings.ChunkSizeLimit)
        , LogFunc(logFunc)
    {
    }

    ui64 GetChannelId() const override {
        return ChannelId;
    }

    ui64 GetValuesCount() const override {
        return SpilledRowCount + PackedRowCount + ChunkRowCount;
    }

    [[nodiscard]]
    bool IsFull() const override {
        if (!Storage) {
            return PackedDataSize + Packer.PackedSizeEstimate() >= MaxStoredBytes;
        }
        return Storage->IsFull();
    }

    virtual void Push(NUdf::TUnboxedValue&& value) override {
        YQL_ENSURE(!OutputType->IsMulti());
        DoPush(&value, 1);
    }

    virtual void WidePush(NUdf::TUnboxedValue* values, ui32 width) override {
        YQL_ENSURE(OutputType->IsMulti());
        YQL_ENSURE(Width == width);
        DoPush(values, width);
    }

    void DoPush(NUdf::TUnboxedValue* values, ui32 width) {
        TProfileGuard guard(ProfileStats ? &ProfileStats->SerializationTime : nullptr);
        if (!BasicStats.FirstRowIn) {
            BasicStats.FirstRowIn = TInstant::Now();
        }

        ui64 rowsInMemory = PackedRowCount + ChunkRowCount;

        LOG("Push request, rows in memory: " << rowsInMemory << ", bytesInMemory: " << (PackedDataSize + Packer.PackedSizeEstimate())
            << ", finished: " << Finished);
        YQL_ENSURE(!IsFull());

        if (Finished) {
            return;
        }

        if (OutputType->IsMulti()) {
            Packer.AddWideItem(values, width);
        } else {
            Packer.AddItem(*values);
        }
        for (ui32 i = 0; i < width; ++i) {
            values[i] = {};
        }

        ChunkRowCount++;
        BasicStats.RowsIn++;

        size_t packerSize = Packer.PackedSizeEstimate();
        if (packerSize >= MaxChunkBytes) {
            Data.emplace_back();
            Data.back().Buffer = FinishPackAndCheckSize();
            BasicStats.Bytes += Data.back().Buffer.size();
            PackedDataSize += Data.back().Buffer.size();
            PackedRowCount += ChunkRowCount;
            Data.back().RowCount = ChunkRowCount;
            ChunkRowCount = 0;
            packerSize = 0;
        }

        while (Storage && PackedDataSize && PackedDataSize + packerSize > MaxStoredBytes) {
            auto& head = Data.front();
            size_t bufSize = head.Buffer.size();
            YQL_ENSURE(PackedDataSize >= bufSize);

            TDqSerializedBatch data;
            data.Proto.SetTransportVersion(TransportVersion);
            data.Proto.SetRows(head.RowCount);
            data.SetPayload(std::move(head.Buffer));
            Storage->Put(NextStoredId++, SaveForSpilling(std::move(data)));

            PackedDataSize -= bufSize;
            PackedRowCount -= head.RowCount;

            SpilledRowCount += head.RowCount;

            if (Y_UNLIKELY(ProfileStats)) {
                ProfileStats->SpilledRows += head.RowCount;
                ProfileStats->SpilledBytes += bufSize + sizeof(head.RowCount);
                ProfileStats->SpilledBlobs++;
            }

            Data.pop_front();
        }

        if (Y_UNLIKELY(ProfileStats)) {
            ProfileStats->MaxMemoryUsage = std::max(ProfileStats->MaxMemoryUsage, PackedDataSize + packerSize);
            ProfileStats->MaxRowsInMemory = std::max(ProfileStats->MaxRowsInMemory, PackedRowCount);
        }
    }

    void Push(NDqProto::TWatermark&& watermark) override {
        YQL_ENSURE(!Watermark);
        Watermark.ConstructInPlace(std::move(watermark));
    }

    void Push(NDqProto::TCheckpoint&& checkpoint) override {
        YQL_ENSURE(!Checkpoint);
        Checkpoint.ConstructInPlace(std::move(checkpoint));
    }

    [[nodiscard]]
    bool Pop(TDqSerializedBatch& data) override {
        LOG("Pop request, rows in memory: " << GetValuesCount() << ", finished: " << Finished);

        if (!HasData()) {
            if (Finished) {
                data.Clear();
                data.Proto.SetTransportVersion(TransportVersion);
            }
            return false;
        }

        data.Clear();
        data.Proto.SetTransportVersion(TransportVersion);
        if (FirstStoredId < NextStoredId) {
            YQL_ENSURE(Storage);
            LOG("Loading spilled blob. BlobId: " << FirstStoredId);
            TBuffer blob;
            if (!Storage->Get(FirstStoredId, blob)) {
                LOG("BlobId " << FirstStoredId << " not ready yet");
                return false;
            }
            ++FirstStoredId;
            data = LoadSpilled(std::move(blob));
            SpilledRowCount -= data.RowCount();
        } else if (!Data.empty()) {
            auto& packed = Data.front();
            PackedRowCount -= packed.RowCount;
            PackedDataSize -= packed.Buffer.size();
            data.Proto.SetRows(packed.RowCount);
            data.SetPayload(std::move(packed.Buffer));
            Data.pop_front();
        } else {
            data.Proto.SetRows(ChunkRowCount);
            data.SetPayload(FinishPackAndCheckSize());
            ChunkRowCount = 0;
        }

        DLOG("Took " << data.RowCount() << " rows");

        BasicStats.Chunks++;
        BasicStats.RowsOut += data.RowCount();
        return true;
    }

    [[nodiscard]]
    bool Pop(NDqProto::TWatermark& watermark) override {
        if (!HasData() && Watermark) {
            watermark = std::move(*Watermark);
            Watermark = Nothing();
            return true;
        }
        return false;
    }

    [[nodiscard]]
    bool Pop(NDqProto::TCheckpoint& checkpoint) override {
        if (!HasData() && Checkpoint) {
            checkpoint = std::move(*Checkpoint);
            Checkpoint = Nothing();
            return true;
        }
        return false;
    }

    [[nodiscard]]
    bool PopAll(TDqSerializedBatch& data) override {
        if (!HasData()) {
            if (Finished) {
                data.Clear();
                data.Proto.SetTransportVersion(TransportVersion);
            }
            return false;
        }

        data.Clear();
        data.Proto.SetTransportVersion(TransportVersion);
        if (SpilledRowCount == 0 && PackedRowCount == 0) {
            data.Proto.SetRows(ChunkRowCount);
            data.SetPayload(FinishPackAndCheckSize());
            ChunkRowCount = 0;
            return true;
        }

        // Repack all - thats why PopAll should never be used
        if (ChunkRowCount) {
            Data.emplace_back();
            Data.back().Buffer = FinishPackAndCheckSize();
            BasicStats.Bytes += Data.back().Buffer.size();
            PackedDataSize += Data.back().Buffer.size();
            PackedRowCount += ChunkRowCount;
            Data.back().RowCount = ChunkRowCount;
            ChunkRowCount = 0;
        }

        NKikimr::NMiniKQL::TUnboxedValueBatch rows(OutputType);
        for (;;) {
            TDqSerializedBatch chunk;
            if (!this->Pop(chunk)) {
                break;
            }
            Packer.UnpackBatch(chunk.PullPayload(), HolderFactory, rows);
        }

        if (OutputType->IsMulti()) {
            rows.ForEachRowWide([this](const auto* values, ui32 width) {
                Packer.AddWideItem(values, width);
            });
        } else {
            rows.ForEachRow([this](const auto& value) {
                Packer.AddItem(value);
            });
        }

        data.Proto.SetRows(rows.RowCount());
        data.SetPayload(FinishPackAndCheckSize());
        YQL_ENSURE(!HasData());
        return true;
    }

    void Finish() override {
        LOG("Finish request");
        Finished = true;

        if (!BasicStats.FirstRowIn) {
            BasicStats.FirstRowIn = TInstant::Now();
        }
    }

    TRope FinishPackAndCheckSize() {
        TRope result = Packer.Finish();
        if (result.size() > ChunkSizeLimit) {
            // TODO: may relax requirement if OOB transport is enabled
            ythrow TDqOutputChannelChunkSizeLimitExceeded() << "Row data size is too big: "
                << result.size() << " bytes, exceeds limit of " << ChunkSizeLimit << " bytes";
        }
        return result;
    }

    bool HasData() const override {
        return GetValuesCount() != 0;
    }

    bool IsFinished() const override {
        return Finished && !HasData();
    }

    ui64 Drop() override { // Drop channel data because channel was finished. Leave checkpoint because checkpoints keep going through channel after finishing channel data transfer.
        ui64 rows = GetValuesCount();
        Data.clear();
        Packer.Clear();
        SpilledRowCount = PackedDataSize = PackedRowCount = ChunkRowCount = 0;
        FirstStoredId = NextStoredId;
        return rows;
    }

    NKikimr::NMiniKQL::TType* GetOutputType() const override {
        return OutputType;
    }

    const TDqOutputChannelStats* GetStats() const override {
        return &BasicStats;
    }

    void Terminate() override {
    }

private:
    const ui64 ChannelId;
    NKikimr::NMiniKQL::TType* OutputType;
    TDqOutputChannelStats BasicStats;
    TDqOutputChannelStats* ProfileStats = nullptr;
    NKikimr::NMiniKQL::TValuePackerTransport<FastPack> Packer;
    const ui32 Width;
    const IDqChannelStorage::TPtr Storage;
    const NMiniKQL::THolderFactory& HolderFactory;
    const NDqProto::EDataTransportVersion TransportVersion;
    const ui64 MaxStoredBytes;
    const ui64 MaxChunkBytes;
    const ui64 ChunkSizeLimit;
    TLogFunc LogFunc;

    struct TSerializedBatch {
        TRope Buffer;
        ui64 RowCount = 0;
    };
    std::deque<TSerializedBatch> Data;

    size_t SpilledRowCount = 0;
    ui64 FirstStoredId = 0;
    ui64 NextStoredId = 0;

    size_t PackedDataSize = 0;
    size_t PackedRowCount = 0;
    
    size_t ChunkRowCount = 0;

    bool Finished = false;

    TMaybe<NDqProto::TWatermark> Watermark;
    TMaybe<NDqProto::TCheckpoint> Checkpoint;
};

} // anonymous namespace


IDqOutputChannel::TPtr CreateDqOutputChannel(ui64 channelId, NKikimr::NMiniKQL::TType* outputType,
    const NKikimr::NMiniKQL::THolderFactory& holderFactory,
    const TDqOutputChannelSettings& settings, const TLogFunc& logFunc)
{
    auto transportVersion = settings.TransportVersion;
    switch(transportVersion) {
        case NDqProto::EDataTransportVersion::DATA_TRANSPORT_VERSION_UNSPECIFIED:
            transportVersion = NDqProto::EDataTransportVersion::DATA_TRANSPORT_UV_PICKLE_1_0;
            [[fallthrough]];
        case NDqProto::EDataTransportVersion::DATA_TRANSPORT_UV_PICKLE_1_0:
        case NDqProto::EDataTransportVersion::DATA_TRANSPORT_OOB_PICKLE_1_0:
            return new TDqOutputChannel<false>(channelId, outputType, holderFactory, settings, logFunc, transportVersion);
        case NDqProto::EDataTransportVersion::DATA_TRANSPORT_UV_FAST_PICKLE_1_0:
        case NDqProto::EDataTransportVersion::DATA_TRANSPORT_OOB_FAST_PICKLE_1_0:
            return new TDqOutputChannel<true>(channelId, outputType, holderFactory, settings, logFunc, transportVersion);
        default:
            YQL_ENSURE(false, "Unsupported transport version " << (ui32)transportVersion);
    }
}

} // namespace NYql::NDq
