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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

template<bool FastPack>
class TDqOutputChannel : public IDqOutputChannel {
public:
    TDqOutputChannel(ui64 channelId, NMiniKQL::TType* outputType, const NMiniKQL::TTypeEnvironment& typeEnv,
        const NMiniKQL::THolderFactory& holderFactory, const TDqOutputChannelSettings& settings, const TLogFunc& logFunc)
        : ChannelId(channelId)
        , OutputType(outputType)
        , BasicStats(ChannelId)
        , ProfileStats(settings.CollectProfileStats ? &BasicStats : nullptr)
        , Packer(false, NMiniKQL::TListType::Create(OutputType, typeEnv))
        , Storage(settings.ChannelStorage)
        , HolderFactory(holderFactory)
        , TransportVersion(settings.TransportVersion)
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

    void Push(NUdf::TUnboxedValue&& value) override {
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

        Packer.AddItem(value);
        value = {};
        ChunkRowCount++;
        BasicStats.RowsIn++;

        size_t packerSize = Packer.PackedSizeEstimate();
        if (packerSize >= MaxChunkBytes) {
            Data.emplace_back();
            Data.back().Buffer = Packer.FinishAndPull();
            BasicStats.Bytes += Data.back().Buffer.Size();
            PackedDataSize += Data.back().Buffer.Size();
            PackedRowCount += ChunkRowCount;
            Data.back().RowCount = ChunkRowCount;
            ChunkRowCount = 0;
            packerSize = 0;
        }

        while (Storage && PackedDataSize && PackedDataSize + packerSize > MaxStoredBytes) {
            auto& head = Data.front();
            size_t bufSize = head.Buffer.Size();
            YQL_ENSURE(PackedDataSize >= bufSize);

            TBuffer blob;
            blob.Reserve(bufSize + sizeof(head.RowCount));
            blob.Append((const char*)&head.RowCount, sizeof(head.RowCount));
            head.Buffer.ForEachPage([&blob](const char *data, size_t len) {
                blob.Append(data, len);
            });

            YQL_ENSURE(blob.Size() == bufSize + sizeof(head.RowCount));
            Storage->Put(NextStoredId++, std::move(blob));

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
    bool Pop(NDqProto::TData& data) override {
        LOG("Pop request, rows in memory: " << GetValuesCount() << ", finished: " << Finished);

        if (!HasData()) {
            if (Finished) {
                data.SetTransportVersion(TransportVersion);
                data.SetRows(0);
                data.ClearRaw();
            }
            return false;
        }

        data.SetTransportVersion(TransportVersion);
        data.ClearRaw();
        if (FirstStoredId < NextStoredId) {
            YQL_ENSURE(Storage);
            LOG("Loading spilled blob. BlobId: " << FirstStoredId);
            TBuffer blob;
            if (!Storage->Get(FirstStoredId, blob)) {
                LOG("BlobId " << FirstStoredId << " not ready yet");
                return false;
            }
            ++FirstStoredId;
            ui64 rows;
            YQL_ENSURE(blob.size() >= sizeof(rows));
            std::memcpy((char*)&rows, blob.data(), sizeof(rows));
            data.SetRows(rows);
            data.MutableRaw()->insert(data.MutableRaw()->end(), blob.data() + sizeof(rows), blob.data() + blob.size());
            SpilledRowCount -= rows;
        } else if (!Data.empty()) {
            auto& packed = Data.front();
            data.SetRows(packed.RowCount);
            data.MutableRaw()->reserve(packed.Buffer.Size());
            packed.Buffer.CopyTo(*data.MutableRaw());
            PackedRowCount -= packed.RowCount;
            PackedDataSize -= packed.Buffer.Size();
            Data.pop_front();
        } else {
            data.SetRows(ChunkRowCount);
            auto buffer = Packer.FinishAndPull();
            data.MutableRaw()->reserve(buffer.Size());
            buffer.CopyTo(*data.MutableRaw());
            ChunkRowCount = 0;
        }

        DLOG("Took " << data.GetRows() << " rows");

        BasicStats.Chunks++;
        BasicStats.RowsOut += data.GetRows();
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
    bool PopAll(NDqProto::TData& data) override {
        if (!HasData()) {
            if (Finished) {
                data.SetTransportVersion(TransportVersion);
                data.SetRows(0);
                data.ClearRaw();
            }
            return false;
        }

        data.SetTransportVersion(TransportVersion);
        data.ClearRaw();
        if (SpilledRowCount == 0 && PackedRowCount == 0) {
            data.SetRows(ChunkRowCount);
            auto buffer = Packer.FinishAndPull();
            data.MutableRaw()->reserve(buffer.Size());
            buffer.CopyTo(*data.MutableRaw());
            ChunkRowCount = 0;
            return true;
        }

        // Repack all - thats why PopAll should never be used
        if (ChunkRowCount) {
            Data.emplace_back();
            Data.back().Buffer = Packer.FinishAndPull();
            BasicStats.Bytes += Data.back().Buffer.Size();
            PackedDataSize += Data.back().Buffer.Size();
            PackedRowCount += ChunkRowCount;
            Data.back().RowCount = ChunkRowCount;
            ChunkRowCount = 0;
        }

        NKikimr::NMiniKQL::TUnboxedValueVector rows;
        for (;;) {
            NDqProto::TData chunk;
            if (!this->Pop(chunk)) {
                break;
            }
            TStringBuf buf(chunk.GetRaw());
            Packer.UnpackBatch(buf, HolderFactory, rows);
        }

        for (auto& row : rows) {
            Packer.AddItem(row);
        }

        data.SetRows(rows.size());
        auto buffer = Packer.FinishAndPull();
        data.MutableRaw()->reserve(buffer.Size());
        buffer.CopyTo(*data.MutableRaw());
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
    const IDqChannelStorage::TPtr Storage;
    const NMiniKQL::THolderFactory& HolderFactory;
    const NDqProto::EDataTransportVersion TransportVersion;
    const ui64 MaxStoredBytes;
    const ui64 MaxChunkBytes;
    const ui64 ChunkSizeLimit;
    TLogFunc LogFunc;

    struct TSerializedBatch {
        NKikimr::NMiniKQL::TPagedBuffer Buffer;
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
    const NKikimr::NMiniKQL::TTypeEnvironment& typeEnv, const NKikimr::NMiniKQL::THolderFactory& holderFactory,
    const TDqOutputChannelSettings& settings, const TLogFunc& logFunc)
{
    if (settings.TransportVersion == NDqProto::EDataTransportVersion::DATA_TRANSPORT_UV_PICKLE_1_0 ||
        settings.TransportVersion == NDqProto::EDataTransportVersion::DATA_TRANSPORT_VERSION_UNSPECIFIED)
    {
        return new TDqOutputChannel<false>(channelId, outputType, typeEnv, holderFactory, settings, logFunc);
    } else {
        YQL_ENSURE(settings.TransportVersion == NDqProto::EDataTransportVersion::DATA_TRANSPORT_UV_FAST_PICKLE_1_0,
            "Unsupported transport version " << (ui32)settings.TransportVersion);
        return new TDqOutputChannel<true>(channelId, outputType, typeEnv, holderFactory, settings, logFunc);
    }
}

} // namespace NYql::NDq
