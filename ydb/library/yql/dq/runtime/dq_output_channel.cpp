#include "dq_output_channel.h"
#include "dq_transport.h"

#include <ydb/library/yql/utils/yql_panic.h>

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

using namespace NKikimr;

bool IsSafeToEstimateValueSize(const NMiniKQL::TType* type) {
    if (type->GetKind() == NMiniKQL::TType::EKind::Data) {
        return true;
    }
    if (type->GetKind() == NMiniKQL::TType::EKind::Optional) {
        const auto* optionalType = static_cast<const NMiniKQL::TOptionalType*>(type);
        return IsSafeToEstimateValueSize(optionalType->GetItemType());
    }
    if (type->GetKind() == NMiniKQL::TType::EKind::Struct) {
        const auto* structType = static_cast<const NMiniKQL::TStructType*>(type);
        for (ui32 i = 0; i < structType->GetMembersCount(); ++i) {
            if (!IsSafeToEstimateValueSize(structType->GetMemberType(i))) {
                return false;
            }
        }
        return true;
    }
    if (type->GetKind() == NMiniKQL::TType::EKind::Tuple) {
        const auto* tupleType = static_cast<const NMiniKQL::TTupleType*>(type);
        for (ui32 i = 0; i < tupleType->GetElementsCount(); ++i) {
            if (!IsSafeToEstimateValueSize(tupleType->GetElementType(i))) {
                return false;
            }
        }
        return true;
    }
    if (type->GetKind() == NMiniKQL::TType::EKind::Variant) {
        const auto* variantType = static_cast<const NMiniKQL::TVariantType*>(type);
        for (ui32 i = 0; i < variantType->GetAlternativesCount(); ++i) {
            if (!IsSafeToEstimateValueSize(variantType->GetAlternativeType(i))) {
                return false;
            }
        }
        return true;
    }
    return false;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

class TDqOutputChannelOld : public IDqOutputChannel {
public:
    TDqOutputChannelOld(ui64 channelId, NMiniKQL::TType* outputType, bool collectProfileStats,
        const NMiniKQL::TTypeEnvironment& typeEnv, const NMiniKQL::THolderFactory& holderFactory, ui64 maxStoredBytes,
        ui64 maxChunkBytes, ui64 chunkSizeLimit, NDqProto::EDataTransportVersion transportVersion, const TLogFunc& logFunc)
        : ChannelId(channelId)
        , OutputType(outputType)
        , BasicStats(ChannelId)
        , ProfileStats(collectProfileStats ? &BasicStats : nullptr)
        , DataSerializer(typeEnv, holderFactory, transportVersion)
        , MaxStoredBytes(maxStoredBytes)
        , MaxChunkBytes(maxChunkBytes)
        , ChunkSizeLimit(chunkSizeLimit)
        , LogFunc(logFunc) {}

    ui64 GetChannelId() const override {
        return ChannelId;
    }

    ui64 GetValuesCount(bool /* inMemoryOnly */) const override {
        return Data.size();
    }

    [[nodiscard]]
    bool IsFull() const override {
        return Data.size() * EstimatedRowBytes >= MaxStoredBytes;
    }

    void Push(NUdf::TUnboxedValue&& value) override {
        if (!BasicStats.FirstRowIn) {
            BasicStats.FirstRowIn = TInstant::Now();
        }

        ui64 rowsInMemory = Data.size();

        LOG("Push request, rows in memory: " << rowsInMemory << ", bytesInMemory: " << (rowsInMemory * EstimatedRowBytes)
            << ", finished: " << Finished);
        YQL_ENSURE(!IsFull());

        if (Finished) {
            return;
        }

        if (!EstimatedRowBytes) {
            EstimatedRowBytes = IsSafeToEstimateValueSize(OutputType)
                ? TDqDataSerializer::EstimateSize(value, OutputType)
                : DataSerializer.CalcSerializedSize(value, OutputType);
            LOG("EstimatedRowSize: " << EstimatedRowBytes << ", type: " << *OutputType
                << ", safe: " << IsSafeToEstimateValueSize(OutputType));
        }

        Data.emplace_back(std::move(value));
        rowsInMemory++;

        BasicStats.Bytes += EstimatedRowBytes;
        BasicStats.RowsIn++;

        if (Y_UNLIKELY(ProfileStats)) {
            ProfileStats->MaxMemoryUsage = std::max(ProfileStats->MaxMemoryUsage, rowsInMemory * EstimatedRowBytes);
            ProfileStats->MaxRowsInMemory = std::max(ProfileStats->MaxRowsInMemory, rowsInMemory);
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
    bool Pop(NDqProto::TData& data, ui64 bytes) override {
        LOG("Pop request, rows in memory: " << Data.size() << ", finished: " << Finished << ", requested: " << bytes
            << ", estimatedRowSize: " << EstimatedRowBytes);

        if (!HasData()) {
            if (Finished) {
                data.SetTransportVersion(DataSerializer.GetTransportVersion());
                data.SetRows(0);
                data.ClearRaw();
            }
            return false;
        }

        bytes = std::min(bytes, MaxChunkBytes);
        auto last = Data.begin();
        if (Y_UNLIKELY(ProfileStats)) {
            TInstant startTime = TInstant::Now();
            data = DataSerializer.Serialize(last, Data.end(), OutputType, bytes);
            ProfileStats->SerializationTime += (TInstant::Now() - startTime);
        } else {
            data = DataSerializer.Serialize(last, Data.end(), OutputType, bytes);
        }


        ui64 takeRows = data.GetRows();
        DLOG("Took " << takeRows << " rows");

        if (EstimatedRowBytes) {
            EstimatedRowBytes = EstimatedRowBytes * 0.6 + std::max<ui64>(data.GetRaw().Size() / takeRows, 1) * 0.4;
            DLOG("Recalc estimated row size: " << EstimatedRowBytes);
        }

        YQL_ENSURE(data.GetRaw().size() < ChunkSizeLimit);

        BasicStats.Chunks++;
        BasicStats.RowsOut += takeRows;

        Data.erase(Data.begin(), last);
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
        if (Data.empty()) {
            if (Finished) {
                data.SetTransportVersion(DataSerializer.GetTransportVersion());
                data.SetRows(0);
                data.ClearRaw();
            }
            return false;
        }

        if (Y_UNLIKELY(ProfileStats)) {
            TInstant startTime = TInstant::Now();
            data = DataSerializer.Serialize(Data.begin(), Data.end(), OutputType);
            ProfileStats->SerializationTime += (TInstant::Now() - startTime);
        } else {
            data = DataSerializer.Serialize(Data.begin(), Data.end(), OutputType);
        }

        BasicStats.Chunks++;
        BasicStats.RowsOut += data.GetRows();

        Data.clear();
        return true;
    }

    bool PopAll(NKikimr::NMiniKQL::TUnboxedValueVector& data) override {
        if (Data.empty()) {
            return false;
        }

        data.reserve(data.size() + Data.size());
        for (auto&& v : Data) {
            data.emplace_back(std::move(v));
        }

        BasicStats.Chunks++;
        BasicStats.RowsOut += data.size();

        Data.clear();
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
        return !Data.empty();
    }

    bool IsFinished() const override {
        return Finished && !HasData();
    }

    ui64 Drop() override { // Drop channel data because channel was finished. Leave checkpoint because checkpoints keep going through channel after finishing channel data transfer.
        ui64 rows = Data.size();
        Data.clear();
        TDataType().swap(Data);
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
    TDqDataSerializer DataSerializer;
    const ui64 MaxStoredBytes;
    ui64 MaxChunkBytes;
    ui64 ChunkSizeLimit;
    TLogFunc LogFunc;

    using TDataType = TDeque<NUdf::TUnboxedValue, NKikimr::NMiniKQL::TMKQLAllocator<NUdf::TUnboxedValue>>;
    TDataType Data;

    ui32 EstimatedRowBytes = 0;
    bool Finished = false;

    TMaybe<NDqProto::TWatermark> Watermark;
    TMaybe<NDqProto::TCheckpoint> Checkpoint;
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

class TDqOutputChannelNew : public IDqOutputChannel {
    struct TSpilledBlob;

public:
    TDqOutputChannelNew(ui64 channelId, NMiniKQL::TType* outputType, const NMiniKQL::TTypeEnvironment& typeEnv,
        const NMiniKQL::THolderFactory& holderFactory, const TDqOutputChannelSettings& settings, const TLogFunc& log)
        : ChannelId(channelId)
        , OutputType(outputType)
        , BasicStats(ChannelId)
        , ProfileStats(settings.CollectProfileStats ? &BasicStats : nullptr)
        , DataSerializer(typeEnv, holderFactory, settings.TransportVersion)
        , Storage(settings.ChannelStorage)
        , MaxStoredBytes(settings.MaxStoredBytes)
        , MaxChunkBytes(settings.MaxChunkBytes)
        , ChunkSizeLimit(settings.ChunkSizeLimit)
        , LogFunc(log)
    {
        if (Storage && 3 * MaxChunkBytes > MaxStoredBytes) {
            MaxChunkBytes = Max(MaxStoredBytes / 3, 1_MB);
        }
    }

    ui64 GetChannelId() const override {
        return ChannelId;
    }

    ui64 GetValuesCount(bool inMemoryOnly) const override {
        if (inMemoryOnly) {
            return DataHead.size() + DataTail.size();
        }
        return DataHead.size() + DataTail.size() + SpilledRows;
    }

    [[nodiscard]]
    bool IsFull() const override {
        if (!Storage) {
            if (MemoryUsed >= MaxStoredBytes) {
                YQL_ENSURE(HasData());
                return true;
            }
            return false;
        }

        return Storage->IsFull();
    }

    void Push(NUdf::TUnboxedValue&& value) override {
        if (!BasicStats.FirstRowIn) {
            BasicStats.FirstRowIn = TInstant::Now();
        }

        ui64 rowsInMemory = DataHead.size() + DataTail.size();

        LOG("Push request, rows in memory: " << rowsInMemory << ", bytesInMemory: " << MemoryUsed
            << ", spilled rows: " << SpilledRows << ", spilled blobs: " << SpilledBlobs.size()
            << ", finished: " << Finished);
        YQL_ENSURE(!IsFull());

        if (Finished) {
            return;
        }

        ui64 estimatedRowSize;

        if (RowFixedSize.has_value()) {
            estimatedRowSize = *RowFixedSize;
        } else {
            bool fixed;
            estimatedRowSize = TDqDataSerializer::EstimateSize(value, OutputType, &fixed);
            if (fixed) {
                RowFixedSize.emplace(estimatedRowSize);
                LOG("Raw has fixed size: " << estimatedRowSize);
            }
        }

        DLOG("Row size: " << estimatedRowSize);

        if (SpilledRows == 0) {
            YQL_ENSURE(DataTail.empty() && SizeTail.empty());
            DataHead.emplace_back(std::move(value));
            SizeHead.emplace_back(estimatedRowSize);
        } else {
            DataTail.emplace_back(std::move(value));
            SizeTail.emplace_back(estimatedRowSize);
        }
        rowsInMemory++;
        MemoryUsed += estimatedRowSize;

        ui64 spilledRows = 0;
        ui64 spilledBytes = 0;
        ui64 spilledBlobs = 0;

        // TODO: add PushAndFinish call
        // if processing is finished we don't have to spill data

        if (MemoryUsed > MaxStoredBytes && Storage) {
            LOG("Not enough memory to store rows in memory only, start spilling, rowsInMemory: " << rowsInMemory
                << ", memoryUsed: " << MemoryUsed << ", MaxStoredBytes: " << MaxStoredBytes);

            TDataType* data = nullptr;
            TDeque<ui64>* size = nullptr;

            ui32 startIndex = std::numeric_limits<ui32>::max();

            if (SpilledRows) {
                // we have spilled rows already, so we can spill all DataTail
                data = &DataTail;
                size = &SizeTail;
                startIndex = 0;
            } else {
                // spill from head
                // but only if we have rows for at least 2 full chunks
                ui32 chunks = 1;
                ui64 chunkSize = 0;
                startIndex = 0;
                for (ui64 i = 0; i < SizeHead.size(); ++i) {
                    chunkSize += SizeHead.at(i);
                    if (chunkSize >= MaxChunkBytes && (i - startIndex) > 0) {
                        // LOG("one more chunk, size: " << chunkSize << ", start next chunk from " << i);
                        chunks++; // this row goes to the next chunk
                        chunkSize = 0;

                        if (chunks >= 3) {
                            data = &DataHead;
                            size = &SizeHead;
                            break;
                        }

                        startIndex = i;
                    }
                }
            }

            // LOG("about to spill from index " << startIndex);

            if (data) {
                while (true) {
                    ui64 chunkSize = 0;
                    ui32 rowsInChunk = 0;
                    ui32 idx = startIndex + spilledRows;

                    while (idx < size->size()) {
                        ui64 rowSize = size->at(idx);
                        if (chunkSize == 0 || chunkSize + rowSize <= MaxChunkBytes) {
                            chunkSize += rowSize;
                            ++rowsInChunk;
                            ++idx;
                        } else {
                            break;
                        }
                    }

                    if (rowsInChunk > 0) {
                        // LOG("about to spill from " << (startIndex + spilledRows) << " to " << (startIndex + spilledRows + rowsInChunk));

                        TDataType::iterator first = std::next(data->begin(), startIndex + spilledRows);
                        TDataType::iterator last = std::next(data->begin(), startIndex + spilledRows + rowsInChunk);

                        NDqProto::TData protoBlob = DataSerializer.Serialize(first, last, OutputType);
                        TBuffer blob;
                        TBufferOutput out{blob};
                        protoBlob.SerializeToArcadiaStream(&out);

                        ui64 blobSize = blob.size();
                        ui64 blobId = NextBlobId++;

                        LOG("Spill blobId: " << blobId << ", size: " << blobSize << ", rows: " << rowsInChunk);
                        Storage->Put(blobId, std::move(blob));

                        SpilledBlobs.emplace_back(TSpilledBlob(blobId, chunkSize, blobSize, rowsInChunk));
                        SpilledRows += rowsInChunk;
                        MemoryUsed -= chunkSize;

                        spilledRows += rowsInChunk;
                        spilledBytes += blobSize;
                        spilledBlobs++;
                    } else {
                        break;
                    }
                }

                if (data == &DataHead) {
                    // move rows after spilled ones to the DataTail
                    YQL_ENSURE(DataTail.empty());
                    for (ui32 i = startIndex + spilledRows; i < data->size(); ++i) {
                        DataTail.emplace_back(std::move(DataHead[i]));
                        SizeTail.emplace_back(SizeHead[i]);
                    }
                    data->erase(std::next(data->begin(), startIndex), data->end());
                    size->erase(std::next(size->begin(), startIndex), size->end());
                } else {
                    YQL_ENSURE(startIndex == 0);
                    data->erase(data->begin(), std::next(data->begin(), spilledRows));
                    size->erase(size->begin(), std::next(size->begin(), spilledRows));
                }
            } else {
                // LOG("nothing to spill");
            }
        }

        BasicStats.Bytes += estimatedRowSize;
        BasicStats.RowsIn++;

        if (Y_UNLIKELY(ProfileStats)) {
            ProfileStats->MaxMemoryUsage = std::max(ProfileStats->MaxMemoryUsage, MemoryUsed);
            ProfileStats->MaxRowsInMemory = std::max(ProfileStats->MaxRowsInMemory, DataHead.size() + DataTail.size());
            if (spilledRows) {
                ProfileStats->SpilledRows += spilledRows;
                ProfileStats->SpilledBytes += spilledBytes;
                ProfileStats->SpilledBlobs += spilledBlobs;
            }
        }

        ValidateUsedMemory();

#if 0
        TStringStream ss;
        ss << "-- DUMP --" << Endl;
        ss << "  Head:" << Endl;
        for (auto& v : DataHead) {
            ss << "    "; v.Dump(ss); ss << Endl;
        }
        ss << "  Spilled: " << SpilledRows << Endl;
        ss << "  Tail:" << Endl;
        for (auto& v : DataTail) {
            ss << "    "; v.Dump(ss); ss << Endl;
        }
        LOG(ss.Str());
#endif
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
    bool Pop(NDqProto::TData& data, ui64 bytes) override {
        LOG("Pop request, rows in memory: " << DataHead.size() << "/" << DataTail.size()
            << ", spilled rows: " << SpilledRows << ", spilled blobs: " << SpilledBlobs.size()
            << ", finished: " << Finished << ", requested: " << bytes << ", maxChunkBytes: " << MaxChunkBytes);

        if (!HasData()) {
            if (Finished) {
                data.SetTransportVersion(DataSerializer.GetTransportVersion());
                data.SetRows(0);
                data.ClearRaw();
            }
            return false;
        }

        bytes = std::min(bytes, MaxChunkBytes);

        if (DataHead.empty()) {
            YQL_ENSURE(SpilledRows);

            TSpilledBlob spilledBlob = SpilledBlobs.front();

            LOG("Loading spilled blob. BlobId: " << spilledBlob.BlobId << ", rows: " << spilledBlob.Rows
                << ", bytes: " << spilledBlob.SerializedSize);
            TBuffer blob;
            if (!Storage->Get(spilledBlob.BlobId, blob)) {
                LOG("BlobId " << spilledBlob.BlobId << " not ready yet");
                // not loaded yet
                return false;
            }

            YQL_ENSURE(blob.size() == spilledBlob.SerializedSize, "" << blob.size() << " != " << spilledBlob.SerializedSize);

            NDqProto::TData protoBlob;
            YQL_ENSURE(protoBlob.ParseFromArray(blob.Data(), blob.size()));
            blob.Reset();

            YQL_ENSURE(protoBlob.GetRows() == spilledBlob.Rows);
            bool hasResult = false;

            // LOG("bytes: " << bytes << ", loaded blob: " << spilledBlob.SerializedSize);

            if (spilledBlob.SerializedSize <= bytes * 2) {
                data.Swap(&protoBlob);
                YQL_ENSURE(data.ByteSizeLong() <= ChunkSizeLimit);
                hasResult = true;
                // LOG("return loaded blob as-is");
            } else {
                NKikimr::NMiniKQL::TUnboxedValueVector buffer;
                DataSerializer.Deserialize(protoBlob, OutputType, buffer);

                for (ui32 i = 0; i < buffer.size(); ++i) {
                    SizeHead.emplace_back(RowFixedSize
                        ? *RowFixedSize
                        : TDqDataSerializer::EstimateSize(buffer[i], OutputType));
                    DataHead.emplace_back(std::move(buffer[i]));

                    MemoryUsed += SizeHead.back();
                }
            }

            SpilledRows -= spilledBlob.Rows;

            SpilledBlobs.pop_front();
            if (SpilledBlobs.empty()) {
                // LOG("no more spilled blobs, move " << DataTail.size() << " tail rows to the head");
                DataHead.insert(DataHead.end(), std::make_move_iterator(DataTail.begin()), std::make_move_iterator(DataTail.end()));
                SizeHead.insert(SizeHead.end(), std::make_move_iterator(SizeTail.begin()), std::make_move_iterator(SizeTail.end()));
                DataTail.clear();
                SizeTail.clear();
            }

            if (hasResult) {
                BasicStats.Chunks++;
                BasicStats.RowsOut += data.GetRows();
                ValidateUsedMemory();
                return true;
            }
        }

        ui32 takeRows = 0;
        ui64 chunkSize = 0;

        while (takeRows == 0 || (takeRows < SizeHead.size() && chunkSize + SizeHead[takeRows] <= bytes)) {
            chunkSize += SizeHead[takeRows];
            ++takeRows;
        }

        DLOG("return rows: " << takeRows << ", size: " << chunkSize);

        auto firstDataIt = DataHead.begin();
        auto lastDataIt = std::next(firstDataIt, takeRows);

        auto firstSizeIt = SizeHead.begin();
        auto lastSizeIt = std::next(firstSizeIt, takeRows);

        if (Y_UNLIKELY(ProfileStats)) {
            TInstant startTime = TInstant::Now();
            data = DataSerializer.Serialize(firstDataIt, lastDataIt, OutputType);
            ProfileStats->SerializationTime += (TInstant::Now() - startTime);
        } else {
            data = DataSerializer.Serialize(firstDataIt, lastDataIt, OutputType);
        }

        YQL_ENSURE(data.ByteSizeLong() <= ChunkSizeLimit);

        BasicStats.Chunks++;
        BasicStats.RowsOut += takeRows;

        DataHead.erase(firstDataIt, lastDataIt);
        SizeHead.erase(firstSizeIt, lastSizeIt);
        MemoryUsed -= chunkSize;

        ValidateUsedMemory();

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
        YQL_ENSURE(!SpilledRows);
        YQL_ENSURE(DataTail.empty());

        if (DataHead.empty()) {
            if (Finished) {
                data.SetTransportVersion(DataSerializer.GetTransportVersion());
                data.SetRows(0);
                data.ClearRaw();
            }
            return false;
        }

        if (Y_UNLIKELY(ProfileStats)) {
            TInstant startTime = TInstant::Now();
            data = DataSerializer.Serialize(DataHead.begin(), DataHead.end(), OutputType);
            ProfileStats->SerializationTime += (TInstant::Now() - startTime);
        } else {
            data = DataSerializer.Serialize(DataHead.begin(), DataHead.end(), OutputType);
        }

        BasicStats.Chunks++;
        BasicStats.RowsOut += data.GetRows();

        DataHead.clear();
        SizeHead.clear();
        MemoryUsed = 0;

        return true;
    }

    bool PopAll(NKikimr::NMiniKQL::TUnboxedValueVector& data) override {
        YQL_ENSURE(!SpilledRows);
        YQL_ENSURE(DataTail.empty());

        if (DataHead.empty()) {
            return false;
        }

        data.reserve(data.size() + DataHead.size());
        for (auto&& v : DataHead) {
            data.emplace_back(std::move(v));
        }

        BasicStats.Chunks++;
        BasicStats.RowsOut += data.size();

        DataHead.clear();
        SizeHead.clear();
        MemoryUsed = 0;

        return true;
    }

    void Finish() override {
        DLOG("Finish request");
        Finished = true;

        if (!BasicStats.FirstRowIn) {
            BasicStats.FirstRowIn = TInstant::Now();
        }
    }

    bool HasData() const override {
        return !DataHead.empty() || SpilledRows != 0 || !DataTail.empty();
    }

    bool IsFinished() const override {
        return Finished && !HasData();
    }

    ui64 Drop() override { // Drop channel data because channel was finished. Leave checkpoint because checkpoints keep going through channel after finishing channel data transfer.
        ui64 rows = DataHead.size() + SpilledRows + DataTail.size();
        DataHead.clear();
        SizeHead.clear();
        DataTail.clear();
        SizeTail.clear();
        MemoryUsed = 0;
        TDataType().swap(DataHead);
        TDataType().swap(DataTail);
        // todo: send remove request
        SpilledRows = 0;
        SpilledBlobs.clear();
        return rows;
    }

    NKikimr::NMiniKQL::TType* GetOutputType() const override {
        return OutputType;
    }

    const TDqOutputChannelStats* GetStats() const override {
        return &BasicStats;
    }

    void Terminate() override {
        if (Storage) {
            Storage.Reset();
        }
    }

    void ValidateUsedMemory() {
#ifndef NDEBUG
        ui64 x = 0;
        for (auto z : SizeHead) x += z;
        for (auto z : SizeTail) x += z;
        YQL_ENSURE(x == MemoryUsed, "" << x << " != " << MemoryUsed);
#endif
    }

private:
    const ui64 ChannelId;
    NKikimr::NMiniKQL::TType* OutputType;
    TDqOutputChannelStats BasicStats;
    TDqOutputChannelStats* ProfileStats = nullptr;
    TDqDataSerializer DataSerializer;
    IDqChannelStorage::TPtr Storage;
    const ui64 MaxStoredBytes;
    ui64 MaxChunkBytes;
    ui64 ChunkSizeLimit;
    TLogFunc LogFunc;
    std::optional<ui32> RowFixedSize;

    struct TSpilledBlob {
        ui64 BlobId;
        ui64 InMemorySize;
        ui64 SerializedSize;
        ui32 Rows;

        TSpilledBlob(ui64 blobId, ui64 inMemorySize, ui64 serializedSize, ui32 rows)
            : BlobId(blobId), InMemorySize(inMemorySize), SerializedSize(serializedSize), Rows(rows) {}
    };

    // DataHead ( . SpilledBlobs (. DataTail)? )?
    using TDataType = TDeque<NUdf::TUnboxedValue, NKikimr::NMiniKQL::TMKQLAllocator<NUdf::TUnboxedValue>>;
    TDataType DataHead;
    TDeque<ui64> SizeHead;
    TDeque<TSpilledBlob> SpilledBlobs;
    TDataType DataTail;
    TDeque<ui64> SizeTail;

    ui64 MemoryUsed = 0; // approx memory usage

    ui64 NextBlobId = 1;
    ui64 SpilledRows = 0;

    bool Finished = false;

    TMaybe<NDqProto::TWatermark> Watermark;
    TMaybe<NDqProto::TCheckpoint> Checkpoint;
};

} // anonymous namespace


IDqOutputChannel::TPtr CreateDqOutputChannel(ui64 channelId, NKikimr::NMiniKQL::TType* outputType,
    const NKikimr::NMiniKQL::TTypeEnvironment& typeEnv, const NKikimr::NMiniKQL::THolderFactory& holderFactory,
    const TDqOutputChannelSettings& settings, const TLogFunc& logFunc)
{
    if (settings.AllowGeneratorsInUnboxedValues) {
        YQL_ENSURE(!settings.ChannelStorage);
        return new TDqOutputChannelOld(channelId, outputType, settings.CollectProfileStats, typeEnv, holderFactory,
            settings.MaxStoredBytes, settings.MaxChunkBytes, settings.ChunkSizeLimit, settings.TransportVersion, logFunc);
    } else {
        return new TDqOutputChannelNew(channelId, outputType, typeEnv, holderFactory, settings, logFunc);
    }
}

} // namespace NYql::NDq
