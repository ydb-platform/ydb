#include "blob.h"
#include "blob_int.h"
#include "header.h"
#include "type_codecs.h"

#include <util/string/builder.h>
#include <util/string/escape.h>
#include <util/system/unaligned_mem.h>
#include <ydb/library/actors/core/log.h>

namespace NKikimr::NPQ {

namespace {

const ui32 CHUNK_SIZE_PLACEMENT = 0xCCCCCCCC;

NScheme::TDataRef GetChunk(const char*& data, const char *end)
{
    ui32 size = ReadUnaligned<ui32>(data);
    data += sizeof(ui32) + size;
    AFL_ENSURE(data <= end);
    return NScheme::TDataRef(data - size, size);
}

template <typename TCodec>
TAutoPtr<NScheme::IChunkCoder> MakeChunk(TBuffer& output)
{
    TCodec codec;
    return codec.MakeChunk(output);
}

void WriteActualChunkSize(TBuffer& output, ui32 sizeOffset)
{
    ui32 currSize = output.size();
    AFL_ENSURE(currSize >= sizeOffset + sizeof(ui32));
    ui32 size = currSize - sizeOffset - sizeof(ui32);

    Y_DEBUG_ABORT_UNLESS(ReadUnaligned<ui32>(output.data() + sizeOffset) == CHUNK_SIZE_PLACEMENT);
    WriteUnaligned<ui32>(output.data() + sizeOffset, size);
}

ui32 WriteTemporaryChunkSize(TBuffer & output)
{
    ui32 sizeOffset = output.Size();

    ui32 sizePlacement = CHUNK_SIZE_PLACEMENT;
    output.Append((const char*)&sizePlacement, sizeof(ui32));

    return sizeOffset;
}

TMessageFlags InitFlags(const TClientBlob& blob) {
    TMessageFlags flags;
    flags.F.HasCreateTimestamp = 1;
    flags.F.HasWriteTimestamp = 1;
    flags.F.HasPartData = !blob.PartData.Empty();
    flags.F.HasUncompressedSize = blob.UncompressedSize != 0;
    flags.F.HasKinesisData = !blob.PartitionKey.empty();
    return flags;
}

ui32 BlobSize(const TClientBlob& blob) {
    auto flags = InitFlags(blob);

    size_t size = sizeof(ui32) /* totalSize */ + sizeof(ui64) /* SeqNo */ + sizeof(ui8) /* flags */;
    if (flags.F.HasPartData) {
        size += sizeof(ui16) /* PartNo */ + sizeof(ui16) /* TotalParts */ + sizeof(ui32) /* sizeof(ui32) */;
    }
    if (flags.F.HasKinesisData) {
        size += sizeof(ui8) + blob.PartitionKey.size() + sizeof(ui8) + blob.ExplicitHashKey.size();
    }
    size += sizeof(ui64) /* WriteTimestamp */ + sizeof(ui64) /* CreateTimestamp */;
    if (flags.F.HasUncompressedSize) {
        size += sizeof(ui32);
    }
    size += sizeof(ui16) + blob.SourceId.size();
    size += blob.Data.size();

    return size;
}


template<NKikimrPQ::TBatchHeader::EPayloadFormat Type>
struct TBatchSerializer {
    TBatchSerializer(TBatch& batch)
        : Batch(batch) {
    }

    void Pack();

    TBatch& Batch;
};


template<NKikimrPQ::TBatchHeader::EPayloadFormat Type>
struct TBatchDeserializer {
    TBatchDeserializer(const TBatch& batch)
        : Batch(batch) {
    }

    void Unpack(TVector<TClientBlob> *blobs) const;

    const TBatch& Batch;
};

template<>
void TBatchSerializer<NKikimrPQ::TBatchHeader::ECompressed>::Pack() {
    Batch.PackedData.Clear();
    bool hasUncompressed = false;
    bool hasKinesis = false;
    for (ui32 i = 0; i < Batch.Blobs.size(); ++i) {
        if (Batch.Blobs[i].UncompressedSize > 0)
            hasUncompressed = true;

        if (!Batch.Blobs[i].PartitionKey.empty() || !Batch.Blobs[i].ExplicitHashKey.empty()) {
            hasKinesis = true;
        }
    }

    Batch.Header.SetFormat(NKikimrPQ::TBatchHeader::ECompressed);
    Batch.Header.SetHasKinesis(hasKinesis);
    ui32 totalCount = Batch.Blobs.size();
    AFL_ENSURE(totalCount == Batch.Header.GetCount() + Batch.Header.GetInternalPartsCount());
    ui32 cnt = 0;
    THashMap<TStringBuf, ui32> reorderMap;
    for (ui32 i = 0; i < Batch.Blobs.size(); ++i) {
        if (Batch.Blobs[i].IsLastPart()) {
            ++cnt;
        }
        ++reorderMap[TStringBuf(Batch.Blobs[i].SourceId)];
    }
    AFL_ENSURE(cnt == Batch.Header.GetCount());
    TVector<ui32> start(reorderMap.size(), 0);
    TVector<ui32> pos(Batch.Blobs.size(), 0);
    ui32 sum = 0;
    ui32 i = 0;
    for (auto it = reorderMap.begin(); it != reorderMap.end(); ++it) {
        start[i] = sum;
        sum += it->second;
        it->second = i;
        ++i;
    }
    for (ui32 i = 0; i < Batch.Blobs.size(); ++i) {
        pos[start[reorderMap[TStringBuf(Batch.Blobs[i].SourceId)]]++] = i;
    }

    //output order
    {
        ui32 sizeOffset = WriteTemporaryChunkSize(Batch.PackedData);
        auto chunk = MakeChunk<NScheme::TVarIntCodec<ui32, false>>(Batch.PackedData);
        for (const auto& p : pos) {
            chunk->AddData((const char*)&p, sizeof(p));
        }
        ui32 size = start.size();
        chunk->AddData((const char*)&size, sizeof(size));
        for (const auto& p : start) {
            chunk->AddData((const char*)&p, sizeof(p));
        }
        chunk->Seal();
        WriteActualChunkSize(Batch.PackedData, sizeOffset);
    }

    //output SourceId
    {
        ui32 sizeOffset = WriteTemporaryChunkSize(Batch.PackedData);
        auto chunk = MakeChunk<NScheme::TVarLenCodec<false>>(Batch.PackedData);
        for (auto it = reorderMap.begin(); it != reorderMap.end(); ++it) {
            chunk->AddData(it->first.data(), it->first.size());
        }
        chunk->Seal();
        WriteActualChunkSize(Batch.PackedData, sizeOffset);
    }

    //output SeqNo
    {
        ui32 sizeOffset = WriteTemporaryChunkSize(Batch.PackedData);
        auto chunk = MakeChunk<NScheme::TDeltaVarIntCodec<ui64, false>>(Batch.PackedData);
        for (const auto& p : pos) {
            chunk->AddData((const char*)&Batch.Blobs[p].SeqNo, sizeof(ui64));
        }
        chunk->Seal();
        WriteActualChunkSize(Batch.PackedData, sizeOffset);
    }

    //output Data
    {
        ui32 sizeOffset = WriteTemporaryChunkSize(Batch.PackedData);
        auto chunk = MakeChunk<NScheme::TVarLenCodec<false>>(Batch.PackedData);
        for (const auto& p : pos) {
            chunk->AddData(Batch.Blobs[p].Data.data(), Batch.Blobs[p].Data.size());
        }
        chunk->Seal();
        WriteActualChunkSize(Batch.PackedData, sizeOffset);
    }

    //output PartData::Pos + payload
    {
        ui32 sizeOffset = WriteTemporaryChunkSize(Batch.PackedData);
        auto chunk = MakeChunk<NScheme::TVarIntCodec<ui32, false>>(Batch.PackedData);
        ui32 cnt = 0;
        for (ui32 i = 0; i < Batch.Blobs.size(); ++i) {
            if (Batch.Blobs[i].PartData) {
                ++cnt;
            }
        }
        chunk->AddData((const char*)&cnt, sizeof(ui32));
        for (ui32 i = 0; i < Batch.Blobs.size(); ++i) {
            if (Batch.Blobs[i].PartData) {
                chunk->AddData((const char*)&i, sizeof(ui32));
                ui32 t = Batch.Blobs[i].PartData->PartNo;
                chunk->AddData((const char*)&t, sizeof(ui32));
                t = Batch.Blobs[i].PartData->TotalParts;
                chunk->AddData((const char*)&t, sizeof(ui32));
                chunk->AddData((const char*)&Batch.Blobs[i].PartData->TotalSize, sizeof(ui32));
            }
        }
        chunk->Seal();
        WriteActualChunkSize(Batch.PackedData, sizeOffset);
    }

    //output Wtime
    {
        ui32 sizeOffset = WriteTemporaryChunkSize(Batch.PackedData);
        auto chunk = MakeChunk<NScheme::TDeltaVarIntCodec<ui64, false>>(Batch.PackedData);
        for (ui32 i = 0; i < Batch.Blobs.size(); ++i) {
            ui64 writeTimestampMs = Batch.Blobs[i].WriteTimestamp.MilliSeconds();
            chunk->AddData((const char*)&writeTimestampMs, sizeof(ui64));
        }
        chunk->Seal();
        WriteActualChunkSize(Batch.PackedData, sizeOffset);
    }

    if (hasKinesis) {
        {
            ui32 sizeOffset = WriteTemporaryChunkSize(Batch.PackedData);
            auto chunk = MakeChunk<NScheme::TVarLenCodec<false>>(Batch.PackedData);
            for (const auto &p : pos) {
                chunk->AddData(Batch.Blobs[p].PartitionKey.data(), Batch.Blobs[p].PartitionKey.size());
            }
            chunk->Seal();
            WriteActualChunkSize(Batch.PackedData, sizeOffset);
        }

        {
            ui32 sizeOffset = WriteTemporaryChunkSize(Batch.PackedData);
            auto chunk = MakeChunk<NScheme::TVarLenCodec<false>>(Batch.PackedData);
            for (const auto &p : pos) {
                chunk->AddData(Batch.Blobs[p].ExplicitHashKey.data(), Batch.Blobs[p].ExplicitHashKey.size());
            }
            chunk->Seal();
            WriteActualChunkSize(Batch.PackedData, sizeOffset);
        }
    }

    //output Ctime
    {
        ui32 sizeOffset = WriteTemporaryChunkSize(Batch.PackedData);
        auto chunk = MakeChunk<NScheme::TDeltaVarIntCodec<ui64, false>>(Batch.PackedData);
        for (ui32 i = 0; i < Batch.Blobs.size(); ++i) {
            ui64 createTimestampMs = Batch.Blobs[i].CreateTimestamp.MilliSeconds();
            chunk->AddData((const char*)&createTimestampMs, sizeof(ui64));
        }
        chunk->Seal();
        WriteActualChunkSize(Batch.PackedData, sizeOffset);
    }

    //output Uncompressed
    if (hasUncompressed) {
        ui32 sizeOffset = WriteTemporaryChunkSize(Batch.PackedData);
        auto chunk = MakeChunk<NScheme::TVarIntCodec<ui32, false>>(Batch.PackedData);
        for (ui32 i = 0; i < Batch.Blobs.size(); ++i) {
            chunk->AddData((const char*)&Batch.Blobs[i].UncompressedSize, sizeof(ui32));
        }
        chunk->Seal();
        WriteActualChunkSize(Batch.PackedData, sizeOffset);
    }

    Batch.Header.SetPayloadSize(Batch.PackedData.size());
}

template<>
void TBatchDeserializer<NKikimrPQ::TBatchHeader::ECompressed>::Unpack(TVector<TClientBlob>* blobs) const {
    AFL_ENSURE(Batch.Header.GetFormat() == NKikimrPQ::TBatchHeader::ECompressed);
    AFL_ENSURE(Batch.PackedData.size());

    ui32 totalBlobs = Batch.Header.GetCount() + Batch.Header.GetInternalPartsCount();
    ui32 partsSize = 0;
    TVector<ui32> end;
    TVector<ui32> pos;
    pos.reserve(totalBlobs);
    const char* data = Batch.PackedData.data();
    const char* dataEnd = Batch.PackedData.data() + Batch.PackedData.size();
    ui32 sourceIdCount = 0;
    TVector<TString> sourceIds;

    static const NScheme::TTypeCodecs ui32Codecs(NScheme::NTypeIds::Uint32), ui64Codecs(NScheme::NTypeIds::Uint64), stringCodecs(NScheme::NTypeIds::String);
    //read order
    {
        auto chunk = NScheme::IChunkDecoder::ReadChunk(GetChunk(data, dataEnd), &ui32Codecs);
        auto iter = chunk->MakeIterator();
        for (ui32 i = 0; i < totalBlobs; ++i) {
            pos.push_back(ReadUnaligned<ui32>(iter->Next().Data()));
        }
        sourceIdCount = ReadUnaligned<ui32>(iter->Next().Data());
        end.reserve(sourceIdCount);
        for (ui32 i = 0; i < sourceIdCount; ++i) {
            end.push_back(ReadUnaligned<ui32>(iter->Next().Data()));
        }
    }

    sourceIds.reserve(sourceIdCount);
    //read SourceId
    {
        auto chunk = NScheme::IChunkDecoder::ReadChunk(GetChunk(data, dataEnd), &stringCodecs);
        auto iter = chunk->MakeIterator();
        for (ui32 i = 0; i < sourceIdCount; ++i) {
            auto ref = iter->Next();
            sourceIds.emplace_back(ref.Data(), ref.Size());
        }
    }
    TVector<ui64> seqNo;
    seqNo.reserve(totalBlobs);

    //read SeqNo
    {
        auto chunk = NScheme::IChunkDecoder::ReadChunk(GetChunk(data, dataEnd), &ui64Codecs);
        auto iter = chunk->MakeIterator();
        for (ui32 i = 0; i < totalBlobs; ++i) {
            seqNo.push_back(ReadUnaligned<ui64>(iter->Next().Data()));
        }
    }
    TVector<TString> dt;
    dt.reserve(totalBlobs);

    //read Data
    {
        auto chunk = NScheme::IChunkDecoder::ReadChunk(GetChunk(data, dataEnd), &stringCodecs);
        auto iter = chunk->MakeIterator();
        for (ui32 i = 0; i < totalBlobs; ++i) {
            auto ref = iter->Next();
            dt.emplace_back(ref.Data(), ref.Size());
        }
    }
    THashMap<ui32, TPartData> partData;

    //read PartData
    {
        auto chunk = NScheme::IChunkDecoder::ReadChunk(GetChunk(data, dataEnd), &ui32Codecs);
        auto iter = chunk->MakeIterator();
        partsSize = ReadUnaligned<ui32>(iter->Next().Data());
        partData.reserve(partsSize);
        for (ui32 i = 0; i < partsSize; ++i) {
            ui32 ps = ReadUnaligned<ui32>(iter->Next().Data());
            ui16 partNo = ReadUnaligned<ui32>(iter->Next().Data());
            ui16 totalParts = ReadUnaligned<ui32>(iter->Next().Data());
            ui32 totalSize = ReadUnaligned<ui32>(iter->Next().Data());
            partData.insert(std::make_pair(ps, TPartData(partNo, totalParts, totalSize)));
        }
    }
    TVector<TInstant> wtime;
    wtime.reserve(totalBlobs);
    {
        auto chunk = NScheme::IChunkDecoder::ReadChunk(GetChunk(data, dataEnd), &ui64Codecs);
        auto iter = chunk->MakeIterator();
        for (ui32 i = 0; i < totalBlobs; ++i) {
            ui64 timestampMs = ReadUnaligned<ui64>(iter->Next().Data());
            wtime.push_back(TInstant::MilliSeconds(timestampMs));
        }
    }
    TVector<TInstant> ctime;
    ctime.reserve(totalBlobs);

    TVector<TString> partitionKey;
    TVector<TString> explicitHash;
    partitionKey.reserve(totalBlobs);
    explicitHash.reserve(totalBlobs);
    if (Batch.Header.GetHasKinesis()) {
        {
            auto chunk = NScheme::IChunkDecoder::ReadChunk(GetChunk(data, dataEnd), &stringCodecs);
            auto iter = chunk->MakeIterator();
            for (ui32 i = 0; i < totalBlobs; ++i) {
                auto ref = iter->Next();
                partitionKey.emplace_back(ref.Data(), ref.Size());
            }
        }

        {
            auto chunk = NScheme::IChunkDecoder::ReadChunk(GetChunk(data, dataEnd), &stringCodecs);
            auto iter = chunk->MakeIterator();
            for (ui32 i = 0; i < totalBlobs; ++i) {
                auto ref = iter->Next();
                explicitHash.emplace_back(ref.Data(), ref.Size());
            }
        }
    } else {
        partitionKey.resize(totalBlobs);
        explicitHash.resize(totalBlobs);
    }

    if (data < dataEnd) { //old versions could not have CTime
        {
            auto chunk = NScheme::IChunkDecoder::ReadChunk(GetChunk(data, dataEnd), &ui64Codecs);
            auto iter = chunk->MakeIterator();
            for (ui32 i = 0; i < totalBlobs; ++i) {
                ui64 timestampMs = ReadUnaligned<ui64>(iter->Next().Data());
                ctime.push_back(TInstant::MilliSeconds(timestampMs));
            }
        }
    } else {
        ctime.resize(totalBlobs); //fill with zero-s
    }

    TVector<ui64> uncompressedSize;
    uncompressedSize.reserve(totalBlobs);
    if (data < dataEnd) { //old versions could not have UncompressedSize
        {
            auto chunk = NScheme::IChunkDecoder::ReadChunk(GetChunk(data, dataEnd), &ui64Codecs);
            auto iter = chunk->MakeIterator();
            for (ui32 i = 0; i < totalBlobs; ++i) {
                uncompressedSize.push_back(ReadUnaligned<ui64>(iter->Next().Data()));
            }
        }
    } else {
        uncompressedSize.resize(totalBlobs); //fill with zero-s
    }

    AFL_ENSURE(data == dataEnd);

    blobs->resize(totalBlobs);
    ui32 currentSID = 0;
    for (ui32 i = 0; i < totalBlobs; ++i) {
        TMaybe<TPartData> pd;
        if (auto it = partData.find(pos[i]); it != partData.end()) {
            pd = it->second;
        }

        bool lastPart = i + 1 == end[currentSID];

        auto& processedBlob = (*blobs)[pos[i]];
         // One SourceId stored for all parts of the message (many client blobs)
        if (lastPart) {
            processedBlob.SourceId = std::move(sourceIds[currentSID]);
        } else {
            processedBlob.SourceId = sourceIds[currentSID];
        }
        processedBlob.SeqNo = seqNo[i];
        processedBlob.Data = std::move(dt[i]);
        processedBlob.PartData = pd;
        processedBlob.WriteTimestamp = wtime[pos[i]];
        processedBlob.CreateTimestamp = ctime[pos[i]];
        processedBlob.UncompressedSize = uncompressedSize[pos[i]];
        processedBlob.PartitionKey = std::move(partitionKey[i]);
        processedBlob.ExplicitHashKey = std::move(explicitHash[i]);

        if (lastPart) {
            ++currentSID;
        }
    }
}

template<>
void TBatchSerializer<NKikimrPQ::TBatchHeader::EUncompressed>::Pack() {
    Batch.Header.SetFormat(NKikimrPQ::TBatchHeader::EUncompressed);
    Batch.PackedData.Clear();
    for (ui32 i = 0; i < Batch.Blobs.size(); ++i) {
        Serialize(Batch.Blobs[i], Batch.PackedData);
    }
    Batch.Header.SetPayloadSize(Batch.PackedData.size());
}

template<>
void TBatchDeserializer<NKikimrPQ::TBatchHeader::EUncompressed>::Unpack(TVector<TClientBlob>* blobs) const {
    AFL_ENSURE(Batch.Header.GetFormat() == NKikimrPQ::TBatchHeader::EUncompressed);
    AFL_ENSURE(Batch.PackedData.size());
    ui32 shift = 0;

    for (ui32 i = 0; i < Batch.GetCount() + Batch.GetInternalPartsCount(); ++i) {
        AFL_ENSURE(shift < Batch.PackedData.size());
        blobs->push_back(DeserializeClientBlob(Batch.PackedData.data() + shift, Batch.PackedData.size() - shift));
        shift += ReadUnaligned<ui32>(Batch.PackedData.data() + shift);
    }
    AFL_ENSURE(shift == Batch.PackedData.size());
}

} // namespace

//
// TClientBlob
//

ui32 TClientBlob::GetSerializedSize() const {
    return BlobSize(*this);
}


//
// TBatch
//

void TBatch::SerializeTo(TString& res) const{
    AFL_ENSURE(Packed);

    ui16 sz = Header.ByteSize();
    res.append((const char*)&sz, sizeof(ui16));

    bool rs = Header.AppendToString(&res);
    AFL_ENSURE(rs);

    res.append(PackedData.data(), PackedData.size());
}

void TBatch::Pack() {
    if (Packed) {
        return;
    }
    Packed = true;

    TBatchSerializer<NKikimrPQ::TBatchHeader::ECompressed>(*this).Pack();

    if (GetPackedSize() > GetUnpackedSize() + GetMaxHeaderSize()) { //packing is not effective, write as-is
        TBatchSerializer<NKikimrPQ::TBatchHeader::EUncompressed>(*this).Pack();
    }

    for (auto& b : Blobs) {
        EndWriteTimestamp = std::max(EndWriteTimestamp, b.WriteTimestamp);
    }

    TVector<TClientBlob> tmp;
    Blobs.swap(tmp);
    InternalPartsPos.resize(0);
    AFL_ENSURE(GetPackedSize() <= GetUnpackedSize() + GetMaxHeaderSize()); //be sure that PackedSize is not bigger than packed size for packing type 0
}

void TBatch::Unpack() {
    if (!Packed)
        return;
    Packed = false;
    AFL_ENSURE(Blobs.empty());
    UnpackTo(&Blobs);
    AFL_ENSURE(InternalPartsPos.empty());
    for (ui32 i = 0; i < Blobs.size(); ++i) {
        auto& b = Blobs[i];
        if (!b.IsLastPart()) {
            InternalPartsPos.push_back(i);
        }
        EndWriteTimestamp = std::max(EndWriteTimestamp, b.WriteTimestamp);
    }
    AFL_ENSURE(InternalPartsPos.size() == GetInternalPartsCount());

    PackedData.Clear();
}

void TBatch::UnpackTo(TVector<TClientBlob> *blobs) const
{
    AFL_ENSURE(PackedData.size());
    auto type = Header.GetFormat();
    switch (type) {
        case NKikimrPQ::TBatchHeader::EUncompressed:
            TBatchDeserializer<NKikimrPQ::TBatchHeader::EUncompressed>(*this).Unpack(blobs);
            break;
        case NKikimrPQ::TBatchHeader::ECompressed:
            TBatchDeserializer<NKikimrPQ::TBatchHeader::ECompressed>(*this).Unpack(blobs);
            break;
        default:
            Y_ABORT("uknown type");
    };
}

void Serialize(const TClientBlob& blob, TBuffer& res) {
    const ui32 totalSize = BlobSize(blob);
    const ui32 expectedSize = res.Size() + totalSize;

    TMessageFlags flags = InitFlags(blob);

    res.Reserve(res.Size() + totalSize);

    res.Append((const char*)&totalSize, sizeof(ui32));
    res.Append((const char*)&blob.SeqNo, sizeof(ui64));
    res.Append((const char*)&flags.V, sizeof(char));

    if (flags.F.HasPartData) {
            res.Append((const char*)&(blob.PartData->PartNo), sizeof(ui16));
            res.Append((const char*)&(blob.PartData->TotalParts),
                       sizeof(ui16));
            res.Append((const char*)&(blob.PartData->TotalSize), sizeof(ui32));
    }

    if (flags.F.HasKinesisData) {
            ui8 partitionKeySize = blob.PartitionKey.size();
            res.Append((const char*)&(partitionKeySize), sizeof(ui8));
            res.Append(blob.PartitionKey.data(), blob.PartitionKey.size());

            ui8 hashKeySize = blob.ExplicitHashKey.size();
            res.Append((const char *)&(hashKeySize), sizeof(ui8));
            res.Append(blob.ExplicitHashKey.data(),
                       blob.ExplicitHashKey.size());
    }

    ui64 writeTimestampMs = blob.WriteTimestamp.MilliSeconds();
    res.Append((const char*)&writeTimestampMs, sizeof(ui64));

    ui64 createTimestampMs = blob.CreateTimestamp.MilliSeconds();
    res.Append((const char*)&createTimestampMs, sizeof(ui64));

    if (flags.F.HasUncompressedSize) {
            res.Append((const char*)&(blob.UncompressedSize), sizeof(ui32));
    }

    ui16 sz = blob.SourceId.size();
    res.Append((const char*)&sz, sizeof(ui16));
    res.Append(blob.SourceId.data(), blob.SourceId.size());
    res.Append(blob.Data.data(), blob.Data.size());

    AFL_ENSURE(res.Size() == expectedSize);
}

TClientBlob DeserializeClientBlob(const char *data, ui32 size) {
    AFL_ENSURE(size > TClientBlob::OVERHEAD);
    ui32 totalSize = ReadUnaligned<ui32>(data);
    AFL_ENSURE(size >= totalSize);
    const char *end = data + totalSize;
    data += sizeof(ui32);

    ui64 seqNo = ReadUnaligned<ui64>(data);
    data += sizeof(ui64);

    TMessageFlags flags(ReadUnaligned<ui8>(data));
    ++data;

    TMaybe<TPartData> partData;
    if (flags.F.HasPartData) {
            ui16 partNo = ReadUnaligned<ui16>(data);
            data += sizeof(ui16);
            ui16 totalParts = ReadUnaligned<ui16>(data);
            data += sizeof(ui16);
            ui32 totalSize = ReadUnaligned<ui32>(data);
            data += sizeof(ui32);
            partData = TPartData{partNo, totalParts, totalSize};
    }

    TString partitionKey;
    TString explicitHashKey;
    if (flags.F.HasKinesisData) {
            ui8 keySize = ReadUnaligned<ui8>(data);
            data += sizeof(ui8);
            partitionKey = TString(data, keySize == 0 ? 256 : keySize);
            data += partitionKey.size();
            keySize = ReadUnaligned<ui8>(data);
            data += sizeof(ui8);
            explicitHashKey = TString(data, keySize);
            data += explicitHashKey.size();
    }

    TInstant writeTimestamp;
    if (flags.F.HasWriteTimestamp) {
            writeTimestamp = TInstant::MilliSeconds(ReadUnaligned<ui64>(data));
            data += sizeof(ui64);
    }
    TInstant createTimestamp;
    if (flags.F.HasCreateTimestamp) {
            createTimestamp = TInstant::MilliSeconds(ReadUnaligned<ui64>(data));
            data += sizeof(ui64);
    }
    ui32 uncompressedSize = 0;
    if (flags.F.HasUncompressedSize) {
            uncompressedSize = ReadUnaligned<ui32>(data);
            data += sizeof(ui32);
    }

    AFL_ENSURE(data < end);
    ui16 sz = ReadUnaligned<ui16>(data);
    data += sizeof(ui16);
    AFL_ENSURE(data + sz <= end);
    TString sourceId(data, sz);
    data += sz;

    TString dt = (data != end) ? TString(data, end - data) : TString{};

    return TClientBlob(std::move(sourceId), seqNo, std::move(dt), partData,
                       writeTimestamp, createTimestamp, uncompressedSize,
                       std::move(partitionKey), std::move(explicitHashKey));
}
} //NKikimr :: NPQ
