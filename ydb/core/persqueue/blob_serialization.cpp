#include "blob.h"
#include "blob_int.h"
#include "header.h"
#include "type_codecs.h"

#include <util/string/builder.h>
#include <util/string/escape.h>
#include <util/system/unaligned_mem.h>

namespace NKikimr::NPQ {

namespace {

const ui32 CHUNK_SIZE_PLACEMENT = 0xCCCCCCCC;

NScheme::TDataRef GetChunk(const char*& data, const char *end)
{
    ui32 size = ReadUnaligned<ui32>(data);
    data += sizeof(ui32) + size;
    Y_ABORT_UNLESS(data <= end);
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
    Y_ABORT_UNLESS(currSize >= sizeOffset + sizeof(ui32));
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

}


//
// TClientBlob
//

void TClientBlob::SerializeTo(TBuffer& res) const
{
    const ui32 totalSize = GetBlobSize();
    const ui32 psize = res.Size();

    res.Reserve(res.Size() + totalSize);

    res.Append((const char*)&totalSize, sizeof(ui32));
    res.Append((const char*)&SeqNo, sizeof(ui64));

    TMessageFlags flags;
    flags.F.HasCreateTimestamp = 1;
    flags.F.HasWriteTimestamp = 1;
    flags.F.HasPartData = !PartData.Empty();
    flags.F.HasUncompressedSize = UncompressedSize != 0;
    flags.F.HasKinesisData = !PartitionKey.empty();

    res.Append((const char*)&flags.V, sizeof(char));

    if (flags.F.HasPartData) {
        res.Append((const char*)&(PartData->PartNo), sizeof(ui16));
        res.Append((const char*)&(PartData->TotalParts), sizeof(ui16));
        res.Append((const char*)&(PartData->TotalSize), sizeof(ui32));
    }

    if (flags.F.HasKinesisData) {
        ui8 partitionKeySize = PartitionKey.size();
        res.Append((const char*)&(partitionKeySize), sizeof(ui8));
        res.Append(PartitionKey.data(), PartitionKey.size());

        ui8 hashKeySize = ExplicitHashKey.size();
        res.Append((const char*)&(hashKeySize), sizeof(ui8));
        res.Append(ExplicitHashKey.data(), ExplicitHashKey.size());
    }

    ui64 writeTimestampMs = WriteTimestamp.MilliSeconds();
    res.Append((const char*)&writeTimestampMs, sizeof(ui64));

    ui64 createTimestampMs = CreateTimestamp.MilliSeconds();
    res.Append((const char*)&createTimestampMs, sizeof(ui64));

    if (flags.F.HasUncompressedSize) {
        res.Append((const char*)&(UncompressedSize), sizeof(ui32));
    }

    ui16 sz = SourceId.size();
    res.Append((const char*)&sz, sizeof(ui16));
    res.Append(SourceId.data(), SourceId.size());
    res.Append(Data.data(), Data.size());

    Y_ABORT_UNLESS(res.Size() == psize + totalSize);
}

TClientBlob TClientBlob::Deserialize(const char* data, ui32 size)
{
    Y_ABORT_UNLESS(size > OVERHEAD);
    ui32 totalSize = ReadUnaligned<ui32>(data);
    Y_ABORT_UNLESS(size >= totalSize);
    const char *end = data + totalSize;
    data += sizeof(ui32);

    ui64 seqNo = ReadUnaligned<ui64>(data);
    data += sizeof(ui64);

    TMessageFlags flags;
    flags.V = ReadUnaligned<ui8>(data);
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

    Y_ABORT_UNLESS(data < end);
    ui16 sz = ReadUnaligned<ui16>(data);
    data += sizeof(ui16);
    Y_ABORT_UNLESS(data + sz < end);
    TString sourceId(data, sz);
    data += sz;

    Y_ABORT_UNLESS(data < end, "size %u SeqNo %" PRIu64 " SourceId %s", size, seqNo, sourceId.c_str());

    TString dt(data, end - data);

    return TClientBlob(std::move(sourceId), seqNo, std::move(dt), partData, writeTimestamp, createTimestamp, uncompressedSize, std::move(partitionKey), std::move(explicitHashKey));
}


//
// TBatch
//

void TBatch::SerializeTo(TString& res) const{
    Y_ABORT_UNLESS(Packed);

    ui16 sz = Header.ByteSize();
    res.append((const char*)&sz, sizeof(ui16));

    bool rs = Header.AppendToString(&res);
    Y_ABORT_UNLESS(rs);

    res.append(PackedData.data(), PackedData.size());
}

void TBatch::Pack() {
    if (Packed)
        return;
    Packed = true;
    PackedData.Clear();
    bool hasUncompressed = false;
    bool hasKinesis = false;
    for (ui32 i = 0; i < Blobs.size(); ++i) {
        if (Blobs[i].UncompressedSize > 0)
            hasUncompressed = true;

        if (!Blobs[i].PartitionKey.empty() || !Blobs[i].ExplicitHashKey.empty()) {
            hasKinesis = true;
        }
    }

    Header.SetFormat(NKikimrPQ::TBatchHeader::ECompressed);
    Header.SetHasKinesis(hasKinesis);
    ui32 totalCount = Blobs.size();
    Y_ABORT_UNLESS(totalCount == Header.GetCount() + Header.GetInternalPartsCount());
    ui32 cnt = 0;
    THashMap<TStringBuf, ui32> reorderMap;
    for (ui32 i = 0; i < Blobs.size(); ++i) {
        if (Blobs[i].IsLastPart())
            ++cnt;
        ++reorderMap[TStringBuf(Blobs[i].SourceId)];
    }
    Y_ABORT_UNLESS(cnt == Header.GetCount());
    TVector<ui32> start(reorderMap.size(), 0);
    TVector<ui32> pos(Blobs.size(), 0);
    ui32 sum = 0;
    ui32 i = 0;
    for (auto it = reorderMap.begin(); it != reorderMap.end(); ++it) {
        start[i] = sum;
        sum += it->second;
        it->second = i;
        ++i;
    }
    for (ui32 i = 0; i < Blobs.size(); ++i) {
        pos[start[reorderMap[TStringBuf(Blobs[i].SourceId)]]++] = i;
    }

    //output order
    {
        ui32 sizeOffset = WriteTemporaryChunkSize(PackedData);
        auto chunk = MakeChunk<NScheme::TVarIntCodec<ui32, false>>(PackedData);
        for (const auto& p : pos) {
            chunk->AddData((const char*)&p, sizeof(p));
        }
        ui32 size = start.size();
        chunk->AddData((const char*)&size, sizeof(size));
        for (const auto& p : start) {
            chunk->AddData((const char*)&p, sizeof(p));
        }
        chunk->Seal();
        WriteActualChunkSize(PackedData, sizeOffset);
    }

    //output SourceId
    {
        ui32 sizeOffset = WriteTemporaryChunkSize(PackedData);
        auto chunk = MakeChunk<NScheme::TVarLenCodec<false>>(PackedData);
        for (auto it = reorderMap.begin(); it != reorderMap.end(); ++it) {
            chunk->AddData(it->first.data(), it->first.size());
        }
        chunk->Seal();
        WriteActualChunkSize(PackedData, sizeOffset);
    }

    //output SeqNo
    {
        ui32 sizeOffset = WriteTemporaryChunkSize(PackedData);
        auto chunk = MakeChunk<NScheme::TDeltaVarIntCodec<ui64, false>>(PackedData);
        for (const auto& p : pos) {
            chunk->AddData((const char*)&Blobs[p].SeqNo, sizeof(ui64));
        }
        chunk->Seal();
        WriteActualChunkSize(PackedData, sizeOffset);
    }

    //output Data
    {
        ui32 sizeOffset = WriteTemporaryChunkSize(PackedData);
        auto chunk = MakeChunk<NScheme::TVarLenCodec<false>>(PackedData);
        for (const auto& p : pos) {
            chunk->AddData(Blobs[p].Data.data(), Blobs[p].Data.size());
        }
        chunk->Seal();
        WriteActualChunkSize(PackedData, sizeOffset);
    }

    //output PartData::Pos + payload
    {
        ui32 sizeOffset = WriteTemporaryChunkSize(PackedData);
        auto chunk = MakeChunk<NScheme::TVarIntCodec<ui32, false>>(PackedData);
        ui32 cnt = 0;
        for (ui32 i = 0; i < Blobs.size(); ++i) {
            if (Blobs[i].PartData)
                ++cnt;
        }
        chunk->AddData((const char*)&cnt, sizeof(ui32));
        for (ui32 i = 0; i < Blobs.size(); ++i) {
            if (Blobs[i].PartData) {
                chunk->AddData((const char*)&i, sizeof(ui32));
                ui32 t = Blobs[i].PartData->PartNo;
                chunk->AddData((const char*)&t, sizeof(ui32));
                t = Blobs[i].PartData->TotalParts;
                chunk->AddData((const char*)&t, sizeof(ui32));
                chunk->AddData((const char*)&Blobs[i].PartData->TotalSize, sizeof(ui32));
            }
        }
        chunk->Seal();
        WriteActualChunkSize(PackedData, sizeOffset);
    }

    //output Wtime
    {
        ui32 sizeOffset = WriteTemporaryChunkSize(PackedData);
        auto chunk = MakeChunk<NScheme::TDeltaVarIntCodec<ui64, false>>(PackedData);
        for (ui32 i = 0; i < Blobs.size(); ++i) {
            ui64 writeTimestampMs = Blobs[i].WriteTimestamp.MilliSeconds();
            chunk->AddData((const char*)&writeTimestampMs, sizeof(ui64));
        }
        chunk->Seal();
        WriteActualChunkSize(PackedData, sizeOffset);
    }

    if (hasKinesis) {
        {
            ui32 sizeOffset = WriteTemporaryChunkSize(PackedData);
            auto chunk = MakeChunk<NScheme::TVarLenCodec<false>>(PackedData);
            for (const auto &p : pos) {
                chunk->AddData(Blobs[p].PartitionKey.data(), Blobs[p].PartitionKey.size());
            }
            chunk->Seal();
            WriteActualChunkSize(PackedData, sizeOffset);
        }

        {
            ui32 sizeOffset = WriteTemporaryChunkSize(PackedData);
            auto chunk = MakeChunk<NScheme::TVarLenCodec<false>>(PackedData);
            for (const auto &p : pos) {
                chunk->AddData(Blobs[p].ExplicitHashKey.data(), Blobs[p].ExplicitHashKey.size());
            }
            chunk->Seal();
            WriteActualChunkSize(PackedData, sizeOffset);
        }
    }

    //output Ctime
    {
        ui32 sizeOffset = WriteTemporaryChunkSize(PackedData);
        auto chunk = MakeChunk<NScheme::TDeltaVarIntCodec<ui64, false>>(PackedData);
        for (ui32 i = 0; i < Blobs.size(); ++i) {
            ui64 createTimestampMs = Blobs[i].CreateTimestamp.MilliSeconds();
            chunk->AddData((const char*)&createTimestampMs, sizeof(ui64));
        }
        chunk->Seal();
        WriteActualChunkSize(PackedData, sizeOffset);
    }

    //output Uncompressed
    if (hasUncompressed) {
        ui32 sizeOffset = WriteTemporaryChunkSize(PackedData);
        auto chunk = MakeChunk<NScheme::TVarIntCodec<ui32, false>>(PackedData);
        for (ui32 i = 0; i < Blobs.size(); ++i) {
            chunk->AddData((const char*)&Blobs[i].UncompressedSize, sizeof(ui32));
        }
        chunk->Seal();
        WriteActualChunkSize(PackedData, sizeOffset);
    }

    Header.SetPayloadSize(PackedData.size());

    if (GetPackedSize() > GetUnpackedSize() + GetMaxHeaderSize()) { //packing is not effective, write as-is
        Header.SetFormat(NKikimrPQ::TBatchHeader::EUncompressed);
        PackedData.Clear();
        for (ui32 i = 0; i < Blobs.size(); ++i) {
            Blobs[i].SerializeTo(PackedData);
        }
        Header.SetPayloadSize(PackedData.size());
    }

    for (auto& b : Blobs) {
        EndWriteTimestamp = std::max(EndWriteTimestamp, b.WriteTimestamp);
    }


    TVector<TClientBlob> tmp;
    Blobs.swap(tmp);
    InternalPartsPos.resize(0);
    Y_ABORT_UNLESS(GetPackedSize() <= GetUnpackedSize() + GetMaxHeaderSize()); //be sure that PackedSize is not bigger than packed size for packing type 0
}

void TBatch::Unpack() {
    if (!Packed)
        return;
    Packed = false;
    Y_ABORT_UNLESS(Blobs.empty());
    UnpackTo(&Blobs);
    Y_ABORT_UNLESS(InternalPartsPos.empty());
    for (ui32 i = 0; i < Blobs.size(); ++i) {
        auto& b = Blobs[i];
        if (!b.IsLastPart()) {
            InternalPartsPos.push_back(i);
        }
        EndWriteTimestamp = std::max(EndWriteTimestamp, b.WriteTimestamp);
    }
    Y_ABORT_UNLESS(InternalPartsPos.size() == GetInternalPartsCount());

    PackedData.Clear();
}

void TBatch::UnpackTo(TVector<TClientBlob> *blobs) const
{
    Y_ABORT_UNLESS(PackedData.size());
    auto type = Header.GetFormat();
    switch (type) {
        case NKikimrPQ::TBatchHeader::EUncompressed:
            UnpackToType0(blobs);
            break;
        case NKikimrPQ::TBatchHeader::ECompressed:
            UnpackToType1(blobs);
            break;
        default:
        Y_ABORT("uknown type");
    };
}

void TBatch::UnpackToType1(TVector<TClientBlob> *blobs) const {
    Y_ABORT_UNLESS(Header.GetFormat() == NKikimrPQ::TBatchHeader::ECompressed);
    Y_ABORT_UNLESS(PackedData.size());
    ui32 totalBlobs = Header.GetCount() + Header.GetInternalPartsCount();
    ui32 partsSize = 0;
    TVector<ui32> end;
    TVector<ui32> pos;
    pos.reserve(totalBlobs);
    const char* data = PackedData.data();
    const char* dataEnd = PackedData.data() + PackedData.size();
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
    if (Header.GetHasKinesis()) {
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

    Y_ABORT_UNLESS(data == dataEnd);

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

void TBatch::UnpackToType0(TVector<TClientBlob> *blobs) const {
    Y_ABORT_UNLESS(Header.GetFormat() == NKikimrPQ::TBatchHeader::EUncompressed);
    Y_ABORT_UNLESS(PackedData.size());
    ui32 shift = 0;

    for (ui32 i = 0; i < GetCount() + GetInternalPartsCount(); ++i) {
        Y_ABORT_UNLESS(shift < PackedData.size());
        blobs->push_back(TClientBlob::Deserialize(PackedData.data() + shift, PackedData.size() - shift));
        shift += ReadUnaligned<ui32>(PackedData.data() + shift);
    }
    Y_ABORT_UNLESS(shift == PackedData.size());
}


}
