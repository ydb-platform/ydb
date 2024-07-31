#include "blob.h"
#include "type_codecs.h"

#include <util/string/builder.h>
#include <util/string/escape.h>
#include <util/system/unaligned_mem.h>

namespace NKikimr {
namespace NPQ {

TBlobIterator::TBlobIterator(const TKey& key, const TString& blob)
    : Key(key)
    , Data(blob.c_str())
    , End(Data + blob.size())
    , Offset(key.GetOffset())
    , Count(0)
    , InternalPartsCount(0)
{
    Y_ABORT_UNLESS(Data != End);
    ParseBatch();
    Y_ABORT_UNLESS(Header.GetPartNo() == Key.GetPartNo());
}

void TBlobIterator::ParseBatch() {
    Y_ABORT_UNLESS(Data < End);
    Header = ExtractHeader(Data, End - Data);
    //Y_ABORT_UNLESS(Header.GetOffset() == Offset);
    Count += Header.GetCount();
    Offset += Header.GetCount();
    InternalPartsCount += Header.GetInternalPartsCount();
    Y_ABORT_UNLESS(Count <= Key.GetCount());
    Y_ABORT_UNLESS(InternalPartsCount <= Key.GetInternalPartsCount());
}

bool TBlobIterator::IsValid()
{
    return Data != End;
}

bool TBlobIterator::Next()
{
    Y_ABORT_UNLESS(IsValid());
    Data += Header.GetPayloadSize() + sizeof(ui16) + Header.ByteSize();
    if (Data == End) { //this was last batch
        Y_ABORT_UNLESS(Count == Key.GetCount());
        Y_ABORT_UNLESS(InternalPartsCount == Key.GetInternalPartsCount());
        return false;
    }
    ParseBatch();
    return true;
}

TBatch TBlobIterator::GetBatch()
{
    Y_ABORT_UNLESS(IsValid());

    return TBatch(Header, Data + sizeof(ui16) + Header.ByteSize());
}

void TClientBlob::CheckBlob(const TKey& key, const TString& blob)
{
    for (TBlobIterator it(key, blob); it.IsValid(); it.Next());
}

void TClientBlob::SerializeTo(TBuffer& res) const
{
    ui32 totalSize = GetBlobSize();
    ui32 psize = res.Size();
    res.Reserve(res.Size() + totalSize);
    res.Append((const char*)&totalSize, sizeof(ui32));
    res.Append((const char*)&SeqNo, sizeof(ui64));
    ui8 outputUncompressedSize = UncompressedSize == 0 ? 0 : HAS_US;
    ui8 outputKinesisData = PartitionKey.empty() ? 0 : HAS_KINESIS;
    if (PartData) {
        ui8 hasPartDataAndTS = HAS_PARTDATA + HAS_TS + HAS_TS2 + outputUncompressedSize + outputKinesisData; //mask
        res.Append((const char*)&hasPartDataAndTS, sizeof(char));
        res.Append((const char*)&(PartData->PartNo), sizeof(ui16));
        res.Append((const char*)&(PartData->TotalParts), sizeof(ui16));
        res.Append((const char*)&(PartData->TotalSize), sizeof(ui32));
    } else {
        ui8 hasTS = HAS_TS + HAS_TS2 + outputUncompressedSize + outputKinesisData; //mask
        res.Append((const char*)&hasTS, sizeof(char));
    }

    if (outputKinesisData) {
        ui8 partitionKeySize = PartitionKey.size();
        res.Append((const char*)&(partitionKeySize), sizeof(ui8));
        res.Append(PartitionKey.data(), PartitionKey.size());
        ui8 hashKeySize = ExplicitHashKey.size();
        res.Append((const char*)&(hashKeySize), sizeof(ui8));
        res.Append(ExplicitHashKey.data(), ExplicitHashKey.size());
    }

    ui64 writeTimestampMs = WriteTimestamp.MilliSeconds();
    ui64 createTimestampMs = CreateTimestamp.MilliSeconds();
    res.Append((const char*)&writeTimestampMs, sizeof(ui64));
    res.Append((const char*)&createTimestampMs, sizeof(ui64));
    if (outputUncompressedSize)
        res.Append((const char*)&(UncompressedSize), sizeof(ui32));

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
    TMaybe<TPartData> partData;
    bool hasPartData = (data[0] & HAS_PARTDATA);//data[0] is mask
    bool hasTS = (data[0] & HAS_TS);
    bool hasTS2 = (data[0] & HAS_TS2);
    bool hasUS = (data[0] & HAS_US);
    bool hasKinesisData = (data[0] & HAS_KINESIS);

    ++data;
    TString partitionKey;
    TString explicitHashKey;

    if (hasPartData) {
        ui16 partNo = ReadUnaligned<ui16>(data);
        data += sizeof(ui16);
        ui16 totalParts = ReadUnaligned<ui16>(data);
        data += sizeof(ui16);
        ui32 totalSize = ReadUnaligned<ui32>(data);
        data += sizeof(ui32);
        partData = TPartData{partNo, totalParts, totalSize};
    }

    if (hasKinesisData) {
        ui8 keySize = ReadUnaligned<ui8>(data);
        data += sizeof(ui8);
        partitionKey = TString(data, keySize == 0 ? 256 : keySize);
        data += partitionKey.size();
        keySize = ReadUnaligned<ui8>(data);
        data += sizeof(ui8);
        explicitHashKey = TString(data, keySize);
        data += explicitHashKey.Size();
    }

    TInstant writeTimestamp;
    TInstant createTimestamp;
    ui32 us = 0;
    if (hasTS) {
        writeTimestamp = TInstant::MilliSeconds(ReadUnaligned<ui64>(data));
        data += sizeof(ui64);
    }
    if (hasTS2) {
        createTimestamp = TInstant::MilliSeconds(ReadUnaligned<ui64>(data));
        data += sizeof(ui64);
    }
    if (hasUS) {
        us = ReadUnaligned<ui32>(data);
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

    return TClientBlob(sourceId, seqNo, dt, std::move(partData), writeTimestamp, createTimestamp, us, partitionKey, explicitHashKey);
}

void TBatch::SerializeTo(TString& res) const{
    Y_ABORT_UNLESS(Packed);

    ui16 sz = Header.ByteSize();
    res.append((const char*)&sz, sizeof(ui16));

    bool rs = Header.AppendToString(&res);
    Y_ABORT_UNLESS(rs);

    res.append(PackedData.data(), PackedData.size());
}

template <typename TCodec>
TAutoPtr<NScheme::IChunkCoder> MakeChunk(TBuffer& output)
{
    TCodec codec;
    return codec.MakeChunk(output);
}

const ui32 CHUNK_SIZE_PLACEMENT = 0xCCCCCCCC;
ui32 WriteTemporaryChunkSize(TBuffer & output)
{
    ui32 sizeOffset = output.Size();

    ui32 sizePlacement = CHUNK_SIZE_PLACEMENT;
    output.Append((const char*)&sizePlacement, sizeof(ui32));

    return sizeOffset;
}

void WriteActualChunkSize(TBuffer& output, ui32 sizeOffset)
{
    ui32 currSize = output.size();
    Y_ABORT_UNLESS(currSize >= sizeOffset + sizeof(ui32));
    ui32 size = currSize - sizeOffset - sizeof(ui32);

    Y_DEBUG_ABORT_UNLESS(ReadUnaligned<ui32>(output.data() + sizeOffset) == CHUNK_SIZE_PLACEMENT);
    WriteUnaligned<ui32>(output.data() + sizeOffset, size);
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
        if (!Blobs[i].IsLastPart())
            InternalPartsPos.push_back(i);
    }
    Y_ABORT_UNLESS(InternalPartsPos.size() == GetInternalPartsCount());
    
    PackedData.Clear();
}

void TBatch::UnpackTo(TVector<TClientBlob> *blobs)
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

NScheme::TDataRef GetChunk(const char*& data, const char *end)
{
    ui32 size = ReadUnaligned<ui32>(data);
    data += sizeof(ui32) + size;
    Y_ABORT_UNLESS(data <= end);
    return NScheme::TDataRef(data - size, size);
}

void TBatch::UnpackToType1(TVector<TClientBlob> *blobs) {
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

    NScheme::TTypeCodecs ui32Codecs(NScheme::NTypeIds::Uint32), ui64Codecs(NScheme::NTypeIds::Uint64), stringCodecs(NScheme::NTypeIds::String);
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
        auto it = partData.find(pos[i]);
        if (it != partData.end())
            pd = it->second;
        (*blobs)[pos[i]] = TClientBlob(sourceIds[currentSID], seqNo[i], dt[i], std::move(pd), wtime[pos[i]], ctime[pos[i]], uncompressedSize[pos[i]],
                                       partitionKey[i], explicitHash[i]);
        if (i + 1 == end[currentSID])
            ++currentSID;
    }
}

void TBatch::UnpackToType0(TVector<TClientBlob> *blobs) {
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


ui32 TBatch::FindPos(const ui64 offset, const ui16 partNo) const {
    Y_ABORT_UNLESS(!Packed);
    if (offset < GetOffset() || offset == GetOffset() && partNo < GetPartNo())
        return Max<ui32>();
    if (offset == GetOffset()) {
        ui32 pos = partNo - GetPartNo();
        return pos < Blobs.size() ? pos : Max<ui32>();
    }
    ui32 pos = offset - GetOffset();
    for (ui32 i = 0; i < InternalPartsPos.size() && InternalPartsPos[i] < pos; ++i)
        ++pos;
    //now pos is position of first client blob from offset
    pos += partNo;
    return  pos < Blobs.size() ? pos : Max<ui32>();
}


void THead::Clear()
{
    Offset = PartNo = PackedSize = 0;
    Batches.clear();
}

ui64 THead::GetNextOffset() const
{
    return Offset + GetCount();
}

ui16 THead::GetInternalPartsCount() const
{
    ui16 res = 0;
    for (auto& b : Batches) {
        res += b.GetInternalPartsCount();
    }
    return res;
}

ui32 THead::GetCount() const
{
    if (Batches.empty())
        return 0;

    //how much offsets before last batch and how much offsets in last batch
    Y_ABORT_UNLESS(Batches.front().GetOffset() == Offset);
    return Batches.back().GetOffset() - Offset + Batches.back().GetCount();
}

IOutputStream& operator <<(IOutputStream& out, const THead& value)
{
    out << "Offset " << value.Offset << " PartNo " << value.PartNo << " PackedSize " << value.PackedSize << " count " << value.GetCount()
        << " nextOffset " << value.GetNextOffset() << " batches " << value.Batches.size();
    return out;
}

ui32 THead::FindPos(const ui64 offset, const ui16 partNo) const {
    ui32 i = 0;
    for (; i < Batches.size(); ++i) {
        //this batch contains blobs with position bigger than requested
        if (Batches[i].GetOffset() > offset || Batches[i].GetOffset() == offset && Batches[i].GetPartNo() > partNo)
             break;
    }
    if (i == 0)
        return Max<ui32>();
    return i - 1;
}

TPartitionedBlob::TRenameFormedBlobInfo::TRenameFormedBlobInfo(const TKey& oldKey, const TKey& newKey, ui32 size) :
    OldKey(oldKey),
    NewKey(newKey),
    Size(size)
{
}

TPartitionedBlob& TPartitionedBlob::operator=(const TPartitionedBlob& x)
{
    Partition = x.Partition;
    Offset = x.Offset;
    InternalPartsCount = x.InternalPartsCount;
    StartOffset = x.StartOffset;
    StartPartNo = x.StartPartNo;
    SourceId = x.SourceId;
    SeqNo = x.SeqNo;
    TotalParts = x.TotalParts;
    TotalSize = x.TotalSize;
    NextPartNo = x.NextPartNo;
    HeadPartNo = x.HeadPartNo;
    Blobs = x.Blobs;
    BlobsSize = x.BlobsSize;
    FormedBlobs = x.FormedBlobs;
    Head = x.Head;
    NewHead = x.NewHead;
    HeadSize = x.HeadSize;
    GlueHead = x.GlueHead;
    GlueNewHead = x.GlueNewHead;
    NeedCompactHead = x.NeedCompactHead;
    MaxBlobSize = x.MaxBlobSize;
    return *this;
}

TPartitionedBlob::TPartitionedBlob(const TPartitionedBlob& x)
    : Partition(x.Partition)
    , Offset(x.Offset)
    , InternalPartsCount(x.InternalPartsCount)
    , StartOffset(x.StartOffset)
    , StartPartNo(x.StartPartNo)
    , SourceId(x.SourceId)
    , SeqNo(x.SeqNo)
    , TotalParts(x.TotalParts)
    , TotalSize(x.TotalSize)
    , NextPartNo(x.NextPartNo)
    , HeadPartNo(x.HeadPartNo)
    , Blobs(x.Blobs)
    , BlobsSize(x.BlobsSize)
    , FormedBlobs(x.FormedBlobs)
    , Head(x.Head)
    , NewHead(x.NewHead)
    , HeadSize(x.HeadSize)
    , GlueHead(x.GlueHead)
    , GlueNewHead(x.GlueNewHead)
    , NeedCompactHead(x.NeedCompactHead)
    , MaxBlobSize(x.MaxBlobSize)
{}

TPartitionedBlob::TPartitionedBlob(const TPartitionId& partition, const ui64 offset, const TString& sourceId, const ui64 seqNo, const ui16 totalParts,
                                    const ui32 totalSize, THead& head, THead& newHead, bool headCleared, bool needCompactHead, const ui32 maxBlobSize,
                                    const ui16 nextPartNo)
    : Partition(partition)
    , Offset(offset)
    , InternalPartsCount(0)
    , StartOffset(head.Offset)
    , StartPartNo(head.PartNo)
    , SourceId(sourceId)
    , SeqNo(seqNo)
    , TotalParts(totalParts)
    , TotalSize(totalSize)
    , NextPartNo(nextPartNo)
    , HeadPartNo(0)
    , BlobsSize(0)
    , Head(head)
    , NewHead(newHead)
    , HeadSize(NewHead.PackedSize)
    , GlueHead(false)
    , GlueNewHead(true)
    , NeedCompactHead(needCompactHead)
    , MaxBlobSize(maxBlobSize)
{
    Y_ABORT_UNLESS(NewHead.Offset == Head.GetNextOffset() && NewHead.PartNo == 0 || headCleared || needCompactHead || Head.PackedSize == 0); // if head not cleared, then NewHead is going after Head
    if (!headCleared) {
        HeadSize = Head.PackedSize + NewHead.PackedSize;
        InternalPartsCount = Head.GetInternalPartsCount() + NewHead.GetInternalPartsCount();
        GlueHead = true;
    } else {
        InternalPartsCount = NewHead.GetInternalPartsCount();
        StartOffset = NewHead.Offset;
        StartPartNo = NewHead.PartNo;
        HeadSize = NewHead.PackedSize;
        GlueHead = false;
    }
    if (HeadSize == 0) {
        StartOffset = offset;
        NewHead.Offset = offset;
        //Y_ABORT_UNLESS(StartPartNo == 0);
    }
}

TString TPartitionedBlob::CompactHead(bool glueHead, THead& head, bool glueNewHead, THead& newHead, ui32 estimatedSize)
{
    TString valueD;
    valueD.reserve(estimatedSize);
    if (glueHead) {
        for (ui32 pp = 0; pp < head.Batches.size(); ++pp) {
            Y_ABORT_UNLESS(head.Batches[pp].Packed);
            head.Batches[pp].SerializeTo(valueD);
        }
    }
    if (glueNewHead) {
        for (ui32 pp = 0; pp < newHead.Batches.size(); ++pp) {
            TBatch *b = &newHead.Batches[pp];
            TBatch batch;
            if (!b->Packed) {
                Y_ABORT_UNLESS(pp + 1 == newHead.Batches.size());
                batch = newHead.Batches[pp];
                batch.Pack();
                b = &batch;
            }
            Y_ABORT_UNLESS(b->Packed);
            b->SerializeTo(valueD);
        }
    }
    return valueD;
}

auto TPartitionedBlob::CreateFormedBlob(ui32 size, bool useRename) -> std::optional<TFormedBlobInfo>
{
    HeadPartNo = NextPartNo;
    ui32 count = (GlueHead ? Head.GetCount() : 0) + (GlueNewHead ? NewHead.GetCount() : 0);

    Y_ABORT_UNLESS(Offset >= (GlueHead ? Head.Offset : NewHead.Offset));

    Y_ABORT_UNLESS(NewHead.GetNextOffset() >= (GlueHead ? Head.Offset : NewHead.Offset));

    TKey tmpKey(TKeyPrefix::TypeTmpData, Partition, StartOffset, StartPartNo, count, InternalPartsCount, false);
    TKey dataKey(TKeyPrefix::TypeData, Partition, StartOffset, StartPartNo, count, InternalPartsCount, false);

    StartOffset = Offset;
    StartPartNo = NextPartNo;
    InternalPartsCount = 0;

    TString valueD = CompactHead(GlueHead, Head, GlueNewHead, NewHead, HeadSize + BlobsSize + (BlobsSize > 0 ? GetMaxHeaderSize() : 0));

    GlueHead = GlueNewHead = false;
    if (!Blobs.empty()) {
        TBatch batch{Offset, Blobs.front().GetPartNo(), std::move(Blobs)};
        Blobs.clear();
        batch.Pack();
        Y_ABORT_UNLESS(batch.Packed);
        batch.SerializeTo(valueD);
    }

    Y_ABORT_UNLESS(valueD.size() <= MaxBlobSize && (valueD.size() + size + 1_MB > MaxBlobSize || HeadSize + BlobsSize + size + GetMaxHeaderSize() <= MaxBlobSize));
    HeadSize = 0;
    BlobsSize = 0;
    TClientBlob::CheckBlob(tmpKey, valueD);
    if (useRename) {
        FormedBlobs.emplace_back(tmpKey, dataKey, valueD.size());
    }
    Blobs.clear();

    return {{useRename ? tmpKey : dataKey, valueD}};
}

auto TPartitionedBlob::Add(TClientBlob&& blob) -> std::optional<TFormedBlobInfo>
{
    Y_ABORT_UNLESS(NewHead.Offset >= Head.Offset);
    ui32 size = blob.GetBlobSize();
    Y_ABORT_UNLESS(InternalPartsCount < 1000); //just check for future packing
    if (HeadSize + BlobsSize + size + GetMaxHeaderSize() > MaxBlobSize) {
        NeedCompactHead = true;
    }
    if (HeadSize + BlobsSize == 0) { //if nothing to compact at all
        NeedCompactHead = false;
    }

    std::optional<TFormedBlobInfo> res;
    if (NeedCompactHead) { // need form blob without last chunk, on start or in case of big head
        NeedCompactHead = false;
        res = CreateFormedBlob(size, true);
    }
    BlobsSize += size + GetMaxHeaderSize();
    ++NextPartNo;
    Blobs.push_back(blob);
    if (!IsComplete()) {
        ++InternalPartsCount;
    }
    return res;
}

auto TPartitionedBlob::Add(const TKey& oldKey, ui32 size) -> std::optional<TFormedBlobInfo>
{
    std::optional<TFormedBlobInfo> res;
    if (NeedCompactHead) {
        NeedCompactHead = false;
        GlueNewHead = false;
        res = CreateFormedBlob(0, false);
    }

    TKey newKey(TKeyPrefix::TypeData,
                Partition,
                NewHead.Offset + oldKey.GetOffset(),
                oldKey.GetPartNo(),
                oldKey.GetCount(),
                oldKey.GetInternalPartsCount(),
                oldKey.IsHead());

    FormedBlobs.emplace_back(oldKey, newKey, size);

    StartOffset += oldKey.GetCount();
    //NewHead.Offset += oldKey.GetOffset() + oldKey.GetCount();

    return res;
}

bool TPartitionedBlob::IsComplete() const
{
    return NextPartNo == TotalParts;
}


bool TPartitionedBlob::IsNextPart(const TString& sourceId, const ui64 seqNo, const ui16 partNo, TString *reason) const
{
    if (sourceId != SourceId || seqNo != SeqNo || partNo != NextPartNo) {
        TStringBuilder s;
        s << "waited sourceId '" << EscapeC(SourceId) << "' seqNo "
          << SeqNo << " partNo " << NextPartNo << " got sourceId '" << EscapeC(sourceId)
          << "' seqNo " << seqNo << " partNo " << partNo;
         *reason = s;
         return false;
    }
    return true;
}


}// NPQ
}// NKikimr

