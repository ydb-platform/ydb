#include "blob.h"
#include "blob_int.h"
#include "header.h"
#include "type_codecs.h"

#include <util/string/builder.h>
#include <util/string/escape.h>
#include <util/system/unaligned_mem.h>

namespace NKikimr {
namespace NPQ {

//
// TPartData
//

TPartData::TPartData(const ui16 partNo, const ui16 totalParts, const ui32 totalSize)
        : PartNo(partNo)
        , TotalParts(totalParts)
        , TotalSize(totalSize) {
}

bool TPartData::IsLastPart() const {
        return PartNo + 1 == TotalParts;
}


//
// TClientBlob
//

TClientBlob::TClientBlob()
    : SeqNo(0)
    , UncompressedSize(0) {
}

TClientBlob::TClientBlob(TString&& sourceId, ui64 seqNo, TString&& data, const TMaybe<TPartData>& partData,
        const TInstant writeTimestamp, const TInstant createTimestamp, const ui64 uncompressedSize,
        TString&& partitionKey, TString&& explicitHashKey)
        : SourceId(std::move(sourceId))
        , SeqNo(seqNo)
        , Data(std::move(data))
        , PartData(partData)
        , WriteTimestamp(writeTimestamp)
        , CreateTimestamp(createTimestamp)
        , UncompressedSize(uncompressedSize)
        , PartitionKey(std::move(partitionKey))
        , ExplicitHashKey(std::move(explicitHashKey)) {
    Y_ENSURE(PartitionKey.size() <= 256);
}

ui32 TClientBlob::GetPartDataSize() const {
    if (PartData) {
        return 1 + sizeof(ui16) + sizeof(ui16) + sizeof(ui32);
    }
    return 1;
}

ui32 TClientBlob::GetKinesisSize() const {
    if (PartitionKey.size() > 0) {
        return 2 + PartitionKey.size() + ExplicitHashKey.size();
    }
    return 0;
}

ui32 TClientBlob::GetBlobSize() const {
    return GetPartDataSize() + OVERHEAD + SourceId.size() + Data.size() + (UncompressedSize == 0 ? 0 : sizeof(ui32)) + GetKinesisSize();
}

ui16 TClientBlob::GetPartNo() const {
    return PartData ? PartData->PartNo : 0;
}

ui16 TClientBlob::GetTotalParts() const {
    return PartData ? PartData->TotalParts : 1;
}

ui16 TClientBlob::GetTotalSize() const {
    return PartData ? PartData->TotalSize : UncompressedSize;
}

bool TClientBlob::IsLastPart() const {
    return !PartData || PartData->IsLastPart();
}


//
// TBatch
//

TBatch::TBatch()
    : Packed(false)
{
    PackedData.Reserve(8_MB);
}

TBatch::TBatch(const ui64 offset, const ui16 partNo)
    : TBatch()
{
    Header.SetOffset(offset);
    Header.SetPartNo(partNo);
    Header.SetUnpackedSize(0);
    Header.SetCount(0);
    Header.SetInternalPartsCount(0);
}

TBatch::TBatch(const NKikimrPQ::TBatchHeader &header, const char* data)
    : Packed(true)
    , Header(header)
    , PackedData(data, header.GetPayloadSize())
{
}

TBatch TBatch::FromBlobs(const ui64 offset, std::deque<TClientBlob>&& blobs) {
    Y_ABORT_UNLESS(!blobs.empty());
    TBatch batch(offset, blobs.front().GetPartNo());
    for (auto& b : blobs) {
        batch.AddBlob(b);
    }
    return batch;
}

void TBatch::AddBlob(const TClientBlob &b) {
    ui32 count = GetCount();
    ui32 unpackedSize = GetUnpackedSize();
    ui32 i = Blobs.size();
    Blobs.push_back(b);
    unpackedSize += b.GetBlobSize();
    if (b.IsLastPart())
        ++count;
    else {
        InternalPartsPos.push_back(i);
    }

    Header.SetUnpackedSize(unpackedSize);
    Header.SetCount(count);
    Header.SetInternalPartsCount(InternalPartsPos.size());

    EndWriteTimestamp = std::max(EndWriteTimestamp, b.WriteTimestamp);
}

ui64 TBatch::GetOffset() const {
    return Header.GetOffset();
}

ui16 TBatch::GetPartNo() const {
    return Header.GetPartNo();
}

ui32 TBatch::GetUnpackedSize() const {
    return Header.GetUnpackedSize();
}

ui32 TBatch::GetCount() const {
    return Header.GetCount();
}

ui16 TBatch::GetInternalPartsCount() const {
    return Header.GetInternalPartsCount();
}

bool TBatch::IsGreaterThan(ui64 offset, ui16 partNo) const {
    return GetOffset() > offset || GetOffset() == offset && GetPartNo() > partNo;
}

bool TBatch::Empty() const {
    return Blobs.empty();
}

TInstant TBatch::GetEndWriteTimestamp() const {
    return EndWriteTimestamp;
}

ui32 TBatch::GetPackedSize() const {
    Y_ABORT_UNLESS(Packed);
    return sizeof(ui16) + PackedData.size() + Header.ByteSize();
}

//
// TBlobIterator
//


TBlobIterator::TBlobIterator(const TKey& key, const TString& blob)
    : Key(key)
    , Data(blob.c_str())
    , End(Data + blob.size())
    , Offset(key.GetOffset())
    , Count(0)
    , InternalPartsCount(0)
{
    Y_ABORT_UNLESS(Data != End,
                   "Key=%s, blob.size=%" PRISZT,
                   Key.ToString().data(), blob.size());
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

TString TClientBlob::DebugString() const {
    auto sb = TStringBuilder() << "{"
        << " SourceId='" << SourceId << "'"
        << ", SeqNo=" << SeqNo
        << ", WriteTimestamp=" << WriteTimestamp
        << ", CreateTimestamp=" << CreateTimestamp
        << ", UncompressedSize=" << UncompressedSize
        << ", PartitionKey='" << PartitionKey << "'"
        << ", ExplicitHashKey='" << ExplicitHashKey << "'";

    if (PartData) {
        sb << ", PartNo=" << PartData->PartNo
           << ", TotalParts=" << PartData->TotalParts
           << ", TotalSize=" << PartData->TotalSize;
    }

    sb << " }";

    return sb;
}

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

NScheme::TDataRef GetChunk(const char*& data, const char *end)
{
    ui32 size = ReadUnaligned<ui32>(data);
    data += sizeof(ui32) + size;
    Y_ABORT_UNLESS(data <= end);
    return NScheme::TDataRef(data - size, size);
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
    ClearBatches();
}

ui64 THead::GetNextOffset() const
{
    return Offset + GetCount();
}

ui16 THead::GetInternalPartsCount() const
{
    return InternalPartsCount;
}

ui32 THead::GetCount() const
{
    if (Batches.empty())
        return 0;

    //how much offsets before last batch and how much offsets in last batch
    Y_ABORT_UNLESS(Batches.front().GetOffset() == Offset,
                   "front.Offset=%" PRIu64 ", offset=%" PRIu64,
                   Batches.front().GetOffset(), Offset);

    return Batches.back().GetOffset() - Offset + Batches.back().GetCount();
}

IOutputStream& operator <<(IOutputStream& out, const THead& value)
{
    out << "Offset " << value.Offset << " PartNo " << value.PartNo << " PackedSize " << value.PackedSize << " count " << value.GetCount()
        << " nextOffset " << value.GetNextOffset() << " batches " << value.Batches.size();
    return out;
}

ui32 THead::FindPos(const ui64 offset, const ui16 partNo) const {
    if (Batches.empty()) {
        return Max<ui32>();
    }

    ui32 i = Batches.size() - 1;
    while (i > 0 && Batches[i].IsGreaterThan(offset, partNo)) {
        --i;
    }

    if (i == 0) {
        if (Batches[i].IsGreaterThan(offset, partNo)) {
            return Max<ui32>();
        } else {
            return 0;
        }
    }

    return i;
}

void THead::AddBatch(const TBatch& batch) {
    auto& b = Batches.emplace_back(batch);
    InternalPartsCount += b.GetInternalPartsCount();
}

void THead::ClearBatches() {
    Batches.clear();
    InternalPartsCount = 0;
}

const std::deque<TBatch>& THead::GetBatches() const {
    return Batches;
}

const TBatch& THead::GetBatch(ui32 idx) const {
    return Batches.at(idx);
}

const TBatch& THead::GetLastBatch() const {
    Y_ABORT_UNLESS(!Batches.empty());
    return Batches.back();
}

TBatch THead::ExtractFirstBatch() {
    Y_ABORT_UNLESS(!Batches.empty());
    auto batch = std::move(Batches.front());
    InternalPartsCount -= batch.GetInternalPartsCount();
    Batches.pop_front();
    return batch;
}

THead::TBatchAccessor THead::MutableBatch(ui32 idx) {
    Y_ABORT_UNLESS(idx < Batches.size());
    return TBatchAccessor(Batches[idx]);
}

THead::TBatchAccessor THead::MutableLastBatch() {
    Y_ABORT_UNLESS(!Batches.empty());
    return TBatchAccessor(Batches.back());
}

void THead::AddBlob(const TClientBlob& blob) {
    Y_ABORT_UNLESS(!Batches.empty());
    auto& batch = Batches.back();
    InternalPartsCount -= batch.GetInternalPartsCount();
    batch.AddBlob(blob);
    InternalPartsCount += batch.GetInternalPartsCount();
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
    FastWrite = x.FastWrite;
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
    , FastWrite(x.FastWrite)
{}

TPartitionedBlob::TPartitionedBlob(const TPartitionId& partition, const ui64 offset, const TString& sourceId, const ui64 seqNo, const ui16 totalParts,
                                    const ui32 totalSize, THead& head, THead& newHead, bool headCleared, bool needCompactHead, const ui32 maxBlobSize,
                                    const ui16 nextPartNo, const bool fastWrite)
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
    , FastWrite(fastWrite)
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

    TKey tmpKey, dataKey;

    if (FastWrite) {
        tmpKey = TKey::ForFastWrite(TKeyPrefix::TypeTmpData, Partition, StartOffset, StartPartNo, count, InternalPartsCount);
        dataKey = TKey::ForFastWrite(TKeyPrefix::TypeData, Partition, StartOffset, StartPartNo, count, InternalPartsCount);
    } else {
        tmpKey = TKey::ForBody(TKeyPrefix::TypeTmpData, Partition, StartOffset, StartPartNo, count, InternalPartsCount);
        dataKey = TKey::ForBody(TKeyPrefix::TypeData, Partition, StartOffset, StartPartNo, count, InternalPartsCount);
    }

    StartOffset = Offset;
    StartPartNo = NextPartNo;
    InternalPartsCount = 0;

    TString valueD = CompactHead(GlueHead, Head, GlueNewHead, NewHead, HeadSize + BlobsSize + (BlobsSize > 0 ? GetMaxHeaderSize() : 0));

    GlueHead = GlueNewHead = false;
    if (!Blobs.empty()) {
        auto batch = TBatch::FromBlobs(Offset, std::move(Blobs));
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
    Y_ABORT_UNLESS(NewHead.Offset >= Head.Offset,
                   "Head.Offset=%" PRIu64 ", NewHead.Offset=%" PRIu64,
                   Head.Offset, NewHead.Offset);
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
    if (HeadSize + BlobsSize == 0) { //if nothing to compact at all
        NeedCompactHead = false;
    }

    std::optional<TFormedBlobInfo> res;
    if (NeedCompactHead) {
        NeedCompactHead = false;
        res = CreateFormedBlob(0, false);

        StartOffset = NewHead.Offset + NewHead.GetCount();
        NewHead.Clear();
        NewHead.Offset = StartOffset;
    }

    auto newKey = TKey::FromKey(oldKey, TKeyPrefix::TypeData, Partition, StartOffset);
    newKey.SetFastWrite();

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
