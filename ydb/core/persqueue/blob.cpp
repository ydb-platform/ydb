#include "blob.h"
#include "header.h"

#include <util/string/builder.h>
#include <util/string/escape.h>
#include <util/system/unaligned_mem.h>

namespace NKikimr {
namespace NPQ {

namespace {

}

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




//
// THead
//

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

void THead::AddBlob(const TClientBlob& blob) {
    Y_ABORT_UNLESS(!Batches.empty());
    auto& batch = Batches.back();
    InternalPartsCount -= batch.GetInternalPartsCount();
    batch.AddBlob(blob);
    InternalPartsCount += batch.GetInternalPartsCount();
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


//
// THead::TBatchAccessor 
//

THead::TBatchAccessor THead::MutableBatch(ui32 idx) {
    Y_ABORT_UNLESS(idx < Batches.size());
    return TBatchAccessor(Batches[idx]);
}

THead::TBatchAccessor THead::MutableLastBatch() {
    Y_ABORT_UNLESS(!Batches.empty());
    return TBatchAccessor(Batches.back());
}

THead::TBatchAccessor::TBatchAccessor(TBatch& batch)
    : Batch(batch)
{}

void THead::TBatchAccessor::Pack() {
    Batch.Pack();
}

void THead::TBatchAccessor::Unpack() {
    Batch.Unpack();
}


//
// TPartitionedBlob
//

bool TPartitionedBlob::IsInited() const {
    return TotalParts > 0;
}

bool TPartitionedBlob::HasFormedBlobs() const {
    return !FormedBlobs.empty();
}

ui64 TPartitionedBlob::GetOffset() const {
    return Offset;
}

ui16 TPartitionedBlob::GetHeadPartNo() const {
    return HeadPartNo;
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

}// NPQ
}// NKikimr
