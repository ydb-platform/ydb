#pragma once
#include "header.h"
#include "key.h"

#include <util/datetime/base.h>
#include <util/generic/size_literals.h>
#include <util/generic/maybe.h>
#include <util/generic/vector.h>

#include <deque>

namespace NKikimr {
namespace NPQ {

struct TPartData {
    ui16 PartNo;
    ui16 TotalParts;
    ui32 TotalSize;

    TPartData(const ui16 partNo, const ui16 totalParts, const ui32 totalSize)
        : PartNo(partNo)
        , TotalParts(totalParts)
        , TotalSize(totalSize)
    {}
};

struct TClientBlob {

    static const ui8 HAS_PARTDATA = 1;
    static const ui8 HAS_TS = 2;
    static const ui8 HAS_TS2 = 4;
    static const ui8 HAS_US = 8;
    static const ui8 HAS_KINESIS = 16;

    TString SourceId;
    ui64 SeqNo;
    TString Data;
    TMaybe<TPartData> PartData;
    TInstant WriteTimestamp;
    TInstant CreateTimestamp;
    ui32 UncompressedSize;
    TString PartitionKey;
    TString ExplicitHashKey;

    TClientBlob()
        : SeqNo(0)
        , UncompressedSize(0)
    {}

    TClientBlob(const TString& sourceId, const ui64 seqNo, const TString& data, TMaybe<TPartData> &&partData, TInstant writeTimestamp, TInstant createTimestamp,
                const ui64 uncompressedSize, const TString& partitionKey, const TString& explicitHashKey)
        : SourceId(sourceId)
        , SeqNo(seqNo)
        , Data(data)
        , PartData(std::move(partData))
        , WriteTimestamp(writeTimestamp)
        , CreateTimestamp(createTimestamp)
        , UncompressedSize(uncompressedSize)
        , PartitionKey(partitionKey)
        , ExplicitHashKey(explicitHashKey)
    {
        Y_ABORT_UNLESS(PartitionKey.size() <= 256);
    }

    ui32 GetPartDataSize() const {
        if (PartData) {
            return 1 + sizeof(ui16) + sizeof(ui16) + sizeof(ui32);
        }
        return 1;
    }

    ui32 GetKinesisSize() const {
        if (PartitionKey.size() > 0) {
            return 2 + PartitionKey.size() + ExplicitHashKey.size();
        }
        return 0;
    }

    ui32 GetBlobSize() const {
        return GetPartDataSize() + OVERHEAD + SourceId.size() + Data.size() + (UncompressedSize == 0 ? 0 : sizeof(ui32)) + GetKinesisSize();
    }

    ui16 GetPartNo() const {
        return PartData ? PartData->PartNo : 0;
    }

    ui16 GetTotalParts() const {
        return PartData ? PartData->TotalParts : 1;
    }

    ui16 GetTotalSize() const {
        return PartData ? PartData->TotalSize : UncompressedSize;
    }

    bool IsLastPart() const {
        return !PartData || PartData->PartNo + 1 == PartData->TotalParts;
    }

    static constexpr ui32 OVERHEAD = sizeof(ui32)/*totalSize*/ + sizeof(ui64)/*SeqNo*/ + sizeof(ui16) /*SourceId*/ + sizeof(ui64) /*WriteTimestamp*/ + sizeof(ui64) /*CreateTimestamp*/;

    void SerializeTo(TBuffer& buffer) const;
    static TClientBlob Deserialize(const char *data, ui32 size);

    static void CheckBlob(const TKey& key, const TString& blob); 
};

static constexpr const ui32 MAX_BLOB_SIZE = 8_MB;

//TBatch represents several clientBlobs. Can be in unpacked state(TVector<TClientBlob> blobs)
//or packed(PackedData)
//on disk representation:
//<ui16 size><serialized proto header><payload>
// size == serialized proto size
// header.PayloadSize == payload size
// payload contains of <ui8 type><data>
// type=0 - not packed serialized data.

struct TBatch {
    bool Packed;
    TVector<TClientBlob> Blobs;
    TVector<ui32> InternalPartsPos;
    NKikimrPQ::TBatchHeader Header;
    TBuffer PackedData;
    TBatch()
        : Packed(false)
    {
        PackedData.Reserve(8_MB);
    }

    TBatch(const ui64 offset, const ui16 partNo, const TVector<TClientBlob>& blobs)
        : Packed(false)
    {
        PackedData.Reserve(8_MB);
        Header.SetOffset(offset);
        Header.SetPartNo(partNo);
        Header.SetUnpackedSize(0);
        Header.SetCount(0);
        Header.SetInternalPartsCount(0);
        for (auto& b : blobs) {
            AddBlob(b);
        }
    }

    TBatch(const ui64 offset, const ui16 partNo, const std::deque<TClientBlob>& blobs)
        : Packed(false)
    {
        PackedData.Reserve(8_MB);
        Header.SetOffset(offset);
        Header.SetPartNo(partNo);
        Header.SetUnpackedSize(0);
        Header.SetCount(0);
        Header.SetInternalPartsCount(0);
        for (auto& b : blobs) {
            AddBlob(b);
        }
    }

    void AddBlob(const TClientBlob &b) {
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
    }

    ui64 GetOffset() const {
        return Header.GetOffset();
    }
    ui16 GetPartNo() const {
        return Header.GetPartNo();
    }
    ui32 GetUnpackedSize() const {
        return Header.GetUnpackedSize();
    }
    ui32 GetCount() const {
        return Header.GetCount();
    }
    ui16 GetInternalPartsCount() const {
        return Header.GetInternalPartsCount();
    }

    TBatch(const NKikimrPQ::TBatchHeader &header, const char* data)
        : Packed(true)
        , Header(header)
        , PackedData(data, header.GetPayloadSize())
    {
    }

    ui32 GetPackedSize() const { Y_ABORT_UNLESS(Packed); return sizeof(ui16) + PackedData.size() + Header.ByteSize(); }
    void Pack();
    void Unpack();
    void UnpackTo(TVector<TClientBlob> *result);
    void UnpackToType0(TVector<TClientBlob> *result);
    void UnpackToType1(TVector<TClientBlob> *result);

    void SerializeTo(TString& res) const;

    ui32 FindPos(const ui64 offset, const ui16 partNo) const;

};

class TBlobIterator {
public:
    TBlobIterator(const TKey& key, const TString& blob);

    //return true is there is batch
    bool IsValid();
    //get next batch and return false if there is no next batch
    bool Next();

    TBatch GetBatch();
private:
    void ParseBatch();

    NKikimrPQ::TBatchHeader Header;

    const TKey& Key;
    const char *Data;
    const char *End;

    ui64 Offset;
    ui32 Count;
    ui16 InternalPartsCount;
};

//THead represents bathes, stored in head(at most 8 Mb)
struct THead {
    std::deque<TBatch> Batches;
    //all batches except last must be packed
    // BlobsSize <= 512Kb
    // size of Blobs after packing must be <= BlobsSize
    //otherwise head will be compacted not in total, some blobs will still remain in head
    //PackedSize + BlobsSize must be <= 8Mb
    ui64 Offset;
    ui16 PartNo;
    ui32 PackedSize;

    THead()
    : Offset(0)
    , PartNo(0)
    , PackedSize(0)
    {}

    void Clear();

    ui64 GetNextOffset() const;

    ui32 GetCount() const;

    ui16 GetInternalPartsCount() const;

    //return Max<ui32> if not such pos in head
    //returns batch with such position
    ui32 FindPos(const ui64 offset, const ui16 partNo) const;
};

IOutputStream& operator <<(IOutputStream& out, const THead& value);


//stucture for tracking written KV-blobs, stored in memory parts of one partitioned blob
class TPartitionedBlob {
public:
    TPartitionedBlob& operator=(const TPartitionedBlob& x);

    TPartitionedBlob(const TPartitionedBlob& x);

    TPartitionedBlob(const TPartitionId& partition, const ui64 offset, const TString& sourceId, const ui64 seqNo,
                     const ui16 totalParts, const ui32 totalSize, THead& head, THead& newHead, bool headCleared, bool needCompactHead, const ui32 maxBlobSize,
                     ui16 nextPartNo = 0);

    struct TFormedBlobInfo {
        TKey Key;
        TString Value;
    };

    std::optional<TFormedBlobInfo> Add(TClientBlob&& blob);
    std::optional<TFormedBlobInfo> Add(const TKey& key, ui32 size);

    bool IsInited() const { return !SourceId.empty(); }

    bool IsComplete() const;

    bool HasFormedBlobs() const { return !FormedBlobs.empty(); }

    ui64 GetOffset() const { return Offset; }
    ui16 GetHeadPartNo() const { return HeadPartNo; }

    bool IsNextPart(const TString& sourceId, const ui64 seqNo, const ui16 partNo, TString *reason) const;

    struct TRenameFormedBlobInfo {
        TRenameFormedBlobInfo() = default;
        TRenameFormedBlobInfo(const TKey& oldKey, const TKey& newKey, ui32 size);

        TKey OldKey;
        TKey NewKey;
        ui32 Size;
    };

    const std::deque<TClientBlob>& GetClientBlobs() const { return Blobs; }
    const std::deque<TRenameFormedBlobInfo>& GetFormedBlobs() const { return FormedBlobs; }

private:
    TString CompactHead(bool glueHead, THead& head, bool glueNewHead, THead& newHead, ui32 estimatedSize);
    std::optional<TFormedBlobInfo> CreateFormedBlob(ui32 size, bool useRename);

private:
    TPartitionId Partition;
    ui64 Offset;
    ui16 InternalPartsCount;
    ui64 StartOffset;
    ui16 StartPartNo;
    TString SourceId;
    ui64 SeqNo;
    ui16 TotalParts;
    ui32 TotalSize;
    ui16 NextPartNo;
    ui16 HeadPartNo;
    std::deque<TClientBlob> Blobs;
    ui32 BlobsSize;
    std::deque<TRenameFormedBlobInfo> FormedBlobs;
    THead &Head;
    THead &NewHead;
    ui32 HeadSize;
    bool GlueHead;
    bool GlueNewHead;
    bool NeedCompactHead;
    ui32 MaxBlobSize;
};

}// NPQ
}// NKikimr
