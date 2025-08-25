#pragma once
#include "key.h"

#include <util/datetime/base.h>
#include <util/generic/size_literals.h>
#include <util/generic/maybe.h>
#include <util/generic/vector.h>

#include <deque>

namespace NKikimr {
namespace NPQ {

class TBlobSerializer;

// Large messages are split into small 512KB parts, and stored in separate parts.
// This structure stores information about the saved part of a large message.
struct TPartData {
    // The serial number of the message part. The first part has a value of 0.
    ui16 PartNo;
    // The number of parts that the message was splitted.
    ui16 TotalParts;
    // The size of the original message in bytes (the sum of the sizes of all parts)
    ui32 TotalSize;

    TPartData(const ui16 partNo, const ui16 totalParts, const ui32 totalSize);

    bool IsLastPart() const;
};

struct TClientBlob {
    static constexpr ui32 OVERHEAD = sizeof(ui32)/*totalSize*/ + sizeof(ui64)/*SeqNo*/ + sizeof(ui16) /*SourceId*/
        + sizeof(ui64) /*WriteTimestamp*/ + sizeof(ui64) /*CreateTimestamp*/;

    TString SourceId;
    ui64 SeqNo;
    TString Data;
    TMaybe<TPartData> PartData;
    TInstant WriteTimestamp;
    TInstant CreateTimestamp;
    ui32 UncompressedSize;
    TString PartitionKey;
    TString ExplicitHashKey;

    TClientBlob();
    TClientBlob(TString&& sourceId, ui64 seqNo, TString&& data, const TMaybe<TPartData>& partData,
        const TInstant writeTimestamp, const TInstant createTimestamp,
        const ui64 uncompressedSize, TString&& partitionKey, TString&& explicitHashKey);

    ui32 GetPartDataSize() const;
    ui32 GetKinesisSize() const;
    ui32 GetBlobSize() const;
    ui16 GetPartNo() const;
    ui16 GetTotalParts() const;
    ui16 GetTotalSize() const;
    bool IsLastPart() const;

    void SerializeTo(TBuffer& buffer) const;
    static TClientBlob Deserialize(const char *data, ui32 size);

    static void CheckBlob(const TKey& key, const TString& blob);

    TString DebugString() const;
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
    TInstant EndWriteTimestamp;

    TBatch();
    TBatch(const ui64 offset, const ui16 partNo);
    TBatch(const NKikimrPQ::TBatchHeader &header, const char* data);

    static TBatch FromBlobs(const ui64 offset, std::deque<TClientBlob>&& blobs);

    void AddBlob(const TClientBlob &b);

    ui64 GetOffset() const;
    ui16 GetPartNo() const;
    ui32 GetUnpackedSize() const;
    ui32 GetCount() const;
    ui16 GetInternalPartsCount() const;

    bool IsGreaterThan(ui64 offset, ui16 partNo) const;

    bool Empty() const;
    TInstant GetEndWriteTimestamp() const;

    ui32 GetPackedSize() const;
    void Pack();
    void Unpack();
    void UnpackTo(TVector<TClientBlob> *result) const;
    void UnpackToType0(TVector<TClientBlob> *result) const;
    void UnpackToType1(TVector<TClientBlob> *result) const;

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

class TPartitionedBlob;

//THead represents bathes, stored in head(at most 8 Mb)
struct THead {
    //all batches except last must be packed
    // BlobsSize <= 512Kb
    // size of Blobs after packing must be <= BlobsSize
    //otherwise head will be compacted not in total, some blobs will still remain in head
    //PackedSize + BlobsSize must be <= 8Mb
private:
    std::deque<TBatch> Batches;
    ui16 InternalPartsCount = 0;

    friend class TPartitionedBlob;

    class TBatchAccessor {
        TBatch& Batch;

    public:
        explicit TBatchAccessor(TBatch& batch);

        void Pack();
        void Unpack();
    };

public:
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

    void AddBatch(const TBatch& batch);
    void ClearBatches();
    const std::deque<TBatch>& GetBatches() const;
    const TBatch& GetBatch(ui32 idx) const;
    const TBatch& GetLastBatch() const;
    TBatchAccessor MutableBatch(ui32 idx);
    TBatchAccessor MutableLastBatch();
    TBatch ExtractFirstBatch();
    void AddBlob(const TClientBlob& blob);

    friend IOutputStream& operator <<(IOutputStream& out, const THead& value);
};

IOutputStream& operator <<(IOutputStream& out, const THead& value);


//stucture for tracking written KV-blobs, stored in memory parts of one partitioned blob
class TPartitionedBlob {
public:
    TPartitionedBlob& operator=(const TPartitionedBlob& x);

    TPartitionedBlob(const TPartitionedBlob& x);

    TPartitionedBlob(const TPartitionId& partition, const ui64 offset, const TString& sourceId, const ui64 seqNo,
                     const ui16 totalParts, const ui32 totalSize, THead& head, THead& newHead, bool headCleared, bool needCompactHead, const ui32 maxBlobSize,
                     ui16 nextPartNo = 0, bool fastWrite = true);

    struct TFormedBlobInfo {
        TKey Key;
        TString Value;
    };

    std::optional<TFormedBlobInfo> Add(TClientBlob&& blob);
    std::optional<TFormedBlobInfo> Add(const TKey& key, ui32 size);

    bool IsInited() const;
    bool IsComplete() const;
    bool HasFormedBlobs() const;

    ui64 GetOffset() const;
    ui16 GetHeadPartNo() const;

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
    bool FastWrite = true;
};

}// NPQ
}// NKikimr
