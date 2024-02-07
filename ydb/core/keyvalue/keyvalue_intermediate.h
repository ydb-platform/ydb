#pragma once
#include "defs.h"
#include "keyvalue_key_range.h"
#include "keyvalue_request_stat.h"
#include <ydb/core/base/logoblob.h>
#include <ydb/core/base/blobstorage.h>
#include <ydb/core/protos/base.pb.h>
#include <ydb/core/protos/msgbus_kv.pb.h>
#include <ydb/core/protos/msgbus.pb.h>
#include <ydb/core/util/fragmented_buffer.h>
#include <ydb/core/keyvalue/protos/events.pb.h>
#include <ydb/library/actors/wilson/wilson_span.h>

namespace NKikimr {
namespace NKeyValue {

struct TIntermediate {
    struct TRead {
        struct TReadItem {
            TLogoBlobID LogoBlobId;
            ui32 BlobOffset;
            ui32 BlobSize;
            ui64 ValueOffset;
            NKikimrProto::EReplyStatus Status;
            bool InFlight;

            TReadItem(TLogoBlobID logoBlobId, ui32 blobOffset, ui32 blobSize, ui64 valueOffset)
                : LogoBlobId(logoBlobId)
                  , BlobOffset(blobOffset)
                  , BlobSize(blobSize)
                  , ValueOffset(valueOffset)
                  , Status(NKikimrProto::UNKNOWN)
                  , InFlight(false)
            {}
        };

        TVector<TReadItem> ReadItems;
        TString Key;
        TFragmentedBuffer Value;
        ui32 Offset;
        ui32 Size;
        ui32 ValueSize;
        ui32 RequestedSize = 0;
        ui64 CreationUnixTime;
        NKikimrClient::TKeyValueRequest::EStorageChannel StorageChannel;
        NKikimrBlobStorage::EGetHandleClass HandleClass;
        NKikimrProto::EReplyStatus Status;
        TString Message;

        TRead();
        TRead(const TString &key, ui32 valueSize, ui64 creationUnixTime,
                NKikimrClient::TKeyValueRequest::EStorageChannel storageChannel);
        NKikimrProto::EReplyStatus ItemsStatus() const;
        NKikimrProto::EReplyStatus CumulativeStatus() const;
        TRope BuildRope();
    };
    struct TRangeRead {
        TDeque<TRead> Reads;
        NKikimrBlobStorage::EGetHandleClass HandleClass;
        NKikimrProto::EReplyStatus Status;
        bool IncludeData;
        ui64 LimitBytes;
    };
    struct TWrite {
        TVector<TLogoBlobID> LogoBlobIds;
        TString Key;
        TRope Data;
        TEvBlobStorage::TEvPut::ETactic Tactic;
        NKikimrBlobStorage::EPutHandleClass HandleClass;
        NKikimrProto::EReplyStatus Status;
        TStorageStatusFlags StatusFlags;
        TDuration Latency;
    };
    struct TDelete {
        TKeyRange Range;
    };
    struct TRename {
        TString OldKey;
        TString NewKey;
    };
    struct TCopyRange {
        TKeyRange Range;
        TString PrefixToAdd;
        TString PrefixToRemove;
    };
    struct TConcat {
        TVector<TString> InputKeys;
        TString OutputKey;
        bool KeepInputs;
    };
    struct TGetStatus {
        NKikimrClient::TKeyValueRequest::EStorageChannel StorageChannel;
        ui32 GroupId;
        NKikimrProto::EReplyStatus Status;
        TStorageStatusFlags StatusFlags;
    };
    struct TTrimLeakedBlobs {
        ui32 MaxItemsToTrim;
        TMultiMap<ui32, ui32> ChannelGroupMap;
        TVector<TLogoBlobID> FoundBlobs;
    };
    struct TSetExecutorFastLogPolicy {
        bool IsAllowed;
    };
    struct TPatch {
        struct TDiff {
            ui32 Offset;
            TRope Buffer;
        };

        TString OriginalKey;
        TLogoBlobID OriginalBlobId;
        TString PatchedKey;
        TLogoBlobID PatchedBlobId;

        NKikimrProto::EReplyStatus Status;
        TStorageStatusFlags StatusFlags;

        TVector<TDiff> Diffs;
    };

    using TCmd = std::variant<TWrite, TDelete, TRename, TCopyRange, TConcat, TPatch>;
    using TReadCmd = std::variant<TRead, TRangeRead>;

    TDeque<TRead> Reads;
    TDeque<TRangeRead> RangeReads;
    TDeque<TWrite> Writes;
    TDeque<TPatch> Patches;
    TDeque<TDelete> Deletes;
    TDeque<TRename> Renames;
    TDeque<TCopyRange> CopyRanges;
    TDeque<TConcat> Concats;
    TDeque<TGetStatus> GetStatuses;
    TMaybe<TTrimLeakedBlobs> TrimLeakedBlobs;
    TMaybe<TSetExecutorFastLogPolicy> SetExecutorFastLogPolicy;
    std::deque<std::pair<TLogoBlobID, bool>> RefCountsIncr;

    TStackVec<TCmd, 1> Commands;
    TStackVec<ui32, 1> WriteIndices;
    TStackVec<ui32, 1> PatchIndices;
    std::optional<TReadCmd> ReadCommand;

    ui64 WriteCount = 0;
    ui64 DeleteCount = 0;
    ui64 RenameCount = 0;
    ui64 CopyRangeCount = 0;
    ui64 ConcatCount = 0;

    ui64 Cookie;
    ui64 Generation;
    ui64 RequestUid;
    TInstant Deadline;
    bool HasCookie;
    bool HasGeneration;
    bool HasIncrementGeneration;

    TActorId RespondTo;
    TActorId KeyValueActorId;

    ui64 TotalSize;
    ui64 TotalSizeLimit;
    ui64 TotalReadsScheduled;
    ui64 TotalReadsLimit;
    ui64 SequentialReadLimit;
    bool IsTruncated;

    ui64 CreatedAtGeneration;
    ui64 CreatedAtStep;

    bool IsReplied;

    bool UsePayloadInResponse = false;

    TRequestStat Stat;

    NKikimrClient::TResponse Response;
    std::vector<TRope> Payload;
    NKikimrKeyValue::ExecuteTransactionResult ExecuteTransactionResponse;
    NKikimrKeyValue::GetStorageChannelStatusResult GetStorageChannelStatusResponse;

    THashMap<ui32, NKikimrKeyValue::StorageChannel*> Channels;

    ui32 EvType = 0;

    NWilson::TSpan Span;

    TIntermediate(TActorId respondTo, TActorId keyValueActorId, ui64 channelGeneration, ui64 channelStep,
            TRequestType::EType requestType, NWilson::TTraceId traceId);

    void UpdateStat();
};

} // NKeyValue
} // NKikimr
