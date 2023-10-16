#pragma once

#include "defs.h"
#include "incrhuge_keeper_common.h"

#include <ydb/core/base/blobstorage.h>

namespace NKikimr {
    namespace NIncrHuge {

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // VDISK <-> INCREMENTAL HUGE BLOB KEEPER INTERFACE
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        // NOTICE: for every request message TEvX there is only single response TEvXResult, containing operation status;
        // one can rely on actual operation execution ONLY if returned status indicates success. To match requests with
        // responses user can use Cookie field in IEventHolder class -- it is copied from request to response for every
        // type of query incremental huge blob keeper provides.

        // TEvIncrHugeInit message is sent from VDisk to incremental huge blob keeper to obtain its personal identifier
        // (owner), which is used in further operations. This 'Owner' is different from the owner obtained from PDisk.
        struct TEvIncrHugeInit : public NActors::TEventLocal<TEvIncrHugeInit, TEvBlobStorage::EvIncrHugeInit> {
            TVDiskID VDiskId;  // VDisk identifier that uniquely identifies VDisk on this keeper
            ui8      Owner;    // owner id for this VDisk on underlying PDisk
            ui64     FirstLsn; // first LSN that is expected to appear in returned list of blobs

            TEvIncrHugeInit(const TVDiskID& vdiskId, ui8 owner, ui64 firstLsn)
                : VDiskId(vdiskId)
                , Owner(owner)
                , FirstLsn(firstLsn)
            {}
        };

        // TEvIncrHugeInitResult is sent in response to TEvIncrHugeInit message and contains status indicating whether
        // operation was carried out successfully, Owner to provide in following requests and a vector of blobs sorted
        // by LSN contaning metadata. This vector is used in recovery of VDisk as it doesn't log incremental huge blob
        // writes to its own log, but it needs to reproduce them at recovery to fill in fresh chunk and generate sync
        // log data.
        struct TEvIncrHugeInitResult : public NActors::TEventLocal<TEvIncrHugeInitResult, TEvBlobStorage::EvIncrHugeInitResult> {
            struct TItem {
                TIncrHugeBlobId Id;   // blob identifier in space of incremental huge blob keeper
                ui64            Lsn;  // LSN of this blob, provided by VDisk when writing this blob
                TBlobMetadata   Meta; // no comments :)
            };

            NKikimrProto::EReplyStatus Status; // operation status
            TVector<TItem>             Items;  // a vector of items with matching LSN's

            TEvIncrHugeInitResult(NKikimrProto::EReplyStatus status, TVector<TItem>&& items)
                : Status(status)
                , Items(std::move(items))
            {}
        };

        // TWritePayload structure contains user fields needed by VDisk when receiving write confirmation. It is
        // simply moved from request to response.
        struct TWritePayload {
            virtual ~TWritePayload() = default;
        };

        using TWritePayloadPtr = std::unique_ptr<TWritePayload>;

        // TEvIncrHugeWrite -- write new blob. Incremental huge blob keeper allocates an identifier (TIncrHugeBlobId)
        // and writes provided data and metadata into storage. We have to mention that identifiers are reused if being
        // deleted, so, if there is a race between delete and write request, write may return identifier just deleted,
        // but with new content. Example: client sends write and gets identifier A. Then client sends delete A and write
        // other blob. If there is a race between write and delete inside keeper (which of them is executed first), then
        // keeper may return A for just written blob and then return success for deletion. It is normal situation. Also
        // this operation doesn't write to log and while huge blob keeper receives only writes (excluding reads and
        // deletes), it maintains strictly sequential disk write requests, this achieving maximum performance.
        struct TEvIncrHugeWrite : public NActors::TEventLocal<TEvIncrHugeWrite, TEvBlobStorage::EvIncrHugeWrite> {
            ui8              Owner;   // the VDisk who writes the data
            ui64             Lsn;     // non-transparent number from owner; compared to FirstLsn on init
            TBlobMetadata    Meta;    // non-transparent blob metadata
            TString           Data;    // buffer with payload; should not be less that minimal blob size
            TWritePayloadPtr Payload; // structure that is moved from request to response

            TEvIncrHugeWrite(ui8 owner, ui64 lsn, const TBlobMetadata& meta, TString&& data, TWritePayloadPtr&& payload)
                : Owner(owner)
                , Lsn(lsn)
                , Meta(meta)
                , Data(std::move(data))
                , Payload(std::move(payload))
            {}
        };

        // TEvIncrHugeWriteResult is sent in response to successful (or unsuccessful) writing of blob. It contains
        // some metadata from request (including TWritePayload) and identifier of just written blob (in case of success).
        struct TEvIncrHugeWriteResult : public NActors::TEventLocal<TEvIncrHugeWriteResult,
                TEvBlobStorage::EvIncrHugeWriteResult> {
            NKikimrProto::EReplyStatus Status;     // operation status
            TIncrHugeBlobId            Id;         // identifier of just written blob in case of success
            TWritePayloadPtr           Payload;    // field moved from request

            TEvIncrHugeWriteResult(NKikimrProto::EReplyStatus status, TIncrHugeBlobId id, TWritePayloadPtr&& payload)
                : Status(status)
                , Id(id)
                , Payload(std::move(payload))
            {}
        };

        // TEvIncrHugeRead request is sent from VDisk to read previously written blob. User should provide correct and
        // existing blob id, otherwise system will crash on Y_ABORT_UNLESS(because VDisk's database became inconsistent).
        // If Size field is zero, then blob is read up to end starting at Offset.
        struct TEvIncrHugeRead : public NActors::TEventLocal<TEvIncrHugeRead, TEvBlobStorage::EvIncrHugeRead> {
            ui8             Owner;  // the owner who wants to read the data
            TIncrHugeBlobId Id;     // identifier of blob being read
            ui32            Offset; // offset inside blob in bytes
            ui32            Size;   // size to read; if set to zero, then blob is read up to end (starting at offset)

            TEvIncrHugeRead(ui8 owner, TIncrHugeBlobId id, ui32 offset, ui32 size)
                : Owner(owner)
                , Id(id)
                , Offset(offset)
                , Size(size)
            {}
        };

        // TEvIncrHugeReadResult contains result of reading blob -- status and payload (in case of success). Payload
        // contains only requested part of blob.
        struct TEvIncrHugeReadResult : public NActors::TEventLocal<TEvIncrHugeReadResult, TEvBlobStorage::EvIncrHugeReadResult> {
            NKikimrProto::EReplyStatus Status; // operation status
            TString                     Data;   // requested blob data range

            TEvIncrHugeReadResult(NKikimrProto::EReplyStatus status, TString&& data)
                : Status(status)
                , Data(std::move(data))
            {}
        };

        // TEvIncrHugeDelete is sent by VDisk to delete unused blob(s). Usually this happens while compacting, but there
        // is no general limit to use this operation. Upon receiving, it generates a log record containing some metadata
        // of deleted blobs, so then this operation is called frequently, it degrades performance of whole system as it
        // starts using log. There is a special identifier -- sequence number -- that is used to prevent repetition of
        // the same operation (e.g. when VDisk issues delete request, then fails and during log replay it issues the
        // same request). It is the responsibility of client to ensure that delete request completes successfully on
        // system failure and further recovery. For example, after compaction, when VDisk writes entrypoint with new
        // SST set and deleted blobs, it should not write next entrypoint of this type until deletion succeeds, nor
        // advance FirstLsnToKeep.
        struct TEvIncrHugeDelete : public NActors::TEventLocal<TEvIncrHugeDelete, TEvBlobStorage::EvIncrHugeDelete> {
            ui8                      Owner; // owner who wants to delete the data
            ui64                     SeqNo; // sequence number; should increase for each query from the same client
            TVector<TIncrHugeBlobId> Ids;   // a vector of blobs user wants to delete

            TEvIncrHugeDelete(ui8 owner, ui64 seqNo, TVector<TIncrHugeBlobId>&& ids)
                : Owner(owner)
                , SeqNo(seqNo)
                , Ids(std::move(ids))
            {}
        };

        // TEvIncrHugeDeleteResult is sent in response to TEvIncrHugeDelete query. Contains just status.
        struct TEvIncrHugeDeleteResult : public NActors::TEventLocal<TEvIncrHugeDeleteResult, TEvBlobStorage::EvIncrHugeDeleteResult> {
            NKikimrProto::EReplyStatus Status; // operation status

            TEvIncrHugeDeleteResult(NKikimrProto::EReplyStatus status)
                : Status(status)
            {}
        };

        // TEvIncrHugeHarakiri is sent by client to indicate that is wants to destroy all its data inside this keeper
        // and cease functioning. FIXME: not implemented yet
        struct TEvIncrHugeHarakiri : public NActors::TEventLocal<TEvIncrHugeHarakiri, TEvBlobStorage::EvIncrHugeHarakiri> {
            ui8 Owner;
        };

        // TEvIncrHugeHarakiriResult is sent in response to TEvIncrHugeHarakiri.
        struct TEvIncrHugeHarakiriResult : public NActors::TEventLocal<TEvIncrHugeHarakiriResult, TEvBlobStorage::EvIncrHugeHarakiriResult> {
            NKikimrProto::EReplyStatus Status; // operation status
        };

        // TEvIncrHugeControlDefrag is used in tests and controls defragmenter threshold. FIXME: remove?
        struct TEvIncrHugeControlDefrag : public NActors::TEventLocal<TEvIncrHugeControlDefrag, TEvBlobStorage::EvIncrHugeControlDefrag> {
            double Threshold;

            TEvIncrHugeControlDefrag(double threshold)
                : Threshold(threshold)
            {}
        };

        // helper function used to construct incremental huge blob keeper's actod id on a local pdisk with specific
        // identifier
        inline NActors::TActorId MakeIncrHugeKeeperId(ui32 pdiskId) {
            char x[12] = {'b', 's', 'I', 'n', 'c', 'r', 'H', 'K', 0, 0, 0, 0};
            x[8] = pdiskId;
            x[9] = pdiskId >> 8;
            x[10] = pdiskId >> 16;
            x[11] = pdiskId >> 24;
            return NActors::TActorId(0, TStringBuf(x, 12));
        }

        inline ui32 PDiskIdFromIncrHugeKeeperId(const TActorId& keeperId) {
            ui64 raw2 = keeperId.RawX2();
            Y_ABORT_UNLESS(raw2 < (ui64(1) << 32));
            ui32 pdiskId = raw2;
            Y_DEBUG_ABORT_UNLESS(keeperId == MakeIncrHugeKeeperId(pdiskId));
            return pdiskId;
        }

    } // NIncrHuge
} // NKikimr
