#pragma once

#include "defs.h"
#include "blobstorage_hullhugerecovery.h"
#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_pdiskctx.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_defrag.h>
#include <ydb/library/actors/wilson/wilson_span.h>

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // TEvHullWriteHugeBlob
    ////////////////////////////////////////////////////////////////////////////
    class TEvHullWriteHugeBlob : public TEventLocal<TEvHullWriteHugeBlob, TEvBlobStorage::EvHullWriteHugeBlob> {
    public:
        const TActorId SenderId;
        const ui64 Cookie;
        const TLogoBlobID LogoBlobId;
        const TIngress Ingress;
        TRope Data;
        const bool IgnoreBlock;
        const NKikimrBlobStorage::EPutHandleClass HandleClass;
        std::unique_ptr<TEvBlobStorage::TEvVPutResult> Result;
        NProtoBuf::RepeatedPtrField<NKikimrBlobStorage::TEvVPut::TExtraBlockCheck> ExtraBlockChecks;

        mutable NLWTrace::TOrbit Orbit;

        TEvHullWriteHugeBlob(const TActorId &senderId,
                             ui64 cookie,
                             const TLogoBlobID &logoBlobId,
                             const TIngress &ingress,
                             TRope&& data,
                             bool ignoreBlock,
                             NKikimrBlobStorage::EPutHandleClass handleClass,
                             std::unique_ptr<TEvBlobStorage::TEvVPutResult> result,
                             NProtoBuf::RepeatedPtrField<NKikimrBlobStorage::TEvVPut::TExtraBlockCheck> *extraBlockChecks)
            : SenderId(senderId)
            , Cookie(cookie)
            , LogoBlobId(logoBlobId)
            , Ingress(ingress)
            , Data(std::move(data))
            , IgnoreBlock(ignoreBlock)
            , HandleClass(handleClass)
            , Result(std::move(result))
        {
            if (extraBlockChecks) {
                ExtraBlockChecks.Swap(extraBlockChecks);
            }
        }

        ui64 ByteSize() const {
            return Data.GetSize();
        }

        TString ToString() const {
            TStringStream str;
            str << "{id# " << LogoBlobId.ToString() << "}";
            return str.Str();
        }
    };

    ////////////////////////////////////////////////////////////////////////////
    // TEvHullLogHugeBlob
    ////////////////////////////////////////////////////////////////////////////
    class TEvHullLogHugeBlob : public TEventLocal<TEvHullLogHugeBlob, TEvBlobStorage::EvHullLogHugeBlob> {
    public:
        const ui64 WriteId;
        const TLogoBlobID LogoBlobID;
        const TIngress Ingress;
        const TDiskPart HugeBlob;
        const bool IgnoreBlock;
        const TActorId OrigClient;
        const ui64 OrigCookie;
        std::unique_ptr<TEvBlobStorage::TEvVPutResult> Result;
        NProtoBuf::RepeatedPtrField<NKikimrBlobStorage::TEvVPut::TExtraBlockCheck> ExtraBlockChecks;

        TEvHullLogHugeBlob(ui64 writeId,
                           const TLogoBlobID &logoBlobID,
                           const TIngress &ingress,
                           const TDiskPart &hugeBlob,
                           bool ignoreBlock,
                           const TActorId &origClient,
                           ui64 origCookie,
                           std::unique_ptr<TEvBlobStorage::TEvVPutResult> result,
                           NProtoBuf::RepeatedPtrField<NKikimrBlobStorage::TEvVPut::TExtraBlockCheck> *extraBlockChecks)
            : WriteId(writeId)
            , LogoBlobID(logoBlobID)
            , Ingress(ingress)
            , HugeBlob(hugeBlob)
            , IgnoreBlock(ignoreBlock)
            , OrigClient(origClient)
            , OrigCookie(origCookie)
            , Result(std::move(result))
        {
            if (extraBlockChecks) {
                ExtraBlockChecks.Swap(extraBlockChecks);
            }
        }
    };

    ////////////////////////////////////////////////////////////////////////////
    // TEvHullHugeBlobLogged
    ////////////////////////////////////////////////////////////////////////////
    class TEvHullHugeBlobLogged : public TEventLocal<TEvHullHugeBlobLogged, TEvBlobStorage::EvHullHugeBlobLogged> {
    public:
        const ui64 WriteId;
        const TDiskPart HugeBlob;
        const ui64 RecLsn;
        const bool SlotIsUsed;

        TEvHullHugeBlobLogged(ui64 writeId, const TDiskPart &hugeBlob,
                              ui64 recLsn, bool slotIsUsed)
            : WriteId(writeId)
            , HugeBlob(hugeBlob)
            , RecLsn(recLsn)
            , SlotIsUsed(slotIsUsed)
        {}

        TString ToString() const {
            TStringStream str;
            str << "{WId# " << WriteId << " HugeBlob# " << HugeBlob.ToString()
                << " Lsn# " << RecLsn << " Used# " << SlotIsUsed << "}";
            return str.Str();
        }
    };

    ////////////////////////////////////////////////////////////////////////////
    // TEvHullFreeHugeSlots
    ////////////////////////////////////////////////////////////////////////////
    class TEvHullFreeHugeSlots : public TEventLocal<TEvHullFreeHugeSlots, TEvBlobStorage::EvHullFreeHugeSlots> {
    public:
        const TDiskPartVec HugeBlobs;
        const ui64 DeletionLsn;
        const TLogSignature Signature; // identifies database we send update for
        const ui64 WId;

        TEvHullFreeHugeSlots(TDiskPartVec &&hugeBlobs, ui64 deletionLsn, TLogSignature signature, ui64 wId)
            : HugeBlobs(std::move(hugeBlobs))
            , DeletionLsn(deletionLsn)
            , Signature(signature)
            , WId(wId)
        {}

        TString ToString() const {
            TStringStream str;
            str << "{" << Signature.ToString() << " DelLsn# " << DeletionLsn << " Slots# " << HugeBlobs.ToString()
                << " WId# " << WId << "}";
            return str.Str();
        }
    };

    ////////////////////////////////////////////////////////////////////////////
    // TEvHugeLockChunks
    // Lock selected chunks for allocation, i.e. slots for these chunks will
    // not be used to store new huge blobs. It's used for defragmentaion purposes,
    // while being defragmented these chunks must not be used for storing new
    // huge blobs
    ////////////////////////////////////////////////////////////////////////////
    class TEvHugeLockChunks : public TEventLocal<TEvHugeLockChunks, TEvBlobStorage::EvHugeLockChunks> {
    public:
        TDefragChunks Chunks;

        TEvHugeLockChunks(TDefragChunks chunks)
            : Chunks(std::move(chunks))
        {}

        TString ToString() const {
            TStringStream str;
            str << "{Chunks# " << FormatList(Chunks) << "}";
            return str.Str();
        }
    };

    class TEvHugeUnlockChunks : public TEventLocal<TEvHugeUnlockChunks, TEvBlobStorage::EvHugeUnlockChunks> {
    public:
        TDefragChunks Chunks;

        TEvHugeUnlockChunks(TDefragChunks chunks)
            : Chunks(std::move(chunks))
        {}
    };

    ////////////////////////////////////////////////////////////////////////////
    // TEvHugeLockChunksResult
    ////////////////////////////////////////////////////////////////////////////
    class TEvHugeLockChunksResult : public TEventLocal<TEvHugeLockChunksResult, TEvBlobStorage::EvHugeLockChunksResult> {
    public:
        TDefragChunks LockedChunks;

        TEvHugeLockChunksResult(TDefragChunks lockedChunks)
            : LockedChunks(std::move(lockedChunks))
        {}
    };

    ////////////////////////////////////////////////////////////////////////////
    // TEvHugeStat
    // Gather huge stat
    ////////////////////////////////////////////////////////////////////////////
    class TEvHugeStat : public TEventLocal<TEvHugeStat, TEvBlobStorage::EvHugeStat> {
    };

    ////////////////////////////////////////////////////////////////////////////
    // TEvHugeStatResult
    // Gather huge stat
    ////////////////////////////////////////////////////////////////////////////
    class TEvHugeStatResult : public TEventLocal<TEvHugeStatResult, TEvBlobStorage::EvHugeStatResult> {
    public:
        NHuge::THeapStat Stat;
    };

    struct TEvHugePreCompact : TEventLocal<TEvHugePreCompact, TEvBlobStorage::EvHugePreCompact> {};

    struct TEvHugePreCompactResult : TEventLocal<TEvHugePreCompactResult, TEvBlobStorage::EvHugePreCompactResult> {
        const ui64 WId; // this is going to be provided in free slots operation
        TEvHugePreCompactResult(ui64 wId) : WId(wId) {}
    };

    ////////////////////////////////////////////////////////////////////////////
    // THugeKeeperCtx
    ////////////////////////////////////////////////////////////////////////////
    class TPDiskCtx;
    class TLsnMngr;
    struct THugeKeeperCtx {
        TIntrusivePtr<TVDiskContext> VCtx;
        TPDiskCtxPtr PDiskCtx;
        TIntrusivePtr<TLsnMngr> LsnMngr;
        TActorId SkeletonId;
        TActorId LoggerId;
        TActorId LogCutterId;
        const TString LocalRecoveryInfoDbg;
        NMonGroup::TLsmHullGroup LsmHullGroup;
        NMonGroup::TDskOutOfSpaceGroup DskOutOfSpaceGroup;
        const bool IsReadOnlyVDisk;

        THugeKeeperCtx(
                TIntrusivePtr<TVDiskContext> vctx,
                TPDiskCtxPtr pdiskCtx,
                TIntrusivePtr<TLsnMngr> lsnMngr,
                TActorId skeletonId,
                TActorId loggerId,
                TActorId logCutterId,
                const TString &localRecoveryInfoDbg,
                bool isReadOnlyVDisk);
        ~THugeKeeperCtx();
    };

    ////////////////////////////////////////////////////////////////////////////
    // CreateHullHugeBlobKeeper
    ////////////////////////////////////////////////////////////////////////////
    IActor *CreateHullHugeBlobKeeper(
            std::shared_ptr<THugeKeeperCtx> hugeKeeperCtx,
            std::shared_ptr<NHuge::THullHugeKeeperPersState> persState);

} // NKikimr
