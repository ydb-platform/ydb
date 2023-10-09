#include "blobstorage_hulllog.h"
#include <ydb/core/blobstorage/vdisk/huge/blobstorage_hullhuge.h>

namespace NKikimr {

    class THullCallback : public NPDisk::TEvLog::ICallback {
         const TLsnSeg Seg;
         TIntrusivePtr<TVDiskContext> VCtx;
         const TActorId SkeletonId;
         const TActorId SyncLogId;
         const TActorId HugeKeeperId;
         std::unique_ptr<IEventBase> SyncLogMsg;
         std::unique_ptr<TEvHullHugeBlobLogged> HugeKeeperNotice;

    public:
        THullCallback(TLsnSeg seg, const TIntrusivePtr<TVDiskContext> &vctx, TActorId skeletonId, TActorId syncLogId,
                TActorId hugeKeeperId, std::unique_ptr<IEventBase> syncLogMsg,
                std::unique_ptr<TEvHullHugeBlobLogged> hugeKeeperNotice)
            : Seg(seg)
            , VCtx(vctx)
            , SkeletonId(skeletonId)
            , SyncLogId(syncLogId)
            , HugeKeeperId(hugeKeeperId)
            , SyncLogMsg(std::move(syncLogMsg))
            , HugeKeeperNotice(std::move(hugeKeeperNotice))
        {}

        void operator ()(TActorSystem *actorSystem, const NPDisk::TEvLogResult &ev) override {
            // check if response is good; return on shutdown
            if (!VCtx->CheckPDiskResponse(*actorSystem, ev))
                return;

            const bool syncLogAlso = static_cast<bool>(SyncLogMsg);
            if (syncLogAlso) {
                actorSystem->Send(SyncLogId, SyncLogMsg.release());
            }

            if (HugeKeeperNotice) {
                Y_ABORT_UNLESS(HugeKeeperId);
                actorSystem->Send(HugeKeeperId, HugeKeeperNotice.release());
            }
        }
    };

    std::unique_ptr<NPDisk::TEvLog> CreateHullUpdate(const std::shared_ptr<THullLogCtx> &hullLogCtx,
                                             TLogSignature signature,
                                             const TString &data,
                                             TLsnSeg seg,
                                             void *cookie,
                                             std::unique_ptr<IEventBase> syncLogMsg,
                                             std::unique_ptr<TEvHullHugeBlobLogged> hugeKeeperNotice)
    {
        auto callback = std::make_unique<THullCallback>(seg, hullLogCtx->VCtx, hullLogCtx->SkeletonId,
              hullLogCtx->SyncLogId, hullLogCtx->HugeKeeperId, std::move(syncLogMsg),
              std::move(hugeKeeperNotice));

        return std::make_unique<NPDisk::TEvLog>(hullLogCtx->PDiskCtx->Dsk->Owner,
                                  hullLogCtx->PDiskCtx->Dsk->OwnerRound,
                                  signature,
                                  TRcBuf(data), //FIXME(innokentii) wrapping
                                  seg,
                                  cookie,
                                  std::move(callback));
    }


    std::unique_ptr<NPDisk::TEvLog> CreateHullUpdate(const std::shared_ptr<THullLogCtx> &hullLogCtx,
                                             TLogSignature signature,
                                             const NPDisk::TCommitRecord &commitRecord,
                                             const TString &data,
                                             TLsnSeg seg,
                                             void *cookie,
                                             std::unique_ptr<IEventBase> syncLogMsg)
    {
        auto callback = std::make_unique<THullCallback>(seg, hullLogCtx->VCtx, hullLogCtx->SkeletonId,
              hullLogCtx->SyncLogId, TActorId(), std::move(syncLogMsg), nullptr);

        return std::make_unique<NPDisk::TEvLog>(hullLogCtx->PDiskCtx->Dsk->Owner,
                                  hullLogCtx->PDiskCtx->Dsk->OwnerRound,
                                  signature,
                                  commitRecord,
                                  TRcBuf(data), //FIXME(innokentii) wrapping
                                  seg,
                                  cookie,
                                  std::move(callback));
    }
    std::unique_ptr<NPDisk::TEvLog> CreateHullUpdate(const std::shared_ptr<THullLogCtx> &hullLogCtx,
                                             TLogSignature signature,
                                             const TRcBuf &data,
                                             TLsnSeg seg,
                                             void *cookie,
                                             std::unique_ptr<IEventBase> syncLogMsg,
                                             std::unique_ptr<TEvHullHugeBlobLogged> hugeKeeperNotice)
    {
        auto callback = std::make_unique<THullCallback>(seg, hullLogCtx->VCtx, hullLogCtx->SkeletonId,
              hullLogCtx->SyncLogId, hullLogCtx->HugeKeeperId, std::move(syncLogMsg),
              std::move(hugeKeeperNotice));

        return std::make_unique<NPDisk::TEvLog>(hullLogCtx->PDiskCtx->Dsk->Owner,
                                  hullLogCtx->PDiskCtx->Dsk->OwnerRound,
                                  signature,
                                  data,
                                  seg,
                                  cookie,
                                  std::move(callback));
    }


    std::unique_ptr<NPDisk::TEvLog> CreateHullUpdate(const std::shared_ptr<THullLogCtx> &hullLogCtx,
                                             TLogSignature signature,
                                             const NPDisk::TCommitRecord &commitRecord,
                                             const TRcBuf &data,
                                             TLsnSeg seg,
                                             void *cookie,
                                             std::unique_ptr<IEventBase> syncLogMsg)
    {
        auto callback = std::make_unique<THullCallback>(seg, hullLogCtx->VCtx, hullLogCtx->SkeletonId,
              hullLogCtx->SyncLogId, TActorId(), std::move(syncLogMsg), nullptr);

        return std::make_unique<NPDisk::TEvLog>(hullLogCtx->PDiskCtx->Dsk->Owner,
                                  hullLogCtx->PDiskCtx->Dsk->OwnerRound,
                                  signature,
                                  commitRecord,
                                  data,
                                  seg,
                                  cookie,
                                  std::move(callback));
    }

} // NKikimr
